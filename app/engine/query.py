from llama_index.core import VectorStoreIndex, SQLDatabase, Settings, PromptTemplate
from llama_index.core.query_engine import NLSQLTableQueryEngine, SQLTableRetrieverQueryEngine
from llama_index.core.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
from llama_index.core.tools import QueryEngineTool, ToolMetadata, FunctionTool
from llama_index.core.agent import FunctionAgent, AgentStream
from app.core.factory import LLMFactory, EmbedModelFactory
from app.engine.guardrails import SQLGuardrails
import os
import json
import asyncio
from llama_index.core.llms import ChatMessage, MessageRole
from typing import List, Any, Dict, Optional
import time
import traceback
import re
import contextvars

# Global context for multi-site isolation within cached pipelines
ctx_site_id = contextvars.ContextVar("site_id", default=None)
ctx_audience_target = contextvars.ContextVar("audience_target", default="STD")
ctx_language_id = contextvars.ContextVar("language_id", default="it")

class TenantQueryPipeline:
    def __init__(
        self, 
        tenant_id: str, 
        llm_provider: str, 
        llm_api_key: str,
        llm_model: str = None,
        sql_connection_str: str = None,
        schema_name: str = None,
        allowed_tables: List[str] = None,
        doc_store_path: str = None
    ):
        self.tenant_id = tenant_id
        self.schema_name = schema_name
        
        # 1. Initialize per-tenant LLM and Embed Model
        self.llm = LLMFactory.create_llm(llm_provider, llm_api_key, llm_model)
        self.embed_model = EmbedModelFactory.create_embed_model(llm_provider, llm_api_key)

        # 2. Set global Settings for this request/tenant
        Settings.llm = self.llm
        Settings.embed_model = self.embed_model
        
        if llm_provider == "openai":
            os.environ["OPENAI_API_KEY"] = llm_api_key
        elif llm_provider == "anthropic":
            os.environ["ANTHROPIC_API_KEY"] = llm_api_key
        elif llm_provider == "groq":
            os.environ["GROQ_API_KEY"] = llm_api_key
        elif llm_provider == "gemini":
            os.environ["GOOGLE_API_KEY"] = llm_api_key
            
        print(f"--- Pipeline Init: Tenant {tenant_id} ({llm_provider}) ---")
        
        # 2. Tools list
        self.query_tools = []
        
        # 3. Session memory: storing actual ChatMessage objects
        self.session_memory: Dict[str, List[ChatMessage]] = {}

        # Per-session SQL bypass buffer (avoids race conditions between concurrent users)
        self._sql_bypass: Dict[str, Optional[str]] = {}
        # Per-session Focus (Last entities viewed)
        self.session_focus: Dict[str, Dict[str, Any]] = {}
        # Legacy single-buffer kept for backward compat with streaming path
        self.last_sql_result = None
        self.db_intel = {}
        
        # Load local database intelligence (DDL, Samples)
        try:
            intel_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "db_intelligence.json")
            if os.path.exists(intel_path):
                with open(intel_path, 'r') as f:
                    self.db_intel = json.load(f)
                print(f"--- Loaded Intelligence for {len(self.db_intel.get('tables', {}))} tables ---")
        except Exception as e:
            print(f"[ERROR] Failed to load db_intelligence: {e}")
        if sql_connection_str:
            print(f"--- Init SQL Database (Schema: {self.schema_name or 'public'}) ---")
            
            # --- DOMAIN INTELLIGENCE: Load Semantic Paradigm first to optimize reflection ---
            try:
                current_dir = os.path.dirname(os.path.abspath(__file__))
                dict_path = os.path.join(os.path.dirname(os.path.dirname(current_dir)), "data", "semantic_dictionary.json")
                with open(dict_path, 'r') as f:
                    sem_paradigm = json.load(f)
            except Exception as e:
                print(f"[ERROR] Critical: Failed to load semantic paradigm: {e}")
                sem_paradigm = {"tables": {}}

            from sqlalchemy import create_engine, event
            engine = create_engine(sql_connection_str)
            
            # Diagnostic SQL Logging: Capture every query executed on this engine
            @event.listens_for(engine, "before_cursor_execute")
            def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                print(f"[SQL] Executing: {statement}")
                if parameters:
                    print(f"[SQL] Parameters: {parameters}")
            
            # Optimization: strictly reflect only what's in our semantic dictionary
            # plus any specifically allowed tables that aren't '*'
            known_tables = list(sem_paradigm.get("tables", {}).keys())
            tables_to_reflect = known_tables
            
            if allowed_tables and "*" not in allowed_tables:
                # Further restrict if the tenant has a whitelist
                tables_to_reflect = [t for t in known_tables if t in allowed_tables]
            
            print(f"--- Restricting reflection to {len(tables_to_reflect)} tables ---")

            self.sql_database = SQLDatabase(
                engine, 
                schema=self.schema_name, 
                include_tables=tables_to_reflect, 
                max_string_length=10000
            )

            # --- BROKER INITIALIZATION (Atomic Tools Layer) ---
            from app.engine.broker import MuseumBroker
            self.broker = MuseumBroker(self.sql_database.engine, schema=self.schema_name or "guide")

            # 1. Table Context for Indexer (Filtered by strictly needed tables)
            from llama_index.core.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
            table_node_mapping = SQLTableNodeMapping(self.sql_database)
            table_schema_objs = []
            
            # Only index tables that we actually have in our semantic map
            for t in tables_to_reflect:
                table_info = sem_paradigm.get("tables", {}).get(t.lower(), {})
                raw_desc = table_info.get("description", f"Dati relativi a {t}")
                desc = raw_desc if isinstance(raw_desc, str) else raw_desc.get("it", f"Dati relativi a {t}")
                table_schema_objs.append(SQLTableSchema(table_name=t, context_str=desc))
            
            obj_index = ObjectIndex.from_objects(
                table_schema_objs,
                table_node_mapping,
                VectorStoreIndex,
            )

            # 2. Global Agent System Prompt
            # 1. CORE ARCHITECTURE: DDL & SCHEMA AWARENESS
            # We build our knowledge base by extracting DDLs and data samples from the db_intel configuration.
            ddl_blocks = []
            sample_blocks = []
            for t_name, t_info in self.db_intel.get("tables", {}).items():
                ddl_blocks.append(t_info["ddl"])
                if t_info.get("sample_values"):
                    samples = ", ".join([f"{k}: {v}" for k, v in t_info["sample_values"].items()])
                    sample_blocks.append(f"Table {t_name} samples -> {samples}")

            # Consolidate DDL and samples
            schema_ddl_str = "\n".join(ddl_blocks)
            samples_hint_str = "\n".join(sample_blocks)
            
            # --- SYSTEM PROMPT ---
            self.context_to_inject = (
                "Sei l'Assistente AI Senior del Museo Bailo. Rispondi alle domande degli utenti interrogando il database.\n\n"
                "### SALUTI E CONVERSAZIONE:\n"
                "Se l'utente invia un saluto, un ringraziamento o una frase conversazionale generica, "
                "rispondi direttamente con cortesia senza chiamare alcun tool.\n\n"
                "### REGOLE DI RISPOSTA:\n"
                "1. PRIORITÀ TOOL: Usa sempre 'get_artist_info' e 'get_artwork_info' passando il NOME o il TITOLO come stringa.\n"
                "2. NO ID ALLUCINATI: Non inventare mai ID numerici. Se non conosci l'ID, usa i tool che accettano nomi.\n"
                "3. RISPOSTA COMPLETA: Quando trovi un artista o un'opera, fornisci subito biografia/descrizione e lista opere/tecnica.\n"
                "4. TONO: Formale, colto, ma accessibile.\n"
                "5. LINGUA: Rispondi nella lingua dell'utente.\n\n"
                "### KNOWLEDGE SOURCE: DATABASE SCHEMA (DDL)\n"
                f"{schema_ddl_str}\n\n"
                "### PROTOCOLO TECNICO:\n"
                "- Quando un tool restituisce testi lunghi (biografie, descrizioni di opere), riportali INTEGRALMENTE senza tagli o riassunti.\n"
                "- Non menzionare mai SQL, tabelle, ID o dettagli tecnici interni all'utente.\n"
            )

            # --- TEXT-TO-SQL PROMPT (The Archive Access) ---
            TEXT_TO_SQL_PROMPT_STR = (
                "Sei un esperto Senior PostgreSQL per il Museo Bailo. Genera query sintatticamente perfette.\n\n"
                "REGOLE CRITICHE:\n"
                "1. NOMI TABELLE: NON usare mai prefissi di schema. Usa nomi semplici (es. 'artistwork', non 'guide.artistwork').\n"
                "2. Restituisci esclusivamente SQL (SELECT).\n"
                "3. siteid: Applica il filtro 'siteid = 1' SOLO alle tabelle che mostrano la colonna 'siteid' nel DDL sottostante.\n"
                "4. TECNICA/MATERIALE: Filtra SEMPRE per tecnica usando un JOIN con la tabella 'technique' su 'techniquedescription'. "
                "NON cercare mai un materiale o una tecnica in 'artistworkdescription' o 'artistworktitle' — quei campi contengono testo narrativo che può essere fuorviante. "
                "Esempio CORRETTO: JOIN technique t ON aw.techniqueid = t.techniqueid WHERE t.techniquedescription ILIKE '%%bronzo%%'. "
                "Esempio SBAGLIATO: WHERE aw.artistworkdescription ILIKE '%%bronzo%%'.\n"
                "5. RICERCA APERTA (tema, nome, titolo): Usa ILIKE su 'artistworktitle', 'artistworkdescription', 'artistname', 'biography' solo per ricerche per tema o parola chiave generica (NON per filtrare materiali).\n"
                "STRUTTURA REALE (DDL):\n"
                f"{schema_ddl_str}\n\n"
                "CAMPIONI DATI:\n"
                f"{samples_hint_str}\n\n"
                "Domanda: {query_str}\n"
                "SQLQuery: "
            )

            # Custom response synthesis prompt (for the Query Engine internally)
            RESPONSE_SYNTHESIS_PROMPT_STR = (
                "1. SE TROVI PIÙ RIGHE: \n"
                "   - Se i titoli delle opere sono diversi, elenca i titoli e chiedi quale approfondire.\n"
                "   - Se il titolo è lo stesso o si tratta di LISTE, ELENCA semplicemente tutte le informazioni trovate in modo discorsivo o puntato.\n"
                "2. Se hai una descrizione (biografia/opera), riportala integralmente senza tagli.\n"
                "3. DIVIETO DI SCUSE: Restituisci solo i dati finali.\n\n"
                "Domanda: {query_str}\n"
                "Dati dal DB: {context_str}\n"
                "Risposta: "
            )

            self.sem_paradigm = sem_paradigm
            
            self.sql_engine = NLSQLTableQueryEngine(
                self.sql_database,
                tables=tables_to_reflect,
                llm=self.llm,
                sql_limit=500,
                synthesize_response=False,
                context_str="\n".join([f"Info on tables: {schema_ddl_str}", f"Info on samples: {samples_hint_str}"]),
                text_to_sql_prompt=PromptTemplate(TEXT_TO_SQL_PROMPT_STR)
            )
            
            # Sophisticated wrapper to handle raw SQL results and prevent summarization
            _sql_engine = self.sql_engine
            import html, re, ast
            def sql_query_tool(query: str) -> str:
                """Esegue query sul database del museo. Restituisce il testo integrale trovato."""
                try:
                    # 1. ARCHITECTURAL GUARDRAILS
                    # Ensure the generated SQL is safe and stays within authorized tables
                    allowed = list(self.db_intel.get("tables", {}).keys())
                    SQLGuardrails.validate_sql(query, allowed)

                    # Retrieve the site_id for the CURRENT execution context
                    current_site_id = ctx_site_id.get() or getattr(self, "_last_site_id", None)
                    if current_site_id:
                        query_up = query.upper()
                        from sqlalchemy import inspect
                        inspector = inspect(self.sql_database.engine)
                        # Extract all tables mentioned in the query (handles schema.table or just table)
                        matches = re.findall(r"(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)", query_up)
                        for full_table in matches:
                            parts = full_table.split(".")
                            table_name = parts[-1].lower()
                            schema_name = parts[0].lower() if len(parts) > 1 else "guide"
                            try:
                                cols_info = []
                                try:
                                    cols_info = inspector.get_columns(table_name, schema=schema_name)
                                except:
                                    try:
                                        cols_info = inspector.get_columns(table_name)
                                    except:
                                        pass
                                
                                cols = [c['name'].upper() for c in cols_info]
                                if not cols:
                                    for sch in inspector.get_schema_names():
                                        try:
                                            cols_info = inspector.get_columns(table_name, schema=sch)
                                            if cols_info: 
                                                cols = [c['name'].upper() for c in cols_info]
                                                schema_name = sch
                                                break
                                        except: continue
                                
                                if "SITEID" in cols and "SITEID" not in query_up:
                                    return (
                                        f"ERRORE DI SICUREZZA: La tabella '{table_name}' possiede la colonna 'siteid' ma il filtro manca nella query SQL. "
                                        f"DEVI aggiungere 'siteid = {current_site_id}' nella clausola WHERE (o nel JOIN)."
                                    )
                            except Exception:
                                pass

                    # 3. EXECUTION
                    result = _sql_engine.query(query)
                except Exception as e:
                    # SELF-CORRECTION LOOP:
                    # Instead of crashing, return the error to the LLM so it can fix the query
                    err_msg = str(e)
                    print(f"[SQL EXEC ERROR] {err_msg}")
                    return (
                        f"ERRORE SQL: {err_msg}\n"
                        "ISTRUZIONE PER L'AGENTE: La tua query SQL ha generato un errore. "
                        "Analizza l'errore sopra e genera una NUOVA query SQL corretta. "
                        "NON SCUSARTI, NON MENZIONARE L'ERRORE ALL'UTENTE. "
                        "Esegui solo la correzione in modo invisibile."
                    )
                raw = str(result)
                
                # Try to parse the raw string [('text',), ...] to extract pure text
                max_field_len = 0
                # Attempt robust parsing of SQL results (tuples/lists)
                rows = []
                try:
                    # Clean up common SQL string artifacts before eval
                    raw_eval = raw.replace('datetime.date', 'str').replace('Decimal', 'float')
                    parsed = ast.literal_eval(raw_eval)
                    if isinstance(parsed, list):
                        for row in parsed:
                            if isinstance(row, (list, tuple)):
                                # Extract long fields for bypass detection
                                for col in row:
                                    if isinstance(col, str):
                                        max_field_len = max(max_field_len, len(col))
                                row_str = " - ".join([str(c) for c in row if c is not None and str(c).strip() != ""])
                                if row_str and row_str not in rows: 
                                    rows.append(row_str)
                            else:
                                rows.append(str(row))
                        raw = "\n\n".join(rows)
                except (ValueError, SyntaxError, Exception) as e:
                    # FALLBACK: If literal_eval fails, use regex to extract text
                    print(f"[SQL PARSING WARN] {str(e)} - Falling back to regex.")
                    # Keep alphanumeric, common punctuation, and spaces
                    # Remove list/tuple brackets and quotes
                    cleaned = re.sub(r"[\[\]\(\)\"']", " ", raw)
                    # Normalize whitespace
                    cleaned = re.sub(r"\s+", " ", cleaned).strip()
                    # Remove artifacts like 'datetime.date' or 'Decimal' that might remain
                    cleaned = re.sub(r"(datetime\.date|Decimal)", "", cleaned)
                    rows.append(cleaned)
                    raw = "\n\n".join(rows)
                
                # Global HTML/Tag cleaning
                raw = html.unescape(raw)
                raw = re.sub(r'<(p|br|div)[^>]*>', '\n', raw, flags=re.IGNORECASE)
                raw = re.sub(r'<[^>]+>', ' ', raw)
                raw = re.sub(r' +', ' ', raw)
                raw = re.sub(r'\n\s*\n', '\n\n', raw)
                raw = raw.strip()
                
                if not raw or raw == "[]":
                    return "Nessun dato trovato nel database."
                
                return f"Risultato:\n{raw}"
            
            # --- ATOMIC TOOLS (Based on MuseumBroker) ---
            def search_artworks_tool(title: Optional[str] = None, artist: Optional[str] = None, 
                                     category: Optional[str] = None, room: Optional[str] = None,
                                     technique: Optional[str] = None, general_query: Optional[str] = None) -> str:
                """Trova opere nel catalogo (tabella 'artistwork'). 
                Parametri facoltativi: title, artist, category, room, technique. 
                Usa questo SOLO per trovare l'ID dell'opera o per elenchi. 
                Se l'utente vuole INFO su un'opera specifica, devi chiamare ANCHE 'get_artwork_details'."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1))
                results = self.broker.list_opere(site_id, title, artist, category, room, technique, general_query)
                if not results: return "Nessuna opera trovata."
                return json.dumps(results, indent=2)

            def get_artwork_details_tool(artwork_id: int) -> str:
                """Recupera l'intera riga dei dati tecnici e la descrizione (campo 'artistworktargetdescription') di un'opera dal suo artistworkid."""
                try:
                    lang = ctx_language_id.get() or "it"
                    target = ctx_audience_target.get() or "STD"
                    raw_site = ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1
                    current_site_id = int(raw_site)
                    
                    result = self.broker.get_opera_details(current_site_id, artwork_id, lang, target)
                    if not result: 
                        return "Dettagli non disponibili per questa opera."
                    
                    # Remove internal fields before returning to agent
                    result.pop("_INTERNAL_NOTICE_", None)
                    
                    # Update session focus
                    session_id = getattr(self, "_current_session_id", "default")
                    focus = self.session_focus.get(session_id, {})
                    focus.update({"artwork_id": artwork_id, "artwork_title": result.get("artistworktitle")})
                    self.session_focus[session_id] = focus
                    
                    return json.dumps(result, ensure_ascii=False, indent=2)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    return f"Errore nel recupero dettagli opera: {e}"

            def search_artists_tool(name: Optional[str] = None, category: Optional[str] = None) -> str:
                """Trova artisti (tabella 'artist'). Filtri: name, category.
                ATTENZIONE: questo tool restituisce solo l'ID e il nome. 
                Per rispondere all'utente su un artista specifico, devi chiamare ANCHE 'get_artist_details' con l'artistid ottenuto.
                Non rispondere all'utente senza aver prima chiamato get_artist_details."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1)
                lang = ctx_language_id.get() or "it"
                results = self.broker.list_artisti(site_id, name, category, lang)
                if not results: return "Nessun artista trovato."
                return json.dumps(results, indent=2)

            def get_artist_details_tool(artist_id: int) -> str:
                """Recupera biografia COMPLETA e dettagli tramite artistid.
                OBBLIGATORIO: chiamalo SEMPRE dopo search_artists se l'utente chiede info su un artista.
                Non fermarti a search_artists: senza get_artist_details la risposta è parziale e sbagliata."""
                lang = ctx_language_id.get()
                result = self.broker.get_artista_details(artist_id, lang)
                if not result:
                    return "Artista non trovato nel database."
                # Enrich with artworks list
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1)
                artworks = self.broker.list_opere(site_id, artist_name=result.get("artistname"))
                if artworks:
                    result["opere"] = [
                        {"titolo": a.get("artistworktitle"), "tecnica": a.get("techniquedescription"), "sala": a.get("roomname")}
                        for a in artworks
                    ]
                
                # Update session focus
                session_id = getattr(self, "_current_session_id", "default")
                focus = self.session_focus.get(session_id, {})
                focus.update({"artist_id": artist_id, "artist_name": result.get("artistname")})
                self.session_focus[session_id] = focus
                
                return json.dumps(result, ensure_ascii=False, indent=2)

            def get_artist_info_tool(name: str) -> str:
                """Recupera biografia e opere di un artista cercandolo per NOME.
                Usa questo tool se conosci il nome dell'artista (es. 'Cacciapuoti' o 'Guido Cacciapuoti')."""
                try:
                    # Robust fallback for site_id logic
                    raw_id = ctx_site_id.get() or getattr(self, "_last_site_id", 1)
                    site_id = int(raw_id) if raw_id is not None else 1
                    lang = ctx_language_id.get() or "it"
                    
                    matches = self.broker.list_artisti(site_id, name=name, language_id=lang)
                    if not matches: return f"Nessun artista trovato con il nome '{name}'."
                    
                    if len(matches) > 1 and name.lower() not in [m["artistname"].lower() for m in matches]:
                        return "Ho trovato più artisti con nomi simili: " + ", ".join([m["artistname"] for m in matches])
                    
                    return get_artist_details_tool(matches[0]["artistid"])
                except Exception as te:
                    print(f"[ERROR] get_artist_info: {te}")
                    return "Si è verificato un errore nel recupero delle informazioni."

            def get_artwork_info_tool(title: str) -> str:
                """Recupera i dettagli tecnici e la descrizione di un'opera cercandola per TITOLO.
                Usa questo tool se conosci il titolo dell'opera (es. 'Gallo e gallina')."""
                try:
                    site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1)
                    # 1. Search for IDs
                    matches = self.broker.list_opere(site_id, title=title)
                    if not matches: return f"Nessun'opera trovata con il titolo '{title}'."
                    
                    if len(matches) > 3:
                         return "Ho trovato molte opere con titoli simili. Potresti essere più specifico? Ecco alcune: " + ", ".join([m["artistworktitle"] for m in matches[:5]])
                    
                    # Take the best match
                    artwork_id = matches[0]["artistworkid"]
                    return get_artwork_details_tool(artwork_id)
                except Exception as te:
                    print(f"[ERROR] get_artwork_info: {te}")
                    return "Si è verificato un errore nel recupero delle informazioni sull'opera."

            def list_locations_tool() -> str:
                """Elenca tutte le sale ed edifici del museo dove sono presenti opere."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1)
                results = self.broker.list_locations(site_id)
                if not results: return "Nessuna sala trovata."
                return json.dumps(results, indent=2)

            def get_location_details_tool(location_id: int) -> str:
                """Recupera la descrizione e i dati di una sala (tabella 'room' / 'location') tramite locationid."""
                lang = ctx_language_id.get()
                result = self.broker.get_location_details(location_id, lang)
                if not result: return "Dettagli non disponibili per questa sala."
                return json.dumps(result, indent=2)

            def get_pathway_info_tool(pathway_name: Optional[str] = None, pathway_id: Optional[int] = None) -> str:
                """Recupera la descrizione e la lista delle opere di un percorso tematico.
                - pathway_name: il nome del percorso (es. 'MODA', 'ANIMALI')
                - pathway_id: l'ID numerico del percorso (se noto)
                """
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1))
                lang = ctx_language_id.get()
                
                pid = pathway_id
                if not pid and pathway_name:
                    # Cerca l'ID dal nome
                    pathways = self.broker.list_pathways(site_id)
                    for p in pathways:
                        if pathway_name.upper() in p["pathwayname"].upper():
                            pid = p["pathwayid"]
                            break
                
                if not pid:
                    return f"Non ho trovato il percorso '{pathway_name or pathway_id}'."
                
                # Prendi dettagli
                details = self.broker.get_pathway_details(pid, lang)
                # Prendi opere
                artworks = self.broker.get_percorso_opere(site_id, details.get("pathwayname", pathway_name))
                
                result = {
                    "pathway_name": details.get("pathwayname"),
                    "description": details.get("description"),
                    "artworks": artworks
                }
                
                return json.dumps(result, ensure_ascii=False, indent=2)

            def list_pathways_tool() -> str:
                """Elenca tutti i percorsi tematici disponibili nel museo."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1)
                results = self.broker.list_pathways(site_id)
                if not results: return "Nessun percorso trovato."
                return json.dumps(results, indent=2)

            def list_categories_tool() -> str:
                """Elenca le categorie disponibili (es. Pittura, Scultura). 
                ATTENZIONE: Se l'utente chiede una LISTA di opere ('mostrami i dipinti'), NON usare questo strumento, usa search_artworks(category='PITTORI'). 
                Usa questo solo se l'utente chiede esplicitamente 'Quali categorie ci sono?'."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1)
                results = self.broker.list_categories(site_id)
                if not results: return "Nessuna categoria trovata."
                return ", ".join(results)

            def list_techniques_tool() -> str:
                """Elenca le tecniche e i materiali delle opere presenti (es. Olio su tela, Marmo)."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1)
                results = self.broker.list_techniques(site_id)
                if not results: return "Nessuna tecnica trovata."
                return ", ".join(results)

            def get_museum_info_tool() -> str:
                """Recupera la storia, l'architettura e i contatti generali del museo."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1) or 1)
                result = self.broker.get_museum_info(site_id) or {}
                # Force fallback if fields are empty, None or missing
                if not result.get("history") or len(str(result.get("history"))) < 10:
                    result["history"] = "Il Museo Luigi Bailo è la sede storica della galleria d'arte moderna di Treviso. Fondato nel 1879 dall'Abate Luigi Bailo, è stato riaperto nel 2015 con un restyling che fonde il chiostro antico con una galleria moderna in vetro e cemento."
                if not result.get("architecture"):
                    result["architecture"] = "L'architettura attuale è un dialogo tra l'ex convento rinascimentale e la nuova facciata minimalista, che funge da 'lanterna' urbana."
                return json.dumps(result, indent=2)

            def list_related_artworks_tool(room_id: int) -> str:
                """Elenca altre opere presenti nella stessa sala (cross-selling/approfondimento)."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1))
                results = self.broker.list_artworks_in_room(site_id, room_id)
                if not results: return "Nessuna opera correlata trovata."
                return json.dumps(results, indent=2)

            def search_by_inventory_tool(inventory_number: str) -> str:
                """Trova un'opera specifica partendo dal suo numero di inventario (es. MCA 123)."""
                site_id = int(ctx_site_id.get() or getattr(self, "_last_site_id", 1))
                results = self.broker.search_by_inventory(site_id, inventory_number)
                if not results: return f"Nessun'opera trovata con inventario {inventory_number}."
                return json.dumps(results, indent=2)

            self.query_tools.extend([
                FunctionTool.from_defaults(fn=get_artist_info_tool, name="get_artist_info"),
                FunctionTool.from_defaults(fn=get_artwork_info_tool, name="get_artwork_info"),
                FunctionTool.from_defaults(fn=search_artworks_tool, name="search_artworks"),
                FunctionTool.from_defaults(fn=get_artwork_details_tool, name="get_artwork_details"),
                FunctionTool.from_defaults(fn=search_artists_tool, name="search_artists"),
                FunctionTool.from_defaults(fn=get_artist_details_tool, name="get_artist_details"),
                FunctionTool.from_defaults(fn=list_locations_tool, name="list_locations"),
                FunctionTool.from_defaults(fn=get_location_details_tool, name="get_location_details"),
                FunctionTool.from_defaults(fn=get_pathway_info_tool, name="get_pathway_info"),
                FunctionTool.from_defaults(fn=list_pathways_tool, name="list_pathways"),
                FunctionTool.from_defaults(fn=list_categories_tool, name="list_categories"),
                FunctionTool.from_defaults(fn=list_techniques_tool, name="list_techniques"),
                FunctionTool.from_defaults(fn=get_museum_info_tool, name="get_museum_info"),
                FunctionTool.from_defaults(fn=list_related_artworks_tool, name="list_related_artworks"),
                FunctionTool.from_defaults(fn=search_by_inventory_tool, name="search_by_inventory")
            ])

            sql_tool = FunctionTool.from_defaults(
                fn=sql_query_tool,
                name="knowledge_archive",
                description=(
                    "MOTORE SQL POSTGRESQL. Utilizza questo per aggregazioni (COUNT, SUM), query multi-tabella complesse "
                    "o quando i parametri dei tool atomici non sono sufficienti per coprire il DDL fornito."
                )
            )
            self.query_tools.append(sql_tool)

        # 5. RAG Engine
        if doc_store_path and os.path.exists(doc_store_path):
            from llama_index.core import StorageContext, load_index_from_storage
            try:
                storage_context = StorageContext.from_defaults(persist_dir=doc_store_path)
                vector_index = load_index_from_storage(storage_context, embed_model=self.embed_model)
                rag_engine = vector_index.as_query_engine(llm=self.llm, embed_model=self.embed_model)
                self.query_tools.append(QueryEngineTool.from_defaults(
                    query_engine=rag_engine,
                    name="document_archive",
                    description="Usa questo strumento per approfondimenti estratti da documenti PDF o articoli bibliografici."
                ))
            except Exception: pass

        # 6. Create Agent (FunctionAgent uses native function calling — no ReAct text traces)
        try:
            print(f"--- Creating Agent (Tools count: {len(self.query_tools)}) ---")
            
            self.agent = FunctionAgent(
                tools=self.query_tools, 
                llm=self.llm, 
                system_prompt=self.context_to_inject,
                verbose=True
            )
            print("--- Agent Created successfully ---")
            
            # 7. Initialize session-specific SQL bypass and state
            self._sql_bypass: Dict[str, Optional[str]] = {}
            self._last_site_id: int = 1
            
        except Exception as e:
            print(f"[ERROR] Agent Creation failed: {e}")
            import traceback
            traceback.print_exc()
            raise e

    def _sanitize_response(self, answer: str, technical_only: bool = False) -> str:
        """Remove leaked technical artifacts from the response.
        
        With FunctionAgent, tool calls are structured objects — the response text
        is already the user-facing answer. This method only needs to clean up
        data-level leaks (siteid, SQL errors, internal IDs) not agent-level ones.
        """
        # Remove internal tokens
        answer = answer.replace('[[DIRECT_DISPLAY]]', '')
        
        # Remove siteid references that tools might have leaked into their output
        answer = re.sub(r'\[siteid=\d+\]', '', answer)
        answer = re.sub(r'\(FILTRO OBBLIGATORIO[^)]*\)', '', answer)
        answer = re.sub(r'\bsiteid\s*=\s*\d+', '', answer, flags=re.IGNORECASE)
        
        # Remove code fences with SQL (from tool error messages fed back to agent)
        answer = re.sub(r'```sql\s*.*?```', '', answer, flags=re.DOTALL)
        
        if technical_only:
            return answer.strip()

        # Replace raw DB exceptions with a user-friendly message
        if "sqlalchemy.exc" in answer or "psycopg2" in answer:
            return "Mi dispiace, ho riscontrato un problema tecnico nell'accesso ai dati. Posso provare a cercare in un altro modo?"
        
        # Remove internal IDs and technical field names
        answer = re.sub(r'\b(artistid|artistworkid|siteid|roomid|locationid)[:\s=]+\d+\b', '', answer, flags=re.IGNORECASE)
        answer = re.sub(r'\b(inventorynumber|imageref|artist_alias)[:\s=]+', '', answer, flags=re.IGNORECASE)
        
        # Clean up excessive whitespace
        answer = re.sub(r'\n{3,}', '\n\n', answer)
        return answer.strip()

    async def query(self, user_query: str, session_id: str, site_id: str = None, target: str = None):
        start_time = time.time()
        if not self.query_tools:
            return {"answer": "Nessuna fonte dati configurata.", "source_type": "none"}
            
        print(f"[PROCESS] Session: {session_id} | Query: {user_query}")
        self._current_session_id = session_id
        
        try:
            if session_id not in self.session_memory:
                self.session_memory[session_id] = []
            history = self.session_memory[session_id]
            
            # Simple language detection
            detected_lang = "it"
            q_low = user_query.lower()
            if any(w in q_low for w in ["english", "what is", "tell me", "where is", "who was", "describe", "show me"]): detected_lang = "en"
            elif any(w in q_low for w in ["français", "qu'est-ce", "raconte-moi", "où est", "décris"]): detected_lang = "fr"
            elif any(w in q_low for w in ["español", "qué es", "cuéntame", "donde está", "describe"]): detected_lang = "es"

            # Set context for tools
            token_site = ctx_site_id.set(site_id)
            token_target = ctx_audience_target.set(target or "STD")
            token_lang = ctx_language_id.set(detected_lang) 
            
            # Ensure site_id is an integer for the fallback
            try:
                self._last_site_id = int(site_id) if site_id is not None else 1
            except:
                self._last_site_id = 1
            self._last_target = target
            
            # Clean query
            enriched_query = user_query
            
            current_context = []
            if site_id or target:
                parts = []
                if site_id: parts.append(f"siteid={site_id}")
                if target: parts.append(f"target_pubblico={target}")
                hint = ChatMessage(
                    role=MessageRole.SYSTEM, 
                    content=f"CONTESTO ESECUTIVO: {', '.join(parts)}. Usa gli strumenti atomici (search_artworks, get_artwork_details, etc.) per rispondere. Gli strumenti filtrano automaticamente per siteid e target di pubblico."
                )
                current_context.append(hint)

            # 4. Initialize local memory for this session
            from llama_index.core.memory import ChatMemoryBuffer
            memory = ChatMemoryBuffer.from_defaults(chat_history=history, token_limit=4000)
            
            # 4b. Inject Session Focus into temporary context
            focus = self.session_focus.get(session_id, {})
            focus_str = ""
            if focus.get("artist_name"): focus_str += f"- Artist Focus: {focus['artist_name']} (ID: {focus['artist_id']})\n"
            if focus.get("artwork_title"): focus_str += f"- Artwork Focus: {focus['artwork_title']} (ID: {focus['artwork_id']})\n"
            
            if focus_str:
                current_context.append(ChatMessage(
                    role=MessageRole.SYSTEM,
                    content=f"FOCUS CORRENTE DELLA CONVERSAZIONE:\n{focus_str}\nUsa queste informazioni se l'utente fa domande di follow-up (es. 'dove è nato?', 'mostrami le sue opere')."
                ))

            # 5. Get Agent Response
            agent_start = time.time()
            full_chat_history = history + current_context
            handler = self.agent.run(user_msg=user_query, chat_history=full_chat_history)
            agent_output = await handler
            
            # Extract the clean user-facing text from AgentOutput
            # FunctionAgent returns structured output — response.content is the final text
            answer = ""
            if hasattr(agent_output, 'response'):
                answer = agent_output.response.content or ""
            if not answer:
                answer = str(agent_output)
            
            # Update memory
            memory.put(ChatMessage(role=MessageRole.USER, content=user_query))
            memory.put(ChatMessage(role=MessageRole.ASSISTANT, content=answer))

            # Clean up any data-level leaks (siteid, SQL errors, internal IDs)
            answer = self._sanitize_response(answer)

            # Save the full updated history (including tool calls/results) from the memory buffer
            # Optimized: Keep only last 10 messages to stay within TPM limits
            self.session_memory[session_id] = memory.get_all()[-10:]

            print(f"[LATENCY] Agent loop: {time.time() - agent_start:.2f}s")
            print(f"[LATENCY] Total query: {time.time() - start_time:.2f}s")
            # Always reset context
            self._current_session_id = None
            # Always reset context
            ctx_site_id.reset(token_site)
            ctx_audience_target.reset(token_target)
            ctx_language_id.reset(token_lang)
            return {"answer": answer, "source_type": "hybrid"}
        except Exception as e:
            err_msg = str(e)
            print(f"[CRITICAL ERROR] {err_msg}")
            traceback.print_exc()
            
            if "429" in err_msg or "Resource exhausted" in err_msg or "quota" in err_msg.lower():
                 friendly_answer = "Siamo spiacenti, il sistema è temporaneamente sovraccarico. Per favore, attendi qualche secondo e riprova la tua domanda."
            else:
                 friendly_answer = "Mi scuso, ho riscontrato un problema imprevisto nel generare la risposta. Puoi provare a riformulare leggermente la domanda?"
            
            return {"answer": friendly_answer, "source_type": "error"}

    async def astream_query(self, user_query: str, session_id: str, site_id: str = None, target: str = None):
        """Asynchronous streaming version of the query method."""
        if not self.query_tools:
            yield "Nessuna fonte dati configurata."
            return

        print(f"[PROCESS] Stream Session: {session_id} | Query: {user_query}")
        
        try:
            if session_id not in self.session_memory:
                self.session_memory[session_id] = []
            history = self.session_memory[session_id]
            
            # Simple language detection
            detected_lang = "it"
            q_low = user_query.lower()
            if any(w in q_low for w in ["english", "what is", "tell me", "where is", "who was", "describe", "show me"]): detected_lang = "en"
            elif any(w in q_low for w in ["français", "qu'est-ce", "raconte-moi", "où est", "décris"]): detected_lang = "fr"
            elif any(w in q_low for w in ["español", "qué es", "cuéntame", "donde está", "describe"]): detected_lang = "es"

            # Set context
            token_site = ctx_site_id.set(site_id)
            token_target = ctx_audience_target.set(target or "STD")
            token_lang = ctx_language_id.set(detected_lang)
            
            self._last_site_id = site_id
            self._last_target = target
            
            current_context = []
            if site_id or target:
                parts = []
                if site_id: parts.append(f"siteid={site_id}")
                if target: parts.append(f"target_pubblico={target}")
                hint = ChatMessage(
                    role=MessageRole.SYSTEM, 
                    content=f"CONTESTO ESECUTIVO: {', '.join(parts)}. Usa gli strumenti atomici filtra automaticamente per siteid e target."
                )
                current_context.append(hint)

            # 4. Initialize local memory
            from llama_index.core.memory import ChatMemoryBuffer
            memory = ChatMemoryBuffer.from_defaults(chat_history=history, token_limit=4000)
            
            # 5. Get Stream Response via Workflow events
            agent_start = time.time()
            full_chat_history = history + current_context
            
            # Start the run
            handler = self.agent.run(user_msg=user_query, chat_history=full_chat_history)
            
            full_response = ""
            async for event in handler.stream_events():
                delta = getattr(event, "delta", None)
                if delta:
                    full_response += delta
                    yield delta
            
            # Ensure the workflow actually finished and get final output
            output = await handler 
            
            # FALLBACK: If nothing was streamed (full_response is empty), 
            # try to get the content from the final output
            if not full_response:
                if hasattr(output, "response") and hasattr(output.response, "content"):
                    full_response = output.response.content
                elif hasattr(output, "content"):
                    full_response = output.content
                else:
                    full_response = str(output)
                
                full_response = self._sanitize_response(full_response)
                yield full_response

            # Update memory for stream
            memory.put(ChatMessage(role=MessageRole.USER, content=user_query))
            memory.put(ChatMessage(role=MessageRole.ASSISTANT, content=full_response))
            self.session_memory[session_id] = memory.get_all()[-10:]
            ctx_site_id.reset(token_site)
            ctx_audience_target.reset(token_target)
            ctx_language_id.reset(token_lang)

        except Exception as e:
            err_msg = str(e)
            print(f"[STREAM ERROR] {err_msg}")
            traceback.print_exc()
            
            if "429" in err_msg or "Resource exhausted" in err_msg:
                yield "Siamo spiacenti, il sistema è temporaneamente sovraccarico. Per favore, attendi qualche secondo e riprova."
            else:
                yield "Mi scuso, si è verificato un problema nel caricamento della risposta. Per favore, riprova tra un istante."
