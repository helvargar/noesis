from llama_index.core import VectorStoreIndex, SQLDatabase, Settings, PromptTemplate
from llama_index.core.query_engine import NLSQLTableQueryEngine, SQLTableRetrieverQueryEngine
from llama_index.core.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
from llama_index.core.tools import QueryEngineTool, ToolMetadata, FunctionTool
from llama_index.core.agent import FunctionAgent, ReActAgent, AgentStream
from app.core.factory import LLMFactory, EmbedModelFactory
from app.engine.guardrails import SQLGuardrails
import os
import json
import asyncio
from llama_index.core.llms import ChatMessage, MessageRole
from typing import List, Any, Dict
import time
import traceback
import re
import contextvars

# Global context for multi-site isolation within cached pipelines
ctx_site_id = contextvars.ContextVar("site_id", default=None)

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

        # 3. Buffer for large SQL results to prevent summarization
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
            # Format the dictionary into a readable guide for the LLM
            semantic_guide = ["MAPPATURA CONCETTI -> TABELLE DB:"]
            for table, info in sem_paradigm.get("tables", {}).items():
                concepts = ", ".join(info.get("concepts", []))
                desc = info.get("description", "")
                semantic_guide.append(f"  {concepts} -> tabella '{table}' ({desc})")

            context_to_inject = (
                "Sei la Guida Virtuale ufficiale del Museo Bailo. Sei un assistente multilingue: rispondi sempre nella LINGUA utilizzata dall'utente nell'ultima domanda.\n\n"
                "CONOSCENZA DEL MUSEO:\n"
                f"{chr(10).join(semantic_guide)}\n\n"
                "REGOLE DI COMPORTAMENTO:\n"
                "1. MULTILINGUA: Rispondi nella lingua dell'utente. Se i dati sono in italiano, traducili tu.\n"
                "2. NO PREFISSI: Inizia SEMPRE la risposta direttamente. È VIETATO scrivere 'Ecco i dettagli', 'Certamente', 'Bengali:', 'Guida:' o qualsiasi altra etichetta introduttiva.\n"
                "3. AMBIGUITÀ E DETTAGLIO: Se l'utente chiede di un'opera o artista specifico:\n"
                "   - DISAMBIGUAZIONE: Se la ricerca DB restituisce più ID DIVERSI, elenca i titoli PULITI (dalla tabella principale) e chiedi quale approfondire. È VIETATO elencare varianti di lingua come opzioni diverse.\n"
                "   - FILTRO LINGUA: Nelle query SQL su tabelle 'lang' o 'description', aggiungi il filtro sulla lingua dell'utente (es. languageid = 'it').\n"
                "   - SELEZIONE TARGET: Usa sempre 'audiencetargetid = STD' quando disponibile.\n"
                "   - COPIA-INCOLLA: Una volta identificata l'opera univoca, fai COPIA-INCOLLA INTEGRALE del testo senza riassumere.\n"
                "4. PROATTIVITÀ: Usa 'knowledge_archive' immediatamente per ogni domanda su fatti reali.\n"
                "5. DIVIETO ASSOLUTO TECNICISMI: È vietato menzionare database, SQL, strumenti, errori di query o scusarsi per fallimenti tecnici. Se una query fallisce, riprova silenziosamente.\n"
                "6. BYPASS: Se ricevi [[DIRECT_DISPLAY]], riporta il testo INTEGRALE senza alcuna modifica o riassunto.\n"
            )

            # Construct a rich DDL context dynamically
            ddl_blocks = []
            sample_blocks = []
            schema_prefix = self.db_intel.get("schema", "guide")
            for t_name, t_info in self.db_intel.get("tables", {}).items():
                ddl_blocks.append(t_info["ddl"])
                if t_info.get("sample_values"):
                    samples = ", ".join([f"{k}: {v}" for k, v in t_info["sample_values"].items()])
                    sample_blocks.append(f"Table {t_name} samples -> {samples}")

            TEXT_TO_SQL_PROMPT_STR = (
                "Sei un esperto Senior PostgreSQL per il Museo Bailo. Genera query sintatticamente perfette.\n\n"
                "REGOLE CRITICHE:\n"
                "1. NOMI TABELLE: NON usare mai prefissi di schema. Usa nomi semplici (es. 'artistwork', non 'guide.artistwork').\n"
                "2. Restituisci esclusivamente SQL (SELECT).\n"
                "3. siteid: Applica il filtro 'siteid = 1' SOLO alle tabelle che mostrano la colonna 'siteid' nel DDL sottostante.\n"
                "4. PULIZIA: Se i dati nel DB contengono tag HTML (es. <p>, <div>), ignorali e restituisci solo il testo pulito.\n"
                "5. RICERCA PARZIALE VS UNIVOCA: Per la DISAMBIGUAZIONE usa 'ILIKE %%term%%'. Una volta che hai identificato l'ID dell'opera (artistworkid), usa SEMPRE 'WHERE artistworkid = X' per recuperare la descrizione, invece di usare di nuovo il titolo. Questo evita di recuperare accidentalmente altre opere con nomi simili.\n"
                "6. MULTILINGUA: Se l'utente scrive in inglese o spagnolo, cerca prima nelle tabelle di localizzazione (es. 'artistworklang', 'artistdescription') filtrando per 'languageid' (es. 'en', 'es').\n\n"
                "STRUTTURA REALE (DDL):\n"
                "{schema_ddl}\n\n"
                "CAMPIONI DATI (FONDAMENTALI PER I FILTRI):\n"
                "{samples_hint}\n\n"
                "GOLDEN QUERIES (ESEMPI):\n"
                "Q: opere sala 9 -> SELECT aw.artistworktitle FROM artistwork aw JOIN room r ON aw.roomid = r.roomid WHERE r.roomname ILIKE '%%SALA 9%%' AND aw.siteid = 1;\n"
                "Q: chi è Martini -> SELECT artistname, artistdescription, biography FROM artist WHERE artistname ILIKE '%%Arturo Martini%%' AND siteid = 1;\n"
                "Q: che sculture ci sono -> SELECT aw.artistworktitle FROM artistwork aw JOIN artist a ON aw.artistid = a.artistid JOIN artistcategory ac ON a.artistcategoryid = ac.artistcategoryid WHERE (ac.artistcategorydescription ILIKE '%%SCULTORI%%' OR ac.artistcategorydescription ILIKE '%%SCULPTORS%%') AND aw.siteid = 1 AND a.siteid = 1;\n"
                "Q: mostrami i dipinti -> SELECT aw.artistworktitle FROM artistwork aw JOIN artist a ON aw.artistid = a.artistid JOIN artistcategory ac ON a.artistcategoryid = ac.artistcategoryid WHERE (ac.artistcategorydescription ILIKE '%%PITTORI%%' OR ac.artistcategorydescription ILIKE '%%PAINTERS%%') AND aw.siteid = 1 AND a.siteid = 1;\n"
                "Q: opere in bronzo -> SELECT aw.artistworktitle FROM artistwork aw JOIN technique t ON aw.techniqueid = t.techniqueid WHERE t.techniquedescription ILIKE '%%BRONZO%%' AND aw.siteid = 1;\n"
                "Q: indirizzo museo -> SELECT address, city FROM site WHERE siteid = 1;\n"
                "Q: opere percorso animali -> SELECT aw.artistworktitle FROM artistwork aw JOIN pathwayspot ps ON aw.artistworkid = ps.artistworkid JOIN pathway p ON ps.pathwayid = p.pathwayid WHERE p.pathwayname ILIKE '%%ANIMALI%%' AND aw.siteid = 1 ORDER BY ps.sortingsequence;\n"
                "Q: info sulla Pisana -> SELECT aw.artistworkid, aw.artistworktitle FROM artistwork aw WHERE aw.artistworktitle ILIKE '%%Pisana%%' AND aw.siteid = 1;\n\n"
                "Domanda: {query_str}\n"
                "SQLQuery: "
            ).replace("{schema_ddl}", "\n".join(ddl_blocks)).replace("{samples_hint}", "\n".join(sample_blocks))
            
            # Formattiamo il dizionario per il SQL Engine
            sql_context_lines = ["DIZIONARIO TABELLE E COLONNE:"]
            for table, info in sem_paradigm.get("tables", {}).items():
                sql_context_lines.append(f"Table '{table}': {info.get('description', '')}")
                for col, col_info in info.get("columns", {}).items():
                    labels = ", ".join(col_info.get("labels", []))
                    sql_context_lines.append(f"  Colonna '{col}': [{labels}]")
            
            sql_context_lines.append("\nPRESCRIZIONE QUERY: 1. Per cercare un'opera, inizia sempre con una query su 'artistwork' per vedere se ci sono nomi simili ed evita ambiguità. 2. Per il testo, usa 'artistworkaudiencetargetdesc' con 'audiencetargetid = STD' AND 'languageid = it' (o lingua utente). 3. Se trovi più ID diversi, elenca solo i titoli della tabella 'artistwork'.")

            # Custom response synthesis prompt
            RESPONSE_SYNTHESIS_PROMPT_STR = (
    "1. SE TROVI PIÙ RIGHE: \n"
    "   - Se i titoli delle opere sono diversi (ambiguità sull'oggetto), elenca i titoli e chiedi quale approfondire.\n"
    "   - Se il titolo è lo stesso o si tratta di LISTE (es. più sale, più opere di un autore, più dettagli), ELENCA semplicemente tutte le informazioni trovate in modo discorsivo o puntato.\n"
    "2. È VIETATO chiedere 'vuoi sapere quale sala?' se le hai già trovate tutte. Riportale subito.\n"
    "3. Se hai una descrizione (biografia/opera), riportala integralmente senza tagli.\n"
    "4. DIVIETO DI SCUSE: È proibito scusarsi per ritardi, errori di sistema o query fallite. Restituisci solo i dati finali.\n"
    "5. Inizia subito, niente prefissi.\n\n"
                "Domanda: {query_str}\n"
                "Dati dal DB: {context_str}\n"
                "Risposta: "
            )

            # Build tool description dynamically
            concept_map_desc = ". ".join([
                f"'{t}' per {', '.join(i.get('concepts', [])[:2])}"
                for t, i in sem_paradigm.get("tables", {}).items()
            ])

            self.sql_engine = NLSQLTableQueryEngine(
                self.sql_database,
                tables=tables_to_reflect,
                llm=self.llm,
                sql_limit=500,
                synthesize_response=False,
                context_str="\n".join(sql_context_lines),
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
                
                # AUTHORITATIVE BYPASS: Trigger if ANY field is long (biography/description)
                if max_field_len > 500:
                    self.last_sql_result = raw
                    # Provide a preview to the agent so it knows what was found
                    preview = raw[:150] + "..." if len(raw) > 150 else raw
                    return f"RISULTATO TROVATO (Anteprima: {preview}). Il sistema visualizzerà il testo integrale. [[DIRECT_DISPLAY]]"
                
                return f"Risultato:\n{raw}"
            
            sql_tool = FunctionTool.from_defaults(
                fn=sql_query_tool,
                name="knowledge_archive",
                description=(
                    f"Archivio certificato del museo. Richiede query SQL PostgreSQL per recuperare dati su: {concept_map_desc}. "
                    "Includi sempre 'siteid = 1' e usa 'ILIKE' per ricerche parziali."
                )
            )
            self.query_tools.append(sql_tool)

            # 4. General Chat Tool
            def general_chat(query: str) -> str:
                """Utile per saluti o chiacchiere che non richiedono dati."""
                return f"Ciao! Sono Gyp. Come posso aiutarti oggi?"
            
            self.query_tools.append(FunctionTool.from_defaults(fn=general_chat, name="greeting_tool"))

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

        # 6. Create Agent with Function Calling capabilities
        try:
            self.agent = FunctionAgent(
                tools=self.query_tools, 
                llm=self.llm, 
                system_prompt=context_to_inject,
                verbose=False
            )
            
        except Exception as e:
            print(f"[ERROR] Pipeline init failed: {e}")
            traceback.print_exc()

        except Exception as e:
            traceback.print_exc()
            return {"answer": f"Si è verificato un errore: {str(e)}", "source_type": "error"}

    @staticmethod
    def _sanitize_response(answer: str) -> str:
        """Remove any leaked technical details from the agent's response."""
        import re
        # Remove tool call blocks (```tool_name ... ``` and ```tool_args ... ```)
        answer = re.sub(r'```tool_name\s*.*?```', '', answer, flags=re.DOTALL)
        answer = re.sub(r'```tool_args\s*.*?```', '', answer, flags=re.DOTALL)
        # Remove any remaining code fences with SQL
        answer = re.sub(r'```sql\s*.*?```', '', answer, flags=re.DOTALL)
        # Remove siteid references
        answer = re.sub(r'\[siteid=\d+\]', '', answer)
        answer = re.sub(r'\(FILTRO OBBLIGATORIO[^)]*\)', '', answer)
        answer = re.sub(r'siteid\s*=\s*\d+', '', answer, flags=re.IGNORECASE)
        # Hide technical database errors and apologies from user
        technical_terms = ["Error:", "SQL", "column", "query", "tabella", "riprovo", "mi scuso", "errore", "precisare"]
        if any(term.lower() in answer.lower() for term in technical_terms):
            if "Nessun dato" not in answer and "Mi dispiace" not in answer:
                answer = "Mi dispiace, non sono riuscito a trovare le informazioni specifiche nel mio archivio in questo momento. Posso aiutarti con qualcos'altro?"
        
        # Aggressive removal of intermediate thoughts if llama-index leaked them
        answer = re.sub(r'Thought:.*?Action:', '', answer, flags=re.DOTALL)
        answer = re.sub(r'Observation:.*', '', answer, flags=re.DOTALL)
        
        # Remove [[DIRECT_DISPLAY]] token if it leaked
        answer = answer.replace('[[DIRECT_DISPLAY]]', '')
        # Remove "Thought:", "Action:", "Observation:" lines (ReAct internals)
        answer = re.sub(r'^(Thought|Action|Observation|Action Input):.*$', '', answer, flags=re.MULTILINE)
        # Clean up excessive whitespace
        answer = re.sub(r'\n{3,}', '\n\n', answer)
        return answer.strip()

    async def query(self, user_query: str, session_id: str, site_id: str = None, target: str = None):
        start_time = time.time()
        if not self.query_tools:
            return {"answer": "Nessuna fonte dati configurata.", "source_type": "none"}
            
        print(f"[PROCESS] Session: {session_id} | Query: {user_query}")
        
        try:
            if session_id not in self.session_memory:
                self.session_memory[session_id] = []
            history = self.session_memory[session_id]
            
            # Set context for tools
            token = ctx_site_id.set(site_id)
            self._last_site_id = site_id # Fallback for thread/context loss
            
            # Clean query
            enriched_query = user_query
            
            current_context = []
            if site_id or target:
                parts = []
                if site_id: parts.append(f"siteid={site_id}")
                if target: parts.append(f"codice_percorso={target}")
                hint = ChatMessage(
                    role=MessageRole.SYSTEM, 
                    content=f"CONTESTO ATTUALE: {', '.join(parts)}. Includi sempre siteid nelle query SQL per artistwork/pathway. Nota: la tabella 'room' non ha siteid, usala solo in JOIN con artistwork."
                )
                current_context.append(hint)

            # 5. Get Agent Response via Native Function Calling
            agent_start = time.time()
            output = await self.agent.run(user_msg=enriched_query, chat_history=current_context + history)
            
            # Extract content from AgentOutput
            if hasattr(output, "response") and hasattr(output.response, "content"):
                answer = output.response.content
            else:
                answer = str(output)
            print(f"[LATENCY] Agent loop: {time.time() - agent_start:.2f}s")

            # --- AUTHORITATIVE BYPASS STRATEGY ---
            if self.last_sql_result:
                print(f"[BYPASS] Triggering authoritative bypass")
                answer = self.last_sql_result
            
            # Reset buffer
            self.last_sql_result = None
            
            # SANITIZE: Remove any leaked technical details
            answer = self._sanitize_response(answer)
            
            # Save original query (not enriched) in history
            history.append(ChatMessage(role=MessageRole.USER, content=user_query))
            history.append(ChatMessage(role=MessageRole.ASSISTANT, content=answer))
            self.session_memory[session_id] = history[-10:]

            print(f"[LATENCY] Total query: {time.time() - start_time:.2f}s")
            # Always reset context
            ctx_site_id.reset(token)
            return {"answer": answer, "source_type": "hybrid"}
        except Exception as e:
            traceback.print_exc()
            return {"answer": f"Si è verificato un errore: {str(e)}", "source_type": "error"}

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
            
            # Set context for tools
            token = ctx_site_id.set(site_id)
            
            enriched_query = user_query
            current_context = []
            if site_id or target:
                parts = []
                if site_id: parts.append(f"siteid={site_id}")
                if target: parts.append(f"codice_percorso={target}")
                hint = ChatMessage(
                    role=MessageRole.SYSTEM, 
                    content=f"CONTESTO ATTUALE: {', '.join(parts)}. Includi sempre siteid nelle query SQL."
                )
                current_context.append(hint)

            # Start the agent workflow run
            handler = self.agent.run(user_msg=enriched_query, chat_history=current_context + history)
            
            full_answer = ""
            agent_start = time.time()
            async for event in handler.stream_events():
                if isinstance(event, AgentStream):
                    if event.delta:
                        full_answer += event.delta
                        # Check if we should immediately stop streaming and bypass
                        if "[[DIRECT_DISPLAY]]" in full_answer and self.last_sql_result:
                            # Break streaming loop to trigger bypass below
                            break
                        yield event.delta
            
            print(f"[LATENCY] Stream Agent loop: {time.time() - agent_start:.2f}s")

            # --- AUTHORITATIVE BYPASS STRATEGY ---
            if self.last_sql_result:
                print(f"[BYPASS] Triggering authoritative bypass during stream")
                # If we were already streaming, we might have sent "Risultato presente..."
                # We can't "take back" what's already sent, but we can send the rest.
                # However, for a clean bypass, we usually want to send ONLY the SQL result.
                # In streaming, we'll just send the last_sql_result as the final chunk if it wasn't already sent.
                # But to be safe, we'll yield the whole thing if it's a bypass.
                # Note: The client should handle clearing its buffer if it sees [[DIRECT_DISPLAY]]
                yield self.last_sql_result
                full_answer = self.last_sql_result
            
            # Reset buffer
            self.last_sql_result = None
            
            # SANITIZE (simplified for stream)
            full_answer = self._sanitize_response(full_answer)
            
            # Update history
            history.append(ChatMessage(role=MessageRole.USER, content=user_query))
            history.append(ChatMessage(role=MessageRole.ASSISTANT, content=full_answer))
            self.session_memory[session_id] = history[-10:]
            
            # Reset context
            ctx_site_id.reset(token)

        except Exception as e:
            traceback.print_exc()
            yield f"Errore durante lo streaming: {str(e)}"
