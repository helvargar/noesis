from llama_index.core import VectorStoreIndex, SQLDatabase, Settings
from llama_index.core.query_engine import NLSQLTableQueryEngine, SQLTableRetrieverQueryEngine
from llama_index.core.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
from llama_index.core.tools import QueryEngineTool, ToolMetadata
from llama_index.core.agent import ReActAgent
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

        # 3. Setup SQL Engine (if db provided)
        if sql_connection_str:
            print(f"--- Init SQL Database (Schema: {self.schema_name or 'public'}) ---")
            from sqlalchemy import create_engine, event
            engine = create_engine(sql_connection_str)
            
            # Diagnostic SQL Logging: Capture every query executed on this engine
            @event.listens_for(engine, "before_cursor_execute")
            def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                print(f"[SQL] Executing: {statement}")
                if parameters:
                    print(f"[SQL] Parameters: {parameters}")

            # Optimization: only reflect what's needed
            tables_to_reflect = None
            if allowed_tables and "*" not in allowed_tables:
                tables_to_reflect = allowed_tables
                print(f"--- Restricting reflection to: {tables_to_reflect} ---")

            self.sql_database = SQLDatabase(engine, schema=self.schema_name, include_tables=tables_to_reflect)
            

            # 1. Hygiene: Strict Whitelist of tables to prevent AI confusion
            # We only allow tables that are part of the core museum guide logic.
            WHITELIST = [
                "artistwork", "artistworklang", "artist", "artistcategory",
                "site", "room", "floor",
                "location", "locationdescription",
                "pathway", "pathwaydescription", "pathwayspot",
                "technique",
            ]
            
            all_tables = self.sql_database.get_usable_table_names()
            clean_tables = [t for t in all_tables if t.lower() in WHITELIST]
            
            print(f"--- Reflecting {len(clean_tables)} whitelist tables (ignored {len(all_tables) - len(clean_tables)} noise tables) ---")
            
            # Re-init SQLDatabase with clean list
            self.sql_database = SQLDatabase(engine, schema=self.schema_name, include_tables=clean_tables)

            # 2. Semantic Mapping: Give AI descriptions for clean tables
            # This is the single source of truth for table descriptions.
            table_context_dict = {
                # --- OPERE ---
                "artistwork": (
                    "Tabella principale OPERE (dati in italiano). "
                    "REGOLE: 1) Usa SEMPRE ILIKE con % per cercare i titoli (es. WHERE artistworktitle ILIKE '%venere%'). MAI usare =. "
                    "2) NON tradurre i titoli: cerca il testo originale dell'utente. "
                    "3) Includi sempre: artistworktitle, artistworkdescription, techniqueid, realizationyear e Nome Autore (JOIN con artist su artistid). "
                    "4) Per la posizione fisica, JOIN con room su roomid. "
                    "5) Per utenti non italiani, usa artistworklang per titoli e descrizioni nella loro lingua."
                ),
                "artistworklang": (
                    "Traduzioni multilingua delle opere. Contiene artistworktitle e artistworkdescription tradotti per lingua "
                    "(languageid: it, en, es, de). JOIN con artistwork su artistworkid. "
                    "Usa questa tabella per cercare opere quando l'utente parla in lingua straniera o per restituire info tradotte."
                ),
                "technique": (
                    "Decodifica delle tecniche artistiche (es. techniqueid=1 -> 'OLIO SU TELA', techniqueid=10 -> 'BRONZO'). "
                    "Multilingua: la colonna languageid indica la lingua della descrizione. "
                    "JOIN con artistwork.techniqueid per mostrare il nome della tecnica invece del solo ID."
                ),
                # --- ARTISTI ---
                "artist": (
                    "Anagrafica ARTISTI. Contiene: artistname, biography, birthplace, birthdate, deathplace, deathdate. "
                    "REGOLE: Usa SEMPRE ILIKE con % (es. WHERE artistname ILIKE '%martini%'). MAI usare =. "
                    "La colonna artistcategoryid si collega ad artistcategory per la categoria (scultore, pittore, ecc.)."
                ),
                "artistcategory": (
                    "Categorie degli artisti (es. SCULTORI, PITTORI, DIRETTORI). Multilingua tramite campo languageid. "
                    "JOIN con artist.artistcategoryid."
                ),
                # --- SPAZI FISICI ---
                "site": "Anagrafica dei musei (es. BAILO). Contiene indirizzo, storia, architettura, contatti.",
                "room": "Sale e gallerie del museo. La colonna 'roomname' contiene il nome (es. 'SALA 9'). JOIN con floor tramite floorid per sapere il piano.",
                "floor": "Piani dell'edificio (es. PIANO TERRA, PRIMO PIANO). JOIN con room.floorid.",
                "location": (
                    "Location fisiche del museo (es. SALA 6 PIANO TERRA, INGRESSO MUSEO). "
                    "Contiene locationname e si collega a room tramite roomid e a site tramite siteid. "
                    "Usata anche nei percorsi (pathwayspot.locationid)."
                ),
                "locationdescription": (
                    "Descrizioni multilingua delle location. Contiene locationname e locationdescription tradotti per lingua "
                    "(languageid: it, en, es, de). JOIN con location su locationid."
                ),
                # --- PERCORSI ---
                "pathway": "Percorsi tematici del museo (es. PERCORSO SCULTURA, PERCORSO ANIMALI). Contiene pathwayname e pathwaydescription.",
                "pathwaydescription": (
                    "Descrizioni multilingua dei percorsi. Contiene pathwayname e pathwaydescription tradotti "
                    "(languageid: it, en, es, de). JOIN con pathway su pathwayid."
                ),
                "pathwayspot": (
                    "TAPPE dei percorsi. Collega un percorso (pathwayid) alle sue tappe ordinate (sortingsequence). "
                    "Ogni tappa può essere un'opera (artistworkid) oppure una location (locationid). "
                    "Per ottenere le opere di un percorso: JOIN pathway -> pathwayspot -> artistwork. "
                    "Per ottenere le location di un percorso: JOIN pathway -> pathwayspot -> location."
                ),
            }

            table_node_mapping = SQLTableNodeMapping(self.sql_database)
            table_schema_objs = []
            for t in clean_tables:
                desc = table_context_dict.get(t, f"Database table named {t}")
                table_schema_objs.append(SQLTableSchema(table_name=t, context_str=desc))
            
            # 3. Build Index
            obj_index = ObjectIndex.from_objects(
                table_schema_objs,
                table_node_mapping,
                VectorStoreIndex,
            )

            # 4. Universal Context Scanner
            # The system automatically learns the vocabulary of the database at startup.
            semantic_context = []
            try:
                from sqlalchemy import text
                
                # Helper to truncate lists for prompt
                def truncate_list(items, limit=10):
                    display_items = items[:limit]
                    result = ", ".join([f"'{item}'" for item in display_items if item])
                    if len(items) > limit:
                        result += f" ... (and {len(items)-limit} more)"
                    return result

                # Use direct SQL execution for context loading - much more reliable
                with engine.connect() as conn:
                    # 1. Load Locations from 'room' table
                    if "room" in clean_tables:
                        try:
                            res = conn.execute(text("SELECT DISTINCT roomname FROM room WHERE roomname IS NOT NULL LIMIT 50"))
                            rows = res.fetchall()
                            if rows:
                                room_names = [r[0] for r in rows if r[0]]
                                semantic_context.append(f"- ROOMS/GALLERIES MAP: {truncate_list(room_names)}")
                        except Exception as e:
                            print(f"[WARN] Failed to load rooms: {e}")

                    # 2. Load Techniques from 'artistwork'
                    if "artistwork" in clean_tables:
                        try:
                            # In production artistwork has techniqueid, joined table or view might have description
                            res = conn.execute(text("SELECT DISTINCT techniqueid FROM artistwork WHERE techniqueid IS NOT NULL LIMIT 20"))
                            rows = res.fetchall()
                            if rows:
                                tech_ids = [r[0] for r in rows if r[0]]
                                semantic_context.append(f"- TECHNIQUES MAP: {truncate_list(tech_ids)}")
                        except Exception as e:
                            print(f"[WARN] Failed to load techniques: {e}")

                    # 3. Load Sample Titles from 'artistwork'
                    if "artistwork" in clean_tables:
                        try:
                            res = conn.execute(text("SELECT artistworktitle FROM artistwork LIMIT 20"))
                            rows = res.fetchall()
                            if rows:
                                titles = [r[0] for r in rows if r[0]]
                                semantic_context.append(f"- SAMPLE TITLES: {truncate_list(titles)}")
                        except Exception as e:
                            print(f"[WARN] Failed to load titles: {e}")

                    # 3. Load Museums/Sites
                    if "site" in clean_tables:
                        try:
                            res = conn.execute(text("SELECT sitename FROM site LIMIT 20"))
                            rows = res.fetchall()
                            if rows:
                                site_names = [r[0] for r in rows if r[0]]
                                semantic_context.append(f"- MUSEUMS/LOCATIONS MAP: {truncate_list(site_names)}")
                        except Exception as e:
                            print(f"[WARN] Failed to load sites: {e}")

            except Exception as e:
                print(f"[WARN] Failed to load semantic context: {e}")

            # 5. Load Semantic Dictionary
            dict_context = ""
            try:
                # Use absolute path relative to this file
                current_dir = os.path.dirname(os.path.abspath(__file__))
                dict_path = os.path.join(os.path.dirname(os.path.dirname(current_dir)), "data", "semantic_dictionary.json")
                if os.path.exists(dict_path):
                    with open(dict_path, 'r') as f:
                        sem_dict = json.load(f)
                        
                    dict_lines = ["=== SEMANTIC SCHEMA MAPPING ==="]
                    for table, config in sem_dict.get("tables", {}).items():
                        if table in clean_tables:
                            labels = config.get("description", {}).get("it", "")
                            cols = []
                            for col, col_config in config.get("columns", {}).items():
                                col_labels = "|".join(col_config.get("labels", []))
                                cols.append(f"{col}({col_labels})")
                            dict_lines.append(f"- {table}: {labels} Cols: {', '.join(cols)}")
                    
                    dict_context = "\n".join(dict_lines)
            except Exception as dict_err:
                print(f"[WARN] Failed to load semantic dictionary: {dict_err}")

            learned_knowledge = "\n".join(semantic_context)
            # Use smaller limit for learned mapping in prompt if needed
            # (already truncated to 10 in logic above, let's keep it 10 for now but watch tokens)

            context_to_inject = (
                "Sei un 'Assistente AI' museale esperto. REGOLE FONDAMENTALI:\n"
                "1. LINGUA: Rispondi SEMPRE nella lingua dell'utente (es. Italiano).\n"
                "2. PERSONALITÀ: Sii amichevole e colto. NON usare mai parole tecniche come 'database', 'strumenti', 'SQL', 'tabelle' o 'query'. Parla come una persona reale.\n"
                "3. MISSIONE: Sei la guida del museo. Aiuta con opere, artisti e percorsi. Puoi scambiare saluti e presentarti.\n"
                "4. LIMITI: Per richieste fuori ambito (es. cucina, sport), rispondi gentilmente che la tua passione e competenza sono limitate alla storia dell'arte del museo.\n"
                "7. COMPLETEZZA: Fornisci elenchi esaustivi se richiesto. Non troncare mai le liste.\n"
                "8. PULIZIA TESTI: Le descrizioni nel database contengono spesso tag HTML. Devi rimuoverli o convertirli in testo piano.\n"
                "9. AMBIGUITÀ: Se una ricerca produce più risultati, elencali e chiedi all'utente di specificare.\n"
                f"9. MAPPATURE:\n{dict_context}\n"
                f"10. VALORI CONOSCIUTI:\n{learned_knowledge}\n"
            )

            from llama_index.core.query_engine import NLSQLTableQueryEngine
            
            sql_specific_context = (
                "STRICT RULES FOR SQL GENERATION:\n"
                "1. FLEXIBLE SEARCH: Use ILIKE with % (e.g., '%fanciulla%amore%') for text matches.\n"
                "2. SITE RIGOR: Filter by siteid if provided.\n"
                "3. OUTPUT: Return ONLY the SQL query code.\n"
                f"DATABASE SCHEMA:\n{dict_context}\n"
            )

            from llama_index.core.prompts import PromptTemplate

            TEXT_TO_SQL_PROMPT_STR = (
                "Sei un esperto PostgreSQL. Traduci la domanda in SQL.\n"
                "REGOLE:\n"
                "1. Usa ILIKE con % (es. '%fanciulla%').\n"
                "2. Usa 'WHERE siteid = X' se specificato.\n"
                "3. Restituisci SOLO la query SQL.\n"
                "Schema:\n{schema}\n"
                "Context: {context_str}\n"
                "Question: {query_str}\n"
                "SQLQuery: "
            )
            
            self.sql_engine = NLSQLTableQueryEngine(
                self.sql_database,
                tables=clean_tables,
                llm=self.llm,
                sql_limit=500,
                context_str=sql_specific_context,
                text_to_sql_prompt=PromptTemplate(TEXT_TO_SQL_PROMPT_STR)
            )
            print("--- SQL Engine Ready (Universal Semantic Mode) ---")
            
            # Wrap with a tool description
            sql_tool = QueryEngineTool.from_defaults(
                query_engine=self.sql_engine,
                name="sql_engine",
                description=(
                    "DA USARE PER TUTTE LE DOMANDE SU: Opere d'arte, Artisti, Stanze/Location e Percorsi. "
                    "Traduce in SQL la domanda. Specifica sempre il siteid se noto."
                ),
            )
            self.query_tools.append(sql_tool)
            print(f"SQL Tool initialized for tables: {tables_to_reflect}")

        # 4. Setup RAG Engine (if docs exist)
        if doc_store_path and os.path.exists(doc_store_path):
            # For PoC we assume a local persist dir exists for valid tenant
            # In prod, this connects to Qdrant/Chroma with tenant filters
            from llama_index.core import StorageContext, load_index_from_storage
            try:
                storage_context = StorageContext.from_defaults(persist_dir=doc_store_path)
                vector_index = load_index_from_storage(
                    storage_context, 
                    embed_model=self.embed_model
                )
                
                rag_engine = vector_index.as_query_engine(
                    llm=self.llm,
                    embed_model=self.embed_model
                )
                
                rag_tool = QueryEngineTool.from_defaults(
                    query_engine=rag_engine,
                    description=(
                        "Useful for answering qualitative questions, summaries, or finding information "
                        "within unsupported text documents, PDFs, or knowledge base articles."
                    ),
                )
                self.query_tools.append(rag_tool)
                print(f"RAG Tool initialized from: {doc_store_path}")
            except Exception as e:
                print(f"Failed to load vector store for tenant {tenant_id}: {e}")

        # 5. Create Router
        # Using LLMSingleSelector to pick the BEST single tool, or MultiSelector for both.
        # Requirement said "or both", so potentially MultiSelector or nested routing.
        # For simplicity/robustness in PoC, SingleSelector is often safer, but let's try LLMSingleSelector first.
        # 3. Build the Agent instead of a rigid Router
        # The Agent can chat AND use tools when needed.
        self.agent = ReActAgent(
            tools=self.query_tools,
            llm=self.llm,
            system_prompt=context_to_inject,
            verbose=True
        )
        print("--- Agent Pipeline Initialization Complete ---")



    async def query(self, user_query: str, site_id: str = None, session_id: str = None) -> dict:
        """
        Executes the trusted query pipeline using native ChatMessage context.
        """
        if not self.query_tools:
            return {"answer": "No data sources configured.", "source_type": "none"}
            
        start_time = time.time()
        session_info = f"[Session: {session_id}] " if session_id else ""
        print(f"[PROCESS] {session_info}Start Agent Query: {user_query}")
        
        try:
            # 1. Retrieve or init history as ChatMessage objects
            if session_id not in self.session_memory:
                self.session_memory[session_id] = []
            
            history = self.session_memory[session_id]
            
            # 2. Construct the current user message with site context
            display_query = user_query
            if site_id:
                user_msg_content = f"{user_query} (siteid: {site_id})"
            else:
                user_msg_content = user_query

            # 3. Call agent with history
            print(f"[PROCESS] Agent thinking (Native Context Mode)...")
            response = await self.agent.run(user_msg=user_msg_content, chat_history=history)
            
            # 4. Update memory with BOTH messages
            history.append(ChatMessage(role=MessageRole.USER, content=user_msg_content))
            history.append(ChatMessage(role=MessageRole.ASSISTANT, content=str(response)))
            
            # Trim history to keep context manageable (last 10 messages)
            if len(history) > 10:
                self.session_memory[session_id] = history[-10:]

            elapsed = time.time() - start_time
            print(f"[PROCESS] Query complete in {elapsed:.2f}s")
            
            return {
                "answer": str(response),
                "source_type": "hybrid"
            }
        except Exception as e:
            print(f"[CRITICAL ERROR] Pipeline query failed: {str(e)}")
            traceback.print_exc()
            return {
                "answer": f"Errore tecnico: {str(e)}",
                "source_type": "error"
            }
