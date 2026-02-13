from llama_index.core import VectorStoreIndex, SQLDatabase, Settings, PromptTemplate
from llama_index.core.query_engine import NLSQLTableQueryEngine, SQLTableRetrieverQueryEngine
from llama_index.core.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
from llama_index.core.tools import QueryEngineTool, ToolMetadata, FunctionTool
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
                "Sei Gyp, l'esperto virtuale del Museo Bailo. Parla in modo colto, cordiale e naturale, come se stessi accompagnando un visitatore.\n\n"
                "CONOSCENZA DEL MUSEO:\n"
                f"{chr(10).join(semantic_guide)}\n\n"
                "REGOLA D'ORO (ACCURACY): Per ogni informazione su artisti, opere, sale o collezioni, DEVI usare 'sql_engine'.\n"
                "1. VERIDICITÀ: Se il database non restituisce nulla per una ricerca specifica (es. un artista non presente), rispondi che non hai informazioni su quel soggetto nel museo.\n"
                "2. NO ALLUCINAZIONI: MAI inventare attribuzioni o fatti. Non attribuire mai opere esistenti (es. di Martini) a un artista cercato ma non trovato (es. Picasso).\n"
                "3. SUGGERIMENTI: Solo se l'utente chiede consigli generici (es. 'cosa mi consigli?'), puoi proporre una selezione delle opere che conosci dal database.\n"
                "4. BYPASS: Per biografie o storie d'opera integrali, includi sempre il token [[DIRECT_DISPLAY]].\n"
                "5. SEGRETEZZA: Non parlare mai di SQL, database o tool. Parla come un esperto umano.\n"
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
                "2. Niente chiacchiere: Restituisci esclusivamente il codice SQL iniziando con SELECT. Nessun commento o spiegazione.\n"
                "3. siteid: Usa sempre 'siteid = 1' per artistwork, artist, site e pathway.\n"
                "4. PULIZIA: Se i dati nel DB contengono tag HTML (es. <p>, <div>), ignorali e restituisci solo il testo pulito.\n\n"
                "STRUTTURA REALE (DDL):\n"
                "{schema_ddl}\n\n"
                "CAMPIONI DATI (FONDAMENTALI PER I FILTRI):\n"
                "{samples_hint}\n\n"
                "GOLDEN QUERIES (ESEMPI):\n"
                "Q: opere sala 9 -> SELECT aw.artistworktitle FROM artistwork aw JOIN room r ON aw.roomid = r.roomid WHERE r.roomname ILIKE '%%SALA 9%%' AND aw.siteid = 1;\n"
                "Q: chi è Martini -> SELECT artistname, artistdescription, biography FROM artist WHERE artistname ILIKE '%%Arturo Martini%%' AND siteid = 1;\n"
                "Q: opere marmo Canova -> SELECT aw.artistworktitle FROM artistwork aw JOIN artist a ON aw.artistid = a.artistid JOIN technique t ON aw.techniqueid = t.techniqueid WHERE a.artistname ILIKE '%%Antonio Canova%%' AND t.techniquedescription ILIKE '%%MARMO%%';\n"
                "Q: indirizzo museo -> SELECT address, city FROM site WHERE siteid = 1;\n\n"
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
            
            sql_context_lines.append("\nPRESCRIZIONE: Privilegia sempre le colonne '...description' per fornire risposte esaustive.")

            # Custom response synthesis prompt - forces the LLM to return full text
            RESPONSE_SYNTHESIS_PROMPT_STR = (
                "Sei Gyp, l'assistente del museo. Basandoti sui risultati SQL, scrivi una risposta ESAUSTIVA.\n"
                "REGOLA D'ORO: Riporta INTEGRALMENTE tutto il testo descrittivo ottenuto (biografie, storie, descrizioni).\n"
                "NON RIASSUMERE mai. Se il testo dal database è lungo, riportalo tutto.\n\n"
                "Domanda: {query_str}\n"
                "Query SQL: {sql_query}\n"
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
                result = _sql_engine.query(query)
                raw = str(result)
                
                # Try to parse the raw string [('text',), ...] to extract pure text
                max_field_len = 0
                # Attempt robust parsing of SQL results (tuples/lists)
                rows = []
                try:
                    # Clean up common SQL string artifacts before eval
                    raw_eval = raw.replace('datetime.date', 'str')
                    parsed = ast.literal_eval(raw_eval)
                    if isinstance(parsed, list):
                        for row in parsed:
                            if isinstance(row, (list, tuple)):
                                # Extract long fields for bypass detection
                                for col in row:
                                    if isinstance(col, str):
                                        max_field_len = max(max_field_len, len(col))
                                row_str = " - ".join([str(c) for c in row if c is not None and str(c).strip() != ""])
                                if row_str: rows.append(row_str)
                            else:
                                rows.append(str(row))
                        raw = "\n\n".join(rows)
                except Exception:
                    # Fallback: manually strip typical SQL artifacts if eval fails
                    raw = re.sub(r'[\[\]\(\)]', '', raw)
                    raw = re.sub(r"None", "", raw)
                    max_field_len = len(raw)
                
                # Global HTML/Tag cleaning
                raw = html.unescape(raw)
                raw = re.sub(r'<(p|br|div)[^>]*>', '\n', raw, flags=re.IGNORECASE)
                raw = re.sub(r'<[^>]+>', ' ', raw)
                raw = re.sub(r' +', ' ', raw)
                raw = re.sub(r'\n\s*\n', '\n\n', raw)
                raw = raw.strip()
                
                if not raw or raw == "[]":
                    return "Nessun dato trovato nel database."
                
                # AUTHORITATIVE BYPASS: Trigger if ANY field is very long (biography/description)
                if max_field_len > 350:
                    self.last_sql_result = raw
                    return f"Risultato presente nel sistema ([[DIRECT_DISPLAY]])."
                
                return f"Risultato:\n{raw}"
            
            sql_tool = FunctionTool.from_defaults(
                fn=sql_query_tool,
                name="sql_engine",
                description=f"Database del museo. Tabelle: {concept_map_desc}. Per testi lunghi restituisce un segnale di bypass."
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
                    name="document_engine",
                    description="Usa questo strumento per info estratte da documenti PDF o articoli esterni al database."
                ))
            except Exception: pass

        # 6. Create Agent
        try:
            self.agent = ReActAgent(
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
        # Hide technical database errors from user
        if "Error:" in answer or "SQL" in answer or "column" in answer:
            if "Nessun dato" not in answer:
                answer = "Mi dispiace, non sono riuscito a trovare le informazioni specifiche nel mio archivio in questo momento. Posso aiutarti con qualcos'altro o vuoi provare a chiedermi di un artista specifico?"
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

            # 5. Get Agent Response
            agent_start = time.time()
            response = await self.agent.run(user_msg=enriched_query, chat_history=current_context + history)
            answer = str(response)
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
            return {"answer": answer, "source_type": "hybrid"}
        except Exception as e:
            traceback.print_exc()
            return {"answer": f"Si è verificato un errore: {str(e)}", "source_type": "error"}
