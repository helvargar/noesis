# Architecture: Noesis AI SaaS - BYO-LLM Knowledge Engine

## 1. High-Level Architecture

The system is a multi-tenant SaaS that provides an API-first interface for companies to plug in their data (SQL + Docs) and their own LLM credentials. The core value prop is the secure orchestration layer.

### Core Components

1.  **API Gateway / Interface Layer (FastAPI)**
    *   Handles authentication (JWT).
    *   Receives user queries.
    *   Injects Tenant Context (Tenant ID, decrypts LLM API Key ephemeral usage).

2.  **Orchestration & Routing Engine (LlamaIndex)**
    *   **RouterQueryEngine**: Analyzes the query to decide routing strategy.
    *   **Strategies**: 
        *   `VectorStore` (Unstructured/Sematic search)
        *   `Text-to-SQL` (Structured data analysis)
        *   `Hybrid` (Synthesizing both)

3.  **Data Layer (Per-Tenant Isolation)**
    *   **Vector Store**: logicially isolated indices (e.g., Qdrant/Chroma with metadata filters or separate collections per tenant).
    *   **SQL Database**: Read-only connection to existing tenant databases.
    *   **Document Storage**: Object storage (S3/MinIO) for source files.

4.  **Security & Governance**
    *   **Guardrails**: Input validation, SQL query whitelisting (AST parsing to forbid DROP/DELETE).
    *   **Key Manager**: AES encryption for storing Tenant LLM keys. System never logs these keys.
    *   **Metering**: Redis/Postgres table to track token usage per Tenant ID.

## 2. Component Details

### B.Y.O. LLM Abstraction
We define an `LLMFactory` that instantiates the correct LlamaIndex LLM class (OpenAI, Azure, Anthropic) at runtime request scope based on Tenant config.

### RAG Pipeline
1.  **Ingestion**: Document -> Chunking (300-500 tokens) -> Embedding (OpenAI/Cohere) -> Vector Store.
2.  **Retrieval**: Sparse + Dense retrieval for better accuracy.
3.  **Synthesis**: LLM takes retrieved chunks + query -> final answer.

### SQL Pipeline
1.  **Schema Context**: Inject *only* whitelisted table schemas into prompt.
2.  **Generation**: LLM generates SQL.
3.  **Validation**: A parser checks for destructive keywords (DELETE, DROP, UPDATE, etc.).
4.  **Execution**: Run against DB with read-only credentials if possible.
5.  **Synthesis**: Results (rows) -> Natural Language.

## 3. Project Structure (Python)

```bash
noesis-backend/
├── app/
│   ├── api/
│   │   ├── dependencies.py    # Auth & Tenant context injection
│   │   ├── routes.py          # /chat, /ingest, /config endpoints
│   ├── core/
│   │   ├── config.py          # App configuration
│   │   ├── security.py        # Encryption logic for API keys
│   │   ├── factory.py         # LLM Factory (runtime instantiation)
│   ├── engine/
│   │   ├── ingest.py          # Document loader & indexer
│   │   ├── query.py           # Router & Generation logic
│   │   ├── sql_engine.py      # Text-to-SQL specialized logic
│   │   ├── guardrails.py      # SQL validation
│   ├── models/                # Pydantic & ORM models
│   │   ├── tenant.py
│   │   ├── chat.py
│   ├── services/
│   │   ├── vector_store.py    # Vector DB interface
│   │   ├── metering.py        # Usage tracking
│   ├── main.py                # Entrypoint
├── tests/
├── requirements.txt
├── .env
```
