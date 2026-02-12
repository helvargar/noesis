from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime

class LLMConfig(BaseModel):
    """Configuration for tenant's LLM provider."""
    provider: Literal["openai", "anthropic", "azure_openai", "groq", "gemini", "ollama"] = "openai"
    model_name: Optional[str] = None  # e.g., "gpt-4-turbo", "claude-3-opus"
    api_key_encrypted: str = ""  # Stored encrypted, never exposed
    
    # Azure-specific (optional)
    azure_endpoint: Optional[str] = None
    azure_deployment: Optional[str] = None
    api_version: Optional[str] = None

class DatabaseConfig(BaseModel):
    """Configuration for tenant's SQL database."""
    enabled: bool = False
    db_type: Literal["postgres", "mysql", "sqlite"] = "postgres"
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    schema_name: str = "public"  # For Postgres/MySQL schemas
    password_encrypted: str = ""  # Stored encrypted
    
    # Security: Whitelist
    allowed_tables: List[str] = Field(default_factory=list)
    allowed_columns: dict[str, List[str]] = Field(default_factory=dict)  # table -> [columns]
    
    # Guardrails
    max_rows: int = 1000
    timeout_seconds: int = 30

class DocumentConfig(BaseModel):
    """Configuration for tenant's document store."""
    enabled: bool = False
    vector_index_path: str = ""  # Path or collection name
    chunk_size: int = 400  # tokens
    chunk_overlap: int = 50

class UsageLimits(BaseModel):
    """Usage limits per billing plan."""
    max_queries_per_month: int = 10000
    max_documents: int = 500
    max_document_size_mb: int = 50

class Tenant(BaseModel):
    """Complete tenant configuration."""
    id: str
    name: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True
    
    # Sub-configurations
    llm: LLMConfig = Field(default_factory=LLMConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    documents: DocumentConfig = Field(default_factory=DocumentConfig)
    limits: UsageLimits = Field(default_factory=UsageLimits)

# --- API Request/Response Models ---

class TenantCreateRequest(BaseModel):
    """Request to create a new tenant."""
    name: str
    llm_provider: Literal["openai", "anthropic", "azure_openai", "groq", "gemini", "ollama"] = "openai"
    llm_api_key: str  # Plain text, will be encrypted before storage
    llm_model_name: Optional[str] = None

class TenantUpdateLLMRequest(BaseModel):
    """Update LLM configuration."""
    provider: Optional[Literal["openai", "anthropic", "azure_openai", "groq", "gemini", "ollama"]] = None
    api_key: Optional[str] = None  # Plain, will be encrypted
    model_name: Optional[str] = None
    azure_endpoint: Optional[str] = None
    azure_deployment: Optional[str] = None

class TenantUpdateDBRequest(BaseModel):
    """Update database configuration."""
    enabled: bool = True
    db_type: Literal["postgres", "mysql", "sqlite"] = "postgres"
    host: str
    port: int = 5432
    database: str
    username: str
    schema_name: Optional[str] = "public"
    password: Optional[str] = None  # Plain, will be encrypted if provided
    allowed_tables: List[str] = Field(default_factory=list)
    allowed_columns: dict[str, List[str]] = Field(default_factory=dict)
    max_rows: int = 1000
    timeout_seconds: int = 30

class TenantPublicView(BaseModel):
    """Safe view of tenant config (no secrets)."""
    id: str
    name: str
    is_active: bool
    llm_provider: str
    llm_model: Optional[str]
    has_llm_key: bool = False
    db_enabled: bool
    db_type: Optional[str]
    db_host: Optional[str]
    db_port: Optional[int]
    db_name: Optional[str]
    db_user: Optional[str]
    db_schema: Optional[str] = "public"
    db_allowed_tables: List[str] = []
    has_db_password: bool = False
    docs_enabled: bool
    created_at: datetime

