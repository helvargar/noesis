"""
Tenant Service - CRUD operations for tenant management.
In production, this would use SQLAlchemy with Postgres.
For PoC, we use an in-memory store with JSON persistence.
"""
import json
import os
from typing import Optional, List
from datetime import datetime
import uuid

from app.models.tenant import (
    Tenant, LLMConfig, DatabaseConfig, DocumentConfig,
    TenantCreateRequest, TenantUpdateLLMRequest, TenantUpdateDBRequest,
    TenantPublicView
)
from app.core.security import encrypt_key, decrypt_key

# Persistence file for PoC
TENANT_STORE_PATH = "./data/tenants.json"

class TenantService:
    """Service layer for tenant CRUD operations."""
    
    def __init__(self):
        self._tenants: dict[str, Tenant] = {}
        self._load_from_disk()
    
    def _load_from_disk(self):
        """Load tenants from JSON file."""
        if os.path.exists(TENANT_STORE_PATH):
            try:
                with open(TENANT_STORE_PATH, "r") as f:
                    data = json.load(f)
                    for tid, tdata in data.items():
                        self._tenants[tid] = Tenant.model_validate(tdata)
            except Exception as e:
                print(f"Warning: Could not load tenants: {e}")
    
    def _save_to_disk(self):
        """Persist tenants to JSON file."""
        os.makedirs(os.path.dirname(TENANT_STORE_PATH), exist_ok=True)
        with open(TENANT_STORE_PATH, "w") as f:
            data = {tid: t.model_dump(mode="json") for tid, t in self._tenants.items()}
            json.dump(data, f, indent=2, default=str)
    
    def create_tenant(self, request: TenantCreateRequest) -> Tenant:
        """Create a new tenant with initial LLM configuration."""
        tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
        
        # Encrypt the API key before storage
        encrypted_key = encrypt_key(request.llm_api_key)
        
        tenant = Tenant(
            id=tenant_id,
            name=request.name,
            llm=LLMConfig(
                provider=request.llm_provider,
                model_name=request.llm_model_name,
                api_key_encrypted=encrypted_key
            ),
            documents=DocumentConfig(
                vector_index_path=f"./data/{tenant_id}_index"
            )
        )
        
        self._tenants[tenant_id] = tenant
        self._save_to_disk()
        return tenant
    
    def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Retrieve tenant by ID."""
        return self._tenants.get(tenant_id)
    
    def list_tenants(self) -> List[TenantPublicView]:
        """List all tenants (safe view, no secrets)."""
        result = []
        for t in self._tenants.values():
            result.append(TenantPublicView(
                id=t.id,
                name=t.name,
                is_active=t.is_active,
                llm_provider=t.llm.provider,
                llm_model=t.llm.model_name,
                has_llm_key=bool(t.llm.api_key_encrypted),
                db_enabled=t.database.enabled,
                db_type=t.database.db_type if t.database.enabled else None,
                db_host=t.database.host if t.database.enabled else None,
                db_port=t.database.port if t.database.enabled else None,
                db_name=t.database.database if t.database.enabled else None,
                db_user=t.database.username if t.database.enabled else None,
                db_schema=t.database.schema_name if t.database.enabled else "public",
                db_allowed_tables=t.database.allowed_tables if t.database.enabled else [],
                has_db_password=bool(t.database.password_encrypted),
                docs_enabled=t.documents.enabled,
                created_at=t.created_at
            ))
        return result

    
    def update_llm_config(self, tenant_id: str, request: TenantUpdateLLMRequest) -> Optional[Tenant]:
        """Update tenant's LLM configuration."""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return None
        
        if request.provider:
            tenant.llm.provider = request.provider
        if request.model_name:
            tenant.llm.model_name = request.model_name
        if request.api_key:
            tenant.llm.api_key_encrypted = encrypt_key(request.api_key)
        if request.azure_endpoint:
            tenant.llm.azure_endpoint = request.azure_endpoint
        if request.azure_deployment:
            tenant.llm.azure_deployment = request.azure_deployment
        
        tenant.updated_at = datetime.utcnow()
        self._save_to_disk()
        return tenant
    
    def update_db_config(self, tenant_id: str, request: TenantUpdateDBRequest) -> Optional[Tenant]:
        """Update tenant's database configuration."""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return None
        
        # Preserve password if not provided
        password_enc = tenant.database.password_encrypted
        if request.password:
            password_enc = encrypt_key(request.password)

        tenant.database = DatabaseConfig(
            enabled=request.enabled,
            db_type=request.db_type,
            host=request.host,
            port=request.port,
            database=request.database,
            username=request.username,
            schema_name=request.schema_name or "public",
            password_encrypted=password_enc,
            allowed_tables=request.allowed_tables,
            allowed_columns=request.allowed_columns,
            max_rows=request.max_rows,
            timeout_seconds=request.timeout_seconds
        )
        
        tenant.updated_at = datetime.utcnow()
        self._save_to_disk()
        return tenant
    
    def enable_documents(self, tenant_id: str, chunk_size: int = 400) -> Optional[Tenant]:
        """Enable document processing for tenant."""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return None
        
        tenant.documents.enabled = True
        tenant.documents.chunk_size = chunk_size
        tenant.updated_at = datetime.utcnow()
        self._save_to_disk()
        return tenant
    
    def delete_tenant(self, tenant_id: str) -> bool:
        """Soft delete (deactivate) a tenant."""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return False
        
        tenant.is_active = False
        tenant.updated_at = datetime.utcnow()
        self._save_to_disk()
        return True
    
    def get_decrypted_llm_key(self, tenant_id: str) -> Optional[str]:
        """
        Internal use only: Get decrypted API key for LLM calls.
        NEVER expose this via API.
        """
        tenant = self._tenants.get(tenant_id)
        if not tenant or not tenant.llm.api_key_encrypted:
            return None
        return decrypt_key(tenant.llm.api_key_encrypted)
    
    def get_db_connection_string(self, tenant_id: str) -> Optional[str]:
        """Build connection string for tenant's database."""
        tenant = self._tenants.get(tenant_id)
        if not tenant or not tenant.database.enabled:
            return None
        
        db = tenant.database
        password = decrypt_key(db.password_encrypted) if db.password_encrypted else ""
        
        if db.db_type == "postgres":
            # For postgres we add search_path to the connection string options
            schema_opt = f"?options=-csearch_path={db.schema_name}" if db.schema_name else ""
            return f"postgresql://{db.username}:{password}@{db.host}:{db.port}/{db.database}{schema_opt}"
        elif db.db_type == "mysql":
            return f"mysql+pymysql://{db.username}:{password}@{db.host}:{db.port}/{db.database}"
        elif db.db_type == "sqlite":
            return f"sqlite:///{db.database}"
        
        return None

# Singleton instance
tenant_service = TenantService()
