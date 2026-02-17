"""
API Routes - Complete CRUD for tenants and chat functionality.
Protected with JWT authentication.
"""
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Depends
from fastapi.responses import StreamingResponse
from typing import List, Optional
import os
import shutil

from app.models.tenant import (
    TenantCreateRequest, TenantUpdateLLMRequest, TenantUpdateDBRequest,
    TenantPublicView
)
from app.models.user import TokenPayload
from app.services.tenant_service import tenant_service
from app.services.metering import metering_service, TenantUsageSummary
from app.api.dependencies import get_current_user, require_admin, require_tenant_access
from app.engine.query import TenantQueryPipeline
from pydantic import BaseModel
router = APIRouter()

# In-memory cache for pipelines to avoid expensive re-init (reflection)
import time
_pipeline_cache = {}

# ==================== TENANT MANAGEMENT (Admin Only) ====================

@router.post("/tenants", response_model=TenantPublicView, tags=["Tenants"])
def create_tenant(
    request: TenantCreateRequest,
    current_user: TokenPayload = Depends(require_admin)
):
    """
    Create a new tenant with initial LLM configuration.
    Admin only.
    """
    tenant = tenant_service.create_tenant(request)
    return TenantPublicView(
        id=tenant.id,
        name=tenant.name,
        is_active=tenant.is_active,
        llm_provider=tenant.llm.provider,
        llm_model=tenant.llm.model_name,
        has_llm_key=bool(tenant.llm.api_key_encrypted),
        db_enabled=tenant.database.enabled,
        db_type=None,
        db_host=None,
        db_port=None,
        db_name=None,
        db_user=None,
        db_schema="public",
        db_allowed_tables=[],
        has_db_password=bool(tenant.database.password_encrypted),
        docs_enabled=tenant.documents.enabled,
        created_at=tenant.created_at
    )

@router.get("/tenants", response_model=List[TenantPublicView], tags=["Tenants"])
def list_tenants(current_user: TokenPayload = Depends(require_admin)):
    """List all tenants. Admin only."""
    return tenant_service.list_tenants()

@router.get("/tenants/{tenant_id}", response_model=TenantPublicView, tags=["Tenants"])
async def get_tenant(
    tenant_id: str,
    current_user: TokenPayload = Depends(get_current_user)
):
    """Get a specific tenant's configuration."""
    # Check access
    await require_tenant_access(tenant_id, current_user)
    
    tenant = tenant_service.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return TenantPublicView(
        id=tenant.id,
        name=tenant.name,
        is_active=tenant.is_active,
        llm_provider=tenant.llm.provider,
        llm_model=tenant.llm.model_name,
        has_llm_key=bool(tenant.llm.api_key_encrypted),
        db_enabled=tenant.database.enabled,
        db_type=tenant.database.db_type if tenant.database.enabled else None,
        db_host=tenant.database.host if tenant.database.enabled else None,
        db_port=tenant.database.port if tenant.database.enabled else None,
        db_name=tenant.database.database if tenant.database.enabled else None,
        db_user=tenant.database.username if tenant.database.enabled else None,
        db_schema=tenant.database.schema_name if tenant.database.enabled else "public",
        db_allowed_tables=tenant.database.allowed_tables if tenant.database.enabled else [],
        has_db_password=bool(tenant.database.password_encrypted),
        docs_enabled=tenant.documents.enabled,
        created_at=tenant.created_at
    )

@router.put("/tenants/{tenant_id}/llm", tags=["Tenants"])
def update_llm_config(
    tenant_id: str,
    request: TenantUpdateLLMRequest,
    current_user: TokenPayload = Depends(require_admin)
):
    """Update tenant's LLM configuration. Admin only."""
    tenant = tenant_service.update_llm_config(tenant_id, request)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {"status": "updated", "tenant_id": tenant_id}

@router.put("/tenants/{tenant_id}/database", tags=["Tenants"])
def update_database_config(
    tenant_id: str,
    request: TenantUpdateDBRequest,
    current_user: TokenPayload = Depends(require_admin)
):
    """
    Configure tenant's SQL database connection. Admin only.
    Includes whitelist of allowed tables and columns for security.
    """
    tenant = tenant_service.update_db_config(tenant_id, request)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {
        "status": "updated",
        "tenant_id": tenant_id,
        "allowed_tables": request.allowed_tables
    }

@router.post("/tenants/test-db", tags=["Tenants"])
async def test_db_connection(
    request: TenantUpdateDBRequest,
    tenant_id: Optional[str] = None,
    current_user: TokenPayload = Depends(require_admin)
):
    """
    Test a database connection before saving.
    Admin only.
    """
    try:
        from sqlalchemy import create_engine, text
        
        password = request.password
        # If password is empty and tenant_id is provided, try to get stored password
        if not password and tenant_id:
            stored_tenant = tenant_service.get_tenant(tenant_id)
            if stored_tenant and stored_tenant.database.password_encrypted:
                from app.core.security import decrypt_key
                password = decrypt_key(stored_tenant.database.password_encrypted)
        
        if not password:
            password = ""

        # Build connection string
        if request.db_type == "postgres":
            uri = f"postgresql://{request.username}:{password}@{request.host}:{request.port}/{request.database}"
        elif request.db_type == "mysql":
            uri = f"mysql+pymysql://{request.username}:{password}@{request.host}:{request.port}/{request.database}"
        elif request.db_type == "sqlite":
            # For sqlite, check if absolute path or local
            uri = f"sqlite:///{request.database}"
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported DB type: {request.db_type}")

        # Try to connect
        engine = create_engine(uri, connect_args={"connect_timeout": 5} if request.db_type != "sqlite" else {})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        return {"status": "success", "message": "Connessione riuscita!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/tenants/fetch-schemas", tags=["Tenants"])
async def fetch_db_schemas(
    request: TenantUpdateDBRequest,
    tenant_id: Optional[str] = None,
    current_user: TokenPayload = Depends(require_admin)
):
    """
    Fetch available schemas from the database.
    Admin only.
    """
    try:
        from sqlalchemy import create_engine, inspect
        
        password = request.password
        if not password and tenant_id:
            stored_tenant = tenant_service.get_tenant(tenant_id)
            if stored_tenant and stored_tenant.database.password_encrypted:
                from app.core.security import decrypt_key
                password = decrypt_key(stored_tenant.database.password_encrypted)
        
        if not password:
            password = ""

        if request.db_type == "postgres":
            uri = f"postgresql://{request.username}:{password}@{request.host}:{request.port}/{request.database}"
        elif request.db_type == "mysql":
            uri = f"mysql+pymysql://{request.username}:{password}@{request.host}:{request.port}/{request.database}"
        else:
            return {"schemas": ["main"] if request.db_type == "sqlite" else []}

        engine = create_engine(uri, connect_args={"connect_timeout": 5})
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()
        
        return {"schemas": schemas}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/tenants/{tenant_id}", tags=["Tenants"])
def delete_tenant(
    tenant_id: str,
    current_user: TokenPayload = Depends(require_admin)
):
    """Soft delete (deactivate) a tenant. Admin only."""
    success = tenant_service.delete_tenant(tenant_id)
    if not success:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {"status": "deactivated", "tenant_id": tenant_id}

# ==================== DOCUMENT UPLOAD ====================

@router.post("/tenants/{tenant_id}/documents", tags=["Documents"])
async def upload_document(
    tenant_id: str,
    file: UploadFile = File(...),
    trigger_indexing: bool = Form(default=True),
    current_user: TokenPayload = Depends(get_current_user)
):
    """
    Upload a document (PDF, TXT, MD) for a tenant.
    Requires tenant access.
    """
    await require_tenant_access(tenant_id, current_user)
    
    tenant = tenant_service.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Validate file type
    allowed_extensions = {".pdf", ".txt", ".md", ".docx"}
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"File type not allowed. Allowed: {allowed_extensions}"
        )
    
    # Save to tenant's raw folder
    raw_dir = f"./data/{tenant_id}_raw"
    os.makedirs(raw_dir, exist_ok=True)
    file_path = os.path.join(raw_dir, file.filename)
    
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Enable documents if not already
    if not tenant.documents.enabled:
        tenant_service.enable_documents(tenant_id)
    
    result = {"status": "uploaded", "file": file.filename, "path": file_path}
    
    # Trigger indexing if requested
    if trigger_indexing:
        try:
            from app.engine.ingest import build_index_for_tenant
            
            api_key = tenant_service.get_decrypted_llm_key(tenant_id)
            if api_key:
                build_index_for_tenant(
                    tenant_id=tenant_id,
                    source_dir=raw_dir,
                    output_dir=tenant.documents.vector_index_path or f"./data/{tenant_id}_index",
                    api_key_enc=tenant.llm.api_key_encrypted,
                    provider=tenant.llm.provider
                )
                result["indexed"] = True
        except Exception as e:
            result["indexed"] = False
            result["indexing_error"] = str(e)
    
    return result

@router.post("/tenants/{tenant_id}/reindex", tags=["Documents"])
async def trigger_reindex(
    tenant_id: str,
    current_user: TokenPayload = Depends(get_current_user)
):
    """Manually trigger re-indexing of all tenant documents."""
    await require_tenant_access(tenant_id, current_user)
    
    tenant = tenant_service.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    raw_dir = f"./data/{tenant_id}_raw"
    if not os.path.exists(raw_dir):
        raise HTTPException(status_code=400, detail="No documents uploaded yet")
    
    try:
        from app.engine.ingest import build_index_for_tenant
        
        build_index_for_tenant(
            tenant_id=tenant_id,
            source_dir=raw_dir,
            output_dir=tenant.documents.vector_index_path or f"./data/{tenant_id}_index",
            api_key_enc=tenant.llm.api_key_encrypted,
            provider=tenant.llm.provider
        )
        return {"status": "reindexed", "tenant_id": tenant_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== CHAT ====================

class ChatRequest(BaseModel):
    query: str
    session_id: Optional[str] = None
    site_id: Optional[str] = None
    target: Optional[str] = None
    stream: bool = False

class ChatResponse(BaseModel):
    answer: str
    sources: Optional[List[str]] = None
    query_type: Optional[str] = None


async def _get_or_create_pipeline(tenant_id: str):
    """
    Helper to get cached pipeline or create a new one.
    Used by chat endpoint and background warmup.
    """
    tenant = tenant_service.get_tenant(tenant_id)
    if not tenant:
        return None
        
    api_key = tenant_service.get_decrypted_llm_key(tenant_id)
    if not api_key:
        return None

    # Cache key based on updated_at to handle config changes
    cache_key = f"{tenant_id}_{tenant.updated_at.isoformat()}"
    
    if cache_key in _pipeline_cache:
        return _pipeline_cache[cache_key]
    
    # Init new pipeline
    print(f"[CACHE] Initializing new pipeline for {tenant_id} (Reflecting tables...)")
    db_uri = tenant_service.get_db_connection_string(tenant_id) if tenant.database.enabled else None
    doc_path = tenant.documents.vector_index_path if tenant.documents.enabled else None
    
    pipeline = TenantQueryPipeline(
        tenant_id=tenant_id,
        llm_provider=tenant.llm.provider,
        llm_api_key=api_key,
        llm_model=tenant.llm.model_name,
        sql_connection_str=db_uri,
        schema_name=tenant.database.schema_name,
        allowed_tables=tenant.database.allowed_tables,
        doc_store_path=doc_path
    )
    _pipeline_cache[cache_key] = pipeline
    return pipeline

async def warmup_pipelines():
    """Background task to initialize pipelines for all active tenants."""
    print("[WARMUP] Starting pipeline warm-up...")
    tenants = tenant_service.list_tenants()
    for t in tenants:
        if t.is_active:
            try:
                # We await here to ensure sequential loading to avoid CPU spikes
                await _get_or_create_pipeline(t.id)
                print(f"[WARMUP] Tenant {t.name} ({t.id}) ready.")
            except Exception as e:
                print(f"[WARMUP] Failed to warm up {t.name}: {e}")
    print("[WARMUP] Completed.")

@router.post("/tenants/{tenant_id}/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(
    tenant_id: str,
    request: ChatRequest
):
    """
    Main chat endpoint. Routes query to SQL or RAG based on content.
    Uses tenant's own LLM credentials.
    """
    # Temporarily disabled auth check as per user request
    # await require_tenant_access(tenant_id, current_user)
    
    tenant = tenant_service.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    if not tenant.is_active:
        raise HTTPException(status_code=403, detail="Tenant is deactivated")
    
    # Check usage limits
    current_usage = metering_service.get_current_month_count(tenant_id)
    if current_usage >= tenant.limits.max_queries_per_month:
        raise HTTPException(
            status_code=429,
            detail=f"Monthly query limit reached ({tenant.limits.max_queries_per_month})"
        )
    
    # Get decrypted credentials
    # Credentials check is handled inside _get_or_create_pipeline but we check here for specific error
    if not tenant_service.get_decrypted_llm_key(tenant_id):
        raise HTTPException(status_code=500, detail="LLM not configured properly")
    
    # Get pipeline (Cached or New) via Helper
    try:
        pipeline = await _get_or_create_pipeline(tenant_id)
        if not pipeline:
             raise HTTPException(status_code=500, detail="Pipeline initialization failed")
    except Exception as e:
        print(f"[ERROR] Pipeline init failed: {e}")
        raise HTTPException(status_code=500, detail=f"AI Engine Error: {str(e)}")

    try:
        if request.stream:
            print(f"[PROCESS] Streaming response for {tenant_id}")
            return StreamingResponse(
                pipeline.astream_query(
                    request.query, 
                    session_id=request.session_id, 
                    site_id=request.site_id, 
                    target=request.target
                ),
                media_type="text/plain"
            )

        result = await pipeline.query(request.query, site_id=request.site_id, session_id=request.session_id, target=request.target)
        answer = result["answer"]
        source_type = result["source_type"]
        
        # Record usage
        estimated_tokens = metering_service.estimate_tokens(request.query, answer)
        metering_service.record_usage(
            tenant_id=tenant_id,
            query_type="hybrid",
            model_used=tenant.llm.model_name or "default",
            estimated_tokens=estimated_tokens,
            success=True
        )
        
        return ChatResponse(
            answer=answer,
            query_type=source_type
        )
        
    except Exception as e:
        # Record failed usage
        metering_service.record_usage(
            tenant_id=tenant_id,
            query_type="unknown",
            model_used=tenant.llm.model_name or "default",
            estimated_tokens=0,
            success=False
        )
        raise HTTPException(status_code=500, detail=str(e))

# ==================== METERING ====================

@router.get("/tenants/{tenant_id}/usage", response_model=TenantUsageSummary, tags=["Metering"])
async def get_usage(
    tenant_id: str,
    year: int,
    month: int,
    current_user: TokenPayload = Depends(get_current_user)
):
    """Get usage summary for a specific month."""
    await require_tenant_access(tenant_id, current_user)
    
    tenant = tenant_service.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return metering_service.get_monthly_summary(tenant_id, year, month)

@router.get("/tenants/{tenant_id}/usage/current", tags=["Metering"])
async def get_current_usage(
    tenant_id: str,
    current_user: TokenPayload = Depends(get_current_user)
):
    """Get current month's query count."""
    await require_tenant_access(tenant_id, current_user)
    
    tenant = tenant_service.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    count = metering_service.get_current_month_count(tenant_id)
    limit = tenant.limits.max_queries_per_month
    
    return {
        "tenant_id": tenant_id,
        "queries_used": count,
        "queries_limit": limit,
        "queries_remaining": max(0, limit - count)
    }
