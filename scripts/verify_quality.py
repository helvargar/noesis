
import asyncio
import os
from app.services.tenant_service import tenant_service
from app.engine.query import TenantQueryPipeline
from app.core.security import decrypt_key

async def run_test():
    tenant_id = "tenant_b4b6daaa"
    tenant = tenant_service.get_tenant(tenant_id)
    api_key = decrypt_key(tenant.llm.api_key_encrypted)
    db_uri = tenant_service.get_db_connection_string(tenant_id)
    
    pipeline = TenantQueryPipeline(
        tenant_id=tenant_id,
        llm_provider=tenant.llm.provider,
        llm_api_key=api_key,
        llm_model=tenant.llm.model_name,
        sql_connection_str=db_uri,
        schema_name=tenant.database.schema_name,
        allowed_tables=["*"],
        doc_store_path=None
    )
    
    # Test 1: Formatting of lists
    print("\n--- TEST 1: LIST FORMATTING ---")
    res1 = await pipeline.query("che opere ci sono?")
    print(f"Answer:\n{res1['answer']}")
    
    # Test 2: Detail for specific entities
    print("\n--- TEST 2: DETAIL (Pisana) ---")
    res2 = await pipeline.query("parlami della pisana")
    print(f"Answer:\n{res2['answer']}")

if __name__ == "__main__":
    asyncio.run(run_test())
