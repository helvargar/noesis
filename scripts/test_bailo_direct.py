#!/usr/bin/env python3
"""
Direct test of the query pipeline to debug Bailo issue.
"""
import asyncio
import os
import sys

# Add the parent directory to the path
sys.path.insert(0, '/Users/administrator/Workspaces/noesis')

from app.engine.query import TenantQueryPipeline
from app.services.tenant_service import TenantService

tenant_service = TenantService()

async def test_bailo():
    tenant_id = "tenant_b4b6daaa"
    tenant = tenant_service.get_tenant(tenant_id)
    
    api_key = tenant_service.get_decrypted_llm_key(tenant_id)
    db_uri = tenant_service.get_db_connection_string(tenant_id)
    
    print(f"Testing with DB URI: {db_uri[:50]}...")
    print(f"LLM: {tenant.llm.provider} / {tenant.llm.model_name}")
    
    pipeline = TenantQueryPipeline(
        tenant_id=tenant_id,
        llm_provider=tenant.llm.provider,
        llm_api_key=api_key,
        llm_model=tenant.llm.model_name,
        sql_connection_str=db_uri,
        schema_name=tenant.database.schema_name,
        allowed_tables=tenant.database.allowed_tables,
        doc_store_path=None
    )
    
    # Test query
    queries = [
        "Che opere ci sono nel museo Bailo?",
        "Elenca le opere al Bailo",
        "SELECT artistworktitle FROM artistwork WHERE siteid = 1 LIMIT 5"
    ]
    
    for q in queries:
        print(f"\n{'='*60}")
        print(f"QUERY: {q}")
        print(f"{'='*60}")
        result = await pipeline.query(q)
        print(f"ANSWER: {result['answer'][:500]}")
        print(f"SOURCE: {result['source_type']}")

if __name__ == "__main__":
    asyncio.run(test_bailo())
