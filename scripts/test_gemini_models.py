
import asyncio
import os
from google import genai
from app.services.tenant_service import tenant_service
from app.core.security import decrypt_key

async def test_models():
    # Load tenant to get key
    tenant_id = "tenant_b4b6daaa"
    tenant = tenant_service.get_tenant(tenant_id)
    api_key = decrypt_key(tenant.llm.api_key_encrypted)
    
    client = genai.Client(api_key=api_key)
    
    models_to_test = [
        "gemini-1.5-flash",
        "gemini-1.5-flash-001",
        "gemini-1.5-flash-002",
        "gemini-1.5-flash-8b",
        "models/gemini-1.5-flash"
    ]
    
    print("Testing Model Availability...")
    for m in models_to_test:
        try:
            print(f"Testing {m}...", end=" ")
            # Just try to get model info
            response = client.models.get(model=m)
            print("OK!")
        except Exception as e:
            print(f"FAILED: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_models())
