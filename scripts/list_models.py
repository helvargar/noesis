import google.generativeai as genai
import os
import sys

# Add the parent directory to the path
sys.path.insert(0, '/Users/administrator/Workspaces/noesis')
from app.services.tenant_service import TenantService

tenant_service = TenantService()
api_key = tenant_service.get_decrypted_llm_key("tenant_b4b6daaa")
genai.configure(api_key=api_key)

for m in genai.list_models():
    if 'generateContent' in m.supported_generation_methods:
        print(m.name)
