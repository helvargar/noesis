import os
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader, StorageContext
from app.core.factory import LLMFactory
from app.core.security import decrypt_key

# Mock config retrieval for ingestion (similar to routes.py)
# In prod, this would be an async worker task
def build_index_for_tenant(tenant_id: str, source_dir: str, output_dir: str, api_key_enc: str, provider: str):
    """
    Reads documents from source_dir, creates embeddings using the tenant's LLM/Embed model,
    and persists to output_dir.
    """
    if not os.path.exists(source_dir):
        print(f"Source directory {source_dir} does not exist.")
        return

    # 1. Setup Tenant LLM context 
    real_key = decrypt_key(api_key_enc)
    from app.core.factory import EmbedModelFactory
    from llama_index.core import Settings
    
    embed_model = EmbedModelFactory.create_embed_model(provider, real_key)
    llm = LLMFactory.create_llm(provider, real_key)
    
    Settings.llm = llm
    Settings.embed_model = embed_model
    
    # 2. Load Data
    documents = SimpleDirectoryReader(source_dir).load_data()
    print(f"Loaded {len(documents)} documents for {tenant_id}")
    
    # 3. Create Index
    index = VectorStoreIndex.from_documents(
        documents,
        embed_model=embed_model,
        llm=llm
    )

    # 4. Save to Disk
    index.storage_context.persist(persist_dir=output_dir)
    print(f"Index saved to {output_dir}")

if __name__ == "__main__":
    # Example usage for PoC setup
    # Make sure to set the encrypted key correctly
    # build_index_for_tenant("tenant_1", "./data/raw", "./data/tenant_1_index", "ENCRYPTED_KEY_HERE", "openai")
    pass
