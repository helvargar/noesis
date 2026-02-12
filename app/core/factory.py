from typing import Literal, Optional
from llama_index.llms.openai import OpenAI
from llama_index.llms.anthropic import Anthropic
from llama_index.core.llms import LLM
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core.embeddings import BaseEmbedding

class LLMFactory:
    @staticmethod
    def create_llm(provider: str, api_key: str, model_name: Optional[str] = None) -> LLM:
        print(f"[DEBUG] Factory creating LLM: {provider} - Model: {model_name}")
        """
        Instantiates a LlamaIndex LLM object based on tenant configuration.
        """
        if provider == "openai":
            return OpenAI(
                model=model_name or "gpt-4o",
                api_key=api_key,
                max_tokens=4096
            )
        elif provider == "anthropic":
            return Anthropic(
                model=model_name or "claude-3-5-sonnet-20240620",
                api_key=api_key
            )
        elif provider == "groq":
            from llama_index.llms.groq import Groq
            # 8b-instant has much higher TPM limits on free tier than 70b
            return Groq(
                model=model_name or "llama-3.1-8b-instant",
                api_key=api_key,
                max_tokens=2048
            )
        elif provider == "gemini":
            from llama_index.llms.google_genai import GoogleGenAI
            
            # FEB 2026 Retirement Mapping: Mapping dead models to active ones
            model_map = {
                "gemini-1.5-flash": "gemini-1.5-flash",
                "gemini-1.5-pro": "gemini-1.5-pro",
                "gemini-2.0-flash": "gemini-2.0-flash",
                "gemini-2.0-pro": "gemini-2.0-pro-exp-0205",
            }
            
            requested = model_name or "gemini-1.5-flash"
            # Clean possible models/ prefix for mapping
            clean_name = requested.replace("models/", "")
            
            target_model = model_map.get(clean_name, clean_name)
            
            if not target_model.startswith("models/"):
                target_model = f"models/{target_model}"
            
            print(f"[DEBUG] Gemini model mapping: {requested} -> {target_model}")
            return GoogleGenAI(
                model=target_model,
                api_key=api_key,
                transport="rest",
                max_tokens=8192,
            )
        elif provider == "ollama":
            from llama_index.llms.ollama import Ollama
            # base_url is usually http://localhost:11434
            # In this case api_key can be used as the base_url
            url = api_key if api_key.startswith('http') else "http://localhost:11434"
            return Ollama(
                model=model_name or "llama3",
                base_url=url,
                request_timeout=60.0
            )
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")

class EmbedModelFactory:
    @staticmethod
    def create_embed_model(provider: str, api_key: str) -> BaseEmbedding:
        """
        Instantiates a LlamaIndex Embedding model.
        Optimization: We use local embeddings by default for schema reflection 
        to drastically reduce latency and avoid cloud API failures/costs.
        """
        try:
            from llama_index.embeddings.huggingface import HuggingFaceEmbedding
            # This is fast, local, and perfect for table names/schema
            return HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")
        except Exception as e:
            print(f"[WARN] Local embeddings failed, falling back to cloud: {e}")
            if provider == "openai":
                return OpenAIEmbedding(api_key=api_key)
            elif provider == "gemini":
                from llama_index.embeddings.google_genai import GoogleGenAIEmbedding
                return GoogleGenAIEmbedding(
                    model_name="models/text-embedding-004", 
                    api_key=api_key
                )
            return None
