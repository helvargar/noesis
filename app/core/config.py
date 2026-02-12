from pydantic_settings import BaseSettings, SettingsConfigDict
import secrets

class Settings(BaseSettings):
    # App Config
    APP_NAME: str = "Noesis AI"
    DEBUG: bool = True
    
    # Master Key for encrypting Tenant API Keys (32-byte url-safe base64)
    MASTER_KEY_FERNET: str = "3q9M1_u5u8PR-XZ7k3z2Kq5v8PR-XZ7k3z2Kq5v8PR8=" 
    
    # JWT Settings (Fixed for PoC to maintain session across reloads)
    JWT_SECRET: str = "noesis_super_secret_dev_key_2024"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_HOURS: int = 24

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()
