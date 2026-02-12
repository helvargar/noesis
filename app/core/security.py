from cryptography.fernet import Fernet
from app.core.config import settings

# In a real scenario, manage this key securely (KMS, Vault)
# For PoC, we use the env var or default
_cipher = Fernet(settings.MASTER_KEY_FERNET.encode())

def encrypt_key(plain_key: str) -> str:
    """Encrypts a tenant's LLM API key for storage."""
    return _cipher.encrypt(plain_key.encode()).decode()

def decrypt_key(encrypted_key: str) -> str:
    """Decrypts the API key for runtime use."""
    return _cipher.decrypt(encrypted_key.encode()).decode()
