from pydantic import BaseModel, EmailStr
from typing import Optional, Literal
from datetime import datetime

class User(BaseModel):
    """User model for authentication."""
    id: str
    email: EmailStr
    hashed_password: str
    role: Literal["admin", "tenant_user"] = "tenant_user"
    tenant_id: Optional[str] = None  # Linked tenant for tenant_user role
    is_active: bool = True
    created_at: datetime = datetime.utcnow()

class UserCreate(BaseModel):
    """Request to create a new user."""
    email: EmailStr
    password: str
    role: Literal["admin", "tenant_user"] = "tenant_user"
    tenant_id: Optional[str] = None

class UserLogin(BaseModel):
    """Login request."""
    email: EmailStr
    password: str

class Token(BaseModel):
    """JWT token response."""
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds

class TokenPayload(BaseModel):
    """JWT payload structure."""
    sub: str  # user_id
    email: str
    role: str
    tenant_id: Optional[str] = None
    exp: int  # expiration timestamp
