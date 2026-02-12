"""
Authentication Service - JWT-based auth with role management.
"""
import json
import os
import uuid
from datetime import datetime, timedelta
from typing import Optional
from passlib.context import CryptContext
import jwt

from app.models.user import User, UserCreate, Token, TokenPayload
from app.core.config import settings

# Password hashing (using sha256_crypt for PoC compatibility; use bcrypt in prod with proper setup)
pwd_context = CryptContext(schemes=["sha256_crypt"], deprecated="auto")

# User store (JSON for PoC, Postgres in prod)
USER_STORE_PATH = "./data/users.json"

# JWT Settings
JWT_SECRET = settings.JWT_SECRET
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24


class AuthService:
    """Handles user authentication and JWT management."""
    
    def __init__(self):
        self._users: dict[str, User] = {}
        self._load_from_disk()
        self._ensure_admin_exists()
    
    def _load_from_disk(self):
        if os.path.exists(USER_STORE_PATH):
            try:
                with open(USER_STORE_PATH, "r") as f:
                    data = json.load(f)
                    for uid, udata in data.items():
                        self._users[uid] = User.model_validate(udata)
            except Exception as e:
                print(f"Warning: Could not load users: {e}")
    
    def _save_to_disk(self):
        os.makedirs(os.path.dirname(USER_STORE_PATH), exist_ok=True)
        with open(USER_STORE_PATH, "w") as f:
            data = {uid: u.model_dump(mode="json") for uid, u in self._users.items()}
            json.dump(data, f, indent=2, default=str)
    
    def _ensure_admin_exists(self):
        """Create default admin if none exists."""
        admins = [u for u in self._users.values() if u.role == "admin"]
        if not admins:
            self.create_user(UserCreate(
                email="admin@noesis.ai",
                password="GeiAdmin01",
                role="admin"
            ))
            print("⚠️  Default admin created: admin@noesis.ai / GeiAdmin01")
    
    def _hash_password(self, password: str) -> str:
        return pwd_context.hash(password)
    
    def _verify_password(self, plain: str, hashed: str) -> bool:
        return pwd_context.verify(plain, hashed)
    
    def create_user(self, request: UserCreate) -> User:
        """Create a new user."""
        # Check if email already exists
        if any(u.email == request.email for u in self._users.values()):
            raise ValueError("Email already registered")
        
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        user = User(
            id=user_id,
            email=request.email,
            hashed_password=self._hash_password(request.password),
            role=request.role,
            tenant_id=request.tenant_id
        )
        
        self._users[user_id] = user
        self._save_to_disk()
        return user
    
    def authenticate(self, email: str, password: str) -> Optional[User]:
        """Verify credentials and return user if valid."""
        for user in self._users.values():
            if user.email == email and user.is_active:
                if self._verify_password(password, user.hashed_password):
                    return user
        return None
    
    def create_token(self, user: User) -> Token:
        """Generate JWT token for authenticated user."""
        expires = datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
        
        payload = {
            "sub": user.id,
            "email": user.email,
            "role": user.role,
            "tenant_id": user.tenant_id,
            "exp": int(expires.timestamp())
        }
        
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
        
        return Token(
            access_token=token,
            expires_in=JWT_EXPIRATION_HOURS * 3600
        )
    
    def verify_token(self, token: str) -> Optional[TokenPayload]:
        """Decode and validate JWT token."""
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            return TokenPayload(**payload)
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None
    
    def get_user(self, user_id: str) -> Optional[User]:
        return self._users.get(user_id)
    
    def get_user_by_email(self, email: str) -> Optional[User]:
        for user in self._users.values():
            if user.email == email:
                return user
        return None


# Singleton
auth_service = AuthService()
