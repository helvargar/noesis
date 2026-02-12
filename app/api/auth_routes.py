"""
Auth Routes - Login, registration, and user management.
"""
from fastapi import APIRouter, HTTPException, Depends
from typing import List

from app.models.user import UserCreate, UserLogin, Token, TokenPayload
from app.services.auth_service import auth_service
from app.api.dependencies import get_current_user, require_admin
from pydantic import BaseModel

router = APIRouter(prefix="/auth", tags=["Authentication"])


class UserPublicView(BaseModel):
    """Safe view of user (no password)."""
    id: str
    email: str
    role: str
    tenant_id: str | None
    is_active: bool


@router.post("/login", response_model=Token)
def login(request: UserLogin):
    """
    Authenticate user and return JWT token.
    """
    user = auth_service.authenticate(request.email, request.password)
    
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Invalid email or password"
        )
    
    return auth_service.create_token(user)


@router.post("/register", response_model=UserPublicView)
def register_user(
    request: UserCreate,
    current_user: TokenPayload = Depends(require_admin)
):
    """
    Register a new user (Admin only).
    Tenant users must be assigned to a tenant.
    """
    if request.role == "tenant_user" and not request.tenant_id:
        raise HTTPException(
            status_code=400,
            detail="tenant_user must have a tenant_id"
        )
    
    try:
        user = auth_service.create_user(request)
        return UserPublicView(
            id=user.id,
            email=user.email,
            role=user.role,
            tenant_id=user.tenant_id,
            is_active=user.is_active
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/me", response_model=UserPublicView)
def get_current_user_info(current_user: TokenPayload = Depends(get_current_user)):
    """
    Get current authenticated user's info.
    """
    user = auth_service.get_user(current_user.sub)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return UserPublicView(
        id=user.id,
        email=user.email,
        role=user.role,
        tenant_id=user.tenant_id,
        is_active=user.is_active
    )


@router.post("/refresh", response_model=Token)
def refresh_token(current_user: TokenPayload = Depends(get_current_user)):
    """
    Refresh JWT token (extend expiration).
    """
    user = auth_service.get_user(current_user.sub)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return auth_service.create_token(user)
