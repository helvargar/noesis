"""
API Dependencies - Authentication and authorization middleware.
"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional

from app.services.auth_service import auth_service
from app.models.user import TokenPayload

# Security scheme
security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> TokenPayload:
    """
    Dependency to extract and validate JWT from Authorization header.
    Returns the token payload with user info.
    """
    token = credentials.credentials
    payload = auth_service.verify_token(token)
    
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    return payload


async def require_admin(
    current_user: TokenPayload = Depends(get_current_user)
) -> TokenPayload:
    """
    Dependency for admin-only endpoints.
    """
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user


async def require_tenant_access(
    tenant_id: str,
    current_user: TokenPayload = Depends(get_current_user)
) -> TokenPayload:
    """
    Dependency to verify user has access to specific tenant.
    Admins can access any tenant; tenant_users only their own.
    """
    if current_user.role == "admin":
        return current_user
    
    if current_user.tenant_id != tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to this tenant"
        )
    
    return current_user


def get_tenant_id_for_user(current_user: TokenPayload, requested_tenant_id: Optional[str] = None) -> str:
    """
    Helper to determine which tenant_id to use.
    Admins must specify; tenant_users use their assigned tenant.
    """
    if current_user.role == "admin":
        if not requested_tenant_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Admin must specify tenant_id"
            )
        return requested_tenant_id
    
    # tenant_user uses their own tenant
    if current_user.tenant_id:
        return current_user.tenant_id
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="User not assigned to any tenant"
    )
