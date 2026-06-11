"""Admin-only user management endpoints.

All routes here are gated by `require_admin` — non-admin sessions get a
403 even though they're logged in.  AuthGuardMiddleware already handles
the 401-on-no-session case before requests reach this router.
"""
from __future__ import annotations

from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from weekly_app.core import auth_users, security

router = APIRouter(prefix="/api/admin", tags=["admin"])


def _normalize_email(email: str) -> str:
    """Light-touch email shape check — exactly one '@', dot in domain.
    Avoids the optional pydantic[email] dependency."""
    e = (email or "").strip().lower()
    if e.count("@") != 1 or "." not in e.split("@", 1)[1] or " " in e:
        raise HTTPException(400, "Invalid email format")
    return e


def require_admin(request: Request) -> str:
    """Dependency: returns the current admin's email, or 403s."""
    email = request.session.get("user_email")
    if not email:
        # Should be caught by AuthGuardMiddleware; defensive guard.
        raise HTTPException(status_code=401, detail="Not authenticated")
    if auth_users.get_role(email) != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return email


# ─── Schemas ─────────────────────────────────────────────────────────
class UserOut(BaseModel):
    email: str
    role: str
    tabs: List[str]
    created_at: Optional[str] = None
    password_updated_at: Optional[str] = None


class UserCreate(BaseModel):
    email:    str
    password: str
    role:     str = "operator"
    tabs:     List[str] = []


class UserPatch(BaseModel):
    role:     Optional[str]       = None
    tabs:     Optional[List[str]] = None
    password: Optional[str]       = None  # optional reset


# ─── Endpoints ───────────────────────────────────────────────────────
@router.get("/users", response_model=List[UserOut])
def list_users_endpoint(_admin: str = Depends(require_admin)):
    return auth_users.list_users()


@router.post("/users", response_model=UserOut)
def create_user_endpoint(body: UserCreate, _admin: str = Depends(require_admin)):
    if body.role not in auth_users.VALID_ROLES:
        raise HTTPException(400, f"Invalid role; must be one of {auth_users.VALID_ROLES}")
    invalid = [t for t in body.tabs if t not in auth_users.KNOWN_TABS]
    if invalid:
        raise HTTPException(400, f"Unknown tabs: {invalid}")
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    try:
        u = auth_users.create_user(
            email=_normalize_email(body.email),
            password_hash=security.hash_password(body.password),
            role=body.role,
            tabs=body.tabs,
        )
    except ValueError as e:
        raise HTTPException(409, str(e))
    return {
        "email": u["email"], "role": u["role"], "tabs": u.get("tabs", []),
        "created_at": u.get("created_at"),
        "password_updated_at": u.get("password_updated_at"),
    }


@router.patch("/users/{email}", response_model=UserOut)
def patch_user_endpoint(
    email: str,
    body: UserPatch,
    admin_email: str = Depends(require_admin),
):
    target = auth_users.find_user(email)
    if not target:
        raise HTTPException(404, "User not found")

    # Refuse to demote the last admin — would lock out user management.
    if (
        body.role is not None
        and body.role != "admin"
        and (target.get("role") or auth_users.DEFAULT_ROLE) == "admin"
        and auth_users.count_admins() <= 1
    ):
        raise HTTPException(400, "Cannot demote the last remaining admin")

    if body.tabs is not None:
        invalid = [t for t in body.tabs if t not in auth_users.KNOWN_TABS]
        if invalid:
            raise HTTPException(400, f"Unknown tabs: {invalid}")

    if body.role is not None or body.tabs is not None:
        try:
            auth_users.update_user(email, role=body.role, tabs=body.tabs)
        except ValueError as e:
            raise HTTPException(400, str(e))

    if body.password is not None:
        if len(body.password) < 8:
            raise HTTPException(400, "Password must be at least 8 characters")
        auth_users.update_password(email, security.hash_password(body.password))

    u = auth_users.find_user(email)
    return {
        "email": u["email"],
        "role":  u.get("role") or auth_users.DEFAULT_ROLE,
        "tabs":  list(u.get("tabs") or []),
        "created_at":          u.get("created_at"),
        "password_updated_at": u.get("password_updated_at"),
    }


@router.delete("/users/{email}")
def delete_user_endpoint(email: str, admin_email: str = Depends(require_admin)):
    target = auth_users.find_user(email)
    if not target:
        raise HTTPException(404, "User not found")
    if target["email"] == admin_email:
        raise HTTPException(400, "Cannot delete your own account")
    if (
        (target.get("role") or auth_users.DEFAULT_ROLE) == "admin"
        and auth_users.count_admins() <= 1
    ):
        raise HTTPException(400, "Cannot delete the last remaining admin")
    auth_users.delete_user(email)
    return {"ok": True, "deleted": email}


@router.get("/tabs")
def tabs_catalog(_admin: str = Depends(require_admin)):
    """Static catalog of assignable tabs (for the admin UI checkboxes)."""
    return {"tabs": list(auth_users.KNOWN_TABS), "roles": list(auth_users.VALID_ROLES)}
