"""Admin-only user management. Mirrors OrderPilot's Users tab."""
from __future__ import annotations

from typing import Literal

import bcrypt
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text

from ..auth import require_admin
from ..database import engine
from ..users_seed import DEFAULT_PASSWORD

router = APIRouter()


def _shape(row) -> dict:
    d = dict(row)
    d.pop("password_hash", None)
    if d.get("created_at"):
        d["created_at"] = d["created_at"].isoformat()
    return d


class CreateIn(BaseModel):
    email: str
    full_name: str | None = None
    role: Literal["admin", "viewer"] = "viewer"
    password: str | None = None  # optional; falls back to DEFAULT_PASSWORD


class PatchIn(BaseModel):
    role: Literal["admin", "viewer"] | None = None
    is_active: bool | None = None
    full_name: str | None = None


def _admin_count(conn) -> int:
    return conn.execute(
        text("SELECT COUNT(*) FROM users WHERE role = 'admin' AND is_active = TRUE")
    ).scalar_one()


@router.get("/users")
def list_users(request: Request):
    require_admin(request)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT id, email, full_name, role, is_active, must_reset_password, "
                "created_at FROM users ORDER BY role DESC, email"
            )
        ).mappings().all()
    return {"users": [_shape(r) for r in rows]}


@router.post("/users")
def create_user(payload: CreateIn, request: Request):
    require_admin(request)
    email = payload.email.strip().lower()
    if "@" not in email:
        raise HTTPException(400, {"error": {"code": "bad_email"}})
    pw = payload.password or DEFAULT_PASSWORD
    pw_hash = bcrypt.hashpw(pw.encode(), bcrypt.gensalt())
    with engine.begin() as conn:
        exists = conn.execute(text("SELECT 1 FROM users WHERE email = :e"), {"e": email}).first()
        if exists:
            raise HTTPException(409, {"error": {"code": "email_exists"}})
        conn.execute(
            text(
                "INSERT INTO users (email, full_name, password_hash, role, is_active, must_reset_password) "
                "VALUES (:e, :n, :h, :r, TRUE, TRUE)"
            ),
            {"e": email, "n": payload.full_name or None, "h": pw_hash, "r": payload.role},
        )
    return {"ok": True}


@router.patch("/users/{user_id}")
def patch_user(user_id: int, payload: PatchIn, request: Request):
    admin = require_admin(request)
    with engine.begin() as conn:
        u = conn.execute(text("SELECT * FROM users WHERE id = :i"), {"i": user_id}).mappings().first()
        if not u:
            raise HTTPException(404, {"error": {"code": "not_found"}})

        # Safety rails on demoting / deactivating the last admin (or self as last admin)
        will_role = payload.role or u["role"]
        will_active = payload.is_active if payload.is_active is not None else u["is_active"]
        if u["role"] == "admin" and u["is_active"] and (will_role != "admin" or not will_active):
            if _admin_count(conn) <= 1:
                raise HTTPException(400, {"error": {"code": "last_admin", "message": "Cannot demote/deactivate the last active admin"}})

        sets, params = [], {"i": user_id}
        if payload.role is not None:
            sets.append("role = :r")
            params["r"] = payload.role
        if payload.is_active is not None:
            sets.append("is_active = :a")
            params["a"] = payload.is_active
        if payload.full_name is not None:
            sets.append("full_name = :n")
            params["n"] = payload.full_name
        if sets:
            conn.execute(text(f"UPDATE users SET {', '.join(sets)} WHERE id = :i"), params)
    return {"ok": True}


@router.post("/users/{user_id}/reset-password")
def admin_reset_password(user_id: int, request: Request):
    require_admin(request)
    with engine.begin() as conn:
        u = conn.execute(text("SELECT email FROM users WHERE id = :i"), {"i": user_id}).mappings().first()
        if not u:
            raise HTTPException(404, {"error": {"code": "not_found"}})
        h = bcrypt.hashpw(DEFAULT_PASSWORD.encode(), bcrypt.gensalt())
        conn.execute(
            text("UPDATE users SET password_hash = :h, must_reset_password = TRUE WHERE id = :i"),
            {"h": h, "i": user_id},
        )
    return {"ok": True, "temporary_password": DEFAULT_PASSWORD}


@router.delete("/users/{user_id}")
def delete_user(user_id: int, request: Request):
    admin = require_admin(request)
    with engine.begin() as conn:
        u = conn.execute(text("SELECT * FROM users WHERE id = :i"), {"i": user_id}).mappings().first()
        if not u:
            raise HTTPException(404, {"error": {"code": "not_found"}})
        if u["email"] == admin["email"]:
            raise HTTPException(400, {"error": {"code": "cannot_delete_self"}})
        if u["role"] == "admin" and u["is_active"] and _admin_count(conn) <= 1:
            raise HTTPException(400, {"error": {"code": "last_admin"}})
        conn.execute(text("DELETE FROM users WHERE id = :i"), {"i": user_id})
    return {"ok": True}
