"""Session-cookie auth against the `users` table. Admin/viewer roles."""
from __future__ import annotations

import os
import secrets

import bcrypt
from fastapi import HTTPException, Request
from sqlalchemy import text

from .database import engine

SESSION_SECRET = os.getenv("SESSION_SECRET")
if not SESSION_SECRET:
    SESSION_SECRET = secrets.token_urlsafe(48)
    print(
        "⚠️  SESSION_SECRET not set — using ephemeral per-process key. "
        "Set it on Render to keep sessions stable across restarts."
    )


def get_user_by_email(email: str) -> dict | None:
    e = (email or "").strip().lower()
    with engine.begin() as conn:
        r = conn.execute(
            text(
                "SELECT id, email, full_name, password_hash, role, is_active, "
                "must_reset_password FROM users WHERE email = :e"
            ),
            {"e": e},
        ).mappings().first()
    return dict(r) if r else None


def verify_login(email: str, password: str) -> dict | None:
    u = get_user_by_email(email)
    if not u or not u["is_active"]:
        return None
    ph = u["password_hash"]
    if isinstance(ph, memoryview):
        ph = bytes(ph)
    try:
        if not bcrypt.checkpw(password.encode(), ph):
            return None
    except Exception:
        return None
    return u


def set_password(email: str, new_password: str) -> None:
    h = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt())
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE users SET password_hash = :h, must_reset_password = FALSE "
                "WHERE email = :e"
            ),
            {"h": h, "e": email.strip().lower()},
        )


def current_user_email(request: Request) -> str | None:
    try:
        return request.session.get("user")
    except AssertionError:
        return None


def current_user(request: Request) -> dict | None:
    email = current_user_email(request)
    if not email:
        return None
    return get_user_by_email(email)


def require_user(request: Request) -> dict:
    u = current_user(request)
    if not u:
        raise HTTPException(status_code=401, detail={"error": {"code": "unauthorized", "message": "Login required"}})
    return u


def require_admin(request: Request) -> dict:
    u = require_user(request)
    if u["role"] != "admin":
        raise HTTPException(status_code=403, detail={"error": {"code": "forbidden", "message": "Admin only"}})
    return u


# Back-compat name kept for any router still importing the old symbol
def check_password(email: str, password: str) -> bool:
    return verify_login(email, password) is not None
