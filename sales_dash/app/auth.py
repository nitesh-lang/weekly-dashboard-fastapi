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


# ── Same-domain SSO from the Weekly dashboard ──────────────────────────────
# Both apps run on one domain and deliberately share SESSION_SECRET by value
# (see repo-root CLAUDE.md boundary #4), so the host Weekly dashboard's
# session cookie can be VERIFIED here without importing anything from
# weekly_app.  A valid weekly login whose email also exists (active) in this
# app's users table is adopted as a sales login — no second sign-in.  Sales
# roles/grants still come from THIS app's users table; a weekly login never
# raises anyone's sales role, and emails absent or deactivated here still
# see the sales login page.
WEEKLY_SSO_COOKIE = "weekly_session"
WEEKLY_SSO_MAX_AGE = 14 * 24 * 60 * 60  # must not exceed weekly's own max_age


def _weekly_sso_email(request: Request) -> str | None:
    cookie = request.cookies.get(WEEKLY_SSO_COOKIE)
    if not cookie:
        return None
    try:
        import base64
        import json

        import itsdangerous

        signer = itsdangerous.TimestampSigner(SESSION_SECRET)
        data = signer.unsign(cookie.encode("utf-8"), max_age=WEEKLY_SSO_MAX_AGE)
        payload = json.loads(base64.b64decode(data))
        email = str(payload.get("user_email") or "").strip().lower()
        return email or None
    except Exception:
        # Bad/expired/foreign cookie → not an error, just not an SSO login.
        return None


def current_user(request: Request) -> dict | None:
    email = current_user_email(request)
    sso_email = _weekly_sso_email(request)

    # Weekly is MASTER: if the weekly login and the sales session disagree,
    # the sales session is stale — typically the old shared info@ cookie from
    # the pre-SSO era still sitting in a teammate's browser (seen live
    # 2026-09-01: Hazique logged into Weekly, sales showed info@).  Drop it
    # and resolve from the weekly identity instead.  The one exception is a
    # session created by an explicit POST /login in THIS app (marked
    # "manual_login"): that is a deliberate act and stays honored, so the
    # sales login page still works as a fallback for people who need it.
    if sso_email and email and email.strip().lower() != sso_email:
        try:
            if not request.session.get("manual_login"):
                request.session.pop("user", None)
                email = None
        except AssertionError:
            email = None

    def _sso_shape(u: dict) -> dict:
        # An SSO session authenticates via the WEEKLY login; the sales
        # bootstrap password is irrelevant to it, so never force the
        # password-reset screen (which demands a current password the
        # teammate may not have — Hazique, 2026-09-02).  Manual sales
        # logins keep the forced reset: they DID use the sales password.
        try:
            manual = bool(request.session.get("manual_login"))
        except AssertionError:
            manual = False
        if not manual and sso_email and u["email"].strip().lower() == sso_email:
            return {**u, "must_reset_password": False}
        return u

    if email:
        u = get_user_by_email(email)
        return _sso_shape(u) if u else None

    if not sso_email:
        return None
    u = get_user_by_email(sso_email)
    if not u or not u["is_active"]:
        return None
    try:
        # Persist the adoption so later requests are a native sales session.
        request.session["user"] = u["email"]
        request.session.pop("manual_login", None)
    except AssertionError:
        pass
    return _sso_shape(u)


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
