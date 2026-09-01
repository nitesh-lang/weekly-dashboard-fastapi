from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from .. import activity
from ..auth import (
    current_user,
    require_user,
    set_password,
    verify_login,
)

router = APIRouter()


class LoginIn(BaseModel):
    email: str
    password: str
    # Browser-generated id tying this sign-in to the events that follow,
    # so time-in-tool can be measured per session.
    session_id: str | None = None


class ResetIn(BaseModel):
    current_password: str
    new_password: str


def _shape(u: dict | None) -> dict:
    if not u:
        return {"user": None}
    return {
        "user": {
            "email": u["email"],
            "full_name": u.get("full_name"),
            "role": u["role"],
            "must_reset_password": bool(u.get("must_reset_password")),
        }
    }


@router.get("/me")
def me(request: Request):
    return _shape(current_user(request))


@router.post("/login")
def login(payload: LoginIn, request: Request):
    u = verify_login(str(payload.email), payload.password)
    if not u:
        return {"ok": False, "error": "Invalid email or password."}
    request.session["user"] = u["email"]
    # Explicit sign-in beats the weekly-SSO takeover (see auth.current_user):
    # without this flag a stale-looking session would be dropped whenever the
    # weekly cookie names a different user.
    request.session["manual_login"] = True
    activity.log(u["email"], "login", session_id=payload.session_id,
                 detail={"role": u["role"]})
    return {"ok": True, **_shape(u)}


@router.post("/logout")
def logout(request: Request, session_id: str | None = None):
    email = request.session.get("user")
    activity.log(email, "logout", session_id=session_id)
    request.session.pop("user", None)
    request.session.pop("manual_login", None)
    return {"ok": True}


@router.post("/reset-password")
def reset_password(payload: ResetIn, request: Request):
    u = require_user(request)
    if verify_login(u["email"], payload.current_password) is None:
        return {"ok": False, "error": "Current password is incorrect."}
    if len(payload.new_password) < 8:
        return {"ok": False, "error": "New password must be at least 8 characters."}
    set_password(u["email"], payload.new_password)
    return {"ok": True}
