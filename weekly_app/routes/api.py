"""JSON-only API routes for the React frontend.

Kept separate from the existing HTML/Jinja routes so the original site
stays untouched during migration. The React app calls these; the legacy
Jinja pages still work for any modules not yet ported.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from weekly_app.core import auth_users, security

router = APIRouter(prefix="/api")


class LoginBody(BaseModel):
    email: str
    password: str


@router.post("/login")
def api_login(request: Request, body: LoginBody):
    user = auth_users.find_user(body.email)
    if not user or not security.verify_password(body.password, user.get("password_hash", "")):
        return JSONResponse({"error": "Invalid email or password."}, status_code=401)
    request.session["user_email"] = user["email"]
    return {"email": user["email"]}


@router.get("/me")
def api_me(request: Request):
    email = request.session.get("user_email")
    if not email:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    return {"email": email}


@router.post("/logout")
def api_logout(request: Request):
    request.session.clear()
    return {"ok": True}
