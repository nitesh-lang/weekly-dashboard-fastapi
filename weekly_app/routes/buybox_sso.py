"""Buybox single sign-on.

The Buybox report is a static build whose datasets are AES-encrypted with
BUYBOX_PASSWORD (client-side PBKDF2 unlock).  This endpoint hands the
credentials to an ALREADY-AUTHENTICATED weekly user so the buybox page can
auto-unlock without a second login.  The Weekly AuthGuardMiddleware 401s
every /api/* request without a weekly session, so anonymous callers never
reach this handler — the in-handler check is belt and braces.

The password is read from the runtime env (the same var the Render build
uses to encrypt the datasets) and is never cached or written anywhere.
"""
from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/api/buybox-sso")
def buybox_sso(request: Request):
    session = getattr(request, "session", None) or {}
    if not session.get("user_email"):
        raise HTTPException(401, "Login required")
    password = os.environ.get("BUYBOX_PASSWORD", "").strip()
    if not password:
        raise HTTPException(503, "BUYBOX_PASSWORD is not configured on the server")
    resp = JSONResponse({
        "username": os.environ.get("BUYBOX_USERNAME", "info@cambiumretail.com"),
        "password": password,
    })
    resp.headers["Cache-Control"] = "no-store"
    return resp
