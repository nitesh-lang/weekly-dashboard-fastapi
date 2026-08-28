"""Sales Dashboard (Multi-brand) — FastAPI backend.

One FastAPI. Per-brand business logic lives in app/services/{nexlev,audio_array}.py
(ported verbatim from the standalone repos). Every DB row carries a `brand`
column so a single Neon Postgres serves both.

Endpoints (JSON, consumed by the Vite frontend):
  POST   /api/auth/login
  POST   /api/auth/logout
  GET    /api/auth/me
  GET    /api/brands
  GET    /api/{brand}/dashboard     — full render context (KPIs, day/week/month,
                                       target-vs-actual, category, donut, model
                                       trend, validation, monthwise chart)
  POST   /api/{brand}/upload         — Excel upload fallback (multi-account)
  POST   /api/{brand}/sync-sales     — internal push (token) — bulk row ingest
  POST   /api/{brand}/pull-sales     — pull today (or ?date=YYYY-MM-DD) from
                                       every SP-API account mapped to the brand
  GET    /api/{brand}/download-ledger.csv
  POST   /api/activity/track     — browser-side usage events (page, export,
                                       heartbeat) recorded against the user
  GET    /api/admin/usage        — per-user consumption report (admin only)
  GET    /healthz
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# .env lives next to this file (backend/.env) — load before anything else
# reads os.getenv (database.py, auth.py, brands.py all rely on it).
load_dotenv(Path(__file__).resolve().parent / ".env")

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.sessions import SessionMiddleware

from app.activity import bootstrap_activity
from app.auth import SESSION_SECRET
from app.routers import activity, auth, dashboard, ledger, sp_pull, users
from app.users_seed import seed_users

seed_users()
bootstrap_activity()

app = FastAPI(title="Sales Dashboard — Multi-brand", version="1.0.0")

# ── CORS ──
_cors_origins = [
    o.strip()
    for o in (os.getenv("CORS_ORIGINS") or "http://localhost:5173").split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Sessions ──
# Cross-origin cookie: Render puts /api and /web on separate subdomains, so
# the browser needs SameSite=None + Secure=true to send the session cookie
# on XHR from the static site to the API. Local dev keeps Lax.
_SESSION_SAMESITE = (os.getenv("SESSION_SAMESITE") or "lax").lower()
if _SESSION_SAMESITE not in ("lax", "strict", "none"):
    _SESSION_SAMESITE = "lax"
_SESSION_SECURE = os.getenv("SESSION_SECURE", "false").lower() == "true"
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    same_site=_SESSION_SAMESITE,
    https_only=_SESSION_SECURE,
    # VENDORED-COPY EDIT (the only one): this app now shares a domain with
    # the Weekly dashboard, whose SessionMiddleware owns the default
    # "session" cookie — without a distinct name the two apps overwrite
    # each other's logins on every request.
    session_cookie="sales_session",
)

# ── Routers ──
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(dashboard.router, prefix="/api", tags=["dashboard"])
app.include_router(ledger.router, prefix="/api", tags=["ledger"])
app.include_router(sp_pull.router, prefix="/api", tags=["sp-api"])
app.include_router(users.router, prefix="/api/admin", tags=["admin"])
app.include_router(activity.router, prefix="/api", tags=["activity"])


# ── Health ──
@app.get("/healthz")
@app.head("/healthz")
def healthz():
    return {"ok": True}


# ── Uniform error envelope ──
@app.exception_handler(RequestValidationError)
async def _validation_handler(_: Request, exc: RequestValidationError):
    return JSONResponse(
        {"error": {"code": "validation_error", "message": "Invalid request", "detail": exc.errors()}},
        status_code=422,
    )


@app.exception_handler(Exception)
async def _unhandled(_: Request, exc: Exception):
    return JSONResponse(
        {"error": {"code": "internal_error", "message": str(exc)}},
        status_code=500,
    )
