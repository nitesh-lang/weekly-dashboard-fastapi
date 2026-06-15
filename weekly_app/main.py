from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import traceback
import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

# --------------------
# ROUTE IMPORTS
# --------------------
from weekly_app.routes.ams_trend import router as ams_trend_router
from weekly_app.routes.ai_chat import router as ai_chat_router
from weekly_app.routes.upload import router as upload_router
from weekly_app.routes.dashboard import router as dashboard_router
from weekly_app.routes.exports import router as export_router
from weekly_app.routes.drilldown import router as drilldown_router
from weekly_app.routes.auth import router as auth_router
from weekly_app.routes.api import router as api_router
from weekly_app.routes.admin import router as admin_router
from weekly_app.routes.insights import router as insights_router
from weekly_app.routes.analytics import router as analytics_router
from weekly_app.routes.margin import router as margin_router
from weekly_app.routes.sync_status import router as sync_status_router
from weekly_app.routes.ams_planning import router as ams_planning_router
from weekly_app.routes.returns import router as returns_router
from weekly_app.routes.returns_overview import router as returns_overview_router
from fastapi.responses import HTMLResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from weekly_app.middleware.auth_guard import AuthGuardMiddleware

# ✅ SALES TREND ROUTER (SKU / MODEL)
from weekly_app.routes.sales_trend import router as sales_trend_router
from weekly_app.routes.AM_sales_trend import router as am_sales_trend_router

# ✅ CATEGORY SALES ROUTER (ALREADY USED BY DASHBOARD)
from weekly_app.routes.category_sales import router as category_sales_router

# ✅ INVENTORY DASHBOARD ROUTER (NEW)
from weekly_app.routes.inventory_dashboard import router as inventory_dashboard_router

# Optional / legacy viewers (UNCHANGED)
from weekly_app.routes.reconciliation_viewer import router as reco_router
from weekly_app.routes.channel_summary_viewer import router as channel_summary_router
from weekly_app.routes.viewer import router as sales_viewer_router

print("🔥🔥🔥 MAIN.PY LOADED — ROUTERS WILL BE MOUNTED 🔥🔥🔥")

# =====================================================
# GLOBAL IN-MEMORY CACHE
# =====================================================

_cache = {}

def get_cached(key):
    """Return cached data if it exists and hasn't expired. Otherwise None."""
    entry = _cache.get(key)
    if entry and datetime.now() < entry["expires"]:
        return entry["data"]
    return None

def set_cached(key, data, ttl_minutes=10):
    """Store data in cache with a TTL (default 10 minutes)."""
    _cache[key] = {
        "data": data,
        "expires": datetime.now() + timedelta(minutes=ttl_minutes)
    }

def clear_cache():
    """Clear all cached entries. Call this after an ETL run."""
    _cache.clear()
    print("🧹 Cache cleared")

# =====================================================
# APP (DEBUG DISABLED FOR PRODUCTION PERFORMANCE)
# =====================================================
app = FastAPI(
    title="Weekly Dashboard",
    debug=False,
)

import os
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware


# Cache-Control middleware — long-cache Vite's content-hashed assets so the
# browser / Render's CDN never re-fetches them.  Hashes change on every
# rebuild so cache invalidation is automatic.  SPA shell stays no-cache so
# the operator always gets the latest references.
class CacheHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        resp = await call_next(request)
        path = request.url.path
        if path.startswith("/static/spa/assets/"):
            resp.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        elif path == "/static/spa/index.html":
            resp.headers["Cache-Control"] = "no-cache, must-revalidate"
        return resp


# Compress JSON / HTML responses ≥ 1KB.  No-op for pre-compressed bytes
# (PNG/JPEG) and a big win for /api/* JSON.
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CacheHeadersMiddleware)

app.mount("/static", StaticFiles(directory="weekly_app/static"), name="static")

templates = Jinja2Templates(directory="weekly_app/templates")

# =====================================================
# AUTH MIDDLEWARE
# Order matters: middleware added LAST is OUTERMOST. SessionMiddleware
# must be outer so request.session is set up before AuthGuardMiddleware
# reads it.
# =====================================================
SESSION_SECRET = os.environ.get("SESSION_SECRET")
if not SESSION_SECRET:
    SESSION_SECRET = "dev-insecure-secret-do-not-use-in-prod"
    print("⚠ SESSION_SECRET not set — using insecure dev default. Set in env for production!")

app.add_middleware(AuthGuardMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="weekly_session",
    https_only=os.environ.get("COOKIE_SECURE", "0") == "1",
    same_site="lax",
    max_age=14 * 24 * 60 * 60,  # 14 days
)

# Auto-seed the bootstrap user on first boot. Idempotent — does nothing
# if the user already exists. Means a fresh Render deploy is usable
# immediately without needing shell access to run scripts/seed_user.py.
# The users file location respects the USERS_FILE env var, so when a
# persistent disk is mounted on Render the seed writes to the disk
# (e.g. /var/data/users.json) and survives subsequent deploys.
try:
    from weekly_app.core.auth_users import ensure_initial_user, USERS_FILE
    print(f"🔐 Users file: {USERS_FILE}")
    if ensure_initial_user():
        print("✅ Auto-seeded initial user")
except Exception as _e:
    print(f"⚠ Auto-seed failed: {_e}")

# =====================================================
# GLOBAL ERROR HANDLER
# Sanitized 500: full traceback goes to server logs only. The client sees a
# short reference id so support can correlate complaints with log entries.
# Set DEBUG_HTML_TRACE=1 in the env to inline the trace for local debugging.
# =====================================================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    ref = uuid.uuid4().hex[:8]
    print(f"❌ UNHANDLED EXCEPTION [ref={ref}] {request.method} {request.url.path}")
    traceback.print_exc()

    if os.environ.get("DEBUG_HTML_TRACE") == "1":
        body = (
            "<h2>Internal Server Error</h2>"
            f"<p>Reference: <code>{ref}</code></p>"
            f"<pre>{traceback.format_exc()}</pre>"
        )
    else:
        body = (
            "<h2>Internal Server Error</h2>"
            f"<p>Something went wrong. Reference: <code>{ref}</code></p>"
        )

    return HTMLResponse(content=body, status_code=500)

# --------------------
# ROUTERS (PRIMARY)
# --------------------
app.include_router(auth_router)
app.include_router(api_router)
app.include_router(admin_router)
app.include_router(insights_router)
app.include_router(upload_router)
app.include_router(dashboard_router)
app.include_router(export_router)
app.include_router(drilldown_router)

# ✅ SALES TREND ROUTER
app.include_router(sales_trend_router)
app.include_router(am_sales_trend_router)

# ✅ CATEGORY SALES ROUTER
app.include_router(category_sales_router)

# ✅ INVENTORY DASHBOARD ROUTER (NEW)
app.include_router(inventory_dashboard_router)

# ✅ AMS TREND ROUTER (NEW)
app.include_router(ams_trend_router)
app.include_router(ai_chat_router)

# ✅ ANALYTICS ROUTER (NEW — Claude-powered cross-module page)
app.include_router(analytics_router)

# ✅ MARGIN ROUTER — /api/margins JSON for the React frontend
app.include_router(margin_router)

# ✅ SYNC STATUS — /api/sync-status returns mtimes of processed snapshots
app.include_router(sync_status_router)

# ✅ AMS PLANNING — /api/ams-planning planning grid + editable notes
app.include_router(ams_planning_router)

# ✅ RETURNS — /api/returns FBA customer returns per ASIN
app.include_router(returns_router)
# ✅ RETURNS OVERVIEW — /api/returns-overview per-ASIN grid from master + returns + sales
app.include_router(returns_overview_router)

print("✅ upload_router mounted")
print("✅ dashboard_router mounted")
print("✅ export_router mounted")
print("✅ drilldown_router mounted")
print("✅ sales_trend_router mounted")
print("✅ category_sales_router mounted")
print("✅ inventory_dashboard_router mounted")
print("✅ ams_trend_router mounted")

# --------------------
# ROUTERS (LEGACY / SAFE)
# ✅ FIXED: removed duplicate inventory_dashboard_router mount that was here
# --------------------
app.include_router(reco_router)
app.include_router(channel_summary_router)
app.include_router(sales_viewer_router)

# --------------------
# DEFAULT LANDING
# --------------------
@app.get("/")
def root():
    return RedirectResponse("/dashboard")

# --------------------
# HEALTH CHECKS
# --------------------
@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/ping")
def ping():
    """
    ✅ Use this endpoint with UptimeRobot (free) to keep the server warm.
    Set UptimeRobot to ping every 5 minutes:
    https://weekly-dashboard-fastapi.onrender.com/ping
    This eliminates the 20-30 second cold start on Render's starter plan.
    """
    return {"status": "app running"}

# =====================================================
# 🔥 SAFE AUTO ETL TRIGGER (EXISTING – UNCHANGED)
# =====================================================
from weekly_app.etl.sales_auto_etl import run_sales_auto_etl

RAW_SALES_BASE = Path("data/raw/sales")

@app.get("/run-etl-latest")
def run_etl_latest():
    """
    SAFE MANUAL ETL TRIGGER

    ✔ Detects latest Week folder (Week 49, 50, 51…)
    ✔ Runs ETL once
    ✔ Writes to weekly_sales_snapshot.csv
    ✔ Does NOT modify dashboard logic
    ✔ Pure append-only addition
    """

    if not RAW_SALES_BASE.exists():
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "message": "data/raw/sales folder not found"
            }
        )

    week_folders = []

    for d in RAW_SALES_BASE.iterdir():
        if d.is_dir() and d.name.lower().startswith("week"):
            try:
                week_no = int("".join(filter(str.isdigit, d.name)))
                week_folders.append((week_no, d.name))
            except Exception:
                continue

    if not week_folders:
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "message": "No Week folders found inside data/raw/sales"
            }
        )

    latest_week = sorted(week_folders, key=lambda x: x[0])[-1][1]

    try:
        result = run_sales_auto_etl(latest_week)

        if result is None:
            return {
                "status": "skipped",
                "week": latest_week,
                "message": "ETL skipped (missing files or no valid data)"
            }

        # ✅ Clear cache after ETL so fresh data is served immediately
        clear_cache()

        return {
            "status": "success",
            "week": latest_week,
            "rows_written": int(len(result))
        }

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "week": latest_week,
                "message": str(e)
            }
        )

# =====================================================
# ✅ AUTO ETL – AMS & INVENTORY SNAPSHOTS
# ✅ FIXED: now async + runs in background so it does NOT block server startup
# =====================================================
from weekly_app.etl.inventory_model_snapshot import run_inventory_etl

@app.on_event("startup")
async def bootstrap_etl_if_needed():
    """
    Render-side ETL is intentionally disabled.

    The operator runs the full ETL pipeline locally
    (scripts/run_weekly_etl.py — six imperative scripts in order) and
    commits the produced data/processed/*.csv files to git.  Render
    receives them via normal deploy and serves them directly — no ETL
    work happens on the server.

    To force a server-side run anyway (e.g., emergency recovery), set
    FORCE_STARTUP_ETL=1 in the Render env.  This is OFF by default
    because the operator's local pipeline includes steps beyond what
    the importable etl.* modules cover (business_ads, step3-5), so
    running only a subset on the server can produce inconsistent data.
    """
    if os.environ.get("FORCE_STARTUP_ETL") != "1":
        print("✅ Render-side ETL disabled — server serves committed snapshots only")
        return

    print("⚠ FORCE_STARTUP_ETL=1 detected — running fallback ETL subset in background")
    print("⚠ Note: this only runs the importable subset (ams/inv/sales/ai),")
    print("⚠ not the full local pipeline.  Use only for emergency recovery.")

    async def run_in_background():
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor()
        for label, fn in [
            ("Inventory model", run_inventory_etl),
            ("Sales auto",      run_sales_auto_etl),
        ]:
            try:
                print(f"🚀 Fallback ETL: {label}…")
                await loop.run_in_executor(executor, fn)
                print(f"✅ {label} ready")
            except Exception:
                print(f"❌ {label} ETL FAILED")
                traceback.print_exc()
        try:
            from weekly_app.etl.build_ai_context import build_ai_context
            print("🤖 Building AI context JSON…")
            await loop.run_in_executor(executor, build_ai_context)
            print("✅ AI context ready")
        except Exception:
            print("❌ AI CONTEXT BUILD FAILED")
            traceback.print_exc()

    asyncio.create_task(run_in_background())


# =====================================================
# REACT SPA CATCH-ALL  (MUST BE LAST)
# Any GET that didn't match an /api/* route, a /static/* file, a health
# endpoint, or one of the legacy Jinja viewers falls through here and
# gets the SPA shell.  React Router takes over from there.
# =====================================================
from fastapi.responses import FileResponse, PlainTextResponse

SPA_INDEX = Path("weekly_app/static/spa/index.html")

@app.get("/{full_path:path}", include_in_schema=False)
async def spa_catchall(full_path: str, request: Request):
    # Never let the catch-all swallow API or static requests — those
    # should already match earlier routes, but this is defense-in-depth
    # in case the SPA bundle isn't built yet.
    if full_path.startswith("api/") or full_path.startswith("static/"):
        return PlainTextResponse("Not Found", status_code=404)

    if not SPA_INDEX.exists():
        return PlainTextResponse(
            "SPA bundle missing.  Run `npm --prefix frontend run build` first.",
            status_code=503,
        )
    # No-cache the shell so the operator always sees the latest <script src>
    # references — the bundled JS/CSS itself is hash-named and long-cached.
    return FileResponse(SPA_INDEX, headers={"Cache-Control": "no-cache, must-revalidate"})