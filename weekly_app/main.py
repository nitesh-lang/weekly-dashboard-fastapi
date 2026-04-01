from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import traceback
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
from fastapi.responses import HTMLResponse
from starlette.middleware.base import BaseHTTPMiddleware

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
app.mount("/static", StaticFiles(directory="weekly_app/static"), name="static")

templates = Jinja2Templates(directory="weekly_app/templates")

# =====================================================
# GLOBAL ERROR HANDLER (SHOW TRACEBACK)
# =====================================================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    print("❌ UNHANDLED EXCEPTION ❌")
    traceback.print_exc()

    return HTMLResponse(
        content=f"""
        <h2>Internal Server Error</h2>
        <pre>{traceback.format_exc()}</pre>
        """,
        status_code=500,
    )

# --------------------
# ROUTERS (PRIMARY)
# --------------------
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

# --------------------
# DEFAULT LANDING
# --------------------
@app.get("/")
def root():
    return RedirectResponse("/dashboard")

# =====================================================
# ✅ AMS ROOT ALIAS (🔥 FIX — ADDITIVE ONLY)
# =====================================================
@app.get("/ams-trend", include_in_schema=False)
def ams_trend_root_alias():
    """
    Root-level alias for AMS Trend UI.
    Keeps router prefix intact.
    """
    return RedirectResponse("/api/ams/view")


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
from weekly_app.etl.ams_model_snapshot import run_ams_model_etl
from weekly_app.etl.inventory_model_snapshot import run_inventory_etl

@app.on_event("startup")
async def auto_run_supporting_etl():
    """
    AUTO-RUN SUPPORTING ETL ON APP STARTUP

    ✔ AMS model snapshot
    ✔ Inventory model snapshot
    ✔ No UI dependency
    ✔ Safe to re-run
    ✅ FIXED: now runs in background thread so server is ready immediately
       Old version blocked startup — users got timeouts while ETL ran
    """

    async def run_in_background():
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor()

        try:
            print("🚀 AUTO ETL: Generating AMS model snapshot...")
            await loop.run_in_executor(executor, run_ams_model_etl)
            print("✅ AMS model snapshot ready")
        except Exception:
            print("❌ AMS MODEL ETL FAILED")
            traceback.print_exc()

        try:
            print("🚀 AUTO ETL: Generating Inventory model snapshot...")
            await loop.run_in_executor(executor, run_inventory_etl)
            print("✅ Inventory model snapshot ready")
        except Exception:
            print("❌ INVENTORY MODEL ETL FAILED")
            traceback.print_exc()

        try:
            print("🚀 AUTO ETL: Generating Sales snapshot...")
            await loop.run_in_executor(executor, run_sales_auto_etl)
            print("✅ Sales snapshot ready")
        except Exception:
            print("❌ SALES AUTO ETL FAILED")
            traceback.print_exc()
            
    # ✅ Fire and forget — server starts accepting requests immediately
    asyncio.create_task(run_in_background())