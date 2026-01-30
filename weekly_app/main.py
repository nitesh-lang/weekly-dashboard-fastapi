from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import traceback
from pathlib import Path

# --------------------
# ROUTE IMPORTS
# --------------------
from weekly_app.routes.ams_trend import router as ams_trend_router
from weekly_app.routes.upload import router as upload_router
from weekly_app.routes.dashboard import router as dashboard_router
from weekly_app.routes.exports import router as export_router
from weekly_app.routes.drilldown import router as drilldown_router

# ✅ SALES TREND ROUTER (SKU / MODEL)
from weekly_app.routes.sales_trend import router as sales_trend_router

# ✅ CATEGORY SALES ROUTER (ALREADY USED BY DASHBOARD)
from weekly_app.routes.category_sales import router as category_sales_router

# ✅ INVENTORY DASHBOARD ROUTER (NEW)
from weekly_app.routes.inventory_dashboard import router as inventory_dashboard_router

# Optional / legacy viewers (UNCHANGED)
# from weekly_app.routes.viewer import router as sales_router
from weekly_app.routes.inventory_dashboard import router as inventory_dashboard_router
from weekly_app.routes.reconciliation_viewer import router as reco_router
from weekly_app.routes.channel_summary_viewer import router as channel_summary_router

print("🔥🔥🔥 MAIN.PY LOADED — ROUTERS WILL BE MOUNTED 🔥🔥🔥")

# =====================================================
# APP (DEBUG ENABLED)
# =====================================================
app = FastAPI(
    title="Weekly Dashboard",
    debug=True   # 🔥 SHOW FULL TRACEBACKS
)

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

# ✅ CATEGORY SALES ROUTER
app.include_router(category_sales_router)

# ✅ INVENTORY DASHBOARD ROUTER (NEW)
app.include_router(inventory_dashboard_router)

# ✅ AMS TREND ROUTER (NEW)
app.include_router(ams_trend_router)

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
# --------------------
# app.include_router(sales_router)
app.include_router(inventory_dashboard_router)
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
# ✅ AUTO ETL – AMS & INVENTORY SNAPSHOTS (UNCHANGED)
# =====================================================
from weekly_app.etl.ams_model_snapshot import run_ams_model_etl
from weekly_app.etl.inventory_model_snapshot import run_inventory_etl

@app.on_event("startup")
def auto_run_supporting_etl():
    """
    AUTO-RUN SUPPORTING ETL ON APP STARTUP

    ✔ AMS model snapshot
    ✔ Inventory model snapshot
    ✔ No UI dependency
    ✔ Safe to re-run
    """

    try:
        print("🚀 AUTO ETL: Generating AMS model snapshot...")
        run_ams_model_etl()
        print("✅ AMS model snapshot ready")

    except Exception:
        print("❌ AMS MODEL ETL FAILED")
        traceback.print_exc()

    try:
        print("🚀 AUTO ETL: Generating Inventory model snapshot...")
        run_inventory_etl()
        print("✅ Inventory model snapshot ready")

    except Exception:
        print("❌ INVENTORY MODEL ETL FAILED")
        traceback.print_exc()
