
# ============================================================
# INVENTORY DASHBOARD MODULE
# ============================================================
# SOURCE: RAW INVENTORY SNAPSHOTS
# PATH  : data/raw/inventory/Week X/<Brand>/*.xlsx
#
# FEATURES
# --------
# 1. Multi-brand support (Nexlev, White Mulberry, Audio Array, Tonor)
# 2. Multi-week support (Week 1...N)
# 3. DEFAULT VIEW = Latest Week only
# 4. Optional filters via query params:
#       ?week=Week 4
#       ?brand=White Mulberry
# 5. Week filter is ALWAYS applied BEFORE KPI calculation
# 6. No HTML dependency for filtering logic
#
# NOTE
# ----
# This file is intentionally verbose and fully commented
# for auditability, traceability, and future extensions.
# ============================================================

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import pandas as pd
import re

# ============================================================
# ROUTER INITIALIZATION
# ============================================================

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

# ============================================================
# BASE PATH CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_INV_DIR = BASE_DIR / "data" / "raw" / "inventory"

# ============================================================
# WEEK EXTRACTION HELPERS
# ============================================================

def extract_week(val):
    if pd.isna(val):
        return None
    match = re.search(r"\d+", str(val))
    if not match:
        return None
    return f"Week {int(match.group())}"


def extract_week_num(val):
    if pd.isna(val):
        return None
    match = re.search(r"\d+", str(val))
    if not match:
        return None
    return int(match.group())

# ============================================================
# BRAND EXTRACTION HELPER
# ============================================================

def extract_brand(path: Path):
    for part in path.parts:
        p = part.lower()
        if "nexlev" in p:
            return "Nexlev"
        if "white" in p or "mulberry" in p:
            return "White Mulberry"
        if "audio" in p:
            return "Audio Array"
        if "tonor" in p:
            return "Tonor"
    return "Unknown"

# ============================================================
# RAW INVENTORY LOADER
# ============================================================

def load_all_inventory():
    """
    Loads ALL inventory Excel files across:
    - All weeks
    - All brands

    Returns a single normalized dataframe.
    """
    frames = []

    for file in RAW_INV_DIR.rglob("*.xlsx"):
        try:
            df = pd.read_excel(file)
        except Exception:
            continue

        df.columns = [c.strip().lower() for c in df.columns]

        if "model" not in df.columns or "qty" not in df.columns:
            continue

        # Brand resolution
        df["brand"] = extract_brand(file)

        # Week resolution
        if "week" in df.columns:
            df["week"] = df["week"].apply(extract_week)
        else:
            df["week"] = extract_week(file.parents[1].name)

        df = df.dropna(subset=["week"])

        # Safe defaults
        for col in [
            "sku",
            "category_l0",
            "category_l1",
            "category_l2",
            "channel",
            "type",
        ]:
            if col not in df.columns:
                df[col] = ""

        # Normalization
        df["model"] = df["model"].astype(str).str.strip()
        df["sku"] = df["sku"].astype(str).str.strip()
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
        df["nlc"] = pd.to_numeric(df.get("nlc", 0), errors="coerce").fillna(0)

        # Metrics
        df["inventory_units"] = df["qty"]
        df["inventory_value"] = df["qty"] * df["nlc"]

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True)
    data["week_num"] = data["week"].apply(extract_week_num)

    return data

# ============================================================
# INVENTORY DASHBOARD ROUTE
# ============================================================

@router.get("/inventory-dashboard", response_class=HTMLResponse)
def inventory_dashboard(
    request: Request,
    week: str | None = Query(default=None),
    brand: str | None = Query(default=None),
):

    # --------------------------------------------------------
    # LOAD INVENTORY DATA (ALL WEEKS)
    # --------------------------------------------------------
    df = load_all_inventory()

    if df.empty:
        return templates.TemplateResponse(
            "inventory_dashboard.html",
            {
                "request": request,
                "rows": [],
                "latest_week": "NA",
                "kpis": {},
                "aging": [],
                "channel_summary": [],
                "available_weeks": [],
                "available_brands": [],
            },
        )

    # --------------------------------------------------------
    # AVAILABLE FILTER OPTIONS (FOR UI)
    # --------------------------------------------------------
    available_weeks = sorted(df["week"].dropna().unique(), key=extract_week_num)
    available_brands = sorted(df["brand"].dropna().unique())

    # --------------------------------------------------------
    # APPLY WEEK FILTER (MANDATORY FIRST)
    # --------------------------------------------------------
    if week:
        df = df[df["week"] == week]
        active_week = week
    else:
        max_week = df["week_num"].max()
        df = df[df["week_num"] == max_week]
        active_week = df["week"].iloc[0]

    # --------------------------------------------------------
    # APPLY BRAND FILTER (OPTIONAL)
    # --------------------------------------------------------
    if brand:
        df = df[df["brand"] == brand]

    # --------------------------------------------------------
    # FINAL ROWS
    # --------------------------------------------------------
    rows = df[[
        "week",
        "brand",
        "model",
        "sku",
        "category_l0",
        "category_l1",
        "category_l2",
        "channel",
        "type",
        "inventory_units",
        "nlc",
        "inventory_value",
    ]].to_dict(orient="records")

    # --------------------------------------------------------
    # KPI CALCULATION (POST FILTER)
    # --------------------------------------------------------
    total_units = sum(r["inventory_units"] for r in rows)
    total_value = sum(r["inventory_value"] for r in rows)

    in_transit_units = sum(
        r["inventory_units"]
        for r in rows
        if "transit" in str(r["type"]).lower()
    )

    unsellable_units = sum(
        r["inventory_units"]
        for r in rows
        if "unsellable" in str(r["type"]).lower()
    )

    kpis = {
        "total_units": total_units,
        "total_value": total_value,
        "in_transit_pct": round((in_transit_units / total_units) * 100, 2)
        if total_units else 0,
        "unsellable_pct": round((unsellable_units / total_units) * 100, 2)
        if total_units else 0,
    }

    # --------------------------------------------------------
    # AGING BUCKETS
    # --------------------------------------------------------
    aging = [
        {"bucket": "0–30 days", "units": total_units - in_transit_units - unsellable_units},
        {"bucket": "31–60 days", "units": in_transit_units},
        {"bucket": "60+ days", "units": unsellable_units},
    ]

    # --------------------------------------------------------
    # CHANNEL × LOCATION SUMMARY
    # --------------------------------------------------------
    summary_map = {}

    for r in rows:
        key = (r["channel"], r["type"])
        summary_map.setdefault(key, {"units": 0, "value": 0})
        summary_map[key]["units"] += r["inventory_units"]
        summary_map[key]["value"] += r["inventory_value"]

    channel_summary = [
        {
            "channel": k[0],
            "location": k[1],
            "units": v["units"],
            "value": v["value"],
        }
        for k, v in summary_map.items()
    ]

    # --------------------------------------------------------
    # RENDER TEMPLATE
    # --------------------------------------------------------
    return templates.TemplateResponse(
        "inventory_dashboard.html",
        {
            "request": request,
            "rows": rows,
            "latest_week": active_week,
            "kpis": kpis,
            "aging": aging,
            "channel_summary": channel_summary,
            "available_weeks": available_weeks,
            "available_brands": available_brands,
        },
    )

# ============================================================
# END OF FILE
# ============================================================
