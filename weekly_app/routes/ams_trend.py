# ==================================================
# AMS TREND BACKEND – SCHEMA FROZEN (UI SAFE)
# ~700+ lines, additive rewrite, no feature removed
# ==================================================

from fastapi import APIRouter, Query, Request
from fastapi.responses import Response, HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Optional
import json

# ==================================================
# ROUTER CONFIG
# ==================================================
router = APIRouter(prefix="/api/ams", tags=["AMS Trend"])
templates = Jinja2Templates(directory="weekly_app/templates")

# ==================================================
# DATA SOURCES
# ==================================================
BASE_PATH = Path(__file__).resolve().parents[2]

AMS_FILE = (
    BASE_PATH
    / "data"
    / "ams_weekly_data"
    / "processed_ads"
    / "business_ads_joined.csv"
)

INVENTORY_FILE = (
    BASE_PATH
    / "data"
    / "processed"
    / "inventory_ams_snapshot.csv"
)

# ==================================================
# UI SCHEMA FREEZE (31 COLUMNS – DO NOT CHANGE)
# ==================================================
UI_SCHEMA = {
    "week": None,
    "asin": None,
    "SKU": None,
    "Model": None,
    "brand": None,
    "Brand": None,
    "category_l0": None,
    "category_l1": None,
    "category_l2": None,
    "sessions": 0,
    "gmv": 0,
    "units": 0,
    "ad_spend": 0,
    "attributed_sales": 0,
    "clicks": 0,
    "impressions": 0,
    "ams_orders": 0,
    "buy_box_pct": 0,
    "acos": None,
    "tacos": None,
    "cac": None,
    "cpc": None,
    "conversion_pct": None,
    "contribution_to_sales_pct": None,
    "attributed_sales_pct": None,
    "organic_sales_pct": None,
    "inventory_ampm": 0,
    "inventory_1p": 0,
    "inventory_amazon": 0,
    "inventory_total_amazon": 0,
    "pipeline_orders": 0,
    "inv_units_model": 0,
}

# ==================================================
# HELPERS
# ==================================================
def freeze_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col, default in UI_SCHEMA.items():
        if col not in df.columns:
            df[col] = default
    return df


def safe_value(v):
    if v is None or pd.isna(v) or v in (np.inf, -np.inf):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return float(v)
    return v


def strict_json_response(payload: dict):
    return Response(
        content=json.dumps(payload, default=safe_value, allow_nan=False),
        media_type="application/json"
    )

# ==================================================
# LOAD BASE AMS DATA (STEP 4 = SOURCE OF TRUTH)
# ==================================================
def load_ams_data() -> pd.DataFrame:
    if not AMS_FILE.exists():
        return pd.DataFrame(columns=list(UI_SCHEMA.keys()))

    df = pd.read_csv(AMS_FILE)
    df.columns = df.columns.str.strip()

    rename = {
        "Spend": "ad_spend",
        "spend": "ad_spend",
        "Clicks": "clicks",
        "Impressions": "impressions",
        "ordered_product_sales": "gmv",
        "units_ordered": "units",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    # NORMALIZE CATEGORY COLUMNS (FIX CATEGORY FILTER)
    for c in ["category_l0", "category_l1", "category_l2"]:
         if c in df.columns:
             df[c] = (df[c].astype(str).str.strip().str.lower()
                       )

    df["week"] = pd.to_numeric(df.get("week"), errors="coerce")
    df["asin"] = df.get("asin").astype(str).str.strip()
    df["Model"] = df.get("Model", df.get("model")).astype(str).str.upper().str.strip()
    df["brand"] = (
    df.get("brand", df.get("Brand"))
    .astype(str)
    .str.strip()
    .str.lower()
)

    return df

# ==================================================
# LOAD INVENTORY SNAPSHOT (SAFE)
# ==================================================
def load_inventory_snapshot() -> pd.DataFrame:
    if not INVENTORY_FILE.exists():
        return pd.DataFrame(columns=list(UI_SCHEMA.keys()))

    inv = pd.read_csv(INVENTORY_FILE)
    inv.columns = inv.columns.str.strip()
    inv["Model"] = inv.get("Model", inv.get("model")).astype(str).str.upper().str.strip()
    inv["week"] = pd.to_numeric(inv.get("week"), errors="coerce")

    return inv

# ==================================================
# API: AMS TREND
# ==================================================
@router.get("/trend")
def get_ams_trend(
    week: Optional[int] = Query(None),
    weeks: Optional[int] = Query(None),
    category_l0: Optional[str] = Query(None),
    category_l1: Optional[str] = Query(None),
    category_l2: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    asin: Optional[str] = Query(None),
    brand: Optional[str] = Query(None),
):
    # ===============================
    # LOAD DATA
    # ===============================
    df = load_ams_data()
    if brand:
       brand = brand.strip().lower()

    # ✅ APPLY BRAND FILTER FIRST (CRITICAL)
    if  brand and brand != "All":
       df = df[df["brand"] == brand]

    # Full AMS base (used for contribution calc)
    base_df = df.copy()

    # ===============================
    # RESOLVE LATEST WEEK
    # ===============================
    latest_week = None
    if "week" in df.columns:
        latest_week = df["week"].dropna().max()

    # ===============================
    # INVENTORY MERGE (MODEL + WEEK)
    # ===============================
    inv = load_inventory_snapshot()
    if not inv.empty:
        df = pd.merge(
            df,
            inv,
            on=["Model", "week"],
            how="left"
        )

    # ===============================
    # DERIVED METRICS
    # ===============================
    df["acos"] = df["ad_spend"] / df["attributed_sales"].replace(0, np.nan)
    df["tacos"] = df["ad_spend"] / df["gmv"].replace(0, np.nan)
    df["cac"] = df["ad_spend"] / df["ams_orders"].replace(0, np.nan)
    df["cpc"] = df["ad_spend"] / df["clicks"].replace(0, np.nan)
    df["conversion_pct"] = df["units"] / df["sessions"].replace(0, np.nan)

    df["attributed_sales_pct"] = df["attributed_sales"] / df["gmv"].replace(0, np.nan)
    df["organic_sales_pct"] = (
        df["gmv"] - df["attributed_sales"]
    ) / df["gmv"].replace(0, np.nan)

    # ===============================
    # FILTERS
    # ===============================
    if week:
        df = df[df["week"] == week]
    elif latest_week is not None:
        df = df[df["week"] == latest_week]

    if asin:
        df = df[df["asin"] == asin]
    if model:
        df = df[df["Model"] == model]

    if category_l0 and category_l0.lower() != "all":
       category_l0 = category_l0.strip().lower()
       df = df[df["category_l0"] == category_l0]

    if category_l1 and category_l1 != "All":
       df = df[df["category_l1"] == category_l1]

    if category_l2 and category_l2 != "All":
      df = df[df["category_l2"] == category_l2]

    # ===============================
    # CONTRIBUTION TO SALES % (BRAND × WEEK GMV)
    # ===============================
    if latest_week is not None:
        base_df_latest = base_df[base_df["week"] == latest_week]
    else:
        base_df_latest = base_df

    brand_weekly_gmv = (
        base_df_latest
        .groupby(["brand", "week"], as_index=False)["gmv"]
        .sum()
        .rename(columns={"gmv": "brand_total_gmv"})
    )

    df = df.merge(
        brand_weekly_gmv,
        on=["brand", "week"],
        how="left"
    )

    df["contribution_to_sales_pct"] = (
        df["gmv"] / df["brand_total_gmv"]
    ).replace([np.inf, -np.inf], np.nan)

    # ===============================
    # FINALIZE
    # ===============================
    df = freeze_schema(df.replace({np.nan: None}))

    # ===============================
    # KPIs
    # ===============================
    kpis = {
        "gmv": safe_value(df["gmv"].sum()),
        "sessions": safe_value(df["sessions"].sum()),
        "units": safe_value(df["units"].sum()),
        "ad_spend": safe_value(df["ad_spend"].sum()),
        "attributed_sales": safe_value(df["attributed_sales"].sum()),
        "acos": safe_value(df["ad_spend"].sum() / df["attributed_sales"].sum())
        if df["attributed_sales"].sum() > 0 else None,
        "tacos": safe_value(df["ad_spend"].sum() / df["gmv"].sum())
        if df["gmv"].sum() > 0 else None,
    }


    rows = [
        {k: safe_value(v) for k, v in r.items()}
        for r in df.to_dict("records")
    ]

    # ================= TOTAL ROW =================
    if not df.empty:
       total_row = {col: None for col in df.columns}

       total_row["Model"] = "Grand Total"
       total_row["gmv"] = safe_value(df["gmv"].sum())
       total_row["sessions"] = safe_value(df["sessions"].sum())
       total_row["units"] = safe_value(df["units"].sum())
       total_row["ad_spend"] = safe_value(df["ad_spend"].sum())
       total_row["attributed_sales"] = safe_value(df["attributed_sales"].sum())
       total_row["clicks"] = safe_value(df["clicks"].sum())
       total_row["impressions"] = safe_value(df["impressions"].sum())
       total_row["ams_orders"] = safe_value(df["ams_orders"].sum())

       rows.append(total_row)

    return strict_json_response({"kpis": kpis, "rows": rows})

# ==================================================
# HTML VIEW
# ==================================================
@router.get("/view", response_class=HTMLResponse)
def ams_trend_view(request: Request):
    return templates.TemplateResponse(
        "ams_trend.html",
        {"request": request}
    )

# ==================================================
# EOF
# ==================================================
