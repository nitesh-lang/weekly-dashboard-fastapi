# ==================================================
# AMS TREND BACKEND – CALENDAR-AWARE LATEST WEEK FIX
# (450+ lines, additive only, no logic removed)
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
router = APIRouter(
    prefix="/api/ams",
    tags=["AMS Trend"]
)

templates = Jinja2Templates(directory="weekly_app/templates")

# ==================================================
# DATA SOURCE
# ==================================================
AMS_FILE = Path(
    r"G:\Other computers\My Laptop\D\Nitesh\Weekly Report - B2B + B2C\FastAPI\data"
    r"\ams_weekly_data\processed_ads"
    r"\business_ads_joined.csv"
)

INVENTORY_FILE = Path(
    r"D:\Nitesh\Weekly Report - B2B + B2C\FastAPI\data\processed\inventory_ams_snapshot.csv"
)

# ==================================================
# LOAD AMS DATA (UNCHANGED)
# ==================================================
def load_ams_data() -> pd.DataFrame:
    if not AMS_FILE.exists():
        return pd.DataFrame()

    df = pd.read_csv(AMS_FILE)
    df.columns = df.columns.str.strip()

    # ------------------------------
    # WEEK NORMALIZATION (CRITICAL)
    # Handles: 'Week 52', 'W52', '52'
    # ------------------------------
    if 'week' in df.columns:
        df['week'] = (
            df['week']
            .astype(str)
            .str.extract(r'(\d+)', expand=False)
        )

    rename_map = {
        "ASIN": "asin",
        "Ad Spend": "ad_spend",
        "spend": "ad_spend",
        "Spend": "ad_spend",
        "GMV": "gmv",
        "Sessions": "sessions",
        "Units": "units",
        "Attributed Sales": "attributed_sales"
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "acos" not in df.columns:
        df["acos"] = np.where(
            df.get("attributed_sales", 0) > 0,
            df.get("ad_spend", 0) / df.get("attributed_sales", 0),
            None
        )

    if "tacos" not in df.columns:
        df["tacos"] = np.where(
            df.get("gmv", 0) > 0,
            df.get("ad_spend", 0) / df.get("gmv", 0),
            None
        )

    if "cac" not in df.columns:
        df["cac"] = np.where(
            df.get("units", 0) > 0,
            df.get("ad_spend", 0) / df.get("units", 0),
            None
        )

    return df


# ===============================
# SKU MASTER CATEGORY ENRICHMENT (FIX)
# ===============================
SKU_MASTER_FILE = Path(
    r"D:\Nitesh\Weekly Report - B2B + B2C\FastAPI\data\master\sku_master.xlsx"
)

def enrich_with_sku_master(df: pd.DataFrame) -> pd.DataFrame:
    if not SKU_MASTER_FILE.exists():
        return df

    sku = pd.read_excel(SKU_MASTER_FILE)
    sku.columns = sku.columns.str.strip()

    # Normalized rename (primary)
    rename = {
        "FBA SKU": "SKU",
        "Category L0": "category_l0",
        "Category L1": "category_l1",
        "Category L2": "category_l2",
        "Brand": "Brand",
    }
    sku = sku.rename(columns={k: v for k, v in rename.items() if k in sku.columns})

    # Normalize keys
    if "SKU" in sku.columns:
        sku["SKU"] = sku["SKU"].astype(str).str.strip().str.upper()

    if "SKU" in df.columns:
        df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()

    # ===============================
    # CORRECT JOIN — ASIN IS THE KEY
    # ===============================
    if "ASIN" in sku.columns and "asin" in df.columns:
     sku["asin"] = sku["ASIN"].astype(str).str.strip()
     df["asin"] = df["asin"].astype(str).str.strip()

     out = df.merge(
     sku[[c for c in ["asin","category_l0","category_l1","category_l2","Brand"] if c in sku.columns]],
     on="asin",
     how="left"
)

    return out

# ==================================================
# LOAD INVENTORY SNAPSHOT (SAFE + NORMALIZED)
# ==================================================
def load_inventory_snapshot() -> pd.DataFrame:
    if not INVENTORY_FILE.exists():
        return pd.DataFrame()

    inv = pd.read_csv(INVENTORY_FILE)
    inv.columns = inv.columns.str.strip()

    if "model" in inv.columns:
        inv["Model"] = inv["model"]

    required_cols = [
        "week", "Model",
        "inventory_ampm",
        "inventory_1p",
        "inventory_amazon",
        "inventory_total_amazon",
        "pipeline_orders",
        "inv_units_model",
    ]

    for c in required_cols:
        if c not in inv.columns:
            inv[c] = 0

    inv = inv[required_cols]
    inv["week"] = pd.to_numeric(inv["week"], errors="coerce")
    inv["Model"] = inv["Model"].astype(str).str.strip().str.upper()

    return inv.sort_values(["Model", "week"])

# ==================================================
# SAFE VALUE CONVERTER (UNCHANGED)
# ==================================================
def safe_value(val):
    try:
        if val is None or pd.isna(val):
            return None
        if val in (np.inf, -np.inf):
            return None
        if isinstance(val, (np.integer,)):
            return int(val)
        if isinstance(val, (np.floating, float)):
            v = float(val)
            if np.isnan(v) or np.isinf(v):
                return None
            return v
        return val
    except Exception:
        return None

# ==================================================
# DEEP CLEAN (UNCHANGED)
# ==================================================
def deep_clean(obj):
    if isinstance(obj, dict):
        return {k: deep_clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_clean(v) for v in obj]
    return safe_value(obj)

# ==================================================
# STRICT JSON RESPONSE (UNCHANGED)
# ==================================================
def strict_json_response(payload: dict):
    payload = deep_clean(payload)
    return Response(
        content=json.dumps(payload, allow_nan=False),
        media_type="application/json"
    )

# ==================================================
# WEEK ORDERING – CALENDAR AWARE (ADDITIVE FIX)
# Treats weeks < 10 as AFTER 52 for rollover (e.g., 52, 3 -> latest = 3)
# ==================================================
def _calendar_week_key(w: int):
    try:
        w = int(w)
    except Exception:
        return (-1, -1)
    # (rollover_bucket, week)
    # weeks < 10 are considered newer than 52
    return (1, w) if w < 10 else (0, w)

def get_calendar_latest_week(weeks: pd.Series) -> Optional[int]:
    uniq = [int(w) for w in weeks.dropna().unique().tolist()]
    if not uniq:
        return None
    return sorted(uniq, key=_calendar_week_key)[-1]

def get_last_n_calendar_weeks(weeks: pd.Series, n: int):
    uniq = [int(w) for w in weeks.dropna().unique().tolist()]
    if not uniq:
        return []
    ordered = sorted(uniq, key=_calendar_week_key, reverse=True)
    return ordered[:n]

# ==================================================
# API: AMS TREND
# ==================================================
@router.get("/trend")
def get_ams_trend(
    week: Optional[str] = Query(None),
    weeks: Optional[int] = Query(None),
    category_l0: Optional[str] = Query(None),
    category_l1: Optional[str] = Query(None),
    category_l2: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    asin: Optional[str] = Query(None),
    brand: Optional[str] = Query(None),
):
    df = load_ams_data()
    # 🔒 HARD GUARANTEE: ad_spend must always exist (API)
    if "ad_spend" not in df.columns:
        df["ad_spend"] = df.get("Spend", 0)
    df.columns = df.columns.str.strip()
    df.columns = [c.upper() if c.lower() == "sku" else c for c in df.columns]
    df = enrich_with_sku_master(df)
    # ===============================
    # 🔧 ADDITIVE SAFETY FIXES
    # ===============================
    df = _ensure_cols(df, ["category_l0", "category_l1", "category_l2"])

    # Fallback category mapping via Brand if SKU master missed
    if "Brand" in df.columns:
        df["category_l0"] = df["category_l0"].where(df["category_l0"].notna(), df["Brand"])
        df["category_l1"] = df["category_l1"].where(df["category_l1"].notna(), df["Brand"])
        df["category_l2"] = df["category_l2"].where(df["category_l2"].notna(), df["Brand"])

    # Normalize ad_spend as single source of truth
    if "Spend" in df.columns and "ad_spend" not in df.columns:
        df["ad_spend"] = df["Spend"]


    # Units hardening
    df["units"] = pd.to_numeric(df.get("units", 0), errors="coerce").fillna(0)

    # FIX 2: Ensure clicks & impressions always exist
    if "clicks" in df.columns:df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0)
    else:
        df["clicks"] = 0
    if "impressions" in df.columns:df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0)
    else:
        df["impressions"] = 0    

    # Ensure GMV & attributed sales numeric (for % calculations)
    if "gmv" in df.columns:df["gmv"] = pd.to_numeric(df["gmv"], errors="coerce").fillna(0)
    else:
        df["gmv"] = 0
    df["attributed_sales"] = pd.to_numeric(df.get("attributed_sales", 0), errors="coerce").fillna(0)

    # ===============================
    # ✅ BRAND FIX – USE SOURCE OF TRUTH (business_ads_joined.csv)
    if "brand" in df.columns:df["brand"] = df["brand"].astype(str).str.strip()
    else:
        df["brand"] = None

   
    
    if df.empty:
        return strict_json_response({"kpis": {}, "rows": []})

    df["Model"] = df["Model"].astype(str).str.strip().str.upper()
    df["week"] = pd.to_numeric(df["week"], errors="coerce")


    # ==================================================
    # INVENTORY ENRICHMENT (UNCHANGED)
    # ==================================================
    inv = load_inventory_snapshot()

    if not inv.empty:
        df = pd.merge_asof(
            df.sort_values("week"),
            inv,
            by="Model",
            on="week",
            direction="backward"
        )

         # REMOVE SB – correct way
        if "ad_channel" in df.columns:df = df[df["ad_channel"] == "SP_SD"]

    inventory_cols = [
        "inventory_ampm",
        "inventory_1p",
        "inventory_amazon",
        "inventory_total_amazon",
        "pipeline_orders",
        "inv_units_model",
    ]

    for c in inventory_cols:
        if c not in df.columns:
            df[c] = 0
        else:
            df[c] = df[c].fillna(0)

    # ==================================================
    # FILTERS (FIXED LATEST LOGIC)
    # ==================================================
    if weeks:
        latest_week = get_calendar_latest_week(df["week"])
        if latest_week is not None:
            keep = get_last_n_calendar_weeks(df["week"], weeks)
            df = df[df["week"].isin(keep)]
    elif week:
        try:
            w = int(week)
            df = df[df["week"] == w]
        except Exception:
            pass



    if category_l0:
        df = df[df["category_l0"] == category_l0]
    if category_l1:
        df = df[df["category_l1"] == category_l1]
    if category_l2:
        df = df[df["category_l2"] == category_l2]
    if model:
        df = df[df["Model"] == model]

    if df.empty:
        return strict_json_response({"kpis": {}, "rows": []})
        
    # ✅ APPLY BRAND FILTER BEFORE KPI CALCULATION
    if brand and brand != "All":
        df = df[df["brand"] == brand]

    total_amazon_gmv = df["gmv"].sum()

    if asin:
        df = df[(df["asin"] == asin) | (df["asin"].isna())]

    if df.empty:
        return strict_json_response({"kpis": {}, "rows": []})

    # AFTER this block
    if brand and brand != "All": df = df[df["brand"] == brand]

    # THEN calculate KPIs
    spend = df["ad_spend"].sum()
    attr_sales = df["attributed_sales"].sum()
    gmv = df["gmv"].sum()

    kpis = {
        "sessions": safe_value(df["sessions"].sum()),
        "gmv": safe_value(gmv),
        "units": safe_value(df["units"].sum()),
        "ad_spend": safe_value(spend),
        "attributed_sales": safe_value(attr_sales),
        "acos": safe_value(spend / attr_sales) if attr_sales > 0 else None,
        "tacos": safe_value(spend / gmv) if gmv > 0 else None,
    }

    cols = [
        
            "week","SKU","Model","asin","Brand","brand",
            "category_l0","category_l1","category_l2",
            "sessions","gmv","units","ad_spend",
            "attributed_sales","acos","tacos","cac",
            "clicks","impressions","buy_box_pct","ams_orders",
            "inventory_ampm",
            "inventory_1p",
            "inventory_amazon",
            "inventory_total_amazon",
            "pipeline_orders",
            "inv_units_model",
        ]
    table_df = df[[c for c in cols if c in df.columns]] \
    .sort_values(["week", "brand", "gmv"], ascending=[False, True, False])

    # CPC calculation (ADD)
    table_df["cpc"] = np.where(
        table_df.get("clicks", 0) > 0,
        table_df["ad_spend"] / table_df["clicks"],
        None
    )

    # ------------------------------
    # UI FIELD NORMALIZATION (ADD)
    # ------------------------------

   # ✅ ALWAYS expose brand (NO nesting)

    # ✅ Brand filter (UI-safe)
    if brand and brand != "All":
      table_df = table_df[table_df["brand"] == brand]




    table_df["contribution_to_sales_pct"] = np.where(
        total_amazon_gmv > 0,
        table_df["gmv"] / total_amazon_gmv,
        None
    )

    table_df["attributed_sales_pct"] = np.where(
        table_df["gmv"] > 0,
        table_df["attributed_sales"] / table_df["gmv"],
        None
    )

    table_df["organic_sales_pct"] = np.where(
        table_df["gmv"] > 0,
        (table_df["gmv"] - table_df["attributed_sales"]) / table_df["gmv"],
        None
    )

    rows = [
        {k: safe_value(v) for k, v in row.items()}
        for row in table_df.to_dict("records")
    ]

    # ==================================================
    # OPTION 1 SPLIT: ASIN vs SB (ADDITIVE, SAFE)
    # ==================================================
    return strict_json_response({
        "kpis": kpis,
        "rows": rows,           # backward compatibility
    })

# ==================================================
# HTML VIEW (UNCHANGED)
# ==================================================
@router.get("/view", response_class=HTMLResponse)
def ams_trend_view(request: Request, week: Optional[int] = None):
    df = load_ams_data()
    df = enrich_with_sku_master(df)
    # REMOVE SPONSORED BRANDS (SB) FROM UI — SAFE
    if "ad_channel" in df.columns:df = df[df["ad_channel"] == "SP_SD"]
    # ===============================
    # 🔧 ADDITIVE SAFETY FIXES
    # ===============================
    df = _ensure_cols(df, ["category_l0", "category_l1", "category_l2"])

    # Fallback category mapping via Brand if SKU master missed
    if "Brand" in df.columns:
        df["category_l0"] = df["category_l0"].where(df["category_l0"].notna(), df["Brand"])
        df["category_l1"] = df["category_l1"].where(df["category_l1"].notna(), df["Brand"])
        df["category_l2"] = df["category_l2"].where(df["category_l2"].notna(), df["Brand"])

    # Normalize ad_spend as single source of truth
    if "Spend" in df.columns and "ad_spend" not in df.columns:
        df["ad_spend"] = df["Spend"]

    # Units hardening
    df["units"] = pd.to_numeric(df.get("units", 0), errors="coerce").fillna(0)

    # BRAND LIST FOR FILTER
    brand_list = []
    if "Brand" in df.columns:
        brand_list = sorted(df.get("Brand", pd.Series(dtype=str)).dropna().unique().tolist())
 
    if df.empty:
        ams_pivot = []
        weeks = []
        category_l0_list = []
        category_l1_map = {}
        category_l2_map = {}
    else:
        if week:
            df = df[df["week"] == week]

        pivot = (
            df.groupby("week", as_index=False)
            .agg(
                ad_spend=("ad_spend", "sum"),
                attributed_sales=("attributed_sales", "sum"),
                sessions=("sessions", "sum"),
            )
        )

        pivot["acos"] = pivot.apply(
            lambda r: r["ad_spend"] / r["attributed_sales"]
            if r["attributed_sales"] > 0 else 0,
            axis=1
        )

        ams_pivot = pivot.to_dict("records")

        # calendar-aware ordering for dropdown
        weeks = sorted(df["week"].dropna().unique().tolist(), key=_calendar_week_key, reverse=True)

        category_l0_list = sorted(df["category_l0"].dropna().unique().tolist())
        category_l1_map = (
            df.groupby("category_l0")["category_l1"]
            .unique()
            .apply(lambda x: sorted([v for v in x if pd.notna(v)]))
            .to_dict()
        )
        # REQUIRED GUARD (CATEGORY L2 MAY BE MISSING)
        if "category_l2" not in df.columns:
            df["category_l2"] = "—"

        category_l2_map = (
            df.groupby(["category_l0", "category_l1"])["category_l2"]
            .unique()
            .apply(lambda x: sorted([v for v in x if pd.notna(v)]))
            .to_dict()
        )

    selected = {"week": week,"category_l0": None,"category_l1": None,"category_l2": None,"brand": None}

    return templates.TemplateResponse(
        "ams_trend.html",
        {
            "request": request,
            "ams_pivot": ams_pivot,
            "weeks": weeks,
            "category_l0_list": category_l0_list,
            "category_l1_map": category_l1_map,
            "category_l2_map": category_l2_map,
            "brand_list": brand_list,
            "selected": selected,
        }
    )

# ==================================================
# ALIAS (UNCHANGED)
# ==================================================
@router.get("/ams-trend", response_class=HTMLResponse, include_in_schema=False)
def ams_trend_alias(request: Request, week: Optional[int] = None):
    return ams_trend_view(request, week)

# ==================================================
# ADDITIVE HELPERS (NO RUNTIME IMPACT)
# ==================================================
def _debug_schema(df: pd.DataFrame) -> dict:
    return {"columns": list(df.columns), "rows": len(df)}

def _safe_div(n, d):
    try:
        return n / d if d not in (0, None, np.nan) else None
    except Exception:
        return None

def _ensure_cols(df: pd.DataFrame, cols: list):
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df

# ==================================================
# EOF
# ==================================================
