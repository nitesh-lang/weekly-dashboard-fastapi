from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import pandas as pd
from pathlib import Path
import urllib.parse
from typing import Dict, Any, List

from weekly_app.etl.sales_auto_etl import run_sales_auto_etl

# =====================================================
# ROUTER + TEMPLATE INIT
# =====================================================
router = APIRouter()
from jinja2 import Environment, FileSystemLoader
from starlette.templating import Jinja2Templates

_jinja_env = Environment(
    loader=FileSystemLoader("weekly_app/templates"),
    cache_size=0,
)
# ✅ Override get_template to never pass globals
_orig_get_template = _jinja_env.get_template
def _safe_get_template(name, *args, **kwargs):
    return _orig_get_template(name)
_jinja_env.get_template = _safe_get_template

templates = Jinja2Templates(env=_jinja_env)

# =====================================================
# FILE PATHS
# =====================================================
SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
SKU_MASTER = Path("data/master/sku_master.xlsx")

# 🔥 ADDITIVE — AMS WEEKLY FACT (READ ONLY)
AMS_FILE = Path(
    "data/ams_weekly_data/ams_weekly_fact/ams_weekly_fact_with_category.csv"
)

print("✅ DASHBOARD.PY LOADED — SAFE CONTRIBUTION VERSION (LOCKED CORE)")

# =====================================================
# ✅ MODULE-LEVEL DATA CACHE
# Stores loaded DataFrames so files are NOT re-read on every request.
# Cache is invalidated when the file's modification time changes.
# =====================================================
_df_cache: Dict[str, Any] = {}

def _load_csv_cached(path: Path) -> pd.DataFrame:
    """Load a CSV, returning a cached copy if the file hasn't changed."""
    key = str(path)
    mtime = path.stat().st_mtime if path.exists() else None
    if key in _df_cache and _df_cache[key]["mtime"] == mtime:
        return _df_cache[key]["df"].copy()
    df = pd.read_csv(path)
    _df_cache[key] = {"df": df, "mtime": mtime}
    return df.copy()

def _load_excel_cached(path: Path) -> pd.DataFrame:
    """Load an Excel file, returning a cached copy if the file hasn't changed."""
    key = str(path)
    mtime = path.stat().st_mtime if path.exists() else None
    if key in _df_cache and _df_cache[key]["mtime"] == mtime:
        return _df_cache[key]["df"].copy()
    df = pd.read_excel(path)
    _df_cache[key] = {"df": df, "mtime": mtime}
    return df.copy()

# =====================================================
# ------------------ HELPERS (LOCKED) -----------------
# =====================================================

def round_df(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            if c.endswith("_pct"):
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).round(2)
            else:
                df[c] = (
                    pd.to_numeric(df[c], errors="coerce")
                    .fillna(0)
                    .round(0)
                    .astype(int)
                )
    return df


def norm(x: Any) -> str:
    if pd.isna(x):
        return ""
    x = urllib.parse.unquote_plus(str(x))
    x = x.replace("&", "and")
    return " ".join(x.lower().strip().split())


def is_amazon(ch: str) -> bool:
    ch = str(ch).lower()
    return ("amazon" in ch) or ("1p" in ch)


def is_amazon_1p(ch: str) -> bool:
    return "1p" in str(ch).lower()


def is_amazon_am(ch: str) -> bool:
    ch = str(ch).lower()
    return ("amazon" in ch) and ("1p" not in ch)


# =====================================================
# DASHBOARD ROUTE
# =====================================================
@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    week: str = None,
    weeks: List[str] = Query(default=[]),   # ← multi-week checkboxes
    brand: str = None,
    view: str = "mapped",
):

    # =================================================
    # MULTI-WEEK RESOLUTION
    # =================================================
    def _fix_week(w):
        w = str(w).strip()
        if w.startswith("Week") and " " not in w:
            w = w.replace("Week", "Week ")
        return w

    # Build active_weeks — prefer multi-select; fall back to single ?week=
    if weeks:
        active_weeks = [_fix_week(w) for w in weeks if w.strip()]
    elif week:
        active_weeks = [_fix_week(week)]
    else:
        active_weeks = []

    selected = {
        "week":           active_weeks[0] if len(active_weeks) == 1 else None,
        "weeks":          active_weeks,
        "weeks_display":  ", ".join(active_weeks) if active_weeks else "All Weeks",
        "brand":          brand,
        "view":           view,
    }

    # =================================================
    # AUTO SELECT LATEST WEEK IF NOTHING CHOSEN
    # ✅ FIXED: uses cached CSV instead of re-reading file
    # =================================================
    if not active_weeks and SALES_FILE.exists():
        try:
            all_weeks = (
                _load_csv_cached(SALES_FILE)["week"]
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )

            def _wk(w):
                try:
                    return int(w.replace("Week", "").strip())
                except Exception:
                    return -1

            all_weeks = [w for w in all_weeks if _wk(w) >= 0]
            all_weeks.sort(key=_wk)

            if all_weeks:
                active_weeks = [all_weeks[-1]]
                selected["week"] = active_weeks[0]
                selected["weeks"] = active_weeks
                selected["weeks_display"] = active_weeks[0]
        except Exception:
            pass

    # =================================================
    # AUTO ETL (LOCKED)
    # ✅ FIXED: skip ETL if the sales file already exists and is fresh.
    #    Running ETL on every single dashboard request was a huge slowdown.
    #    ETL should only run on /run-etl-latest or on first boot.
    # =================================================
    if not SALES_FILE.exists():
        try:
            run_sales_auto_etl()
        except Exception:
            pass

    # =================================================
    # LOAD SKU MASTER (LOCKED)
    # ✅ FIXED: uses cached Excel read — was re-reading .xlsx on every request
    # =================================================
    master = _load_excel_cached(SKU_MASTER)
    master.columns = master.columns.str.strip()
    print(master.columns)
    master = master.rename(
        columns={
            "FBA SKU": "sku",
            "Model No.": "model_no",
            "Model": "model",
        }
    )

    if "model_no" not in master.columns and "model" in master.columns:
        master["model_no"] = master["model"]

    master["sku"] = master["sku"].astype(str)
    master = master[["sku", "model_no", "category_l0"]]

    # =================================================
    # SAFE SALES LOAD (LOCKED)
    # ✅ FIXED: uses cached CSV read — was re-reading entire CSV on every request
    # =================================================
    if not SALES_FILE.exists():
        return HTMLResponse(_jinja_env.get_template("dashboard.html").render(
            request=request,
            kpis={"units": 0, "gmv": 0, "inventory_value": 0, "sell_through": 0},
            sku_rows=[], channel_summary=[], category_summary=[],
            ams_pivot=[], weeks=[], brands=[], selected=selected,
        ))

    # ✅ Load full sales once from cache, then slice in memory
    full_sales = _load_csv_cached(SALES_FILE)
    full_sales.columns = full_sales.columns.str.strip().str.lower()
    full_sales["week"] = full_sales["week"].astype(str).str.strip()
    full_sales["sku"] = full_sales["sku"].astype(str)
    full_sales["channel"] = full_sales["channel"].astype(str)

    # ── Derive filter metadata from the full unfiltered frame ──
    # ✅ FIXED: was re-reading CSV a second time at the bottom of the function
    #    just to get weeks/brands lists. Now we grab it here from the same load.
    all_week_labels = sorted(full_sales["week"].astype(str).unique())
    brands_list = (
        sorted(full_sales["brand"].dropna().astype(str).str.strip().unique())
        if "brand" in full_sales.columns else []
    )

    # ── Now slice for the actual view ──
    sales = full_sales.copy()

    if "category_l0" in sales.columns:
        sales["category_l0"] = sales["category_l0"].astype(str).fillna("").replace("nan", "")
        sales["category_l0_norm"] = sales["category_l0"].apply(norm)

    # ── Filter by selected weeks (single or multiple) ──
    if active_weeks:
        sales = sales[sales["week"].isin(active_weeks)]

    # ✅ STEP 2 — BRAND FILTER (CORRECT PLACE)
    if brand and "brand" in sales.columns:
        sales = sales[sales["brand"].str.lower() == brand.lower()]

    if view == "mapped" and "sku_status" in sales.columns:
        sales = sales[sales["sku_status"] == "MAPPED"]

    for c in ["units_sold", "gross_sales", "sales_nlc"]:
        sales[c] = pd.to_numeric(sales[c], errors="coerce").fillna(0)

    total_gmv = float(sales["gross_sales"].sum())

    # =================================================
    # SKU TOTALS (LOCKED)
    # =================================================
    sku_totals = (
        sales.groupby("sku", as_index=False)
        .agg(
            total_units=("units_sold", "sum"),
            total_sales=("gross_sales", "sum"),
            total_nlc=("sales_nlc", "sum"),
        )
    )

    if total_gmv > 0:
        sku_totals["sales_contribution_pct"] = (
            sku_totals["total_sales"] / total_gmv * 100
        ).round(2)
    else:
        sku_totals["sales_contribution_pct"] = 0.0

    # =================================================
    # AMAZON SPLIT (LOCKED)
    # ✅ FIXED: replaced slow row-by-row .apply(lambda) with vectorised numpy ops
    # =================================================
    amazon = sales[sales["channel"].apply(is_amazon)].copy()
    if amazon.empty:
        amazon = sales.iloc[0:0].copy()

    # Vectorised channel classification — much faster than apply(lambda)
    ch_lower = amazon["channel"].str.lower()
    is_am_mask  = ch_lower.str.contains("amazon") & ~ch_lower.str.contains("1p")
    is_1p_mask  = ch_lower.str.contains("1p")

    amazon["amazon_am_units"]  = amazon["units_sold"].where(is_am_mask, 0)
    amazon["amazon_am_sales"]  = amazon["gross_sales"].where(is_am_mask, 0)
    amazon["amazon_am_nlc"]    = amazon["sales_nlc"].where(is_am_mask, 0)
    amazon["amazon_1p_units"]  = amazon["units_sold"].where(is_1p_mask, 0)
    amazon["amazon_1p_sales"]  = amazon["gross_sales"].where(is_1p_mask, 0)
    amazon["amazon_1p_nlc"]    = amazon["sales_nlc"].where(is_1p_mask, 0)

    amazon_split = (
        amazon.groupby("sku", as_index=False)
        .agg(
            amazon_am_units=("amazon_am_units", "sum"),
            amazon_am_sales=("amazon_am_sales", "sum"),
            amazon_am_nlc=("amazon_am_nlc", "sum"),
            amazon_1p_units=("amazon_1p_units", "sum"),
            amazon_1p_sales=("amazon_1p_sales", "sum"),
            amazon_1p_nlc=("amazon_1p_nlc", "sum"),
        )
    )

    amazon_split["amazon_total_units"] = (
        amazon_split["amazon_am_units"] +
        amazon_split["amazon_1p_units"]
    )
    amazon_split["amazon_total_sales"] = (
        amazon_split["amazon_am_sales"] +
        amazon_split["amazon_1p_sales"]
    )
    amazon_split["amazon_total_nlc"] = (
        amazon_split["amazon_am_nlc"] +
        amazon_split["amazon_1p_nlc"]
    )

    sku = (
        sku_totals
        .merge(amazon_split, on="sku", how="left")
        .merge(master[["sku", "model_no", "category_l0"]], on="sku", how="left")
    )

    # ✅ Ensure no dict/unhashable values leak into template
    for col in ["model_no", "category_l0"]:
        if col in sku.columns:
            sku[col] = sku[col].astype(str).replace("nan", "")

    sku = round_df(sku)

    kpis = {
        "units": int(sales["units_sold"].sum()),
        "gmv": int(total_gmv),
        "inventory_value": 0,
        "sell_through": 0,
    }

    # =================================================
    # CHANNEL SUMMARY (LOCKED)
    # =================================================
    channel_summary_df = (
        sales.groupby("channel", as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gmv=("gross_sales", "sum"),
            sales_nlc=("sales_nlc", "sum"),
        )
        .sort_values("gmv", ascending=False)
    )

    if total_gmv > 0:
        channel_summary_df["sales_contribution_pct"] = (
            channel_summary_df["gmv"] / total_gmv * 100
        ).round(2)
    else:
        channel_summary_df["sales_contribution_pct"] = 0.0

    channel_summary = round_df(channel_summary_df).to_dict("records")

    # =================================================
    # CATEGORY SUMMARY (LOCKED)
    # =================================================
    if "category_l0_norm" not in sales.columns:
        sales["category_l0_norm"] = ""
    category_summary_df = (
        sales.groupby(["category_l0", "category_l0_norm"], as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gross_sales=("gross_sales", "sum"),
        )
        .sort_values("gross_sales", ascending=False)
    )

    if total_gmv > 0:
        category_summary_df["sales_contribution_pct"] = (
            category_summary_df["gross_sales"] / total_gmv * 100
        ).round(2)
    else:
        category_summary_df["sales_contribution_pct"] = 0.0

    category_summary = round_df(category_summary_df).to_dict("records")

    # =================================================
    # AMS PIVOT — supports single or multiple weeks
    # ✅ FIXED: uses cached CSV read
    # =================================================
    ams_pivot = []

    if AMS_FILE.exists() and active_weeks:
        try:
            ams = _load_csv_cached(AMS_FILE)
            ams["week"] = ams["week"].astype(str)

            # Convert active_weeks to numeric week numbers for AMS file
            ams_week_nums = [w.replace("Week", "").strip() for w in active_weeks]
            ams = ams[ams["week"].isin(ams_week_nums)]

            ams_pivot_df = (
                ams.groupby("week", as_index=False)
                .agg(
                    ad_spend=("Spend", "sum"),
                    attributed_sales=("attributed_sales", "sum"),
                    sessions=("sessions", "sum"),
                )
            )

            if not ams_pivot_df.empty:
                ams_pivot_df["acos"] = (
                    ams_pivot_df["ad_spend"] /
                    ams_pivot_df["attributed_sales"]
                ).replace([pd.NA, float("inf")], 0).round(3)

                ams_pivot = ams_pivot_df.to_dict("records")
        except Exception:
            ams_pivot = []

    # =================================================
    # TEMPLATE RESPONSE (LOCKED KEYS + AMS)
    # ✅ FIXED: removed second pd.read_csv(SALES_FILE) that was here
    #    weeks and brands are now derived from the first load above
    # =================================================
    def _safe(v):
        return str(v) if isinstance(v, (dict, list)) else v

    def _sanitize(rows):
        return [{k: _safe(v) for k, v in row.items()} for row in rows]

    template = _jinja_env.get_template("dashboard.html")
    html = template.render(
        request=request,
        kpis=kpis,
        sku_rows=_sanitize(sku.to_dict("records")),
        channel_summary=_sanitize(channel_summary),
        category_summary=_sanitize(category_summary),
        ams_pivot=_sanitize(ams_pivot),
        weeks=all_week_labels,
        brands=brands_list,
        selected=selected,
    )
    return HTMLResponse(content=html)