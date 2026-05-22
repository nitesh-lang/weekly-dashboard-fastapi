from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

import math
import re
import traceback
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
INVENTORY_FILE = Path("data/processed/inventory_model_snapshot.csv")

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
# React SPA owns `/dashboard` now; the function below still serves the JSON
# payload via `router.add_api_route("/api/dashboard", ...)` at the bottom.
# Old Jinja decorator intentionally removed for the SPA-only routing strategy.
def dashboard(
    request: Request,
    week: str = None,
    weeks: List[str] = Query(default=[]),   # ← multi-week checkboxes
    brand: str = None,                       # legacy single-brand
    brands: List[str] = Query(default=[]),  # ← multi-brand checkboxes
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

    # Build active_brands — prefer multi-select; fall back to single ?brand=
    active_brands = [b.strip() for b in (brands or []) if b and b.strip()]
    if not active_brands and brand and brand.strip().lower() not in ("none", "all", ""):
        active_brands = [brand.strip()]

    selected = {
        "week":           active_weeks[0] if len(active_weeks) == 1 else None,
        "weeks":          active_weeks,
        "weeks_display":  ", ".join(active_weeks) if active_weeks else "All Weeks",
        # Legacy single-brand: filled when exactly one brand selected so
        # downstream URL building (export buttons, drilldown links) keeps
        # working unchanged. Set to None for multi-brand selections so
        # those links don't lie about a single brand.
        "brand":          active_brands[0] if len(active_brands) == 1 else None,
        "brands":         active_brands,
        "brands_display": ", ".join(active_brands) if active_brands else "All Brands",
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
    master = master.rename(
        columns={
            "FBA SKU": "sku",
            "Model No.": "model_no",
            "Model": "model",
            "ASIN": "asin",
            "Asin": "asin",
        }
    )

    if "model_no" not in master.columns and "model" in master.columns:
        master["model_no"] = master["model"]

    master["sku"] = master["sku"].astype(str)
    if "asin" not in master.columns:
        master["asin"] = ""
    master["asin"] = master["asin"].astype(str).str.strip().replace("nan", "")
    master = master[["sku", "model_no", "asin", "category_l0"]]

    # =================================================
    # SAFE SALES LOAD (LOCKED)
    # ✅ FIXED: uses cached CSV read — was re-reading entire CSV on every request
    # =================================================
    if not SALES_FILE.exists():
        return HTMLResponse(_jinja_env.get_template("dashboard.html").render(
            request=request,
            kpis={"units": 0, "gmv": 0, "inventory_value": 0, "sell_through": 0},
            sku_rows=[], sku_total=0, channel_summary=[], category_summary=[],
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
    all_week_labels = sorted(
        full_sales["week"].astype(str).unique(),
        key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else 0
    )
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

    # ✅ STEP 2 — BRAND FILTER (multi-select aware)
    if active_brands and "brand" in sales.columns:
        lowered = [b.lower() for b in active_brands]
        sales = sales[sales["brand"].str.lower().isin(lowered)]

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
        .merge(master[["sku", "model_no", "asin", "category_l0"]], on="sku", how="left")
    )

    # ✅ Ensure no dict/unhashable values leak into template
    for col in ["model_no", "asin", "category_l0"]:
        if col in sku.columns:
            sku[col] = sku[col].astype(str).replace("nan", "")

    sku = round_df(sku)

    # Sort SKU table highest → lowest by total sales (matches the lazy-load API).
    if "total_sales" in sku.columns:
        sku = sku.sort_values("total_sales", ascending=False, kind="stable").reset_index(drop=True)

    # =================================================
    # INVENTORY KPIs (inventory_value + sell_through)
    # Inventory is a stock snapshot, not a weekly flow, so we use the
    # LATEST week present in the snapshot regardless of the active_weeks
    # sales filter.  Brand filter still applies so the KPI matches the
    # rest of the dashboard's scope.
    # =================================================
    inventory_value_total = 0
    sell_through_pct = 0.0
    if INVENTORY_FILE.exists():
        try:
            inv = _load_csv_cached(INVENTORY_FILE)
            if not inv.empty:
                # Use latest available inventory week (data may lag sales).
                def _wk_num(w):
                    try:    return int(str(w).replace("Week", "").strip())
                    except: return -1
                inv_weeks = sorted(inv["week"].dropna().astype(str).str.strip().unique(),
                                   key=_wk_num)
                if inv_weeks:
                    latest_inv_week = inv_weeks[-1]
                    inv_latest = inv[inv["week"].astype(str).str.strip() == latest_inv_week].copy()

                    # Apply brand filter so KPI matches the rest of the page.
                    if active_brands:
                        inv_latest = inv_latest[
                            inv_latest["brand"].astype(str).str.strip().isin(active_brands)
                        ]

                    inventory_value_total = int(inv_latest["inventory_value"].fillna(0).sum())
                    inv_units_total       = float(inv_latest["inventory_units"].fillna(0).sum())
                    sold_units_total      = float(sales["units_sold"].fillna(0).sum())
                    denom = sold_units_total + inv_units_total
                    if denom > 0:
                        sell_through_pct = round(sold_units_total / denom * 100, 1)
        except Exception:
            # Inventory KPI is non-blocking — fall back to zeros silently
            # so a corrupt snapshot can't break the whole dashboard.
            traceback.print_exc()

    kpis = {
        "units": int(sales["units_sold"].sum()),
        "gmv": int(total_gmv),
        "inventory_value": inventory_value_total,
        "sell_through": sell_through_pct,
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
    # WEEKLY TREND DATA (last N weeks)
    # - weekly_trend     : brand-filtered weekly GMV/units (full history)
    # - weekly_all_brands: same but unfiltered by brand — used as the
    #                      faint overlay on the trend chart so users see
    #                      relative performance when a brand is selected
    # - kpi_sparks_*     : last 8 weeks of each KPI (units / gmv / nlc)
    #                      for the mini sparklines inside KPI tiles
    # - brand_mix        : per-brand share of GMV for the selected weeks
    #                      (donut chart). Computed from `sales` (already
    #                      filtered by week+view) but NOT by brand.
    # =================================================
    def _wk_n(w):
        m = re.search(r"\d+", str(w))
        return int(m.group()) if m else 0

    # Brand+view filter (NOT week) — full history we'll trim client-side
    trend_base = full_sales.copy()
    if active_brands and "brand" in trend_base.columns:
        lowered = [b.lower() for b in active_brands]
        trend_base = trend_base[trend_base["brand"].str.lower().isin(lowered)]
    if view == "mapped" and "sku_status" in trend_base.columns:
        trend_base = trend_base[trend_base["sku_status"] == "MAPPED"]
    for c in ["units_sold", "gross_sales", "sales_nlc"]:
        if c not in trend_base.columns:
            trend_base[c] = 0
        trend_base[c] = pd.to_numeric(trend_base[c], errors="coerce").fillna(0)

    weekly_trend_df = (
        trend_base.groupby("week", as_index=False)
        .agg(gmv=("gross_sales", "sum"), units=("units_sold", "sum"))
    )
    weekly_trend_df["_n"] = weekly_trend_df["week"].apply(_wk_n)
    weekly_trend_df = weekly_trend_df.sort_values("_n")
    weekly_trend = (
        weekly_trend_df[["week", "gmv", "units"]]
        .assign(gmv=lambda d: d["gmv"].round(0).astype(int),
                units=lambda d: d["units"].round(0).astype(int))
        .to_dict("records")
    )

    # All-brands overlay — only computed (and only used by the chart)
    # when at least one brand is filtered, to give context vs the unfiltered total.
    weekly_all_brands = []
    if active_brands:
        all_b = full_sales.copy()
        if view == "mapped" and "sku_status" in all_b.columns:
            all_b = all_b[all_b["sku_status"] == "MAPPED"]
        for c in ["units_sold", "gross_sales"]:
            if c not in all_b.columns:
                all_b[c] = 0
            all_b[c] = pd.to_numeric(all_b[c], errors="coerce").fillna(0)
        all_b_df = (
            all_b.groupby("week", as_index=False)
            .agg(gmv=("gross_sales", "sum"), units=("units_sold", "sum"))
        )
        all_b_df["_n"] = all_b_df["week"].apply(_wk_n)
        all_b_df = all_b_df.sort_values("_n")
        weekly_all_brands = (
            all_b_df[["week", "gmv", "units"]]
            .assign(gmv=lambda d: d["gmv"].round(0).astype(int),
                    units=lambda d: d["units"].round(0).astype(int))
            .to_dict("records")
        )

    # KPI sparklines — last 8 weeks
    kpi_spark_df = weekly_trend_df.tail(8).copy()
    if "sales_nlc" in trend_base.columns:
        nlc_df = (
            trend_base.groupby("week", as_index=False)
            .agg(nlc=("sales_nlc", "sum"))
        )
        nlc_df["_n"] = nlc_df["week"].apply(_wk_n)
        nlc_df = nlc_df.sort_values("_n").tail(8)
        kpi_spark_df = kpi_spark_df.merge(nlc_df[["week", "nlc"]], on="week", how="left")
    else:
        kpi_spark_df["nlc"] = 0
    kpi_spark_df["nlc"] = kpi_spark_df["nlc"].fillna(0)

    kpi_sparks = {
        "weeks": kpi_spark_df["week"].tolist(),
        "units": [int(x) for x in kpi_spark_df["units"].tolist()],
        "gmv":   [int(x) for x in kpi_spark_df["gmv"].tolist()],
        "nlc":   [int(x) for x in kpi_spark_df["nlc"].tolist()],
    }

    # Week-over-week deltas (latest vs prior)
    def _delta(seq):
        if len(seq) < 2 or seq[-2] == 0:
            return None
        return round(((seq[-1] - seq[-2]) / seq[-2]) * 100, 1)

    kpi_deltas = {
        "units": _delta(kpi_sparks["units"]),
        "gmv":   _delta(kpi_sparks["gmv"]),
        "nlc":   _delta(kpi_sparks["nlc"]),
    }

    # Brand mix — share of GMV by brand for the SELECTED weeks.
    # Hidden when exactly one brand is filtered (would be a 100% donut
    # of itself). Shown when no brand is filtered OR when multiple
    # brands are filtered (to compare share among the selection).
    brand_mix = []
    if "brand" in full_sales.columns and len(active_brands) != 1:
        bm_base = full_sales.copy()
        if active_weeks:
            bm_base = bm_base[bm_base["week"].isin(active_weeks)]
        # If multiple brands are selected, restrict the donut to those
        if active_brands:
            lowered = [b.lower() for b in active_brands]
            bm_base = bm_base[bm_base["brand"].str.lower().isin(lowered)]
        if view == "mapped" and "sku_status" in bm_base.columns:
            bm_base = bm_base[bm_base["sku_status"] == "MAPPED"]
        for c in ["gross_sales"]:
            if c not in bm_base.columns:
                bm_base[c] = 0
            bm_base[c] = pd.to_numeric(bm_base[c], errors="coerce").fillna(0)
        bm = bm_base.groupby("brand", as_index=False).agg(gmv=("gross_sales", "sum"))
        bm = bm[bm["gmv"] > 0]
        total = bm["gmv"].sum() or 1
        bm["pct"] = (bm["gmv"] / total * 100).round(2)
        bm = bm.sort_values("gmv", ascending=False)
        brand_mix = bm.to_dict("records")

    # Freshness label
    from datetime import datetime as _dt
    latest_week_label = (
        sorted(all_week_labels, key=_wk_n)[-1] if all_week_labels else "—"
    )
    generated_at = _dt.utcnow().strftime("%Y-%m-%d %H:%M UTC")

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
                    ad_spend=("spend", "sum"),
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
        # NaN / Infinity aren't valid JSON — pandas returns these for empty
        # columns and Starlette's JSONResponse blows up on them.  Coerce to
        # null here so the React side just sees a clean missing value.
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return str(v) if isinstance(v, (dict, list)) else v

    def _sanitize(rows):
        return [{k: _safe(v) for k, v in row.items()} for row in rows]

    def _clean_nan(obj):
        """Recursively replace NaN / Inf in any nested dict/list with None.
        Applied to the whole JSON payload before serialization."""
        if isinstance(obj, dict):
            return {k: _clean_nan(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean_nan(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    # Send ALL rows in the initial HTML
    all_sku_rows = _sanitize(sku.to_dict("records"))

    # ── React frontend asks for JSON; legacy Jinja flow gets HTML ──
    wants_json = (
        request.url.path.startswith("/api/")
        or "application/json" in (request.headers.get("accept") or "")
        or request.query_params.get("format") == "json"
    )
    if wants_json:
        from fastapi.responses import JSONResponse
        return JSONResponse(_clean_nan({
            "kpis":              kpis,
            "kpi_sparks":        kpi_sparks,
            "kpi_deltas":        kpi_deltas,
            "weekly_trend":      weekly_trend,
            "weekly_all_brands": weekly_all_brands,
            "brand_mix":         brand_mix,
            "channel_summary":   _sanitize(channel_summary),
            "category_summary":  _sanitize(category_summary),
            "ams_pivot":         _sanitize(ams_pivot),
            "sku_rows":          all_sku_rows,
            "sku_total":         len(all_sku_rows),
            "weeks":             all_week_labels,
            "brands":            brands_list,
            "selected":          selected,
            "latest_week_label": latest_week_label,
            "generated_at":      generated_at,
        }))

    template = _jinja_env.get_template("dashboard.html")
    html = template.render(
        request=request,
        kpis=kpis,
        sku_rows=all_sku_rows,
        sku_total=len(all_sku_rows),
        channel_summary=_sanitize(channel_summary),
        category_summary=_sanitize(category_summary),
        ams_pivot=_sanitize(ams_pivot),
        weeks=all_week_labels,
        brands=brands_list,
        selected=selected,
        weekly_trend=weekly_trend,
        weekly_all_brands=weekly_all_brands,
        brand_mix=brand_mix,
        kpi_sparks=kpi_sparks,
        kpi_deltas=kpi_deltas,
        latest_week_label=latest_week_label,
        generated_at=generated_at,
    )
    return HTMLResponse(content=html)

# ── JSON API: paginated SKU rows for infinite scroll ──────────
@router.get("/api/dashboard/sku-rows")
def dashboard_sku_rows_api(
    request: Request,
    week: str = None,
    weeks: List[str] = Query(default=[]),
    brand: str = None,
    brands: List[str] = Query(default=[]),
    view: str = "mapped",
    page: int = 1,
    page_size: int = 100,
):
    """Returns JSON rows for infinite scroll on dashboard SKU table."""
    from fastapi.responses import JSONResponse

    # Re-use same logic as main dashboard endpoint
    def _fix_week(w):
        w = str(w).strip()
        if w.startswith("Week") and " " not in w:
            w = w.replace("Week", "Week ")
        return w

    if weeks:
        active_weeks = [_fix_week(w) for w in weeks if w.strip()]
    elif week:
        active_weeks = [_fix_week(week)]
    else:
        active_weeks = []

    try:
        # ── MASTER (XLSX, not CSV) — same loader the main dashboard uses ──
        master = _load_excel_cached(SKU_MASTER)
        master.columns = master.columns.str.strip()
        master = master.rename(columns={
            "FBA SKU": "sku",
            "Model No.": "model_no",
            "Model": "model",
            "ASIN": "asin",
            "Asin": "asin",
        })
        if "model_no" not in master.columns and "model" in master.columns:
            master["model_no"] = master["model"]
        master["sku"] = master["sku"].astype(str)
        if "asin" not in master.columns:
            master["asin"] = ""
        master["asin"] = master["asin"].astype(str).str.strip().replace("nan", "")
        master = master[["sku", "model_no", "asin", "category_l0"]]

        # ── SALES ──
        full_sales = _load_csv_cached(SALES_FILE)
        full_sales.columns = full_sales.columns.str.strip().str.lower()
        full_sales["week"] = full_sales["week"].astype(str).str.strip()
        full_sales["sku"] = full_sales["sku"].astype(str)
        full_sales["channel"] = full_sales["channel"].astype(str)

        # Default to latest week if nothing selected — matches initial render
        if not active_weeks:
            all_wks = full_sales["week"].astype(str).unique().tolist()
            def _wk(w):
                try:
                    return int(w.replace("Week", "").strip())
                except Exception:
                    return -1
            all_wks = [w for w in all_wks if _wk(w) >= 0]
            all_wks.sort(key=_wk)
            if all_wks:
                active_weeks = [all_wks[-1]]

        # Resolve multi-brand from either ?brands= (plural) or legacy ?brand=
        active_brands = [b.strip() for b in (brands or []) if b and b.strip()]
        if not active_brands and brand and brand not in ("None", "All", ""):
            active_brands = [brand.strip()]

        sales = full_sales.copy()
        if active_weeks:
            sales = sales[sales["week"].isin(active_weeks)]
        if active_brands:
            lowered = [b.lower() for b in active_brands]
            sales = sales[sales["brand"].str.strip().str.lower().isin(lowered)]
        if view == "mapped" and "sku_status" in sales.columns:
            sales = sales[sales["sku_status"] == "MAPPED"]

        for c in ["units_sold", "gross_sales", "sales_nlc"]:
            if c not in sales.columns:
                sales[c] = 0
            sales[c] = pd.to_numeric(sales[c], errors="coerce").fillna(0)

        total_gmv = float(sales["gross_sales"].sum())

        sku_totals = sales.groupby("sku", as_index=False).agg(
            total_units=("units_sold", "sum"),
            total_sales=("gross_sales", "sum"),
            total_nlc=("sales_nlc", "sum"),
        )
        if total_gmv > 0:
            sku_totals["sales_contribution_pct"] = (
                sku_totals["total_sales"] / total_gmv * 100
            ).round(2)
        else:
            sku_totals["sales_contribution_pct"] = 0.0

        # ── AMAZON SPLIT (AM / 1P / TOTAL) ──
        amazon = sales[sales["channel"].apply(is_amazon)].copy()
        if not amazon.empty:
            ch_lower = amazon["channel"].str.lower()
            is_am_mask = ch_lower.str.contains("amazon") & ~ch_lower.str.contains("1p")
            is_1p_mask = ch_lower.str.contains("1p")

            amazon["amazon_am_units"] = amazon["units_sold"].where(is_am_mask, 0)
            amazon["amazon_am_sales"] = amazon["gross_sales"].where(is_am_mask, 0)
            amazon["amazon_am_nlc"]   = amazon["sales_nlc"].where(is_am_mask, 0)
            amazon["amazon_1p_units"] = amazon["units_sold"].where(is_1p_mask, 0)
            amazon["amazon_1p_sales"] = amazon["gross_sales"].where(is_1p_mask, 0)
            amazon["amazon_1p_nlc"]   = amazon["sales_nlc"].where(is_1p_mask, 0)

            amazon_split = amazon.groupby("sku", as_index=False).agg(
                amazon_am_units=("amazon_am_units", "sum"),
                amazon_am_sales=("amazon_am_sales", "sum"),
                amazon_am_nlc=("amazon_am_nlc", "sum"),
                amazon_1p_units=("amazon_1p_units", "sum"),
                amazon_1p_sales=("amazon_1p_sales", "sum"),
                amazon_1p_nlc=("amazon_1p_nlc", "sum"),
            )
        else:
            amazon_split = pd.DataFrame(columns=[
                "sku",
                "amazon_am_units", "amazon_am_sales", "amazon_am_nlc",
                "amazon_1p_units", "amazon_1p_sales", "amazon_1p_nlc",
            ])

        amazon_split["amazon_total_units"] = (
            amazon_split.get("amazon_am_units", 0) + amazon_split.get("amazon_1p_units", 0)
        )
        amazon_split["amazon_total_sales"] = (
            amazon_split.get("amazon_am_sales", 0) + amazon_split.get("amazon_1p_sales", 0)
        )
        amazon_split["amazon_total_nlc"] = (
            amazon_split.get("amazon_am_nlc", 0) + amazon_split.get("amazon_1p_nlc", 0)
        )

        sku = (
            sku_totals
            .merge(amazon_split, on="sku", how="left")
            .merge(master, on="sku", how="left")
        )

        for col in ["model_no", "asin", "category_l0"]:
            if col in sku.columns:
                sku[col] = sku[col].astype(str).replace("nan", "")

        sku = round_df(sku).fillna(0)
        sku = sku.sort_values("total_sales", ascending=False)

        def _safe(v):
            import math
            if isinstance(v, float) and math.isnan(v): return 0
            return str(v) if isinstance(v, (dict, list)) else v

        all_rows = [{k: _safe(v) for k, v in row.items()} for row in sku.to_dict("records")]
        start = (page - 1) * page_size
        end   = start + page_size
        return JSONResponse({
            "rows": all_rows[start:end],
            "total": len(all_rows),
            "page": page,
            "has_more": end < len(all_rows),
        })
    except Exception:
        import traceback; traceback.print_exc()
        return JSONResponse({"rows": [], "total": 0, "page": page, "has_more": False})


# Expose the same dashboard handler at /api/dashboard so the React app can
# fetch JSON via the Vite proxy without colliding with the browser-facing
# /dashboard route (which React Router owns client-side).
router.add_api_route(
    "/api/dashboard",
    dashboard,
    methods=["GET"],
    response_class=JSONResponse,
)
