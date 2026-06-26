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
import time

# ==================================================
# RESPONSE CACHE
# ==================================================
# AMS Trend's biggest perf cost is rebuilding the 3,700-row × 37-col
# payload on every filter click — ~750ms warm just for the data prep,
# plus ~3 MB JSON to ship.  The data only changes once a week, so we
# memoize the serialized JSON bytes keyed on the normalized filter
# tuple.  TTL=300s gives 5 min of repeat-hit speed (effectively the
# operator's entire session of clicking around) while still flushing
# fast enough that a mid-session data refresh is reflected.
#
# We deliberately cache the *bytes*, not the Response object.
# GZipMiddleware mutates response.raw_headers in place (it shares the
# header list with its own initial_message), so reusing a Response
# object across requests leaves it with a stale Content-Encoding: gzip
# header while self.body is still the uncompressed JSON — the next
# pass through GZip double-counts and h11 aborts the wire with
# "Too much data for declared Content-Length".
_RESPONSE_CACHE: dict[str, tuple[float, bytes]] = {}
_RESPONSE_CACHE_TTL = 300  # seconds

def _cache_key(prefix: str, *args) -> str:
    """Build a deterministic key from (endpoint, *filters)."""
    parts = [prefix]
    for a in args:
        if a is None:
            parts.append("∅")
        elif isinstance(a, (list, tuple)):
            parts.append(",".join(str(x) for x in sorted(a)))
        else:
            parts.append(str(a))
    return "|".join(parts)

def _cached_or(key: str, build_fn):
    """Return a fresh Response wrapping cached JSON bytes, or run
    build_fn() to fill cache.  build_fn() must return a Response whose
    .body is the JSON payload bytes (strict_json_response does)."""
    now = time.time()
    hit = _RESPONSE_CACHE.get(key)
    if hit and (now - hit[0]) < _RESPONSE_CACHE_TTL:
        return Response(content=hit[1], media_type="application/json")
    resp = build_fn()
    _RESPONSE_CACHE[key] = (now, resp.body)
    # Bound the cache size — 64 entries is plenty for the AMS Trend
    # filter combinatorics an operator actually clicks through.
    if len(_RESPONSE_CACHE) > 64:
        oldest = min(_RESPONSE_CACHE, key=lambda k: _RESPONSE_CACHE[k][0])
        _RESPONSE_CACHE.pop(oldest, None)
    return resp

# ==================================================
# ROUTER CONFIG
# ==================================================
router = APIRouter(prefix="/api/ams", tags=["AMS Trend"])
from jinja2 import Environment, FileSystemLoader
_env = Environment(loader=FileSystemLoader("weekly_app/templates"), cache_size=0)

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

    from weekly_app.core.df_cache import load_csv_cached
    df = load_csv_cached(AMS_FILE)
    df.columns = df.columns.str.strip()

    rename = {
        "Spend": "ad_spend",
        "spend": "ad_spend",
        "Clicks": "clicks",
        "Impressions": "impressions",
        "ordered_product_sales": "gmv",
        "14 day total sales": "gmv",
        "14 day total sales (₹)": "gmv",
        "units_ordered": "units",
        "14 day total units": "units",
        "14 day total units (#)": "units",
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

    # Operator rule: Fossil has no ads campaigns and is not tracked on
    # AMS Trend.  Filter at the loader so the page, filters, KPIs and
    # exports all stay consistent.
    df = df[df["brand"] != "fossil"]

    return df

# ==================================================
# LOAD INVENTORY SNAPSHOT
# Builds the model × week inventory pivot from raw inventory data.
# Replaces the legacy precomputed inventory_ams_snapshot.csv which
# was being emitted empty by a broken ETL — leaving every inventory
# column at 0 for every SKU on the AMS Trend page.
#
# Schema produced (matches what get_ams_trend's merge expects):
#   Model, week, inventory_ampm, inventory_1p, inventory_amazon,
#   inventory_total_amazon, pipeline_orders, inv_units_model
# ==================================================
def load_inventory_snapshot() -> pd.DataFrame:
    """Per-(asin, model, week) inventory pivot.

    Reads from data/processed/inventory_model_snapshot.csv (pre-built by
    the inventory ETL).  This is ~100x faster than re-aggregating from
    the 83 raw inventory xlsx files on every cold cache miss, which was
    causing ~78s request timeouts on Render.  The CSV is already master-
    aligned (canonical SKU/Model/Brand per ASIN) so we don't need to
    re-apply the alignment here.
    """
    from pathlib import Path
    from weekly_app.core.df_cache import load_csv_cached
    snap = Path("data/processed/inventory_model_snapshot.csv")
    if not snap.exists():
        return pd.DataFrame(columns=list(UI_SCHEMA.keys()))
    try:
        df = load_csv_cached(snap)
    except Exception:
        return pd.DataFrame(columns=list(UI_SCHEMA.keys()))
    if df.empty:
        return pd.DataFrame(columns=list(UI_SCHEMA.keys()))

    # Robust blank check first (BEFORE astype-str, so .isna() still
    # catches np.nan and pd.NA).  Different pandas versions stringify
    # NaN differently — "nan" / "<NA>" / "" — so we union .isna()
    # with a lowercase string check to cover every variant.  Without
    # this, the runner's pandas dropped these rows silently in the
    # downstream groupby while local pandas kept them, producing a
    # 3634-unit cross-platform audit delta.
    _blank_asin = df["asin"].isna() | (
        df["asin"].astype(str).str.strip().str.lower().isin(["", "nan", "none", "<na>", "n/a"])
    )
    df = df[~_blank_asin]

    df["Model"]    = df["model"].astype(str).str.upper().str.strip()
    df["asin"]     = df["asin"].astype(str).str.strip()
    df["week_num"] = df["week"].astype(str).str.extract(r"(\d+)").astype(float)

    # Filter by channel directly — the snapshot CSV's `type` column is
    # empty (type gets derived from channel at render time by
    # _resolve_type elsewhere).  Using `type` here silently zeroed out
    # AMPM and 1P inventory for ~every week.  Channel-based filters
    # mirror the operator's CHANNEL_TO_TYPE mapping in inventory_dashboard.
    chan_lower = df["channel"].astype(str).str.strip().str.lower()

    # Channel bucket definitions live in weekly_app/core/channel_buckets.py
    # — single source of truth so AMS Trend and Amazon+1P sales trend
    # can never drift apart on the "what counts as Amazon-side?" rule.
    from weekly_app.core.channel_buckets import (
        AMAZON_SIDE_CHANNELS, PIPELINE_CHANNELS,
    )

    df["__ampm"]     = df["inventory_units"].where(chan_lower == "ampm",                0)
    df["__1p"]       = df["inventory_units"].where(chan_lower == "1p",                  0)
    df["__amazon"]   = df["inventory_units"].where(chan_lower == "amazon",              0)
    df["__pipeline"] = df["inventory_units"].where(chan_lower.isin(PIPELINE_CHANNELS),  0)
    # Sanity assertion: the 3 buckets we pivot into below MUST be the
    # AMAZON_SIDE_CHANNELS set.  If someone adds a 4th column above
    # without updating the constant, this fires.
    assert AMAZON_SIDE_CHANNELS == {"ampm", "amazon", "1p"}, \
        "ams_trend.py inventory pivot expects exactly AMPM + Amazon + 1P; " \
        "channel_buckets.AMAZON_SIDE_CHANNELS has drifted"

    asin_pivot = df.groupby(["asin", "Model", "week_num"], as_index=False).agg(
        inventory_ampm   =("__ampm", "sum"),
        inventory_1p     =("__1p", "sum"),
        inventory_amazon =("__amazon", "sum"),
        pipeline_orders  =("__pipeline", "sum"),
    )
    # Operator rule (2026-05-29): total Amazon-side inventory =
    # AMPM warehouse + Amazon FBA + 1P.  AMPM was previously excluded
    # and is the "buffer" stock sitting at the operator's warehouse
    # before being shipped to Amazon FBA / vendor.
    asin_pivot["inventory_total_amazon"] = (
        asin_pivot["inventory_ampm"]
        + asin_pivot["inventory_amazon"]
        + asin_pivot["inventory_1p"]
    )

    model_total = df.groupby(["Model", "week_num"], as_index=False)["inventory_units"].sum()
    model_total = model_total.rename(columns={"inventory_units": "inv_units_model"})
    pivot = asin_pivot.merge(model_total, on=["Model", "week_num"], how="left")
    pivot = pivot.rename(columns={"week_num": "week"})
    pivot["week"] = pd.to_numeric(pivot["week"], errors="coerce")

    for c in ["inventory_ampm", "inventory_1p", "inventory_amazon",
              "inventory_total_amazon", "pipeline_orders", "inv_units_model"]:
        pivot[c] = pd.to_numeric(pivot[c], errors="coerce").fillna(0).astype(int)

    return pivot

# ==================================================
# API: AMS TREND
# ==================================================
@router.get("/trend")
def get_ams_trend(
    week: Optional[int] = Query(None),
    weeks: Optional[int] = Query(None),
    sel_weeks: Optional[list[int]] = Query(default=None),
    category_l0: Optional[str] = Query(None),
    category_l1: Optional[str] = Query(None),
    category_l2: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    asin: Optional[str] = Query(None),
    brand: Optional[list[str]] = Query(default=None),
):
    # 5-min response cache — same filter combo hits memory instead of
    # re-running the load+merge+derive+serialize pipeline (~750ms warm).
    cache_key = _cache_key("trend", week, weeks, sel_weeks, category_l0,
                           category_l1, category_l2, model, asin, brand)
    return _cached_or(cache_key, lambda: _build_trend_response(
        week=week, weeks=weeks, sel_weeks=sel_weeks,
        category_l0=category_l0, category_l1=category_l1,
        category_l2=category_l2, model=model, asin=asin, brand=brand,
    ))


def _build_trend_response(
    week, weeks, sel_weeks, category_l0, category_l1, category_l2,
    model, asin, brand,
):
    # ===============================
    # LOAD DATA
    # ===============================
    df = load_ams_data()

    # ✅ APPLY BRAND FILTER FIRST (CRITICAL)
    # `brand` is a multi-value query param now — filter via isin(). Empty list
    # or only-"All" entries mean no brand restriction.
    brands_norm = []
    if brand:
        brands_norm = [b.strip().lower() for b in brand if b and b.strip().lower() != "all"]
    if brands_norm:
        df = df[df["brand"].isin(brands_norm)]

    # Full AMS base (used for contribution calc)
    base_df = df.copy()

    # ===============================
    # RESOLVE LATEST WEEK + AVAILABLE WEEK LIST
    # ===============================
    latest_week = None
    all_weeks_list: list[int] = []
    if "week" in df.columns:
        all_weeks_list = sorted(int(w) for w in df["week"].dropna().unique())
        latest_week = all_weeks_list[-1] if all_weeks_list else None

    # Default window = last 12 weeks. When the operator hasn't picked
    # anything we want a useful trend snapshot for the overview chart;
    # the table picker still lets them narrow further.
    default_window = all_weeks_list[-12:] if len(all_weeks_list) >= 12 else all_weeks_list

    # ===============================
    # INVENTORY MERGE (MODEL + WEEK)
    # ===============================
    inv = load_inventory_snapshot()
    if not inv.empty:
        # Per-(asin, week) join — gives each AMS row its actual ASIN's
        # inventory, not the model-level total smeared across variants.
        # Fall back to Model-only inventory rollup for rows whose ASIN
        # isn't in the inventory snapshot (e.g. 1P-only ASINs with no
        # FBA stock are missing from the inventory join key).
        df = pd.merge(
            df, inv,
            left_on=["asin", "week"], right_on=["asin", "week"],
            how="left", suffixes=("", "_inv"),
        )
        # If the per-ASIN join missed (no inventory for this ASIN), fall
        # back to the model-level rollup so the column isn't blank where
        # we have model-level signal.
        miss = df["inv_units_model"].isna()
        if miss.any():
            inv_by_model = (inv.drop_duplicates(["Model", "week"])
                                [["Model", "week", "inventory_ampm", "inventory_1p",
                                  "inventory_amazon", "inventory_total_amazon",
                                  "pipeline_orders", "inv_units_model"]])
            df_miss = df.loc[miss, ["Model", "week"]].merge(
                inv_by_model, on=["Model", "week"], how="left"
            )
            for col in ["inventory_ampm","inventory_1p","inventory_amazon",
                        "inventory_total_amazon","pipeline_orders","inv_units_model"]:
                df.loc[miss, col] = df_miss[col].values

    # ===============================
    # DERIVED METRICS
    # ===============================
    df["acos"] = df["ad_spend"] / df["attributed_sales"].replace(0, np.nan)
    df["tacos"] = df["ad_spend"] / df["gmv"].replace(0, np.nan)
    # ROAS = Attributed Sales / Ad Spend. Recomputed here so the wrong
    # value baked into older copies of business_ads_joined.csv (when
    # step4 used gmv/Spend) doesn't pass through to the UI.
    df["roas"] = df["attributed_sales"] / df["ad_spend"].replace(0, np.nan)
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
    if sel_weeks:
        df = df[df["week"].isin(sel_weeks)]
    elif week:
        df = df[df["week"] == week]
    elif default_window:
        df = df[df["week"].isin(default_window)]

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

    # Restore master-canonical Model casing AND canonical SKU for every
    # row whose ASIN is in master.  Earlier in this route Model gets
    # uppercased so the join with the inventory pivot is case-insensitive;
    # that join is done by now, so it's safe to put master's casing back.
    # overwrite_sku=True also pulls the canonical SKU from master so the
    # row's SKU + Model + ASIN are all guaranteed identical to master.
    from weekly_app.core.master_override import apply_master_model
    apply_master_model(df, asin_col="asin", sku_col="SKU",
                       model_col="Model", overwrite_sku=True)

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

    return strict_json_response({
        "kpis": kpis,
        "rows": rows,
        "all_weeks": all_weeks_list,
        "default_weeks": default_window,
    })

# ==================================================
# API: AMS INSIGHTS
# ==================================================
# Same filter contract as /api/ams/trend so the SPA can call both
# in parallel on every filter change.  Returns expert-level
# performance commentary derived from the row-level data — the
# kind of read a senior PPC analyst would write on Monday.
# Read-only, no side effects.
# ==================================================
@router.get("/insights")
def get_ams_insights(
    week: Optional[int] = Query(None),
    weeks: Optional[int] = Query(None),
    sel_weeks: Optional[list[int]] = Query(default=None),
    category_l0: Optional[str] = Query(None),
    category_l1: Optional[str] = Query(None),
    category_l2: Optional[str] = Query(None),
    # model + asin accept repeated values (?model=A&model=B) so the
    # operator can scope the Performance read to a specific Model /
    # ASIN selection from the AMS Trend pickers.
    model: Optional[list[str]] = Query(default=None),
    asin: Optional[list[str]] = Query(default=None),
    brand: Optional[list[str]] = Query(default=None),
):
    cache_key = _cache_key("insights", week, weeks, sel_weeks, category_l0,
                           category_l1, category_l2, model, asin, brand)
    return _cached_or(cache_key, lambda: _build_insights_response(
        week=week, weeks=weeks, sel_weeks=sel_weeks,
        category_l0=category_l0, category_l1=category_l1,
        category_l2=category_l2, model=model, asin=asin, brand=brand,
    ))


def _build_insights_response(
    week, weeks, sel_weeks, category_l0, category_l1, category_l2,
    model, asin, brand,
):
    from weekly_app.core.ams_insights import build_insights

    # Reuse the same load+filter pipeline /api/ams/trend uses so
    # insights can never disagree with the table on the same screen.
    df = load_ams_data()

    brands_norm: list[str] = []
    if brand:
        brands_norm = [b.strip().lower() for b in brand if b and b.strip().lower() != "all"]
    if brands_norm:
        df = df[df["brand"].isin(brands_norm)]

    # Window default — last 12 weeks if nothing chosen.
    if "week" in df.columns:
        all_weeks_list = sorted(int(w) for w in df["week"].dropna().unique())
        default_window = all_weeks_list[-12:] if len(all_weeks_list) >= 12 else all_weeks_list
    else:
        default_window = []

    # Merge per-(asin, week) inventory so the inventory section can read
    # inventory_total_amazon at the right row-level grain.
    inv = load_inventory_snapshot()
    if not inv.empty:
        df = pd.merge(
            df, inv,
            left_on=["asin", "week"], right_on=["asin", "week"],
            how="left", suffixes=("", "_inv"),
        )
        miss = df["inv_units_model"].isna() if "inv_units_model" in df.columns else None
        if miss is not None and miss.any():
            inv_by_model = (inv.drop_duplicates(["Model", "week"])
                                [["Model", "week", "inventory_ampm", "inventory_1p",
                                  "inventory_amazon", "inventory_total_amazon",
                                  "pipeline_orders", "inv_units_model"]])
            df_miss = df.loc[miss, ["Model", "week"]].merge(
                inv_by_model, on=["Model", "week"], how="left"
            )
            for col in ["inventory_ampm", "inventory_1p", "inventory_amazon",
                        "inventory_total_amazon", "pipeline_orders", "inv_units_model"]:
                df.loc[miss, col] = df_miss[col].values

    # Apply the rest of the filters identically to /trend.
    if sel_weeks:
        df = df[df["week"].isin(sel_weeks)]
        used = sel_weeks
    elif week:
        df = df[df["week"] == week]
        used = [week]
    elif default_window:
        df = df[df["week"].isin(default_window)]
        used = default_window
    else:
        used = []

    if asin:
        df = df[df["asin"].isin(asin)]
    if model:
        df = df[df["Model"].isin(model)]
    if category_l0 and category_l0.lower() != "all":
        df = df[df["category_l0"] == category_l0.strip().lower()]
    if category_l1 and category_l1 != "All":
        df = df[df["category_l1"] == category_l1]
    if category_l2 and category_l2 != "All":
        df = df[df["category_l2"] == category_l2]

    insights = build_insights(df, selected_weeks=used)
    return strict_json_response(insights)


# ==================================================
# HTML VIEW
# ==================================================
# React SPA owns `/ams-trend`; old Jinja /api/ams/view route disabled.
def ams_trend_view(request: Request):
    df = load_ams_data()
    all_weeks = sorted(df["week"].dropna().unique().astype(int).tolist())
    # Brand values used by the multi-select picker. We pass the display-cased
    # version (Title Case) so the UI is readable; route compares case-insensitive.
    raw_brands = (
        df["brand"].dropna().astype(str).str.strip().str.lower().unique().tolist()
    )
    all_brands = sorted(b.title() for b in raw_brands if b and b != "unknown")
    return HTMLResponse(_env.get_template("ams_trend.html").render(
        request=request,
        all_weeks=all_weeks,
        all_brands=all_brands,
    ))

# ==================================================
# EXPORT CSV
# ==================================================
@router.get("/export")
def export_ams_trend(
    week: Optional[int] = Query(None),
    sel_weeks: Optional[list[int]] = Query(default=None),
    category_l0: Optional[str] = Query(None),
    category_l1: Optional[str] = Query(None),
    category_l2: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    asin: Optional[str] = Query(None),
    brand: Optional[list[str]] = Query(default=None),
):
    # Reuse existing logic
    response = get_ams_trend(
        week=week,
        weeks=None,
        sel_weeks=sel_weeks,
        category_l0=category_l0,
        category_l1=category_l1,
        category_l2=category_l2,
        model=model,
        asin=asin,
        brand=brand,
    )

    data = json.loads(response.body)
    df = pd.DataFrame(data["rows"])

    csv_data = df.to_csv(index=False)

    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=ams_trend_export.csv"
        },
    )
# ==================================================
# EOF
# ==================================================
