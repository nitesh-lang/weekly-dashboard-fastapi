# ============================================================
# AMAZON + 1P SALES TREND (WITH SESSIONS + CONVERSION)
# DUPLICATE SAFE • ZERO SAFE • GRAND TOTAL FIXED
# ============================================================

from fastapi import APIRouter, Request, Query
from typing import List, Optional
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import pandas as pd
import re

from weekly_app.core.json_utils import clean_nan
from weekly_app.core.data_norm import normalize_keys
from weekly_app.core.df_cache import load_csv_cached, load_excel_cached

# ============================================================
# ROUTER INIT
# ============================================================

router = APIRouter()
from jinja2 import Environment, FileSystemLoader
_env = Environment(loader=FileSystemLoader("weekly_app/templates"), cache_size=0)

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "processed"
AMS_DATA_DIR = BASE_DIR / "data" / "ams_weekly_data" / "processed_ads"
MASTER_FILE = BASE_DIR / "data" / "master" / "sku_master.xlsx"


def load_asin_by_model() -> dict:
    """{MODEL: 'ASIN1, ASIN2'} from sku_master, comma-joined distinct ASINs."""
    if not MASTER_FILE.exists():
        return {}
    try:
        m = load_excel_cached(MASTER_FILE)
    except Exception:
        return {}
    m.columns = m.columns.str.strip()
    normalize_keys(m)
    asin_col = next((c for c in ["ASIN", "Asin", "asin"] if c in m.columns), None)
    model_col = next((c for c in ["Model", "Model No.", "model"] if c in m.columns), None)
    if not asin_col or not model_col:
        return {}
    m[model_col] = m[model_col].astype(str).str.strip().str.upper()
    m[asin_col] = m[asin_col].astype(str).str.strip()
    m = m[m[asin_col].ne("") & m[asin_col].ne("nan") & m[asin_col].ne("-")]
    # Defensive: coerce every value to str inside the comprehension so a
    # stray float NaN that slipped past the filter can't crash str.join().
    return {
        model: ", ".join(sorted({
            s for s in (str(v).strip() for v in grp[asin_col])
            if s and s.lower() not in ("nan", "none", "-")
        }))
        for model, grp in m.groupby(model_col)
    }

# ============================================================
# NORMALIZATION
# ============================================================

def norm(x):
    return str(x).strip()

def norm_model(x):
    return str(x).strip().upper()

def extract_week(v):
    m = re.search(r"\d+", str(v))
    return int(m.group()) if m else None


# ============================================================
# FILE FINDER
# ============================================================

def find_file(base, stems):
    for f in base.iterdir():
        if not f.is_file():
            continue
        name = f.name.lower().replace(" ", "_")
        for s in stems:
            if s in name:
                return f
    raise FileNotFoundError(stems)


# ============================================================
# LOAD SALES SNAPSHOT
# ============================================================

def load_sales():

    f = find_file(DATA_DIR, ["weekly_sales_snapshot", "weekly_sales"])
    df = load_csv_cached(f)

    df.columns = [c.strip().lower() for c in df.columns]
    # Some ETLs emit duplicate columns (e.g. both 'model' and 'Model'
    # which collide after lowercase). Drop dupes; keep the first.
    df = df.loc[:, ~df.columns.duplicated()]

    df = df.rename(columns={
        "units_sold": "units",
        "gross_sales": "sales"
    })

    required_cols = [
        "week", "model", "brand",
        "units", "sales", "channel",
        "category_l0", "category_l1", "category_l2"
    ]

    for c in required_cols:
        if c not in df.columns:
            df[c] = 0

    df["brand"] = df["brand"].astype(str).str.lower().str.strip()
    df["model"] = df["model"].apply(norm_model)
    df["channel"] = df["channel"].astype(str).str.lower().str.strip()

    df["units"] = pd.to_numeric(df["units"], errors="coerce").fillna(0)
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)

    df["week_num"] = df["week"].apply(extract_week)

    return df


# ============================================================
# LOAD BUSINESS (SESSIONS + CONVERSION)
# ============================================================

def load_business():

    try:
        f = find_file(AMS_DATA_DIR, ["business_ads_joined"])
    except FileNotFoundError:
        # On Render the file may not be present (no AMS data uploaded yet).
        # Return an empty frame with the schema downstream code expects so
        # the route can still render rather than 500ing.
        return pd.DataFrame(columns=[
            "model", "week", "sessions", "conversion_pct", "week_num",
        ])

    df = load_csv_cached(f)

    df.columns = [c.strip().lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    for c in ["model", "week", "sessions", "conversion_pct"]:
        if c not in df.columns:
            df[c] = 0

    df["model"] = df["model"].apply(norm_model)
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0)
    df["conversion_pct"] = pd.to_numeric(
        df["conversion_pct"], errors="coerce"
    ).fillna(0)

    df["week_num"] = df["week"].apply(extract_week)

    return df


# ============================================================
# INVENTORY SNAPSHOT
# ============================================================

def load_inventory(latest_week):
    """Returns model → total inventory units for the given week_num.

    Sourced from load_all_inventory() (raw weekly inventory xlsx files).
    The legacy data/processed/inventory_ams_snapshot.csv was being
    produced empty by a broken ETL, leaving every SKU's inventory
    column at 0 on this page. Bypassing it.
    """
    try:
        from weekly_app.routes.inventory_dashboard import load_all_inventory
        df = load_all_inventory()
    except Exception:
        return {}

    if df is None or df.empty or latest_week is None:
        return {}

    df = df[df["week_num"] == latest_week]
    if df.empty:
        return {}

    df = df.copy()
    df["model"] = df["model"].astype(str).str.upper().str.strip()

    # Amazon+1P page → only Amazon-side stock counts.
    # Single source of truth in weekly_app/core/channel_buckets.py so
    # this can never drift from ams_trend's inventory_total_amazon.
    from weekly_app.core.channel_buckets import AMAZON_SIDE_CHANNELS
    chan = df["channel"].astype(str).str.strip().str.lower()
    df = df[chan.isin(AMAZON_SIDE_CHANNELS)]
    if df.empty:
        return {}

    return (
        df.groupby("model")["inventory_units"]
        .sum()
        .astype(int)
        .to_dict()
    )


# ============================================================
# TREND LOGIC
# ============================================================

def trend(seq):
    """UP / DOWN / FLAT / N/A across an arbitrary-length week sequence.

    - 0 values  → "FLAT"  (defensive; shouldn't happen for real rows)
    - 1 value   → "N/A"   (single week selected — no comparison possible)
    - 2 values  → direct last-vs-first compare
    - 3+ values → last value vs average of all prior values, with a 5%
                  deadband so tiny week-to-week wobble doesn't flip the
                  arrow. Considers the full selected window, not just the
                  last 3 weeks.
    """
    if not seq:
        return "FLAT"
    if len(seq) == 1:
        return "N/A"
    if len(seq) == 2:
        a, b = seq
        if b > a:
            return "UP"
        if b < a:
            return "DOWN"
        return "FLAT"

    last = seq[-1]
    prior = seq[:-1]
    prior_avg = sum(prior) / len(prior)

    if prior_avg <= 0:
        if last > 0:
            return "UP"
        if last < 0:
            return "DOWN"
        return "FLAT"

    pct = (last - prior_avg) / prior_avg
    if pct > 0.05:
        return "UP"
    if pct < -0.05:
        return "DOWN"
    return "FLAT"


# ============================================================
# CORE BUILDER
# ============================================================

def build_amazon_sales_trend(sales_df, business_df, sel_weeks=None):

    # -------- LAST 4 WEEKS ----------
    weeks_df = (
        sales_df[["week", "week_num"]]
        .dropna()
        .drop_duplicates()
        .sort_values("week_num")
    )

    # ✅ If weeks selected, use those — otherwise default to last 4
    if sel_weeks:
        weeks_df = weeks_df[weeks_df["week"].isin(sel_weeks)]
    else:
        weeks_df = weeks_df.tail(4)

    weeks = weeks_df["week"].tolist()

    if weeks_df.empty:
        return [], []

    latest_week = weeks_df["week_num"].iloc[-1]
    inventory_map = load_inventory(latest_week)

    # -------- AGGREGATE SALES FIRST (IMPORTANT FIX) ----------
    sales_agg = (
    sales_df
    .groupby(["model", "week", "week_num"], as_index=False)
    .agg({
        "brand": "first",
        "category_l0": "first",
        "category_l1": "first",
        "category_l2": "first",
        "units": "sum",
        "sales": "sum"
    })
)
    
    # -------- AGGREGATE BUSINESS FIRST (DUPLICATE SAFE) ----------
    business_agg = (
        business_df
        .groupby(["model", "week_num"], as_index=False)
        .agg({
            "sessions": "max",          # prevent duplication
            "conversion_pct": "mean"   # IMPORTANT: use mean
        })
    )

    # -------- MERGE ----------
    merged = sales_agg.merge(
        business_agg,
        on=["model", "week_num"],
        how="left"
    )

    merged["sessions"] = merged["sessions"].fillna(0)
    merged["conversion_pct"] = merged["conversion_pct"].fillna(0)

    # -------- AGGREGATE SALES ----------
    # -------- AGGREGATE SALES (FIXED) ----------
    merged_agg = (
    merged
    .groupby(
        ["model", "week"],   # ONLY model + week
        as_index=False
    )
    .agg({
        "brand": "first",
        "category_l0": "first",
        "category_l1": "first",
        "category_l2": "first",
        "units": "sum",
        "sales": "sum",
        "sessions": "max",          # prevent duplication
        "conversion_pct": "max"
    })
)

    # ============================================================
    # BUILD DATA STRUCTURE (FIXED INDENTATION)
    # ============================================================

    data = {}

    for _, r in merged_agg.iterrows():

        model = r["model"]
        week = r["week"]

        if model not in data:
            data[model] = {
                "brand": str(r.get("brand", "") or ""),
                "model": model,
                "category_l0": str(r.get("category_l0", "") or "").replace("nan", ""),
                "category_l1": str(r.get("category_l1", "") or "").replace("nan", ""),
                "category_l2": str(r.get("category_l2", "") or "").replace("nan", ""),
                "weeks": {}
            }

        data[model]["weeks"][week] = {
            "units": r["units"],
            "sales": r["sales"],
            "sessions": r["sessions"],
        }

    # -------- TOTAL SALES FOR % ----------
    total_sales = {
        w: sum(
            v["weeks"].get(w, {}).get("sales", 0)
            for v in data.values()
        ) or 1
        for w in weeks
    }

    rows = []

    # ============================================================
    # BUILD FINAL ROWS
    # ============================================================
    asin_by_model = load_asin_by_model()
    from weekly_app.core.master_override import model_to_skus
    sku_by_model_map = model_to_skus()

    for model, v in data.items():

        units_seq = [
            v["weeks"].get(w, {}).get("units", 0)
            for w in weeks
        ]

        sessions_seq = [
            v["weeks"].get(w, {}).get("sessions", 0)
            for w in weeks
        ]

        total_units = sum(units_seq)
        total_sessions = sum(sessions_seq)

        # Two distinct conversion KPIs (was: same formula in both → identical numbers):
        #   last_4w_conversion  = window-aggregate conversion
        #                         (total_units / total_sessions × 100)
        #                         — weighted by session volume
        #   avg_4w_conversion   = simple mean of weekly conversion rates
        #                         — each week weighted equally regardless of session volume
        weekly_conv = [
            (units_seq[i] / sessions_seq[i] * 100) if sessions_seq[i] > 0 else 0
            for i in range(len(units_seq))
        ]

        # ASIN + SKU sourced from master (comma-joined for models that
        # back multiple SKUs) so identifiers stay identical to master.
        model_u = str(model).upper().strip()
        skus_for_model = sku_by_model_map.get(model_u, [])
        row = {
            "model": model,
            "brand": v.get("brand"),
            "asin": asin_by_model.get(model, ""),
            "sku":  ", ".join(skus_for_model),
            "category_l0": v.get("category_l0"),
            "category_l1": v.get("category_l1"),
            "category_l2": v.get("category_l2"),

            "last_4w_units": total_units,
            "avg_4w_units": round(total_units / max(len(units_seq), 1), 2),

            "last_4w_sessions": total_sessions,
            "avg_4w_sessions": round(total_sessions / max(len(sessions_seq), 1), 2),

            "last_4w_conversion": round((total_units / total_sessions) * 100, 2) if total_sessions > 0 else 0,
            "avg_4w_conversion": round(sum(weekly_conv) / len(weekly_conv), 2) if weekly_conv else 0,

            "trend": trend(units_seq),
            "inventory_units": inventory_map.get(model, 0)

            }

        # -------- DYNAMIC WEEK FIELDS ----------
        for w in weeks:

            week_data = v["weeks"].get(w, {})

            row[f"{w}_units"] = week_data.get("units", 0)
            row[f"{w}_sales"] = round(week_data.get("sales", 0), 2)

            row[f"{w}_sales_pct"] = round(
                (week_data.get("sales", 0) / total_sales[w]) * 100,
                2
            )

            row[f"{w}_sessions"] = week_data.get("sessions", 0)
            u = week_data.get("units", 0)
            s = week_data.get("sessions", 0)

            row[f"{w}_conversion"] = round((u / s) * 100, 2) if s > 0 else 0

        rows.append(row)

    # ============================================================
    # GRAND TOTAL (FIXED POSITION)
    # ============================================================

    if rows:

        grand_total_row = {
            "model": "GRAND TOTAL",
            "brand": "",
            "asin": "",
            "sku":  "",
            "category_l0": "",
            "category_l1": "",
            "category_l2": "",
        }

        for w in weeks:
            grand_total_row[f"{w}_units"] = sum(r.get(f"{w}_units", 0) for r in rows)
            grand_total_row[f"{w}_sales"] = sum(r.get(f"{w}_sales", 0) for r in rows)
            grand_total_row[f"{w}_sessions"] = sum(r.get(f"{w}_sessions", 0) for r in rows)
            grand_total_row[f"{w}_conversion"] = round(
                sum(r.get(f"{w}_conversion", 0) for r in rows), 2
            )
            grand_total_row[f"{w}_sales_pct"] = 100

        # If only one SKU (filtered), show total at top
        if len(rows) == 1:
            rows.insert(0, grand_total_row)
        else:
            rows.append(grand_total_row)
        

    return rows, weeks


# ============================================================
# ROUTE – AMAZON + 1P ONLY
# ============================================================

# React SPA owns `/amazon-sales-trend`; JSON via add_api_route alias below.
def amazon_sales_trend(
    request: Request,
    brand: str = "All",                          # legacy single-brand kept for back-compat
    brands: List[str] = Query(default=[]),       # multi-brand checkboxes
    sel_weeks: Optional[List[str]] = Query(default=None)
):
    sales = load_sales()

    # -------- FILTER AMAZON + 1P ----------
    sales = sales[
        sales["channel"].astype(str).str.strip().str.lower()
        .isin(["amazon", "1p sales"])
    ]

    # Operator rule: Fossil is excluded from the Amazon + 1P trend page.
    # Drop before computing the brand dropdown so it doesn't surface there
    # either.  Sales snapshot still carries Fossil for other modules.
    sales = sales[sales["brand"].astype(str).str.strip().str.lower() != "fossil"]

    # -------- ALL BRANDS (for dropdown) — before brand filter ----------
    all_brands = sorted(sales["brand"].dropna().astype(str).unique())

    # Resolve the effective brand list: multi takes precedence over legacy single.
    eff_brands_lower = [b.strip().lower() for b in brands if b and b.strip().lower() != "all"]
    if not eff_brands_lower and brand and brand != "All":
        eff_brands_lower = [brand.strip().lower()]

    if eff_brands_lower:
        sales = sales[
            sales["brand"].astype(str).str.strip().str.lower().isin(eff_brands_lower)
        ]

    business = load_business()

    rows, weeks = build_amazon_sales_trend(sales, business, sel_weeks)

    # Get all available weeks for the picker (unfiltered)
    all_sales = load_sales()
    all_sales = all_sales[
        all_sales["channel"].astype(str).str.strip().str.lower()
        .isin(["amazon", "1p sales"])
    ]
    all_sales = all_sales[all_sales["brand"].astype(str).str.strip().str.lower() != "fossil"]
    all_weeks = sorted(
        all_sales["week"].dropna().unique().tolist(),
        key=lambda x: extract_week(x) or 0
    )

    # Pass selected brands back in their original (display) casing so the
    # checkboxes can match. We look up display name from all_brands.
    display_by_lower = {b.lower().strip(): b for b in all_brands}
    selected_brands_display = [display_by_lower[k] for k in eff_brands_lower if k in display_by_lower]

    if request.url.path.startswith("/api/"):
        from fastapi.responses import JSONResponse
        return JSONResponse(clean_nan({
            "rows":            rows,
            "weeks":           weeks,
            "all_weeks":       all_weeks,
            "brands":          all_brands,
            "selected_brands": selected_brands_display,
            "selected_weeks":  sel_weeks or [],
        }))

    return HTMLResponse(_env.get_template("sales_trend_amazon.html").render(
        request=request,
        rows=rows,
        weeks=weeks,
        all_weeks=all_weeks,
        brands=all_brands,
        selected_brands=selected_brands_display,
        selected_weeks=sel_weeks or [],
        page_title="Amazon + 1P Sales Trend",
    ))


# JSON alias for React frontend.
from fastapi.responses import JSONResponse as _JR_AM
router.add_api_route("/api/amazon-sales-trend", amazon_sales_trend, methods=["GET"], response_class=_JR_AM)
