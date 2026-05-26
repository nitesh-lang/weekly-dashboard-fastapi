
# ============================================================
# SALES TREND – LAST 4 WEEKS (WITH BRAND FILTER)
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

router = APIRouter()
from jinja2 import Environment, FileSystemLoader
_env = Environment(loader=FileSystemLoader("weekly_app/templates"), cache_size=0)

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "processed"
MASTER_FILE = BASE_DIR / "data" / "master" / "sku_master.xlsx"


def load_asin_by_model() -> dict:
    """Returns {MODEL: 'ASIN1, ASIN2'} — comma-joined unique ASINs from
    sku_master. Models with no ASIN return ''. Used to label sales-trend
    rows (which aggregate at model level) with their underlying ASIN(s)."""
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
    out = {}
    for model, grp in m.groupby(model_col):
        # Defensive: coerce every value to str so a stray float NaN that
        # slipped past the filter can't crash str.join().  Same fix as
        # AM_sales_trend.load_asin_by_model.
        uniq = sorted({
            s for s in (str(v).strip() for v in grp[asin_col])
            if s and s.lower() not in ("nan", "none", "-")
        })
        out[model] = ", ".join(uniq)
    return out

# ============================================================
# NORMALIZATION
# ============================================================

def norm(x):
    return str(x).strip()

def norm_model(x):
    return str(x).strip().upper()

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

    df = df.rename(columns={
        "units_sold": "units",
        "gross_sales": "sales"
    })

    for c in ["week", "model", "brand", "units", "sales"]:
        if c not in df.columns:
            df[c] = 0

    df["brand"] = df["brand"].astype(str).str.strip().str.lower()
    df["model"] = df["model"].apply(norm_model)
    df["units"] = pd.to_numeric(df["units"], errors="coerce").fillna(0).astype(int)
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)

    df["week_num"] = df["week"].astype(str).apply(
        lambda x: int(re.search(r"\d+", x).group()) if re.search(r"\d+", x) else None
    )

    return df

# ============================================================
# INVENTORY SNAPSHOT (LATEST WEEK)
# ============================================================

def load_inventory(latest_week):
    try:
        f = find_file(DATA_DIR, ["inventory_model_snapshot"])
    except FileNotFoundError:
        return {}

    df = load_csv_cached(f)
    df.columns = [c.strip().lower() for c in df.columns]

    df["model"] = df["model"].apply(norm_model)
    df["inventory_units"] = pd.to_numeric(
        df["inventory_units"], errors="coerce"
    ).fillna(0)

    df["week_num"] = df["week"].astype(str).apply(
        lambda x: int(re.search(r"\d+", x).group()) if re.search(r"\d+", x) else None
    )

    df = df[df["week_num"] == latest_week]
    return df.set_index("model")["inventory_units"].to_dict()

# ============================================================
# TREND LOGIC
# ============================================================

def trend(units):
    """UP / DOWN / FLAT / N/A across an arbitrary-length week sequence.

    Same rules as AM_sales_trend.trend() — see that docstring.
    """
    if not units:
        return "FLAT"
    if len(units) == 1:
        return "N/A"
    if len(units) == 2:
        a, b = units
        if b > a:
            return "UP"
        if b < a:
            return "DOWN"
        return "FLAT"

    last = units[-1]
    prior = units[:-1]
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
# ROUTE
# ============================================================

# React SPA owns `/sales-trend`; JSON via add_api_route alias below.
def sales_trend(
    request: Request,
    brand: str = "All",                              # legacy single-brand
    brands: List[str] = Query(default=[]),           # multi-brand checkboxes
    sel_weeks: Optional[List[str]] = Query(default=None)
):
    sales = load_sales()

    all_weeks = sorted(
        sales["week"].dropna().unique().tolist(),
        key=lambda x: int(re.search(r"\d+", str(x)).group()) if re.search(r"\d+", str(x)) else 0
    )

    # Resolve effective brand list: multi wins over legacy single.
    eff_brands_lower = [b.strip().lower() for b in (brands or []) if b and b.strip().lower() != "all"]
    if not eff_brands_lower and brand and brand != "All":
        eff_brands_lower = [brand.strip().lower()]

    base = sales
    if eff_brands_lower:
        base = sales[sales["brand"].isin(eff_brands_lower)]

    weeks_df = (
        base[["week", "week_num"]]
        .dropna()
        .drop_duplicates()
        .sort_values("week_num")
    )

    if sel_weeks:
        weeks_df = weeks_df[weeks_df["week"].isin(sel_weeks)]
    else:
        weeks_df = weeks_df.tail(4)

    weeks = weeks_df["week"].tolist()
    latest_week = weeks_df["week_num"].iloc[-1]

    inventory = load_inventory(latest_week)

    data = {}

    # FIX: iterate over base (brand-filtered), NOT full sales
    for _, r in base.iterrows():
        model = r["model"]
        week = r["week"]

        data.setdefault(model, {
            "brand": str(r.get("brand", "") or ""),
            "model": model,
            "category_l0": str(r.get("category_l0", "") or "").replace("nan", ""),
            "category_l1": str(r.get("category_l1", "") or "").replace("nan", ""),
            "category_l2": str(r.get("category_l2", "") or "").replace("nan", ""),
            "weeks": {}
        })

        data[model]["weeks"].setdefault(week, {"units": 0, "sales": 0})
        data[model]["weeks"][week]["units"] += r["units"]
        data[model]["weeks"][week]["sales"] += r["sales"]

    total_sales = {
        w: sum(v["weeks"].get(w, {}).get("sales", 0) for v in data.values()) or 1
        for w in weeks
    }

    asin_by_model = load_asin_by_model()
    from weekly_app.core.master_override import model_to_skus
    sku_by_model_map = model_to_skus()
    rows = []

    for model, v in data.items():
        units_seq = [v["weeks"].get(w, {}).get("units", 0) for w in weeks]

        # Source ASIN + SKU from master via the model's uppercase key.
        # Same model can carry multiple SKUs / ASINs — join with commas
        # so the cell stays scannable while keeping all the identifiers.
        model_u = str(model).upper().strip()
        skus_for_model = sku_by_model_map.get(model_u, [])
        row = {
            "model": model,
            "brand": v.get("brand"),
            "asin": asin_by_model.get(model, ""),
            "sku":  ", ".join(skus_for_model),
            "category_l0": v["category_l0"],
            "category_l1": v["category_l1"],
            "category_l2": v["category_l2"],
            "last_4w_units": sum(units_seq),
            "avg_4w": round(sum(units_seq) / max(len(units_seq), 1), 2),
            "trend": trend(units_seq),
            "inventory_units": inventory.get(model, 0)
        }

        for w in weeks:
            s = v["weeks"].get(w, {}).get("sales", 0)
            u = v["weeks"].get(w, {}).get("units", 0)
            row[f"{w}_units"] = u
            row[f"{w}_sales"] = round(s, 2)
            row[f"{w}_sales_pct"] = round((s / total_sales[w]) * 100, 2)

        rows.append(row)
    # ================= GRAND TOTAL =================
    grand = {
        "model": "Grand Total",
        "brand": "",
        "asin": "",
        "sku":  "",
        "category_l0": "",
        "category_l1": "",
        "category_l2": "",
         "inventory_units": "",
          "trend": "",
          }
    for w in weeks:
        grand[f"{w}_units"] = sum(r[f"{w}_units"] for r in rows)
        grand[f"{w}_sales"] = round(sum(r[f"{w}_sales"] for r in rows), 2)
        grand[f"{w}_sales_pct"] = 0.0

    grand["last_4w_units"] = sum(r["last_4w_units"] for r in rows)
    grand["avg_4w"] = 0.0

        
    rows.append(grand)   # ✅ HERE (after loop ends)

    all_brands = sorted(sales["brand"].dropna().astype(str).unique())
    # Map lowercase keys back to display casing so the picker can match.
    display_by_lower = {b.lower().strip(): b for b in all_brands}
    selected_brands_display = [display_by_lower[k] for k in eff_brands_lower if k in display_by_lower]

    # Send ALL rows in the initial HTML
    all_rows = rows
    trend_total = len(all_rows)

    if request.url.path.startswith("/api/"):
        from fastapi.responses import JSONResponse
        return JSONResponse(clean_nan({
            "rows": all_rows,
            "trend_total": trend_total,
            "weeks": weeks,
            "all_weeks": all_weeks,
            "brands": all_brands,
            "selected_brands": selected_brands_display,
            "selected_weeks": sel_weeks or [],
        }))

    return HTMLResponse(_env.get_template("sales_trend_sku.html").render(
        request=request,
        rows=all_rows,
        trend_total=trend_total,
        weeks=weeks,
        all_weeks=all_weeks,
        brands=all_brands,
        selected_brands=selected_brands_display,
        selected_weeks=sel_weeks or [],
    ))



# ── JSON API: paginated rows for sales trend infinite scroll ──
@router.get("/api/sales-trend/rows")
def sales_trend_rows_api(
    request: Request,
    brand: str = "All",                              # legacy single-brand
    brands: List[str] = Query(default=[]),           # multi-brand checkboxes
    sel_weeks: Optional[List[str]] = Query(default=None),
    page: int = 1,
    page_size: int = 100,
):
    from fastapi.responses import JSONResponse
    try:
        sales = load_sales()
        all_wks = sorted(sales["week"].dropna().unique().tolist(),
            key=lambda x: int(re.search(r"\d+", str(x)).group()) if re.search(r"\d+", str(x)) else 0)

        eff_brands_lower = [b.strip().lower() for b in (brands or []) if b and b.strip().lower() != "all"]
        if not eff_brands_lower and brand and brand != "All":
            eff_brands_lower = [brand.strip().lower()]

        base = sales
        if eff_brands_lower:
            base = sales[sales["brand"].isin(eff_brands_lower)]

        weeks_df = base[["week","week_num"]].dropna().drop_duplicates().sort_values("week_num")
        if sel_weeks:
            weeks_df = weeks_df[weeks_df["week"].isin(sel_weeks)]
        else:
            weeks_df = weeks_df.tail(4)
        weeks = weeks_df["week"].tolist()

        data = {}
        for _, r in base.iterrows():
            m, w = r["model"], r["week"]
            data.setdefault(m, {"brand":str(r.get("brand","")or""),"model":m,
                "category_l0":str(r.get("category_l0","")or""),
                "category_l1":str(r.get("category_l1","")or""),
                "category_l2":str(r.get("category_l2","")or""), "weeks":{}})
            data[m]["weeks"].setdefault(w, {"units":0,"sales":0})
            data[m]["weeks"][w]["units"] += r["units"]
            data[m]["weeks"][w]["sales"] += r["sales"]

        total_sales = {w: sum(v["weeks"].get(w,{}).get("sales",0) for v in data.values()) or 1 for w in weeks}
        inventory = load_inventory(weeks_df["week_num"].iloc[-1] if not weeks_df.empty else 0)
        asin_by_model = load_asin_by_model()

        rows = []
        for m, v in data.items():
            units_seq = [v["weeks"].get(w,{}).get("units",0) for w in weeks]
            row = {"model":m,"brand":v.get("brand"),"asin":asin_by_model.get(m,""),
                   "category_l0":v["category_l0"],
                   "category_l1":v["category_l1"],"category_l2":v["category_l2"],
                   "last_4w_units":sum(units_seq),"avg_4w":round(sum(units_seq)/max(len(units_seq),1),2),
                   "trend":trend(units_seq),"inventory_units":inventory.get(m,0)}
            for w in weeks:
                s = v["weeks"].get(w,{}).get("sales",0)
                u = v["weeks"].get(w,{}).get("units",0)
                row[f"{w}_units"] = u
                row[f"{w}_sales"] = round(s,2)
                row[f"{w}_sales_pct"] = round((s/total_sales[w])*100,2)
            rows.append(row)

        start = (page-1)*page_size
        end   = start+page_size
        return JSONResponse({"rows":rows[start:end],"total":len(rows),"page":page,"has_more":end<len(rows)})
    except Exception:
        import traceback; traceback.print_exc()
        return JSONResponse({"rows":[],"total":0,"page":page,"has_more":False})


# JSON alias for the React frontend.
from fastapi.responses import JSONResponse as _JR
router.add_api_route("/api/sales-trend", sales_trend, methods=["GET"], response_class=_JR)
