from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List
import pandas as pd
from pathlib import Path
import urllib.parse
import re

from weekly_app.core.json_utils import clean_nan

router = APIRouter()
from jinja2 import Environment, FileSystemLoader
_env = Environment(loader=FileSystemLoader("weekly_app/templates"), cache_size=0)

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")

print("✅ CATEGORY_SALES.PY LOADED")



# -------------------------------------------------
# NORMALIZER (STRICT + SAFE)
# -------------------------------------------------
def norm(x):
    if pd.isna(x):
        return ""
    x = urllib.parse.unquote_plus(str(x))
    x = x.replace("&", "and")
    return " ".join(x.lower().strip().split())


def extract_week(v):
    """
    Converts:
      'Week 4' -> 4
      '4'      -> 4
      None     -> None
    """
    if pd.isna(v):
        return None
    m = re.search(r"(\d+)", str(v))
    return int(m.group(1)) if m else None


def format_inr(x):   # 👈 SAME INDENT LEVEL AS extract_week
    if pd.isna(x):
        return "₹ 0"
    return f"₹ {x:,.0f}"


# -------------------------------------------------
# CATEGORY SALES VIEW (L0 → L1 → L2)
# -------------------------------------------------
# React SPA owns `/category-sales`; JSON via add_api_route alias below.
def category_sales(
    request: Request,
    level: str = "l0",
    value: str | None = None,
    week: str | None = None,
    weeks: List[str] = Query(default=[]),
    sel_weeks: List[str] = Query(default=[]),
    brand: str | None = None,                       # legacy single-brand
    brands: List[str] = Query(default=[]),          # multi-brand checkboxes
):
    active = weeks or sel_weeks
    # ---------------- LOAD FILE ----------------
    if not SALES_FILE.exists():
        return HTMLResponse("Sales file not found", status_code=500)

    from weekly_app.core.df_cache import load_csv_cached
    df = load_csv_cached(SALES_FILE)

    # ---------------- NORMALIZE TEXT ----------------
    for c in ["brand"]:
        if c in df.columns:
            df[c] = df[c].apply(norm)
    for c in ["category_l0", "category_l1", "category_l2"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.title()

    # ---------------- WEEK NORMALIZATION ----------------
    df["week_num"] = df["week"].apply(extract_week)

    # Drop invalid weeks
    df = df[df["week_num"].notna()]

    # ---------------- BUILD AVAILABLE WEEKS (CRITICAL FIX) ----------------
    available_weeks = (
        df[["week", "week_num"]]
        .drop_duplicates()
        .sort_values("week_num")
        ["week"]
        .tolist()
    )

    latest_week_num = df["week_num"].max()

    # ---------------- WEEK FILTER ----------------
    if active:
        active_nums = [extract_week(w) for w in active if extract_week(w)]
        if active_nums:
            df = df[df["week_num"].isin(active_nums)]
            week = ", ".join(f"Week {n}" for n in sorted(active_nums))
        else:
            df = df[df["week_num"] == latest_week_num]
            week = f"Week {latest_week_num}"
    elif week not in (None, "", "None"):
        selected_week = extract_week(week)
        if selected_week is not None:
            df = df[df["week_num"] == selected_week]
            week = f"Week {selected_week}"
    else:
        df = df[df["week_num"] == latest_week_num]
        week = f"Week {latest_week_num}"

    # ---------------- BRAND FILTER ----------------
    # Multi-brand wins over legacy single brand.
    eff_brands = [norm(b) for b in (brands or []) if b and b.strip()]
    if not eff_brands and brand not in (None, "", "None"):
        eff_brands = [norm(brand)]
    if eff_brands:
        df = df[df["brand"].isin(eff_brands)]

    # ---------------- PARENT FILTER ----------------
    # Categories are stored title-cased (see normalization above); value comes
    # from the URL and is normalized to lowercase via norm(). Compare both
    # sides through norm() so the case difference doesn't silently drop rows.
    if value:
        value = norm(value)
        if level == "l1":
            df = df[df["category_l0"].apply(norm) == value]
        elif level == "l2":
            df = df[df["category_l1"].apply(norm) == value]

    # ---------------- GROUP COLUMN ----------------
    group_col = f"category_{level}"
    if group_col not in df.columns:
        return HTMLResponse(f"Invalid category level: {level}", status_code=400)

    # ---------------- AGGREGATION ----------------
    summary = (
        df.groupby(group_col, as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gross_sales=("gross_sales", "sum"),
        )
        .sort_values("gross_sales", ascending=False)
    )

    # ---------------- GRAND TOTAL ----------------
    total_row = pd.DataFrame([{group_col: "Grand Total",
                            "units_sold": summary["units_sold"].sum(),
                            "gross_sales": summary["gross_sales"].sum(),
                            }])
    summary = pd.concat([summary, total_row], ignore_index=True)
    # ✅ Keep as raw numbers — JS in template handles formatting
    summary["gross_sales"] = pd.to_numeric(summary["gross_sales"], errors="coerce").fillna(0).round(0).astype(int)
    summary["units_sold"] = pd.to_numeric(summary["units_sold"], errors="coerce").fillna(0).round(0).astype(int)

    # ── GMV % — compute before Grand Total distorts the denominator ──
    total_gmv = summary.iloc[:-1]["gross_sales"].sum() or 1  # exclude Grand Total row
    summary["gmv_pct"] = (summary["gross_sales"] / total_gmv * 100).round(1)
    summary.loc[summary.index[-1], "gmv_pct"] = 100.0  # Grand Total = 100%


    # ---------------- RENDER ----------------
    # Reuse cached frame instead of re-reading the CSV.
    all_brands = sorted(
        load_csv_cached(SALES_FILE)["brand"].dropna().apply(norm).unique().tolist()
    )

    if request.url.path.startswith("/api/"):
        from fastapi.responses import JSONResponse
        return JSONResponse(clean_nan({
            "rows":            summary.to_dict("records"),
            "weeks":           list(available_weeks),
            "brands":          all_brands,
            "level":           level,
            "value":           value,
            "week":            week,
            "selected_brands": eff_brands,
            "sel_weeks":       list(active) if active else [],
        }))

    return HTMLResponse(_env.get_template("category_sales.html").render(
        request=request,
        rows=summary.to_dict("records"),
        weeks=available_weeks,
        brands=all_brands,
        level=level,
        value=value,
        week=week,
        selected_brands=eff_brands,
        sel_weeks=active,
    ))


# JSON alias for React frontend.
from fastapi.responses import JSONResponse as _JR_CAT
router.add_api_route("/api/category-sales", category_sales, methods=["GET"], response_class=_JR_CAT)
