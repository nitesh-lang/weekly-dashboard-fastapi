from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path
import re

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
SKU_MASTER = Path("data/master/sku_master.xlsx")

print("✅ SALES_TREND_CATEGORY.PY LOADED")


# =====================================================
# HELPERS
# =====================================================
def extract_week(w):
    """
    Converts:
      'Week 4' -> 4
      '4'      -> 4
      None     -> None
    """
    if pd.isna(w):
        return None
    m = re.search(r"(\d+)", str(w))
    return int(m.group(1)) if m else None


# =====================================================
# CATEGORY TREND PAGE (LAST 4 WEEKS, DYNAMIC)
# =====================================================
@router.get("/sales-trend/category", response_class=HTMLResponse)
def category_trend(request: Request):

    # ---------- LOAD SALES SNAPSHOT ----------
    if not SALES_FILE.exists():
        return HTMLResponse("Sales snapshot not found", status_code=500)

    sales = pd.read_csv(SALES_FILE)

    sales["model"] = sales["model"].astype(str).str.strip()
    sales["week_num"] = sales["week"].apply(extract_week)

    # Drop rows with invalid week
    sales = sales[sales["week_num"].notna()]

    # ---------- LOAD SKU MASTER ----------
    master = pd.read_excel(SKU_MASTER)
    master.columns = (
        master.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    if "model_no" in master.columns and "model" not in master.columns:
        master = master.rename(columns={"model_no": "model"})

    master["model"] = master["model"].astype(str).str.strip()

    # ---------- MERGE CATEGORY ----------
    sales = sales.merge(
        master[["model", "category_l0", "category_l1", "category_l2"]],
        on="model",
        how="left"
    )

    # ---------- WEEK SELECTION (CRITICAL FIX) ----------
    # Always take the latest 4 weeks AVAILABLE
    all_weeks = sorted(sales["week_num"].unique())
    last_4_weeks = all_weeks[-4:] if len(all_weeks) >= 4 else all_weeks

    # Filter sales to last 4 weeks dynamically
    sales_4w = sales[sales["week_num"].isin(last_4_weeks)]

    # ---------- AGGREGATE CATEGORY ----------
    cat = (
        sales_4w
        .groupby(
            ["category_l0", "category_l1", "category_l2"],
            as_index=False
        )
        .agg(
            units=("units_sold", "sum"),
            sales_value=("gross_sales", "sum"),
        )
        .sort_values("units", ascending=False)
    )

    total_units = cat["units"].sum()
    cat["contribution_pct"] = (
        (cat["units"] / total_units * 100)
        if total_units > 0 else 0
    ).round(2)

    # ---------- FORMAT ROWS ----------
    rows = []
    for _, r in cat.iterrows():
        rows.append({
            "category_l0": r["category_l0"],
            "category_l1": r["category_l1"],
            "category_l2": r["category_l2"],
            "units": int(r["units"]),
            "sales_value": int(r["sales_value"]),
            "contribution_pct": float(r["contribution_pct"]),
        })

    # ---------- RENDER ----------
    return templates.TemplateResponse(
        "sales_trend_category.html",
        {
            "request": request,
            "weeks": [f"Week {w}" for w in last_4_weeks],
            "rows": rows,
        }
    )
