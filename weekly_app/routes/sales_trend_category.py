from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
SKU_MASTER = Path("data/master/sku_master.xlsx")
RAW_INVENTORY = Path("data/raw/inventory")
RAW_AMS = Path("data/raw/ams")

# =====================================================
# HELPERS
# =====================================================
def week_key(w):
    try:
        return int("".join(filter(str.isdigit, str(w))))
    except:
        return -1


# =====================================================
# CATEGORY TREND PAGE
# =====================================================
@router.get("/sales-trend/category", response_class=HTMLResponse)
def category_trend(request: Request):

    # ---------- LOAD SALES ----------
    sales = pd.read_csv(SALES_FILE)
    sales["week"] = sales["week"].astype(str).str.strip()
    sales["model"] = sales["model"].astype(str).str.strip()

    # ---------- LOAD MASTER ----------
    master = pd.read_excel(SKU_MASTER)
    master.columns = (
        master.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    if "model_no" in master.columns and "model" not in master.columns:
        master = master.rename(columns={"model_no": "model"})

    # ---------- MERGE CATEGORY ----------
    sales = sales.merge(
        master[["model", "category_l0", "category_l1", "category_l2"]],
        on="model",
        how="left"
    )

    # ---------- WEEK LOGIC ----------
    weeks = sorted(
        [w for w in sales["week"].unique() if week_key(w) >= 0],
        key=week_key
    )
    last_4 = weeks[-4:]

    # ---------- AGGREGATE CATEGORY ----------
    cat = (
        sales[sales["week"].isin(last_4)]
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
        cat["units"] / total_units * 100 if total_units > 0 else 0
    ).round(2)

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

    return templates.TemplateResponse(
        "sales_trend_category.html",
        {
            "request": request,
            "weeks": last_4,
            "rows": rows,
        }
    )
