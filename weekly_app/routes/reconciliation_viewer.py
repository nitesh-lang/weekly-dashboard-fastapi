from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
INV_FILE = Path("data/processed/weekly_inventory_snapshot.csv")


# ======================================================
# HYGIENE
# ======================================================
def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "sku_status" in df.columns:
        df["sku_status"] = df["sku_status"].astype(str).str.strip().str.upper()

    for c in ["units_sold", "inventory_units"]:
        if c in df.columns:
            df[c] = df[c].fillna(0).astype(int)

    for c in ["gross_sales", "inventory_value", "nlc"]:
        if c in df.columns:
            df[c] = df[c].fillna(0).round(2)

    return df


# ======================================================
# RECONCILIATION VIEWER (SINGLE SOURCE OF TRUTH)
# ======================================================
@router.get("/viewer/reconciliation", response_class=HTMLResponse)
def reconciliation_viewer(
    request: Request,
    week: str | None = None,
    brand: str | None = None,
    channel: str | None = None,
    view: str = "mapped",
):
    if not SALES_FILE.exists() or not INV_FILE.exists():
        return templates.TemplateResponse(
            "reconciliation_viewer.html",
            {
                "request": request,
                "error": "Run Sales & Inventory ETL first.",
                "rows": [],
                "weeks": [],
                "brands": [],
                "channels": [],
                "selected": {},
            },
        )

    # ---------------- LOAD ----------------
    sales = clean(pd.read_csv(SALES_FILE))
    inv = clean(pd.read_csv(INV_FILE))

    # ---------------- FILTERS ----------------
    if week:
        sales = sales[sales["week_start"] == week]
        inv = inv[inv["week_start"] == week]

    if brand:
        sales = sales[sales["brand"] == brand]
        inv = inv[inv["brand"] == brand]

    if channel:
        sales = sales[sales["channel"] == channel]
        inv = inv[inv["channel"] == channel]

    if view == "mapped":
        sales = sales[sales["sku_status"] == "MAPPED"]
        inv = inv[inv["sku_status"] == "MAPPED"]

    # ---------------- AGGREGATE FIRST ----------------
    sales_g = sales.groupby(
        ["week_start", "brand", "channel", "sku", "sku_status"],
        as_index=False
    ).agg(
        units_sold=("units_sold", "sum"),
        gross_sales=("gross_sales", "sum"),
    )

    inv_g = inv.groupby(
        ["week_start", "brand", "channel", "sku", "sku_status"],
        as_index=False
    ).agg(
        inventory_units=("inventory_units", "sum"),
        inventory_value=("inventory_value", "sum"),
        nlc=("nlc", "max"),
        category_l0=("category_l0", "first"),
        category_l1=("category_l1", "first"),
        category_l2=("category_l2", "first"),
    )

    # ---------------- MERGE (TRUTH) ----------------
    final = (
        sales_g.merge(
            inv_g,
            on=["week_start", "brand", "channel", "sku", "sku_status"],
            how="outer",
        )
        .fillna(0)
    )

    # ---------------- FLAGS ----------------
    final["sell_through_gap"] = final["inventory_units"] - final["units_sold"]

    final["stockout_flag"] = final["units_sold"] > final["inventory_units"]
    final["dead_stock_flag"] = (
        (final["inventory_units"] > 0) & (final["units_sold"] == 0)
    )

    final["stockout_flag"] = final["stockout_flag"].map({True: "YES", False: "NO"})
    final["dead_stock_flag"] = final["dead_stock_flag"].map({True: "YES", False: "NO"})

    # ---------------- PRIORITY ----------------
    final["priority"] = 0
    final.loc[final["stockout_flag"] == "YES", "priority"] = 2
    final.loc[final["dead_stock_flag"] == "YES", "priority"] = 1

    final = final.sort_values(
        ["priority", "inventory_value", "gross_sales"],
        ascending=[False, False, False],
    )

    # ---------------- DROPDOWNS ----------------
    weeks = sorted(final["week_start"].dropna().unique().tolist())
    brands = sorted(final["brand"].dropna().unique().tolist())
    channels = sorted(final["channel"].dropna().unique().tolist())

    return templates.TemplateResponse(
        "reconciliation_viewer.html",
        {
            "request": request,
            "rows": final.to_dict("records"),
            "weeks": weeks,
            "brands": brands,
            "channels": channels,
            "selected": {
                "week": week,
                "brand": brand,
                "channel": channel,
                "view": view,
            },
            "error": None,
        },
    )
