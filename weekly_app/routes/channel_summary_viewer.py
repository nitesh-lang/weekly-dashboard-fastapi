from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
INVENTORY_FILE = Path("data/processed/weekly_inventory_snapshot.csv")


@router.get("/viewer/channel-summary", response_class=HTMLResponse)
def channel_summary_viewer(
    request: Request,
    week: str | None = None,
    brand: str | None = None,
):
    if not SALES_FILE.exists() or not INVENTORY_FILE.exists():
        return templates.TemplateResponse(
            "channel_summary.html",
            {
                "request": request,
                "error": "Run Sales and Inventory ETL first.",
                "rows": [],
                "weeks": [],
                "brands": [],
                "selected": {},
            },
        )

    sales = pd.read_csv(SALES_FILE)
    inventory = pd.read_csv(INVENTORY_FILE)

    # -----------------------------
    # FILTER BASE
    # -----------------------------
    if week:
        sales = sales[sales["week_start"] == week]
        inventory = inventory[inventory["week_start"] == week]

    if brand:
        sales = sales[sales["brand"] == brand]
        inventory = inventory[inventory["brand"] == brand]

    # -----------------------------
    # SALES AGGREGATION
    # -----------------------------
    sales_agg = (
        sales
        .groupby(["week_start", "brand", "channel"], as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gross_sales=("gross_sales", "sum"),
        )
    )

    # -----------------------------
    # INVENTORY AGGREGATION
    # -----------------------------
    inv_agg = (
        inventory
        .groupby(["week_start", "brand", "channel"], as_index=False)
        .agg(
            inventory_units=("inventory_units", "sum"),
            inventory_value=("inventory_value", "sum"),
        )
    )

    # -----------------------------
    # MERGE
    # -----------------------------
    summary = sales_agg.merge(
        inv_agg,
        on=["week_start", "brand", "channel"],
        how="outer",
    ).fillna(0)

    # -----------------------------
    # DERIVED METRICS
    # -----------------------------
    summary["sell_through_pct"] = (
        summary["units_sold"] /
        summary["inventory_units"].replace(0, pd.NA)
    ) * 100

    summary["sell_through_pct"] = summary["sell_through_pct"].fillna(0).round(1)

    # -----------------------------
    # DROPDOWNS
    # -----------------------------
    weeks = sorted(
        set(sales["week_start"].unique()).union(inventory["week_start"].unique())
    )
    brands = sorted(
        set(sales["brand"].dropna().unique()).union(inventory["brand"].dropna().unique())
    )

    rows = summary.to_dict(orient="records")

    return templates.TemplateResponse(
        "channel_summary.html",
        {
            "request": request,
            "rows": rows,
            "weeks": weeks,
            "brands": brands,
            "selected": {
                "week": week,
                "brand": brand,
            },
            "error": None,
        },
    )
