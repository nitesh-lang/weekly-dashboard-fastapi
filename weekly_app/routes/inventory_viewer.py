from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

INVENTORY_FILE = Path("data/processed/weekly_inventory_snapshot.csv")


@router.get("/viewer/inventory", response_class=HTMLResponse)
def inventory_snapshot_viewer(
    request: Request,
    week: str | None = None,
    brand: str | None = None,
    channel: str | None = None,
    sku_status: str | None = "MAPPED",
):
    # -----------------------------
    # FILE SAFETY
    # -----------------------------
    if not INVENTORY_FILE.exists():
        return templates.TemplateResponse(
            "inventory_viewer.html",
            {
                "request": request,
                "error": "Inventory snapshot not found. Run Inventory ETL first.",
                "rows": [],
                "weeks": [],
                "brands": [],
                "channels": [],
                "sku_statuses": ["MAPPED", "UNMAPPED"],
                "selected": {},
            },
        )

    df = pd.read_csv(INVENTORY_FILE)

    if df.empty:
        return templates.TemplateResponse(
            "inventory_viewer.html",
            {
                "request": request,
                "error": "Inventory snapshot is empty.",
                "rows": [],
                "weeks": [],
                "brands": [],
                "channels": [],
                "sku_statuses": ["MAPPED", "UNMAPPED"],
                "selected": {},
            },
        )

    # -----------------------------
    # DROPDOWN VALUES
    # -----------------------------
    weeks = sorted(df["week_start"].dropna().unique().tolist())
    brands = sorted(df["brand"].dropna().unique().tolist())
    channels = sorted(df["channel"].dropna().unique().tolist())

    # -----------------------------
    # APPLY FILTERS
    # -----------------------------
    filtered = df.copy()

    if week:
        filtered = filtered[filtered["week_start"] == week]

    if brand:
        filtered = filtered[filtered["brand"] == brand]

    if channel:
        filtered = filtered[filtered["channel"] == channel]

    if sku_status in ["MAPPED", "UNMAPPED"]:
        filtered = filtered[filtered["sku_status"] == sku_status]

    # -----------------------------
    # SORT FOR READABILITY
    # -----------------------------
    filtered = filtered.sort_values(
        by=["inventory_value", "inventory_units"],
        ascending=False,
        na_position="last",
    )

    rows = filtered.to_dict(orient="records")

    return templates.TemplateResponse(
        "inventory_viewer.html",
        {
            "request": request,
            "rows": rows,
            "weeks": weeks,
            "brands": brands,
            "channels": channels,
            "sku_statuses": ["MAPPED", "UNMAPPED"],
            "selected": {
                "week": week,
                "brand": brand,
                "channel": channel,
                "sku_status": sku_status,
            },
            "error": None,
        },
    )
