import re
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")


def _wk_n(w: str) -> int:
    m = re.search(r"\d+", str(w))
    return int(m.group()) if m else 0


@router.get("/viewer/sales", response_class=HTMLResponse)
def sales_snapshot_viewer(
    request: Request,
    week: str | None = None,
    brand: str | None = None,
    channel: str | None = None,
):
    if not SALES_FILE.exists():
        return templates.TemplateResponse(
            "sales_viewer.html",
            {
                "request": request,
                "error": "Sales snapshot not found. Run Sales ETL first.",
                "rows": [],
                "weeks": [],
                "brands": [],
                "channels": [],
                "selected": {},
            },
        )

    df = pd.read_csv(SALES_FILE)
    df.columns = [c.strip().lower() for c in df.columns]

    # Distinct dropdown values come from the FULL (unfiltered) snapshot
    # so the user can always switch to a different combo.
    weeks = (
        sorted(df["week"].dropna().astype(str).unique().tolist(), key=_wk_n)
        if "week" in df.columns else []
    )
    brands = (
        sorted(df["brand"].dropna().astype(str).str.strip().unique().tolist())
        if "brand" in df.columns else []
    )
    channels = (
        sorted(df["channel"].dropna().astype(str).str.strip().unique().tolist())
        if "channel" in df.columns else []
    )

    # Apply server-side filters from query params
    filtered = df
    if week and "week" in filtered.columns:
        filtered = filtered[filtered["week"].astype(str) == str(week)]
    if brand and "brand" in filtered.columns:
        filtered = filtered[
            filtered["brand"].astype(str).str.strip() == brand.strip()
        ]
    if channel and "channel" in filtered.columns:
        filtered = filtered[
            filtered["channel"].astype(str).str.strip() == channel.strip()
        ]

    rows = filtered.to_dict(orient="records")

    return templates.TemplateResponse(
        "sales_viewer.html",
        {
            "request": request,
            "rows": rows,
            "weeks": weeks,
            "brands": brands,
            "channels": channels,
            "selected": {
                "week": week or "",
                "brand": brand or "",
                "channel": channel or "",
            },
            "error": None,
        },
    )
