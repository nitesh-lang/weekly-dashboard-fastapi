import re
from typing import List
from fastapi import APIRouter, Request, Query
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
    # Persistence aliases — page_chrome.js forwards `weeks` and `sel_weeks`
    # from other modules' filter bars, so accept both here so the filter
    # isn't silently dropped on cross-page navigation.
    weeks: List[str] = Query(default=[]),
    sel_weeks: List[str] = Query(default=[]),
    brand: str | None = None,
    channel: str | None = None,
    sku: str | None = None,
    # Accepted (but unused — viewer has no mapped/all toggle) so a
    # ?view=all forwarded from analytics doesn't get rejected.
    view: str | None = None,
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

    # Resolve active week filter from any of the param names that the
    # other pages might have forwarded. Falls back to the legacy
    # single ?week= for backward compatibility with bookmarks.
    active_weeks = [w for w in list(weeks) + list(sel_weeks) if str(w).strip()]
    if not active_weeks and week:
        active_weeks = [week]

    # Distinct dropdown values come from the FULL (unfiltered) snapshot
    # so the user can always switch to a different combo.
    all_weeks = (
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
    if active_weeks and "week" in filtered.columns:
        filtered = filtered[filtered["week"].astype(str).isin(active_weeks)]
    if brand and "brand" in filtered.columns:
        filtered = filtered[
            filtered["brand"].astype(str).str.strip() == brand.strip()
        ]
    if channel and "channel" in filtered.columns:
        filtered = filtered[
            filtered["channel"].astype(str).str.strip() == channel.strip()
        ]
    if sku and "sku" in filtered.columns:
        filtered = filtered[
            filtered["sku"].astype(str).str.strip() == sku.strip()
        ]

    rows = filtered.to_dict(orient="records")

    # Single-week is the common case; the template's <select name="week">
    # uses the `selected.week` string to mark its option.
    selected_week_label = active_weeks[0] if len(active_weeks) == 1 else (week or "")

    return templates.TemplateResponse(
        "sales_viewer.html",
        {
            "request": request,
            "rows": rows,
            "weeks": all_weeks,
            "brands": brands,
            "channels": channels,
            "selected": {
                "week": selected_week_label,
                "brand": brand or "",
                "channel": channel or "",
                "sku": sku or "",
            },
            "error": None,
        },
    )
