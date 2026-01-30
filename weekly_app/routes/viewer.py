from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")


@router.get("/ping")
def ping():
    return {"status": "viewer router active"}


@router.get("/viewer/sales", response_class=HTMLResponse)
def sales_snapshot_viewer(request: Request):
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

    rows = df.to_dict(orient="records")

    return templates.TemplateResponse(
        "sales_viewer.html",
        {
            "request": request,
            "rows": rows,
            "weeks": [],
            "brands": [],
            "channels": [],
            "selected": {},
            "error": None,
        },
    )
