from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

AMS_FILE = Path("data/processed/weekly_ams_snapshot.csv")


def clean_round(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    int_cols = ["orders", "clicks", "impressions"]
    float_cols = ["spend", "sales", "roas", "acos", "tacos"]

    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).round(2)

    return df


@router.get("/drilldown/ams", response_class=HTMLResponse)
def ams_drilldown(
    request: Request,
    week: str | None = None,
    brand: str | None = None,
):
    if not AMS_FILE.exists():
        return HTMLResponse("AMS data not available", status_code=404)

    ams = pd.read_csv(AMS_FILE)

    # -------- Filters --------
    if week:
        ams = ams.loc[ams["week_start"] == week].copy()

    if brand:
        ams = ams.loc[ams["brand"] == brand].copy()

    # -------- Campaign Level --------
    df = (
        ams.groupby(
            ["campaign_name", "campaign_type"], as_index=False
        )
        .agg(
            spend=("spend", "sum"),
            sales=("sales", "sum"),
            orders=("orders", "sum"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
    )

    df["roas"] = (df["sales"] / df["spend"]).replace([pd.NA, pd.NaT], 0)
    df["acos"] = (df["spend"] / df["sales"]).replace([pd.NA, pd.NaT], 0)

    df = clean_round(df).sort_values("spend", ascending=False)

    rows = df.to_dict("records")

    return templates.TemplateResponse(
        "ams_drill.html",
        {
            "request": request,
            "rows": rows,
            "title": "AMS Drilldown – Campaign Performance",
        },
    )
