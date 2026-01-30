from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from pathlib import Path
import pandas as pd
from io import StringIO

router = APIRouter(prefix="/export/ams", tags=["AMS"])

AMS_FILE = Path("data/processed/weekly_ams_snapshot.csv")


# ==================================================
# CSV RESPONSE
# ==================================================
def csv_response(df: pd.DataFrame, name: str):
    buf = StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={name}"},
    )


# ==================================================
# AMS SUMMARY EXPORT
# ==================================================
@router.get("/summary")
def export_ams_summary(
    week: str = Query(None),
    brand: str = Query(None),
):
    if not AMS_FILE.exists():
        return csv_response(pd.DataFrame(), "ams_summary.csv")

    df = pd.read_csv(AMS_FILE)

    if week:
        df = df[df["week_start"] == week]

    if brand:
        df = df[df["brand"] == brand]

    out = (
        df.groupby(["week_start", "brand"], as_index=False)
        .agg(
            spend=("spend", "sum"),
            sales=("sales", "sum"),
            orders=("orders", "sum"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
    )

    out["roas"] = out["sales"] / out["spend"].replace(0, pd.NA)
    out["acos"] = out["spend"] / out["sales"].replace(0, pd.NA)

    return csv_response(out.fillna(0), "ams_summary.csv")


# ==================================================
# AMS SKU EXPORT
# ==================================================
@router.get("/sku")
def export_ams_sku(
    week: str = Query(None),
    brand: str = Query(None),
):
    if not AMS_FILE.exists():
        return csv_response(pd.DataFrame(), "ams_sku.csv")

    df = pd.read_csv(AMS_FILE)

    if week:
        df = df[df["week_start"] == week]

    if brand:
        df = df[df["brand"] == brand]

    return csv_response(df, "ams_sku.csv")
