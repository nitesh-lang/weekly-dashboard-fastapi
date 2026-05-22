"""
Returns endpoint — exposes data/processed/returns_snapshot.csv as JSON.

Frontend joins by ASIN (canonical key) or SKU as fallback to show a
"Returns %" column on Dashboard SKU table + AMS Planning page.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()

RETURNS_CSV = Path("data/processed/returns_snapshot.csv")
_cache: dict = {"mtime": None, "body": None}


def _build_payload_json() -> str:
    if not RETURNS_CSV.exists():
        return json.dumps({"rows": [], "brands": [], "row_count": 0, "available": False})
    df = pd.read_csv(RETURNS_CSV)
    brands = sorted([b for b in df["brand"].dropna().unique().tolist() if str(b).strip()])
    rows_json = df.to_json(orient="records", date_format="iso")
    return (
        '{"rows":' + rows_json
        + ',"brands":' + json.dumps(brands)
        + ',"row_count":' + str(len(df))
        + ',"available":true}'
    )


@router.get("/api/returns")
def get_returns():
    if RETURNS_CSV.exists():
        mtime = RETURNS_CSV.stat().st_mtime
        if _cache["mtime"] == mtime and _cache["body"] is not None:
            body = _cache["body"]
        else:
            body = _build_payload_json()
            _cache["mtime"] = mtime
            _cache["body"]  = body
    else:
        body = _build_payload_json()
    return Response(content=body, media_type="application/json")
