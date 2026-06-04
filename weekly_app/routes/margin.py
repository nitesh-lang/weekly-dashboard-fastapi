"""
Margin endpoint — exposes data/processed/margin_snapshot.csv as JSON.

The React frontend fetches this once and merges by (brand, model) into
whichever page wants to show net/gross margin.  Same auth + caching as
the other /api/* endpoints (cookie session via AuthGuardMiddleware).
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()

MARGIN_CSV = Path("data/processed/margin_snapshot.csv")

# Module-level cache — reread when mtime changes.  Cheap because CSV is small (~50 KB).
_cache: dict = {"mtime": None, "body": None}


def _build_payload_json() -> str:
    """
    Build the JSON body string.  Uses pandas' to_json so NaN → null is handled
    by the engine instead of Python's json module (which raises on NaN).
    """
    if not MARGIN_CSV.exists():
        return json.dumps({"rows": [], "brands": [], "row_count": 0, "available": False})

    from weekly_app.core.df_cache import load_csv_cached
    df = load_csv_cached(MARGIN_CSV)
    brands = sorted(df["brand"].dropna().unique().tolist())
    # to_json writes valid JSON with null for NaN, then we wrap it in an object.
    rows_json = df.to_json(orient="records", date_format="iso")
    return (
        '{"rows":' + rows_json +
        ',"brands":' + json.dumps(brands) +
        ',"row_count":' + str(len(df)) +
        ',"available":true}'
    )


@router.get("/api/margins")
def get_margins():
    if MARGIN_CSV.exists():
        mtime = MARGIN_CSV.stat().st_mtime
        if _cache["mtime"] == mtime and _cache["body"] is not None:
            body = _cache["body"]
        else:
            body = _build_payload_json()
            _cache["mtime"] = mtime
            _cache["body"]  = body
    else:
        body = _build_payload_json()
    return Response(content=body, media_type="application/json")
