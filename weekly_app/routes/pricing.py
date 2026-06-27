"""Pricing snapshot route — serves data/processed/price_snapshot.csv
as JSON for the React `Price` page.

The snapshot is built once a week by scripts/sp_pricing_pull.py (and
on every weekly-sync cron).  This route just reads + reshapes it; no
SP-API calls happen at request time.

Schema (one row per ASIN):
  asin, sku, brand, model,
  price_<account> for each of our seller accounts,
  buybox_price, buybox_seller_id, buybox_belongs_to_us,
  currency, fetched_at
"""
from __future__ import annotations

from pathlib import Path

import math
import pandas as pd
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from weekly_app.core.df_cache import load_csv_cached

router = APIRouter(prefix="/api/pricing", tags=["pricing"])

REPO_ROOT  = Path(__file__).resolve().parents[2]
SNAP_CSV   = REPO_ROOT / "data" / "processed" / "price_snapshot.csv"

# The seller-account columns we expect in the snapshot — mirrors the
# pull script's ACCOUNT_COL map.  Order here drives the column order
# in the API response so the React table stays deterministic.
ACCOUNT_COLUMNS = [
    ("price_audioarray",     "Audio Array"),
    ("price_nexlev",         "Nexlev"),
    ("price_viomi",          "Tonor"),  # VIOMI seller account hosts Tonor listings
    ("price_whitemulberry",  "White Mulberry"),
]


def _safe(v):
    """JSON-safe scalar.  NaN / +-inf / "nan" string → None."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    s = str(v).strip()
    if s.lower() in ("nan", "none", ""):
        return None
    return v


@router.get("")
def get_pricing():
    if not SNAP_CSV.exists():
        return JSONResponse(
            {
                "rows": [], "accounts": [c[1] for c in ACCOUNT_COLUMNS],
                "fetched_at": None,
                "empty_reason": (
                    "price_snapshot.csv not generated yet — "
                    "run scripts/sp_pricing_pull.py (or wait for the Mon cron)."
                ),
            },
            status_code=200,
        )

    try:
        df = load_csv_cached(SNAP_CSV)
    except Exception as e:
        return JSONResponse(
            {"rows": [], "accounts": [c[1] for c in ACCOUNT_COLUMNS],
             "fetched_at": None, "error": f"snapshot unreadable: {e}"},
            status_code=500,
        )

    if df.empty:
        return JSONResponse(
            {"rows": [], "accounts": [c[1] for c in ACCOUNT_COLUMNS],
             "fetched_at": None,
             "empty_reason": "snapshot present but contains no rows."},
            status_code=200,
        )

    # Normalise — strip strings, coerce numerics for the listed prices.
    for col in ["asin", "sku", "brand", "model",
                "buybox_seller_id", "currency", "fetched_at"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"nan": ""})

    for col, _ in ACCOUNT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA
    for col in ("buybox_price", "amazon_1p_price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA

    fetched_at = ""
    if "fetched_at" in df.columns and not df["fetched_at"].empty:
        # All rows in one pull share the same timestamp; just take the first non-empty.
        fetched_at = next((v for v in df["fetched_at"].tolist() if v), "")

    rows = []
    for _, r in df.iterrows():
        row = {
            "asin":  _safe(r.get("asin")),
            "sku":   _safe(r.get("sku")),
            "brand": _safe(r.get("brand")),
            "model": _safe(r.get("model")),
            "amazon_1p_price":      _safe(r.get("amazon_1p_price")),
            "buybox_price":         _safe(r.get("buybox_price")),
            "buybox_seller_id":     _safe(r.get("buybox_seller_id")),
            "buybox_belongs_to_us": bool(r.get("buybox_belongs_to_us")) if pd.notna(r.get("buybox_belongs_to_us")) else False,
            "currency":   _safe(r.get("currency")) or "INR",
        }
        for col, label in ACCOUNT_COLUMNS:
            row[label] = _safe(r.get(col))
        rows.append(row)

    return JSONResponse({
        "rows":       rows,
        "accounts":   [label for _, label in ACCOUNT_COLUMNS],
        "fetched_at": fetched_at,
    })
