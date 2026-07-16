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
FEES_CSV   = REPO_ROOT / "data" / "processed" / "referral_fees_snapshot.csv"

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
    """JSON-safe scalar.  NaN / +-inf / pd.NA / "nan" string → None."""
    if v is None:
        return None
    # pandas.NA / numpy.NaN / any missing marker
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    # numpy int / float wrappers -> native
    if hasattr(v, "item"):
        try:
            v = v.item()
        except Exception:
            pass
    s = str(v).strip()
    if s.lower() in ("nan", "none", "", "<na>"):
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

    # Merge in referral fees per ASIN (pre-GST, matches Manage Inventory
    # display).  Snapshot is populated by scripts/sp_referral_fees_pull.py.
    # If any ASIN has multiple account rows (rare cross-listing), keep
    # the latest by fetched_at.
    fees_by_asin: dict[str, dict] = {}
    if FEES_CSV.exists():
        try:
            fdf = load_csv_cached(FEES_CSV)
            fdf = fdf.sort_values("fetched_at").drop_duplicates("asin", keep="last")
            for _, fr in fdf.iterrows():
                fees_by_asin[str(fr.get("asin", "")).strip()] = {
                    "referral_pct":  _safe(fr.get("referral_pct")),
                    "referral_rs":   _safe(fr.get("referral_rs")),
                    "fba_fees_rs":   _safe(fr.get("fba_fees_rs")),
                    "total_fees_pct":_safe(fr.get("total_fees_pct")),
                    "price_used_rs": _safe(fr.get("price_used_rs")),
                }
        except Exception as e:
            print(f"[pricing route] fees merge skipped: {e!r}")

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
        # Attach per-ASIN referral fees (pre-GST) if we have them
        asin_key = str(r.get("asin", "")).strip()
        fee = fees_by_asin.get(asin_key, {})
        row["referral_pct"]   = fee.get("referral_pct")
        row["referral_rs"]    = fee.get("referral_rs")
        row["fba_fees_rs"]    = fee.get("fba_fees_rs")
        row["total_fees_pct"] = fee.get("total_fees_pct")
        row["fee_price_rs"]   = fee.get("price_used_rs")
        rows.append(row)

    return JSONResponse({
        "rows":       rows,
        "accounts":   [label for _, label in ACCOUNT_COLUMNS],
        "fetched_at": fetched_at,
    })
