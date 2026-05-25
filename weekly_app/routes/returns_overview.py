"""
Returns Overview endpoint — per-ASIN view across the full SKU master.

Every ASIN in sku_master.xlsx gets a row (including Fossil and ASINs with no
returns), with returns + sales overlay attached.  This is the data source for
the dedicated /returns tab.

Source of truth:
  master ASINs  → sku_master.xlsx
  returns       → data/processed/returns_snapshot.csv (FBA + 1P combined)
  sold qty      → already pre-computed inside returns_snapshot (units_sold_30d)
                  for ASINs with returns; for ASINs without returns we still
                  compute the 4-week sum so every row has the denominator.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import APIRouter
from fastapi.responses import Response

from weekly_app.core.json_utils import clean_nan
from weekly_app.core.data_norm import normalize_keys

router = APIRouter()

MASTER     = Path("data/master/sku_master.xlsx")
RETURNS    = Path("data/processed/returns_snapshot.csv")
SALES      = Path("data/processed/weekly_sales_snapshot.csv")
SALES_WEEKS = 12

_cache: dict = {"mtime": None, "body": None}


def _latest_n_weeks(df: pd.DataFrame, n: int) -> list[int]:
    def _wk_num(w):
        try: return int(str(w).replace("Week", "").strip())
        except Exception: return -1
    nums = sorted({_wk_num(w) for w in df["week"].unique() if _wk_num(w) >= 0})
    return nums[-n:] if nums else []


def _sales_30d_by_asin(master_asin_to_skus: dict[str, list[str]]) -> dict[str, int]:
    """{asin → units sold across all channels in the last 4 weeks}.

    Folds SKU-level weekly sales onto ASIN using the master mapping.
    """
    if not SALES.exists():
        return {}
    df = pd.read_csv(SALES)
    df.columns = df.columns.str.strip()
    if not {"week", "sku", "units_sold"}.issubset(df.columns):
        return {}
    weeks = _latest_n_weeks(df, SALES_WEEKS)
    if not weeks:
        return {}
    def _wk_num(w):
        try: return int(str(w).replace("Week", "").strip())
        except Exception: return -1
    df["_wk"] = df["week"].apply(_wk_num)
    df = df[df["_wk"].isin(weeks)].copy()
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce").fillna(0)
    df["sku"] = df["sku"].astype(str).str.strip()
    by_sku = df.groupby("sku", as_index=False)["units_sold"].sum()
    sku_to_units = {row["sku"]: int(row["units_sold"]) for _, row in by_sku.iterrows()}

    out: dict[str, int] = {}
    for asin, skus in master_asin_to_skus.items():
        out[asin] = sum(sku_to_units.get(s, 0) for s in skus)
    return out


def _build_payload_json() -> str:
    if not MASTER.exists():
        return json.dumps({"rows": [], "brands": [], "row_count": 0, "available": False})

    master = pd.read_excel(MASTER)
    master.columns = master.columns.str.strip()
    normalize_keys(master)
    rename = {
        "FBA SKU":      "sku",
        "ASIN":         "asin",
        "Brand":        "brand",
        "Model":        "model",
        "category_l0":  "category_l0",
        "category_l1":  "category_l1",
    }
    master = master.rename(columns={k: v for k, v in rename.items() if k in master.columns})
    for c in ("sku", "asin", "brand", "model", "category_l0", "category_l1"):
        if c in master.columns:
            master[c] = master[c].astype(str).str.strip().replace({"nan": "", "None": ""})
        else:
            master[c] = ""

    # Keep rows that have an ASIN — that's the join key
    master = master[master["asin"].str.len() > 0]

    # Fossil isn't part of the AMS / 3P + 1P operator world; the returns
    # reports don't cover Fossil either, so 0-everywhere rows add noise.
    master = master[master["brand"].str.lower() != "fossil"]

    # Build ASIN → [SKUs] map (an ASIN can have multiple FBA SKUs)
    asin_to_skus: dict[str, list[str]] = {}
    asin_first: dict[str, dict] = {}
    for _, r in master.iterrows():
        asin = r["asin"]
        sku  = r["sku"]
        asin_to_skus.setdefault(asin, []).append(sku)
        if asin not in asin_first:
            asin_first[asin] = {
                "brand":       r.get("brand", ""),
                "model":       r.get("model", ""),
                "category_l0": r.get("category_l0", ""),
                "category_l1": r.get("category_l1", ""),
                "sku":         sku,
            }

    # Sales (last 4 weeks) per ASIN
    sales_lookup = _sales_30d_by_asin(asin_to_skus)

    # Returns lookup (from already-computed snapshot)
    returns_by_asin: dict[str, dict] = {}
    if RETURNS.exists():
        rdf = pd.read_csv(RETURNS)
        rdf.columns = rdf.columns.str.strip()
        for _, r in rdf.iterrows():
            asin = str(r.get("asin", "")).strip()
            if not asin:
                continue
            returns_by_asin[asin] = {
                "return_units":      int(r.get("return_units", 0) or 0),
                "returns_3p":        int(r.get("returns_3p", 0) or 0),
                "returns_1p":        int(r.get("returns_1p", 0) or 0),
                "sellable_units":    int(r.get("sellable_units", 0) or 0),
                "unsellable_units":  int(r.get("unsellable_units", 0) or 0),
                "sellable_pct":      float(r.get("sellable_pct", 0) or 0),
                "top_reason":        str(r.get("top_reason", "") or ""),
                "last_return_at":    str(r.get("last_return_at", "") or ""),
                "source_files":      str(r.get("source_files", "") or ""),
            }

    rows = []
    for asin, meta in asin_first.items():
        sold = int(sales_lookup.get(asin, 0))
        ret  = returns_by_asin.get(asin, {})
        units = int(ret.get("return_units", 0))
        return_pct = round(units / sold * 100, 1) if (units > 0 and sold > 0) else None
        rows.append({
            "brand":             meta["brand"],
            "model":             meta["model"],
            "sku":               meta["sku"],
            "asin":              asin,
            "category_l0":       meta["category_l0"],
            "category_l1":       meta["category_l1"],
            "units_sold_30d":    sold,
            "return_units":      units,
            "returns_3p":        int(ret.get("returns_3p", 0)),
            "returns_1p":        int(ret.get("returns_1p", 0)),
            "return_pct":        return_pct,
            "sellable_units":    int(ret.get("sellable_units", 0)),
            "unsellable_units":  int(ret.get("unsellable_units", 0)),
            "top_reason":        ret.get("top_reason", ""),
            "last_return_at":    ret.get("last_return_at", ""),
            "source_files":      ret.get("source_files", ""),
        })

    brands = sorted({r["brand"] for r in rows if r["brand"]})
    # The `default=` lambda below never fires for NaN floats because json.dumps
    # recognises floats natively (and would emit literal `NaN`).  Use clean_nan
    # to pre-walk the payload and replace NaN/Inf with None before serialising.
    return json.dumps(clean_nan({
        "rows":          rows,
        "brands":        brands,
        "row_count":     len(rows),
        "available":     True,
        "window_weeks":  SALES_WEEKS,
    }), ensure_ascii=False)


@router.get("/api/returns-overview")
def get_returns_overview():
    parts = [(p, p.stat().st_mtime if p.exists() else 0) for p in (MASTER, RETURNS, SALES)]
    cur = tuple(m for _, m in parts)
    if _cache["mtime"] == cur and _cache["body"] is not None:
        return Response(content=_cache["body"], media_type="application/json")
    body = _build_payload_json()
    _cache["mtime"] = cur
    _cache["body"]  = body
    return Response(content=body, media_type="application/json")
