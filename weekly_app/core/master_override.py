"""
Render-time master enforcement.

Whenever a dashboard renders a row with an ASIN (or SKU), the Model
field shown to the operator must come from sku_master.xlsx — not from
whatever value was set when the snapshot was last built, and not from
intermediate transformations (e.g. uppercasing for case-insensitive
joins).  This module provides the lookup + override helpers used by
the route handlers right before they ship rows to the client.

Why render-time, not ETL-time:
  • The sync script does ETL-time enforcement (master overwrites SKU +
    Model in raw rows via ASIN lookup) so snapshots are usually clean.
  • But there's a window between (a) operator editing master and
    (b) the ETL chain re-running.  Without render-time override, the
    dashboard would show stale Model values during that window.
  • Some routes also uppercase Model for case-insensitive joins
    (ams_trend joins on Model with the inventory pivot).  Restoring
    master's casing at the end of the route preserves correctness
    of the join AND consistency of the displayed value.

The override is by-ASIN first (most specific), then by-SKU fallback.
"""
from __future__ import annotations
from pathlib import Path
import re

import pandas as pd

from weekly_app.core.df_cache import load_excel_cached

MASTER_PATH = Path("data/master/sku_master.xlsx")


def master_lookups() -> tuple[dict[str, dict], dict[str, dict]]:
    """Returns ({asin → {sku, model, brand}}, {sku → {asin, model, brand}}).
    Variation ASINs from master are included in the ASIN map so a
    child ASIN resolves to the same record as its parent."""
    try:
        m = load_excel_cached(MASTER_PATH)
    except Exception:
        return {}, {}
    m = m.copy()
    m.columns = m.columns.str.strip()

    asin_to_rec: dict[str, dict] = {}
    sku_to_rec:  dict[str, dict] = {}
    for _, r in m.iterrows():
        model = str(r.get("Model") or "").strip()
        if not model or model.lower() == "nan":
            continue
        asin = str(r.get("ASIN") or "").strip()
        sku  = str(r.get("FBA SKU") or "").strip()
        osku = str(r.get("Original SKU") or "").strip()
        brand = str(r.get("Brand") or "").strip()
        rec = {"model": model, "sku": sku, "asin": asin, "brand": brand}
        if asin and asin.lower() != "nan":
            asin_to_rec[asin] = rec
        if sku and sku.lower() != "nan":
            sku_to_rec.setdefault(sku, rec)
        if osku and osku != sku and osku.lower() != "nan":
            sku_to_rec.setdefault(osku, rec)
        v = str(r.get("Variation ASINs") or "").strip()
        if v and v.lower() != "nan":
            for x in re.split(r"[,\s/|;]+", v):
                x = x.strip()
                if x:
                    asin_to_rec.setdefault(x, rec)
    return asin_to_rec, sku_to_rec


def asin_to_model_map() -> dict[str, str]:
    """Compatibility shim — {asin → model}."""
    a, _ = master_lookups()
    return {k: v["model"] for k, v in a.items()}


def sku_to_model_map() -> dict[str, str]:
    """Compatibility shim — {sku → model}."""
    _, s = master_lookups()
    return {k: v["model"] for k, v in s.items()}


def model_to_skus() -> dict[str, list[str]]:
    """{MODEL_UPPER → [unique SKUs that share this model in master]}.
    Mirrors load_asin_by_model so model-aggregated routes can label
    each model row with its underlying SKUs."""
    try:
        m = load_excel_cached(MASTER_PATH)
    except Exception:
        return {}
    m = m.copy()
    m.columns = m.columns.str.strip()
    if "Model" not in m.columns or "FBA SKU" not in m.columns:
        return {}
    m["_model_u"] = m["Model"].astype(str).str.strip().str.upper()
    m["_sku"]     = m["FBA SKU"].astype(str).str.strip()
    out: dict[str, list[str]] = {}
    for model_u, grp in m.groupby("_model_u"):
        skus = sorted({
            s for s in grp["_sku"]
            if s and s.lower() not in ("nan", "none", "-")
        })
        out[model_u] = skus
    return out


def apply_master_model(
    df: pd.DataFrame,
    *,
    asin_col: str | None = None,
    sku_col: str | None = None,
    model_col: str = "model",
    overwrite_sku: bool = False,
) -> pd.DataFrame:
    """Override df[model_col] with master's Model where the row's ASIN
    (or SKU as fallback) is in master.  Mutates df in place; returns
    df for chainability.  Rows that don't resolve via either key are
    left untouched.

    Set overwrite_sku=True to ALSO overwrite the SKU column from master
    (only meaningful when looking up by ASIN — when looking up by SKU,
    the SKU is already canonical by definition)."""
    if model_col not in df.columns:
        return df
    asin_rec, sku_rec = master_lookups()
    if not asin_rec and not sku_rec:
        return df

    # Primary: by ASIN — overwrites Model (and optionally SKU)
    if asin_col and asin_col in df.columns and asin_rec:
        s = df[asin_col].astype(str).str.strip()
        mask = s.isin(asin_rec)
        if mask.any():
            df.loc[mask, model_col] = s[mask].map(lambda a: asin_rec[a]["model"])
            if overwrite_sku and sku_col and sku_col in df.columns:
                df.loc[mask, sku_col] = s[mask].map(
                    lambda a: asin_rec[a]["sku"] or df.loc[s == a, sku_col].iloc[0]
                )

    # Fallback: by SKU.  Catches rows whose ASIN is empty but SKU is in master.
    if sku_col and sku_col in df.columns and sku_rec:
        s = df[sku_col].astype(str).str.strip()
        mask = s.isin(sku_rec)
        if mask.any():
            df.loc[mask, model_col] = s[mask].map(lambda k: sku_rec[k]["model"])

    return df
