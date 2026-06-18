"""Shared category-backfill helpers.

Raw operator files (Inventory Snapshot.xlsx, amazon_sales.xlsx, other
channels, business_report) frequently don't carry category_l0/l1/l2
populated for every row.  Only the SP-API enriched files and select
master-aligned exports do.  Without backfill, downstream snapshots end
up with ~50% category coverage and the UI's category-filter pickers
return blank/incomplete results.

This helper pulls categories from sku_master.xlsx (the source of
truth) and gives ETLs a single function to fill missing categories
on any DataFrame that has ASIN and/or Model columns.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from weekly_app.core.df_cache import load_excel_cached


MASTER_PATH = Path("data/master/sku_master.xlsx").resolve()


def _norm_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().replace({"nan": "", "None": "", "<NA>": ""})


def load_master_category_maps(master_path: Path | None = None) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, Tuple[str, str, str]]]:
    """Build {ASIN → category_lN} and {Model → category triple} lookups.

    Returns (asin_l0, asin_l1, asin_l2, model_to_triple) where the model
    map gives the first non-empty (l0, l1, l2) for each Model key (uppercase).
    """
    p = Path(master_path) if master_path is not None else (Path(__file__).resolve().parents[2] / "data" / "master" / "sku_master.xlsx")
    asin_l0: Dict[str, str] = {}
    asin_l1: Dict[str, str] = {}
    asin_l2: Dict[str, str] = {}
    model_map: Dict[str, Tuple[str, str, str]] = {}
    if not p.exists():
        return asin_l0, asin_l1, asin_l2, model_map

    try:
        m = load_excel_cached(p)
    except Exception:
        return asin_l0, asin_l1, asin_l2, model_map
    m = m.copy()
    m.columns = m.columns.str.strip()
    if "ASIN" not in m.columns:
        return asin_l0, asin_l1, asin_l2, model_map

    m["ASIN_n"]  = _norm_str(m["ASIN"])
    m["Model_n"] = _norm_str(m["Model"]).str.upper() if "Model" in m.columns else ""
    for c in ("category_l0", "category_l1", "category_l2"):
        if c not in m.columns:
            m[c] = ""
        m[c] = _norm_str(m[c])

    for _, r in m.iterrows():
        a = r["ASIN_n"]
        if a:
            if r["category_l0"]: asin_l0[a] = r["category_l0"]
            if r["category_l1"]: asin_l1[a] = r["category_l1"]
            if r["category_l2"]: asin_l2[a] = r["category_l2"]
        mk = r["Model_n"]
        if mk and mk not in model_map:
            trip = (r["category_l0"], r["category_l1"], r["category_l2"])
            if any(trip):
                model_map[mk] = trip
    return asin_l0, asin_l1, asin_l2, model_map


def fill_categories(
    df: pd.DataFrame,
    asin_col: str = "asin",
    model_col: str = "model",
    brand_col: str = "brand",
    propagate_within: Tuple[str, ...] = ("brand", "model"),
) -> pd.DataFrame:
    """Backfill empty category_l0/l1/l2 on `df` using ASIN → Model lookups
    from sku_master, then propagate within the named group columns so
    every row of the same product carries the same category triple.

    Mutates and returns `df` for chaining.
    """
    a_l0, a_l1, a_l2, m_map = load_master_category_maps()

    # Ensure category cols exist
    for c in ("category_l0", "category_l1", "category_l2"):
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].astype(str).str.strip().replace({"nan": "", "None": "", "<NA>": ""})

    # Build normalized lookup keys without polluting the frame
    asin_keys  = df[asin_col].astype(str).str.strip() if asin_col in df.columns else None
    model_keys = df[model_col].astype(str).str.strip().str.upper() if model_col in df.columns else None

    for idx, (c, amap) in enumerate((
        ("category_l0", a_l0),
        ("category_l1", a_l1),
        ("category_l2", a_l2),
    )):
        empty = df[c].eq("") | df[c].isna()
        if asin_keys is not None and amap and empty.any():
            df.loc[empty, c] = asin_keys[empty].map(amap)
            df[c] = df[c].fillna("")
        empty = df[c].eq("") | df[c].isna()
        if model_keys is not None and m_map and empty.any():
            df.loc[empty, c] = model_keys[empty].map(
                lambda k: m_map.get(k, ("", "", ""))[idx]
            )
            df[c] = df[c].fillna("")

    # Propagate within group so straggling channel/type rows pick up
    # the master value resolved on a sibling row.
    if propagate_within and all(g in df.columns for g in propagate_within):
        for c in ("category_l0", "category_l1", "category_l2"):
            df[c] = df.groupby(list(propagate_within))[c].transform(
                lambda s: s.replace("", pd.NA).ffill().bfill().fillna("")
            )

    return df
