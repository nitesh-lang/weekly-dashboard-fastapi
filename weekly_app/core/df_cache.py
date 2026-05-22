"""
Shared DataFrame cache.

mtime-keyed in-memory cache for pandas reads.  The same `data/processed/*.csv`
and `data/master/sku_master.xlsx` files are read by many routes; without this
cache each request re-parses them, costing 200-3000 ms per call.

Cache returns a `.copy()` so route handlers can mutate the DataFrame without
poisoning subsequent requests.  The cost of the copy is negligible vs. a
re-read of the source file.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

_df_cache: Dict[str, Any] = {}


def load_csv_cached(path: Path) -> pd.DataFrame:
    """Read a CSV, returning a cached copy if the file's mtime hasn't changed."""
    key = str(path)
    mtime = path.stat().st_mtime if path.exists() else None
    if key in _df_cache and _df_cache[key]["mtime"] == mtime:
        return _df_cache[key]["df"].copy()
    df = pd.read_csv(path)
    _df_cache[key] = {"df": df, "mtime": mtime}
    return df.copy()


def load_excel_cached(path: Path) -> pd.DataFrame:
    """Read an Excel file (xlsx), returning a cached copy if mtime unchanged.

    Excel parsing is ~10x slower than CSV — caching the sku_master.xlsx is
    the single biggest perf win for the operator routes."""
    key = str(path)
    mtime = path.stat().st_mtime if path.exists() else None
    if key in _df_cache and _df_cache[key]["mtime"] == mtime:
        return _df_cache[key]["df"].copy()
    df = pd.read_excel(path)
    _df_cache[key] = {"df": df, "mtime": mtime}
    return df.copy()
