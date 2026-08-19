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
    the single biggest perf win for the operator routes.

    Cold-start acceleration (2026-08-19): when WEEKLY_USE_PG=1 and the
    Postgres mirror holds a byte-identical copy of this file (size match in
    _sync_meta — sizes survive git checkouts, mtimes don't), the cold load
    comes from PG instead of parsing xlsx.  Warm hits stay pure in-memory
    exactly as before.  ANY doubt — size mismatch, table missing, DB down —
    silently parses the file, so behaviour can degrade to yesterday's,
    never below it."""
    key = str(path)
    mtime = path.stat().st_mtime if path.exists() else None
    if key in _df_cache and _df_cache[key]["mtime"] == mtime:
        return _df_cache[key]["df"].copy()
    df = _pg_cold_load(path)
    if df is None:
        df = pd.read_excel(path)
    _df_cache[key] = {"df": df, "mtime": mtime}
    return df.copy()


def _pg_cold_load(path: Path) -> "pd.DataFrame | None":
    """Fetch the mirror copy of `path`, but only when provably fresh."""
    try:
        from weekly_app.core import pg_query
        if not pg_query.enabled() or not path.exists():
            return None
        import re
        tbl = re.sub(r"[^a-z0-9_]", "_", path.stem.lower())
        eng = pg_query._get_engine()
        if eng is None:
            pg_query._count("df_cache", tbl, "fallback")
            return None
        from sqlalchemy import text
        with eng.connect() as con:
            row = con.execute(
                text("SELECT size_bytes FROM _sync_meta WHERE table_name = :t"),
                {"t": tbl},
            ).fetchone()
            if row is None or int(row[0]) != path.stat().st_size:
                pg_query._count("df_cache", tbl, "fallback")
                return None
            df = pd.read_sql(text(f'SELECT * FROM "{tbl}"'), con)
        if df.empty:
            pg_query._count("df_cache", tbl, "fallback")
            return None
        pg_query._count("df_cache", tbl, "pg")
        return df
    except Exception:
        try:
            from weekly_app.core import pg_query
            pg_query._count("df_cache", "unknown", "fallback")
        except Exception:
            pass
        return None
