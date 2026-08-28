"""ASIN → Model / category_l2 lookup, cached on first use.

Source: backend/data/master/sku_master.xlsx (copy of the Weekly FastAPI
project's canonical sku_master). Reload cadence: on process restart or
after 30 min TTL — long enough to skip repeated reads, short enough that
updating the workbook doesn't require a redeploy.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pandas as pd

_MASTER_XLSX = Path(__file__).resolve().parent.parent / "data" / "master" / "sku_master.xlsx"
_TTL = 30 * 60
_lock = threading.Lock()
_cache: dict[str, tuple[float, dict[str, dict[str, str]]]] = {}


def _load() -> dict[str, dict[str, str]]:
    if not _MASTER_XLSX.exists():
        return {}
    df = pd.read_excel(_MASTER_XLSX)
    df.columns = [str(c).strip() for c in df.columns]
    if "ASIN" not in df.columns:
        return {}
    df = df.copy()
    df["ASIN"] = df["ASIN"].astype(str).str.strip().str.upper()
    df = df[df["ASIN"].ne("")].drop_duplicates(subset=["ASIN"], keep="first")

    def _clean(v) -> str:
        s = str(v or "").strip()
        # pandas NaN → 'nan', empty numeric zeros → '0'; neither is a real
        # category / model. Treat both as empty.
        return "" if s.lower() in ("", "nan", "none", "nat", "0", "0.0") else s

    out: dict[str, dict[str, str]] = {}
    for _, r in df.iterrows():
        asin = r["ASIN"]
        model = _clean(r.get("Model"))
        cat = _clean(r.get("category_l2")) or _clean(r.get("category_l1")) or _clean(r.get("category_l0"))
        out[asin] = {
            "model": model,
            "category_l2": cat,
            "brand": _clean(r.get("Brand")),
        }
    return out


def lookup_map() -> dict[str, dict[str, str]]:
    with _lock:
        hit = _cache.get("m")
        now = time.time()
        if hit and (now - hit[0]) < _TTL:
            return hit[1]
        data = _load()
        _cache["m"] = (now, data)
        return data


def get(asin: str) -> dict[str, str]:
    return lookup_map().get(str(asin).strip().upper(), {"model": "", "category_l2": "", "brand": ""})
