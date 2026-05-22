"""
Sync-status endpoint — exposes mtimes of the key processed snapshots so the
React sidebar can show "Synced · 2h ago" instead of static text.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter

router = APIRouter()

SNAPSHOTS = {
    "sales":     Path("data/processed/weekly_sales_snapshot.csv"),
    "inventory": Path("data/processed/inventory_model_snapshot.csv"),
    "ams":       Path("data/ams_weekly_data/ams_weekly_fact/ams_weekly_fact_with_category.csv"),
    "margin":    Path("data/processed/margin_snapshot.csv"),
}


def _file_info(path: Path) -> dict:
    if not path.exists():
        return {"present": False, "mtime": None, "latest_week": None}
    stat = path.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
    # Try to read the latest week if the file has a "week" column — best-effort.
    latest_week: Optional[str] = None
    try:
        df = pd.read_csv(path, usecols=lambda c: c.lower() == "week", nrows=20_000)
        if not df.empty and df.columns.tolist():
            col = df.columns[0]
            weeks = df[col].dropna().astype(str).str.strip().unique().tolist()
            def _wk(w: str) -> int:
                try:    return int(w.replace("Week", "").strip())
                except Exception: return -1
            weeks = [w for w in weeks if _wk(w) >= 0]
            if weeks:
                weeks.sort(key=_wk)
                latest_week = weeks[-1]
    except Exception:
        pass
    return {
        "present":     True,
        "mtime":       mtime,
        "size_bytes":  stat.st_size,
        "latest_week": latest_week,
    }


@router.get("/api/sync-status")
def sync_status():
    snapshots = {name: _file_info(p) for name, p in SNAPSHOTS.items()}
    # Newest mtime across all snapshots — the headline "data is X old" number.
    mtimes = [s["mtime"] for s in snapshots.values() if s["mtime"]]
    newest = max(mtimes) if mtimes else None
    return {
        "newest_mtime": newest,
        "snapshots":    snapshots,
    }
