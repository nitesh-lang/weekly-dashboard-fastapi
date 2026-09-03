"""Refresh buybox's sku_master from the WEEKLY project's master.

Operator rule (2026-09-03): the weekly repo's data/master/sku_master.xlsx
is the single source of truth — buybox's copy had drifted (missing 65
newer ASINs incl. the 2026-09 Fossil adds). Historical ASINs that exist
only in the buybox copy are KEPT (259 rows at cutover) so old months'
brand mapping never breaks; on any overlap the weekly row wins (verified
zero brand conflicts at cutover).

Run from anywhere; paths are repo-relative. The monthly-sync workflow
runs this before the pulls, so every monthly refresh maps brands with the
current weekly master.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent          # buybox_src/
WEEKLY_MASTER = ROOT.parent / "data" / "master" / "sku_master.xlsx"
BUYBOX_MASTER = ROOT / "data" / "master" / "sku_master.xlsx"


def main() -> None:
    weekly = pd.read_excel(WEEKLY_MASTER, dtype=str)
    if BUYBOX_MASTER.exists():
        legacy = pd.read_excel(BUYBOX_MASTER, dtype=str)
        keep = legacy[~legacy["ASIN"].isin(set(weekly["ASIN"].dropna()))]
        # align historical rows onto the weekly schema; unknown cols dropped
        keep = keep.reindex(columns=weekly.columns)
        merged = pd.concat([weekly, keep], ignore_index=True)
        extra = len(keep)
    else:
        merged, extra = weekly, 0
    BUYBOX_MASTER.parent.mkdir(parents=True, exist_ok=True)
    merged.to_excel(BUYBOX_MASTER, index=False)
    print(f"sku_master synced: {len(weekly)} weekly rows + {extra} historical-only = {len(merged)}")


if __name__ == "__main__":
    main()
