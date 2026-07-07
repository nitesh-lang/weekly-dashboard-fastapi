"""
Snapshot sanity gate — pre-commit blocker for CURRENT-week regressions.

Runs in the weekly-sync workflow AFTER all ETL steps and BEFORE the git
commit.  Compares the freshly-generated snapshot CSVs against the
already-committed versions on origin/main.  Exits non-zero (fail-loud,
blocks the commit) if the current week's row count or unit total dropped
by more than the tolerance threshold.

Guards against the class of regression that hit on 2026-07-07:
  - 52505bd committed W27 inventory at 1488 rows / 182,422 u (good)
  - Next auto-sync (d251777) re-ran the ETL and wrote 1248 rows / 96,145 u
    — every manual channel down ~50% due to a Linux openpyxl parse quirk
  - Historical-preservation guard only defends weeks < max-1, so the
    current week was overwritten silently
  - The bad snapshot reached origin/main, then Render, then the operator
    caught it only via the Inventory Detail export

Contract:
  - Fresh row count < FLOOR × committed row count ->BLOCK
  - Fresh unit total < FLOOR × committed unit total ->BLOCK
  - Anything else ->pass silently

Bypass:
  Set SANITY_GATE_ALLOW=1 in the env to convert failures into warnings
  (used when the operator legitimately wants to accept a large drop,
  e.g. a brand exit or a channel being retired).
"""
from __future__ import annotations

import os
import subprocess
import sys
from io import StringIO
from pathlib import Path

import pandas as pd


# Threshold: fresh must be at least FLOOR × committed on both dimensions.
# 0.70 catches the ~50% AMPM/OpenOrder drops we've seen without tripping
# on legitimate week-over-week variation (typical WoW churn is <10%).
FLOOR = 0.70

# Snapshots we gate.  (path, unit_col) — unit_col is used to compute the
# per-week total.  Row count is the length of the current-week slice.
SNAPSHOTS = [
    ("data/processed/inventory_model_snapshot.csv", "inventory_units"),
    ("data/processed/weekly_sales_snapshot.csv",    "units_sold"),
]


def _current_week_label(df: pd.DataFrame) -> str | None:
    """Return the latest week label in the frame, e.g. 'Week 27'.
    Weeks are labelled 'Week N'; extract N and pick the max."""
    if "week" not in df.columns or df["week"].dropna().empty:
        return None
    weeks = df["week"].dropna().astype(str)
    ranked = weeks.str.extract(r"(\d+)").astype(float)
    top = ranked[0].idxmax()
    return weeks.loc[top]


def _committed_snapshot(path: str) -> pd.DataFrame | None:
    """Read the origin/main version of `path` via `git show`.  Returns
    None if the file doesn't exist upstream (fresh add) — no comparison
    possible, so the gate passes for that snapshot."""
    try:
        blob = subprocess.check_output(
            ["git", "show", f"origin/main:{path}"],
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return None
    try:
        return pd.read_csv(StringIO(blob.decode("utf-8", errors="replace")))
    except Exception as e:
        print(f"::warning::sanity-gate: could not parse committed {path}: {e}")
        return None


def _summarise(df: pd.DataFrame, week: str, unit_col: str) -> tuple[int, int]:
    """Return (row_count, total_units) for the given week slice."""
    slice_ = df[df["week"].astype(str) == week]
    rows = len(slice_)
    units = int(pd.to_numeric(slice_.get(unit_col, 0), errors="coerce").fillna(0).sum())
    return rows, units


def _refresh_origin() -> None:
    """Ensure origin/main is up-to-date locally so `git show` sees the
    latest committed snapshot to compare against."""
    subprocess.run(
        ["git", "fetch", "--quiet", "origin", "main"],
        check=False,
    )


def check_one(path: str, unit_col: str) -> list[str]:
    """Return a list of failure messages (empty if the snapshot is OK)."""
    fresh_path = Path(path)
    if not fresh_path.exists():
        return [f"{path}: missing on disk after ETL — nothing to commit"]

    try:
        fresh = pd.read_csv(fresh_path)
    except Exception as e:
        return [f"{path}: could not read fresh file — {e}"]

    committed = _committed_snapshot(path)
    if committed is None:
        # New file / brand-new snapshot — nothing to compare against.
        print(f"sanity-gate: {path} not on origin/main yet — skipping (first commit)")
        return []

    # Compare the CURRENT week (= max week in fresh) against the same week
    # in committed.  If the committed version doesn't have that week yet
    # (fresh week is newer), fall back to comparing against committed's
    # latest week — that catches "fresh has W27 but tiny" vs "committed
    # W26 was healthy" regressions too.
    week = _current_week_label(fresh)
    if not week:
        return [f"{path}: no week column / empty — refuse to commit"]

    if week in committed["week"].astype(str).values:
        cmp_week = week
    else:
        cmp_week = _current_week_label(committed)
        if not cmp_week:
            print(f"sanity-gate: {path} — committed has no weeks, skipping")
            return []

    fresh_rows,  fresh_units  = _summarise(fresh,     week,     unit_col)
    old_rows,    old_units    = _summarise(committed, cmp_week, unit_col)

    fails: list[str] = []
    if old_rows > 0 and fresh_rows < FLOOR * old_rows:
        fails.append(
            f"{path}: {week} rows dropped {old_rows} ->{fresh_rows} "
            f"({fresh_rows/old_rows:.0%} of committed, floor {FLOOR:.0%})"
        )
    if old_units > 0 and fresh_units < FLOOR * old_units:
        fails.append(
            f"{path}: {week} {unit_col} dropped {old_units:,} ->{fresh_units:,} "
            f"({fresh_units/old_units:.0%} of committed, floor {FLOOR:.0%})"
        )

    if not fails:
        print(
            f"sanity-gate: {path} {week} OK "
            f"(rows {fresh_rows} vs {old_rows}, units {fresh_units:,} vs {old_units:,})"
        )
    return fails


def main() -> int:
    _refresh_origin()

    all_fails: list[str] = []
    for path, unit_col in SNAPSHOTS:
        all_fails.extend(check_one(path, unit_col))

    if not all_fails:
        print("sanity-gate: all snapshots within floor — commit allowed")
        return 0

    banner = "SNAPSHOT SANITY GATE FAILED"
    print(f"::error::{banner}")
    for msg in all_fails:
        print(f"::error::  {msg}")

    if os.environ.get("SANITY_GATE_ALLOW") == "1":
        print("::warning::SANITY_GATE_ALLOW=1 set — proceeding despite failures")
        return 0

    print(
        "::error::Aborting before commit.  Investigate the ETL, fix the raw "
        "source, or set SANITY_GATE_ALLOW=1 to force-commit."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
