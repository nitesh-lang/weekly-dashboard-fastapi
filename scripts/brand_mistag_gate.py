"""
Brand-mistag gate — pre-commit blocker for the weekly-sync workflow.

Reads pipeline_integrity_audit.xlsx sheet `27_ams_vs_sales_brand_history`
(populated by Check 27 in scripts/pipeline_integrity_audit.py) and fails
the workflow if it flagged any (brand, week) rows large enough to be a
mistag class of bug.

Check 27 was purpose-built for the 2026-07-13 Fossil-to-Audio-Array
mistag class (sku_master phantom-NaN-survivor dumps Fossil GMV into
brand=Audio Array via step4's drop_duplicates).  Its threshold is very
sensitive (Rs 10K AND >= 2%) so it flags any drift.  This gate is
tighter — only rows above both:

    |delta_rs|  >= Rs 5,00,000   (Rs 5 lakh)
    |delta_pct| >= 30%

trigger the hard-block.  Rationale: the 2026-07-13 incident was
+Rs 5.28 Cr across W14-W25 for Audio Array alone, roughly +150% per
week.  Ordinary drifts (WM 1P vendor SP-API blocked → -5..-15% for
W18-W23; current-week attribution lag → +5..+15% on W(latest)) stay
well below.

Override:
    Repo/workflow secret `SANITY_GATE_ALLOW=1` bypasses the gate
    (same override as the snapshot sanity gate).

Exit codes:
    0   — no definite mistags OR override in effect OR audit xlsx missing
    1   — one or more definite mistag rows (needs operator investigation)

Contract:
    A "clean" audit sheet has exactly one row with a `note` column like
    "clean - AMS Trend GMV vs Sales Trend Amazon+1P per brand (ALL weeks)".
    A "flagged" sheet has multiple rows without a `note`-only column.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd


AUDIT_XLSX = Path("data/processed/pipeline_integrity_audit.xlsx")
SHEET_NAME = "27_ams_vs_sales_brand_history"

GATE_DELTA_RS = 500_000.0   # Rs 5 lakh
GATE_DELTA_PCT = 30.0       # 30% of the sales-side value


def main() -> int:
    if os.environ.get("SANITY_GATE_ALLOW") == "1":
        print("[brand-mistag gate] SANITY_GATE_ALLOW=1 - skipping (operator override).")
        return 0

    if not AUDIT_XLSX.exists():
        print(f"[brand-mistag gate] {AUDIT_XLSX} missing; nothing to gate on.  Skipping.")
        return 0

    try:
        df = pd.read_excel(AUDIT_XLSX, sheet_name=SHEET_NAME)
    except ValueError as e:
        print(f"[brand-mistag gate] sheet '{SHEET_NAME}' not found ({e}); skipping.")
        return 0

    if "note" in df.columns and len(df) <= 1:
        print(f"[brand-mistag gate] Check 27 clean ({SHEET_NAME}).  Gate passed.")
        return 0

    df["_abs_rs"] = df["delta_rs"].abs()
    df["_abs_pct"] = df["delta_pct"].abs()
    mistag = df[(df["_abs_rs"] >= GATE_DELTA_RS) & (df["_abs_pct"] >= GATE_DELTA_PCT)]
    advisory = df[~df.index.isin(mistag.index)]

    show_cols = ["week", "brand", "sales_trend_amz_side",
                 "ams_trend_gmv", "delta_rs", "delta_pct"]

    if not advisory.empty:
        print(f"[brand-mistag gate] {len(advisory)} advisory row(s) below gate threshold "
              f"(|Rs| < {GATE_DELTA_RS:,.0f} OR |%| < {GATE_DELTA_PCT}%) - "
              f"logged, not blocking.")
        with pd.option_context("display.max_rows", 10,
                               "display.max_columns", None,
                               "display.width", 200):
            print(advisory[show_cols].head(10).to_string(index=False))
        print()

    if mistag.empty:
        print("[brand-mistag gate] no definite mistag rows.  Gate passed.")
        return 0

    n = len(mistag)
    print(f"::error::Brand-mistag gate FIRED - {n} brand/week row(s) above gate threshold"
          f" (|Rs| >= {GATE_DELTA_RS:,.0f} AND |%| >= {GATE_DELTA_PCT}%).")
    print("::error::This is the 2026-07-13 Fossil-to-Audio-Array mistag pattern."
          "  A step4 / step5 change may have regressed, OR the workflow ran"
          " with stale checked-out code.  Verify the fix landed + re-run this job.")
    print()
    print("Definite mistag rows:")
    with pd.option_context("display.max_rows", 20,
                           "display.max_columns", None,
                           "display.width", 200):
        print(mistag[show_cols].head(20).to_string(index=False))

    print()
    print("Bypass: set repo secret SANITY_GATE_ALLOW=1 for a single run"
          " (only if you've verified the drift is legitimate).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
