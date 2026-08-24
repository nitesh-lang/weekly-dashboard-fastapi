"""Pull Pipeline + Open Order (imports / in-transit) from OrderPilot.

    python scripts/oms_imports.py --week 34          # add --dry-run to preview

Writes `data/raw/inventory/Week N/<Brand>/Imports (OrderPilot).xlsx` for the
four import brands.  `inventory_model_snapshot` rglobs every xlsx in the week
folder, so the file is picked up with no ETL wiring — it just has to carry the
same columns as the operator's sheet (SKU / ASIN / Brand / Model / Qty /
Channel / Week).

Channel mapping (reference_pipeline_vs_open_order_channel):
    import_tracker.pipeline_qty   -> "Pipeline"    in-transit FROM China
    import_tracker.open_order_qty -> "Open Order"  PO placed TO China

W34 onward, per operator 2026-08-24.  W33 and earlier keep whatever the
manual Inventory Snapshot carried — do NOT backfill, the numbers were
already published.

⚠ DOUBLE-COUNT GUARD: the operator's `Inventory Snapshot.xlsx` has
historically carried Pipeline / Open Order rows of its own.  When this file
exists for a week, `inventory_model_snapshot` drops those channels from the
operator's sheet for that week — see `_drop_operator_imports` there.  Without
that guard the same units would land twice.

Zero-qty rows are written too: a SKU whose pipeline went to 0 must show as 0
rather than vanish, or the week looks like the SKU was never in transit
(feedback_never_drop_rows_silently).
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW_INV = ROOT / "data" / "raw" / "inventory"
ORDERPILOT_ENV = Path(r"D:\OrderPilot by Cambium\orderpilot\backend\.env")

_ANCHOR_WEEK, _ANCHOR_SUN = 33, date(2026, 8, 9)

BRAND_FOLDER = {
    "audio_array":    "Audio_Array",
    "nexlev":         "Nexlev",
    "white_mulberry": "White_Mulberry",
    "tonor":          "Tonor",
}

SQL = """
SELECT sm.brand::text                       AS brand,
       sm.internal_sku                      AS sku,
       COALESCE(NULLIF(it.asin, ''), sm.ean, '') AS asin,
       COALESCE(NULLIF(it.model_name, ''), sm.model_code, '') AS model,
       COALESCE(it.pipeline_qty, 0)         AS pipeline_qty,
       COALESCE(it.open_order_qty, 0)       AS open_order_qty,
       it.pipeline_eta                      AS pipeline_eta
FROM import_tracker it
JOIN sku_master sm ON sm.id = it.sku_id
ORDER BY 1, 2
"""


def week_dates(week: int) -> tuple[date, date]:
    sun = _ANCHOR_SUN + timedelta(weeks=week - _ANCHOR_WEEK)
    return sun, sun + timedelta(days=6)


def database_url() -> str:
    url = os.environ.get("OMS_DATABASE_URL")
    if not url and ORDERPILOT_ENV.exists():
        m = re.search(r"^DATABASE_URL=(.+)$", ORDERPILOT_ENV.read_text(), re.M)
        url = m.group(1).strip().strip('"').strip("'") if m else None
    if not url:
        sys.exit("No DATABASE_URL (set OMS_DATABASE_URL or fix OrderPilot .env path)")
    return url


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.week < 34:
        sys.exit(f"--week {args.week}: imports come from OrderPilot for W34 ONWARD only. "
                 f"Earlier weeks were published from the manual Inventory Snapshot; "
                 f"backfilling would change already-published numbers.")

    import psycopg2
    start, end = week_dates(args.week)
    print(f"import_tracker snapshot -> Week {args.week} ({start} .. {end})")
    with psycopg2.connect(database_url()) as con:
        df = pd.read_sql(SQL, con)

    if df.empty:
        print("import_tracker returned no rows — nothing written.")
        return 1

    # Long-format: one row per (sku, channel), matching the operator sheet.
    frames = []
    for col, channel in (("pipeline_qty", "Pipeline"), ("open_order_qty", "Open Order")):
        f = df[["brand", "sku", "asin", "model", col]].copy()
        f = f.rename(columns={col: "Qty"})
        f["Channel"] = channel
        frames.append(f)
    long = pd.concat(frames, ignore_index=True)
    long["Week"] = f"Week {args.week}"
    long = long.rename(columns={"brand": "_brand", "sku": "SKU",
                                "asin": "ASIN", "model": "Model"})
    long["Brand"] = long["_brand"].str.replace("_", " ").str.title()

    eta = pd.to_datetime(df["pipeline_eta"], errors="coerce")
    if eta.notna().any():
        print(f"  pipeline ETA range: {eta.min().date()} .. {eta.max().date()}")

    grand_p = grand_o = 0
    for brand_key, bdf in long.groupby("_brand"):
        folder = BRAND_FOLDER.get(brand_key)
        if not folder:
            print(f"  WARN unknown brand {brand_key!r} — skipped")
            continue
        out = bdf[["SKU", "ASIN", "Brand", "Model", "Qty", "Channel", "Week"]].copy()
        p = int(out.loc[out.Channel == "Pipeline", "Qty"].sum())
        o = int(out.loc[out.Channel == "Open Order", "Qty"].sum())
        grand_p += p
        grand_o += o
        print(f"  {folder:<16} {len(out):>4} rows   Pipeline {p:>7,}   Open Order {o:>7,}")
        if args.dry_run:
            continue
        d = RAW_INV / f"Week {args.week}" / folder
        d.mkdir(parents=True, exist_ok=True)
        out.to_excel(d / "Imports (OrderPilot).xlsx", index=False)

    print(f"\n{'DRY RUN' if args.dry_run else 'WROTE'}   "
          f"Pipeline {grand_p:,}   Open Order {grand_o:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
