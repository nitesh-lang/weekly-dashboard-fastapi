"""Re-pull specific Audio Array days on the SHIPPED (retail) vendor basis.

Why this exists
---------------
Ordered Revenue books a PO cancellation as a negative amount on the day
Amazon cancels, and Amazon never restates the original order date — a
re-pull of 03-08-2026 on 14-08-2026 returned byte-identical figures, so
the +₹4.79 L phantom PO on 03-08 and the -₹3.56 L reversal on 05-08 can
never self-heal. Shipped Revenue is the same retail scale and carries no
such reversal.

This writes the SAME way /api/{brand}/pull-sales does — same plan filter,
same replace-day delete, same save_ledger upsert — so nothing is special
about rows it produces. It exists only so a handful of days can be fixed
without waiting on a deploy.

Usage
-----
    cd backend
    # creds: either already in backend/.env, or exported first
    python ../scripts/repull_vendor_shipped.py 2026-08-03 2026-08-04 \
                                               2026-08-05 2026-08-06

    --brand audio_array   (default; only brand with a vendor account)
    --basis shipped       (default; 'ordered' reproduces the old behaviour)
    --dry-run             fetch and print, write nothing
    --pace 120            seconds between days; Amazon's vendor-report quota
                          is ~1 create/min, so do not go below ~60

Required env (same names Render uses):
    DATABASE_URL
    SP_LWA_CLIENT_ID_AUDIOARRAY
    SP_LWA_CLIENT_SECRET_AUDIOARRAY
    SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(BACKEND))
os.chdir(BACKEND)  # planning-file paths in services/*.py are relative to backend/

try:
    from dotenv import load_dotenv

    load_dotenv(BACKEND / ".env")
except ImportError:
    pass

import pandas as pd
from sqlalchemy import text

from app.brands import get_brand
from app.database import engine
from app.ledger_io import canonical_account, delete_day, save_ledger
from app.pullers.sp_client import SpApiError
from app.pullers.vendor_sales import pull_brand_vendor_sales


def day_totals(brand_key: str, day: str) -> tuple[float, float, int]:
    with engine.begin() as c:
        r = c.execute(
            text(
                "SELECT COALESCE(SUM(sales),0)::float, COALESCE(SUM(units),0)::float, "
                "COUNT(*) FROM ledger WHERE brand=:b AND account='Vendor Central' "
                "AND date=:d"
            ),
            {"b": brand_key, "d": day},
        ).fetchone()
    return float(r[0]), float(r[1]), int(r[2])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("dates", nargs="+", help="YYYY-MM-DD, one or more")
    ap.add_argument("--brand", default="audio_array")
    ap.add_argument("--basis", default="shipped", choices=["shipped", "ordered"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--pace", type=int, default=120)
    args = ap.parse_args()

    brand = get_brand(args.brand)
    if brand is None:
        print(f"unknown brand {args.brand!r}")
        return 2
    svc = brand.load_services()

    for i, ds in enumerate(args.dates):
        d = pd.to_datetime(ds).date()
        sales_date = pd.Timestamp(d)
        before = day_totals(brand.key, ds)

        try:
            vendor_rows = pull_brand_vendor_sales(brand, d, basis=args.basis)
        except SpApiError as e:
            print(f"[{ds}] FETCH FAILED — day left untouched: {e}")
            continue

        # Plan filter, identical to routers/sp_pull.py.
        plan_asins: set[str] = set()
        plan_df = svc.load_planning_main(sales_date)
        if not plan_df.empty and "asin" in plan_df.columns:
            plan_asins = set(plan_df["asin"].astype(str).str.strip())
        if not plan_asins:
            print(f"[{ds}] no planning ASINs for this month — refusing to write")
            continue

        built_frames = []
        counts: dict[str, int] = {}
        for label, rows in vendor_rows.items():
            acct = canonical_account(label) or label
            if not rows:
                counts[acct] = 0
                continue
            built = svc.build_rows_from_df(pd.DataFrame(rows), acct, sales_date, True)
            if built is not None and not built.empty:
                built = built[built["ASIN"].astype(str).isin(plan_asins)]
            counts[acct] = 0 if built is None or built.empty else len(built)
            if built is not None and not built.empty:
                built_frames.append(built)

        if not built_frames:
            # Same safety rule as the router: an empty response never wipes
            # rows that are already there.
            print(f"[{ds}] 0 rows after plan filter — day left untouched (before={before[0]:,.0f})")
            continue

        combined = pd.concat(built_frames, ignore_index=True)
        new_sales = float(combined["sales"].sum())
        new_units = float(combined["units"].sum())

        if args.dry_run:
            print(f"[{ds}] DRY RUN  before={before[0]:>12,.0f}/{before[1]:>6,.0f}u n={before[2]}"
                  f"   would write={new_sales:>12,.0f}/{new_units:>6,.0f}u n={len(combined)}")
        else:
            with engine.begin() as conn:
                delete_day(brand.key, d, list(counts.keys()), conn=conn)
                save_ledger(brand.key, combined, conn=conn)
            after = day_totals(brand.key, ds)
            print(f"[{ds}] before={before[0]:>12,.0f}/{before[1]:>6,.0f}u n={before[2]:<4}"
                  f"  after={after[0]:>12,.0f}/{after[1]:>6,.0f}u n={after[2]:<4}"
                  f"  basis={args.basis}")

        if i < len(args.dates) - 1:
            time.sleep(args.pace)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
