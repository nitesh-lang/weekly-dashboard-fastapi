"""One-shot cleanup: delete ledger rows whose ASIN isn't in the corresponding
month's planning file. Uses the Neon connection string + on-disk planning
workbooks. Prints a preview by default; pass --commit to actually delete.

Safe: works per (brand, month). Rows with a plan-file miss for that month
are removed; rows where the plan file itself is missing are preserved.
"""
from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path

import pandas as pd
import psycopg2

DEST_URL = os.getenv(
    "DEST_URL",
    "postgresql://neondb_owner:npg_DFo7HqWST0hs"
    "@ep-noisy-rain-aztnfk88.c-3.ap-southeast-1.aws.neon.tech/neondb?sslmode=require",
)

PLANNING_ROOT = Path(__file__).resolve().parent.parent / "backend" / "data" / "planning"


def load_plan(brand_key: str, month_str: str) -> set[str]:
    stamp = pd.to_datetime(month_str + "-01").strftime("%b %Y")
    hits = glob.glob(str(PLANNING_ROOT / brand_key / f"ASIN Planning file - {stamp}.xlsx"))
    if not hits:
        return set()
    df = pd.read_excel(hits[0])
    df.columns = [str(c).strip().lower().replace(" ", "") for c in df.columns]
    if "asin" not in df.columns:
        return set()
    return set(df["asin"].astype(str).str.strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--brand", choices=("nexlev", "audio_array"), required=True)
    ap.add_argument("--commit", action="store_true", help="Actually delete; default is dry-run")
    args = ap.parse_args()

    with psycopg2.connect(DEST_URL) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT to_char(date, 'YYYY-MM') FROM ledger WHERE brand = %s ORDER BY 1",
            (args.brand,),
        )
        months = [r[0] for r in cur.fetchall()]
        print(f"{args.brand}: {len(months)} months in ledger")

        grand_del = 0
        grand_kept = 0
        for m in months:
            asins = load_plan(args.brand, m)
            if not asins:
                cur.execute(
                    "SELECT COUNT(*) FROM ledger WHERE brand=%s AND to_char(date,'YYYY-MM')=%s",
                    (args.brand, m),
                )
                keep = cur.fetchone()[0]
                grand_kept += keep
                print(f"  {m}  plan_asins= 0 (MISSING FILE)  keeping {keep} rows (no filter)")
                continue

            cur.execute(
                "SELECT COUNT(*) FROM ledger WHERE brand=%s AND to_char(date,'YYYY-MM')=%s AND asin <> ALL(%s::text[])",
                (args.brand, m, list(asins)),
            )
            del_count = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM ledger WHERE brand=%s AND to_char(date,'YYYY-MM')=%s AND asin = ANY(%s::text[])",
                (args.brand, m, list(asins)),
            )
            keep_count = cur.fetchone()[0]
            grand_del += del_count
            grand_kept += keep_count
            print(
                f"  {m}  plan_asins={len(asins):>4}  keep={keep_count:>6,}  drop_out_of_plan={del_count:>6,}"
            )

            if args.commit and del_count > 0:
                cur.execute(
                    "DELETE FROM ledger WHERE brand=%s AND to_char(date,'YYYY-MM')=%s AND asin <> ALL(%s::text[])",
                    (args.brand, m, list(asins)),
                )

        if args.commit:
            conn.commit()
            print(f"\nCOMMITTED  deleted={grand_del:,}  kept={grand_kept:,}")
        else:
            print(f"\nDRY-RUN  would_delete={grand_del:,}  would_keep={grand_kept:,}")
            print("Pass --commit to actually delete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
