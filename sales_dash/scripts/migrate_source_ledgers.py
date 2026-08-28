"""One-shot migration: source Render Postgres ledgers → merged Neon DB.

Read-only on the source side. Only INSERT (with ON CONFLICT DO UPDATE) on the
destination so the migration is idempotent — running it twice just refreshes
the same rows, never duplicates.

Both source DBs share the same ledger shape:
    ledger(date, account, asin, sales, net_sales, units)

The destination adds a `brand` column keying which source the row came from.
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg2
import psycopg2.extras

SOURCES = {
    "nexlev": os.getenv(
        "SRC_NEXLEV_URL",
        "postgresql://nexlev_db_v2_user:gCNh14kRDp9MpY02uWjFyPgQDkQBdpeG"
        "@dpg-d6c0g0i4d50c73d14gag-a.virginia-postgres.render.com:5432/nexlev_db_v2",
    ),
    "audio_array": os.getenv(
        "SRC_AUDIOARRAY_URL",
        "postgresql://audio_array_db_user:wN4jWymBOxancZJ7A4QmwTMKrr47tKdA"
        "@dpg-d5bp3p24d50c73fdi4ig-a.virginia-postgres.render.com:5432/audio_array_db",
    ),
}

DEST_URL = os.getenv(
    "DEST_URL",
    "postgresql://neondb_owner:npg_DFo7HqWST0hs"
    "@ep-noisy-rain-aztnfk88.c-3.ap-southeast-1.aws.neon.tech/neondb?sslmode=require",
)

FETCH_SQL = 'SELECT date, account, asin, sales, net_sales, units FROM ledger'
UPSERT_SQL = """
INSERT INTO ledger (brand, date, account, asin, sales, net_sales, units)
VALUES %s
ON CONFLICT (brand, date, account, asin)
DO UPDATE SET
    sales     = EXCLUDED.sales,
    net_sales = EXCLUDED.net_sales,
    units     = EXCLUDED.units
"""


def migrate(brand: str, src_url: str, dest_url: str, dry_run: bool) -> None:
    print(f"\n=== {brand} ===")
    with psycopg2.connect(src_url) as sconn, sconn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    ) as scur:
        scur.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM ledger")
        total, mn, mx = scur.fetchone()
        print(f"  source rows: {total:,}  dates: {mn} → {mx}")
        if not total:
            return
        scur.execute(FETCH_SQL)
        rows = scur.fetchall()

    payload = [
        (
            brand,
            r["date"],
            (r["account"] or "").strip() or None,
            (r["asin"] or "").strip() or None,
            r["sales"],
            r["net_sales"],
            r["units"],
        )
        for r in rows
        if r["account"] and r["asin"]
    ]
    print(f"  clean rows: {len(payload):,}")

    if dry_run:
        print("  dry-run — skipping insert")
        return

    with psycopg2.connect(dest_url) as dconn, dconn.cursor() as dcur:
        dcur.execute("SELECT COUNT(*) FROM ledger WHERE brand = %s", (brand,))
        before = dcur.fetchone()[0]
        psycopg2.extras.execute_values(dcur, UPSERT_SQL, payload, page_size=500)
        dcur.execute("SELECT COUNT(*) FROM ledger WHERE brand = %s", (brand,))
        after = dcur.fetchone()[0]
    print(f"  dest before: {before:,}  after: {after:,}  delta: {after - before:,}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--brand", choices=["nexlev", "audio_array", "all"], default="all")
    args = ap.parse_args()

    targets = list(SOURCES.items()) if args.brand == "all" else [(args.brand, SOURCES[args.brand])]
    for brand, url in targets:
        try:
            migrate(brand, url, DEST_URL, args.dry_run)
        except Exception as e:
            print(f"  FAILED: {e}", file=sys.stderr)
            raise
    print("\ndone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
