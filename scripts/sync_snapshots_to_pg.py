"""Mirror the processed snapshots into Postgres (Neon).

    python scripts/sync_snapshots_to_pg.py            # sync changed files
    python scripts/sync_snapshots_to_pg.py --force    # resync everything

Postgres is a READ MIRROR, not the source of truth.  The CSVs/xlsx stay
exactly as they are; the ETLs keep writing them; git keeps versioning them.
This script runs AFTER the ETL chain (local recipe + cron) and copies each
snapshot into a table of the same name.  Routes read the mirror via
weekly_app/core/df_cache.py when WEEKLY_USE_PG=1, and silently fall back to
the file when the table is missing or PG is unreachable — so a dead DB can
never break the dashboard.

Freshness is tracked in _sync_meta by source file mtime+size: unchanged
files are skipped, so the weekly incremental sync moves only what the ETL
actually rewrote.

Connection: WEEKLY_DATABASE_URL env var, else the same key in .env.
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent.parent

# Everything routes read through df_cache.  A file listed here but absent on
# disk is skipped; a table absent from PG makes df_cache fall back to the
# file — so this list can only ever make things faster, never break them.
SYNC_SOURCES: list[Path] = sorted(
    [
        *ROOT.glob("data/processed/*.csv"),
        ROOT / "data" / "master" / "sku_master.xlsx",
        ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv",
        ROOT / "data" / "ams_weekly_data" / "processed_ads" / "ads_weekly_aggregated.csv",
        ROOT / "data" / "ams_weekly_data" / "ams_weekly_fact" / "ams_weekly_fact.csv",
        ROOT / "data" / "ams_weekly_data" / "ams_weekly_fact" / "ams_weekly_fact_with_category.csv",
    ]
)


def table_name(path: Path) -> str:
    """`weekly_sales_snapshot.csv` -> `weekly_sales_snapshot`.  Must stay in
    step with df_cache._pg_table_for()."""
    return re.sub(r"[^a-z0-9_]", "_", path.stem.lower())


def database_url() -> str:
    url = os.environ.get("WEEKLY_DATABASE_URL")
    if not url:
        env_file = ROOT / ".env"
        if env_file.exists():
            m = re.search(r"^WEEKLY_DATABASE_URL=(.+)$", env_file.read_text(), re.M)
            if m:
                url = m.group(1).strip()
    if not url:
        sys.exit("WEEKLY_DATABASE_URL not set (env or .env)")
    return url.replace("postgresql://", "postgresql+psycopg2://", 1)


def load_frame(path: Path) -> pd.DataFrame:
    if path.suffix == ".xlsx":
        return pd.read_excel(path)
    return pd.read_csv(path, low_memory=False)


def push(df: pd.DataFrame, tbl: str, engine) -> None:
    """Replace-in-transaction: build fresh alongside, then swap.  Readers
    never see a half-loaded table."""
    tmp = f"_incoming_{tbl}"
    with engine.begin() as con:
        con.execute(text(f'DROP TABLE IF EXISTS "{tmp}"'))
        df.head(0).to_sql(tmp, con, index=False)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        raw = con.connection.dbapi_connection
        cols = ", ".join(f'"{c}"' for c in df.columns)
        with raw.cursor() as cur:
            cur.copy_expert(
                f'COPY "{tmp}" ({cols}) FROM STDIN WITH (FORMAT csv)', buf
            )
        con.execute(text(f'DROP TABLE IF EXISTS "{tbl}"'))
        con.execute(text(f'ALTER TABLE "{tmp}" RENAME TO "{tbl}"'))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    engine = create_engine(database_url(), pool_pre_ping=True)
    with engine.begin() as con:
        con.execute(text(
            "CREATE TABLE IF NOT EXISTS _sync_meta ("
            " table_name text PRIMARY KEY, source_path text, mtime double precision,"
            " size_bytes bigint, rows bigint, synced_at timestamptz DEFAULT now())"
        ))
        meta = {
            r[0]: (r[1], r[2])
            for r in con.execute(text("SELECT table_name, mtime, size_bytes FROM _sync_meta"))
        }

    synced = skipped = 0
    t0 = time.time()
    for path in SYNC_SOURCES:
        if not path.exists() or path.name.startswith("_"):
            continue
        tbl = table_name(path)
        st = path.stat()
        if not args.force and meta.get(tbl) == (st.st_mtime, st.st_size):
            skipped += 1
            continue
        df = load_frame(path)
        push(df, tbl, engine)
        with engine.begin() as con:
            con.execute(text(
                "INSERT INTO _sync_meta (table_name, source_path, mtime, size_bytes, rows, synced_at)"
                " VALUES (:t, :p, :m, :s, :r, now())"
                " ON CONFLICT (table_name) DO UPDATE SET source_path=:p, mtime=:m,"
                " size_bytes=:s, rows=:r, synced_at=now()"
            ), {"t": tbl, "p": str(path.relative_to(ROOT)), "m": st.st_mtime,
                "s": st.st_size, "r": len(df)})
        print(f"  -> {tbl:<38} {len(df):>7,} rows")
        synced += 1

    print(f"\nsynced {synced}, unchanged {skipped}, {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
