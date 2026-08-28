"""Shared DB engine + one-time schema bootstrap.

Schema mirrors the columns both source apps expect
(date, account, asin, sales, net_sales, units) and adds a `brand`
column so Nexlev / Audio Array data coexist safely.

Primary key is (brand, date, account, asin) — matches the
ON CONFLICT (date, account, asin) upsert in the ported services once
the query is scoped by brand.
"""
from __future__ import annotations

import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL") or "sqlite:///./local.db"
IS_SQLITE = DATABASE_URL.startswith("sqlite")

engine: Engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"check_same_thread": False} if IS_SQLITE else {},
)


_TS_DEFAULT = "CURRENT_TIMESTAMP" if IS_SQLITE else "NOW()"
_TS_TYPE = "TIMESTAMP" if IS_SQLITE else "TIMESTAMPTZ"

_JSON_TYPE = "TEXT" if IS_SQLITE else "JSONB"

DDL = f"""
CREATE TABLE IF NOT EXISTS ledger (
    brand      TEXT        NOT NULL,
    date       DATE        NOT NULL,
    account    TEXT        NOT NULL,
    asin       TEXT        NOT NULL,
    sales      NUMERIC     NOT NULL DEFAULT 0,
    net_sales  NUMERIC     NOT NULL DEFAULT 0,
    units      NUMERIC     NOT NULL DEFAULT 0,
    created_at {_TS_TYPE}   NOT NULL DEFAULT {_TS_DEFAULT},
    PRIMARY KEY (brand, date, account, asin)
);
-- ledger primary key preserved above.

CREATE INDEX IF NOT EXISTS ix_ledger_brand_date ON ledger (brand, date);
CREATE INDEX IF NOT EXISTS ix_ledger_brand_asin ON ledger (brand, asin);

-- Enrichment columns (nullable) — populated from sku_master on ingest.
ALTER TABLE ledger ADD COLUMN IF NOT EXISTS model TEXT;
ALTER TABLE ledger ADD COLUMN IF NOT EXISTS category_l2 TEXT;

CREATE TABLE IF NOT EXISTS sync_runs (
    id                 BIGSERIAL PRIMARY KEY,
    brand              TEXT      NOT NULL,
    kind               TEXT      NOT NULL DEFAULT 'sp_api',
    sales_date         DATE,
    started_at         {_TS_TYPE} NOT NULL DEFAULT {_TS_DEFAULT},
    ended_at           {_TS_TYPE},
    ok                 BOOLEAN,
    total_rows         INTEGER DEFAULT 0,
    out_of_plan_count  INTEGER DEFAULT 0,
    fetched_by_account {_JSON_TYPE},
    rows_by_account    {_JSON_TYPE},
    warnings           {_JSON_TYPE},
    error              TEXT,
    triggered_by       TEXT
);
ALTER TABLE sync_runs ADD COLUMN IF NOT EXISTS triggered_by TEXT;

CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_started
    ON sync_runs (brand, started_at DESC);

CREATE TABLE IF NOT EXISTS users (
    id                    BIGSERIAL PRIMARY KEY,
    email                 TEXT      NOT NULL UNIQUE,
    full_name             TEXT,
    password_hash         BYTEA     NOT NULL,
    role                  TEXT      NOT NULL DEFAULT 'viewer',
    is_active             BOOLEAN   NOT NULL DEFAULT TRUE,
    must_reset_password   BOOLEAN   NOT NULL DEFAULT FALSE,
    created_at            {_TS_TYPE} NOT NULL DEFAULT {_TS_DEFAULT}
);
"""


def bootstrap_schema() -> None:
    # Statements run one at a time so a single unsupported one cannot abort
    # the rest. On SQLite (local dev only) some Postgres DDL has no
    # equivalent — notably ALTER TABLE ... ADD COLUMN IF NOT EXISTS — so
    # those are skipped with a warning. On Postgres nothing is swallowed:
    # a failing statement is raised, because production must not boot on a
    # half-built schema.
    for stmt in DDL.strip().split(";"):
        s = stmt.strip()
        if not s:
            continue
        try:
            with engine.begin() as conn:
                conn.execute(text(s))
        except Exception as exc:
            if not IS_SQLITE:
                raise
            print(f"[schema] skipped on sqlite: {s.splitlines()[0][:70]}… ({exc.__class__.__name__})")


bootstrap_schema()
