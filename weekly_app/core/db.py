"""Minimal Postgres helper for the users table.

We only need a single table here (users), so we deliberately skip
SQLAlchemy and use raw psycopg2 — fewer moving parts and the auth
helpers are trivial enough that hand-rolled SQL is easier to audit
than ORM models.

`get_conn()` returns a fresh connection every call (no pool).  Auth
queries are sub-millisecond and the app is low-concurrency; pooling
would just add a dep without measurable benefit.

`is_enabled()` is the toggle the rest of the code uses:
  - DATABASE_URL set → Postgres backend
  - not set         → JSON-file backend (local dev, no Render Postgres
                      attached yet)
"""
from __future__ import annotations

import os
from typing import Optional

# Importing psycopg2 lazily so local-dev environments without it
# installed can still load the module (they just won't have Postgres
# available, which is fine — JSON backend kicks in).
_psycopg2 = None
def _get_psycopg2():
    global _psycopg2
    if _psycopg2 is None:
        import psycopg2 as _p
        _psycopg2 = _p
    return _psycopg2


def database_url() -> Optional[str]:
    """Render exposes the Postgres URL as DATABASE_URL.  Old `postgres://`
    schemes get normalised to `postgresql://` because some clients reject
    the legacy form."""
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        return None
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url


def is_enabled() -> bool:
    return database_url() is not None


def get_conn():
    """One-shot connection.  Caller is responsible for closing it
    (use a context manager: `with get_conn() as conn: ...`)."""
    url = database_url()
    if not url:
        raise RuntimeError("DATABASE_URL not set — get_conn() called with no backend")
    return _get_psycopg2().connect(url)


# ─────────────────────────────────────────────────────────────────────
# Schema: one table.  JSON column for `tabs` so we don't need an extra
# join table for the small per-user tab allowlist.
# ─────────────────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    email                TEXT PRIMARY KEY,
    password_hash        TEXT NOT NULL,
    role                 TEXT NOT NULL DEFAULT 'operator',
    tabs                 JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    password_updated_at  TIMESTAMPTZ
);
"""


def init_schema() -> bool:
    """Idempotent: creates the users table if missing.  Returns True if
    the connection was made, False if DATABASE_URL isn't set (so the
    caller falls back to JSON without surprise)."""
    if not is_enabled():
        return False
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
    return True
