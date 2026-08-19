"""Flag-gated Postgres pushdown with an unconditional file fallback.

Contract (operator, 2026-08-19: "tool must work fine, nothing dropped,
absolute security and visibility"):

  * OFF unless WEEKLY_USE_PG=1 — local dev and any emergency rollback are
    one env var away from pure-file behaviour.
  * fetch() returns a DataFrame or None.  None — table missing, DB down,
    SQL error, ANYTHING — means the caller runs its existing file path.
    The file path is never deleted, so PG can degrade the tool to exactly
    what it was yesterday, never below it.
  * Security: connection string only from env/.env (gitignored, repo
    secret); Neon enforces TLS (sslmode=require in the URI).  Nothing here
    ever writes — the engine is used for SELECT only.
  * Visibility: every fetch is counted per (route, table, outcome) and
    exposed at /api/pg-status alongside mirror freshness, so "which source
    served this" is a query, not a guess.
"""
from __future__ import annotations

import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent

_engine = None
_engine_lock = threading.Lock()
_engine_failed_at: float = 0.0
_RETRY_SECONDS = 300  # after a connection failure, stay on files for 5 min

# (route, table, outcome) -> count.  outcome: "pg" | "fallback"
stats: Dict[tuple, int] = {}
_stats_lock = threading.Lock()


def enabled() -> bool:
    return os.environ.get("WEEKLY_USE_PG") == "1"


def _url() -> Optional[str]:
    url = os.environ.get("WEEKLY_DATABASE_URL")
    if not url:
        env = ROOT / ".env"
        if env.exists():
            m = re.search(r"^WEEKLY_DATABASE_URL=(.+)$", env.read_text(), re.M)
            url = m.group(1).strip() if m else None
    if url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def _get_engine():
    global _engine, _engine_failed_at
    if _engine is not None:
        return _engine
    if time.time() - _engine_failed_at < _RETRY_SECONDS:
        return None
    with _engine_lock:
        if _engine is not None:
            return _engine
        try:
            from sqlalchemy import create_engine
            url = _url()
            if not url:
                _engine_failed_at = time.time()
                return None
            eng = create_engine(
                url, pool_pre_ping=True, pool_size=2, max_overflow=3,
                connect_args={"connect_timeout": 5},
            )
            with eng.connect():
                pass
            _engine = eng
            return _engine
        except Exception:
            _engine_failed_at = time.time()
            return None


def _count(route: str, table: str, outcome: str) -> None:
    with _stats_lock:
        stats[(route, table, outcome)] = stats.get((route, table, outcome), 0) + 1


def fetch(
    table: str,
    route: str,
    where: Optional[Dict[str, Any]] = None,
    columns: Optional[Sequence[str]] = None,
) -> Optional[pd.DataFrame]:
    """SELECT [columns] FROM table WHERE col = val / col IN (vals).

    Returns None on ANY problem — caller must treat None as "use the file
    path".  Never raises.
    """
    if not enabled():
        return None
    eng = _get_engine()
    if eng is None:
        _count(route, table, "fallback")
        return None
    try:
        from sqlalchemy import text
        if not re.fullmatch(r"[a-z0-9_]+", table):
            return None
        cols = "*"
        if columns:
            safe = [c for c in columns if re.fullmatch(r"[A-Za-z0-9_ ()%/.-]+", c)]
            if len(safe) != len(columns):
                return None
            cols = ", ".join(f'"{c}"' for c in safe)
        clauses, params = [], {}
        for i, (col, val) in enumerate((where or {}).items()):
            if not re.fullmatch(r"[A-Za-z0-9_ ()%/.-]+", col):
                return None
            if isinstance(val, (list, tuple, set)):
                if not val:
                    return None
                names = []
                for j, v in enumerate(val):
                    params[f"p{i}_{j}"] = v
                    names.append(f":p{i}_{j}")
                clauses.append(f'"{col}" IN ({", ".join(names)})')
            else:
                params[f"p{i}"] = val
                clauses.append(f'"{col}" = :p{i}')
        sql = f'SELECT {cols} FROM "{table}"'
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        with eng.connect() as con:
            df = pd.read_sql(text(sql), con, params=params)
        _count(route, table, "pg")
        return df
    except Exception:
        _count(route, table, "fallback")
        return None


def status() -> Dict[str, Any]:
    """Everything an operator needs to trust (or distrust) the mirror."""
    out: Dict[str, Any] = {
        "enabled": enabled(),
        "engine_up": _get_engine() is not None if enabled() else None,
        "counters": [
            {"route": r, "table": t, "outcome": o, "count": c}
            for (r, t, o), c in sorted(stats.items())
        ],
    }
    if out.get("engine_up"):
        try:
            from sqlalchemy import text
            with _get_engine().connect() as con:
                rows = con.execute(text(
                    "SELECT table_name, rows, synced_at FROM _sync_meta ORDER BY table_name"
                )).fetchall()
            out["mirror"] = [
                {"table": r[0], "rows": int(r[1]), "synced_at": str(r[2])} for r in rows
            ]
        except Exception as e:
            out["mirror_error"] = str(e)[:200]
    return out
