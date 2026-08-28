"""Persistent log of every SP-API pull attempt.

Every /pull-sales call records a row here — success and failure — so the
frontend can render a "last sync" pill and a run history without needing
to grep Render logs.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text

from .database import engine


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, default=str)
    except Exception:
        return json.dumps({"repr": repr(v)})


def start_run(
    brand: str, kind: str, sales_date: datetime | None, triggered_by: str | None = None
) -> int:
    with engine.begin() as conn:
        r = conn.execute(
            text(
                "INSERT INTO sync_runs (brand, kind, sales_date, triggered_by) "
                "VALUES (:b, :k, :d, :u) RETURNING id"
            ),
            {
                "b": brand,
                "k": kind,
                "d": sales_date.date() if sales_date else None,
                "u": triggered_by,
            },
        )
        return int(r.scalar_one())


def finish_run(
    run_id: int,
    *,
    ok: bool,
    total_rows: int = 0,
    out_of_plan_count: int = 0,
    fetched_by_account: dict | None = None,
    rows_by_account: dict | None = None,
    warnings: list | None = None,
    error: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE sync_runs SET
                    ended_at           = CURRENT_TIMESTAMP,
                    ok                 = :ok,
                    total_rows         = :tr,
                    out_of_plan_count  = :oop,
                    fetched_by_account = :fa,
                    rows_by_account    = :ra,
                    warnings           = :wn,
                    error              = :er
                WHERE id = :id
                """
            ),
            {
                "ok": ok,
                "tr": int(total_rows or 0),
                "oop": int(out_of_plan_count or 0),
                "fa": _dumps(fetched_by_account or {}),
                "ra": _dumps(rows_by_account or {}),
                "wn": _dumps(warnings or []),
                "er": error,
                "id": run_id,
            },
        )


def recent_runs(brand: str, limit: int = 50) -> list[dict]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT id, brand, kind, sales_date, started_at, ended_at, ok, "
                "total_rows, out_of_plan_count, fetched_by_account, rows_by_account, "
                "warnings, error, triggered_by "
                "FROM sync_runs WHERE brand = :b "
                "ORDER BY started_at DESC LIMIT :n"
            ),
            {"b": brand, "n": limit},
        ).mappings().all()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        # JSON columns come back as str on SQLite, dict on Postgres
        for k in ("fetched_by_account", "rows_by_account", "warnings"):
            v = d.get(k)
            if isinstance(v, str):
                try:
                    d[k] = json.loads(v)
                except Exception:
                    d[k] = None
        d["sales_date"] = str(d["sales_date"]) if d["sales_date"] else None
        d["started_at"] = d["started_at"].isoformat() if d["started_at"] else None
        d["ended_at"] = d["ended_at"].isoformat() if d["ended_at"] else None
        out.append(d)
    return out


def last_ok_run(brand: str) -> dict | None:
    with engine.begin() as conn:
        r = conn.execute(
            text(
                "SELECT id, sales_date, started_at, ended_at, total_rows "
                "FROM sync_runs WHERE brand = :b AND ok = TRUE "
                "ORDER BY started_at DESC LIMIT 1"
            ),
            {"b": brand},
        ).mappings().first()
    if not r:
        return None
    d = dict(r)
    d["sales_date"] = str(d["sales_date"]) if d["sales_date"] else None
    d["started_at"] = d["started_at"].isoformat() if d["started_at"] else None
    d["ended_at"] = d["ended_at"].isoformat() if d["ended_at"] else None
    return d
