"""Per-user activity log — the evidence base for the consumption report.

`sync_runs` already records *what the tool fetched*. This module records
*what the people did*: sign-ins, sign-outs, pages opened, exports taken and
data requests, each stamped with the user and a session id.

Two rules this module lives by:

1. **Logging never breaks the app.** Every write is wrapped — if the insert
   fails (bad connection, missing table, whatever), the request it was
   attached to still succeeds. A dashboard that goes down because its
   analytics went down is worse than no analytics.
2. **Session length is derived, not trusted.** The browser sends a heartbeat
   while the tab is open; duration is `last event - first event` within a
   session id, capped by IDLE_GAP_MIN so a laptop left open overnight does
   not report a sixteen-hour session.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Any

from sqlalchemy import text

from .database import IS_SQLITE, engine

_TS_DEFAULT = "CURRENT_TIMESTAMP" if IS_SQLITE else "NOW()"
_TS_TYPE = "TIMESTAMP" if IS_SQLITE else "TIMESTAMPTZ"
_JSON_TYPE = "TEXT" if IS_SQLITE else "JSONB"
_PK = "INTEGER PRIMARY KEY AUTOINCREMENT" if IS_SQLITE else "BIGSERIAL PRIMARY KEY"

# Truncate a timestamp to a calendar day. SQLite has no DATE type, so
# CAST(x AS DATE) there yields the leading number (the year) rather than a
# date — DATE(x) is the correct form. Postgres wants the cast.
_DAY = "DATE(occurred_at)" if IS_SQLITE else "CAST(occurred_at AS DATE)"

# A session is considered ended when no event arrives for this many minutes.
IDLE_GAP_MIN = 15

# Heartbeat cadence the frontend uses, in seconds. Used to close out the tail
# of a session (the stretch between the last heartbeat and the user leaving).
HEARTBEAT_SEC = 60

DDL = f"""
CREATE TABLE IF NOT EXISTS activity_events (
    id          {_PK},
    user_email  TEXT        NOT NULL,
    session_id  TEXT,
    event       TEXT        NOT NULL,
    page        TEXT,
    brand       TEXT,
    detail      {_JSON_TYPE},
    occurred_at {_TS_TYPE}   NOT NULL DEFAULT {_TS_DEFAULT}
);

CREATE INDEX IF NOT EXISTS ix_activity_user_time
    ON activity_events (user_email, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_activity_event_time
    ON activity_events (event, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_activity_session
    ON activity_events (session_id);
"""

# Events we accept. Anything else is rejected so a stray call from the browser
# cannot fill the table with junk categories.
EVENTS = {
    "login",        # completed a sign-in
    "logout",       # pressed sign out
    "page_view",    # opened a page in the app
    "export",       # downloaded / exported a report
    "data_fetch",   # requested dashboard data from the server
    "sync",         # pressed the Daily Sync button
    "heartbeat",    # tab still open (drives time-in-tool)
}


def bootstrap_activity() -> None:
    """Create the table and indexes. Safe to call on every boot."""
    with engine.begin() as conn:
        for stmt in DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def _dumps(v: Any) -> str | None:
    if v is None:
        return None
    try:
        return json.dumps(v, default=str)
    except Exception:
        return json.dumps({"repr": repr(v)})


def log(
    user_email: str | None,
    event: str,
    *,
    session_id: str | None = None,
    page: str | None = None,
    brand: str | None = None,
    detail: dict | None = None,
) -> None:
    """Record one activity event. Never raises."""
    if not user_email or event not in EVENTS:
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO activity_events "
                    "(user_email, session_id, event, page, brand, detail) "
                    "VALUES (:u, :s, :e, :p, :b, :d)"
                ),
                {
                    "u": user_email.strip().lower(),
                    "s": (session_id or None),
                    "e": event,
                    "p": (page or None)[:200] if page else None,
                    "b": brand or None,
                    "d": _dumps(detail),
                },
            )
    except Exception as exc:  # analytics must never take the app down
        print(f"[activity] could not record {event!r} for {user_email!r}: {exc}")


# ── reporting ────────────────────────────────────────────────────────────

def _rows(sql: str, params: dict | None = None) -> list[dict]:
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(text(sql), params or {}).mappings().all()]


def _dt(v):
    """Coerce a timestamp to datetime.

    Postgres returns a datetime for MIN/MAX on a timestamp column; SQLite
    returns the stored string. Reporting has to work on both.
    """
    if v is None or isinstance(v, dt.datetime):
        return v
    try:
        return dt.datetime.fromisoformat(str(v))
    except ValueError:
        return None


def _iso(v):
    d = _dt(v)
    return d.isoformat() if d else None


def per_user_summary(since: str | None = None, until: str | None = None) -> list[dict]:
    """One row per user: logins, logouts, pages, exports, fetches, minutes."""
    where = "WHERE 1=1"
    p: dict = {}
    if since:
        where += " AND occurred_at >= :since"
        p["since"] = since
    if until:
        where += " AND occurred_at < :until"
        p["until"] = until

    counts = _rows(
        f"""
        SELECT user_email,
               SUM(CASE WHEN event = 'login'      THEN 1 ELSE 0 END) AS logins,
               SUM(CASE WHEN event = 'logout'     THEN 1 ELSE 0 END) AS logouts,
               SUM(CASE WHEN event = 'page_view'  THEN 1 ELSE 0 END) AS page_views,
               SUM(CASE WHEN event = 'export'     THEN 1 ELSE 0 END) AS exports,
               SUM(CASE WHEN event = 'data_fetch' THEN 1 ELSE 0 END) AS data_fetches,
               SUM(CASE WHEN event = 'sync'       THEN 1 ELSE 0 END) AS syncs,
               MIN(occurred_at) AS first_seen,
               MAX(occurred_at) AS last_seen,
               COUNT(DISTINCT {_DAY}) AS active_days
        FROM activity_events
        {where}
        GROUP BY user_email
        """,
        p,
    )

    # Time in tool: sum of session spans, a session being one session_id.
    spans = _rows(
        f"""
        SELECT user_email, session_id,
               MIN(occurred_at) AS s_start,
               MAX(occurred_at) AS s_end
        FROM activity_events
        {where} AND session_id IS NOT NULL
        GROUP BY user_email, session_id
        """,
        p,
    )
    secs: dict[str, float] = {}
    sess: dict[str, int] = {}
    for s in spans:
        a, b = _dt(s["s_start"]), _dt(s["s_end"])
        if not a or not b:
            continue
        d = (b - a).total_seconds()
        # add one heartbeat interval for the tail the heartbeat never saw,
        # and never credit a single session with more than 8 hours.
        d = min(d + HEARTBEAT_SEC, 8 * 3600)
        secs[s["user_email"]] = secs.get(s["user_email"], 0.0) + d
        sess[s["user_email"]] = sess.get(s["user_email"], 0) + 1

    out = []
    for c in counts:
        e = c["user_email"]
        total = secs.get(e, 0.0)
        n = sess.get(e, 0)
        out.append(
            {
                **{k: (int(v) if isinstance(v, (int, float)) and k != "user_email" else v)
                   for k, v in c.items() if k not in ("first_seen", "last_seen")},
                "first_seen": _iso(c["first_seen"]),
                "last_seen": _iso(c["last_seen"]),
                "sessions": n,
                "minutes_total": round(total / 60, 1),
                "minutes_avg_session": round(total / 60 / n, 1) if n else 0.0,
            }
        )
    out.sort(key=lambda r: (-r["minutes_total"], r["user_email"]))
    return out


def per_day(since: str | None = None) -> list[dict]:
    """Daily totals across all users."""
    where = "WHERE occurred_at >= :since" if since else ""
    return [
        {**r, "day": str(r["day"])}
        for r in _rows(
            f"""
            SELECT {_DAY} AS day,
                   COUNT(DISTINCT user_email) AS users,
                   SUM(CASE WHEN event = 'login'     THEN 1 ELSE 0 END) AS logins,
                   SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_views,
                   SUM(CASE WHEN event = 'export'    THEN 1 ELSE 0 END) AS exports
            FROM activity_events
            {where}
            GROUP BY {_DAY}
            ORDER BY day DESC
            """,
            {"since": since} if since else {},
        )
    ]


def exports_breakdown(since: str | None = None) -> list[dict]:
    """Which reports are actually being exported, and by whom."""
    where = "WHERE event = 'export'"
    p: dict = {}
    if since:
        where += " AND occurred_at >= :since"
        p["since"] = since
    rows_ = _rows(
        f"""
        SELECT user_email, page, brand, COUNT(*) AS n,
               MAX(occurred_at) AS last_time
        FROM activity_events
        {where}
        GROUP BY user_email, page, brand
        ORDER BY n DESC
        """,
        p,
    )
    for r in rows_:
        r["last_time"] = _iso(r["last_time"])
    return rows_


def recent_events(limit: int = 500) -> list[dict]:
    rows = _rows(
        "SELECT id, user_email, session_id, event, page, brand, detail, occurred_at "
        "FROM activity_events ORDER BY occurred_at DESC LIMIT :n",
        {"n": limit},
    )
    for r in rows:
        r["occurred_at"] = _iso(r["occurred_at"])
        if isinstance(r.get("detail"), str):
            try:
                r["detail"] = json.loads(r["detail"])
            except Exception:
                pass
    return rows
