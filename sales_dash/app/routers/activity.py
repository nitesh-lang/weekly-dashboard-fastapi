"""Activity tracking intake + the admin usage report it feeds.

POST /api/activity/track   — the browser reports a page view, export or heartbeat
GET  /api/admin/usage      — per-user consumption figures (admin only)
GET  /api/admin/usage/events — raw event log (admin only)
"""
from __future__ import annotations

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel

from .. import activity
from ..auth import require_admin, require_user

router = APIRouter()


class TrackIn(BaseModel):
    event: str
    session_id: str | None = None
    page: str | None = None
    brand: str | None = None
    detail: dict | None = None


@router.post("/activity/track")
def track(payload: TrackIn, request: Request):
    """Record one browser-side event against the signed-in user.

    Returns ok unconditionally — the browser must never retry or surface an
    error because a tracking call did not land.
    """
    u = require_user(request)
    activity.log(
        u["email"],
        payload.event,
        session_id=payload.session_id,
        page=payload.page,
        brand=payload.brand,
        detail=payload.detail,
    )
    return {"ok": True}


@router.get("/admin/usage")
def usage(
    request: Request,
    since: str | None = Query(None, description="ISO date, e.g. 2026-08-01"),
    until: str | None = Query(None, description="ISO date, exclusive"),
):
    require_admin(request)
    return {
        "since": since,
        "until": until,
        "per_user": activity.per_user_summary(since, until),
        "per_day": activity.per_day(since),
        "exports": activity.exports_breakdown(since),
    }


@router.get("/admin/usage/events")
def usage_events(request: Request, limit: int = Query(500, ge=1, le=5000)):
    require_admin(request)
    return {"events": activity.recent_events(limit)}
