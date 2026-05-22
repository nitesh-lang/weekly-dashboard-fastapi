"""Auth guard middleware.

SPA-only strategy: every non-API GET request returns the React shell.  The
SPA calls /api/me on mount to decide what to do — if unauth, React Router
navigates to /login (also served by the SPA).  So this middleware only
needs to enforce 401 on /api/* endpoints; the browser shell itself is
always allowed to load.

Legacy Jinja routes (/upload, /viewer/*, /analytics, /forgot-password,
/reset-password) still require an authenticated session — they're handled
via the same 401 path as APIs, then the user lands on the SPA login.
"""
from urllib.parse import quote

from fastapi.responses import RedirectResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

PUBLIC_PATHS = {
    "/login",
    "/logout",
    "/forgot-password",
    "/reset-password",
    "/health",
    "/ping",
    "/favicon.ico",
    # JSON auth endpoints for the React frontend
    "/api/login",
    "/api/me",     # returns 401 itself when unauth; safe to expose
    "/api/logout",
}

PUBLIC_PREFIXES = (
    "/static/",
)

# Legacy Jinja UI paths that still want the cookie-redirect flow.  Anything
# NOT in this set or under PUBLIC_PREFIXES falls through to the SPA shell
# unconditionally — React then decides whether to send the user to /login.
JINJA_GUARDED_PREFIXES = (
    "/upload",
    "/viewer/",
    "/analytics",
    "/export/",
    "/run-etl",
    "/drilldown/ams",       # the standalone Jinja AMS drilldown
    "/sales-trend/category",
)


class AuthGuardMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
            return await call_next(request)

        if request.session.get("user_email"):
            return await call_next(request)

        # Unauthenticated.  Three buckets:
        #   1. /api/*  → 401 JSON so fetch() callers can react.
        #   2. legacy Jinja paths → 303 to /login (the SPA handles it).
        #   3. anything else → pass through so the SPA shell can load;
        #      React calls /api/me on mount and routes to /login itself.
        if path.startswith("/api/"):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        if any(path.startswith(p) for p in JINJA_GUARDED_PREFIXES):
            next_url = path
            if request.url.query:
                next_url += "?" + request.url.query
            return RedirectResponse(f"/login?next={quote(next_url, safe='')}", status_code=303)

        # SPA route — let the React app load and handle auth client-side.
        return await call_next(request)
