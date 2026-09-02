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
    "/tools",               # external-tool launcher (Jinja page)
    "/buybox",              # embedded Buybox Report static build
    "/margin",              # merged Margin Calculator (margin_src/) — weekly
                            # session is its ONLY auth, so the whole prefix
                            # must 303 anonymous visitors to /login
)


# Pure ASGI middleware (not BaseHTTPMiddleware).  Starlette's BaseHTTPMiddleware
# re-streams response bodies through an internal _StreamingResponse, which
# desyncs with GZipMiddleware's Content-Length on large JSON payloads —
# h11 then aborts the response with "Too much data for declared Content-Length".
# The pure-ASGI form passes through send() calls verbatim and sidesteps the bug.
class AuthGuardMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
            await self.app(scope, receive, send)
            return

        # SessionMiddleware mounts the session dict on scope["session"].
        session = scope.get("session") or {}
        if session.get("user_email"):
            await self.app(scope, receive, send)
            return

        # Unauthenticated.  Three buckets:
        #   1. /api/*  → 401 JSON so fetch() callers can react.
        #   2. legacy Jinja paths → 303 to /login (the SPA handles it).
        #   3. anything else → pass through so the SPA shell can load;
        #      React calls /api/me on mount and routes to /login itself.
        if path.startswith("/api/"):
            response = JSONResponse({"error": "unauthorized"}, status_code=401)
            await response(scope, receive, send)
            return

        if any(path.startswith(p) for p in JINJA_GUARDED_PREFIXES):
            next_url = path
            query = scope.get("query_string", b"")
            if query:
                next_url += "?" + query.decode("latin-1")
            response = RedirectResponse(
                f"/login?next={quote(next_url, safe='')}", status_code=303
            )
            await response(scope, receive, send)
            return

        # SPA route — let the React app load and handle auth client-side.
        await self.app(scope, receive, send)
