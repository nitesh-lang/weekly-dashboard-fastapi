"""Auth guard middleware.

Redirects unauthenticated HTML requests to /login (preserving the original
target via ?next=). API routes (/api/*) get a 401 JSON response so that
fetch() callers don't follow redirects to the login HTML.
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
}

PUBLIC_PREFIXES = (
    "/static/",
)


class AuthGuardMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
            return await call_next(request)

        # Session is set up by SessionMiddleware (must be outer of this).
        if request.session.get("user_email"):
            return await call_next(request)

        if path.startswith("/api/"):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        next_url = path
        if request.url.query:
            next_url += "?" + request.url.query
        return RedirectResponse(f"/login?next={quote(next_url, safe='')}", status_code=303)
