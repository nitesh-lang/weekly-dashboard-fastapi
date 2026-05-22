"""LWA refresh-token → access-token exchange with in-memory caching."""
from __future__ import annotations

import time
from dataclasses import dataclass

import httpx

from .config import Config, LWA_TOKEN_URL


@dataclass
class _CachedToken:
    access_token: str
    expires_at: float


_cache: _CachedToken | None = None


def get_access_token(cfg: Config, *, force_refresh: bool = False) -> str:
    global _cache
    now = time.time()
    if not force_refresh and _cache and _cache.expires_at - 60 > now:
        return _cache.access_token

    resp = httpx.post(
        LWA_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": cfg.refresh_token,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    body = resp.json()
    _cache = _CachedToken(
        access_token=body["access_token"],
        expires_at=now + int(body.get("expires_in", 3600)),
    )
    return _cache.access_token


def token_diagnostics(cfg: Config) -> dict:
    """Mint a token and return safe diagnostic info — no token leaked."""
    t0 = time.time()
    token = get_access_token(cfg, force_refresh=True)
    elapsed = time.time() - t0
    assert _cache is not None
    return {
        "ok": True,
        "expires_in_seconds": int(_cache.expires_at - time.time()),
        "mint_latency_ms": int(elapsed * 1000),
        "token_prefix": token[:6] + "…",
    }
