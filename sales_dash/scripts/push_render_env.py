"""Push env vars into the two Render services via REST API.

The Render CLI (v2) has no env-var subcommand, so we use the REST API with
the API key already at ~/.render/cli.yaml. Reads secrets from the user's
FastAPI project .env so no token has to be typed here.

Usage:
    python scripts/push_render_env.py \\
        --api-service <srv-id-of-sales-dashboard-api> \\
        --web-service <srv-id-of-sales-dashboard-web> \\
        --neon-url "postgresql://…?sslmode=require"
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import requests
import yaml

FASTAPI_ENV = Path(
    r"D:/Nitesh/Nitesh Gdrive/Nitesh/Weekly Report - B2B + B2C/FastAPI/.env"
)
RENDER_CLI_CONFIG = Path.home() / ".render" / "cli.yaml"

API_ENV_KEYS = [
    "SP_LWA_CLIENT_ID",
    "SP_LWA_CLIENT_SECRET",
    "SP_REFRESH_TOKEN_NEXLEV",
    "SP_REFRESH_TOKEN_AUDIOARRAY",
    "SP_REFRESH_TOKEN_VIOMI",
    "SP_REFRESH_TOKEN_CAMBIUMRETAIL",
    "SP_LWA_CLIENT_ID_AUDIOARRAY",
    "SP_LWA_CLIENT_SECRET_AUDIOARRAY",
    "SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY",
]

DEFAULT_AUTH_PASSWORD = "Cambium@109"
DEFAULT_ADMIN_UPLOAD_KEY = "1234"


def load_render_key() -> str:
    if not RENDER_CLI_CONFIG.exists():
        sys.exit(f"Render CLI config not found at {RENDER_CLI_CONFIG}. Run `render login` first.")
    cfg = yaml.safe_load(RENDER_CLI_CONFIG.read_text())
    key = (cfg.get("api") or {}).get("key")
    if not key:
        sys.exit("api.key missing in ~/.render/cli.yaml")
    return key


def load_fastapi_env() -> dict[str, str]:
    if not FASTAPI_ENV.exists():
        sys.exit(f".env not found at {FASTAPI_ENV}")
    out: dict[str, str] = {}
    for line in FASTAPI_ENV.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def put_env_var(session: requests.Session, service_id: str, key: str, value: str) -> None:
    url = f"https://api.render.com/v1/services/{service_id}/env-vars/{key}"
    r = session.put(url, json={"value": value}, timeout=30)
    if r.status_code not in (200, 201):
        print(f"    ✗ {key}: HTTP {r.status_code} {r.text[:200]}")
        return
    print(f"    ✓ {key}")


def redeploy(session: requests.Session, service_id: str) -> None:
    url = f"https://api.render.com/v1/services/{service_id}/deploys"
    r = session.post(url, json={"clearCache": "do_not_clear"}, timeout=30)
    if r.status_code not in (200, 201):
        print(f"    ✗ redeploy: HTTP {r.status_code} {r.text[:200]}")
        return
    print(f"    ✓ redeploy triggered: {r.json().get('id')}")


def get_service(session: requests.Session, service_id: str) -> dict:
    r = session.get(f"https://api.render.com/v1/services/{service_id}", timeout=30)
    r.raise_for_status()
    return r.json()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-service", required=True, help="srv-… id for sales-dashboard-api")
    ap.add_argument("--web-service", required=True, help="srv-… id for sales-dashboard-web")
    ap.add_argument("--neon-url", required=True, help="Neon Postgres pooled connection string")
    ap.add_argument("--auth-password", default=DEFAULT_AUTH_PASSWORD)
    ap.add_argument("--admin-upload-key", default=DEFAULT_ADMIN_UPLOAD_KEY)
    args = ap.parse_args()

    key = load_render_key()
    src = load_fastapi_env()

    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {key}", "Accept": "application/json"})

    api_svc = get_service(sess, args.api_service)
    web_svc = get_service(sess, args.web_service)
    api_url = api_svc.get("serviceDetails", {}).get("url") or api_svc.get("url") or ""
    web_url = web_svc.get("serviceDetails", {}).get("url") or web_svc.get("url") or ""
    print(f"api: {api_svc.get('name')} → {api_url or '(no url yet)'}")
    print(f"web: {web_svc.get('name')} → {web_url or '(no url yet)'}")

    print(f"\n[api] pushing env vars → {args.api_service}")
    put_env_var(sess, args.api_service, "DATABASE_URL", args.neon_url)
    put_env_var(sess, args.api_service, "AUTH_PASSWORD", args.auth_password)
    put_env_var(sess, args.api_service, "ADMIN_UPLOAD_KEY", args.admin_upload_key)
    if web_url:
        put_env_var(sess, args.api_service, "CORS_ORIGINS", web_url)

    missing = []
    for k in API_ENV_KEYS:
        v = src.get(k)
        if not v:
            missing.append(k)
            continue
        put_env_var(sess, args.api_service, k, v)
    if missing:
        print(f"    ⚠ missing in FastAPI .env: {', '.join(missing)}")

    print(f"\n[web] pushing env vars → {args.web_service}")
    if api_url:
        put_env_var(sess, args.web_service, "VITE_API_BASE", api_url)
    else:
        print("    ⚠ api service has no url yet — VITE_API_BASE not set")

    print("\ntriggering redeploys…")
    print(" api:")
    redeploy(sess, args.api_service)
    print(" web:")
    redeploy(sess, args.web_service)

    print("\ndone.")
    print(f" api: {api_url}")
    print(f" web: {web_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
