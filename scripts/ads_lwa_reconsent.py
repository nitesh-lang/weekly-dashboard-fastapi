"""One-shot helper to regenerate `AMS_ADS_REFRESH_TOKEN` after LWA
consent expires or is revoked.

Flow:
  1. Reads AMS_ADS_CLIENT_ID + AMS_ADS_CLIENT_SECRET from .env / env.
  2. Starts a local server on http://localhost:8765/callback.
  3. Opens your browser to the Amazon LWA consent URL.
  4. You log in as the Ads account holder → grant permission.
  5. Amazon redirects to localhost with ?code=…; we exchange for a
     long-lived refresh token.
  6. We call /v2/profiles to enumerate all authorized profiles.
  7. We emit the finished JSON blob in AdPilot dict-shape, ready to
     paste into GitHub secret `AMS_ADS_REFRESH_TOKEN`.

**Before running:** in the Amazon Developer Console (or Seller Central →
Apps and Services → Develop Apps) open your LWA app and add
`http://localhost:8765/callback` to **Allowed Return URLs**. Save.
Amazon permits `http://localhost` as a special case for dev flows.

Then:
    python scripts/ads_lwa_reconsent.py
    # follow the browser prompt

Output prints to stdout AND writes to
`data/processed/ads_lwa_reconsent/refresh_token_<ts>.json` (gitignored via
your existing data/processed exclusions — but delete the file after
pasting into GitHub secrets).
"""
from __future__ import annotations

import datetime as dt
import http.server
import json
import os
import socketserver
import sys
import threading
import urllib.parse
import webbrowser
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass

PORT           = 8765
REDIRECT_URI   = f"http://localhost:{PORT}/callback"
LWA_AUTH_URL   = "https://www.amazon.com/ap/oa"
LWA_TOKEN_URL  = "https://api.amazon.com/auth/o2/token"
ADS_PROFILES   = "https://advertising-api-eu.amazon.com/v2/profiles"
SCOPE          = "advertising::campaign_management"

_code_holder: dict[str, str] = {}


class _CBHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        code = qs.get("code", [None])[0]
        err  = qs.get("error", [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if code:
            _code_holder["code"] = code
            self.wfile.write(b"<h2>Consent received.</h2>"
                             b"<p>You can close this tab and return to the terminal.</p>")
        else:
            _code_holder["error"] = err or "unknown"
            self.wfile.write(f"<h2>Consent failed: {err}</h2>".encode())

    def log_message(self, *_):  # silence default access log
        return


def _wait_for_code(timeout_sec: int = 300) -> str:
    server = socketserver.TCPServer(("localhost", PORT), _CBHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"⏳ Listening on {REDIRECT_URI} … up to {timeout_sec}s")
    start = dt.datetime.now()
    try:
        while (dt.datetime.now() - start).total_seconds() < timeout_sec:
            if "code" in _code_holder:
                return _code_holder["code"]
            if "error" in _code_holder:
                raise RuntimeError(f"consent error: {_code_holder['error']}")
            threading.Event().wait(0.5)
    finally:
        server.shutdown()
    raise TimeoutError("No consent received within timeout.")


def main() -> int:
    client_id     = os.environ.get("AMS_ADS_CLIENT_ID")
    client_secret = os.environ.get("AMS_ADS_CLIENT_SECRET")
    if not client_id or not client_secret:
        print("ERROR: AMS_ADS_CLIENT_ID / AMS_ADS_CLIENT_SECRET missing from env / .env")
        return 1

    consent_url = (
        f"{LWA_AUTH_URL}?"
        + urllib.parse.urlencode({
            "client_id":    client_id,
            "scope":        SCOPE,
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
        })
    )
    print("═" * 70)
    print("1) Verify your LWA app has this in Allowed Return URLs:")
    print(f"     {REDIRECT_URI}")
    print("   (Amazon Developer Console → Login with Amazon → your app → Web Settings)")
    print("═" * 70)
    print("\n2) Opening consent URL in browser…")
    print(f"     {consent_url}\n")
    try:
        webbrowser.open(consent_url)
    except Exception:
        print("   (couldn't auto-open — paste the URL above into your browser)")

    code = _wait_for_code()
    print(f"✔ Got authorization code (len={len(code)})")

    print("\n3) Exchanging code for refresh_token…")
    r = requests.post(LWA_TOKEN_URL, data={
        "grant_type":    "authorization_code",
        "code":          code,
        "redirect_uri":  REDIRECT_URI,
        "client_id":     client_id,
        "client_secret": client_secret,
    }, timeout=30)
    if r.status_code >= 400:
        print(f"ERROR {r.status_code}: {r.text[:400]}")
        return 1
    tok = r.json()
    refresh_token = tok["refresh_token"]
    access_token  = tok["access_token"]
    print(f"✔ refresh_token acquired (len={len(refresh_token)})")

    print("\n4) Enumerating profiles via /v2/profiles…")
    pr = requests.get(ADS_PROFILES, headers={
        "Authorization":                   f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": client_id,
    }, timeout=30)
    if pr.status_code >= 400:
        print(f"ERROR {pr.status_code}: {pr.text[:400]}")
        return 1
    profiles = pr.json()
    print(f"✔ {len(profiles)} profile(s) authorized")
    for p in profiles:
        acct = p.get("accountInfo") or {}
        print(f"   · profileId={p.get('profileId')}  "
              f"cc={p.get('countryCode')}  "
              f"type={acct.get('type')}  "
              f"name={acct.get('name')}")

    # Filter to IN and shape into AdPilot-style dict
    in_profiles = [p for p in profiles
                   if (p.get("countryCode") or "").upper() == "IN"]
    if not in_profiles:
        print("\n⚠ No IN profiles in this consent — is the account correct?")
    dict_blob = {}
    for p in in_profiles:
        acct = p.get("accountInfo") or {}
        pid = str(p.get("profileId"))
        dict_blob[pid] = {
            "refresh_token": refresh_token,
            "profile_name":  acct.get("name") or pid,
            "account_type":  (acct.get("type") or "").lower(),  # 'seller' | 'vendor'
            "marketplace":   p.get("countryCode") or "IN",
            "profile_id":    pid,
        }

    payload = json.dumps(dict_blob, indent=2)
    print("\n═══ New AMS_ADS_REFRESH_TOKEN payload ═══")
    print(payload)
    print("═" * 70)

    out_dir = ROOT / "data" / "processed" / "ads_lwa_reconsent"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = out_dir / f"refresh_token_{ts}.json"
    out_file.write_text(payload, encoding="utf-8")
    print(f"\n→ Also saved to: {out_file.relative_to(ROOT)}")
    print("\nNext step: paste the payload above into the GitHub secret")
    print("  Settings → Secrets and variables → Actions → AMS_ADS_REFRESH_TOKEN → Update")
    print("Or via CLI:")
    print("  gh secret set AMS_ADS_REFRESH_TOKEN --repo nitesh-lang/weekly-dashboard-fastapi < "
          f"{out_file.relative_to(ROOT)}")
    print("\n⚠ Delete the local file after pasting — it holds a live refresh token.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
