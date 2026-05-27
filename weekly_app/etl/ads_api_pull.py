"""
Amazon Ads API pull — unified SP + SD + SB campaign → ASIN structure.

For every configured profile (one per ad account), this script:

  Sponsored Products (SP):
    POST /sp/campaigns/list      → all SP campaigns
    POST /sp/productAds/list     → product ads (campaignId, adGroupId, asin)
    Roll-up: campaign_name → set of advertised ASINs

  Sponsored Display (SD):
    POST /sd/campaigns/list
    POST /sd/productAds/list
    Same roll-up

  Sponsored Brands (SB):
    POST /sb/v4/campaigns/list
    POST /sb/v4/ads/list         (creative payload has products[].productAsin
                                   for Product Collection ads,
                                   subpages[].asin for Store Spotlight,
                                   productAsin for Brand Video.
                                   Pure Brand-Store ads have no ASINs
                                   — left at brand level downstream.)
    Same roll-up

Outputs (one per ad type — sb_ingest's Layer 0 reads sb_campaign_asins.json;
the SP and SD files become available for future automation / cross-checks):

  data/processed/sp_campaign_asins.json
  data/processed/sd_campaign_asins.json
  data/processed/sb_campaign_asins.json

Each entry: { "<Campaign Name>": { "brand": "...", "asins": [...],
                                    "ad_types": [...], "profile_id": "..." } }

Required env vars:
  AMS_ADS_CLIENT_ID
  AMS_ADS_CLIENT_SECRET
  AMS_ADS_REFRESH_TOKEN

Run:
  python -m weekly_app.etl.ads_api_pull            # all three
  python -m weekly_app.etl.ads_api_pull sp         # one ad type
  python -m weekly_app.etl.ads_api_pull sp sd      # subset
"""
from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path
from typing import Callable

import requests

ROOT = Path(__file__).resolve().parent.parent.parent

# Pull AMS_ADS_* env vars from a project-root .env file if present.
# Real env vars (Render env, shell exports) override the .env so prod
# keeps working without the file.
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass    # python-dotenv is in requirements.txt; safe-skip if missing locally
OUT_DIR = ROOT / "data" / "processed"

# India (.in) is part of the Amazon Ads API's EU region.
# (Far East = JP/AU/SG; NA = US/CA/MX/BR.)
ADS_API_BASE  = "https://advertising-api-eu.amazon.com"
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

# Default profile list — used only when AMS_ADS_REFRESH_TOKEN is a bare
# `Atzr|...` string.  When it's the AdPilot-style JSON dict, profiles
# are auto-discovered from the dict's keys (see parse_refresh_token_env).
PROFILES_FALLBACK = [
    {"id": "866679611348308",   "label": "Cambium Retail (seller)", "brand": "White Mulberry"},
    {"id": "3563790765020958",  "label": "White Mulberry (vendor)", "brand": "White Mulberry"},
    {"id": "3633693611482083",  "label": "AudioArray (vendor)",     "brand": "Audio Array"},
    {"id": "1122755254933937",  "label": "Nexlev (vendor)",         "brand": "Nexlev"},
]

# Map a profile's profile_name (from the JSON dict) → canonical brand
# folder.  Substring matched, case-insensitive.
PROFILE_NAME_TO_BRAND = [
    ("audio array",     "Audio Array"),
    ("audioarray",      "Audio Array"),
    ("nexlev",          "Nexlev"),
    ("white mulberry",  "White Mulberry"),
    ("cambium retail",  "White Mulberry"),
    ("tonor",           "Tonor"),
    ("fossil",          "Fossil"),
]


def _brand_for_profile_name(name: str) -> str:
    n = (name or "").lower()
    for kw, brand in PROFILE_NAME_TO_BRAND:
        if kw in n:
            return brand
    return "(unmapped)"


def parse_refresh_token_env(raw: str) -> tuple[str, list[dict]]:
    """Returns (refresh_token_str, profiles_list).

    Two supported formats for AMS_ADS_REFRESH_TOKEN:
      1. Bare `Atzr|...` string — uses PROFILES_FALLBACK.
      2. AdPilot-style JSON dict keyed by profile_id:
           {"<profile_id>": {"refresh_token": "Atzr|...",
                              "profile_name": "...",
                              "account_type": "seller"|"vendor",
                              "marketplace":  "IN",
                              "profile_id":   "..." }, ...}
         In this case we auto-discover the profile list AND extract
         the embedded refresh token.
    """
    raw = (raw or "").strip()
    if raw.startswith("{"):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"AMS_ADS_REFRESH_TOKEN looks like JSON but failed to parse: {e}"
            )
        token: str | None = None
        profiles: list[dict] = []
        for pid, info in data.items():
            if not isinstance(info, dict):
                continue
            tok = info.get("refresh_token")
            if not tok:
                continue
            if token is None:
                token = tok
            name = str(info.get("profile_name", pid))
            profiles.append({
                "id":           str(info.get("profile_id", pid)),
                "label":        f"{name} ({info.get('account_type','?')})",
                "brand":        _brand_for_profile_name(name),
                "account_type": info.get("account_type", ""),
                "marketplace":  info.get("marketplace", "IN"),
            })
        if not token or not profiles:
            raise RuntimeError("AMS_ADS_REFRESH_TOKEN JSON has no usable refresh_token entries.")
        return token, profiles
    # Bare token
    return raw, PROFILES_FALLBACK

# v3 / v4 versioned content types
SB_CAMPAIGNS_CT  = "application/vnd.sbcampaignresource.v4+json"
SB_ADS_CT        = "application/vnd.sbadresource.v4+json"
SP_CAMPAIGNS_CT  = "application/vnd.spCampaign.v3+json"
SP_PRODUCTADS_CT = "application/vnd.spProductAd.v3+json"


# ── Auth ────────────────────────────────────────────────────────────────
def get_access_token() -> tuple[str, list[dict]]:
    """Returns (access_token, profiles).  Profiles come from the JSON
    refresh-token dict if the env is in AdPilot format, else from
    PROFILES_FALLBACK."""
    cid = os.environ.get("AMS_ADS_CLIENT_ID", "")
    sec = os.environ.get("AMS_ADS_CLIENT_SECRET", "")
    tok_env = os.environ.get("AMS_ADS_REFRESH_TOKEN", "")
    missing = [k for k, v in (("AMS_ADS_CLIENT_ID", cid),
                              ("AMS_ADS_CLIENT_SECRET", sec),
                              ("AMS_ADS_REFRESH_TOKEN", tok_env)) if not v]
    if missing:
        raise RuntimeError(
            f"Missing env var(s): {', '.join(missing)}.  "
            f"Set locally and in Render → service → Environment."
        )
    tok, profiles = parse_refresh_token_env(tok_env)
    r = requests.post(LWA_TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": tok,
        "client_id": cid,
        "client_secret": sec,
    }, timeout=30)
    if r.status_code >= 400:
        try:
            body = r.json()
            err = body.get("error", "unknown")
            desc = body.get("error_description", "")
            # Mask the secret in any echoed traceback
            hint = ""
            if err == "invalid_client":
                hint = "  → CLIENT_ID or CLIENT_SECRET is wrong (or has whitespace)"
            elif err == "invalid_grant":
                hint = "  → REFRESH_TOKEN expired/revoked, or doesn't belong to this LWA app"
            elif err == "invalid_request":
                hint = "  → Refresh token format is malformed (truncated paste? extra newline?)"
            raise RuntimeError(
                f"LWA token endpoint returned {r.status_code} {err}: {desc}{hint}"
            )
        except (ValueError, KeyError):
            raise RuntimeError(f"LWA token endpoint returned {r.status_code}: {r.text[:200]}")
    return r.json()["access_token"], profiles


def base_headers(profile_id: str, access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": os.environ["AMS_ADS_CLIENT_ID"],
        "Amazon-Advertising-API-Scope":    profile_id,
    }


# ── Generic HTTP helpers ────────────────────────────────────────────────
def _retry(call: Callable[[], requests.Response], retries: int = 4) -> requests.Response:
    for attempt in range(retries):
        r = call()
        if r.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt
            time.sleep(wait)
            continue
        return r
    return r  # last response, possibly an error — caller decides


def _post_paginated_v4(url: str, headers: dict, body: dict, list_key: str) -> list:
    """v3/v4 list endpoints — body-based pagination via nextToken."""
    out: list = []
    next_token = None
    while True:
        b = dict(body); b["maxResults"] = 100   # API cap is 100 in EU region
        if next_token: b["nextToken"] = next_token
        r = _retry(lambda: requests.post(url, headers=headers, json=b, timeout=60))
        if r.status_code == 401:
            raise RuntimeError(f"401 at {url} — token / scope wrong")
        if r.status_code >= 400:
            # Bubble up the body so the operator can see what Amazon's
            # complaining about (usually a schema/header issue).
            raise RuntimeError(
                f"{r.status_code} at {url}\n  body sent: {b}\n  response: {r.text[:500]}"
            )
        data = r.json()
        out.extend(data.get(list_key, []))
        next_token = data.get("nextToken")
        if not next_token:
            break
    return out


def _get_v2(url: str, headers: dict, params: dict | None = None) -> list:
    """v2 list endpoints (SP/SD) — query-string pagination via startIndex."""
    out: list = []
    start = 0
    count = 5000
    p = dict(params or {})
    while True:
        p["startIndex"] = start
        p["count"]      = count
        r = _retry(lambda: requests.get(url, headers=headers, params=p, timeout=60))
        if r.status_code == 401:
            raise RuntimeError(f"401 at {url} — token / scope wrong")
        r.raise_for_status()
        chunk = r.json()
        if not isinstance(chunk, list):
            break
        out.extend(chunk)
        if len(chunk) < count:
            break
        start += count
    return out


# ── Per-ad-type pulls ───────────────────────────────────────────────────
def pull_sb_for_profile(profile: dict, token: str) -> dict[str, dict]:
    h = base_headers(profile["id"], token)
    cam_h = {**h, "Accept": SB_CAMPAIGNS_CT, "Content-Type": SB_CAMPAIGNS_CT}
    ads_h = {**h, "Accept": SB_ADS_CT,       "Content-Type": SB_ADS_CT}

    campaigns = _post_paginated_v4(f"{ADS_API_BASE}/sb/v4/campaigns/list",
                                    cam_h, {}, "campaigns")
    ads = _post_paginated_v4(f"{ADS_API_BASE}/sb/v4/ads/list", ads_h, {}, "ads")

    by_cid = {c["campaignId"]: c for c in campaigns}
    out: dict[str, dict] = {}
    for ad in ads:
        cid = ad.get("campaignId")
        if not cid or cid not in by_cid:
            continue
        creative = ad.get("creative") or {}
        # v4 SB schema: creative.asins is the canonical list, and
        # creative.type names the ad type ("PRODUCT_COLLECTION",
        # "STORE_SPOTLIGHT", "BRAND_VIDEO", "VIDEO", etc.).  Older
        # fields kept as defensive fallbacks.
        ad_type = (creative.get("type")
                   or creative.get("creativeType")
                   or ad.get("creativeType")
                   or "unknown")
        asins: list[str] = []
        # Primary: creative.asins (a plain list of strings)
        for a in creative.get("asins") or []:
            if a:
                asins.append(a)
        # Store Spotlight: one ASIN per subpage tile
        for sp in creative.get("subpages") or []:
            a = sp.get("asin") or sp.get("productAsin")
            if a: asins.append(a)
        # Defensive legacy fallbacks
        for p in creative.get("products") or []:
            a = p.get("productAsin") or p.get("asin")
            if a: asins.append(a)
        if not asins:
            a = creative.get("productAsin") or creative.get("featuredAsin")
            if a: asins.append(a)

        name = by_cid[cid].get("name", "").strip()
        rec = out.setdefault(name, {"asins": [], "ad_types": []})
        rec["asins"].extend(asins)
        rec["ad_types"].append(str(ad_type))
    return out


def pull_sp_for_profile(profile: dict, token: str) -> dict[str, dict]:
    # SP v3 — POST /sp/campaigns/list and /sp/productAds/list (v2 GET is
    # deprecated and 404s).  Same nextToken pagination as SB v4, just
    # different content types + list keys.
    h = base_headers(profile["id"], token)
    cam_h = {**h, "Accept": SP_CAMPAIGNS_CT,  "Content-Type": SP_CAMPAIGNS_CT}
    ads_h = {**h, "Accept": SP_PRODUCTADS_CT, "Content-Type": SP_PRODUCTADS_CT}

    campaigns = _post_paginated_v4(f"{ADS_API_BASE}/sp/campaigns/list",
                                    cam_h, {}, "campaigns")
    ads = _post_paginated_v4(f"{ADS_API_BASE}/sp/productAds/list",
                              ads_h, {}, "productAds")

    by_cid = {c["campaignId"]: c for c in campaigns}
    out: dict[str, dict] = {}
    for ad in ads:
        cid = ad.get("campaignId")
        if not cid or cid not in by_cid:
            continue
        asin = ad.get("asin")
        name = by_cid[cid].get("name", "").strip()
        if not asin or not name:
            continue
        rec = out.setdefault(name, {"asins": [], "ad_types": ["productAd"]})
        rec["asins"].append(asin)
    return out


def pull_sd_for_profile(profile: dict, token: str) -> dict[str, dict]:
    h = base_headers(profile["id"], token)
    campaigns = _get_v2(f"{ADS_API_BASE}/sd/campaigns", h)
    ads       = _get_v2(f"{ADS_API_BASE}/sd/productAds", h)
    by_cid = {c["campaignId"]: c for c in campaigns}
    out: dict[str, dict] = {}
    for ad in ads:
        cid = ad.get("campaignId")
        if not cid or cid not in by_cid:
            continue
        asin = ad.get("asin")
        name = by_cid[cid].get("name", "").strip()
        if not asin or not name:
            continue
        rec = out.setdefault(name, {"asins": [], "ad_types": ["productAd"]})
        rec["asins"].append(asin)
    return out


# ── Orchestrator ────────────────────────────────────────────────────────
PULLERS = {
    "sp": ("SP", pull_sp_for_profile, OUT_DIR / "sp_campaign_asins.json"),
    "sd": ("SD", pull_sd_for_profile, OUT_DIR / "sd_campaign_asins.json"),
    "sb": ("SB", pull_sb_for_profile, OUT_DIR / "sb_campaign_asins.json"),
}


def _dedupe_rec(rec: dict) -> dict:
    seen, uniq = set(), []
    for a in rec.get("asins", []):
        if a in seen: continue
        seen.add(a); uniq.append(a)
    rec["asins"] = uniq
    rec["ad_types"] = sorted(set(rec.get("ad_types", [])))
    return rec


def run(which: list[str]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    token, profiles = get_access_token()
    print(f"🔑 LWA access token: ✓  ({len(profiles)} profile(s) discovered)")
    for p in profiles:
        print(f"     · {p['id']:<18} {p['label']:<40} → {p['brand']}")

    for code in which:
        label, puller, out_path = PULLERS[code]
        print(f"\n━━━ {label} ━━━")
        merged: dict[str, dict] = {}
        for prof in profiles:
            print(f"▶ profile {prof['id']} ({prof['label']})")
            try:
                res = puller(prof, token)
            except requests.HTTPError as e:
                print(f"   ⚠ HTTP: {e}")
                continue
            except RuntimeError as e:
                # Verbose API error already captured by _post_paginated_v4
                print(f"   ⚠ {e}")
                continue
            except Exception as e:
                print(f"   ⚠ {type(e).__name__}: {e}")
                continue
            print(f"   {len(res):>5} campaigns with ASIN detail")
            for name, rec in res.items():
                existing = merged.get(name)
                if existing:
                    existing["asins"].extend(rec["asins"])
                    existing["ad_types"].extend(rec.get("ad_types", []))
                else:
                    merged[name] = {
                        "brand":      prof["brand"],
                        "asins":      list(rec["asins"]),
                        "ad_types":   list(rec.get("ad_types", [])),
                        "profile_id": prof["id"],
                    }

        for rec in merged.values():
            _dedupe_rec(rec)

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        no_asin = sum(1 for r in merged.values() if not r["asins"])
        print(f"\n✅ {label}: {len(merged)} campaigns  ({no_asin} with no ASIN)  →  {out_path.relative_to(ROOT)}")


def main() -> None:
    args = [a.lower() for a in sys.argv[1:]]
    if not args:
        which = ["sp", "sd", "sb"]
    else:
        which = [a for a in args if a in PULLERS]
        if not which:
            print(f"Unknown ad type(s).  Choices: sp, sd, sb")
            sys.exit(2)
    run(which)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
