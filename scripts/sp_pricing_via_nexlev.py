"""SP-API Pricing puller — Amazon 1P + Buy Box + per-account 3P prices
via Nexlev's Pricing-scoped token.

Workaround for the AA/WM/CRPL Pricing-scope gap (their refresh_tokens
pre-date the Pricing role addition to the AdPilot LWA app 2026-07-16;
reauth pending).  Nexlev's Amazon Inbound LWA app HAS Pricing scope,
and getItemOffers returns ALL offers on ANY ASIN regardless of the
calling account — so we can pull Buy Box + Amazon 1P + 3P offers for
every non-Fossil brand via Nexlev's token alone.

Endpoint: GET /products/pricing/v0/items/{ASIN}/offers
  Returns:
    Summary.BuyBoxPrices[]        — Buy Box winner (any fulfillment)
    Summary.LowestPrices[]        — lowest per fulfillmentChannel
                                    (Amazon = 1P, Merchant = 3P)
    Offers[]                      — every seller's offer with SellerId,
                                    LandedPrice, PrimeInformation, etc.

Rate limit: ~1 req/sec sustained.  Puller runs at 0.55s sleep to stay
under that.  ~230 AA + 35 Tonor + N Nexlev + N WM ASINs → 4-6 min.

Per-account 3P attribution:
  Uses `data/master/our_seller_ids.json` — {brand_key: SellerId, ...}.
  When present, Offers[] gets filtered by SellerId and each account's
  landed price populates `price_<account>` columns.
  When absent OR seller_id not in the offers, that column is null.

Output: data/processed/price_snapshot_nexlev.csv (wide format, one
row per ASIN).  Schema mirrors sp_pricing_pull.py's output so the UI
can consume either.

CLI:
  python scripts/sp_pricing_via_nexlev.py
      # all non-Fossil ASINs, all brands
  python scripts/sp_pricing_via_nexlev.py --brand audio_array,tonor
      # comma-separated brands (folder-slug form)
  python scripts/sp_pricing_via_nexlev.py --brand nexlev --limit 20
      # smoke test
  python scripts/sp_pricing_via_nexlev.py --out data/processed/foo.csv
      # custom output path

Required env: SP_LWA_CLIENT_ID, SP_LWA_CLIENT_SECRET,
              SP_REFRESH_TOKEN_NEXLEV
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


REPO_ROOT   = Path(__file__).resolve().parent.parent
SKU_MASTER  = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
SELLER_IDS  = REPO_ROOT / "data" / "master" / "our_seller_ids.json"
OUT_DEFAULT = REPO_ROOT / "data" / "processed" / "price_snapshot_nexlev.csv"

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
MARKET_IN  = "A21TJRUUN4KGV"   # amazon.in marketplace + Amazon India's SellerId

# Brand-folder slug → sku_master Brand column value
BRAND_ALIASES: dict[str, str] = {
    "audio_array":    "Audio Array",
    "audioarray":     "Audio Array",
    "audio array":    "Audio Array",
    "aa":             "Audio Array",
    "nexlev":         "Nexlev",
    "tonor":          "Tonor",
    "white_mulberry": "White Mulberry",
    "whitemulberry":  "White Mulberry",
    "wm":             "White Mulberry",
}

# Reverse — brand string → column-suffix slug used in output CSV
COL_SLUG: dict[str, str] = {
    "Audio Array":    "audioarray",
    "Nexlev":         "nexlev",
    "Tonor":          "tonor",
    "White Mulberry": "whitemulberry",
    "Viomi":          "viomi",
}


def load_seller_ids() -> dict[str, str]:
    """Load per-account SellerId map; returns {} if file absent."""
    if not SELLER_IDS.exists():
        return {}
    try:
        return {k.strip().lower(): v for k, v in json.loads(SELLER_IDS.read_text()).items()}
    except Exception as e:
        print(f"[warn] failed to parse {SELLER_IDS}: {e}")
        return {}


def get_access_token(acct: str = "NEXLEV") -> str:
    cid = os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get("SP_LWA_CLIENT_SECRET")
    rt  = os.environ.get(f"SP_REFRESH_TOKEN_{acct}")
    if not (cid and sec and rt):
        raise SystemExit(f"Missing env for {acct}: need SP_LWA_CLIENT_ID / SECRET / "
                         f"SP_REFRESH_TOKEN_{acct}")
    r = requests.post(LWA_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": rt,
        "client_id": cid,
        "client_secret": sec,
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def fetch_offers(tok: str, asin: str) -> dict | None:
    """GET one ASIN's offers.  Returns parsed payload dict or None on error."""
    r = requests.get(
        f"{SPAPI_HOST}/products/pricing/v0/items/{asin}/offers",
        headers={"x-amz-access-token": tok},
        params={"MarketplaceId": MARKET_IN, "ItemCondition": "New"},
        timeout=30,
    )
    if r.status_code == 200:
        return (r.json().get("payload") or {})
    return {"__error": f"HTTP {r.status_code}"}


def parse_offers(payload: dict, seller_ids: dict[str, str]) -> dict:
    """Extract Buy Box, Amazon 1P, per-account 3P prices from offers payload."""
    if not payload or payload.get("__error"):
        return {"error": (payload or {}).get("__error", "empty payload")}

    summary = payload.get("Summary") or {}
    offers  = payload.get("Offers") or []

    # Buy Box price + winner
    bb = (summary.get("BuyBoxPrices") or [{}])[0]
    bb_price  = ((bb.get("LandedPrice") or {}).get("Amount"))
    bb_seller = None  # Summary doesn't expose SellerId; find from Offers[IsBuyBoxWinner]
    for o in offers:
        if o.get("IsBuyBoxWinner"):
            bb_seller = o.get("SellerId")
            break

    # Amazon 1P price (fulfillmentChannel = Amazon in LowestPrices)
    amz_1p = None
    for lp in (summary.get("LowestPrices") or []):
        if lp.get("fulfillmentChannel") == "Amazon":
            amz_1p = (lp.get("LandedPrice") or {}).get("Amount")
            break

    # 3P lowest merchant price
    merchant_low = None
    for lp in (summary.get("LowestPrices") or []):
        if lp.get("fulfillmentChannel") == "Merchant":
            merchant_low = (lp.get("LandedPrice") or {}).get("Amount")
            break

    # Offer count
    oc = ((summary.get("NumberOfOffers") or [{}])[0] or {}).get("OfferCount")

    # Per-account 3P prices — filter Offers[] by known seller IDs
    per_account: dict[str, float | None] = {}
    for brand_key, sid in seller_ids.items():
        slug = COL_SLUG.get(BRAND_ALIASES.get(brand_key, brand_key.title()),
                            brand_key.lower())
        match = next((o for o in offers if o.get("SellerId") == sid), None)
        per_account[slug] = ((match.get("ListingPrice") or {}).get("Amount")
                             if match else None)

    return {
        "buybox_price":       bb_price,
        "buybox_seller_id":   bb_seller,
        "amazon_1p_price":    amz_1p,
        "merchant_low_price": merchant_low,
        "offer_count":        oc,
        **{f"price_{slug}": v for slug, v in per_account.items()},
    }


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    ap = argparse.ArgumentParser()
    ap.add_argument("--brand", help="Comma-separated brand slugs (e.g. audio_array,tonor). "
                                     "Default: all non-Fossil brands in sku_master.")
    ap.add_argument("--limit", type=int, help="First N ASINs (smoke test).")
    ap.add_argument("--out",   type=Path, default=OUT_DEFAULT, help="Output CSV path.")
    ap.add_argument("--sleep", type=float, default=0.55,
                    help="Sec between calls (rate limit ~1 req/sec).")
    args = ap.parse_args()

    if not SKU_MASTER.exists():
        raise SystemExit(f"sku_master missing at {SKU_MASTER}")
    master = pd.read_excel(SKU_MASTER).rename(columns={"FBA SKU": "sku"})
    master = master.dropna(subset=["ASIN"]).copy()
    master["ASIN"] = master["ASIN"].astype(str).str.strip()

    # Brand filter
    if args.brand:
        wanted = {BRAND_ALIASES.get(b.strip().lower(), b.strip())
                  for b in args.brand.split(",") if b.strip()}
        master = master[master["Brand"].astype(str).str.strip().isin(wanted)]
        print(f"Brands: {sorted(wanted)}")
    else:
        # Default: everything except Fossil
        master = master[master["Brand"].astype(str).str.strip().str.lower() != "fossil"]
        print(f"Brands: all non-Fossil ({sorted(master['Brand'].astype(str).str.strip().unique())})")

    asins = master["ASIN"].unique().tolist()
    if args.limit:
        asins = asins[:args.limit]
    if not asins:
        print("No ASINs to process.  Exiting.")
        return 0
    print(f"ASINs: {len(asins)}")

    seller_ids = load_seller_ids()
    if seller_ids:
        print(f"Loaded {len(seller_ids)} seller IDs from {SELLER_IDS.name}: "
              f"{sorted(seller_ids.keys())}")
    else:
        print(f"No {SELLER_IDS.name} found — per-account 3P prices will be null. "
              f"Add {{brand: SellerId}} JSON to enable that column.")

    tok = get_access_token("NEXLEV")

    rows = []
    fetched_at = datetime.now(timezone.utc).isoformat()
    for i, asin in enumerate(asins, 1):
        payload = fetch_offers(tok, asin)
        parsed = parse_offers(payload or {}, seller_ids)
        rows.append({"asin": asin, "fetched_at": fetched_at, **parsed})
        if i % 25 == 0:
            got = sum(1 for r in rows if r.get("amazon_1p_price") or r.get("buybox_price"))
            print(f"  {i}/{len(asins)}  got={got}", flush=True)
        time.sleep(args.sleep)

    df = pd.DataFrame(rows)
    df = df.merge(
        master[["ASIN", "sku", "Brand", "Model"]].rename(
            columns={"ASIN": "asin", "Brand": "brand", "Model": "model"}
        ),
        on="asin", how="left",
    )

    # Canonical column order — sku/brand/model first, then prices, then metadata
    front = ["asin", "sku", "brand", "model"]
    prices = [c for c in df.columns
              if c.startswith("price_") or c in ("amazon_1p_price", "buybox_price",
                                                 "merchant_low_price")]
    meta = ["buybox_seller_id", "offer_count", "error", "fetched_at"]
    cols = front + prices + [c for c in meta if c in df.columns]
    df = df[[c for c in cols if c in df.columns]]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)

    print()
    print(f"✓ wrote {args.out.relative_to(REPO_ROOT)} ({len(df)} rows)")
    print(f"  Amazon 1P coverage: {df['amazon_1p_price'].notna().sum()}/{len(df)}")
    print(f"  Buy Box coverage:   {df['buybox_price'].notna().sum()}/{len(df)}")
    for c in df.columns:
        if c.startswith("price_"):
            print(f"  {c:<30} {df[c].notna().sum()}/{len(df)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
