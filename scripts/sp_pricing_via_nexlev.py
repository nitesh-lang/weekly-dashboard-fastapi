"""Buy Box owner puller — for every ASIN, WHO owns the Buy Box today?

Answers the operator question: for our brands' ASINs on Amazon.in, is
Amazon (1P), us (our seller account), or a competitor 3P reseller
winning the Buy Box right now — and at what price?

## Use cases

  Investigate 1P Buy Box:  --brand audio_array
                            --brand tonor
       Both brands have Amazon 1P vendor sales.  This shows which
       ASINs Amazon India (SellerId A21TJRUUN4KGV) actually wins
       Buy Box on vs which get taken by a 3P reseller (yours or
       competitor).

  Investigate 3P Buy Box:  --brand nexlev
                            --brand white_mulberry
       Both brands sell primarily via their own 3P seller accounts.
       Shows if your Nexlev / WM account is winning Buy Box vs
       another reseller undercutting you.

## How it works

Uses `getItemOffers` (v0 per-ASIN) which returns every seller's offer
on the ASIN, plus Amazon's Summary block with Buy Box winner and
lowest per-fulfillment-channel price.  Calls go through Nexlev's LWA
token because AA/WM/CRPL tokens pre-date the Pricing role addition
to the AdPilot app (2026-07-16); Nexlev's Amazon Inbound app HAS
Pricing scope, and pricing endpoints return the same marketplace-
wide data regardless of the calling account.

Rate limit ~1 req/sec sustained → 0.55s sleep between calls.
226 AA / 35 Tonor / ~200 Nexlev / ~90 WM ASINs = 3-5 min per brand.

## Us vs them attribution

Provide `data/master/our_seller_ids.json` in this format:
  {
    "audio_array":    "A28KGDTB760OU9",
    "nexlev":         "A_your_nexlev_id",
    "tonor":          "A15YBOSC0EMOS0",
    "white_mulberry": "A_your_wm_id"
  }
Then output's `buybox_seller_name` column shows:
  - "Amazon India (1P)"  → Amazon 1P wins
  - "Audio Array (3P)"   → your AA seller wins
  - "Other 3P (SellerId)" → a competitor wins (you have SellerId to
                            search on Seller Central or Google)
Without seller_ids.json, all your accounts appear as "Other 3P".

## Output

`data/processed/price_snapshot_nexlev.csv` (or --out) with columns:
  asin, sku, brand, model,
  amazon_1p_price,      # from LowestPrices where fulfillmentChannel=Amazon
                        # (includes FBA 3P offers, not just Amazon 1P)
  buybox_price,         # Buy Box landed price
  merchant_low_price,   # cheapest 3P offer regardless of Buy Box
  price_<slug>,         # your account's landed price (needs seller_ids.json)
  buybox_seller_name,   # who owns Buy Box (readable)
  buybox_seller_id,     # raw SellerId (source of truth)
  offer_count, error, fetched_at

The script ALSO prints a **Buy Box owner leaderboard** at the end —
top winners across all ASINs pulled, with counts and share.  That
leaderboard is the quickest way to spot competitors.

## CLI

  python scripts/sp_pricing_via_nexlev.py --brand nexlev
  python scripts/sp_pricing_via_nexlev.py --brand white_mulberry
  python scripts/sp_pricing_via_nexlev.py --brand audio_array
  python scripts/sp_pricing_via_nexlev.py --brand tonor
  python scripts/sp_pricing_via_nexlev.py --brand nexlev,white_mulberry
  python scripts/sp_pricing_via_nexlev.py --brand audio_array --limit 25   # smoke test
  python scripts/sp_pricing_via_nexlev.py --brand tonor --out data/processed/tonor_bb.csv

Required env in .env:
  SP_LWA_CLIENT_ID, SP_LWA_CLIENT_SECRET, SP_REFRESH_TOKEN_NEXLEV
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
MARKET_IN  = "A21TJRUUN4KGV"   # amazon.in marketplace

# Well-known SellerIds (readable name in Buy Box column).  Our own accounts
# get overlaid from data/master/our_seller_ids.json when available.
KNOWN_SELLERS: dict[str, str] = {
    "A21TJRUUN4KGV":  "Amazon India (1P)",
    "A2XU06AVSXMD3D": "Cloudtail India",   # kept for historical rows
    "AT95IG9ONZD7S":  "Appario Retail",    # kept for historical rows
}

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


def parse_offers(payload: dict, seller_ids: dict[str, str],
                 seller_name_map: dict[str, str]) -> dict:
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
    # Human-readable name
    if bb_seller is None:
        bb_seller_name = None
    elif bb_seller in seller_name_map:
        bb_seller_name = seller_name_map[bb_seller]
    else:
        bb_seller_name = f"Other 3P ({bb_seller})"

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
        "buybox_seller_name": bb_seller_name,
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
        print(f"No {SELLER_IDS.name} found — per-account 3P price columns will be null "
              f"AND our accounts will show as 'Other 3P' in buybox_seller_name. "
              f"Add {{brand: SellerId}} JSON to enable friendly names + per-account cols.")

    # Build reverse map for buybox_seller_name
    seller_name_map = dict(KNOWN_SELLERS)
    for brand_key, sid in seller_ids.items():
        brand_full = BRAND_ALIASES.get(brand_key, brand_key.title())
        seller_name_map[sid] = f"{brand_full} (3P)"

    tok = get_access_token("NEXLEV")

    rows = []
    fetched_at = datetime.now(timezone.utc).isoformat()
    for i, asin in enumerate(asins, 1):
        payload = fetch_offers(tok, asin)
        parsed = parse_offers(payload or {}, seller_ids, seller_name_map)
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
    meta = ["buybox_seller_name", "buybox_seller_id", "offer_count", "error", "fetched_at"]
    cols = front + prices + [c for c in meta if c in df.columns]
    df = df[[c for c in cols if c in df.columns]]

    out_path = args.out if args.out.is_absolute() else (REPO_ROOT / args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print()
    try:
        print(f"✓ wrote {out_path.relative_to(REPO_ROOT)} ({len(df)} rows)")
    except ValueError:
        print(f"✓ wrote {out_path} ({len(df)} rows)")
    print(f"  Amazon 1P coverage: {df['amazon_1p_price'].notna().sum()}/{len(df)}")
    print(f"  Buy Box coverage:   {df['buybox_price'].notna().sum()}/{len(df)}")
    for c in df.columns:
        if c.startswith("price_"):
            print(f"  {c:<30} {df[c].notna().sum()}/{len(df)}")

    # ── Buy Box owner leaderboard — the point of this pull ──
    print()
    print("─" * 72)
    print("BUY BOX OWNER LEADERBOARD  (who's winning Buy Box across these ASINs)")
    print("─" * 72)
    has_bb = df[df["buybox_seller_id"].notna()].copy()
    if has_bb.empty:
        print("  (no Buy Box winners found — Amazon returned empty Summary for all ASINs)")
    else:
        # Group + count
        lb = (has_bb.groupby(["buybox_seller_id", "buybox_seller_name"])
                    .size()
                    .reset_index(name="asins_won")
                    .sort_values("asins_won", ascending=False))
        total_bb = int(lb["asins_won"].sum())
        print(f"  {total_bb} ASINs with Buy Box owner identified "
              f"(of {len(df)} pulled)\n")
        print(f"  {'SellerId':<16}  {'Name':<32}  {'ASINs':>5}  Share")
        print(f"  {'─'*16}  {'─'*32}  {'─'*5}  ─────")
        for _, row in lb.head(15).iterrows():
            share = row["asins_won"] / total_bb * 100
            print(f"  {row['buybox_seller_id']:<16}  "
                  f"{row['buybox_seller_name'][:32]:<32}  "
                  f"{row['asins_won']:>5}  {share:5.1f}%")
        if len(lb) > 15:
            print(f"  ... {len(lb)-15} more sellers with 1-few ASINs each")
        print()
        print("Look up unknown SellerIds via storefront URL:")
        print("  https://www.amazon.in/sp?seller=<SellerId>")
        print("Add your own accounts to data/master/our_seller_ids.json")
        print("to replace 'Other 3P' with your account name in future runs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
