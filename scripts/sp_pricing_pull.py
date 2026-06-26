"""SP-API Pricing pull — selling prices per (account, ASIN) for every
non-Fossil ASIN in sku_master.

Two SP-API endpoints are used:

  1. getPricing  (POST /products/pricing/v0/price)
     - Returns OUR seller account's own listed offers for a batch of
       up to 20 ASINs. Rate: ~10 r/s burst, 1 r/s steady — fast.
     - Called once per seller account; output is "this account lists
       ASIN X at Consumer price Y".

  2. getItemOffersBatch  (POST /batches/products/pricing/v0/itemOffers)
     - Returns full offer list per ASIN: every active seller's price,
       plus Summary.BuyBoxPrices.  Rate: ~0.1 r/s — slow.
     - Called once across all ASINs (any account's creds work — the
       response is the same offer list regardless of caller).
     - We extract: buy-box landed price + the buy-box winner's
       SellerId.  We do NOT try to label "Amazon 1P vs other 3P" —
       the SellerId is recorded as-is; whether the operator sees that
       as 1P is reflected by buybox_belongs_to_us (False).

Output: data/processed/price_snapshot.csv (wide format, one row per
ASIN, with per-account listed-price columns):

  asin, sku, brand, model,
  price_audioarray, price_nexlev, price_viomi, price_whitemulberry,
  buybox_price, buybox_seller_id, buybox_belongs_to_us,
  currency, fetched_at

Operator rule: Fossil is excluded everywhere on the dashboard, so
Fossil ASINs are dropped before any SP-API call.  CAMBIUMRETAIL is
the Fossil seller account → also skipped.

Required env (same shared LWA app as the other 3P seller pulls):
  SP_LWA_CLIENT_ID
  SP_LWA_CLIENT_SECRET
  SP_REFRESH_TOKEN_AUDIOARRAY
  SP_REFRESH_TOKEN_NEXLEV
  SP_REFRESH_TOKEN_VIOMI
  SP_REFRESH_TOKEN_WHITEMULBERRY

CLI:
  python scripts/sp_pricing_pull.py             # full pull
  python scripts/sp_pricing_pull.py --limit 50  # quick smoke test
  python scripts/sp_pricing_pull.py --skip-buybox  # 3P-only fast path
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────
REPO_ROOT  = Path(__file__).resolve().parent.parent
SKU_MASTER = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
OUT_CSV    = REPO_ROOT / "data" / "processed" / "price_snapshot.csv"

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"   # IN marketplace = EU region
IN_MKT     = "A21TJRUUN4KGV"

# Operator rule: Fossil excluded everywhere on the dashboard, so the
# corresponding seller account (CAMBIUMRETAIL) is also skipped.
ACCOUNTS = [
    "AUDIOARRAY",     # also covers Tonor listings
    "NEXLEV",
    "VIOMI",
    "WHITEMULBERRY",
]

# Column names mirror the account list in lowercase.
ACCOUNT_COL = {a: f"price_{a.lower()}" for a in ACCOUNTS}

EXCLUDED_BRANDS = {"fossil"}


# ─────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────
def get_access_token(account: str) -> str | None:
    cid = os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get("SP_LWA_CLIENT_SECRET")
    rt  = os.environ.get(f"SP_REFRESH_TOKEN_{account}")
    if not (cid and sec and rt):
        print(f"  ⚠ [{account}] missing SP_LWA_* / SP_REFRESH_TOKEN_{account} — skipping")
        return None
    r = requests.post(LWA_URL, data={
        "grant_type":    "refresh_token",
        "refresh_token": rt,
        "client_id":     cid,
        "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        print(f"  ⚠ [{account}] LWA failed HTTP {r.status_code}: {r.text[:200]}")
        return None
    return r.json()["access_token"]


# ─────────────────────────────────────────────────────────────────────
# Master loader
# ─────────────────────────────────────────────────────────────────────
def load_master(limit: int | None = None) -> pd.DataFrame:
    if not SKU_MASTER.exists():
        raise SystemExit(f"sku_master.xlsx missing at {SKU_MASTER}")
    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()
    out = m[["ASIN", "FBA SKU", "Brand", "Model"]].copy()
    out.columns = ["asin", "sku", "brand", "model"]
    for c in out.columns:
        out[c] = out[c].astype(str).str.strip().replace({"nan": "", "None": "", "-": ""})
    out = out[out["asin"].ne("")]
    # Drop Fossil per operator rule
    out = out[~out["brand"].str.lower().isin(EXCLUDED_BRANDS)]
    # Deduplicate by ASIN; if a (rare) ASIN is multi-brand, keep first
    out = out.drop_duplicates(subset=["asin"]).reset_index(drop=True)
    if limit:
        out = out.head(limit)
    return out


# ─────────────────────────────────────────────────────────────────────
# getPricing — per account, batch of up to 20 ASINs
# Returns dict[asin] = float price (Consumer, ItemCondition=New) if listed.
# ─────────────────────────────────────────────────────────────────────
def fetch_account_listings(account: str, asins: list[str]) -> dict[str, float]:
    """Return {asin: listed_consumer_price} for ASINs this account is
    selling.  ASINs the account doesn't list are simply absent from
    the dict — not zero, not None."""
    tok = get_access_token(account)
    if not tok:
        return {}
    H = {"x-amz-access-token": tok}
    out: dict[str, float] = {}

    # Batch of 20 ASINs per call.
    for i in range(0, len(asins), 20):
        chunk = asins[i:i + 20]
        params = {
            "MarketplaceId": IN_MKT,
            "ItemType":      "Asin",
            "Asins":         ",".join(chunk),
            "CustomerType":  "Consumer",
            "ItemCondition": "New",
        }
        try:
            r = requests.get(
                f"{SPAPI_HOST}/products/pricing/v0/price",
                params=params, headers=H, timeout=30,
            )
        except requests.RequestException as e:
            print(f"  ⚠ [{account}] HTTP error on chunk {i//20}: {e}")
            time.sleep(2)
            continue
        if r.status_code == 429:
            # Throttled — back off and retry once
            time.sleep(5)
            r = requests.get(
                f"{SPAPI_HOST}/products/pricing/v0/price",
                params=params, headers=H, timeout=30,
            )
        if r.status_code != 200:
            # 404 / 400 on a single chunk shouldn't sink the whole pull
            print(f"  ⚠ [{account}] HTTP {r.status_code} on chunk {i//20}: {r.text[:160]}")
            continue
        payload = r.json()
        for prod in (payload.get("payload") or []):
            asin = (prod.get("ASIN") or "").strip()
            if not asin:
                continue
            offers = ((prod.get("Product") or {}).get("Offers") or [])
            if not offers:
                continue
            # We requested ItemCondition=New + CustomerType=Consumer;
            # the response is already filtered, but defensively pick
            # the lowest listing price across returned offers (an
            # account can have multiple SKUs for the same ASIN).
            prices = []
            for off in offers:
                lp = ((off.get("BuyingPrice") or {}).get("ListingPrice") or {})
                amt = lp.get("Amount")
                if amt is not None:
                    prices.append(float(amt))
            if prices:
                out[asin] = min(prices)
        # 1 r/s steady throughput target — sleep a hair under that.
        time.sleep(0.15)
    return out


# ─────────────────────────────────────────────────────────────────────
# getItemOffersBatch — one call covers up to 20 ASINs; returns full
# offer list + buy-box summary.  We extract buy-box price + winner.
# Rate: 0.1 r/s, burst 1 — sleep 10s between batches.
# ─────────────────────────────────────────────────────────────────────
def fetch_buybox(asins: list[str], any_account: str) -> dict[str, dict]:
    """Return {asin: {buybox_price, buybox_seller_id, currency}}."""
    tok = get_access_token(any_account)
    if not tok:
        print(f"  ⚠ buy-box pull skipped — no creds for {any_account}")
        return {}
    H = {"x-amz-access-token": tok, "Content-Type": "application/json"}
    out: dict[str, dict] = {}

    chunks = [asins[i:i + 20] for i in range(0, len(asins), 20)]
    print(f"  buy-box: {len(asins)} ASINs in {len(chunks)} batches "
          f"(~{len(chunks) * 11}s at the 0.1 r/s budget)")

    for idx, chunk in enumerate(chunks):
        body = {"requests": [
            {
                "uri":           f"/products/pricing/v0/items/{a}/offers",
                "method":        "GET",
                "MarketplaceId": IN_MKT,
                "ItemCondition": "New",
                "CustomerType":  "Consumer",
            }
            for a in chunk
        ]}
        try:
            r = requests.post(
                f"{SPAPI_HOST}/batches/products/pricing/v0/itemOffers",
                json=body, headers=H, timeout=60,
            )
        except requests.RequestException as e:
            print(f"  ⚠ batch {idx} HTTP error: {e}")
            time.sleep(15)
            continue

        if r.status_code == 429:
            time.sleep(30)
            r = requests.post(
                f"{SPAPI_HOST}/batches/products/pricing/v0/itemOffers",
                json=body, headers=H, timeout=60,
            )
        if r.status_code != 200:
            print(f"  ⚠ batch {idx} HTTP {r.status_code}: {r.text[:200]}")
            time.sleep(11)
            continue

        responses = (r.json().get("responses") or [])
        for resp in responses:
            body_o = resp.get("body") or {}
            payload = body_o.get("payload") or {}
            asin = (payload.get("ASIN") or "").strip()
            if not asin:
                continue
            summary = payload.get("Summary") or {}
            bbs = summary.get("BuyBoxPrices") or []
            currency = None
            buybox_price = None
            for p in bbs:
                lp = p.get("LandedPrice") or p.get("ListingPrice") or {}
                amt = lp.get("Amount")
                if amt is not None:
                    buybox_price = float(amt)
                    currency = lp.get("CurrencyCode") or currency
                    break
            # Find the buy-box winner in Offers (IsBuyBoxWinner=True)
            winner_id = None
            for off in (payload.get("Offers") or []):
                if off.get("IsBuyBoxWinner"):
                    winner_id = (off.get("SellerId") or "").strip() or None
                    if currency is None:
                        lp = (off.get("ListingPrice") or {})
                        currency = lp.get("CurrencyCode") or currency
                    break

            if buybox_price is None and winner_id is None:
                # No active buy-box — record nothing for this ASIN
                continue
            out[asin] = {
                "buybox_price":     buybox_price,
                "buybox_seller_id": winner_id,
                "currency":         currency or "INR",
            }
        # 0.1 r/s budget = 1 call per 10s
        time.sleep(11)
    return out


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────
def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap the number of ASINs (smoke testing)")
    ap.add_argument("--skip-buybox", action="store_true",
                    help="Skip the slow buy-box pull; per-account listings only")
    args = ap.parse_args()

    master = load_master(limit=args.limit)
    if master.empty:
        print("ERROR: master loaded zero ASINs (after excluding Fossil).")
        return 1
    asins = master["asin"].tolist()
    print(f"Pricing pull: {len(asins)} ASINs across {len(ACCOUNTS)} accounts.")

    # 1. Per-account listings (fast)
    listings: dict[str, dict[str, float]] = {}
    for acct in ACCOUNTS:
        print(f"  [{acct}] getPricing for {len(asins)} ASINs…")
        prices = fetch_account_listings(acct, asins)
        listings[acct] = prices
        print(f"  [{acct}] → {len(prices)} listed.")

    # 2. Buy-box (slow; opt-out via --skip-buybox)
    buybox = {}
    if not args.skip_buybox:
        print("  Buy-box & seller pull (getItemOffersBatch — slow)…")
        # Use whichever account succeeded for LWA first; the response
        # doesn't depend on caller identity for this endpoint.
        any_acct = next((a for a in ACCOUNTS if listings.get(a) is not None), ACCOUNTS[0])
        buybox = fetch_buybox(asins, any_acct)
        print(f"  → buy-box for {len(buybox)} ASINs.")

    # Identify our seller IDs so we can tag buybox_belongs_to_us = True
    # when our offer is winning.  We need at least one MyOffer hit per
    # account from getItemOffers; without it we can't know our SellerId.
    # As a pragmatic fallback the operator can drop a JSON file at
    # data/master/our_seller_ids.json with {"AUDIOARRAY": "A1XYZ...", ...};
    # we'll use it if present.
    our_seller_ids: set[str] = set()
    sid_file = REPO_ROOT / "data" / "master" / "our_seller_ids.json"
    if sid_file.exists():
        try:
            import json
            with open(sid_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            our_seller_ids = {str(v).strip() for v in cfg.values() if v}
            print(f"  Loaded {len(our_seller_ids)} known seller IDs from {sid_file.name}")
        except Exception as e:
            print(f"  ⚠ couldn't read {sid_file.name}: {e}")

    # 3. Build wide row per ASIN
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = []
    for _, m in master.iterrows():
        asin = m["asin"]
        row = {
            "asin":  asin,
            "sku":   m["sku"],
            "brand": m["brand"],
            "model": m["model"],
        }
        for acct in ACCOUNTS:
            row[ACCOUNT_COL[acct]] = listings.get(acct, {}).get(asin)
        bb = buybox.get(asin, {})
        row["buybox_price"]         = bb.get("buybox_price")
        row["buybox_seller_id"]     = bb.get("buybox_seller_id")
        row["buybox_belongs_to_us"] = (
            bool(our_seller_ids) and bb.get("buybox_seller_id") in our_seller_ids
        )
        row["currency"]   = bb.get("currency", "INR")
        row["fetched_at"] = fetched_at
        rows.append(row)

    df = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    print(f"✓ wrote {OUT_CSV.relative_to(REPO_ROOT)} ({len(df)} rows)")

    # Friendly summary
    print()
    print(f"  Coverage by account:")
    for acct in ACCOUNTS:
        col = ACCOUNT_COL[acct]
        print(f"    {acct:<16}  {df[col].notna().sum():>5} / {len(df)}")
    if "buybox_price" in df.columns:
        print(f"    buy-box found    {df['buybox_price'].notna().sum():>5} / {len(df)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
