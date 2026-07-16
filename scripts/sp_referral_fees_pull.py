"""SP-API Product Fees pull — per-ASIN referral %, FBA fees, closing fees,
and total cost of sale for every sku_master ASIN.

Uses the getFeesEstimateForASIN endpoint
(POST /products/fees/v0/items/{ASIN}/feesEstimate).  Requires the
Pricing role on the SP-API app.  If a seller account's token doesn't
have Pricing enabled yet (403), the script logs + skips that account
so partial-rollout works (proof-of-concept 2026-07-16: Nexlev only,
others pending reauth).

Fees returned by Amazon are computed at a HYPOTHETICAL listing price
we send in the request — the referral PERCENT is what matters (the
absolute Rs amount scales linearly with price).  We use PRICE_REF
below (₹1000) as the reference; the CSV records both the % and the
Rs at that reference so operators know the assumption.

Output: data/processed/referral_fees_snapshot.csv (one row per ASIN
per account that had access — same ASIN may appear under multiple
accounts if it's cross-listed).

Columns:
  asin, brand, model, sku, account,
  referral_rs, referral_pct,
  variable_closing_rs, per_item_rs, fba_fees_rs, total_fees_rs,
  price_ref_rs, currency, fetched_at

CLI:
  python scripts/sp_referral_fees_pull.py                 # all accounts + all ASINs
  python scripts/sp_referral_fees_pull.py --account NEXLEV
  python scripts/sp_referral_fees_pull.py --limit 20      # smoke test
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

REPO_ROOT  = Path(__file__).resolve().parent.parent
SKU_MASTER  = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
PRICE_SNAP  = REPO_ROOT / "data" / "processed" / "price_snapshot.csv"
OUT_CSV     = REPO_ROOT / "data" / "processed" / "referral_fees_snapshot.csv"

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
IN_MKT     = "A21TJRUUN4KGV"

# Referral fee % on Amazon India is TIERED by listed price (e.g. Kitchen
# Appliances: X% up to Rs 500, Y% above).  Passing a hypothetical price
# that doesn't match the ASIN's real listing gives the wrong %.
# Prefer the ASIN's actual listed price from price_snapshot.csv; fall
# back to PRICE_REF_FALLBACK when unavailable.
PRICE_REF_FALLBACK = 1000

# Accounts we might have tokens for.  Fossil (CAMBIUMRETAIL) is
# excluded from operator reports so we skip it here too.
ACCOUNTS = ["AUDIOARRAY", "NEXLEV", "VIOMI", "WHITEMULBERRY"]

# Sustained rate limit per Amazon docs: 1 req/sec.  We add 0.1s
# buffer so a burst doesn't tip us into 429.
CALL_INTERVAL_SEC = 1.1


def get_access_token(account: str) -> str | None:
    """Exchange the account's refresh_token for a short-lived access_token.
    Returns None if the account has no refresh_token or LWA rejects it."""
    refresh = os.environ.get(f"SP_REFRESH_TOKEN_{account}")
    client_id = os.environ.get("SP_LWA_CLIENT_ID")
    client_secret = os.environ.get("SP_LWA_CLIENT_SECRET")
    if not (refresh and client_id and client_secret):
        return None
    r = requests.post(
        LWA_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    if r.status_code != 200:
        print(f"  [{account}] LWA failed: {r.status_code} {r.text[:200]}")
        return None
    return r.json()["access_token"]


def fetch_fees_for_asin(access_token: str, asin: str, price: float) -> dict | None:
    """Call the Product Fees API for one ASIN at the given listing price.
    Returns parsed dict or None on error (which is logged)."""
    url = f"{SPAPI_HOST}/products/fees/v0/items/{asin}/feesEstimate"
    payload = {
        "FeesEstimateRequest": {
            "MarketplaceId": IN_MKT,
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {
                "ListingPrice": {"CurrencyCode": "INR", "Amount": price},
            },
            "Identifier": asin,
        }
    }
    r = requests.post(
        url,
        headers={"x-amz-access-token": access_token, "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if r.status_code == 403:
        # Pricing role missing on this app — bail (caller will skip account)
        raise PermissionError(r.text[:400])
    if r.status_code == 429:
        # Rate-limited — sleep + retry once
        time.sleep(2.0)
        r = requests.post(url, headers={"x-amz-access-token": access_token,
                                        "Content-Type": "application/json"},
                          json=payload, timeout=30)
    if r.status_code != 200:
        return {"__error__": f"{r.status_code} {r.text[:200]}"}
    d = r.json()
    fees_res = d.get("payload", {}).get("FeesEstimateResult", {})
    if fees_res.get("Status") != "Success":
        return {"__error__": fees_res.get("Error", {}).get("Message", "unknown")}
    est = fees_res.get("FeesEstimate", {})
    total = est.get("TotalFeesEstimate", {}).get("Amount", 0)
    by_type = {f.get("FeeType"): f.get("FinalFee", {}).get("Amount", 0)
               for f in est.get("FeeDetailList", [])}
    return {
        "referral_rs": by_type.get("ReferralFee", 0),
        "variable_closing_rs": by_type.get("VariableClosingFee", 0),
        "per_item_rs": by_type.get("PerItemFee", 0),
        "fba_fees_rs": by_type.get("FBAFees", 0),
        "total_fees_rs": total,
    }


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", action="append",
                    help="Restrict to one account (e.g. NEXLEV). Can repeat.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Only pull the first N ASINs (smoke test).")
    args = ap.parse_args()

    accounts = args.account or ACCOUNTS

    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()
    m["ASIN"] = m["ASIN"].astype(str).str.strip()
    m = m[m["ASIN"].str.match(r"^B[0-9A-Z]{9}$", na=False)]
    # Skip Fossil universe as per operator rule.
    m = m[m["Brand"].astype(str).str.strip().str.lower() != "fossil"]
    print(f"sku_master ASINs (non-Fossil): {len(m)}")

    # Load per-ASIN listed price so we can query Amazon at the REAL
    # price (referral % is tiered, so hypothetical prices give wrong
    # rates).  Prefer buybox_price when set, else account-specific
    # column, else fall back.
    price_by_asin: dict[str, float] = {}
    if PRICE_SNAP.exists():
        try:
            ps = pd.read_csv(PRICE_SNAP)
            ps.columns = ps.columns.str.strip().str.lower()
            for _, r in ps.iterrows():
                asin_v = str(r.get("asin", "")).strip()
                if not asin_v:
                    continue
                p = None
                for col in ("buybox_price", "price_nexlev", "price_audioarray",
                            "price_whitemulberry", "price_viomi"):
                    if col in ps.columns:
                        v = r.get(col)
                        if pd.notna(v) and float(v) > 0:
                            p = float(v)
                            break
                if p:
                    price_by_asin[asin_v] = p
            print(f"Loaded prices for {len(price_by_asin)} ASINs from {PRICE_SNAP.name}")
        except Exception as e:
            print(f"  ⚠ price_snapshot load failed: {e!r}; will use fallback price")
    else:
        print(f"  ⚠ {PRICE_SNAP} not found; using fallback price INR {PRICE_REF_FALLBACK}")

    all_rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for account in accounts:
        print(f"\n=== {account} ===")
        tok = get_access_token(account)
        if not tok:
            print(f"  skipped — no token or LWA failed")
            continue

        # Restrict to ASINs of this account's primary brand(s).  If AA
        # account covers Tonor too (per operator memory), include both.
        brand_filter = {
            "AUDIOARRAY":    {"Audio Array", "Tonor"},
            "NEXLEV":        {"Nexlev"},
            "VIOMI":         {"Viomi", "Nexlev"},   # Viomi ASINs live in the Viomi account
            "WHITEMULBERRY": {"White Mulberry"},
        }.get(account, set())

        asins_df = m[m["Brand"].astype(str).str.strip().isin(brand_filter)]
        if args.limit:
            asins_df = asins_df.head(args.limit)
        print(f"  ASINs to probe: {len(asins_df)}")

        role_missing = False
        errors = 0
        for i, (_, row) in enumerate(asins_df.iterrows(), 1):
            if role_missing:
                break
            asin = row["ASIN"]
            price_used = price_by_asin.get(asin, PRICE_REF_FALLBACK)
            try:
                res = fetch_fees_for_asin(tok, asin, price_used)
            except PermissionError as e:
                print(f"  [{account}] Pricing role NOT enabled (403) — skipping account")
                print(f"    detail: {str(e)[:200]}")
                role_missing = True
                break
            if res is None:
                errors += 1
            elif "__error__" in res:
                errors += 1
                if errors <= 3:
                    print(f"  [{asin}] {res['__error__']}")
            else:
                referral_pct = round(res["referral_rs"] / price_used * 100, 2) if price_used else 0
                total_fees_pct = round(res["total_fees_rs"] / price_used * 100, 2) if price_used else 0
                all_rows.append({
                    "asin":                asin,
                    "brand":               str(row.get("Brand", "")).strip(),
                    "model":               str(row.get("Model", "")).strip(),
                    "sku":                 str(row.get("FBA SKU", "")).strip(),
                    "account":             account,
                    "referral_rs":         res["referral_rs"],
                    "referral_pct":        referral_pct,
                    "variable_closing_rs": res["variable_closing_rs"],
                    "per_item_rs":         res["per_item_rs"],
                    "fba_fees_rs":         res["fba_fees_rs"],
                    "total_fees_rs":       res["total_fees_rs"],
                    "total_fees_pct":      total_fees_pct,
                    "price_used_rs":       price_used,
                    "price_source":        "listed" if asin in price_by_asin else "fallback",
                    "currency":            "INR",
                    "fetched_at":          now,
                })
            time.sleep(CALL_INTERVAL_SEC)
            if i % 25 == 0:
                print(f"    progress: {i}/{len(asins_df)} ok={len(all_rows)} err={errors}")

        print(f"  [{account}] done — {len(all_rows)} rows total so far, {errors} errors")

    if not all_rows:
        print("\nNo rows collected — check Pricing role on the SP-API app.")
        return 2

    out = pd.DataFrame(all_rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if OUT_CSV.exists():
        # Append-and-dedupe: keep newest fetched_at per (account, asin).
        old = pd.read_csv(OUT_CSV)
        merged = pd.concat([old, out], ignore_index=True)
        merged = merged.sort_values("fetched_at").drop_duplicates(
            subset=["account", "asin"], keep="last"
        )
        merged.to_csv(OUT_CSV, index=False)
        print(f"\nWrote {len(merged)} rows -> {OUT_CSV} (merged with prior)")
    else:
        out.to_csv(OUT_CSV, index=False)
        print(f"\nWrote {len(out)} rows -> {OUT_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
