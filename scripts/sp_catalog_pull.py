"""SP-API Catalog Items pull — per-ASIN PDP content (title, brand,
bullets, dimensions, sales rank) for every sku_master ASIN.

Uses the getCatalogItem endpoint
(GET /catalog/2022-04-01/items/{ASIN}).  Requires the Product Listing
role on the SP-API app.  If a seller account's token doesn't have
Product Listing enabled yet (403), the script logs + skips that
account (partial-rollout friendly).

Any account's token can fetch a given ASIN's catalog data (Amazon's
catalog is marketplace-scoped, not account-scoped), so we default to
using the FIRST account with Product Listing granted and iterate
every non-Fossil ASIN once.  --account can override.

Output: data/processed/catalog_snapshot.csv (one row per ASIN).

Columns:
  asin, brand_master, model_master, sku_master,
  title, brand_amazon, product_type, item_type_keyword,
  bullets (semicolon-joined), color, size,
  dimensions_cm, weight_g,
  main_image, image_count,
  browse_classification, sales_rank_top,
  fetched_at

CLI:
  python scripts/sp_catalog_pull.py                       # all ASINs, first available account
  python scripts/sp_catalog_pull.py --account NEXLEV      # force account
  python scripts/sp_catalog_pull.py --brand Nexlev        # only Nexlev ASINs
  python scripts/sp_catalog_pull.py --limit 20            # smoke test
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
SKU_MASTER = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
OUT_CSV    = REPO_ROOT / "data" / "processed" / "catalog_snapshot.csv"

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
IN_MKT     = "A21TJRUUN4KGV"

ACCOUNTS_PREFERENCE = ["NEXLEV", "AUDIOARRAY", "VIOMI", "WHITEMULBERRY"]

# Rate limit for Catalog Items API: ~2 req/sec sustained.
CALL_INTERVAL_SEC = 0.55

INCLUDED_DATA = "attributes,identifiers,images,productTypes,salesRanks,summaries,classifications"


def get_access_token(account: str) -> str | None:
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


def fetch_catalog_item(access_token: str, asin: str) -> dict | None:
    url = f"{SPAPI_HOST}/catalog/2022-04-01/items/{asin}"
    params = {"marketplaceIds": IN_MKT, "includedData": INCLUDED_DATA}
    r = requests.get(
        url,
        headers={"x-amz-access-token": access_token},
        params=params,
        timeout=30,
    )
    if r.status_code == 403:
        raise PermissionError(r.text[:400])
    if r.status_code == 429:
        time.sleep(2.0)
        r = requests.get(url, headers={"x-amz-access-token": access_token},
                         params=params, timeout=30)
    if r.status_code == 404:
        return {"__error__": "not-in-catalog"}
    if r.status_code != 200:
        return {"__error__": f"{r.status_code} {r.text[:200]}"}
    return r.json()


def _first(v):
    """Amazon returns most attributes as a list of {marketplace_id, value}
    dicts.  Return the first non-empty value."""
    if not v:
        return ""
    if isinstance(v, list):
        for item in v:
            if isinstance(item, dict) and "value" in item and item["value"]:
                return item["value"]
        return ""
    return v


def parse_catalog(d: dict) -> dict:
    """Flatten the getCatalogItem response into the CSV row shape."""
    if not d or "__error__" in d:
        return {"__error__": d.get("__error__", "empty")}
    summ = (d.get("summaries") or [{}])[0]
    attrs = d.get("attributes") or {}
    images_lists = d.get("images") or []
    images = images_lists[0].get("images", []) if images_lists else []
    ranks = d.get("salesRanks") or [{}]
    rank0 = (ranks[0].get("classificationRanks") or [{}])[0] if ranks else {}
    prod_types = d.get("productTypes") or [{}]
    classifs = d.get("classifications") or [{}]
    cls0 = (classifs[0].get("classifications") or [{}])[0] if classifs else {}

    # Dimensions: attrs["item_dimensions"] or attrs["package_dimensions"]
    item_dims = _first(attrs.get("item_dimensions"))
    dims_str = ""
    if isinstance(item_dims, dict):
        L = (item_dims.get("length") or {}).get("value", "")
        W = (item_dims.get("width")  or {}).get("value", "")
        H = (item_dims.get("height") or {}).get("value", "")
        if any((L, W, H)):
            dims_str = f"{L}x{W}x{H}"

    weight = _first(attrs.get("item_weight"))
    if isinstance(weight, dict):
        weight_val = weight.get("value", "")
    else:
        weight_val = weight

    bullets_raw = _first(attrs.get("bullet_point"))
    if isinstance(bullets_raw, list):
        bullets = "; ".join(str(x) for x in bullets_raw if x)
    else:
        bullets = str(bullets_raw or "")

    return {
        "title":                 summ.get("itemName", ""),
        "brand_amazon":          summ.get("brandName") or _first(attrs.get("brand")),
        "product_type":          prod_types[0].get("productType", "") if prod_types else "",
        "item_type_keyword":     _first(attrs.get("item_type_keyword")),
        "bullets":               bullets,
        "color":                 summ.get("color") or _first(attrs.get("color")),
        "size":                  summ.get("size")  or _first(attrs.get("size")),
        "dimensions_cm":         dims_str,
        "weight_g":              weight_val,
        "main_image":            (images[0].get("link") if images else ""),
        "image_count":           len(images),
        "browse_classification": cls0.get("displayName", ""),
        "sales_rank_top":        rank0.get("rank") if rank0 else "",
    }


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", help="Force a specific account's token (e.g. NEXLEV)")
    ap.add_argument("--brand",   help="Only pull ASINs of this brand")
    ap.add_argument("--limit",   type=int, help="First N ASINs (smoke test)")
    args = ap.parse_args()

    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()
    m["ASIN"] = m["ASIN"].astype(str).str.strip()
    m = m[m["ASIN"].str.match(r"^B[0-9A-Z]{9}$", na=False)]
    m = m[m["Brand"].astype(str).str.strip().str.lower() != "fossil"]
    if args.brand:
        m = m[m["Brand"].astype(str).str.strip().str.lower() == args.brand.lower()]
    if args.limit:
        m = m.head(args.limit)
    m = m.drop_duplicates(subset=["ASIN"])
    print(f"ASINs to probe: {len(m)}")

    # Pick account
    tok = None
    account_used = None
    for a in [args.account] if args.account else ACCOUNTS_PREFERENCE:
        if not a:
            continue
        tok = get_access_token(a)
        if tok:
            account_used = a
            print(f"Using account: {a}")
            break
    if not tok:
        print("No usable account token — check env vars.")
        return 2

    all_rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    role_missing = False
    errors = 0

    for i, (_, row) in enumerate(m.iterrows(), 1):
        asin = row["ASIN"]
        try:
            raw = fetch_catalog_item(tok, asin)
        except PermissionError as e:
            print(f"  Product Listing role NOT enabled on {account_used} (403) — aborting")
            print(f"    detail: {str(e)[:200]}")
            role_missing = True
            break
        parsed = parse_catalog(raw)
        if "__error__" in parsed:
            errors += 1
            if errors <= 5:
                print(f"  [{asin}] {parsed['__error__']}")
            continue
        parsed.update({
            "asin":         asin,
            "brand_master": str(row.get("Brand", "")).strip(),
            "model_master": str(row.get("Model", "")).strip(),
            "sku_master":   str(row.get("FBA SKU", "")).strip(),
            "fetched_at":   now,
        })
        all_rows.append(parsed)
        time.sleep(CALL_INTERVAL_SEC)
        if i % 50 == 0:
            print(f"    progress: {i}/{len(m)} ok={len(all_rows)} err={errors}")

    if role_missing:
        return 2
    if not all_rows:
        print("No catalog rows collected.")
        return 1

    out = pd.DataFrame(all_rows)
    # Column order (identifiers first, then content)
    cols = ["asin", "brand_master", "model_master", "sku_master",
            "title", "brand_amazon", "product_type", "item_type_keyword",
            "bullets", "color", "size", "dimensions_cm", "weight_g",
            "main_image", "image_count", "browse_classification",
            "sales_rank_top", "fetched_at"]
    out = out[[c for c in cols if c in out.columns]]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if OUT_CSV.exists():
        old = pd.read_csv(OUT_CSV)
        merged = pd.concat([old, out], ignore_index=True)
        merged = merged.sort_values("fetched_at").drop_duplicates(
            subset=["asin"], keep="last"
        )
        merged.to_csv(OUT_CSV, index=False)
        print(f"\nWrote {len(merged)} rows -> {OUT_CSV} (merged with prior)")
    else:
        out.to_csv(OUT_CSV, index=False)
        print(f"\nWrote {len(out)} rows -> {OUT_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
