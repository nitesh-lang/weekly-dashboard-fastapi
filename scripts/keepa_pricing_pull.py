"""Keepa pricing pull — per-account 3P + Amazon 1P selling prices for
every non-Fossil ASIN in sku_master.

Uses the Keepa `/product` endpoint (paid plan; key in env as
KEEPA_API_KEY).  Output is the same wide CSV schema the
sp_pricing_pull.py script writes — backend and frontend already
expect it — plus one new column, `amazon_1p_price`, which Keepa
makes trivial to surface.

Why Keepa instead of SP-API: SP-API Pricing requires the Pricing
role to be present on each seller's *consented* refresh token; the
operator's existing AdPilot tokens were issued before that role was
added, so every Pricing call 403s.  Keepa side-steps the consent
dance — single API key, licensed Amazon data, ~3 tokens per ASIN.

Token cost (Power plan reference):
  ~3 tokens / ASIN with offers=20 + stats=1
  525 non-Fossil ASINs  →  ~1,600 tokens  (well inside any paid plan)

CLI:
  python scripts/keepa_pricing_pull.py             # full pull
  python scripts/keepa_pricing_pull.py --limit 25  # quick smoke test
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


# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).resolve().parent.parent
SKU_MASTER  = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
OUT_CSV     = REPO_ROOT / "data" / "processed" / "price_snapshot.csv"
SELLER_IDS  = REPO_ROOT / "data" / "master" / "our_seller_ids.json"

KEEPA_URL   = "https://api.keepa.com/product"
DOMAIN_IN   = 10  # amazon.in marketplace

# Operator rule: Fossil excluded everywhere on the dashboard.
EXCLUDED_BRANDS = {"fossil"}

# Brand → CSV column.  Tonor listings sit under the VIOMI seller
# account in our SP-API setup, so the operator-facing column for
# Tonor ASINs is `price_viomi`.  Mirrors pricing.py's ACCOUNT_COLUMNS.
BRAND_TO_COL: dict[str, str] = {
    "audio array":     "price_audioarray",
    "nexlev":          "price_nexlev",
    "tonor":           "price_viomi",
    "white mulberry":  "price_whitemulberry",
}

# All possible per-account columns (ensures empty cols are still written).
ALL_ACCOUNT_COLS = sorted(set(BRAND_TO_COL.values()))

# Keepa csv-index constants — see https://discord.com/channels/.../keepa
# We only need the AMAZON 1P and BUY_BOX rows for `stats.current[]`.
CSV_AMAZON          = 0   # Amazon's own offer price (1P)
CSV_NEW_3P_LOWEST   = 1   # Lowest 3P New offer
CSV_BUY_BOX_LANDED  = 18  # Buy-box landed price (incl. shipping)


# ─────────────────────────────────────────────────────────────────────
# sku_master loader (same shape as sp_pricing_pull.load_master)
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
    out = out[~out["brand"].str.lower().isin(EXCLUDED_BRANDS)]
    out = out.drop_duplicates(subset=["asin"]).reset_index(drop=True)
    if limit:
        out = out.head(limit)
    return out


def load_our_seller_ids() -> dict[str, str]:
    """Optional mapping {ACCOUNT_NAME: sellerId}.  Used to (a) tag the
    buy-box-belongs-to-us flag and (b) pick the precise OUR offer
    from Keepa's offers[] array instead of falling back to lowest-3P."""
    if not SELLER_IDS.exists():
        return {}
    try:
        with open(SELLER_IDS, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k.strip(): str(v).strip() for k, v in data.items() if v}
    except Exception as e:
        print(f"  WARN: couldn't read {SELLER_IDS.name}: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────
# Keepa call
# ─────────────────────────────────────────────────────────────────────
def keepa_batch(api_key: str, asins: list[str]) -> list[dict]:
    """Call Keepa /product for up to 100 ASINs in one shot.  Returns
    the `products` list (may be shorter than `asins` if Keepa has no
    record for one)."""
    params = {
        "key":     api_key,
        "domain":  DOMAIN_IN,
        "asin":    ",".join(asins),
        "offers":  20,    # request up to 20 current offers
        "stats":   1,     # include current/avg stats summary
        "update":  0,     # serve cached if fresh; saves tokens
        "history": 0,     # skip full price history; we only need current
    }
    try:
        r = requests.get(KEEPA_URL, params=params, timeout=120)
    except requests.RequestException as e:
        print(f"  ERROR: HTTP error: {e}")
        return []
    if r.status_code == 429:
        # Out of tokens — wait the refillIn hint if present, else 30s.
        try:
            wait_ms = int(r.json().get("refillIn", 30_000))
        except Exception:
            wait_ms = 30_000
        print(f"  Throttled by Keepa; sleeping {wait_ms//1000}s…")
        time.sleep(max(5, wait_ms // 1000) + 1)
        try:
            r = requests.get(KEEPA_URL, params=params, timeout=120)
        except requests.RequestException as e:
            print(f"  ERROR: retry HTTP error: {e}")
            return []
    if r.status_code != 200:
        print(f"  ERROR: Keepa HTTP {r.status_code}: {r.text[:200]}")
        return []
    j = r.json()
    if "error" in j:
        print(f"  ERROR: Keepa error: {j['error']}")
        return []
    return j.get("products", []) or []


# ─────────────────────────────────────────────────────────────────────
# Parsers
# ─────────────────────────────────────────────────────────────────────
def _paise_to_rupees(v) -> float | None:
    """Keepa prices are INR paise (smallest unit) as ints.  -1 = no data."""
    if v is None:
        return None
    try:
        iv = int(v)
    except (TypeError, ValueError):
        return None
    if iv < 0:
        return None
    return round(iv / 100.0, 2)


def _stats_current(product: dict, idx: int) -> float | None:
    stats = product.get("stats") or {}
    cur = stats.get("current") or []
    if idx >= len(cur):
        return None
    return _paise_to_rupees(cur[idx])


def _our_offer_price(product: dict, our_seller_ids: set[str]) -> float | None:
    """Walk offers[] looking for an offer whose sellerId is in our
    known seller IDs.  Picks the smallest current listing price among
    matches (an account can rarely have multiple SKUs on one ASIN)."""
    if not our_seller_ids:
        return None
    matches: list[float] = []
    for off in (product.get("offers") or []):
        sid = (off.get("sellerId") or "").strip()
        if sid not in our_seller_ids:
            continue
        # offerCSV: alternating triples (date, price, shipping).  Last
        # triple is most recent; we use the listing price (not landed)
        # to match how the operator thinks of "their SP".
        csv = off.get("offerCSV") or []
        if len(csv) < 3:
            continue
        last_price = csv[-2]  # price portion of last triple
        rupees = _paise_to_rupees(last_price)
        if rupees is not None:
            matches.append(rupees)
    if not matches:
        return None
    return min(matches)


def _buy_box(product: dict, our_seller_ids: set[str]) -> dict:
    stats = product.get("stats") or {}
    bb_price = _paise_to_rupees(stats.get("buyBoxPrice"))
    if bb_price is None:
        bb_price = _stats_current(product, CSV_BUY_BOX_LANDED)
    bb_winner_id = None
    hist = stats.get("buyBoxSellerIdHistory") or []
    # buyBoxSellerIdHistory is a flat list of seller-ids ordered by time;
    # the last entry is the current winner.
    if hist:
        bb_winner_id = str(hist[-1]).strip() or None
    belongs = bool(our_seller_ids and bb_winner_id in our_seller_ids)
    return {
        "buybox_price":         bb_price,
        "buybox_seller_id":     bb_winner_id,
        "buybox_belongs_to_us": belongs,
    }


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────
def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap the number of ASINs (smoke testing)")
    args = ap.parse_args()

    api_key = os.environ.get("KEEPA_API_KEY")
    if not api_key:
        print("ERROR: KEEPA_API_KEY missing from environment (.env)")
        return 1

    master = load_master(limit=args.limit)
    if master.empty:
        print("ERROR: master loaded zero ASINs (after excluding Fossil).")
        return 1
    asins = master["asin"].tolist()
    print(f"Keepa pricing pull: {len(asins)} ASINs (domain=amazon.in).")

    our_seller_ids = load_our_seller_ids()
    if our_seller_ids:
        print(f"  Known seller IDs loaded for {len(our_seller_ids)} accounts.")
    else:
        print(f"  No {SELLER_IDS.name} found — per-account columns will "
              "fall back to lowest 3P offer for that ASIN's brand.")

    # Batch into chunks of 100 — Keepa's per-call ASIN cap.
    per_asin: dict[str, dict] = {}
    chunks = [asins[i:i + 100] for i in range(0, len(asins), 100)]
    for idx, chunk in enumerate(chunks):
        print(f"  Batch {idx + 1}/{len(chunks)}: {len(chunk)} ASINs…")
        products = keepa_batch(api_key, chunk)
        # Index by ASIN so order doesn't matter.
        for prod in products:
            asin = (prod.get("asin") or "").strip()
            if asin:
                per_asin[asin] = prod
        # Keepa's response carries `tokensLeft`; surface it for visibility.
        # (Not strictly needed — useful when troubleshooting quotas.)
        time.sleep(1.0)  # gentle pacing; well below any plan's rate limit

    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    our_seller_ids_set = set(our_seller_ids.values())

    rows = []
    for _, m in master.iterrows():
        asin     = m["asin"]
        brand    = m["brand"]
        brand_lc = brand.strip().lower()
        row: dict = {
            "asin":  asin,
            "sku":   m["sku"],
            "brand": brand,
            "model": m["model"],
        }
        # Empty per-account columns by default — only the row's own
        # brand-mapped slot gets populated.
        for c in ALL_ACCOUNT_COLS:
            row[c] = None

        prod = per_asin.get(asin)
        if prod is None:
            # Keepa had no record — still emit the row with NaNs so the
            # frontend can show the ASIN as "not found".
            row["amazon_1p_price"]      = None
            row["buybox_price"]         = None
            row["buybox_seller_id"]     = None
            row["buybox_belongs_to_us"] = False
            row["currency"]   = "INR"
            row["fetched_at"] = fetched_at
            rows.append(row)
            continue

        # 1P Amazon price
        row["amazon_1p_price"] = _stats_current(prod, CSV_AMAZON)

        # 3P per account — prefer OUR offer when we know our seller IDs,
        # else fall back to the lowest 3P for the ASIN's brand slot.
        target_col = BRAND_TO_COL.get(brand_lc)
        if target_col:
            ours = _our_offer_price(prod, our_seller_ids_set)
            fallback = _stats_current(prod, CSV_NEW_3P_LOWEST)
            row[target_col] = ours if ours is not None else fallback

        # Buy box
        row.update(_buy_box(prod, our_seller_ids_set))
        row["currency"]   = "INR"
        row["fetched_at"] = fetched_at
        rows.append(row)

    df = pd.DataFrame(rows)
    # Stable column order — matches what pricing.py + the React table expect,
    # with amazon_1p_price inserted right after the per-account block.
    ordered = ["asin", "sku", "brand", "model",
               *ALL_ACCOUNT_COLS,
               "amazon_1p_price",
               "buybox_price", "buybox_seller_id", "buybox_belongs_to_us",
               "currency", "fetched_at"]
    df = df[[c for c in ordered if c in df.columns]]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    print(f"OK wrote {OUT_CSV.relative_to(REPO_ROOT)} ({len(df)} rows)")

    # Coverage report
    print()
    print("  Coverage:")
    for c in ALL_ACCOUNT_COLS:
        print(f"    {c:<24}  {df[c].notna().sum():>5} / {len(df)}")
    print(f"    {'amazon_1p_price':<24}  {df['amazon_1p_price'].notna().sum():>5} / {len(df)}")
    print(f"    {'buybox_price':<24}  {df['buybox_price'].notna().sum():>5} / {len(df)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
