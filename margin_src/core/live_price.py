"""Live Amazon price + fee lookup for the margin calculator.

Ported from the AM-Ops tool's pricing.py (D:\\Nitesh\\Audio Array Project\\
fossil-am-tool), which is the only implementation known to work against
Amazon IN today:

  * The **v0 pricing APIs are dead** (getPricing / getItemOffers / batch all
    403 since ~2026-08-24 — that is what turned the weekly price_snapshot
    all-NULL). The live path is the **Product Pricing 2022-05-01
    competitiveSummary batch**.
  * Only the **NEXLEV** LWA app carries the Pricing role. competitiveSummary
    is ASIN-scoped public data, so that one token prices every brand's ASINs
    (override with PRICING_ACCOUNT).
  * Buybox = the featured offer with the highest summed
    glanceViewWeightPercentage — NOT the lowest price. Picking min(price)
    named the wrong seller whenever two sellers sat at the same price.

Fees come from getMyFeesEstimates at the live price, split into the pieces
the calculator actually edits: referral, fulfilment (FBA), closing.

Kept deliberately small: one ASIN per call (the calculator is a per-SKU
tool), a short TTL cache so repeated clicks don't burn the ~0.1 r/s
competitiveSummary budget, and hard timeouts — this runs inside the shared
512MB web instance (CLAUDE.md boundary #12).
"""
from __future__ import annotations

import os
import threading
import time

import requests

LWA_URL = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = os.environ.get("SP_API_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com")
MARKETPLACE_ID = os.environ.get("SP_MARKETPLACE_ID", "A21TJRUUN4KGV")
PRICING_ACCOUNT = os.environ.get("PRICING_ACCOUNT", "NEXLEV")

CB_SELLER_ID = "A1WYWER0W24N8S"                       # Cocoblu Retail (1P)
OUR_SELLER_IDS = {"A28KGDTB760OU9", "A15YBOSC0EMOS0"}  # AA 3P, VIOMI 3P

_CACHE_TTL = 600  # seconds
_cache: dict[str, tuple[float, dict]] = {}
_token_cache: tuple[float, str] | None = None
_lock = threading.Lock()


class LivePriceError(RuntimeError):
    """Anything the operator should see verbatim in the UI."""


def _token() -> str:
    """LWA access token for the pricing account (cached ~50 min)."""
    global _token_cache
    with _lock:
        if _token_cache and time.time() < _token_cache[0]:
            return _token_cache[1]
    acct = PRICING_ACCOUNT
    cid = os.environ.get(f"SP_LWA_CLIENT_ID_{acct}") or os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get(f"SP_LWA_CLIENT_SECRET_{acct}") or os.environ.get("SP_LWA_CLIENT_SECRET")
    rt = os.environ.get(f"SP_REFRESH_TOKEN_{acct}")
    if not (cid and sec and rt):
        raise LivePriceError(
            f"SP-API credentials for {acct} are not set on this server "
            "(needs SP_LWA_CLIENT_ID / SP_LWA_CLIENT_SECRET / "
            f"SP_REFRESH_TOKEN_{acct}).")
    try:
        r = requests.post(LWA_URL, timeout=30, data={
            "grant_type": "refresh_token", "refresh_token": rt,
            "client_id": cid, "client_secret": sec})
        r.raise_for_status()
        tok = r.json()["access_token"]
    except requests.RequestException as e:
        raise LivePriceError(f"Amazon sign-in failed: {e}") from e
    with _lock:
        _token_cache = (time.time() + 50 * 60, tok)
    return tok


def _price_of(offer: dict):
    lp = (offer.get("listingPrice") or {}).get("amount")
    ship = 0.0
    for s in offer.get("shippingOptions") or []:
        if (s.get("shippingOptionType") or "DEFAULT") == "DEFAULT":
            ship = (s.get("price") or {}).get("amount") or 0.0
    return None if lp is None else float(lp) + float(ship)


def _extract(body: dict) -> dict:
    """Featured (buybox) + our + CB prices out of a competitiveSummary body."""
    weights: dict[str, float] = {}
    prices: dict[str, float] = {}
    for fbo in body.get("featuredBuyingOptions") or []:
        for off in fbo.get("segmentedFeaturedOffers") or []:
            sid = off.get("sellerId", "")
            p = _price_of(off)
            if p is None or not sid:
                continue
            w = 0.0
            for seg in off.get("featuredOfferSegments") or []:
                w += float((seg.get("segmentDetails") or {})
                           .get("glanceViewWeightPercentage") or 0.0)
            weights[sid] = weights.get(sid, 0.0) + w
            if sid not in prices or p < prices[sid]:
                prices[sid] = p
    bb = None
    bb_seller = ""
    if weights:
        bb_seller = max(weights, key=lambda s: (weights[s], -prices.get(s, 0.0)))
        bb = prices.get(bb_seller)

    our = cb = None
    offers = []
    for lpo in body.get("lowestPricedOffers") or []:
        offers.extend(lpo.get("offers") or [])
    for off in offers:
        sid = off.get("sellerId", "")
        p = _price_of(off)
        if p is None:
            continue
        if sid in OUR_SELLER_IDS and (our is None or p < our):
            our = p
        elif sid == CB_SELLER_ID and (cb is None or p < cb):
            cb = p
    if bb is not None:
        if bb_seller in OUR_SELLER_IDS and (our is None or bb < our):
            our = bb
        if bb_seller == CB_SELLER_ID and (cb is None or bb < cb):
            cb = bb
    return {
        "buybox_price": bb,
        "buybox_seller": bb_seller,
        "buybox_is_ours": bb_seller in OUR_SELLER_IDS,
        "buybox_is_cb": bb_seller == CB_SELLER_ID,
        "our_price": our,
        "cb_price": cb,
    }


def _summary(token: str, asin: str) -> dict:
    h = {"x-amz-access-token": token, "Content-Type": "application/json"}
    body = {"requests": [{
        "asin": asin, "marketplaceId": MARKETPLACE_ID,
        "includedData": ["featuredBuyingOptions", "lowestPricedOffers"],
        "method": "GET",
        "uri": "/products/pricing/2022-05-01/items/competitiveSummary"}]}
    url = f"{SPAPI_HOST}/batches/products/pricing/2022-05-01/items/competitiveSummary"
    for attempt in range(3):
        r = requests.post(url, headers=h, json=body, timeout=45)
        if r.status_code == 429:
            time.sleep(4 * (attempt + 1))
            continue
        if r.status_code >= 400:
            raise LivePriceError(f"Amazon pricing API {r.status_code}: {r.text[:180]}")
        for resp in r.json().get("responses", []):
            b = resp.get("body") or {}
            if b.get("asin"):
                return b
        return {}
    raise LivePriceError("Amazon pricing API is throttling — try again in a minute.")


# Amazon's fee names vary by category; map them onto the calculator's fields.
_REFERRAL = {"referralfee"}
_FULFIL = {"fbafees", "fulfillmentfees", "fbafulfillmentfee", "fbaweightbasedfee",
           "fbaperunitfulfillmentfee"}
_CLOSING = {"variableclosingfee", "closingfee", "perltemfee", "peritemfee"}


def _fees(token: str, asin: str, price: float) -> dict:
    """Referral / fulfilment / closing at `price`, plus the total."""
    h = {"x-amz-access-token": token, "Content-Type": "application/json"}
    body = [{
        "FeesEstimateRequest": {
            "MarketplaceId": MARKETPLACE_ID,
            "PriceToEstimateFees": {
                "ListingPrice": {"CurrencyCode": "INR", "Amount": price}},
            "Identifier": f"margin-{asin}",
            "IsAmazonFulfilled": True,
        },
        "IdType": "ASIN", "IdValue": asin}]
    for attempt in range(3):
        r = requests.post(f"{SPAPI_HOST}/products/fees/v0/feesEstimate",
                          headers=h, json=body, timeout=45)
        if r.status_code == 429:
            time.sleep(3 * (attempt + 1))
            continue
        if r.status_code >= 400:
            raise LivePriceError(f"Amazon fees API {r.status_code}: {r.text[:180]}")
        break
    else:
        raise LivePriceError("Amazon fees API is throttling — try again in a minute.")

    out = {"referral_fee": None, "fulfilment_fee": None,
           "closing_fee": None, "total_fees": None, "fee_error": None}
    for res in r.json():
        est = res.get("FeesEstimate") or {}
        err = res.get("Error") or {}
        if err and not est:
            out["fee_error"] = str(err.get("Message") or err)[:200]
            continue
        out["total_fees"] = (est.get("TotalFeesEstimate") or {}).get("Amount")
        for d in est.get("FeeDetailList") or []:
            kind = str(d.get("FeeType") or "").replace(" ", "").lower()
            amt = (d.get("FeeAmount") or {}).get("Amount")
            if amt is None:
                continue
            if kind in _REFERRAL:
                out["referral_fee"] = (out["referral_fee"] or 0) + float(amt)
            elif kind in _FULFIL:
                out["fulfilment_fee"] = (out["fulfilment_fee"] or 0) + float(amt)
            elif kind in _CLOSING:
                out["closing_fee"] = (out["closing_fee"] or 0) + float(amt)
    return out


def lookup(asin: str, price_override: float | None = None) -> dict:
    """Live price + fee breakdown for one ASIN. Cached for 10 minutes."""
    asin = (asin or "").strip().upper()
    if len(asin) != 10:
        raise LivePriceError(f"{asin or '(blank)'} is not a valid ASIN.")
    ck = f"{asin}:{price_override or ''}"
    with _lock:
        hit = _cache.get(ck)
        if hit and time.time() - hit[0] < _CACHE_TTL:
            return {**hit[1], "cached": True}

    tok = _token()
    body = _summary(tok, asin)
    data = _extract(body) if body else {
        "buybox_price": None, "buybox_seller": "", "buybox_is_ours": False,
        "buybox_is_cb": False, "our_price": None, "cb_price": None}

    # Fee basis: the price the operator will actually sell at — buybox first,
    # then our own offer, then CB's.
    basis = price_override or data["buybox_price"] or data["our_price"] or data["cb_price"]
    fees = {"referral_fee": None, "fulfilment_fee": None, "closing_fee": None,
            "total_fees": None, "fee_error": None}
    if basis:
        fees = _fees(tok, asin, float(basis))

    result = {
        "asin": asin, **data, **fees,
        "fee_basis_price": float(basis) if basis else None,
        "referral_pct": (round(100 * fees["referral_fee"] / float(basis), 2)
                         if fees.get("referral_fee") and basis else None),
        "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "account": PRICING_ACCOUNT,
        "cached": False,
    }
    with _lock:
        _cache[ck] = (time.time(), result)
    return result
