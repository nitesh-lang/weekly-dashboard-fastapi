"""Daily 1P vendor sales — GET_VENDOR_SALES_REPORT.

Single-day window per pull. Rows shaped for /api/{brand}/sync-sales
so the brand's services module treats them the same as the manual
Vendor Central Excel upload used to work.

REVENUE BASIS
-------------
Two bases exist and they are NOT interchangeable:

  ordered  — `orderedRevenue` / `orderedUnits`, MANUFACTURING view.
             Retail value of customer orders. Amazon books a PO
             cancellation as a NEGATIVE amount on the day it cancels,
             not by reversing the original order date. On 03-08-2026
             B0GN97RFPR took a +₹4.79 L / +283 u order; on 05-08-2026
             Amazon cancelled it as -₹3.56 L / -232 u, which dragged
             the whole 05-08 dashboard day to -₹1.85 L.

  shipped  — `shippedRevenue` / `shippedUnits`, SOURCING view.
             Retail value of what actually shipped. Same scale as
             ordered revenue (both retail), but cancellations never
             appear because a cancelled PO simply never ships.

Default stays `ordered` so a routine daily pull keeps behaving exactly
as it always has and the ledger does not quietly acquire two bases.
Shipped is opt-in per call — ?basis=shipped on /api/{brand}/pull-sales,
or the "Revenue basis" picker on the Sync page — and was introduced to
repair 03-06 Aug 2026 without touching the rest of the history.
VENDOR_SALES_BASIS flips the deployment-wide default if the whole
ledger is ever rebased onto shipped.

NOTE ON `shippedCogs`: the MANUFACTURING view carries Shipped COGS,
which is WHOLESALE (what Amazon pays), not retail. It is used only as
a last-resort fallback and is logged loudly, because silently swapping
retail for wholesale would drop the brand's revenue by its vendor
margin with nothing on screen to explain it.
"""
from __future__ import annotations

import os
from datetime import date

from .sp_client import MARKETPLACE, SpApiError, get_access_token, submit_and_download
from ..brands import Brand, SpAccount

# "shipped" | "ordered"
DEFAULT_BASIS = (os.getenv("VENDOR_SALES_BASIS") or "ordered").strip().lower()

# Shipped revenue only exists in the SOURCING view; ordered revenue is
# present in both, and MANUFACTURING is what the account was pulled with
# historically, so keep it for the ordered basis.
_VIEW_FOR_BASIS = {"shipped": "SOURCING", "ordered": "MANUFACTURING"}


def _money(v) -> float:
    """Vendor money fields are {"amount": x, "currencyCode": "INR"}."""
    if isinstance(v, dict):
        return float(v.get("amount") or 0)
    return float(v or 0)


def _count(v) -> int:
    """Unit fields are usually a bare int, but Amazon has returned
    {"amount": n} for some report versions."""
    if isinstance(v, dict):
        return int(v.get("amount") or 0)
    return int(v or 0)


def _pull_vendor_day(acct: SpAccount, day: date, basis: str = DEFAULT_BASIS) -> list[dict]:
    """Shipped basis tries SOURCING first, then MANUFACTURING.

    Which view carries shipped money depends on the vendor agreement, and
    Audio Array's SOURCING response came back with real shippedUnits but a
    zeroed shippedRevenue. Rather than guess, ask SOURCING and — only if it
    yields no revenue — ask MANUFACTURING for shippedCogs. Costs a second
    Amazon report on that path, which is worth it against writing a day of
    zeros. Ordered basis is a single MANUFACTURING call as always.
    """
    if acct.kind != "vendor":
        return []
    basis = (basis or DEFAULT_BASIS).strip().lower()
    if basis not in _VIEW_FOR_BASIS:
        raise SpApiError(f"unknown vendor basis {basis!r} (expected 'shipped' or 'ordered')")
    if basis == "shipped":
        problems: list[str] = []
        for view in ("SOURCING", "MANUFACTURING"):
            try:
                return _pull_vendor_view(acct, day, basis="shipped", view=view)
            except SpApiError as e:
                problems.append(f"{view}: {e}")
        raise SpApiError("; ".join(problems))
    return _pull_vendor_view(acct, day, basis=basis, view=_VIEW_FOR_BASIS[basis])


def _pull_vendor_view(acct: SpAccount, day: date, basis: str, view: str) -> list[dict]:
    token = get_access_token(
        os.getenv(acct.lwa_client_id_env),
        os.getenv(acct.lwa_client_secret_env),
        os.getenv(acct.refresh_token_env),
    )
    body = {
        "reportType": "GET_VENDOR_SALES_REPORT",
        "marketplaceIds": [MARKETPLACE],
        # UTC Z-format matches Amazon's expected shape more strictly than +00:00
        "dataStartTime": f"{day.isoformat()}T00:00:00Z",
        "dataEndTime": f"{day.isoformat()}T23:59:59Z",
        "reportOptions": {
            "reportPeriod": "DAY",
            "distributorView": view,
            "sellingProgram": "RETAIL",
        },
    }
    # Vendor reports occasionally queue for 5-10 min under Amazon-side load,
    # so poll longer than the seller default.
    payload = submit_and_download(token, body, poll_seconds=5, poll_max=180)

    # Vendor JSON has two lists:
    #   salesByAsin      — per (asin, day) rows  ← THIS is what we want
    #   salesAggregate   — account-level daily totals, no asin key
    # Prefer the per-ASIN view; fall back only if empty.
    entries = payload.get("salesByAsin") or payload.get("salesAggregate") or []
    if not entries:
        # A genuinely empty day is not an error. Returning [] lets the
        # safe-replace guard in sp_pull.py leave existing rows alone, which is
        # what the ordered-basis code did too.
        return []

    # Which field carries revenue on this basis. Decided once from the whole
    # payload rather than per row: a single ASIN that happens to have no
    # shipments must not silently drop the account onto a different basis.
    if basis == "shipped":
        # Gate on VALUE, not on the key existing. Amazon returns the
        # shippedRevenue key for accounts that have no sourcing agreement,
        # populated with zeros, while shippedUnits carries real counts — on
        # 03-08-2026 that wrote 269 shipped units against ₹0 revenue and made
        # the day read ₹2.35 L. A day of zeros is worse than no day at all.
        rev_total = sum(_money((r or {}).get("shippedRevenue")) for r in entries)
        cogs_total = sum(_money((r or {}).get("shippedCogs")) for r in entries)
        if rev_total:
            rev_key, units_key = "shippedRevenue", "shippedUnits"
        elif cogs_total:
            rev_key, units_key = "shippedCogs", "shippedUnits"
            print(
                f"[vendor] {acct.label} {day}: no shippedRevenue value in the "
                f"{view} view — falling back to shippedCogs (WHOLESALE, not "
                f"retail). Numbers will sit below the ordered-revenue history."
            )
        else:
            # Raising leaves the account out of accounts_with_rows, so the
            # safe-replace guard in sp_pull.py keeps whatever is already
            # stored for the day instead of overwriting it with zeros.
            raise SpApiError(
                f"{acct.label}: no shipped revenue in the {view} view "
                f"({len(entries)} rows; shippedRevenue and shippedCogs both "
                f"total 0). Day left untouched."
            )
    else:
        rev_key, units_key = "orderedRevenue", "orderedUnits"

    out: list[dict] = []
    for r in entries:
        asin = str(r.get("asin") or "").strip()
        if not asin:
            continue
        out.append(
            {
                "ASIN": asin,
                # Key name states the basis so build_rows_from_df picks the
                # matching units column instead of guessing.
                rev_key: _money(r.get(rev_key)),
                units_key: _count(r.get(units_key)),
            }
        )
    return out


def pull_brand_vendor_sales(
    brand: Brand, day: date, basis: str = DEFAULT_BASIS,
    errors_out: list[str] | None = None,
) -> dict[str, list[dict]]:
    """`errors_out` collects per-account failures — see the note in
    seller_sales.pull_brand_seller_sales; without it, one account failing
    while another succeeds is silent."""
    per_account: dict[str, list[dict]] = {}
    errors: list[str] = []
    for acct in brand.sp_accounts:
        if acct.kind != "vendor":
            continue
        try:
            per_account[acct.label] = _pull_vendor_day(acct, day, basis=basis)
        except SpApiError as e:
            errors.append(f"{acct.label}: {e}")
            per_account[acct.label] = []
    if errors_out is not None:
        errors_out.extend(errors)
    if errors and not any(per_account.values()):
        raise SpApiError("; ".join(errors))
    return per_account
