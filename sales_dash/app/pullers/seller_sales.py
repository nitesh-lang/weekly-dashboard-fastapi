"""Daily 3P seller sales — GET_SALES_AND_TRAFFIC_REPORT.

Same report the existing weekly puller uses, but with
`dateGranularity=DAY, asinGranularity=PARENT` and a single-day window
so the resulting rows match the shape /api/{brand}/sync-sales expects.

Output rows (per parent ASIN, per seller account):
    { "parentASIN": "...", "orderedProductSales": <net>, "unitsOrdered": <int> }

The brand's `build_rows_from_df` in the ported services module then
filters against the planning ASINs, so a Nexlev ASIN sitting in an
Audio Array seller account naturally routes to Nexlev only.
"""
from __future__ import annotations

import os
from datetime import date

from .sp_client import MARKETPLACE, SpApiError, get_access_token, submit_and_download
from ..brands import Brand, SpAccount


def _pull_seller_day(acct: SpAccount, day: date) -> list[dict]:
    if acct.kind != "seller":
        return []
    token = get_access_token(
        os.getenv(acct.lwa_client_id_env),
        os.getenv(acct.lwa_client_secret_env),
        os.getenv(acct.refresh_token_env),
    )
    body = {
        "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
        "marketplaceIds": [MARKETPLACE],
        "dataStartTime": f"{day.isoformat()}T00:00:00+00:00",
        "dataEndTime": f"{day.isoformat()}T23:59:59+00:00",
        "reportOptions": {
            "asinGranularity": "PARENT",
            "dateGranularity": "DAY",
        },
    }
    payload = submit_and_download(token, body)

    out: list[dict] = []
    for r in payload.get("salesAndTrafficByAsin", []) or []:
        sa = r.get("salesByAsin") or {}
        parent = str(r.get("parentAsin") or r.get("childAsin") or "").strip()
        if not parent:
            continue
        gross = (sa.get("orderedProductSales") or {}).get("amount", 0) or 0
        units = sa.get("unitsOrdered", 0) or 0
        out.append(
            {
                "parentASIN": parent,
                "ASIN": parent,
                "orderedProductSales": float(gross),
                "unitsOrdered": int(units),
            }
        )
    return out


def pull_brand_seller_sales(
    brand: Brand, day: date, errors_out: list[str] | None = None
) -> dict[str, list[dict]]:
    """Return `{account_label: rows}` for every seller account attached
    to the brand. Skips vendor accounts.

    Pass `errors_out` to see PER-ACCOUNT failures. Without it a single
    account failing — Amazon answering 429 QuotaExceeded, most often — is
    invisible: the exception is only raised when EVERY account fails, so one
    dead account just quietly contributes zero rows and the day looks thin
    for no stated reason. That is exactly how 18-08-2026 slipped through.
    """
    per_account: dict[str, list[dict]] = {}
    errors: list[str] = []
    for acct in brand.sp_accounts:
        if acct.kind != "seller":
            continue
        try:
            per_account[acct.label] = _pull_seller_day(acct, day)
        except SpApiError as e:
            errors.append(f"{acct.label}: {e}")
            per_account[acct.label] = []
    if errors_out is not None:
        errors_out.extend(errors)
    if errors and not any(per_account.values()):
        # All seller accounts failed — surface upstream.
        raise SpApiError("; ".join(errors))
    return per_account
