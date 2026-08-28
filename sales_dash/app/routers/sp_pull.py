"""On-demand SP-API sales pull.

Frontend hits POST /api/{brand}/pull-sales?date=YYYY-MM-DD. The router
fetches every SP account attached to the brand (seller + vendor),
assembles a `SyncPayload`, and internally invokes the same /sync-sales
logic so numbers land in the ledger identically to a manual Excel
upload.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from .. import activity
from ..auth import require_user
from ..brands import get_brand
from ..database import engine
from ..ledger_io import canonical_account, day_totals, delete_day, save_ledger
from ..pullers.seller_sales import pull_brand_seller_sales
from ..pullers.sp_client import SpApiError
from ..pullers.vendor_sales import DEFAULT_BASIS, pull_brand_vendor_sales
from ..sync_runs import finish_run, last_ok_run, recent_runs, start_run

router = APIRouter()

# A re-pull carrying less than this fraction of what is already stored for an
# account/day is treated as an immature report, not as a real decline, and is
# refused. 0.5 is deliberately loose — it is meant to catch the 6%-of-a-day
# case, not to police ordinary day-to-day movement.
MATURITY_MIN_FRACTION = float(os.getenv("LEDGER_MATURITY_MIN_FRACTION") or 0.5)


@router.get("/{brand_key}/sync-runs")
def sync_runs_list(
    brand_key: str,
    limit: int = Query(50, ge=1, le=200),
    _user: str = Depends(require_user),
):
    brand = get_brand(brand_key)
    if brand is None:
        raise HTTPException(status_code=404, detail={"error": {"code": "unknown_brand"}})
    return {
        "brand": brand.key,
        "last_ok": last_ok_run(brand.key),
        "runs": recent_runs(brand.key, limit=limit),
    }


def _parse_day(s: str | None) -> date:
    if not s:
        # Default to yesterday — SP-API reports for today are usually incomplete
        return (datetime.utcnow() - timedelta(days=1)).date()
    d = pd.to_datetime(s, errors="coerce")
    if pd.isna(d):
        raise HTTPException(status_code=400, detail={"error": {"code": "bad_date"}})
    return d.date()


@router.post("/{brand_key}/pull-sales")
def pull_sales(
    brand_key: str,
    request: Request,
    day: str | None = Query(None, alias="date"),
    replace_day: bool = Query(True),
    basis: str = Query(
        DEFAULT_BASIS,
        description="Vendor revenue basis: 'shipped' (Shipped Revenue, SOURCING "
                    "view — default) or 'ordered' (Ordered Revenue, MANUFACTURING "
                    "view). Seller/3P accounts are unaffected.",
    ),
    force_replace: bool = Query(
        False,
        description="Bypass the maturity guard and overwrite a day even when the "
                    "incoming pull is far smaller than what is already stored. "
                    "Needed only when a day genuinely collapsed.",
    ),
    _user: str = Depends(require_user),
):
    brand = get_brand(brand_key)
    if brand is None:
        raise HTTPException(status_code=404, detail={"error": {"code": "unknown_brand"}})

    basis = (basis or DEFAULT_BASIS).strip().lower()
    if basis not in ("shipped", "ordered"):
        raise HTTPException(
            status_code=400,
            detail={"error": {"code": "bad_basis", "message": "basis must be 'shipped' or 'ordered'"}},
        )

    d = _parse_day(day)
    svc = brand.load_services()

    _email = _user.get("email") if isinstance(_user, dict) else str(_user)
    run_id = start_run(brand.key, "sp_api", pd.Timestamp(d), triggered_by=_email)
    activity.log(_email, "sync", page="sp-api-sync", brand=brand.key,
                 detail={"sales_date": str(d), "basis": basis, "run_id": run_id})

    errors: list[str] = []
    # errors_out catches PER-ACCOUNT failures. The pullers only raise when
    # every account fails, so without this one throttled account (429
    # QuotaExceeded) silently contributes zero rows and the day just looks
    # thin — no warning, nothing on the sync history to explain it.
    try:
        seller_rows = pull_brand_seller_sales(brand, d, errors_out=errors)
    except SpApiError as e:
        errors.append(f"seller: {e}")
        seller_rows = {}
    try:
        vendor_rows = pull_brand_vendor_sales(brand, d, basis=basis, errors_out=errors)
    except SpApiError as e:
        errors.append(f"vendor: {e}")
        vendor_rows = {}

    if not seller_rows and not vendor_rows:
        finish_run(
            run_id,
            ok=False,
            warnings=errors,
            error="; ".join(errors) or "no data",
        )
        raise HTTPException(
            status_code=502,
            detail={"error": {"code": "sp_api_failed", "message": "; ".join(errors) or "no data"}},
        )

    sales_date = pd.Timestamp(d)
    built_by_account: dict[str, pd.DataFrame] = {}
    rows_by_account: dict[str, int] = {}
    fetched_by_account: dict[str, int] = {}
    account_labels: list[str] = []

    # PLAN-AWARE INGEST — only ASINs present in the brand's planning file for
    # this month land in the ledger. Every account is still pulled (a planned
    # ASIN might sell via Cambium Retail even if rarely), but out-of-plan rows
    # are dropped before save. Same behaviour as the source apps pre-silent-drop-fix.
    plan_asins_ingest: set[str] = set()
    try:
        plan_df = svc.load_planning_main(sales_date)
        if not plan_df.empty and "asin" in plan_df.columns:
            plan_asins_ingest = set(plan_df["asin"].astype(str).str.strip())
    except Exception:
        pass

    # What plan-aware ingest THREW AWAY, per account. Without this the drop is
    # invisible: out_of_plan_asins below is computed from rows that already
    # survived the filter, so it is structurally always 0 and reported "0"
    # on 18-08-2026 while silently discarding Rs 1,85,135 of real Nexlev
    # revenue that had moved onto new variation-parent ASINs.
    dropped_by_account: dict[str, dict] = {}

    def _plan_filter(df: pd.DataFrame, account: str | None = None) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        # STRICT: if the brand's planning file for this sales_date is missing
        # or empty, drop everything. Silently ingesting the whole SP-API
        # response created cross-brand bloat on Aug 1 when Nexlev didn't
        # have its Aug 2026 plan yet. Better to refuse the ingest and force
        # the operator to add the plan file than to store noise.
        if not plan_asins_ingest:
            return df.iloc[0:0]
        keep = df["ASIN"].astype(str).isin(plan_asins_ingest)
        dropped = df[~keep]
        if account and not dropped.empty:
            rev = float(dropped["sales"].sum())
            if rev:
                worst = (dropped.groupby(dropped["ASIN"].astype(str))["sales"]
                         .sum().sort_values(ascending=False))
                dropped_by_account[account] = {
                    "asins": int(dropped["ASIN"].nunique()),
                    "sales": round(rev, 2),
                    "units": float(dropped["units"].sum()),
                    "top": [{"ASIN": a, "sales": round(float(s), 2)}
                            for a, s in worst.head(10).items()],
                }
        return df[keep]

    for label, rows in seller_rows.items():
        acct = canonical_account(label) or label
        account_labels.append(acct)
        fetched_by_account[acct] = len(rows)
        if not rows:
            rows_by_account[acct] = 0
            continue
        built = svc.build_rows_from_df(pd.DataFrame(rows), acct, sales_date, False)
        built = _plan_filter(built, acct)
        rows_by_account[acct] = 0 if built is None or built.empty else int(len(built))
        if built is not None and not built.empty:
            built_by_account[acct] = built

    for label, rows in vendor_rows.items():
        acct = canonical_account(label) or label
        account_labels.append(acct)
        fetched_by_account[acct] = len(rows)
        if not rows:
            rows_by_account[acct] = 0
            continue
        built = svc.build_rows_from_df(pd.DataFrame(rows), acct, sales_date, True)
        built = _plan_filter(built, acct)
        rows_by_account[acct] = 0 if built is None or built.empty else int(len(built))
        if built is not None and not built.empty:
            built_by_account[acct] = built

    # SAFE-REPLACE GUARD
    # Only delete-and-replace ROWS FOR ACCOUNTS that returned non-empty data
    # this call. If SP-API 500s / returns empty for an account, leave the
    # existing rows in place — an all-empty response never wipes good data.
    accounts_with_rows = [
        a for a, n in rows_by_account.items() if n > 0
    ]
    skipped_empty = [a for a, n in rows_by_account.items() if n == 0]

    with engine.begin() as conn:
        # MATURITY GUARD
        # The safe-replace guard above only catches a COMPLETELY empty
        # response. A partially-populated one walks straight through it, and
        # that is the more common failure: GET_SALES_AND_TRAFFIC_REPORT keeps
        # filling in for days after a date closes, so a day pulled too early
        # is real-looking but a fraction of the truth. 18-08-2026 was pulled
        # twice and stored at ~6% of its eventual value across four separate
        # seller accounts, and nothing flagged it.
        #
        # So: if an account already has rows for this day and the incoming
        # pull is a small fraction of them, keep what is stored and say so.
        # Growth is always allowed — a maturing report only ever goes up.
        stored = day_totals(brand.key, d, accounts_with_rows, conn=conn)
        guard_blocked: list[dict] = []
        if not force_replace:
            for acct in list(accounts_with_rows):
                prev = stored.get(acct)
                if not prev or prev["sales"] <= 0:
                    continue
                incoming = float(built_by_account[acct]["sales"].sum())
                if incoming >= prev["sales"] * MATURITY_MIN_FRACTION:
                    continue
                guard_blocked.append({
                    "account": acct,
                    "stored_sales": round(prev["sales"], 2),
                    "incoming_sales": round(incoming, 2),
                    "fraction": round(incoming / prev["sales"], 3),
                })
                # Drop it from BOTH the delete list and the save — save_ledger
                # upserts, so leaving the rows in would overwrite even with
                # replace_day=false.
                accounts_with_rows.remove(acct)
                built_by_account.pop(acct, None)
                rows_by_account[acct] = 0

        if guard_blocked:
            for g in guard_blocked:
                errors.append(
                    f"{g['account']}: incoming {g['incoming_sales']:,.0f} is "
                    f"{g['fraction']:.0%} of stored {g['stored_sales']:,.0f} — "
                    f"kept existing rows (report still maturing?). "
                    f"Re-run with force_replace=true to overwrite anyway."
                )

        if replace_day and accounts_with_rows:
            delete_day(brand.key, d, accounts_with_rows, conn=conn)
        combined = pd.DataFrame()
        out_of_plan_asins: list[str] = []
        if built_by_account:
            combined = pd.concat(built_by_account.values(), ignore_index=True)
            save_ledger(brand.key, combined, conn=conn)
            try:
                plan_for_day = svc.load_planning_main(sales_date)
                if not plan_for_day.empty:
                    allowed = set(plan_for_day["asin"].astype(str))
                    out_of_plan_asins = sorted(set(combined["ASIN"].astype(str)) - allowed)
            except Exception:
                pass

    # Discarded revenue is a warning, not a footnote — a planning file that has
    # fallen behind the catalogue (new ASINs, re-parented variations) looks
    # exactly like a bad sales day on the dashboard.
    for acct, drop in sorted(dropped_by_account.items(), key=lambda kv: -kv[1]["sales"]):
        top = ", ".join(f"{t['ASIN']} {t['sales']:,.0f}" for t in drop["top"][:3])
        errors.append(
            f"{acct}: dropped {drop['sales']:,.0f} of sales on {drop['asins']} ASIN(s) "
            f"NOT in this month's planning file ({top}). Add them to the plan and "
            f"re-pull if this revenue belongs to {brand.key}."
        )

    total_rows = int(sum(rows_by_account.values()))
    finish_run(
        run_id,
        ok=True,
        total_rows=total_rows,
        out_of_plan_count=len(out_of_plan_asins),
        fetched_by_account=fetched_by_account,
        rows_by_account=rows_by_account,
        warnings=errors,
    )
    return {
        "ok": True,
        "brand": brand.key,
        "sales_date": d.isoformat(),
        "replace_day": replace_day,
        "vendor_basis": basis,
        "fetched_by_account": fetched_by_account,
        "rows_by_account": rows_by_account,
        "skipped_empty_accounts": skipped_empty,
        "guard_blocked_accounts": guard_blocked,
        "dropped_out_of_plan": dropped_by_account,
        "total_rows": total_rows,
        "out_of_plan_count": len(out_of_plan_asins),
        "out_of_plan_asins": out_of_plan_asins,
        "warnings": errors,
        "run_id": run_id,
    }
