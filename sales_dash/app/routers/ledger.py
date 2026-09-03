"""Ledger ingest — Excel upload (fallback) and JSON sync (SP-API pullers).

Both paths route through the brand's `build_rows_from_df` / `build_rows`
in the ported services module, so numbers match whatever the source
dashboard produced.
"""
from __future__ import annotations

import io
import os
import secrets
from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel

from .. import activity
from ..auth import require_user
from ..brands import get_brand
from ..database import engine
from ..ledger_io import delete_day, load_ledger, save_ledger, canonical_account

router = APIRouter()

ADMIN_UPLOAD_KEY = (os.getenv("ADMIN_UPLOAD_KEY") or "").strip() or None
SYNC_SALES_TOKEN = os.getenv("SYNC_SALES_TOKEN")


# ─── Excel upload (unchanged behavior; kept as fallback) ──
@router.post("/{brand_key}/upload")
async def upload(
    brand_key: str,
    request: Request,
    upload_key: str = Form(...),
    sales_date: str = Form(...),
    replace_day: Optional[str] = Form(None),
    files: list[UploadFile] = File(default_factory=list),
    accounts: str = Form(""),
    is_vendor_flags: str = Form(""),
    _user: str = Depends(require_user),
):
    brand = get_brand(brand_key)
    if brand is None:
        raise HTTPException(status_code=404, detail={"error": {"code": "unknown_brand"}})
    if not ADMIN_UPLOAD_KEY:
        return {"ok": False, "error": "Admin key missing on server."}
    if upload_key.strip() != ADMIN_UPLOAD_KEY:
        return {"ok": False, "error": "Invalid admin upload key."}

    d = pd.to_datetime(sales_date, errors="coerce")
    if pd.isna(d):
        return {"ok": False, "error": "Invalid sales date."}

    account_list = [a.strip() for a in accounts.split("|") if a.strip()]
    vendor_list = [f.strip().lower() in ("1", "true", "yes") for f in is_vendor_flags.split("|")]

    if len(files) != len(account_list) or len(files) != len(vendor_list):
        return {"ok": False, "error": "files/accounts/is_vendor_flags length mismatch"}

    svc = brand.load_services()
    all_rows: list[pd.DataFrame] = []
    for file, acct, is_vendor in zip(files, account_list, vendor_list):
        if not file or not file.filename:
            continue
        try:
            rows = svc.build_rows(file=file.file, account=acct, sales_date=d, is_vendor=is_vendor)
        except Exception as e:
            return {"ok": False, "error": f"{acct}: {e}"}
        if rows is not None and not rows.empty:
            all_rows.append(rows)

    if not all_rows:
        return {"ok": False, "error": "No valid sales rows found."}

    final_df = pd.concat(all_rows, ignore_index=True)

    with engine.begin() as conn:
        if (replace_day or "").lower() in ("1", "true", "on", "yes"):
            delete_day(brand.key, d.date(), account_list, conn=conn)
        save_ledger(brand.key, final_df, conn=conn)

    msg = "Sales uploaded successfully."
    try:
        plan_for_day = svc.load_planning_main(d)
        if not plan_for_day.empty:
            allowed = set(plan_for_day["asin"].astype(str))
            oop = int(final_df.loc[~final_df["ASIN"].astype(str).isin(allowed), "ASIN"].nunique())
            if oop:
                msg += f" {oop} ASIN(s) had no target this month — kept & shown as 'No Plan'."
    except Exception:
        pass

    return {"ok": True, "message": msg, "rows": int(len(final_df))}


# ─── JSON sync (called by SP-API pullers) ──
class SyncAccountBlock(BaseModel):
    account: str
    is_vendor: bool = False
    rows: list[dict]


class SyncPayload(BaseModel):
    sales_date: str
    replace_day: bool = True
    accounts: list[SyncAccountBlock]


@router.post("/{brand_key}/sync-sales")
async def sync_sales(brand_key: str, payload: SyncPayload, request: Request):
    if not SYNC_SALES_TOKEN:
        raise HTTPException(status_code=503, detail={"error": {"code": "sync_disabled"}})
    token = request.headers.get("x-sync-token", "")
    if not token or not secrets.compare_digest(token, SYNC_SALES_TOKEN):
        raise HTTPException(status_code=401, detail={"error": {"code": "unauthorized"}})

    brand = get_brand(brand_key)
    if brand is None:
        raise HTTPException(status_code=404, detail={"error": {"code": "unknown_brand"}})
    svc = brand.load_services()

    d = pd.to_datetime(payload.sales_date, errors="coerce")
    if pd.isna(d):
        raise HTTPException(status_code=400, detail={"error": {"code": "bad_date"}})

    all_rows: list[pd.DataFrame] = []
    rows_by_account: dict[str, int] = {}
    for block in payload.accounts:
        acct = canonical_account(block.account) or block.account
        if not acct:
            continue
        built = svc.build_rows_from_df(pd.DataFrame(block.rows), acct, d, block.is_vendor)
        rows_by_account[acct] = 0 if built is None or built.empty else int(len(built))
        if built is not None and not built.empty:
            all_rows.append(built)

    with engine.begin() as conn:
        if payload.replace_day:
            delete_day(brand.key, d.date(), [b.account for b in payload.accounts], conn=conn)
        combined = pd.DataFrame()
        out_of_plan_asins: list[str] = []
        if all_rows:
            combined = pd.concat(all_rows, ignore_index=True)
            save_ledger(brand.key, combined, conn=conn)
            try:
                plan_for_day = svc.load_planning_main(d)
                if not plan_for_day.empty:
                    allowed = set(plan_for_day["asin"].astype(str))
                    out_of_plan_asins = sorted(set(combined["ASIN"].astype(str)) - allowed)
            except Exception:
                pass

    return {
        "ok": True,
        "brand": brand.key,
        "sales_date": str(d.date()),
        "replace_day": payload.replace_day,
        "rows_by_account": rows_by_account,
        "total_rows": int(sum(rows_by_account.values())),
        "out_of_plan_count": len(out_of_plan_asins),
        "out_of_plan_asins": out_of_plan_asins,
    }


# ─── CSV download ──
@router.get("/{brand_key}/download-ledger.csv")
def download_ledger(
    brand_key: str,
    month: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    plan_scope: bool = True,
    _user: str = Depends(require_user),
):
    brand = get_brand(brand_key)
    if brand is None:
        raise HTTPException(status_code=404, detail={"error": {"code": "unknown_brand"}})

    activity.log(
        _user.get("email") if isinstance(_user, dict) else str(_user),
        "export",
        page="ledger-csv",
        brand=brand.key,
        detail={"month": month, "from": from_date, "to": to_date,
                "plan_scope": plan_scope},
    )

    svc = brand.load_services()

    ledger = load_ledger(brand.key)
    if ledger.empty:
        return Response(content="date,account,ASIN,sales,net_sales,units\n", media_type="text/csv")

    if from_date and to_date:
        f = pd.to_datetime(from_date, errors="coerce")
        t = pd.to_datetime(to_date, errors="coerce")
        if pd.notna(f) and pd.notna(t):
            ledger = svc.filter_by_date_range(ledger, f, t)
    elif month:
        start = pd.to_datetime(month, format="%b %Y")
        end = (start + pd.offsets.MonthEnd(1)).normalize()
        ledger = svc.filter_by_date_range(ledger, start, end)

    # Plan-scope — only rows whose ASIN is in the brand's planning file for
    # the covered period. Matches dashboard behaviour + kills the confusion
    # of seeing rows from shared seller accounts (Cambium Retail / Audio
    # Array) that happened to land in this brand's ledger under the old
    # silent-drop-fix ingest.
    if plan_scope and not ledger.empty:
        try:
            months = sorted(ledger["date"].dropna().dt.to_period("M").unique())
            plan_asins: set[str] = set()
            for m in months:
                p = svc.load_planning_main(m.to_timestamp())
                if not p.empty and "asin" in p.columns:
                    plan_asins.update(p["asin"].astype(str).str.strip())
            if plan_asins:
                ledger = ledger[ledger["ASIN"].astype(str).isin(plan_asins)]
        except Exception:
            # If planning files can't be read, keep the raw ledger — never
            # return an empty CSV silently.
            pass

    # Brand column (operator request 2026-09-03): the product's brand from
    # sku_master, per ASIN — enriched at export time so it covers all
    # historical rows without a migration or re-ingest.
    if not ledger.empty and "ASIN" in ledger.columns:
        from .. import sku_master
        lut = sku_master.lookup_map()
        ledger = ledger.copy()
        ledger["brand"] = (
            ledger["ASIN"].astype(str).str.strip().str.upper()
            .map(lambda a: (lut.get(a) or {}).get("brand", ""))
        )

    csv_bytes = ledger.to_csv(index=False).encode("utf-8")
    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"content-disposition": f'attachment; filename="{brand.key}-ledger.csv"'},
    )
