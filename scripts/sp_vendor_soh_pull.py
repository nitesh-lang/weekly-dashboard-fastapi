"""
SP-API Vendor SOH Pull (1P / Vendor Central) — Weekly Pipeline
==============================================================

Pulls sellable on-hand inventory per ASIN from the Vendor Inventory Report,
splits by brand via sku_master.xlsx, and writes per-brand xlsx files into the
current week's inventory folder.

Accounts:
    AUDIOARRAY     -> Cambium Retail Pvt Ltd (Mumbai - cb) vendor account.
                     Sells both Audio Array + Tonor brands; split via sku_master.
                     Feeds CB Replenishment in AM_Replenishment.
    WHITEMULBERRY  -> Clicktech vendor account for White Mulberry.
                     Feeds Clicktech Replenishment in AM_Replenishment.

Outputs (per current week NN computed from snapshot date, Sun-Sat convention):
    data/raw/inventory/Week {NN}/Audio_Array/Vendor SOH (SP-API).xlsx
    data/raw/inventory/Week {NN}/Tonor/Vendor SOH (SP-API).xlsx
    data/raw/inventory/Week {NN}/White_Mulberry/Vendor SOH (SP-API).xlsx
    data/raw/inventory/Week {NN}/_audit/vendor_soh_unmatched_<acct>.csv

Schema produced (matches the `channel == "1p"` slice of Inventory Snapshot.xlsx):
    SKU, ASIN, Brand, Model, category_l0, category_l1, category_l2,
    NLC, Qty, Channel (always "1p"), Type, Week,
    OpenPOUnits, Aged90PlusUnits, UnsellableUnits, SellableCost, SnapshotDate

Required env vars (.env at repo root):
    SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY
    SP_API_VENDOR_REFRESH_TOKEN_WHITEMULBERRY
    SP_LWA_CLIENT_ID_AUDIOARRAY      SP_LWA_CLIENT_SECRET_AUDIOARRAY
    SP_LWA_CLIENT_ID_WHITEMULBERRY   SP_LWA_CLIENT_SECRET_WHITEMULBERRY

Quirks:
    * Amazon's vendor inventory data has a ~3-day lag. A Sunday/Monday run
      requesting Saturday's data will fail with "data not yet available" —
      the script auto-falls-back up to 3 days. For reliable Saturday SOH,
      run Tue 07:00 IST or later.
    * Vendor inventory date window must be <= 7 days. Script uses 7d window
      ending at the target Saturday.

Usage:
    python scripts/sp_vendor_soh_pull.py                  # both accounts, last Sat
    python scripts/sp_vendor_soh_pull.py --account AUDIOARRAY
    python scripts/sp_vendor_soh_pull.py --date 2026-06-13
"""
from __future__ import annotations

import argparse
import functools
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
except Exception:
    pass
print = functools.partial(print, flush=True)

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
MARKETPLACE_ID = "A21TJRUUN4KGV"
REPO_ROOT  = Path(__file__).resolve().parent.parent

# sku_master lives in data/master in this pipeline (NOT data/input)
SKU_MASTER = REPO_ROOT / "data" / "master" / "sku_master.xlsx"

# Account -> brand -> folder slug used under data/raw/inventory/Week NN/
ACCOUNTS = {
    # Cambium Retail Pvt Ltd (CRPL) vendor account holds three brands:
    # Audio Array, Tonor, AND White Mulberry.  We loop all three so the
    # CRPL response gets split into each brand's folder based on
    # sku_master's ASIN→Brand mapping.  ASINs that aren't yet tagged in
    # master land in the _audit/ folder for the operator to backfill.
    "AUDIOARRAY": {
        "Audio Array":    "Audio_Array",
        "Tonor":          "Tonor",
        "White Mulberry": "White_Mulberry",
    },
    # Clicktech vendor account (legacy) — kept in the script for
    # completeness, but the current refresh token points at a dormant
    # account with ~20 old ASINs / 1 sellable unit.  WM's active
    # listings actually live under CRPL above.  Safe to leave wired —
    # produces empty xlsx + audit each run.
    "WHITEMULBERRY": {
        "White Mulberry": "White_Mulberry",
    },
}

SNAPSHOT_COLS = [
    "SKU", "ASIN", "Brand", "Model",
    "category_l0", "category_l1", "category_l2",
    "NLC", "Qty", "Channel", "Type", "Week",
    "OpenPOUnits", "Aged90PlusUnits", "UnsellableUnits",
    "SellableCost", "SnapshotDate",
]


def get_access_token(acct: str) -> str:
    cid = os.environ.get(f"SP_LWA_CLIENT_ID_{acct}")     or os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get(f"SP_LWA_CLIENT_SECRET_{acct}") or os.environ.get("SP_LWA_CLIENT_SECRET")
    rt  = os.environ.get(f"SP_API_VENDOR_REFRESH_TOKEN_{acct}")
    if not rt:
        raise SystemExit(f"Missing SP_API_VENDOR_REFRESH_TOKEN_{acct} in .env")
    if not cid or not sec:
        raise SystemExit(f"Missing LWA creds for {acct}")
    r = requests.post(LWA_URL, data={
        "grant_type": "refresh_token", "refresh_token": rt,
        "client_id": cid, "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"LWA failed for {acct}: HTTP {r.status_code} {r.text[:300]}")
    return r.json()["access_token"]


def most_recent_saturday(reference: datetime | None = None) -> str:
    """Most recent Saturday on or before (reference - 2d). Sun-Sat convention."""
    ref = (reference or datetime.now(timezone.utc)) - timedelta(days=2)
    offset = (ref.weekday() - 5) % 7  # Mon=0..Sun=6 -> Sat=5
    sat = (ref - timedelta(days=offset)).date()
    return sat.isoformat()


def sun_sat_week_number(date_str: str) -> int:
    """Operator's Sun-Sat week number for a date. W21=2026-05-17..23 reference.
    Trick: shift the date forward 1 day so Saturday maps to Sunday, then take
    the ISO week of that shifted date (which is Mon-Sun aligned)."""
    d = datetime.fromisoformat(date_str).date()
    return (d + timedelta(days=1)).isocalendar().week


class DataNotAvailable(Exception):
    pass


def _reuse_done_report(token: str, report_type: str,
                       start_date: str, end_date: str) -> str | None:
    """Most recent DONE report of this type+window, created in the last 24h.
    Same rationale as sp_vendor_sales_pull: skip Amazon's queue on same-day
    re-runs, but never reuse older documents — vendor content matures and a
    stale doc would freeze old numbers."""
    try:
        r = requests.get(
            f"{SPAPI_HOST}/reports/2021-06-30/reports",
            params={"reportTypes": report_type, "marketplaceIds": MARKETPLACE_ID,
                    "processingStatuses": "DONE", "pageSize": 10},
            headers={"x-amz-access-token": token}, timeout=30)
        if r.status_code != 200:
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        for rpt in (r.json().get("reports") or []):
            if (rpt.get("dataStartTime") or "")[:10] != start_date:
                continue
            if (rpt.get("dataEndTime") or "")[:10] != end_date:
                continue
            created = rpt.get("createdTime") or ""
            try:
                if datetime.fromisoformat(created.replace("Z", "+00:00")) < cutoff:
                    continue
            except ValueError:
                continue
            return rpt.get("reportDocumentId")
    except requests.RequestException:
        pass
    return None


def _adaptive_polls(budget_seconds: int):
    """Yield (waited, delay): 2s doubling to a 30s cap within the budget."""
    delay, waited = 2.0, 0.0
    while waited < budget_seconds:
        time.sleep(delay)
        waited += delay
        yield waited, delay
        delay = min(delay * 2, 30.0)


def pull_vendor_inventory(token: str, snapshot_date: str) -> list[dict]:
    headers = {"x-amz-access-token": token, "Content-Type": "application/json"}
    end_dt = datetime.fromisoformat(snapshot_date).replace(tzinfo=timezone.utc, hour=23, minute=59, second=59)
    start_dt = end_dt - timedelta(days=6)
    end   = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    body = {
        "reportType": "GET_VENDOR_INVENTORY_REPORT",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime": start, "dataEndTime": end,
        "reportOptions": {
            "reportPeriod": "DAY",
            "distributorView": "MANUFACTURING",
            "sellingProgram": "RETAIL",
        },
    }
    doc_id = _reuse_done_report(token, "GET_VENDOR_INVENTORY_REPORT", start[:10], end[:10])
    if doc_id:
        print(f"  reusing DONE report from last 24h (doc={doc_id[:12]}…)")
    else:
        print(f"  create report: {start} -> {end}")
        r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports", json=body, headers=headers, timeout=30)
        if r.status_code != 202:
            raise SystemExit(f"create report failed: HTTP {r.status_code} {r.text[:300]}")
        rep_id = r.json()["reportId"]
        print(f"  reportId: {rep_id}")

    # 15-min budget, adaptive 2s→30s (was 180 flat 5s polls).  Vendor
    # reports occasionally take the full budget under Amazon queue load
    # (observed on WM 1P first run — IN_PROGRESS 5+ min before DONE).
    for waited, _ in ([] if doc_id else _adaptive_polls(900)):
        rr = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rep_id}",
                          headers={"x-amz-access-token": token}, timeout=30)
        if rr.status_code != 200:
            continue
        j = rr.json()
        status = j.get("processingStatus")
        print(f"  poll[{int(waited)}s]: {status}")
        if status == "DONE":
            doc_id = j.get("reportDocumentId")
            break
        if status in ("FATAL", "CANCELLED"):
            doc_id = j.get("reportDocumentId")
            err_text = _read_error_doc(token, doc_id) if doc_id else ""
            if "not yet available" in err_text.lower():
                raise DataNotAvailable(err_text.strip())
            print(f"  FATAL body: {err_text[:600]}")
            raise SystemExit(f"report {status}")
    if not doc_id:
        raise SystemExit("timed out waiting for report")

    rd = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
                      headers={"x-amz-access-token": token}, timeout=30)
    rd.raise_for_status()
    doc = rd.json()
    dl = requests.get(doc["url"], timeout=120)
    dl.raise_for_status()
    raw = dl.content
    if doc.get("compressionAlgorithm") == "GZIP":
        import gzip
        raw = gzip.decompress(raw)
    payload = json.loads(raw.decode("utf-8", errors="replace"))
    return payload.get("inventoryByAsin", []) or []


def _read_error_doc(token: str, doc_id: str) -> str:
    rd = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
                      headers={"x-amz-access-token": token}, timeout=30)
    if rd.status_code != 200:
        return ""
    doc = rd.json()
    dl = requests.get(doc["url"], timeout=60)
    if dl.status_code != 200:
        return ""
    raw = dl.content
    if doc.get("compressionAlgorithm") == "GZIP":
        import gzip
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def latest_snapshot(rows: list[dict], target_date: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["startDate"] = df["startDate"].astype(str)
    if (df["startDate"] == target_date).any():
        chosen = target_date
    else:
        chosen = df["startDate"].max()
        print(f"  target {target_date} not in response, using latest {chosen}")
    snap = df[df["startDate"] == chosen].copy()

    def _amt(x):
        return x.get("amount") if isinstance(x, dict) else None

    snap["SellableCost"] = snap.get("sellableOnHandInventoryCost", pd.Series([None]*len(snap))).apply(_amt)
    return pd.DataFrame({
        "ASIN":            snap["asin"].astype(str).str.strip().str.upper(),
        "Qty":             pd.to_numeric(snap.get("sellableOnHandInventoryUnits"), errors="coerce").fillna(0).astype(int),
        "OpenPOUnits":     pd.to_numeric(snap.get("openPurchaseOrderUnits"),  errors="coerce").fillna(0).astype(int),
        "Aged90PlusUnits": pd.to_numeric(snap.get("aged90PlusDaysSellableInventoryUnits"), errors="coerce").fillna(0).astype(int),
        "UnsellableUnits": pd.to_numeric(snap.get("unsellableOnHandInventoryUnits"), errors="coerce").fillna(0).astype(int),
        "SellableCost":    pd.to_numeric(snap["SellableCost"], errors="coerce"),
        "SnapshotDate":    chosen,
    })


def annotate_with_master(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if not SKU_MASTER.exists():
        print(f"  WARN: {SKU_MASTER} not found — Brand/Model left blank")
        df["Brand"] = ""
        df["Model"] = ""
        return df
    sm = pd.read_excel(SKU_MASTER)
    sm.columns = sm.columns.str.strip()
    sm["ASIN"] = sm["ASIN"].astype(str).str.strip().str.upper()
    keep = ["ASIN", "Brand", "Model", "category_l0", "category_l1", "category_l2", "NLC", "FBA SKU"]
    sm = sm[[c for c in keep if c in sm.columns]].drop_duplicates(subset=["ASIN"], keep="first")
    sm = sm.rename(columns={"FBA SKU": "SKU"})
    return df.merge(sm, on="ASIN", how="left")


def write_brand_xlsx(df: pd.DataFrame, brand: str, week_no: int, folder_slug: str) -> int:
    sub = df[df["Brand"].astype(str).str.strip().str.lower() == brand.lower()].copy()
    if sub.empty:
        print(f"  WARN: no ASINs matched brand={brand}")
    sub["Channel"] = "1p"
    sub["Type"] = ""
    sub["Week"] = week_no
    for col in SNAPSHOT_COLS:
        if col not in sub.columns:
            sub[col] = ""
    sub = sub[SNAPSHOT_COLS]
    out_dir = REPO_ROOT / "data" / "raw" / "inventory" / f"Week {week_no}" / folder_slug
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "Vendor SOH (SP-API).xlsx"
    sub.to_excel(out_path, index=False)
    print(f"  -> {out_path.relative_to(REPO_ROOT)}  ({len(sub)} rows, sum Qty={int(sub['Qty'].sum())})")
    return len(sub)


def run_account(acct: str, snapshot_date: str, fallback_days: int = 3) -> None:
    brands = ACCOUNTS[acct]
    print(f"\n=== {acct} (target {snapshot_date}) ===")
    tok = get_access_token(acct)

    rows = None
    effective_date = snapshot_date
    try_date = snapshot_date
    for attempt in range(fallback_days + 1):
        try_date = (datetime.fromisoformat(snapshot_date).date() - timedelta(days=attempt)).isoformat()
        try:
            rows = pull_vendor_inventory(tok, try_date)
            effective_date = try_date
            if attempt > 0:
                print(f"  fell back from {snapshot_date} to {try_date} (Amazon hadn't published target yet)")
            break
        except DataNotAvailable:
            print(f"  data not available for {try_date}; trying earlier date")
            continue
    if rows is None:
        print(f"  gave up after {fallback_days+1} attempts; latest tried = {try_date}")
        return

    print(f"  inventoryByAsin rows pulled: {len(rows)}")
    snap = latest_snapshot(rows, effective_date)
    if snap.empty:
        print(f"  no SOH rows for {acct}")
        return
    print(f"  effective date: {snap['SnapshotDate'].iat[0]}, ASINs={len(snap)}, sellable={int(snap['Qty'].sum())}")
    annotated = annotate_with_master(snap)
    present = annotated["Brand"].astype(str).str.strip().value_counts().to_dict()
    print(f"  brand split (per sku_master): {present}")

    week_no = sun_sat_week_number(effective_date)
    print(f"  Sun-Sat week: W{week_no}")

    for brand, folder_slug in brands.items():
        write_brand_xlsx(annotated, brand, week_no, folder_slug)

    # Unmatched ASINs → audit folder for the week
    unm = annotated[annotated["Brand"].isna() | (annotated["Brand"].astype(str).str.lower() == "nan")].copy()
    if not unm.empty:
        audit_dir = REPO_ROOT / "data" / "raw" / "inventory" / f"Week {week_no}" / "_audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        unm_path = audit_dir / f"vendor_soh_unmatched_{acct.lower()}.csv"
        cols = ["ASIN", "Qty", "OpenPOUnits", "Aged90PlusUnits", "UnsellableUnits", "SellableCost", "SnapshotDate"]
        unm = unm.sort_values("Qty", ascending=False)
        unm[cols].to_csv(unm_path, index=False)
        print(f"  {len(unm)} ASINs missing from sku_master (SOH={int(unm['Qty'].sum())}) -> {unm_path.relative_to(REPO_ROOT)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", choices=list(ACCOUNTS.keys()) + ["ALL"], default="ALL")
    ap.add_argument("--date", default=None,
                    help="Snapshot date YYYY-MM-DD (default: most recent Saturday)")
    args = ap.parse_args()
    load_dotenv(REPO_ROOT / ".env")
    snap_date = args.date or most_recent_saturday()
    print(f"Snapshot date: {snap_date}  (Sun-Sat W{sun_sat_week_number(snap_date)})")
    targets = list(ACCOUNTS.keys()) if args.account == "ALL" else [args.account]
    for acct in targets:
        run_account(acct, snap_date)


if __name__ == "__main__":
    main()
