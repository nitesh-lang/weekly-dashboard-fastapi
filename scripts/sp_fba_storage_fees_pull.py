"""FBA storage + long-term storage fees, per ASIN per month, all seller accounts.

Amazon charges these silently against the settlement — no one prices them into
margins until they're pulled. Verified live 07/09/26: Nexlev alone paid
Rs 1,14,997 storage in Aug-26 (SC-01 Rs 11,769/mo), and LTSF was already
burning on aged stock (LR-03: Rs 1,345 in Aug).

Reports (both verified working on our tokens):
  GET_FBA_STORAGE_FEE_CHARGES_DATA                      -> monthly storage per ASIN/FC
  GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA -> LTSF per ASIN (snapshot)

Output: data/processed/storage_fees_snapshot.csv
  account, asin, sku(from LTSF where present), month, storage_fee, ltsf_fee,
  avg_qty_on_hand
One row per (account, asin, month). The margin tool's actuals endpoint reads
this to price warehousing into the Actual column (fee / avg monthly units).

Usage:
  python scripts/sp_fba_storage_fees_pull.py                # last full month
  python scripts/sp_fba_storage_fees_pull.py --month 2026-07
  python scripts/sp_fba_storage_fees_pull.py --account NEXLEV
"""
from __future__ import annotations

import argparse
import gzip
import io
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_CSV = REPO_ROOT / "data" / "processed" / "storage_fees_snapshot.csv"

SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
MARKETPLACE_IN = "A21TJRUUN4KGV"

SELLER_ACCOUNTS = ["AUDIOARRAY", "CAMBIUMRETAIL", "NEXLEV", "VIOMI", "WHITEMULBERRY"]

STORAGE_RT = "GET_FBA_STORAGE_FEE_CHARGES_DATA"
LTSF_RT = "GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA"


def _lwa_token(account: str) -> str | None:
    rt = os.environ.get(f"SP_REFRESH_TOKEN_{account}")
    if not rt:
        print(f"  WARN: SP_REFRESH_TOKEN_{account} unset — skipping {account}.")
        return None
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token", "refresh_token": rt,
        "client_id": os.environ["SP_LWA_CLIENT_ID"],
        "client_secret": os.environ["SP_LWA_CLIENT_SECRET"]}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def _pull_report(tok: str, report_type: str, start_iso: str, end_iso: str) -> pd.DataFrame | None:
    H = {"x-amz-access-token": tok, "content-type": "application/json"}
    body = {"reportType": report_type, "marketplaceIds": [MARKETPLACE_IN],
            "dataStartTime": start_iso, "dataEndTime": end_iso}
    c = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports", json=body, headers=H, timeout=30)
    if c.status_code != 202:
        print(f"    create {report_type}: HTTP {c.status_code} {c.text[:150]}")
        return None
    rid = c.json()["reportId"]
    status = "?"
    for _ in range(40):
        j = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rid}",
                         headers={"x-amz-access-token": tok}, timeout=30).json()
        status = j.get("processingStatus")
        if status in ("DONE", "FATAL", "CANCELLED"):
            break
        time.sleep(15)
    if status != "DONE":
        print(f"    {report_type}: {status}")
        return None
    d = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{j['reportDocumentId']}",
                     headers={"x-amz-access-token": tok}, timeout=30).json()
    raw = requests.get(d["url"], timeout=120).content
    if d.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    return pd.read_csv(io.BytesIO(raw), sep="\t", dtype=str)


def run_account(account: str, month: str) -> pd.DataFrame | None:
    print(f"\n=== {account} ({month}) ===")
    tok = _lwa_token(account)
    if not tok:
        return None
    y, m = map(int, month.split("-"))
    start = date(y, m, 1)
    end = (date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1))
    s_iso, e_iso = f"{start}T00:00:00Z", f"{end}T23:59:59Z"

    st = _pull_report(tok, STORAGE_RT, s_iso, e_iso)
    lt = _pull_report(tok, LTSF_RT, s_iso, e_iso)

    frames = []
    if st is not None and not st.empty and "asin" in st.columns:
        st["storage_fee"] = pd.to_numeric(st["estimated-monthly-storage-fee"], errors="coerce").fillna(0)
        st["avg_qty_on_hand"] = pd.to_numeric(st.get("average-quantity-on-hand"), errors="coerce").fillna(0)
        g = st.groupby(st["asin"].str.strip().str.upper(), as_index=True).agg(
            storage_fee=("storage_fee", "sum"), avg_qty_on_hand=("avg_qty_on_hand", "sum"))
        frames.append(g)
        print(f"    storage: {len(st)} rows, Rs {g['storage_fee'].sum():,.0f}")
    else:
        print("    storage: no data")
    if lt is not None and not lt.empty and "asin" in lt.columns:
        for c in ("long-time-range-long-term-storage-fee", "short-time-range-long-term-storage-fee"):
            lt[c] = pd.to_numeric(lt.get(c), errors="coerce").fillna(0)
        lt["ltsf_fee"] = lt["long-time-range-long-term-storage-fee"] + lt["short-time-range-long-term-storage-fee"]
        g2 = lt.groupby(lt["asin"].str.strip().str.upper(), as_index=True).agg(ltsf_fee=("ltsf_fee", "sum"))
        frames.append(g2)
        print(f"    LTSF: {len(lt)} rows, Rs {g2['ltsf_fee'].sum():,.0f}")
    else:
        print("    LTSF: no data")
    if not frames:
        return None
    out = pd.concat(frames, axis=1).fillna(0).reset_index().rename(columns={"index": "asin"})
    if "ltsf_fee" not in out.columns:
        out["ltsf_fee"] = 0.0
    if "storage_fee" not in out.columns:
        out["storage_fee"] = 0.0
    if "avg_qty_on_hand" not in out.columns:
        out["avg_qty_on_hand"] = 0.0
    out["account"] = account
    out["month"] = month
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", choices=SELLER_ACCOUNTS + ["ALL"], default="ALL")
    ap.add_argument("--month", default=None, help="YYYY-MM (default: last full month)")
    args = ap.parse_args()
    load_dotenv(REPO_ROOT / ".env")

    if args.month:
        month = args.month
    else:
        first_of_this = date.today().replace(day=1)
        last_month_end = first_of_this - timedelta(days=1)
        month = f"{last_month_end.year}-{last_month_end.month:02d}"

    targets = SELLER_ACCOUNTS if args.account == "ALL" else [args.account]
    results = [df for a in targets if (df := run_account(a, month)) is not None]
    if not results:
        print("No data pulled — nothing written.")
        sys.exit(1)
    new = pd.concat(results, ignore_index=True)[
        ["account", "asin", "month", "storage_fee", "ltsf_fee", "avg_qty_on_hand"]]

    # merge with existing snapshot: replace this month's rows for the pulled
    # accounts, keep everything else (history accumulates month over month)
    if OUT_CSV.exists():
        old = pd.read_csv(OUT_CSV, dtype={"month": str})
        keep = old[~((old["month"] == month) & (old["account"].isin(targets)))]
        new = pd.concat([keep, new], ignore_index=True)
    new.to_csv(OUT_CSV, index=False)
    tot = new[new["month"] == month]
    print(f"\nWROTE {OUT_CSV.name}: {month} storage Rs {tot['storage_fee'].sum():,.0f} "
          f"+ LTSF Rs {tot['ltsf_fee'].sum():,.0f} across {tot['asin'].nunique()} ASINs "
          f"({len(new)} rows total incl. history)")


if __name__ == "__main__":
    main()
