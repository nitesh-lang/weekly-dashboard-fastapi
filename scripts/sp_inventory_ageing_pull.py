"""Ageing & non-moving inventory — Audio Array + Tonor, both channels.

One command for Fossil/AM ops:
    python scripts/sp_inventory_ageing_pull.py

Pulls two verified SP-API reports:
  3P (FBA, AUDIOARRAY seller account):
      GET_FBA_INVENTORY_PLANNING_DATA — per-SKU age buckets
      (0-90 / 91-180 / 181-270 / 271-365 / 365+ days)
  1P (vendor, CRPL account = Audio Array + Tonor):
      GET_VENDOR_INVENTORY_REPORT, last full Sun-Sat week
      (window MUST be Sunday-aligned or Amazon returns FATAL)
      — sellable/unsellable on-hand + aged-90+ units & cost + sell-through

Output: data/processed/inventory_ageing_AA_Tonor.xlsx
  Sheets: Summary | FBA_3P_age | Vendor_1P_age
Filtered to Audio Array + Tonor via sku_master; unmapped ASINs are kept on a
separate sheet so aged stock can't hide behind a missing master row.

First proven 08/09/26: AA 1P had 3,607u / Rs1.24Cr aged 90+ (AM-W22 negative
sell-through); AA 3P healthy (nothing past 270d).
"""
from __future__ import annotations

import gzip
import io
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_XLSX = REPO_ROOT / "data" / "processed" / "inventory_ageing_AA_Tonor.xlsx"
MASTER = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
MKT = "A21TJRUUN4KGV"
BRANDS = {"Audio Array", "Tonor"}


def _lwa(env_key: str) -> str:
    rt = os.environ.get(env_key)
    if not rt:
        raise SystemExit(f"Missing {env_key} in .env")
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token", "refresh_token": rt,
        "client_id": os.environ["SP_LWA_CLIENT_ID"],
        "client_secret": os.environ["SP_LWA_CLIENT_SECRET"]}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def _pull(tok: str, body: dict) -> bytes | None:
    H = {"x-amz-access-token": tok, "content-type": "application/json"}
    c = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports", json=body, headers=H, timeout=30)
    if c.status_code != 202:
        print(f"  create failed: HTTP {c.status_code} {c.text[:150]}")
        return None
    rid = c.json()["reportId"]
    for _ in range(40):
        j = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rid}",
                         headers={"x-amz-access-token": tok}, timeout=30).json()
        st = j.get("processingStatus")
        if st in ("DONE", "FATAL", "CANCELLED"):
            break
        time.sleep(12)
    if st != "DONE":
        print(f"  report {body['reportType']}: {st}")
        return None
    d = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{j['reportDocumentId']}",
                     headers={"x-amz-access-token": tok}, timeout=30).json()
    raw = requests.get(d["url"], timeout=120).content
    if d.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    return raw


def main() -> None:
    load_dotenv(REPO_ROOT / ".env")
    m = pd.read_excel(MASTER)
    brand_map = dict(zip(m["ASIN"].astype(str).str.strip(), m["Brand"].astype(str)))
    model_map = dict(zip(m["ASIN"].astype(str).str.strip(), m["Model"].astype(str)))

    # ── 3P: FBA inventory planning (age buckets) ─────────────────────────
    print("3P: FBA inventory planning (AUDIOARRAY seller)…")
    t3 = _lwa("SP_REFRESH_TOKEN_AUDIOARRAY")
    raw = _pull(t3, {"reportType": "GET_FBA_INVENTORY_PLANNING_DATA",
                     "marketplaceIds": [MKT]})
    fba = pd.DataFrame()
    if raw is not None:
        fba = pd.read_csv(io.BytesIO(raw), sep="\t", dtype=str)
        keep = ["sku", "asin", "product-name", "available",
                "inv-age-0-to-90-days", "inv-age-91-to-180-days",
                "inv-age-181-to-270-days", "inv-age-271-to-365-days",
                "inv-age-365-plus-days", "units-shipped-t30",
                "estimated-storage-cost-next-month"]
        fba = fba[[c for c in keep if c in fba.columns]].copy()
        for c in fba.columns[3:]:
            fba[c] = pd.to_numeric(fba[c], errors="coerce").fillna(0)
        fba["brand"] = fba["asin"].astype(str).str.strip().map(brand_map).fillna("UNMAPPED")
        fba["model"] = fba["asin"].astype(str).str.strip().map(model_map).fillna("?")
        fba = fba[fba["brand"].isin(BRANDS | {"UNMAPPED"})]
        print(f"  {len(fba)} rows")

    # ── 1P: vendor inventory, last full Sun-Sat week ─────────────────────
    print("1P: vendor inventory (CRPL = AA + Tonor)…")
    today = date.today()
    last_sat = today - timedelta(days=(today.weekday() + 2) % 7 or 7)
    last_sun = last_sat - timedelta(days=6)
    tv = _lwa("SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY")
    raw2 = _pull(tv, {"reportType": "GET_VENDOR_INVENTORY_REPORT", "marketplaceIds": [MKT],
                      "dataStartTime": f"{last_sun}T00:00:00Z",
                      "dataEndTime": f"{last_sat}T23:59:59Z",
                      "reportOptions": {"reportPeriod": "WEEK",
                                        "sellingProgram": "RETAIL",
                                        "distributorView": "MANUFACTURING"}})
    ven = pd.DataFrame()
    if raw2 is not None:
        ven = pd.DataFrame(json.loads(raw2)["inventoryByAsin"])
        amt = lambda x: x.get("amount") if isinstance(x, dict) else x
        for c in [c for c in ven.columns if c.endswith("Cost")]:
            ven[c] = ven[c].apply(amt)
        keep = ["asin", "sellableOnHandInventoryUnits", "sellableOnHandInventoryCost",
                "unsellableOnHandInventoryUnits",
                "aged90PlusDaysSellableInventoryUnits", "aged90PlusDaysSellableInventoryCost",
                "sellThroughRate", "openPurchaseOrderUnits", "unfilledCustomerOrderedUnits"]
        ven = ven[[c for c in keep if c in ven.columns]].copy()
        for c in ven.columns[1:]:
            ven[c] = pd.to_numeric(ven[c], errors="coerce")
        ven["brand"] = ven["asin"].astype(str).str.strip().map(brand_map).fillna("UNMAPPED")
        ven["model"] = ven["asin"].astype(str).str.strip().map(model_map).fillna("?")
        ven["window"] = f"{last_sun} → {last_sat}"
        print(f"  {len(ven)} rows (week {last_sun} → {last_sat})")

    if fba.empty and ven.empty:
        print("Nothing pulled — aborting.")
        sys.exit(1)

    # ── Summary + write ──────────────────────────────────────────────────
    lines = []
    for b in sorted(BRANDS):
        if not ven.empty:
            s = ven[ven["brand"] == b]
            lines.append({"channel": "1P vendor", "brand": b,
                          "on_hand_units": int(s["sellableOnHandInventoryUnits"].sum()),
                          "on_hand_cost": round(float(s["sellableOnHandInventoryCost"].sum()), 0),
                          "aged90p_units": int(s["aged90PlusDaysSellableInventoryUnits"].sum()),
                          "aged90p_cost": round(float(s["aged90PlusDaysSellableInventoryCost"].sum()), 0)})
        if not fba.empty:
            s = fba[fba["brand"] == b]
            lines.append({"channel": "3P FBA", "brand": b,
                          "on_hand_units": int(s["available"].sum()),
                          "aged90p_units": int(s["inv-age-91-to-180-days"].sum()
                                               + s["inv-age-181-to-270-days"].sum()
                                               + s["inv-age-271-to-365-days"].sum()
                                               + s["inv-age-365-plus-days"].sum())})
    summary = pd.DataFrame(lines)
    unmapped = pd.concat([
        fba[fba["brand"] == "UNMAPPED"] if not fba.empty else pd.DataFrame(),
        ven[ven["brand"] == "UNMAPPED"] if not ven.empty else pd.DataFrame()],
        ignore_index=True)

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as xw:
        summary.to_excel(xw, "Summary", index=False)
        if not fba.empty:
            fba[fba["brand"].isin(BRANDS)].sort_values("inv-age-365-plus-days", ascending=False) \
                .to_excel(xw, "FBA_3P_age", index=False)
        if not ven.empty:
            ven[ven["brand"].isin(BRANDS)].sort_values("aged90PlusDaysSellableInventoryCost",
                                                       ascending=False) \
                .to_excel(xw, "Vendor_1P_age", index=False)
        if not unmapped.empty:
            unmapped.to_excel(xw, "Unmapped_ASINs", index=False)

    print(f"\nWROTE {OUT_XLSX}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
