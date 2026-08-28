"""ONE-OFF — rebuild Audio Array 1P (Vendor Central) sales for 2-6 Aug 2026
from SHIPPED units instead of ORDERED units.

Why
---
A wrong price went live on those dates. Orders piled in against it and were
then cancelled, and Vendor Central books a cancellation on the day it happens,
not the day the order was placed. The result in the ledger:

    03 Aug   +887 units   +Rs 16,99,720     <- the rush
    05 Aug   -292 units   -Rs  3,24,172     <- the clawback

Neither number describes a day of trading. Shipped units do — they are what
actually left the warehouse — so we re-state those five days on shipped units
and freeze them.

Revenue has to be derived
-------------------------
GET_VENDOR_SALES_REPORT returns `shippedRevenue: 0` and `shippedCogs: 0` on
every row for this account, in BOTH the MANUFACTURING and SOURCING views —
Amazon simply does not populate them here. (The weekly capture files under
`data/raw/sales/Week */*/Vendor Sales (SP-API).xlsx` carry a ShippedRevenue
column for the same reason and it is 0 in every week on record.) Only
`shippedUnits` is real. So:

    shipped revenue(asin, day) = shipped units(asin, day) x ASP(asin)

ASP comes from a clean window AFTER the bad price was corrected (default
8-14 Aug), as sum(orderedRevenue) / sum(orderedUnits) per ASIN. ASP cannot be
taken from 2-6 Aug itself — that IS the wrong price. ASINs with no clean-window
sales fall back to the planning file's implied ASP (Aug Goal Projected /
Monthly Unit); anything still without an ASP is reported and skipped, never
written as zero revenue.

Writes go through /api/{brand}/sync-sales with replace_day, and the payload
names only "Vendor Central", so the AA seller accounts are untouched.

Usage
-----
    python scripts/freeze_aa_vendor_shipped.py                 # dry run
    python scripts/freeze_aa_vendor_shipped.py --apply         # write
    python scripts/freeze_aa_vendor_shipped.py --cache-dir DIR # reuse pulls

Do NOT re-run the ordinary daily /pull-sales for 2-6 Aug afterwards; it would
put the cancelled-order numbers back.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import httpx
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
BACKEND = REPO / "backend"
sys.path.insert(0, str(BACKEND))

# SP-API creds live in the FastAPI project's .env (same source as
# scripts/push_render_env.py); backend/.env keeps them blank on purpose.
FASTAPI_ENV = Path(r"D:/Nitesh/Nitesh Gdrive/Nitesh/Weekly Report - B2B + B2C/FastAPI/.env")
RENDER_CLI_CONFIG = Path.home() / ".render" / "cli.yaml"
API_SERVICE_ID = "srv-d9ku14b7uimc738629h0"

TARGET_DAYS = ["2026-08-02", "2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06"]
ASP_DAYS = ["2026-08-08", "2026-08-09", "2026-08-10", "2026-08-11",
            "2026-08-12", "2026-08-13", "2026-08-14"]
ACCOUNT = "Vendor Central"


def load_env() -> None:
    for env_file in (FASTAPI_ENV, BACKEND / ".env"):
        if not env_file.exists():
            continue
        for line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")
            if v:
                os.environ.setdefault(k.strip(), v)


def amount(v) -> float:
    return float(v.get("amount") or 0) if isinstance(v, dict) else float(v or 0)


def vendor_day(day: str, cache_dir: Path | None) -> list[dict]:
    """Per-ASIN rows for one day, from cache if present else SP-API."""
    if cache_dir:
        cached = cache_dir / f"vendor_{day}.json"
        if cached.exists():
            return json.loads(cached.read_text(encoding="utf-8"))

    from app.pullers.sp_client import MARKETPLACE, get_access_token, submit_and_download

    token = get_access_token(
        os.getenv("SP_LWA_CLIENT_ID_AUDIOARRAY"),
        os.getenv("SP_LWA_CLIENT_SECRET_AUDIOARRAY"),
        os.getenv("SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY"),
    )
    payload = submit_and_download(
        token,
        {
            "reportType": "GET_VENDOR_SALES_REPORT",
            "marketplaceIds": [MARKETPLACE],
            "dataStartTime": f"{day}T00:00:00Z",
            "dataEndTime": f"{day}T23:59:59Z",
            "reportOptions": {
                "reportPeriod": "DAY",
                "distributorView": "MANUFACTURING",
                "sellingProgram": "RETAIL",
            },
        },
        poll_seconds=5,
        poll_max=180,
    )
    rows = payload.get("salesByAsin") or []
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / f"vendor_{day}.json").write_text(json.dumps(rows), encoding="utf-8")
    return rows


def clean_window_asp(cache_dir: Path | None, days: list[str]) -> dict[str, float]:
    rev: dict[str, float] = {}
    units: dict[str, float] = {}
    for day in days:
        for r in vendor_day(day, cache_dir):
            a = str(r.get("asin") or "").strip().upper()
            if not a:
                continue
            rev[a] = rev.get(a, 0.0) + amount(r.get("orderedRevenue"))
            units[a] = units.get(a, 0.0) + amount(r.get("orderedUnits"))
    # Only ASINs that actually sold in the window carry a trustworthy ASP.
    return {a: rev[a] / units[a] for a in units if units[a] > 0 and rev.get(a, 0) > 0}


def planning_asp(plan_path: Path) -> tuple[dict[str, float], set[str]]:
    """Implied ASP per ASIN from the planning sheet, plus the plan ASIN set."""
    df = pd.read_excel(plan_path, sheet_name="Main", engine="openpyxl")
    df.columns = [re.sub(r"[^a-z0-9]", "", str(c).lower()) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    asin_col = "asin"
    goal_col = next((c for c in df.columns if c.endswith("goalprojected")), None)
    unit_col = next((c for c in df.columns if c == "monthlyunit"), None)
    asins = set(df[asin_col].astype(str).str.strip().str.upper())
    asp: dict[str, float] = {}
    if goal_col and unit_col:
        for _, r in df.iterrows():
            a = str(r[asin_col]).strip().upper()
            g = pd.to_numeric(r[goal_col], errors="coerce")
            u = pd.to_numeric(r[unit_col], errors="coerce")
            if pd.notna(g) and pd.notna(u) and u > 0 and g > 0:
                asp[a] = float(g) / float(u)
    return asp, asins


def sync_token() -> str:
    """Read SYNC_SALES_TOKEN off the Render service.

    Deliberately NOT from the local env first: the FastAPI project's .env
    carries an unrelated 9-char SYNC_SALES_TOKEN which load_env() would
    otherwise shadow the real one with, and the API just answers 401.
    """
    if not RENDER_CLI_CONFIG.exists():
        tok = (os.getenv("SYNC_SALES_TOKEN") or "").strip()
        if tok:
            return tok
        sys.exit("No Render CLI config and no SYNC_SALES_TOKEN in the environment.")
    text = RENDER_CLI_CONFIG.read_text(encoding="utf-8")
    m = re.search(r"key:\s*(\S+)", text)
    if not m:
        sys.exit("api.key missing in ~/.render/cli.yaml")
    r = httpx.get(
        f"https://api.render.com/v1/services/{API_SERVICE_ID}/env-vars",
        headers={"Authorization": f"Bearer {m.group(1)}", "Accept": "application/json"},
        params={"limit": 100},
        timeout=30,
    )
    r.raise_for_status()
    for item in r.json():
        ev = item.get("envVar") or item
        if ev.get("key") == "SYNC_SALES_TOKEN":
            return ev["value"]
    sys.exit("SYNC_SALES_TOKEN not found on the Render service.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="https://sales-dashboard-api-23ii.onrender.com")
    ap.add_argument("--brand", default="audio_array")
    ap.add_argument("--days", nargs="*", default=TARGET_DAYS)
    ap.add_argument("--asp-days", nargs="*", default=ASP_DAYS)
    ap.add_argument("--cache-dir", type=Path, default=None,
                    help="reuse/store raw SP-API day pulls as JSON")
    ap.add_argument("--apply", action="store_true", help="write to the ledger")
    args = ap.parse_args()

    load_env()

    plan_path = BACKEND / "data" / "planning" / "audio_array" / "ASIN Planning file - Aug 2026.xlsx"
    plan_asp, plan_asins = planning_asp(plan_path)
    print(f"planning file: {plan_path.name} — {len(plan_asins)} ASINs, "
          f"{len(plan_asp)} with an implied ASP")

    print(f"ASP window {args.asp_days[0]}..{args.asp_days[-1]} …")
    asp = clean_window_asp(args.cache_dir, args.asp_days)
    print(f"  {len(asp)} ASINs priced from actual post-correction sales")

    grand_old = grand_new = 0.0
    grand_old_u = grand_new_u = 0.0
    payloads: list[tuple[str, list[dict]]] = []
    no_asp: dict[str, float] = {}

    for day in args.days:
        rows = vendor_day(day, args.cache_dir)
        out: list[dict] = []
        old_rev = old_u = new_rev = new_u = 0.0
        for r in rows:
            a = str(r.get("asin") or "").strip().upper()
            if not a or a not in plan_asins:
                continue
            old_rev += amount(r.get("orderedRevenue"))
            old_u += amount(r.get("orderedUnits"))
            su = amount(r.get("shippedUnits"))
            if su <= 0:
                continue
            unit_price = asp.get(a) or plan_asp.get(a)
            if not unit_price:
                no_asp[a] = no_asp.get(a, 0.0) + su
                continue
            new_rev += su * unit_price
            new_u += su
            out.append({"ASIN": a, "orderedRevenue": round(su * unit_price, 2),
                        "orderedUnits": int(su)})
        payloads.append((day, out))
        grand_old += old_rev; grand_new += new_rev
        grand_old_u += old_u; grand_new_u += new_u
        print(f"[{day}] ordered {old_rev:>13,.0f} / {old_u:>6.0f}u   ->   "
              f"shipped {new_rev:>13,.0f} / {new_u:>6.0f}u   ({len(out)} ASINs)")

    print(f"{'TOTAL':10} ordered {grand_old:>13,.0f} / {grand_old_u:>6.0f}u   ->   "
          f"shipped {grand_new:>13,.0f} / {grand_new_u:>6.0f}u")
    if no_asp:
        total_u = sum(no_asp.values())
        print(f"\n!! {len(no_asp)} ASIN(s) / {total_u:.0f} shipped units have no ASP from "
              f"either source and were SKIPPED:")
        for a, u in sorted(no_asp.items(), key=lambda kv: -kv[1])[:15]:
            print(f"   {a}  {u:.0f}u")

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply to commit.")
        return 0

    token = sync_token()
    for day, rows in payloads:
        if not rows:
            print(f"[{day}] no rows — skipped (existing ledger rows left alone)")
            continue
        r = httpx.post(
            f"{args.api}/api/{args.brand}/sync-sales",
            headers={"x-sync-token": token},
            json={
                "sales_date": day,
                "replace_day": True,
                "accounts": [{"account": ACCOUNT, "is_vendor": True, "rows": rows}],
            },
            timeout=300,
        )
        if r.status_code != 200:
            print(f"[{day}] FAILED HTTP {r.status_code} {r.text[:300]}")
            continue
        js = r.json()
        print(f"[{day}] written — rows={js.get('total_rows')} "
              f"out_of_plan={js.get('out_of_plan_count')}")
    print("\nDone. Do NOT re-run the plain daily pull for these dates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
