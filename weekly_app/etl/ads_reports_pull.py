"""
Amazon Ads Reports API v3 — SP / SD / SB metric pull.

Replaces the manual per-week Excel export step.  Pulls daily-grain
performance data, aggregates to (brand × Sun-Sat week × ad_type × ASIN),
writes per-brand `ads_report_week<N>.xlsx` files with SP / SD / SB
sheets in the schema downstream ETL already consumes.

Report types pulled per profile:
  - spAdvertisedProduct        (SP, per-ASIN metrics — direct)
  - sdAdvertisedProduct        (SD, per-ASIN metrics — direct)
  - sbAds                      (SB, per-AD metrics — needs ASIN split)
  - sbPurchasedProduct         (SB, per-(AD × ASIN) attributed sales —
                                  used to weight the spend split)

Modes:
  python -m weekly_app.etl.ads_reports_pull submit  --start YYYY-MM-DD --end YYYY-MM-DD
       Submit all reports up-front, save reportIds to a state file, exit.
  python -m weekly_app.etl.ads_reports_pull poll
       Resume from the state file, poll until everything is DONE/FAILED,
       download + write per-brand xlsx files.
  python -m weekly_app.etl.ads_reports_pull run    --start YYYY-MM-DD --end YYYY-MM-DD
       submit + poll in one shot (blocks until all reports complete).

State file:
  data/processed/.ads_reports_state.json
"""
from __future__ import annotations
import argparse, datetime as dt, gzip, json, os, sys, time
from pathlib import Path
from collections import defaultdict
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass

from weekly_app.etl.ads_api_pull import (
    get_access_token, base_headers, ADS_API_BASE,
)

REPORTS_CT = "application/vnd.createasyncreportrequest.v3+json"

STATE_FILE = ROOT / "data" / "processed" / ".ads_reports_state.json"
OUT_BASE   = ROOT / "data" / "ams_weekly_data"

# ── Report configs ─────────────────────────────────────────────────────
# Columns chosen to mirror the operator-exported xlsx schema.  All
# `*7d` metrics use the 7-day attribution window (Amazon default).
REPORT_TYPES: dict[str, dict] = {
    "sp_adprod": {
        "reportTypeId": "spAdvertisedProduct",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["advertiser"],
        "timeUnit":     "DAILY",
        "columns": [
            "date",
            "campaignName", "campaignId",
            "adGroupName",  "adGroupId",
            "advertisedAsin", "advertisedSku",
            "impressions", "clicks", "cost",
            "purchases7d", "sales7d", "unitsSoldClicks7d",
        ],
        "sheet": "SP",
    },
    "sd_adprod": {
        "reportTypeId": "sdAdvertisedProduct",
        "adProduct":    "SPONSORED_DISPLAY",
        "groupBy":      ["advertiser"],
        "timeUnit":     "DAILY",
        "columns": [
            "date",
            "campaignName", "campaignId",
            "adGroupName",  "adGroupId",
            "promotedAsin", "promotedSku",
            "impressions", "clicks", "cost",
            "purchases", "sales", "unitsSold",
        ],
        "sheet": "SD",
    },
    "sb_ad": {
        # SB has no per-ASIN cost data — pull per-AD and split later.
        # Amazon v3: sbAds requires groupBy=["ads"] (plural).  Metric
        # columns use bare names (NOT 14d-suffixed — that's only for
        # sbPurchasedProduct, frustratingly).
        "reportTypeId": "sbAds",
        "adProduct":    "SPONSORED_BRANDS",
        "groupBy":      ["ads"],
        "timeUnit":     "DAILY",
        "columns": [
            "date",
            "campaignName", "campaignId",
            "adGroupName",  "adGroupId",
            "adId",
            "impressions", "clicks", "cost",
            "purchases", "sales", "unitsSold",
        ],
        "sheet": "SB",
    },
    "sb_purchproduct": {
        # Per-(AD × ASIN) purchased products — drives sales-weighted split.
        # Amazon v3 sbPurchasedProduct only exposes new-to-brand metrics
        # at the purchasedAsin level (not total sales).  NTB sales correlate
        # closely enough with total sales for split-weighting purposes.
        "reportTypeId": "sbPurchasedProduct",
        "adProduct":    "SPONSORED_BRANDS",
        "groupBy":      ["purchasedAsin"],
        "timeUnit":     "DAILY",
        "columns": [
            "date",
            "campaignId", "adGroupId",
            "purchasedAsin",
            "newToBrandUnitsSold14d",
            "newToBrandSales14d",
            "newToBrandPurchases14d",
        ],
        "sheet": "SB_PURCH",
    },
}


# ── Sun-Sat week math ──────────────────────────────────────────────────
def sun_sat_week(d: dt.date) -> tuple[int, dt.date, dt.date]:
    """Returns (week_num, sun_date, sat_date) for the Sun-Sat week
    containing `d`.  Week 1 = the Sun-Sat week containing Jan 1 of d.year."""
    # Sunday on or before d.  Python: Mon=0..Sun=6 → days_since_sun:
    # Sun=0, Mon=1, ..., Sat=6
    days_since_sun = (d.weekday() + 1) % 7
    sun = d - dt.timedelta(days=days_since_sun)
    sat = sun + dt.timedelta(days=6)
    # Week 1 anchors on the Sunday on or before Jan 1
    year_start = dt.date(d.year, 1, 1)
    ys_days_since_sun = (year_start.weekday() + 1) % 7
    w1_sun = year_start - dt.timedelta(days=ys_days_since_sun)
    week_num = ((sun - w1_sun).days // 7) + 1
    return week_num, sun, sat


# ── Date-range chunking (Amazon caps most reports at 31 days) ──────────
def chunk_range(start: dt.date, end: dt.date, max_days: int = 31) -> list[tuple[dt.date, dt.date]]:
    out, cur = [], start
    while cur <= end:
        nxt = min(cur + dt.timedelta(days=max_days - 1), end)
        out.append((cur, nxt))
        cur = nxt + dt.timedelta(days=1)
    return out


# ── Auth helper for reports endpoints ──────────────────────────────────
def _reports_headers(profile_id: str, token: str) -> dict:
    h = base_headers(profile_id, token)
    h["Content-Type"] = REPORTS_CT
    h["Accept"]       = REPORTS_CT
    return h


# ── Submit one report request ──────────────────────────────────────────
def submit_report(profile_id: str, token: str, rtype_key: str,
                  start: dt.date, end: dt.date, max_retries: int = 6) -> str:
    cfg = REPORT_TYPES[rtype_key]
    body = {
        "name": f"{rtype_key} {start.isoformat()}_{end.isoformat()} p{profile_id}",
        "startDate": start.isoformat(),
        "endDate":   end.isoformat(),
        "configuration": {
            "adProduct":    cfg["adProduct"],
            "groupBy":      cfg["groupBy"],
            "columns":      cfg["columns"],
            "reportTypeId": cfg["reportTypeId"],
            "timeUnit":     cfg["timeUnit"],
            "format":       "GZIP_JSON",
        }
    }
    for attempt in range(max_retries):
        r = requests.post(f"{ADS_API_BASE}/reporting/reports",
                          headers=_reports_headers(profile_id, token),
                          json=body, timeout=60)
        if r.status_code == 429:
            # Amazon SB endpoints throttle aggressively — back off generously
            wait = 10 * (attempt + 1)  # 10, 20, 30, 40, 50, 60s
            time.sleep(wait)
            continue
        if r.status_code >= 400:
            raise RuntimeError(
                f"submit {rtype_key}/{profile_id}/{start}-{end}: "
                f"{r.status_code} {r.text[:400]}"
            )
        return r.json()["reportId"]
    raise RuntimeError(
        f"submit {rtype_key}/{profile_id}/{start}-{end}: 429 (max retries)"
    )


def _clip_start_for_retention(rtype_key: str, start: dt.date) -> dt.date:
    """SD and SB reports only retain ~60 days of data; clip start accordingly.
    SP retains ~95 days so no clip needed there."""
    if rtype_key in ("sd_adprod", "sb_ad", "sb_purchproduct"):
        floor = dt.date.today() - dt.timedelta(days=60)
        if start < floor:
            return floor
    return start


# ── Check + download one report ─────────────────────────────────────────
def check_report(profile_id: str, token: str, report_id: str) -> dict:
    """Returns full status JSON (status, url, failureReason, ...)."""
    r = requests.get(f"{ADS_API_BASE}/reporting/reports/{report_id}",
                     headers=base_headers(profile_id, token), timeout=30)
    if r.status_code >= 400:
        return {"status": "ERROR_HTTP", "failureReason": f"{r.status_code} {r.text[:200]}"}
    return r.json()


def download_report(url: str) -> list[dict]:
    d = requests.get(url, timeout=180)
    raw = gzip.decompress(d.content)
    data = json.loads(raw)
    return data if isinstance(data, list) else []


# ── State persistence ──────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"jobs": []}

def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


# ── Submit all reports for a date range ─────────────────────────────────
def submit_all(start: dt.date, end: dt.date) -> dict:
    token, profiles = get_access_token()
    print(f"🔑 token OK · {len(profiles)} profile(s)")
    chunks = chunk_range(start, end, max_days=31)
    print(f"📅 range {start}..{end} → {len(chunks)} chunk(s) of ≤31 days")

    jobs = []
    for prof in profiles:
        for rtype in REPORT_TYPES.keys():
            for (cs, ce) in chunks:
                try:
                    rid = submit_report(prof["id"], token, rtype, cs, ce)
                    jobs.append({
                        "profile_id": prof["id"],
                        "profile_label": prof["label"],
                        "brand": prof["brand"],
                        "rtype": rtype,
                        "start": cs.isoformat(),
                        "end":   ce.isoformat(),
                        "report_id": rid,
                        "status": "PENDING",
                        "url": None,
                        "failure": None,
                        "rows": 0,
                    })
                    print(f"  ✓ {rtype:<16} {prof['id']:<18} {cs}..{ce}  rid={rid[:8]}…")
                except RuntimeError as e:
                    msg = str(e)
                    jobs.append({
                        "profile_id": prof["id"], "profile_label": prof["label"],
                        "brand": prof["brand"], "rtype": rtype,
                        "start": cs.isoformat(), "end": ce.isoformat(),
                        "report_id": None,
                        "status": "SUBMIT_ERROR",
                        "url": None,
                        "failure": msg[:300],
                        "rows": 0,
                    })
                    print(f"  ✗ {rtype:<16} {prof['id']:<18} {cs}..{ce}  → {msg[:120]}")
                time.sleep(0.3)  # gentle pacing
    state = {"submitted_at": dt.datetime.utcnow().isoformat() + "Z",
             "start": start.isoformat(), "end": end.isoformat(),
             "jobs": jobs}
    save_state(state)
    print(f"\n💾 wrote {STATE_FILE.relative_to(ROOT)}  ({len(jobs)} jobs)")
    return state


# ── Resubmit only the failed jobs from the state file ─────────────────
def resubmit_failed() -> dict:
    state = load_state()
    if not state.get("jobs"):
        print("⚠ no state file — run submit first")
        return state
    token, _ = get_access_token()
    pending_terminal = ("FAILED", "SUBMIT_ERROR", "ERROR_HTTP")
    fail_jobs = [j for j in state["jobs"] if j["status"] in pending_terminal]
    print(f"🔁 resubmitting {len(fail_jobs)} failed job(s)")
    for job in fail_jobs:
        cs = dt.date.fromisoformat(job["start"])
        ce = dt.date.fromisoformat(job["end"])
        # SD retention clip
        cs2 = _clip_start_for_retention(job["rtype"], cs)
        if cs2 > ce:
            job["status"]  = "SKIPPED_RETENTION"
            job["failure"] = f"start {cs} beyond retention window"
            print(f"  ⊘ {job['rtype']:<16} {job['profile_id']:<18} {cs}..{ce}  (out of window)")
            continue
        try:
            rid = submit_report(job["profile_id"], token, job["rtype"], cs2, ce)
            job["report_id"] = rid
            job["start"]     = cs2.isoformat()
            job["status"]    = "PENDING"
            job["failure"]   = None
            job["url"]       = None
            print(f"  ✓ {job['rtype']:<16} {job['profile_id']:<18} {cs2}..{ce}  rid={rid[:8]}…")
        except RuntimeError as e:
            job["status"]  = "SUBMIT_ERROR"
            job["failure"] = str(e)[:300]
            print(f"  ✗ {job['rtype']:<16} {job['profile_id']:<18} {cs2}..{ce}  → {str(e)[:120]}")
        # SB endpoints throttle hard — slow pace
        time.sleep(4.0 if job["rtype"].startswith("sb_") else 1.2)
    save_state(state)
    return state


# ── Poll all submitted reports ─────────────────────────────────────────
def poll_all(max_wait_sec: int = 14400, poll_interval: int = 45) -> dict:
    state = load_state()
    if not state.get("jobs"):
        print("⚠ no state file — run submit first")
        return state
    token, _ = get_access_token()
    deadline = time.time() + max_wait_sec
    while time.time() < deadline:
        pending = [j for j in state["jobs"]
                   if j["status"] in ("PENDING", "PROCESSING")
                   and j.get("report_id")]
        done_ok   = [j for j in state["jobs"] if j["status"] == "COMPLETED"]
        done_fail = [j for j in state["jobs"]
                     if j["status"] in ("FAILED", "SUBMIT_ERROR", "ERROR_HTTP")]
        if not pending:
            print(f"\n✅ all done · {len(done_ok)} ok · {len(done_fail)} failed")
            return state
        print(f"⏱  pending={len(pending)}  ok={len(done_ok)}  fail={len(done_fail)}"
              f"  → polling…")
        for job in pending:
            res = check_report(job["profile_id"], token, job["report_id"])
            st  = res.get("status", "ERROR_HTTP")
            if st in ("PENDING", "PROCESSING"):
                job["status"] = st
                continue
            if st == "COMPLETED":
                job["status"] = "COMPLETED"
                job["url"]    = res.get("url")
            elif st == "FAILED":
                job["status"]  = "FAILED"
                job["failure"] = res.get("failureReason", "")
            else:
                job["status"]  = st
                job["failure"] = res.get("failureReason", "")
        save_state(state)
        time.sleep(poll_interval)
    print(f"⚠ poll deadline reached — some jobs still pending")
    return state


# ── Combine all completed report rows into per-brand DataFrames ───────
def aggregate_to_weekly() -> dict[str, dict[int, dict[str, pd.DataFrame]]]:
    """Returns {brand: {week_num: {sheet_name: df}}}."""
    state = load_state()
    if not state.get("jobs"):
        raise RuntimeError("no state file — submit/poll first")
    token, _ = get_access_token()

    # Pull all rows, attach metadata, then aggregate
    raw_rows: list[dict] = []
    for job in state["jobs"]:
        if job["status"] != "COMPLETED" or not job["url"]:
            continue
        try:
            rows = download_report(job["url"])
        except Exception as e:
            print(f"  ⚠ download {job['report_id'][:8]} ({job['rtype']}): {e}")
            continue
        job["rows"] = len(rows)
        for row in rows:
            row["_rtype"]   = job["rtype"]
            row["_brand"]   = job["brand"]
            row["_profile"] = job["profile_id"]
            raw_rows.append(row)
    save_state(state)

    if not raw_rows:
        print("⚠ no rows from any completed report")
        return {}

    df = pd.DataFrame(raw_rows)
    # Parse date and derive Sun-Sat week
    df["date_d"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date_d"])
    def _wk(d): return sun_sat_week(d)[0]
    df["week"] = df["date_d"].apply(_wk)

    # ─── Coverage check ───────────────────────────────────────────
    # The rolling 14-day API window slides forward each cron run.
    # When the window's start crosses past a Sun-Sat week's Sunday,
    # subsequent pulls return only a 1-6 day fragment of that week,
    # and naively writing that fragment OVERWRITES the previously-
    # correct canonical xlsx with degraded data (W21 lost ~₹3L,
    # W22 lost ~₹6L this way).
    #
    # Fix: for each week, only keep it in the output if EVERY day
    # Sun..Sat is present in the daily pull.  Partial-coverage weeks
    # get skipped entirely — the swap step then leaves the canonical
    # file alone, preserving its full-coverage data from when the
    # week was first pulled.
    fully_covered: set[int] = set()
    for wk_num, sub_dates in df.groupby("week")["date_d"]:
        # Recover the canonical Sun-Sat span for this week from any
        # date in the group (sun_sat_week is consistent).
        any_date = sub_dates.iloc[0]
        _, sun, sat = sun_sat_week(any_date)
        days_seen = set(sub_dates.unique())
        expected  = {sun + dt.timedelta(days=i) for i in range(7)}
        if expected.issubset(days_seen):
            fully_covered.add(int(wk_num))
    skipped = sorted(set(df["week"].astype(int).unique()) - fully_covered)
    if skipped:
        print(f"⚠ skipping partially-covered weeks (preserve canonical): {skipped}")
    df = df[df["week"].astype(int).isin(fully_covered)]
    if df.empty:
        print("⚠ no fully-covered weeks in this window — no xlsx will be written")
        return {}

    # Normalise column names per ad type into a shared schema
    out: dict[str, dict[int, dict[str, pd.DataFrame]]] = defaultdict(lambda: defaultdict(dict))
    for rtype, cfg in REPORT_TYPES.items():
        sub = df[df["_rtype"] == rtype].copy()
        if sub.empty:
            continue
        # Per-report-type normalization
        if rtype == "sp_adprod":
            sub = sub.rename(columns={
                "advertisedAsin": "asin",
                "advertisedSku":  "sku",
                "cost":           "Spend",
                "impressions":    "Impressions",
                "clicks":         "Clicks",
                "purchases7d":    "ams_orders",
                "sales7d":        "attributed_sales",
                "unitsSoldClicks7d": "units_sold",
            })
        elif rtype == "sd_adprod":
            sub = sub.rename(columns={
                "promotedAsin": "asin",
                "promotedSku":  "sku",
                "cost":         "Spend",
                "impressions":  "Impressions",
                "clicks":       "Clicks",
                "purchases":    "ams_orders",
                "sales":        "attributed_sales",
                "unitsSold":    "units_sold",
            })
        elif rtype == "sb_ad":
            sub = sub.rename(columns={
                "cost":          "Spend",
                "impressions":   "Impressions",
                "clicks":        "Clicks",
                "purchases":     "ams_orders",
                "sales":         "attributed_sales",
                "unitsSold":     "units_sold",
            })
        elif rtype == "sb_purchproduct":
            sub = sub.rename(columns={
                "purchasedAsin":         "asin",
                "newToBrandSales14d":    "purch_sales",
                "newToBrandUnitsSold14d":"purch_units",
                "newToBrandPurchases14d":"purch_orders",
            })

        # Aggregate to per (brand × week × key cols)
        if rtype in ("sp_adprod", "sd_adprod"):
            grp = sub.groupby(["_brand", "week", "campaignName", "campaignId",
                               "adGroupName", "adGroupId", "asin", "sku"],
                              as_index=False).agg({
                "Spend": "sum", "Impressions": "sum", "Clicks": "sum",
                "ams_orders": "sum", "attributed_sales": "sum",
                "units_sold": "sum",
            })
        elif rtype == "sb_ad":
            grp = sub.groupby(["_brand", "week", "campaignName", "campaignId",
                               "adGroupName", "adGroupId", "adId"],
                              as_index=False).agg({
                "Spend": "sum", "Impressions": "sum", "Clicks": "sum",
                "ams_orders": "sum", "attributed_sales": "sum",
                "units_sold": "sum",
            })
        elif rtype == "sb_purchproduct":
            grp = sub.groupby(["_brand", "week", "campaignId", "adGroupId", "asin"],
                              as_index=False).agg({
                "purch_sales":  "sum",
                "purch_units":  "sum",
                "purch_orders": "sum",
            })

        for brand, brsub in grp.groupby("_brand"):
            for wk, wkdf in brsub.groupby("week"):
                out[brand][int(wk)][cfg["sheet"]] = wkdf.drop(columns=["_brand", "week"])
    return out


# ── Write per-brand per-week xlsx files ────────────────────────────────
def write_excels(weekly: dict[str, dict[int, dict[str, pd.DataFrame]]],
                 suffix: str = "_api") -> None:
    """Write data/ams_weekly_data/<Brand>/ads_report_week<N><suffix>.xlsx.

    Default `_api` suffix keeps API-sourced files separate from the
    operator-exported `ads_report_week<N>.xlsx` files so we can reconcile
    before swapping over.  Pass suffix="" to overwrite the canonical file.
    """
    brand_folder = {
        "Audio Array":     "Audio_Array",
        "Nexlev":          "Nexlev",
        "White Mulberry":  "White_Mulberry",
        "Tonor":           "Tonor",
        "Fossil":          "Fossil",
    }
    for brand, by_week in weekly.items():
        folder = brand_folder.get(brand, brand.replace(" ", "_"))
        out_dir = OUT_BASE / folder
        out_dir.mkdir(parents=True, exist_ok=True)
        for wk, sheets in by_week.items():
            path = out_dir / f"ads_report_week{wk}{suffix}.xlsx"
            with pd.ExcelWriter(path, engine="openpyxl", mode="w") as xw:
                for sheet_name, sdf in sheets.items():
                    sdf.to_excel(xw, sheet_name=sheet_name, index=False)
            print(f"  📁 {brand:<15} W{wk:>2}  → {path.relative_to(ROOT)}  "
                  f"({sum(len(s) for s in sheets.values())} rows)")


# ── CLI ─────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("submit", help="submit all reports for a date range")
    s.add_argument("--start", required=True, help="YYYY-MM-DD")
    s.add_argument("--end",   required=True, help="YYYY-MM-DD")
    sub.add_parser("poll", help="poll all submitted reports until done")
    sub.add_parser("resubmit", help="resubmit only the failed jobs in the state file")
    sub.add_parser("write", help="aggregate completed reports → xlsx files")
    r = sub.add_parser("run", help="submit → poll → write in one shot")
    r.add_argument("--start", required=True)
    r.add_argument("--end",   required=True)
    args = ap.parse_args()

    if args.cmd in ("submit", "run"):
        s_date = dt.date.fromisoformat(args.start)
        e_date = dt.date.fromisoformat(args.end)
        print(f"━━━ SUBMIT ━━━")
        submit_all(s_date, e_date)
    if args.cmd == "resubmit":
        print(f"━━━ RESUBMIT FAILED ━━━")
        resubmit_failed()
    if args.cmd in ("poll", "run"):
        print(f"\n━━━ POLL ━━━")
        poll_all()
    if args.cmd in ("write", "run"):
        print(f"\n━━━ WRITE ━━━")
        weekly = aggregate_to_weekly()
        write_excels(weekly)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠ interrupted — state is saved, resume with `poll`/`write`")
        sys.exit(130)
