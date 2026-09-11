"""Cohort return curve — what fraction of a month's orders EVENTUALLY come back.

Why this exists
---------------
The reconciliation charges returns on an IN-MONTH basis: refunds posted in
August divided by units shipped in August. Those are different populations. A
refund posted in August mostly belongs to June and July orders, and August's
own returns have barely started arriving. At Rs 2,071 of net cost per return,
every 100 units of mismatch is Rs 2.07L — and the error systematically
FLATTERS a growing month, because the denominator has grown while the
numerator is still being fed by smaller earlier months.

What it does
------------
1. Pulls GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA over a long window.
2. Pulls All-Orders month by month, so every return's order-id can be dated.
3. Keys every return to the month the ORDER was placed, not the month the
   refund posted, and measures the lag in months.
4. Builds the cumulative curve: of everything a cohort will ever return, how
   much has landed by month 0, 1, 2, ...
5. Reads the lifetime rate off the MATURED cohorts only, and uses the curve to
   gross up a young month to its expected lifetime rate.

Cohorts younger than the maturity point are reported but never used to set the
rate — that is the whole point.

    python scripts/sp_returns_cohort.py --account NEXLEV --months 13
"""
from __future__ import annotations

import argparse
import gzip
import io
import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
IN_MKT = "A21TJRUUN4KGV"
CACHE = ROOT / "data" / "raw" / "_cohort"

# A cohort is MATURE when the curve has essentially stopped moving. Amazon.in
# FBA return windows are 10-30 days, plus grace, plus the time to physically
# get the unit back and scan it. Three full months after the order month is
# comfortably past that; it is verified against the real curve below and
# printed, so the assumption is never invisible.
MATURITY_LAG = 3


def _token(account: str) -> str:
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": os.environ[f"SP_REFRESH_TOKEN_{account}"],
        "client_id": os.environ["SP_LWA_CLIENT_ID"],
        "client_secret": os.environ["SP_LWA_CLIENT_SECRET"]}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def _report(account: str, rtype: str, start: str, end: str, tag: str) -> pd.DataFrame:
    """Submit / poll / download one report, cached on disk by tag.

    These reports are slow and the cache is what makes re-running this script
    cheap, so a partial or failed pull must NEVER be written to it.
    """
    CACHE.mkdir(parents=True, exist_ok=True)
    cached = CACHE / f"{account}_{tag}.csv"
    if cached.exists():
        return pd.read_csv(cached, dtype=str)

    tok = _token(account)
    H = {"x-amz-access-token": tok, "Content-Type": "application/json"}
    body = {"reportType": rtype, "marketplaceIds": [IN_MKT],
            "dataStartTime": start, "dataEndTime": end}
    r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports",
                      json=body, headers=H, timeout=30)
    if r.status_code != 202:
        print(f"    {tag}: create failed HTTP {r.status_code} {r.text[:160]}")
        return pd.DataFrame()
    rid = r.json()["reportId"]

    doc_id, st = None, "?"
    for _ in range(100):
        time.sleep(5)
        rr = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rid}",
                          headers={"x-amz-access-token": tok}, timeout=30)
        if rr.status_code != 200:
            continue
        st = rr.json().get("processingStatus")
        if st == "DONE":
            doc_id = rr.json().get("reportDocumentId")
            break
        if st in ("FATAL", "CANCELLED"):
            break
    if not doc_id:
        print(f"    {tag}: {st}")
        return pd.DataFrame()

    doc = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
                       headers={"x-amz-access-token": tok}, timeout=30).json()
    raw = requests.get(doc["url"], timeout=180).content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    text = raw.decode("utf-8", errors="replace")
    if not text.strip():
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(text), sep="\t", dtype=str)
    df.to_csv(cached, index=False)
    print(f"    {tag}: {len(df):,} rows")
    return df


def month_list(months: int, upto: date) -> list[tuple[int, int]]:
    out, y, m = [], upto.year, upto.month
    for _ in range(months):
        out.append((y, m))
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return sorted(out)


def pull_orders(account: str, months: list[tuple[int, int]]) -> pd.DataFrame:
    """All-Orders month by month. One report per month keeps each one small
    enough that Amazon actually produces it."""
    frames = []
    for y, m in months:
        end = date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1)
        df = _report(account, "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
                     f"{date(y, m, 1)}T00:00:00Z", f"{end}T23:59:59Z",
                     f"orders_{y}-{m:02d}")
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="NEXLEV")
    ap.add_argument("--months", type=int, default=13)
    ap.add_argument("--target", default=None, help="YYYY-MM to restate (default: last full month)")
    args = ap.parse_args()
    load_dotenv(ROOT / ".env")

    today = date.today()
    last_full = today.replace(day=1) - timedelta(days=1)
    target = args.target or f"{last_full.year}-{last_full.month:02d}"
    months = month_list(args.months, last_full)
    print(f"=== {args.account} cohort returns: {months[0][0]}-{months[0][1]:02d} "
          f"-> {months[-1][0]}-{months[-1][1]:02d} ===")

    print("  orders...")
    od = pull_orders(args.account, months)
    if od.empty:
        raise SystemExit("no order data - cannot date any return, so no cohort can be built")

    print("  returns...")
    r_start = f"{date(months[0][0], months[0][1], 1)}T00:00:00Z"
    rd = _report(args.account, "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA",
                 r_start, f"{today}T00:00:00Z", f"returns_{args.months}m")
    if rd.empty:
        raise SystemExit("no returns data")

    # ── orders: units SHIPPED per order, and the order's own month ──────
    od["amazon-order-id"] = od["amazon-order-id"].astype(str).str.strip()
    od["qty"] = pd.to_numeric(od["quantity"], errors="coerce").fillna(0)
    od["pdate"] = pd.to_datetime(od["purchase-date"], errors="coerce", utc=True)
    od = od[od["pdate"].notna()]
    od["cohort"] = od["pdate"].dt.strftime("%Y-%m")
    shipped = od[od["item-status"].astype(str).eq("Shipped")]
    cohort_units = shipped.groupby("cohort")["qty"].sum()
    # order-id -> its month. An order is one month by definition.
    o2c = dict(zip(od["amazon-order-id"], od["cohort"]))

    # ── returns: date each one back to its ORDER month ──────────────────
    rd["order-id"] = rd["order-id"].astype(str).str.strip()
    rd["rqty"] = pd.to_numeric(rd["quantity"], errors="coerce").fillna(0)
    rd["rdate"] = pd.to_datetime(rd["return-date"], errors="coerce", utc=True)
    rd = rd[rd["rdate"].notna()]
    rd["cohort"] = rd["order-id"].map(o2c)

    matched = rd[rd["cohort"].notna()].copy()
    orphan_u = float(rd.loc[rd["cohort"].isna(), "rqty"].sum())
    total_u = float(rd["rqty"].sum())
    # MEASURE the join, do not assume it. Orphans are returns whose order
    # predates the order window; they inflate an in-month rate and are exactly
    # what a cohort basis is meant to exclude.
    print(f"  returns matched to an order month: {total_u - orphan_u:,.0f} of "
          f"{total_u:,.0f} units ({(total_u - orphan_u) / total_u * 100:.1f}%); "
          f"{orphan_u:,.0f} units belong to orders older than the window")

    matched["rmonth"] = matched["rdate"].dt.strftime("%Y-%m")
    to_i = lambda s: s.str[:4].astype(int) * 12 + s.str[5:7].astype(int)
    matched["lag"] = to_i(matched["rmonth"]) - to_i(matched["cohort"])
    matched = matched[matched["lag"] >= 0]

    # ── the curve ───────────────────────────────────────────────────────
    piv = (matched.groupby(["cohort", "lag"])["rqty"].sum()
           .unstack(fill_value=0).sort_index())
    piv = piv.reindex(columns=range(0, int(piv.columns.max()) + 1), fill_value=0)
    cum = piv.cumsum(axis=1)
    units = cohort_units.reindex(piv.index).fillna(0)
    rate = cum.div(units, axis=0) * 100

    newest_i = to_i(pd.Series(piv.index)).max()
    age = {c: newest_i - (int(c[:4]) * 12 + int(c[5:7])) for c in piv.index}
    mature = [c for c in piv.index if age[c] >= MATURITY_LAG and units[c] > 0]

    print()
    print("  cumulative return rate by months since the order month (%)")
    print("  cohort    units " + "".join(f"{l:>8}" for l in rate.columns[:7]))
    for c in piv.index:
        flag = "" if c in mature else "  <- still filling"
        print(f"  {c}  {units[c]:7,.0f} "
              + "".join(f"{rate.loc[c, l]:8.2f}" for l in rate.columns[:7]) + flag)

    if not mature:
        raise SystemExit("no cohort is mature enough to read a lifetime rate from")

    lifetime = float(rate.loc[mature, rate.columns.max()].mean())
    # Shape of the curve, from matured cohorts only: what share of lifetime
    # returns has arrived by lag L. This is what grosses up a young month.
    shape = (rate.loc[mature].mean() / lifetime).clip(upper=1.0)

    print()
    print(f"  MATURED cohorts ({', '.join(mature)}): lifetime return rate "
          f"{lifetime:.2f}% of units shipped")
    print("  share of lifetime returns arrived by lag: "
          + ", ".join(f"m{l}={shape[l] * 100:.0f}%" for l in list(shape.index)[:5]))

    if target in rate.index:
        t_age = age[target]
        seen = float(rate.loc[target, min(t_age, rate.columns.max())])
        frac = float(shape[min(t_age, shape.index.max())]) or 1.0
        expected = seen / frac
        print()
        print(f"  {target}: {seen:.2f}% has arrived after {t_age} month(s); that is "
              f"{frac * 100:.0f}% of a cohort's lifetime returns")
        print(f"  {target} EXPECTED LIFETIME RETURN RATE = {expected:.2f}% "
              f"of {units[target]:,.0f} units shipped = {expected / 100 * units[target]:,.0f} units")

    # Two outputs. The curve is for inspection; the SUMMARY is what the
    # reconciliation consumes, and it carries each cohort's AGE - without that
    # a reader cannot tell a matured 4% from a one-month-old 4% that is still
    # filling, which is the entire distinction this script exists to make.
    out = ROOT / "data" / "processed" / f"returns_cohort_{args.account}.csv"
    tidy = rate.reset_index().melt(id_vars="cohort", var_name="lag_months",
                                   value_name="cum_return_pct")
    tidy["units_shipped"] = tidy["cohort"].map(units)
    tidy["age_months"] = tidy["cohort"].map(age)
    tidy["mature"] = tidy["cohort"].isin(mature)
    tidy["account"] = args.account
    tidy.to_csv(out, index=False)

    rows = []
    maxlag = int(rate.columns.max())
    for c in piv.index:
        a = int(age[c])
        obs = float(rate.loc[c, min(a, maxlag)])
        frac = float(shape[min(a, int(shape.index.max()))]) or 1.0
        rows.append({"account": args.account, "cohort": c,
                     "units_shipped": float(units[c]), "age_months": a,
                     "observed_return_pct": round(obs, 4),
                     "share_of_lifetime_arrived": round(frac, 4),
                     "expected_lifetime_return_pct": round(obs / frac, 4),
                     "mature": c in mature,
                     "maturity_lag_months": MATURITY_LAG,
                     "lifetime_rate_from_matured_pct": round(lifetime, 4)})
    summ = ROOT / "data" / "processed" / f"returns_cohort_summary_{args.account}.csv"
    pd.DataFrame(rows).to_csv(summ, index=False)
    print(f"\n-> {out.name}\n-> {summ.name}")


if __name__ == "__main__":
    main()
