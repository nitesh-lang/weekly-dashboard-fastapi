"""Rolling re-pull of the last few days for every brand.

Why this exists
---------------
GET_SALES_AND_TRAFFIC_REPORT is not final when a date closes — Amazon keeps
filling it in for days afterwards. 18-08-2026 was pulled twice on the morning
of the 20th and landed at ~6% of its real value across four separate seller
accounts; the same request that afternoon returned 9x more, and 19-08 was
still arriving with 19 ASINs against a normal ~100.

Pulling each day exactly once therefore freezes whatever Amazon happened to
have ready. This walks a rolling window instead, so a day that was immature
yesterday is corrected today, and keeps being corrected until it settles.

Safe to run repeatedly: the maturity guard in /pull-sales refuses any re-pull
that comes back materially smaller than what is already stored, so a bad
report late in the window cannot undo a good one.

Schedule it daily (Task Scheduler / cron):
    python scripts/rolling_repull.py

Options:
    --days N      how many closed days back to cover (default 4)
    --end DATE    last day of the window (default: yesterday)
    --brands ...  default: nexlev audio_array
    --sleep SEC   pacing between calls (default 240)
    --retries N   attempts per (day, brand) when Amazon throttles (default 3)
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta

import httpx

API = "https://sales-dashboard-api-23ii.onrender.com"
EMAIL = "info@cambiumretail.com"
PASSWORD = "Cambium@109"


def login(client: httpx.Client, api: str) -> None:
    r = client.post(f"{api}/api/auth/login", json={"email": EMAIL, "password": PASSWORD}, timeout=60)
    if r.status_code != 200:
        sys.exit(f"login failed: HTTP {r.status_code} {r.text[:200]}")


def pull(client: httpx.Client, api: str, brand: str, day: str) -> tuple[bool, str]:
    """Returns (settled, summary). settled=False means worth retrying —
    Amazon throttled or an account came back empty."""
    r = client.post(f"{api}/api/{brand}/pull-sales", params={"date": day}, timeout=900)
    if r.status_code != 200:
        return False, f"HTTP {r.status_code} {r.text[:160]}"
    js = r.json()
    warn = js.get("warnings") or []
    blocked = js.get("guard_blocked_accounts") or []
    bits = [f"rows={js.get('total_rows')}", f"accts={js.get('rows_by_account')}"]
    if blocked:
        bits.append("GUARD=" + ", ".join(
            f"{b['account']} {b['fraction']:.0%} of stored — kept existing" for b in blocked))
    if warn:
        bits.append(f"warn={warn}")
    # A guard block is a correct, final outcome — do not retry it. Warnings
    # that are not guard blocks mean an account failed and is worth another go.
    settled = not [w for w in warn if "kept existing rows" not in str(w)]
    return settled, "  ".join(bits)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default=API)
    ap.add_argument("--days", type=int, default=4)
    ap.add_argument("--end", default=None, help="YYYY-MM-DD, default yesterday")
    ap.add_argument("--brands", nargs="*", default=["nexlev", "audio_array"])
    ap.add_argument("--sleep", type=int, default=240)
    ap.add_argument("--retries", type=int, default=3)
    args = ap.parse_args()

    end = (datetime.strptime(args.end, "%Y-%m-%d").date() if args.end
           else date.today() - timedelta(days=1))
    days = [(end - timedelta(days=i)).isoformat() for i in range(args.days)]
    days.reverse()  # oldest first — the freshest day benefits from the longest wait

    print(f"window: {days[0]} .. {days[-1]}   brands: {', '.join(args.brands)}")

    with httpx.Client(follow_redirects=True) as client:
        login(client, args.api)
        failures: list[str] = []
        for day in days:
            for brand in args.brands:
                for attempt in range(1, args.retries + 1):
                    started = time.time()
                    try:
                        settled, summary = pull(client, args.api, brand, day)
                    except Exception as e:                      # noqa: BLE001
                        settled, summary = False, f"EXC {e}"
                    took = int(time.time() - started)
                    tag = "" if settled else f"  (attempt {attempt}/{args.retries})"
                    print(f"[{brand} {day}] {took}s  {summary}{tag}", flush=True)
                    if settled:
                        break
                    if attempt < args.retries:
                        # Report quota refills over minutes, so back off hard
                        # rather than hammering a throttled account.
                        time.sleep(args.sleep * attempt)
                else:
                    failures.append(f"{brand} {day}")
                time.sleep(args.sleep)

    if failures:
        print(f"\nUNSETTLED after {args.retries} attempts: {', '.join(failures)}")
        print("Amazon was most likely throttling. Re-run later; it is idempotent.")
    print("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
