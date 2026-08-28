"""
BuyBox Data Validator — run after generate_data.py
Usage:  python data/validate_data.py
        python data/validate_data.py --strict   (exits with error code if critical issues found)

Checks every brand × month for:
  1. Ad Sales > Net Sales (wrong ads date range)
  2. AMS Orders > Net Units (same root cause)
  3. Buybox % too low (<5% avg — likely double-division bug)
  4. TACOS > 50% (unrealistic, likely bad data)
  5. Decimal values in integer fields (AMS, Impressions, Clicks)
  6. Missing data (0 sessions, 0 sales, 0 ads for a brand-month)
  7. Month-over-month spikes > 500% (likely wrong period)
  8. Negative Organic Sales (ad sales exceeding net at ASIN level)
"""

import json, os, sys

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH  = os.path.join(SCRIPT_DIR, "..", "src", "raw_data.json")
STRICT     = "--strict" in sys.argv

# ── Load ──────────────────────────────────────────────────────────────────────
if not os.path.exists(DATA_PATH):
    print(f"ERROR: {DATA_PATH} not found. Run generate_data.py first.")
    sys.exit(1)

with open(DATA_PATH) as f:
    data = json.load(f)

brands = sorted(set(r["Brand"] for r in data))
months = ["Jan", "Feb", "Mar", "Apr"]

# ── Helpers ───────────────────────────────────────────────────────────────────
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

critical_count = 0
warning_count  = 0

def critical(msg):
    global critical_count
    critical_count += 1
    print(f"  {RED}🔴 CRITICAL:{RESET} {msg}")

def warning(msg):
    global warning_count
    warning_count += 1
    print(f"  {YELLOW}⚠️  WARNING:{RESET}  {msg}")

def passed(msg):
    print(f"  {GREEN}✅ PASS:{RESET}     {msg}")

# ── Run checks ────────────────────────────────────────────────────────────────
print(f"\n{BOLD}{'='*70}{RESET}")
print(f"{BOLD}  BuyBox Data Audit — {len(data)} rows across {len(brands)} brands{RESET}")
print(f"{BOLD}{'='*70}{RESET}\n")

prev_sales = {}

for brand in brands:
    print(f"{BOLD}── {brand} {'─'*(55-len(brand))}{RESET}")

    for month in months:
        rows = [r for r in data if r["Brand"] == brand and r["Month"] == month]
        if not rows:
            continue

        net       = sum(r.get("TotalNetSalesValue", 0) or 0 for r in rows)
        ad_sales  = sum(r.get("TotalAdsSales", 0) or 0 for r in rows)
        ad_spend  = sum(r.get("TotalAdsSpend", 0) or 0 for r in rows)
        units     = sum(r.get("NetUnits", 0) or 0 for r in rows)
        ams       = sum(r.get("AmsOrders", 0) or 0 for r in rows)
        sessions  = sum(r.get("Sessions", 0) or 0 for r in rows)
        bbs       = [r.get("BuyboxPct", 0) or 0 for r in rows if (r.get("BuyboxPct", 0) or 0) > 0]
        avg_bb    = (sum(bbs) / len(bbs) * 100) if bbs else 0
        ratio     = (ad_sales / net * 100) if net > 0 else 0
        tacos     = (ad_spend / net * 100) if net > 0 else 0
        bad_asins = [r for r in rows if (r.get("TotalAdsSales", 0) or 0) > (r.get("TotalNetSalesValue", 0) or 0) and (r.get("TotalAdsSales", 0) or 0) > 0]
        zero_org  = [r for r in rows if (r.get("OrganiSales", 0) or 0) == 0 and (r.get("TotalAdsSales", 0) or 0) > 0]
        dec_ams   = [r for r in rows if r.get("AmsOrders", 0) and r["AmsOrders"] != int(r["AmsOrders"])]
        dec_imp   = [r for r in rows if r.get("Impressions", 0) and r["Impressions"] != int(r["Impressions"])]

        label = f"{month}"
        issues_found = False

        # CHECK 1: Ad Sales > Net Sales
        if ratio > 100:
            critical(f"{label}: Ad Sales (₹{ad_sales:,.0f}) > Net Sales (₹{net:,.0f}) — ratio {ratio:.0f}%. Ads file likely covers wrong date range.")
            issues_found = True

        # CHECK 2: AMS Orders > Net Units
        if ams > units and ams > 0 and units > 0:
            critical(f"{label}: AMS Orders ({ams:,}) > Net Units ({units:,}). Same root cause — ads data period mismatch.")
            issues_found = True

        # CHECK 3: Buybox too low
        if 0 < avg_bb < 5:
            critical(f"{label}: Avg Buybox = {avg_bb:.1f}% (expected 20-90%). Likely XLSX double-division bug in generate_data.py.")
            issues_found = True

        # CHECK 4: TACOS > 50%
        if tacos > 50:
            critical(f"{label}: TACOS = {tacos:.1f}% (expected 10-25%). Data integrity issue.")
            issues_found = True

        # CHECK 5: Decimal integer fields
        if dec_ams:
            warning(f"{label}: {len(dec_ams)} ASINs have decimal AMS Orders (should be whole numbers).")
            issues_found = True
        if dec_imp:
            warning(f"{label}: {len(dec_imp)} ASINs have decimal Impressions.")
            issues_found = True

        # CHECK 6: Missing data
        if sessions == 0 and net == 0:
            warning(f"{label}: Zero sessions and zero sales — data may be missing entirely.")
            issues_found = True
        elif sessions == 0 and net > 0:
            warning(f"{label}: Zero sessions but ₹{net:,.0f} in sales — business report may be missing.")
            issues_found = True

        # CHECK 7: Month-over-month spike
        key = f"{brand}"
        if key in prev_sales and prev_sales[key] > 0 and net > 0:
            pct_change = ((net - prev_sales[key]) / prev_sales[key]) * 100
            if abs(pct_change) > 500:
                warning(f"{label}: Net Sales changed {pct_change:+.0f}% vs previous month (₹{prev_sales[key]:,.0f} → ₹{net:,.0f}). Verify data.")
                issues_found = True
        prev_sales[key] = net

        # CHECK 8: Too many ASINs with zero organic
        if len(zero_org) > len(rows) * 0.5 and len(zero_org) > 5:
            warning(f"{label}: {len(zero_org)}/{len(rows)} ASINs have zero organic sales (ad sales ≥ net sales at ASIN level).")
            issues_found = True

        if not issues_found:
            passed(f"{label}: All checks OK — Net=₹{net:,.0f} BB={avg_bb:.0f}% TACOS={tacos:.1f}% AdRatio={ratio:.0f}%")

    print()

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"{BOLD}{'='*70}{RESET}")
if critical_count == 0 and warning_count == 0:
    print(f"{GREEN}{BOLD}  ALL CLEAR — No issues found across all brands and months.{RESET}")
elif critical_count == 0:
    print(f"{YELLOW}{BOLD}  {warning_count} warning(s) found. No critical issues.{RESET}")
else:
    print(f"{RED}{BOLD}  {critical_count} CRITICAL issue(s) and {warning_count} warning(s) found.{RESET}")
    print(f"{RED}  Fix critical issues before using the dashboard for reporting.{RESET}")
print(f"{BOLD}{'='*70}{RESET}\n")

if STRICT and critical_count > 0:
    sys.exit(1)
