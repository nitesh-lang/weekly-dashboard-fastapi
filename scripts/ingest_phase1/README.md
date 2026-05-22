# Phase 1 — Ads ingestion (local, isolated)

Pulls Sponsored Products / Sponsored Display / Sponsored Brands reports
from Amazon Ads API for a single week, splits rows into per-brand
xlsx files using `data/master/sku_master.xlsx` as the ASIN → brand
bridge.

## What this DOES NOT do

- ❌ Does not modify any file in `weekly_app/`, `data/processed/`,
  `data/raw/`, or any other live-production location
- ❌ Does not run automatically — you invoke it manually
- ❌ Does not deploy to Render — strictly local execution
- ❌ Does not modify `sku_master.xlsx` (read-only)

## What it DOES

- Writes to `data/raw_phase1/ads/Week N/<Brand>/ads_report_weekN.xlsx`
- Output xlsx columns + sheet names match what your existing manual
  uploads use, so the eventual cutover is a one-line path change
- Logs `⚠ ASIN X not in sku_master` lines whenever it drops a row;
  drops them to `Unassigned/ads_report_weekN.xlsx` for inspection

## Setup (one-time)

1. Make sure you have Python 3.10+:
   ```bash
   python --version    # macOS: python3 --version
   ```
2. From the project root:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Copy the env template and fill in your secrets:
   ```bash
   cp scripts/ingest_phase1/.env.example scripts/ingest_phase1/.env
   # then open scripts/ingest_phase1/.env in an editor and fill values
   ```

## Usage

### Sanity check — does my auth work?
```bash
python -m scripts.ingest_phase1.run_ads --test-auth
```
Should print: `✓ Auth OK. Access token minted, expires in N seconds.`

### Find your profile IDs
If you don't already know the 4 profile IDs, this lists every profile
your token can see:
```bash
python -m scripts.ingest_phase1.run_ads --list-profiles
```
Output looks like:
```
profile_id    country  currency  account_id     account_type   name
123456789012  IN       INR       A1B2C3D4E5     seller         Nexlev
234567890123  IN       INR       A1B2C3D4E5     seller         Audio Array
...
```
Copy the relevant `profile_id` values into your `.env`.

### Run the actual pull
Default (Sunday–Saturday window for the given week number):
```bash
python -m scripts.ingest_phase1.run_ads --week 19 --year 2026
```

If your dashboard's "Week 19" maps to a different 7-day window
(Amazon's Ads console week vs. the dashboard's week may differ),
override the date range explicitly:
```bash
python -m scripts.ingest_phase1.run_ads --week 19 --start 2026-05-04 --end 2026-05-10
```
The `--week` value is only used as the **filename label**
(`ads_report_week19.xlsx`); the date window controls what data is pulled.

Output appears under `data/raw_phase1/ads/Week 19/`:
```
Nexlev/ads_report_week19.xlsx          (sheets: SP, SD, SB)
Audio_Array/ads_report_week19.xlsx
Tonor/ads_report_week19.xlsx
White_Mulberry/ads_report_week19.xlsx  (includes Cambium Retail spend)
Unassigned/ads_report_week19.xlsx      (ASINs not in sku_master)
```
Folder names match the existing `data/ams_weekly_data/<Folder>/`
structure exactly, so the cutover is a literal `mv`.

## Verification checklist

1. Open each per-brand xlsx — confirm SP / SD / SB sheets have rows
2. Sum `Spend` across all files for the week → should match
   what Ads Console shows for that week (within ±0.5%)
3. Open `Unassigned/ads_report_week19.xlsx` — every ASIN listed is
   a gap in `sku_master.xlsx` that needs to be filled
4. Compare a single brand's xlsx vs the file you'd have manually
   uploaded for the same week — column-by-column

## Cutover (when you trust it)

There is NO automatic cutover. When ready, the manual step is:
```bash
# Move (don't copy) the per-brand files to the production raw folder
mv data/raw_phase1/ads/Week\ 19/Nexlev/ads_report_week19.xlsx \
   data/ams_weekly_data/Nexlev/ads_report_week19.xlsx
# ...repeat for other brands
```
Existing `etl/step3_ads_aggregation.py` picks them up unchanged.

## Constraints reminder

This entire folder respects:
- **No modification** of anything under `weekly_app/`, `data/processed/`,
  `data/raw/`, or `data/ams_weekly_data/` (those are live files).
- **Read-only** access to `data/master/sku_master.xlsx`.
- **Write-only** to `data/raw_phase1/` (a new, gitignored directory).
