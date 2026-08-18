"""Build the weekly per-brand `other_channels.xlsx` straight from OrderPilot's OMS.

    python scripts/oms_other_channels.py --week 33

That's the whole process.  Reads OrderPilot's Postgres, pulls the week's
non-Amazon sales, and writes one xlsx per brand into
`data/raw/sales/Week <N>/<Brand>/other_channels.xlsx` in exactly the shape
`sales_auto_etl` already expects.  Add `--dry-run` to print the numbers
without writing.

Decisions baked in (operator, 2026-08-17):
  * Window is **order_date**, converted to IST, Sun-Sat inclusive.
  * **All OMS amounts are already NET of GST** — Sale Amount is
    `line_amount_paise / 100` with NO /1.18.  Do not "fix" this.
  * `amazon` is excluded (it comes from the SP-API pull) and so is `other`.
  * `cancelled` / `returned` are excluded — the house rule, matching
    `routers/ampm_balance.py`.

Two traps this deliberately avoids:
  * **Channel names are mapped, never passed through.** `sales_auto_etl`
    takes the SHEET NAME as the channel string verbatim, so emitting
    OrderPilot's own labels would fork every channel away from ~20 weeks of
    history — and `1p` reaching a sheet name would also break
    `business_report_derive`, which looks up the "1p Sales" tab by name.
  * **The ASIN column is omitted when it would be entirely blank.** A
    present-but-empty ASIN column used to make the whole sheet vanish on
    Linux CI; that is fixed in sales_auto_etl now, but not emitting a dead
    column is still the honest output.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW_SALES = ROOT / "data" / "raw" / "sales"

# OrderPilot's .env is the source of truth for the connection string.
# Override with OMS_DATABASE_URL if the repo ever moves.
ORDERPILOT_ENV = Path(r"D:\OrderPilot by Cambium\orderpilot\backend\.env")

# Sun-Sat anchor: W33 = 2026-08-09 .. 2026-08-15.
_ANCHOR_WEEK, _ANCHOR_SUN = 33, date(2026, 8, 9)

# OMS channel  ->  canonical Weekly sheet name.  Left side must match the
# order_channel enum; right side must match the historical channel strings
# in weekly_sales_snapshot.csv EXACTLY.
CHANNEL_SHEET = {
    "1p":         "1p Sales",
    "b2b":        "B2B",
    "blinkit":    "Blinkit Sales",   # note the suffix; inventory side is bare "Blinkit"
    "flipkart":   "Flipkart",
    "d2c_aa":     "D2C - Audio Array",
    "d2c_nexlev": "D2C - Nexlev",
}

BRAND_FOLDER = {
    "audio_array":    "Audio_Array",
    "nexlev":         "Nexlev",
    "white_mulberry": "White_Mulberry",
    "tonor":          "Tonor",
}

SQL = """
SELECT o.channel::text                        AS channel,
       sm.brand::text                         AS brand,
       sm.internal_sku                        AS sku,
       COALESCE(NULLIF(oi.external_asin, ''), sm.ean, '') AS asin,
       COALESCE(sm.model_code, '')            AS model,
       SUM(oi.qty)                            AS qty,
       SUM(oi.line_amount_paise) / 100.0      AS sale_amount
FROM orders o
JOIN order_items oi ON oi.order_id = o.id
LEFT JOIN sku_master sm ON sm.id = oi.sku_id
WHERE (o.order_date AT TIME ZONE 'Asia/Kolkata')::date BETWEEN %s AND %s
  AND o.status NOT IN ('cancelled', 'returned')
  AND o.channel::text NOT IN ('amazon', 'other')
  -- BLINKIT ONLY: "Fulfilled by Blinkit" is our sell-out and counts as sales;
  -- anything shipped FROM one of our warehouses is stock sent TO Blinkit, which
  -- is a transfer, not a sale — counting it would inflate the channel and then
  -- double-count against the eventual sell-out.  `warehouse_id IS NULL` is the
  -- test because _resolve_is_fba now asks only "does Fulfilled by name one of
  -- OUR warehouses" — Blinkit/FBA/MCF all leave it NULL, AMPM/ANDH/GOR set it.
  -- No other channel splits this way: B2B and D2C ship from our warehouses and
  -- are still genuine sales, so the filter is scoped to blinkit alone.
  AND (o.channel::text <> 'blinkit' OR o.warehouse_id IS NULL)
GROUP BY 1, 2, 3, 4, 5
HAVING SUM(oi.qty) > 0
ORDER BY 1, 2, 3
"""


def week_dates(week: int) -> tuple[date, date]:
    sun = _ANCHOR_SUN + timedelta(weeks=week - _ANCHOR_WEEK)
    return sun, sun + timedelta(days=6)


def database_url() -> str:
    url = os.environ.get("OMS_DATABASE_URL")
    if url:
        return url
    if not ORDERPILOT_ENV.exists():
        sys.exit(f"Can't find OrderPilot .env at {ORDERPILOT_ENV} — set OMS_DATABASE_URL instead.")
    env = dict(re.findall(r"^([A-Z_]+)=(.*)$", ORDERPILOT_ENV.read_text(), re.M))
    if "DATABASE_URL" not in env:
        sys.exit(f"No DATABASE_URL in {ORDERPILOT_ENV}")
    return env["DATABASE_URL"].strip().strip('"').strip("'")


def fetch(week: int) -> pd.DataFrame:
    import psycopg2

    start, end = week_dates(week)
    print(f"OMS window: {start} (Sun) -> {end} (Sat)   [order_date, IST]")
    with psycopg2.connect(database_url()) as con:
        df = pd.read_sql(SQL, con, params=(start, end))
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    df = fetch(args.week)
    if df.empty:
        print("No non-Amazon OMS rows in that window — nothing written.")
        return 1

    unmapped = sorted(set(df["channel"]) - set(CHANNEL_SHEET))
    if unmapped:
        # Loud on purpose.  A silently-skipped channel is the exact failure
        # mode that cost W32 three channels.
        sys.exit(
            f"UNMAPPED CHANNEL(S): {unmapped}\n"
            f"Add them to CHANNEL_SHEET with their canonical Weekly sheet name "
            f"before running again — do NOT let them through unnamed."
        )

    no_brand = df[df["brand"].isna()]
    if not no_brand.empty:
        print(f"WARN: {len(no_brand)} row(s) have no brand (unmapped sku_id) — "
              f"{int(no_brand['qty'].sum())} units, Rs{no_brand['sale_amount'].sum():,.0f}. "
              f"Fix the SKU mapping in OrderPilot; they are NOT written.")
        df = df[df["brand"].notna()]

    df["sheet"] = df["channel"].map(CHANNEL_SHEET)
    grand = 0.0

    for brand, bdf in df.groupby("brand"):
        folder = BRAND_FOLDER.get(brand)
        if not folder:
            print(f"WARN: unknown brand {brand!r} — skipped")
            continue
        out_dir = RAW_SALES / f"Week {args.week}" / folder
        out_path = out_dir / "other_channels.xlsx"

        sheets: dict[str, pd.DataFrame] = {}
        for sheet, sdf in bdf.groupby("sheet"):
            out = sdf[["sku", "asin", "model", "qty", "sale_amount"]].copy()
            out.columns = ["SKU", "ASIN", "Model", "Qty", "Sale Amount"]
            out["Brand"] = brand.replace("_", " ").title()
            # Drop ASIN entirely rather than ship a column of blanks.
            if not out["ASIN"].astype(str).str.strip().any():
                out = out.drop(columns=["ASIN"])
            sheets[sheet] = out

        total = float(bdf["sale_amount"].sum())
        grand += total
        names = ", ".join(f"{s} ({len(d)}r)" for s, d in sorted(sheets.items()))
        print(f"  {folder:<16} {names}   Rs{total:,.0f}")

        if args.dry_run:
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        if out_path.exists():
            print(f"    (overwriting existing {out_path.name})")
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            for sheet, sdf in sorted(sheets.items()):
                sdf.to_excel(w, sheet_name=sheet[:31], index=False)

    print(f"\n{'DRY RUN — nothing written' if args.dry_run else 'WROTE'}   "
          f"grand total Rs{grand:,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
