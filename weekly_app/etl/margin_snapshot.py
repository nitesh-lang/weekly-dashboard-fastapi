"""
Margin snapshot ETL — reads per-brand Excel exports from
`data/raw/margin/*.xlsx` and produces a single tidy CSV at
`data/processed/margin_snapshot.csv`.

Input expectation (one file per brand, brand name == filename without .xlsx):
    data/raw/margin/Audio Array.xlsx
    data/raw/margin/Nexlev.xlsx
    data/raw/margin/White Mulberry.xlsx
    ...

Each file has a sheet named "Margin Data" with 42 cols.  We keep the
operationally meaningful columns and drop the rest.

Fossil has no margin file — silently skipped if absent.

Run standalone:
    python weekly_app/etl/margin_snapshot.py

Or via the unified runner:
    python scripts/run_weekly_etl.py margin
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).resolve().parent.parent.parent
RAW_DIR    = ROOT / "data" / "raw" / "margin"
OUT_FILE   = ROOT / "data" / "processed" / "margin_snapshot.csv"
SHEET_NAME = "Margin Data"

# Columns we keep from the source — order = output column order.
KEEP_COLS = [
    "SKU", "ASIN", "Model",
    "Category - L1", "Category - L2",
    "Product Name",
    "Latest FOB",
    "NLC",
    "DP",
    "MRP",
    "BAU Deal SP",
    "BAU SP/MOP(MP)",
    "Gross Margin", "Gross Margin %",
    "Net Margin",   "Net Margin %",
]

# Friendly snake_case names the React app can rely on
RENAME = {
    "SKU":              "sku",
    "ASIN":             "asin",
    "Model":            "model",
    "Category - L1":    "category_l1",
    "Category - L2":    "category_l2",
    "Product Name":     "product_name",
    "Latest FOB":       "latest_fob",
    "NLC":              "nlc",
    "DP":               "dp",
    "MRP":              "mrp",
    "BAU Deal SP":      "bau_deal_sp",
    "BAU SP/MOP(MP)":   "bau_sp_mop",
    "Gross Margin":     "gross_margin",
    "Gross Margin %":   "gross_margin_pct",
    "Net Margin":       "net_margin",
    "Net Margin %":     "net_margin_pct",
}


def _read_one(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME)
    available = [c for c in KEEP_COLS if c in df.columns]
    missing = set(KEEP_COLS) - set(available)
    if missing:
        print(f"  ⚠ {path.name}: missing columns {missing} — they'll be NaN")
    out = df[available].copy()
    for c in missing:
        out[c] = None
    out = out[KEEP_COLS]                       # enforce column order
    out = out.rename(columns=RENAME)
    out["brand"] = path.stem                    # filename (sans .xlsx) IS the brand
    return out


def run_margin_etl() -> int:
    if not RAW_DIR.exists():
        print(f"⚠ No margin folder at {RAW_DIR} — skipping")
        return 0

    files = sorted(RAW_DIR.glob("*.xlsx"))
    if not files:
        print(f"⚠ {RAW_DIR} is empty — skipping")
        return 0

    frames = []
    for f in files:
        if f.name.startswith("~$"):           # Excel lock files
            continue
        print(f"📥 Reading {f.name}…")
        try:
            frames.append(_read_one(f))
        except Exception as exc:
            print(f"  ❌ {f.name} failed: {exc}")

    if not frames:
        print("⚠ No usable files — exiting")
        return 0

    out = pd.concat(frames, ignore_index=True)

    # Final tidy: brand first, then everything else.
    out = out[["brand"] + [c for c in out.columns if c != "brand"]]

    # Drop rows with no SKU AND no Model — they're noise.
    before = len(out)
    out = out[out["sku"].notna() | out["model"].notna()]
    dropped = before - len(out)
    if dropped:
        print(f"🧹 Dropped {dropped} rows with no SKU/Model")

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(out):,} rows → {OUT_FILE.relative_to(ROOT)}")
    print(f"   Brands: {sorted(out['brand'].unique().tolist())}")
    return len(out)


if __name__ == "__main__":
    try:
        run_margin_etl()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
