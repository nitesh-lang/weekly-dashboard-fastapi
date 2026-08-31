"""
Reviews snapshot ETL — reads Helium-10/Keepa exports from
`data/raw/reviews/*.csv` (one CSV per brand) and produces a tidy
per-ASIN aggregate at `data/processed/reviews_snapshot.csv`.

Input schema (Helium-10 product database export):
    Image, Title, Sales Rank: Current, Sales Rank: 30/90/365 days avg.,
    Sales Rank: Drops last 30 days, Reviews: Rating, Reviews: Rating Count,
    ASIN, Brand, Monthly Sales Trends: Monthly Sold (Last Known)

We keep just ASIN + Brand + the two review fields — everything else is
informational and not needed for the AMS Planning grid.

Output schema (one row per ASIN):
    asin, brand, avg_rating, rating_count
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).resolve().parent.parent.parent
RAW_DIR  = ROOT / "data" / "raw" / "reviews"
# Primary source since 2026-08-31 (operator: "take ratings and review from
# keepa uploaded files — that's the file I used to upload"): the dated
# per-brand Keepa exports the operator uploads via Data → Keepa Upload.
# Same schema as the Helium-10 exports (ASIN / Brand / Reviews: Rating /
# Reviews: Rating Count), but refreshed weekly through the dashboard and
# with the brand guaranteed by the FOLDER name. RAW_DIR remains the
# fallback for any brand without a Keepa folder.
KEEPA_BSR_DIR = ROOT / "buybox_src" / "data" / "BSR"
OUT_FILE = ROOT / "data" / "processed" / "reviews_snapshot.csv"

# Source column names (Helium-10 default headers — quoted with colons).
COL_ASIN         = "ASIN"
COL_BRAND        = "Brand"
COL_RATING       = "Reviews: Rating"
COL_RATING_COUNT = "Reviews: Rating Count"

# lowercased filename stem -> canonical brand string.  The snapshot's brand
# values MUST match the rest of the pipeline exactly; a case variant forks
# the brand in every groupby.  Windows is case-insensitive, so the file on
# disk is `White mulberry.csv` while git tracks `White Mulberry.csv` — the
# stem alone is not trustworthy either.
CANONICAL_BRANDS = {
    "audio array":    "Audio Array",
    "nexlev":         "Nexlev",
    "tonor":          "Tonor",
    "white mulberry": "White Mulberry",
    "fossil":         "Fossil",
}


def _canonical_brand(stem: str) -> str:
    """Map a filename stem onto the canonical brand.  Unknown stems pass
    through title-cased rather than being dropped — a new brand file
    should show up in the snapshot, not vanish silently."""
    return CANONICAL_BRANDS.get(stem.strip().lower(), stem.strip().title())


def _read_one(path: Path) -> pd.DataFrame:
    """Read one brand's Helium-10 export.  Handles the UTF-8 BOM the
    Helium-10 downloader prepends.  Coerces blanks / dashes to NaN."""
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    needed = {COL_ASIN, COL_BRAND, COL_RATING, COL_RATING_COUNT}
    have   = set(df.columns)
    if not needed.issubset(have):
        print(f"  ⚠ {path.name}: missing required columns ({needed - have}) — skipping")
        return pd.DataFrame()

    out = df[[COL_ASIN, COL_BRAND, COL_RATING, COL_RATING_COUNT]].copy()
    out.columns = ["asin", "brand", "avg_rating", "rating_count"]

    # Normalise
    out["asin"]  = out["asin"].astype(str).str.strip()

    # BRAND COMES FROM THE FILENAME, NOT THE COLUMN.
    #
    # This module's contract is one CSV per brand, so `White Mulberry.csv`
    # IS White Mulberry — the filename is operator-controlled and stable.
    # The Brand column is whatever Helium-10 happens to emit, and it is
    # NOT stable: exports on 2026-08-18 and again on 2026-08-24 both wrote
    # `nexlev` and `TONOR` (lowercase / uppercase) where every prior week
    # had `Nexlev` and `Tonor`.  Trusting that column split both brands in
    # two against ~20 weeks of history — the same class as the BIW channel
    # split — and nothing catches it: check 7 (brand_name_consistency)
    # covers the sales and inventory snapshots, not reviews.
    #
    # It also folds strays back where they belong: `White Mulberry.csv`
    # carries one `Coleshome` ASIN (6,748 ratings) which had always been
    # counted as White Mulberry until Helium-10 started labelling it
    # separately.  Filename-sourcing keeps that continuity.
    out["brand"] = _canonical_brand(path.stem)
    out = out[(out["asin"] != "") & (out["asin"].str.lower() != "nan")]

    # Numeric coercion — Helium-10 exports use "-" for missing values
    out["avg_rating"]   = pd.to_numeric(out["avg_rating"].replace({"-": pd.NA}),   errors="coerce")
    out["rating_count"] = pd.to_numeric(out["rating_count"].replace({"-": pd.NA}), errors="coerce")
    return out


def _keepa_sources() -> dict[str, Path]:
    """{canonical brand: newest dated Keepa CSV} from the upload store."""
    out: dict[str, Path] = {}
    if not KEEPA_BSR_DIR.exists():
        return out
    for d in sorted(KEEPA_BSR_DIR.iterdir()):
        if not d.is_dir():
            continue
        dated = sorted(d.glob("*.csv"))          # yy-mm-dd names sort by date
        if dated:
            out[_canonical_brand(d.name)] = dated[-1]
    return out


def run_reviews_etl() -> int:
    keepa = _keepa_sources()

    # One source file per brand: Keepa upload wins; Helium-10 file only for
    # brands the Keepa store doesn't cover — so nothing ever drops out.
    per_brand: dict[str, Path] = {}
    if RAW_DIR.exists():
        for p in sorted(RAW_DIR.glob("*.csv")):
            if not p.name.startswith("~"):
                per_brand[_canonical_brand(p.stem)] = p
    per_brand.update(keepa)

    if not per_brand:
        print(f"⚠ No review sources in {KEEPA_BSR_DIR} or {RAW_DIR} — skipping")
        return 0

    frames = []
    for brand, f in sorted(per_brand.items()):
        src = "keepa" if brand in keepa else "helium10"
        print(f"📥 {brand}: {f.name} [{src}]")
        d = _read_one(f)
        if not d.empty:
            d["brand"] = brand      # folder/filename is the brand authority
            frames.append(d)

    if not frames:
        print("⚠ No usable reviews rows")
        return 0

    raw = pd.concat(frames, ignore_index=True)
    print(f"   Combined {len(raw):,} product rows across {len(frames)} files")

    # Multiple sources sometimes have the same ASIN (different scraping runs).
    # Keep the row with the highest rating_count — that's the freshest snapshot.
    raw = raw.sort_values("rating_count", ascending=False, na_position="last")
    agg = raw.drop_duplicates(subset=["asin"], keep="first").reset_index(drop=True)

    # Order columns + final round-trip
    agg = agg[["asin", "brand", "avg_rating", "rating_count"]]

    # Cast rating_count to int where possible (drops fractional from the few
    # rows that came back as floats).  Keep NaN for missing values.
    agg["rating_count"] = agg["rating_count"].astype("Int64")

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(agg):,} ASINs → {OUT_FILE.relative_to(ROOT)}")
    print()
    print("   Per-brand summary:")
    for b, grp in agg.groupby("brand"):
        n          = len(grp)
        with_rate  = grp["avg_rating"].notna().sum()
        sum_counts = int(grp["rating_count"].fillna(0).sum())
        avg_rate   = grp["avg_rating"].mean() if with_rate else None
        rate_str   = f"{avg_rate:.2f}" if avg_rate is not None else "—"
        print(f"     {b:<18} {n:>4} ASINs | {with_rate:>3} rated | {sum_counts:>7,} total reviews | avg {rate_str}")
    return len(agg)


if __name__ == "__main__":
    try:
        run_reviews_etl()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
