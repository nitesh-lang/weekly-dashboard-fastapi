"""
Compare a month's per-brand CSVs against a git ref (default HEAD).

Use after re-running any monthly puller to confirm the refresh moved
numbers where you expected (and didn't silently drop brands / ASINs).

    python Scripts/diff_month.py --month 2026-06
    python Scripts/diff_month.py --month 2026-06 --old HEAD~1
    python Scripts/diff_month.py --month 2026-06 --files sales_seller,sales_vendor

Emits a per-brand-per-file table:
  rows old -> new (+/- N)
  sum(units)  old -> new (+/- N)
  sum(sales)  old -> new (+/- N)
  ASINs added / dropped

Reads working-tree files from disk and previous-version files via
`git show <ref>:<path>`.  Non-tracked new files show as "new" (no old
comparison).  Missing new files show as "deleted".
"""
from __future__ import annotations

import argparse
import io
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]

# File-type -> (asin_col, [(label, col)] metrics to sum)
FILE_SPECS = {
    "sales_seller.csv": (
        "(Child) ASIN",
        [("units", "Units Ordered"), ("sales", "Ordered Product Sales")],
    ),
    "sales_vendor.csv": (
        "ASIN",
        [
            ("ordered_units", "Qty"),
            ("ordered_sales", "Sale"),
            ("shipped_units", "ShippedUnits"),
            ("shipped_sales", "ShippedRevenue"),
        ],
    ),
    "ads_sp.csv": (
        "asin",
        [
            ("spend", "Spend"),
            ("impressions", "Impressions"),
            ("clicks", "Clicks"),
            ("attr_sales", "14 Day Total Sales (₹)"),
            ("attr_units", "14 Day Total Units (#)"),
        ],
    ),
    "ads_sd.csv": (
        "asin",
        [
            ("spend", "Spend"),
            ("impressions", "Impressions"),
            ("clicks", "Clicks"),
            ("attr_sales", "14 Day Total Sales (₹)"),
            ("attr_units", "14 Day Total Units (#)"),
        ],
    ),
    "ads_sb_attributed.csv": (
        "asin",
        [
            ("spend", "spend"),
            ("impressions", "impressions"),
            ("clicks", "clicks"),
            ("attr_sales", "attributed_sales"),
            ("attr_units", "units_sold"),
        ],
    ),
}


def git_show(ref: str, rel_path: str) -> pd.DataFrame | None:
    """Return CSV at `ref:rel_path` as DataFrame, or None if not tracked there."""
    result = subprocess.run(
        ["git", "-C", str(REPO), "show", f"{ref}:{rel_path}"],
        capture_output=True,
    )
    if result.returncode != 0:
        return None
    return pd.read_csv(io.BytesIO(result.stdout))


def load_current(abs_path: Path) -> pd.DataFrame | None:
    if not abs_path.exists():
        return None
    return pd.read_csv(abs_path)


def summarise(df: pd.DataFrame | None, metrics: list[tuple[str, str]]) -> dict:
    if df is None:
        return {"rows": None, **{label: None for label, _ in metrics}}
    out = {"rows": len(df)}
    for label, col in metrics:
        out[label] = float(df[col].sum()) if col in df.columns else None
    return out


def fmt_num(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:,.2f}"
    return f"{v:,}"


def fmt_delta(old, new):
    if old is None and new is None:
        return "-"
    if old is None:
        return f"NEW ({fmt_num(new)})"
    if new is None:
        return f"DELETED (was {fmt_num(old)})"
    if isinstance(old, float) or isinstance(new, float):
        d = new - old
        sign = "+" if d >= 0 else ""
        return f"{sign}{d:,.2f}"
    d = new - old
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:,}"


def compare_file(brand_dir: Path, filename: str, month: str, old_ref: str) -> dict | None:
    spec = FILE_SPECS[filename]
    asin_col, metrics = spec
    rel = brand_dir.relative_to(REPO).as_posix() + f"/{filename}"
    abs_path = brand_dir / filename

    new_df = load_current(abs_path)
    old_df = git_show(old_ref, rel)

    if new_df is None and old_df is None:
        return None

    new_stats = summarise(new_df, metrics)
    old_stats = summarise(old_df, metrics)

    new_asins = set(new_df[asin_col].dropna().astype(str)) if (new_df is not None and asin_col in new_df.columns) else set()
    old_asins = set(old_df[asin_col].dropna().astype(str)) if (old_df is not None and asin_col in old_df.columns) else set()

    return {
        "brand": brand_dir.parent.name,
        "file": filename,
        "old": old_stats,
        "new": new_stats,
        "asins_added": sorted(new_asins - old_asins),
        "asins_dropped": sorted(old_asins - new_asins),
    }


def print_diff(results: list[dict]) -> None:
    for r in results:
        header = f"  {r['brand']} / {r['file']}"
        print(header)
        print(f"    rows           {fmt_num(r['old']['rows']):>14} -> {fmt_num(r['new']['rows']):>14}   ({fmt_delta(r['old']['rows'], r['new']['rows'])})")
        for label in r["old"].keys():
            if label == "rows":
                continue
            print(f"    {label:14s} {fmt_num(r['old'][label]):>14} -> {fmt_num(r['new'][label]):>14}   ({fmt_delta(r['old'][label], r['new'][label])})")
        if r["asins_added"]:
            preview = ", ".join(r["asins_added"][:5])
            more = f" (+{len(r['asins_added'])-5} more)" if len(r["asins_added"]) > 5 else ""
            print(f"    +ASIN ({len(r['asins_added'])}): {preview}{more}")
        if r["asins_dropped"]:
            preview = ", ".join(r["asins_dropped"][:5])
            more = f" (+{len(r['asins_dropped'])-5} more)" if len(r["asins_dropped"]) > 5 else ""
            print(f"    -ASIN ({len(r['asins_dropped'])}): {preview}{more}")
        print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="e.g. 2026-06")
    ap.add_argument("--old", default="HEAD", help="git ref for previous version (default HEAD)")
    ap.add_argument(
        "--files",
        default=",".join(FILE_SPECS.keys()),
        help="comma-separated filenames to diff",
    )
    args = ap.parse_args()

    wanted = [f.strip() if f.strip().endswith(".csv") else f"{f.strip()}.csv" for f in args.files.split(",")]
    data_root = REPO / "data"

    brand_dirs = sorted([p for p in data_root.iterdir() if p.is_dir() and (p / args.month).exists()])
    if not brand_dirs:
        print(f"No brand dirs found with month {args.month}")
        return 1

    results = []
    for brand in brand_dirs:
        month_dir = brand / args.month
        for fname in wanted:
            if fname not in FILE_SPECS:
                continue
            r = compare_file(month_dir, fname, args.month, args.old)
            if r is not None:
                results.append(r)

    if not results:
        print(f"No CSVs matched for month {args.month} / files {wanted}")
        return 1

    print(f"Diff — month {args.month} · old={args.old} · new=working tree\n")
    print_diff(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
