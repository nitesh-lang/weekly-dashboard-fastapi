"""
Returns snapshot ETL — reads all FBA Customer Returns CSVs from
`data/raw/returns/*.csv` and produces a tidy per-ASIN aggregate at
`data/processed/returns_snapshot.csv`.

Input schema (Seller Central FBA Customer Returns report — same across
all four files: "Audio Array FBA Returns.csv", "Nexlev.csv", "CRPL.csv",
"viomi.csv"):
    return-date, order-id, sku, asin, fnsku, product-name, quantity,
    fulfillment-center-id, detailed-disposition, reason,
    license-plate-number, customer-comments

The file name doesn't determine the brand — ASIN does.  Each CSV is one
Seller-Central account, but products are scattered across them (e.g.
"viomi.csv" contains Nexlev products).  So we ignore the filename and
look up brand + model via sku_master.xlsx by ASIN.

Also reads `1p Returns.xlsx` (Vendor Central retail analytics export) and
folds its per-ASIN "Customer Returns" count into the total — 1P returns
have no disposition / reason, so they only add to return_units (not the
sellable / unsellable split).

Output schema (one row per ASIN, post-aggregation):
    brand, model, sku, asin, category_l0, category_l1,
    return_units            (FBA + 1P combined)
    returns_3p              (FBA only)
    returns_1p              (Vendor Central Customer Returns)
    return_value            (return_units × NLC from master, best-effort)
    sellable_units          (FBA "Sellable" disposition only)
    unsellable_units        (FBA non-sellable: defective / damaged / etc)
    sellable_pct            (sellable_units / returns_3p, % of FBA returns)
    units_sold_30d          (units sold over the last 12 weeks, all channels —
                             matches the returns report's ~90d window; field
                             name kept for backward compatibility)
    return_pct              (return_units / units_sold_30d × 100, null when 0 sales)
    top_reason              (most common FBA reason code; blank for 1P-only ASINs)
    last_return_at          (max FBA return-date)
    source_files            ("Audio Array FBA Returns; Nexlev; 1P")
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from weekly_app.core.data_norm import normalize_keys

ROOT       = Path(__file__).resolve().parent.parent.parent
RAW_DIR    = ROOT / "data" / "raw" / "returns"
ONEP_FILE  = RAW_DIR / "1p Returns.xlsx"
MASTER     = ROOT / "data" / "master" / "sku_master.xlsx"
OUT_FILE   = ROOT / "data" / "processed" / "returns_snapshot.csv"
SALES_FILE = ROOT / "data" / "processed" / "weekly_sales_snapshot.csv"

# Returns reports cover ~90 days of customer returns.  Sales pipeline is
# weekly, so we use the last 12 weeks (≈84 days) as the denominator —
# matches the returns window without going over.
SALES_WINDOW_WEEKS = 12

# FBA disposition values that count as "still sellable inventory".  Anything
# else (Defective, Customer Damaged, Carrier Damaged, Distributor Damaged,
# Warehouse Damaged) is lost cost.
SELLABLE_DISPOSITIONS = {"sellable"}


def _load_master() -> pd.DataFrame:
    if not MASTER.exists():
        print(f"⚠ sku_master not found at {MASTER} — returns won't be brand-tagged")
        return pd.DataFrame(columns=["asin", "brand", "model",
                                     "category_l0", "category_l1", "sku", "nlc"])
    m = pd.read_excel(MASTER)
    m.columns = m.columns.str.strip()
    normalize_keys(m)
    rename = {
        "FBA SKU":     "sku",
        "ASIN":        "asin",
        "Brand":       "brand",
        "Model":       "model",
        "NLC":         "nlc",
        "category_l0": "category_l0",
        "category_l1": "category_l1",
    }
    m = m.rename(columns={k: v for k, v in rename.items() if k in m.columns})
    keep = ["asin", "sku", "brand", "model", "category_l0", "category_l1", "nlc"]
    keep = [c for c in keep if c in m.columns]
    m = m[keep].copy()
    for c in ["asin", "sku", "brand", "model", "category_l0", "category_l1"]:
        if c in m.columns:
            m[c] = m[c].astype(str).str.strip()
    return m


def _build_sales_30d_by_asin() -> dict[str, int]:
    """{asin → units sold across all channels in the last 4 weeks}.

    Sums sales per SKU from weekly_sales_snapshot, then folds onto ASIN via
    sku_master (one ASIN can have multiple SKUs across FBA/1P accounts).
    """
    if not SALES_FILE.exists() or not MASTER.exists():
        return {}

    df = pd.read_csv(SALES_FILE)
    df.columns = df.columns.str.strip()
    if "week" not in df.columns or "sku" not in df.columns or "units_sold" not in df.columns:
        return {}

    def _wk_num(w):
        try: return int(str(w).replace("Week", "").strip())
        except Exception: return -1

    df["_wk"] = df["week"].apply(_wk_num)
    df = df[df["_wk"] >= 0]
    if df.empty:
        return {}

    last_n = sorted(df["_wk"].unique())[-SALES_WINDOW_WEEKS:]
    print(f"   Sales denominator window: weeks {last_n}")
    df = df[df["_wk"].isin(last_n)].copy()
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce").fillna(0)
    df["sku"]        = df["sku"].astype(str).str.strip()

    by_sku = df.groupby("sku", as_index=False)["units_sold"].sum()

    # SKU → ASIN
    m = pd.read_excel(MASTER)
    m.columns = m.columns.str.strip()
    m = m.rename(columns={"FBA SKU": "sku", "ASIN": "asin"})
    if "sku" not in m.columns or "asin" not in m.columns:
        return {}
    m = m[["sku", "asin"]].copy()
    m["sku"]  = m["sku"].astype(str).str.strip()
    m["asin"] = m["asin"].astype(str).str.strip()
    m = m[(m["sku"] != "") & (m["asin"] != "")]

    joined = by_sku.merge(m, on="sku", how="inner")
    by_asin = joined.groupby("asin", as_index=False)["units_sold"].sum()
    return {row["asin"]: int(row["units_sold"]) for _, row in by_asin.iterrows()}


def _read_1p_returns() -> dict[str, int]:
    """{asin → 1P customer returns count over the report period}.

    The 1P Returns export is a Vendor Central retail analytics report.  Row 0
    contains metadata, so the real header is row 1.  Columns we need: ASIN,
    Customer Returns.
    """
    if not ONEP_FILE.exists():
        print(f"   (no 1P returns file at {ONEP_FILE.name} — skipping 1P)")
        return {}
    try:
        df = pd.read_excel(ONEP_FILE, header=1)
    except Exception as e:
        print(f"   ⚠ Failed to read {ONEP_FILE.name}: {e}")
        return {}
    df.columns = [str(c).strip() for c in df.columns]
    if "ASIN" not in df.columns or "Customer Returns" not in df.columns:
        print(f"   ⚠ {ONEP_FILE.name}: missing ASIN or Customer Returns col — skipping 1P")
        return {}
    df = df[["ASIN", "Customer Returns"]].copy()
    df["ASIN"] = df["ASIN"].astype(str).str.strip()
    df["Customer Returns"] = pd.to_numeric(df["Customer Returns"], errors="coerce").fillna(0)
    df = df[(df["ASIN"] != "") & (df["ASIN"].str.lower() != "nan")]
    df = df.groupby("ASIN", as_index=False)["Customer Returns"].sum()
    out = {row["ASIN"]: int(row["Customer Returns"]) for _, row in df.iterrows() if row["Customer Returns"] > 0}
    print(f"📥 1P Returns: {len(out)} ASINs with returns, {sum(out.values()):,} total units")
    return out


def _read_one(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    expected = {"asin", "sku", "quantity", "detailed-disposition", "reason", "return-date"}
    have = {c.lower() for c in df.columns}
    if not expected.issubset(have):
        print(f"  ⚠ {path.name}: missing required columns ({expected - have}) — skipping")
        return pd.DataFrame()
    # Normalise column names
    df = df.rename(columns={c: c.lower() for c in df.columns})
    df["source_file"] = path.stem
    return df


def run_returns_etl() -> int:
    if not RAW_DIR.exists():
        print(f"⚠ Returns raw folder not found at {RAW_DIR}")
        return 0

    files = sorted([p for p in RAW_DIR.glob("*.csv") if not p.name.startswith("~")])
    if not files:
        print(f"⚠ {RAW_DIR} has no CSVs — skipping")
        return 0

    frames = []
    for f in files:
        print(f"📥 Reading {f.name}…")
        d = _read_one(f)
        if not d.empty:
            frames.append(d)

    if not frames:
        print("⚠ No usable returns rows")
        return 0

    raw = pd.concat(frames, ignore_index=True)
    print(f"   Combined {len(raw):,} return events across {len(frames)} files")

    # Normalize fields
    raw["asin"]                 = raw["asin"].astype(str).str.strip()
    raw["sku"]                  = raw["sku"].astype(str).str.strip()
    raw["quantity"]             = pd.to_numeric(raw["quantity"], errors="coerce").fillna(0)
    raw["detailed-disposition"] = raw["detailed-disposition"].astype(str).str.strip().str.lower()
    raw["reason"]               = raw["reason"].astype(str).str.strip()
    raw["return-date"]          = pd.to_datetime(raw["return-date"], errors="coerce", utc=True)

    # Drop empty ASINs (can't be looked up downstream)
    before = len(raw)
    raw = raw[raw["asin"].str.len() > 0]
    dropped = before - len(raw)
    if dropped:
        print(f"   Dropped {dropped} rows with no ASIN")

    # Flag sellable vs unsellable
    raw["_sellable"] = raw["detailed-disposition"].isin(SELLABLE_DISPOSITIONS).astype(int)

    # Aggregate by ASIN.  We pick SKU as the first one we see — most ASINs
    # have a single SKU but some have multiple FNSKUs across accounts.
    def _agg(group: pd.DataFrame) -> pd.Series:
        sellable   = int((group["quantity"] * group["_sellable"]).sum())
        unsellable = int((group["quantity"] * (1 - group["_sellable"])).sum())
        total_3p   = sellable + unsellable
        # Top reason by event count (not quantity — small returns count too)
        reasons = group["reason"].replace({"nan": ""}).tolist()
        reasons = [r for r in reasons if r]
        top = Counter(reasons).most_common(1)[0][0] if reasons else ""
        sources = sorted({s for s in group["source_file"].dropna().unique() if s})
        return pd.Series({
            "sku":              group["sku"].iloc[0],
            "returns_3p":       total_3p,
            "sellable_units":   sellable,
            "unsellable_units": unsellable,
            "sellable_pct":     round(sellable / total_3p * 100, 1) if total_3p else 0,
            "top_reason":       top,
            "last_return_at":   group["return-date"].max().isoformat() if pd.notna(group["return-date"].max()) else "",
            "source_files":     "; ".join(sources),
        })

    agg = raw.groupby("asin", as_index=False).apply(_agg, include_groups=False).reset_index(drop=True)

    # ── Fold in 1P returns (Vendor Central Customer Returns) ──
    one_p = _read_1p_returns()
    if one_p:
        # Add 1P-only ASINs as new rows (no FBA disposition data for them)
        existing_asins = set(agg["asin"].astype(str))
        one_p_only = [asin for asin in one_p if asin not in existing_asins]
        if one_p_only:
            extra = pd.DataFrame([{
                "asin":             asin,
                "sku":              "",
                "returns_3p":       0,
                "sellable_units":   0,
                "unsellable_units": 0,
                "sellable_pct":     0,
                "top_reason":       "",
                "last_return_at":   "",
                "source_files":     "1P",
            } for asin in one_p_only])
            agg = pd.concat([agg, extra], ignore_index=True)
        agg["returns_1p"] = agg["asin"].map(one_p).fillna(0).astype(int)
        # Mark "1P" in source_files for ASINs that have 1P returns
        agg["source_files"] = agg.apply(
            lambda r: (r["source_files"] + ("; 1P" if r["returns_1p"] > 0 and "1P" not in str(r["source_files"]) else "")).strip("; "),
            axis=1,
        )
    else:
        agg["returns_1p"] = 0

    # Grand total: FBA + 1P
    agg["return_units"] = agg["returns_3p"].astype(int) + agg["returns_1p"].astype(int)

    # Join to master for brand / model / category / NLC
    master = _load_master()
    if not master.empty:
        agg = agg.merge(master[["asin", "brand", "model", "category_l0", "category_l1", "nlc"]],
                        on="asin", how="left")
    else:
        for c in ["brand", "model", "category_l0", "category_l1", "nlc"]:
            agg[c] = ""

    # Best-effort value estimate (units × NLC).  NULL when NLC unknown.
    agg["return_value"] = (agg["return_units"] * agg["nlc"]).where(agg["nlc"].notna()).round(0)

    # Return % = return_units / units_sold_last_4_weeks × 100
    sales_lookup = _build_sales_30d_by_asin()
    agg["units_sold_30d"] = agg["asin"].map(sales_lookup).fillna(0).astype(int)
    agg["return_pct"] = (
        (agg["return_units"] / agg["units_sold_30d"] * 100)
        .where(agg["units_sold_30d"] > 0)
        .round(1)
    )

    # Reorder columns
    out_cols = [
        "brand", "model", "sku", "asin",
        "category_l0", "category_l1",
        "return_units", "returns_3p", "returns_1p", "return_value",
        "sellable_units", "unsellable_units", "sellable_pct",
        "units_sold_30d", "return_pct",
        "top_reason", "last_return_at", "source_files",
    ]
    agg = agg[[c for c in out_cols if c in agg.columns]]

    # Fill string NaNs
    for c in ["brand", "model", "sku", "category_l0", "category_l1", "top_reason", "source_files"]:
        if c in agg.columns:
            agg[c] = agg[c].fillna("").astype(str)

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(agg):,} ASINs → {OUT_FILE.relative_to(ROOT)}")
    by_brand = agg.groupby("brand")["return_units"].sum().sort_values(ascending=False)
    print()
    print("   Returns by brand:")
    for b, n in by_brand.items():
        print(f"     {b:<18} {int(n):>6,} units")
    return len(agg)


if __name__ == "__main__":
    try:
        run_returns_etl()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
