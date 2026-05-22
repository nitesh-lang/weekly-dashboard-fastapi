"""
Amazon SOH + Intransit snapshot ETL — reads FBA inventory reports from
`data/raw/inbound/*.csv` (one per brand: WM, Audio Array, Nexlev, Viomi)
and produces a per-ASIN aggregate at `data/processed/inbound_snapshot.csv`.

Each source is a standard Seller-Central FBA "Manage Inventory" export.
Two metrics surfaced to the AMS Planning grid:

    am_intransit = afn-inbound-working-quantity (P)
                 + afn-inbound-shipped-quantity (Q)
        Units en route to Amazon but not yet on the shelf.

    am_soh       = afn-total-quantity        (N)
                 − afn-unsellable-quantity   (L)
        Current Amazon-side sellable stock — total minus unsellable
        (damaged / customer returns awaiting disposition).

Same ASIN can appear in multiple files (e.g. WM.csv and viomi.csv share
Nektar SKUs).  We dedupe by ASIN keeping the row with the highest
combined (am_soh + am_intransit) — the freshest snapshot.

Output schema (one row per ASIN):
    asin, sku, am_soh, am_intransit
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).resolve().parent.parent.parent
RAW_DIR  = ROOT / "data" / "raw" / "inbound"
OUT_FILE = ROOT / "data" / "processed" / "inbound_snapshot.csv"

COL_ASIN       = "asin"
COL_SKU        = "sku"
COL_TOTAL      = "afn-total-quantity"            # column N
COL_UNSELLABLE = "afn-unsellable-quantity"       # column L
COL_INB_WORK   = "afn-inbound-working-quantity"  # column P
COL_INB_SHIP   = "afn-inbound-shipped-quantity"  # column Q


def _read_one(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    needed = {COL_ASIN, COL_SKU, COL_TOTAL, COL_UNSELLABLE, COL_INB_WORK, COL_INB_SHIP}
    have   = set(df.columns)
    if not needed.issubset(have):
        print(f"  ⚠ {path.name}: missing required columns ({needed - have}) — skipping")
        return pd.DataFrame()

    out = df[[COL_ASIN, COL_SKU, COL_TOTAL, COL_UNSELLABLE, COL_INB_WORK, COL_INB_SHIP]].copy()
    out.columns = ["asin", "sku", "_total", "_unsellable", "_p", "_q"]

    out["asin"] = out["asin"].astype(str).str.strip()
    out["sku"]  = out["sku"].astype(str).str.strip()
    out = out[(out["asin"] != "") & (out["asin"].str.lower() != "nan")]

    for c in ("_total", "_unsellable", "_p", "_q"):
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    # AM SOH       = total − unsellable  (current sellable Amazon stock)
    # AM Intransit = inbound working + inbound shipped (P + Q)
    out["am_soh"]       = (out["_total"] - out["_unsellable"]).clip(lower=0).astype(int)
    out["am_intransit"] = (out["_p"] + out["_q"]).astype(int)
    out["source"]       = path.stem
    return out[["asin", "sku", "am_soh", "am_intransit", "source"]]


def run_inbound_etl() -> int:
    if not RAW_DIR.exists():
        print(f"⚠ Inbound raw folder not found at {RAW_DIR}")
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
        print("⚠ No usable inbound rows")
        return 0

    raw = pd.concat(frames, ignore_index=True)
    print(f"   Combined {len(raw):,} rows across {len(frames)} files")

    # Dedupe by ASIN — keep the row with the highest combined inventory
    # (SOH + intransit), which is the freshest non-stale snapshot when the
    # same ASIN appears in multiple brand files (e.g. WM/viomi duplicates).
    raw["_freshness"] = raw["am_soh"] + raw["am_intransit"]
    raw = raw.sort_values("_freshness", ascending=False)
    agg = raw.drop_duplicates(subset=["asin"], keep="first").reset_index(drop=True)
    agg = agg[["asin", "sku", "am_soh", "am_intransit"]]

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(agg):,} ASINs → {OUT_FILE.relative_to(ROOT)}")
    print()
    print(f"   Total AM SOH (sellable Amazon stock): {int(agg['am_soh'].sum()):,}")
    print(f"   Total AM Intransit (P + Q):           {int(agg['am_intransit'].sum()):,}")
    print(f"   ASINs with AM SOH > 0:                {int((agg['am_soh'] > 0).sum()):,}")
    print(f"   ASINs with intransit > 0:             {int((agg['am_intransit'] > 0).sum()):,}")
    return len(agg)


if __name__ == "__main__":
    try:
        run_inbound_etl()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
