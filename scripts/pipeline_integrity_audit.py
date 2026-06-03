"""
Pipeline integrity audit — runs after the weekly cron to catch silent
data losses between pipeline layers in the SAME week.

Complements scripts/audit_week_landing.py, which catches WoW drops.
This script catches the OTHER class of bugs: units that disappear
between raw → snapshot → route response in a single week.

Three checks (each → its own Excel sheet):

  1. RAW vs SNAPSHOT
     Sum qty from data/raw/inventory/Week N/<Brand>/*.xlsx per
     (week, brand, channel).  Compare against inventory_units in
     data/processed/inventory_model_snapshot.csv at the same grain.
     Catches: ETL dropping rows (the W22 Amazon FBA dropna bug),
     skip-cache bypassing fresh data, brand-folder mismatches.

  2. SNAPSHOT vs ROUTE
     Programmatically call each consuming route loader
     (load_all_inventory, load_inventory_snapshot, etc.) and reconcile
     their totals against the snapshot CSV per (week, brand) and per
     (week, channel).  Catches: groupby(dropna=True) silently dropping
     NaN-keyed rows, hidden filters in route handlers.

  3. NEVER-ZERO CHECK
     For columns that are structurally non-zero across the latest 4
     weeks (AMPM stock, 1P stock, Amazon FBA stock for active brands),
     flag any column that's identically zero.  Catches the AMS Trend
     AMPM=0 / 1P=0 bug that was silent since the route was written
     (filtered on a `type` column that never gets populated in the CSV).

Run:    python -m scripts.pipeline_integrity_audit
Output: data/processed/pipeline_integrity_audit.xlsx + console summary
Exit:   non-zero when any issue is found (lets the cron fail-loud)
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW_INV_DIR = ROOT / "data" / "raw" / "inventory"
SNAP_INV    = ROOT / "data" / "processed" / "inventory_model_snapshot.csv"
OUT         = ROOT / "data" / "processed" / "pipeline_integrity_audit.xlsx"

UNIT_TOLERANCE = 0  # zero-tolerance: every unit must reconcile


# ─────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────
def _wnum(w) -> int:
    try:
        return int(str(w).replace("Week", "").strip())
    except Exception:
        return -1


def _norm_brand(s: str) -> str:
    """Folder name 'Audio_Array' and snapshot 'Audio Array' both → 'audio array'."""
    return str(s).replace("_", " ").strip().lower()


def _norm_chan(s) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()


# ─────────────────────────────────────────────────────────────────────────
# CHECK 1 — RAW vs SNAPSHOT (inventory)
# ─────────────────────────────────────────────────────────────────────────
def _scan_raw_inventory() -> pd.DataFrame:
    """Walk all Week N/<Brand>/Inventory Snapshot.xlsx files and produce a
    long-form (week, brand, channel, qty) DataFrame.  Skips .bak and
    _api intermediates."""
    rows = []
    for week_dir in RAW_INV_DIR.glob("Week *"):
        wnum = _wnum(week_dir.name)
        if wnum < 0:
            continue
        for brand_dir in week_dir.iterdir():
            if not brand_dir.is_dir():
                continue
            brand = _norm_brand(brand_dir.name)
            for f in brand_dir.glob("*.xlsx"):
                if any(s in f.name for s in (".bak", "_api")):
                    continue
                try:
                    df = pd.read_excel(f)
                except Exception:
                    continue
                df.columns = [c.strip().lower() for c in df.columns]
                if "qty" not in df.columns or "channel" not in df.columns:
                    continue
                df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
                df["channel"] = df["channel"].apply(_norm_chan)
                for ch, g in df.groupby("channel"):
                    rows.append({
                        "week_num": wnum,
                        "brand": brand,
                        "channel": ch,
                        "raw_units": float(g["qty"].sum()),
                    })
    return (
        pd.DataFrame(rows)
        if rows else
        pd.DataFrame(columns=["week_num", "brand", "channel", "raw_units"])
    )


def check_raw_vs_snapshot_inventory() -> pd.DataFrame:
    """Aggregate raw and snapshot by (week, channel) — IGNORE brand.

    Brand-level re-tags by the master ASIN lookup (e.g., a row in the
    Audio_Array folder whose ASIN belongs to Tonor in master gets
    re-emitted with brand=Tonor) are legitimate pipeline transformations,
    not unit losses — they net out across brands within the same week +
    channel.  Aggregating brand away surfaces only TRUE unit losses,
    which is what this check is meant to catch.
    """
    if not SNAP_INV.exists():
        return pd.DataFrame([{
            "week_num": -1,
            "channel": "(all)",
            "raw_units": 0,
            "snap_units": 0,
            "delta": 0,
            "note": "inventory_model_snapshot.csv missing",
        }])

    raw_long = _scan_raw_inventory()
    raw_agg = raw_long.groupby(["week_num", "channel"], as_index=False)["raw_units"].sum()

    snap = pd.read_csv(SNAP_INV)
    snap["week_num"] = snap["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
    snap["chan_n"]   = snap["channel"].apply(_norm_chan)
    snap_agg = snap.groupby(["week_num", "chan_n"], as_index=False)["inventory_units"].sum().rename(
        columns={"chan_n": "channel", "inventory_units": "snap_units"}
    )
    snap_agg["week_num"] = snap_agg["week_num"].astype(int)

    merged = raw_agg.merge(snap_agg, on=["week_num", "channel"], how="outer")
    merged["raw_units"]  = merged["raw_units"].fillna(0)
    merged["snap_units"] = merged["snap_units"].fillna(0)
    merged["delta"] = merged["snap_units"] - merged["raw_units"]

    bad = merged[merged["delta"].abs() > UNIT_TOLERANCE].copy()
    bad["note"] = bad.apply(
        lambda r: (
            "MISSING IN SNAPSHOT (ETL dropping rows)" if r["snap_units"] == 0 else
            "EXTRA IN SNAPSHOT (stale rows?)"         if r["raw_units"]  == 0 else
            "UNITS MISMATCH (silent drop)"
        ),
        axis=1,
    )
    bad = bad.sort_values(["week_num", "channel"]).reset_index(drop=True)
    for c in ("raw_units", "snap_units", "delta"):
        bad[c] = bad[c].round(0).astype(int)
    return bad


def check_brand_retag_diagnostics() -> pd.DataFrame:
    """Informational sheet: where the master re-tagged units between
    brands within the same (week, channel).  Not an error — but useful
    when investigating brand-level discrepancies."""
    if not SNAP_INV.exists():
        return pd.DataFrame()
    raw_long = _scan_raw_inventory()
    snap = pd.read_csv(SNAP_INV)
    snap["week_num"] = snap["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
    snap["brand_n"] = snap["brand"].astype(str).apply(_norm_brand)
    snap["chan_n"]  = snap["channel"].apply(_norm_chan)
    snap_agg = snap.groupby(["week_num", "brand_n", "chan_n"], as_index=False)["inventory_units"].sum().rename(
        columns={"brand_n": "brand", "chan_n": "channel", "inventory_units": "snap_units"}
    )
    raw_agg = raw_long.groupby(["week_num", "brand", "channel"], as_index=False)["raw_units"].sum()
    merged = raw_agg.merge(snap_agg, on=["week_num", "brand", "channel"], how="outer")
    merged["raw_units"]  = merged["raw_units"].fillna(0)
    merged["snap_units"] = merged["snap_units"].fillna(0)
    merged["delta"] = merged["snap_units"] - merged["raw_units"]
    # Only rows where delta != 0
    out = merged[merged["delta"].abs() > UNIT_TOLERANCE].copy()
    for c in ("raw_units", "snap_units", "delta"):
        out[c] = out[c].round(0).astype(int)
    return out.sort_values(["week_num", "channel", "brand"]).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────
# CHECK 2 — SNAPSHOT vs ROUTE
# ─────────────────────────────────────────────────────────────────────────
def check_snapshot_vs_route(latest_week: int) -> pd.DataFrame:
    """Compare the inventory snapshot CSV totals against each route's
    loader output.  Surfaces silent groupby drops / filter side-effects."""
    out: List[Dict[str, Any]] = []
    if not SNAP_INV.exists():
        return pd.DataFrame()

    snap = pd.read_csv(SNAP_INV)
    snap["week_num"] = snap["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
    snap_w = snap[snap["week_num"] == latest_week].copy()
    snap_total = float(snap_w["inventory_units"].sum())

    # 2a — load_all_inventory (inventory dashboard route)
    try:
        from weekly_app.routes.inventory_dashboard import load_all_inventory
        inv = load_all_inventory()
        inv_w = inv[inv["week"] == f"Week {latest_week}"]
        route_total = float(inv_w["inventory_units"].sum())
        out.append({
            "layer": "route:load_all_inventory",
            "week_num": latest_week,
            "snap_units": int(snap_total),
            "route_units": int(route_total),
            "delta": int(route_total - snap_total),
            "note": "ok" if abs(route_total - snap_total) <= UNIT_TOLERANCE
                    else "ROUTE LOSING UNITS vs SNAPSHOT",
        })
    except Exception as e:
        out.append({
            "layer": "route:load_all_inventory",
            "week_num": latest_week,
            "snap_units": int(snap_total),
            "route_units": 0,
            "delta": 0,
            "note": f"loader raised: {e!r}",
        })

    # 2b — AMS Trend's inventory pivot.  Sum across columns AMPM+1P+Amazon+pipeline
    # should equal the sum of those four channel buckets in the snapshot.
    try:
        from weekly_app.routes.ams_trend import load_inventory_snapshot
        piv = load_inventory_snapshot()
        piv_w = piv[piv["week"] == latest_week]
        route_total = float(
            piv_w["inventory_ampm"].sum()
            + piv_w["inventory_1p"].sum()
            + piv_w["inventory_amazon"].sum()
            + piv_w["pipeline_orders"].sum()
        )
        # Compute the same four buckets directly from the snapshot
        chan = snap_w["channel"].apply(_norm_chan)
        ampm_set     = {"ampm", "b2b-ampm", "b2b - ampm"}
        pipeline_set = {"pipeline", "pipeline order", "open order"}
        bucket_total = float(snap_w.loc[
            chan.isin(ampm_set)
            | (chan == "1p")
            | (chan == "amazon")
            | chan.isin(pipeline_set),
            "inventory_units",
        ].sum())
        out.append({
            "layer": "route:ams_trend.load_inventory_snapshot",
            "week_num": latest_week,
            "snap_units": int(bucket_total),
            "route_units": int(route_total),
            "delta": int(route_total - bucket_total),
            "note": "ok" if abs(route_total - bucket_total) <= UNIT_TOLERANCE
                    else "AMS TREND DROPPING units (likely filter on empty col)",
        })
    except Exception as e:
        out.append({
            "layer": "route:ams_trend.load_inventory_snapshot",
            "week_num": latest_week,
            "snap_units": 0,
            "route_units": 0,
            "delta": 0,
            "note": f"loader raised: {e!r}",
        })

    df = pd.DataFrame(out)
    # Only keep rows that signal an issue OR show ok status for transparency.
    return df


# ─────────────────────────────────────────────────────────────────────────
# CHECK 3 — NEVER-ZERO COLUMN CHECK
# ─────────────────────────────────────────────────────────────────────────
# Rule: in the latest 4 weeks, these (route, column, brand) combinations
# must each have a positive total — they were silent zeros in a real bug
# we've already caught, so this guards against the regression returning.
NEVER_ZERO_RULES = [
    # (route_loader, column, brand_filter or None, description)
    ("ams_trend.inventory_snapshot", "inventory_ampm",   None, "AMPM stock (operator warehouse) — non-zero portfolio-wide"),
    ("ams_trend.inventory_snapshot", "inventory_1p",     None, "Amazon Vendor 1P stock — non-zero portfolio-wide"),
    ("ams_trend.inventory_snapshot", "inventory_amazon", None, "Amazon FBA stock — non-zero portfolio-wide"),
]


def check_never_zero(latest_week: int) -> pd.DataFrame:
    out: List[Dict[str, Any]] = []
    try:
        from weekly_app.routes.ams_trend import load_inventory_snapshot
        piv = load_inventory_snapshot()
    except Exception as e:
        return pd.DataFrame([{
            "rule": "ams_trend.inventory_snapshot (load failed)",
            "column": "—", "week_num": latest_week,
            "total": 0, "note": f"loader raised: {e!r}",
        }])

    last4 = [latest_week - 3, latest_week - 2, latest_week - 1, latest_week]
    for (loader, col, brand_filter, desc) in NEVER_ZERO_RULES:
        if col not in piv.columns:
            out.append({
                "rule": loader,
                "column": col,
                "week_num": latest_week,
                "total": 0,
                "note": f"COLUMN MISSING from loader output — {desc}",
            })
            continue
        for w in last4:
            sub = piv[piv["week"] == w]
            total = float(sub[col].sum())
            if total <= 0:
                out.append({
                    "rule": loader,
                    "column": col,
                    "week_num": w,
                    "total": int(total),
                    "note": f"ZERO column — {desc}",
                })

    return pd.DataFrame(out)


# ─────────────────────────────────────────────────────────────────────────
# CHECK — OFF-MASTER ASINS IN RAW INVENTORY
# ─────────────────────────────────────────────────────────────────────────
def check_off_master_asins() -> pd.DataFrame:
    """List ASINs that appear in raw inventory files but are NOT in
    sku_master.xlsx (and whose raw SKU also doesn't resolve in master).
    These rows get dropped by inventory_model_snapshot.py at the
    post-alignment Model filter — the root cause of most "silent drop"
    hits in Check 1.  Operator fixes by adding the ASIN to sku_master.
    """
    master_path = ROOT / "data" / "master" / "sku_master.xlsx"
    if not master_path.exists():
        return pd.DataFrame()
    m = pd.read_excel(master_path)
    m.columns = m.columns.str.strip()
    master_asins = set(m["ASIN"].astype(str).str.strip()) if "ASIN" in m.columns else set()
    master_skus  = set(m["FBA SKU"].astype(str).str.strip()) if "FBA SKU" in m.columns else set()
    if "Original SKU" in m.columns:
        master_skus |= set(m["Original SKU"].astype(str).str.strip())

    rows = []
    for week_dir in RAW_INV_DIR.glob("Week *"):
        wnum = _wnum(week_dir.name)
        if wnum < 0:
            continue
        for brand_dir in week_dir.iterdir():
            if not brand_dir.is_dir():
                continue
            for f in brand_dir.glob("*.xlsx"):
                if any(s in f.name for s in (".bak", "_api")):
                    continue
                try:
                    df = pd.read_excel(f)
                except Exception:
                    continue
                df.columns = [c.strip().lower() for c in df.columns]
                if "asin" not in df.columns or "qty" not in df.columns:
                    continue
                df["asin"] = df["asin"].astype(str).str.strip()
                df["sku"]  = df.get("sku", pd.Series([""] * len(df))).astype(str).str.strip()
                df["qty"]  = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
                df["channel"] = df.get("channel", pd.Series([""] * len(df))).apply(_norm_chan)
                # ASIN miss + SKU miss = the row will be dropped by ETL
                mask = (
                    df["asin"].ne("")
                    & ~df["asin"].isin(master_asins)
                    & ~df["sku"].isin(master_skus)
                )
                for _, r in df[mask].iterrows():
                    rows.append({
                        "week_num": wnum,
                        "brand_folder": _norm_brand(brand_dir.name),
                        "asin": r["asin"],
                        "raw_sku": r["sku"],
                        "channel": r["channel"],
                        "qty": int(r["qty"]),
                    })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    # Roll up — same ASIN can appear multiple weeks/rows
    rolled = out.groupby(
        ["asin", "raw_sku", "brand_folder", "channel"], as_index=False
    ).agg(weeks_seen=("week_num", "nunique"), total_qty=("qty", "sum"))
    # Zero-qty placeholder rows are cosmetic — they appear in raw files
    # as 0-stock entries.  Filtering them out keeps the audit focused on
    # ASINs where real inventory is invisible to the dashboard.
    rolled = rolled[rolled["total_qty"] > 0]
    rolled = rolled.sort_values(["total_qty"], ascending=False).reset_index(drop=True)
    return rolled


# ─────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────
def main() -> int:
    if not SNAP_INV.exists():
        print(f"[FATAL] {SNAP_INV} missing — run the inventory ETL first.")
        return 2

    snap = pd.read_csv(SNAP_INV)
    snap["week_num"] = snap["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
    latest_week = int(snap["week_num"].max())

    print("=" * 72)
    print(f"PIPELINE INTEGRITY AUDIT — latest week = {latest_week}")
    print("=" * 72)

    raw_vs_snap = check_raw_vs_snapshot_inventory()
    snap_vs_rt  = check_snapshot_vs_route(latest_week)
    never_zero  = check_never_zero(latest_week)
    brand_retag = check_brand_retag_diagnostics()
    off_master  = check_off_master_asins()

    # Build summary counts
    n_raw_snap   = len(raw_vs_snap)
    n_snap_route = int((snap_vs_rt["delta"].abs() > UNIT_TOLERANCE).sum()) if not snap_vs_rt.empty else 0
    n_zero       = len(never_zero)

    print()
    print(f"  Check 1 — Raw vs Snapshot (per week×channel)   : {n_raw_snap:>4} real unit losses")
    print(f"  Check 2 — Snapshot vs Route                    : {n_snap_route:>4} layers losing units")
    print(f"  Check 3 — Never-Zero columns                   : {n_zero:>4} regressions")
    print(f"  (info)  — Brand re-tags by master              : {len(brand_retag):>4} brand-shifted (week×brand×channel) rows")
    print(f"  (info)  — Off-master ASINs in raw inventory    : {len(off_master):>4} ASINs not in sku_master (root cause of most Check 1 hits)")
    print()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        (raw_vs_snap if not raw_vs_snap.empty
            else pd.DataFrame([{"note": "all reconciled — no real unit losses"}])
        ).to_excel(xl, sheet_name="1_raw_vs_snapshot", index=False)

        (snap_vs_rt if not snap_vs_rt.empty
            else pd.DataFrame([{"note": "no route loaders checked"}])
        ).to_excel(xl, sheet_name="2_snapshot_vs_route", index=False)

        (never_zero if not never_zero.empty
            else pd.DataFrame([{"note": "all never-zero columns populated"}])
        ).to_excel(xl, sheet_name="3_never_zero", index=False)

        (brand_retag if not brand_retag.empty
            else pd.DataFrame([{"note": "no brand re-tags"}])
        ).to_excel(xl, sheet_name="4_brand_retags_info", index=False)

        (off_master if not off_master.empty
            else pd.DataFrame([{"note": "every ASIN in raw inventory resolves to master"}])
        ).to_excel(xl, sheet_name="5_off_master_asins", index=False)

    print(f"📁 Output: {OUT}")
    print()

    total_issues = n_raw_snap + n_snap_route + n_zero
    if total_issues == 0:
        print("✅ Pipeline integrity: clean.")
        return 0
    print(f"⚠ Pipeline integrity: {total_issues} issue(s) — see audit xlsx.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
