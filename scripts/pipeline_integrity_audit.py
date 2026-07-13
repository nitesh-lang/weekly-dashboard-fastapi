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

import re
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW_INV_DIR = ROOT / "data" / "raw" / "inventory"
SNAP_INV    = ROOT / "data" / "processed" / "inventory_model_snapshot.csv"
SNAP_SALES  = ROOT / "data" / "processed" / "weekly_sales_snapshot.csv"
SNAP_AMS    = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
MASTER_FILE = ROOT / "data" / "master" / "sku_master.xlsx"
OUT         = ROOT / "data" / "processed" / "pipeline_integrity_audit.xlsx"

UNIT_TOLERANCE = 0  # zero-tolerance: every unit must reconcile

# Brands the audit considers "active" — Fossil is excluded everywhere by
# operator rule (no ads campaigns, off the AMS Trend / Amazon+1P pages).
# Any Fossil-driven mismatch is silenced so fail-loud only fires on
# regressions that affect the brands actually being reported on.
ACTIVE_BRANDS  = {"audio array", "nexlev", "tonor", "white mulberry"}
IGNORED_BRANDS = {"fossil"}


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
    """Canonical channel key.  Mirrors the ETL's CHANNEL_CANONICAL but
    folded to lowercase + collapses 'B2B-AMPM' / 'B2B - AMPM' onto one
    key so the audit doesn't double-flag the same channel."""
    if s is None:
        return ""
    raw = str(s).strip().lower()
    # Collapse whitespace around dashes and around spaces
    raw = " ".join(raw.split())
    aliases = {
        "b2b-ampm": "b2b - ampm",
    }
    return aliases.get(raw, raw)


# ─────────────────────────────────────────────────────────────────────────
# CHECK 1 — RAW vs SNAPSHOT (inventory)
# ─────────────────────────────────────────────────────────────────────────
def _scan_raw_inventory() -> pd.DataFrame:
    """Walk all Week N/<Brand>/Inventory Snapshot.xlsx files and produce a
    long-form (week, brand, channel, qty) DataFrame.  Skips .bak and
    _api intermediates and IGNORED_BRANDS folders."""
    rows = []
    for week_dir in RAW_INV_DIR.glob("Week *"):
        wnum = _wnum(week_dir.name)
        if wnum < 0:
            continue
        for brand_dir in week_dir.iterdir():
            if not brand_dir.is_dir():
                continue
            brand = _norm_brand(brand_dir.name)
            if brand in IGNORED_BRANDS:
                continue
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
    snap["brand_n"]  = snap["brand"].astype(str).apply(_norm_brand)
    snap = snap[~snap["brand_n"].isin(IGNORED_BRANDS)]
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
        # Compute the same four buckets directly from the snapshot,
        # using the centralized channel constants the route imports.
        # If channel_buckets.py changes, this check follows automatically.
        from weekly_app.core.channel_buckets import (
            AMAZON_SIDE_CHANNELS, PIPELINE_CHANNELS,
        )
        chan = snap_w["channel"].apply(_norm_chan)
        # Match the route's intentional filter (rows with blank ASIN
        # are excluded from the per-ASIN pivot — pipeline orders for
        # unlisted products).  Robust against every NaN representation
        # by combining .isna() with a lowercase string check; older
        # iteration used only str-based isin which missed pd.NA on
        # newer pandas.
        _is_blank = snap_w["asin"].isna() | (
            snap_w["asin"].astype(str).str.strip().str.lower()
            .isin(["", "nan", "none", "<na>", "n/a"])
        )
        has_asin = ~_is_blank
        bucket_total = float(snap_w.loc[
            (chan.isin(AMAZON_SIDE_CHANNELS) | chan.isin(PIPELINE_CHANNELS))
            & has_asin,
            "inventory_units",
        ].sum())
        # Diagnostic: surface the breakdown in workflow logs so a future
        # delta is interpretable without re-downloading the artifact.
        print(f"[audit-check2b] bucket(chan-only)={int(snap_w.loc[chan.isin(AMAZON_SIDE_CHANNELS)|chan.isin(PIPELINE_CHANNELS),'inventory_units'].sum())} "
              f"bucket(chan+has_asin)={int(bucket_total)} "
              f"route={int(route_total)} blank_asin_rows={int(_is_blank.sum())}")
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
            brand_norm = _norm_brand(brand_dir.name)
            if brand_norm in IGNORED_BRANDS:
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
                        "brand_folder": brand_norm,
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
# CHECK 6 — CHANNEL NAME CASE DRIFT
# ─────────────────────────────────────────────────────────────────────────
def check_channel_case_drift() -> pd.DataFrame:
    """Flag channels whose name casing varies across weeks within the
    inventory snapshot.  W20 storing the 1P channel as 'lowercase 1p'
    while every other week uses '1P' caused 1P units to look like they
    went to zero for one week.  Operator should normalise casing in
    the raw file or the ETL should lowercase at ingest.
    """
    if not SNAP_INV.exists():
        return pd.DataFrame()
    inv = pd.read_csv(SNAP_INV)
    inv["wn"]   = inv["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
    inv["chan_raw"]   = inv["channel"].astype(str)
    inv["chan_norm"]  = inv["chan_raw"].str.strip().str.lower()
    grp = inv.groupby("chan_norm")["chan_raw"].nunique()
    drift_channels = grp[grp > 1].index.tolist()
    out = []
    for ch_norm in drift_channels:
        sub = inv[inv["chan_norm"] == ch_norm]
        for raw, g in sub.groupby("chan_raw"):
            out.append({
                "channel_normalised": ch_norm,
                "channel_raw_value":  raw,
                "weeks":              sorted(g["wn"].dropna().astype(int).unique().tolist()),
                "rows":               len(g),
            })
    return pd.DataFrame(out)


# ─────────────────────────────────────────────────────────────────────────
# CHECK 7 — WENT-TO-ZERO WITH HISTORY
# ─────────────────────────────────────────────────────────────────────────
def check_went_to_zero(latest_week: int) -> pd.DataFrame:
    """Across sales / inventory / ams_trend, flag any (active brand,
    channel/metric) pair that had positive activity in any of the prior
    3 weeks but came up zero this week.  Distinct from never-zero check
    in that the prior baseline is per-row (not global).
    Catches: a brand silently dropping off a channel, an ETL filtering
    bug, a missing raw file for one brand.
    """
    PRIOR = [latest_week - 3, latest_week - 2, latest_week - 1]
    out: List[Dict[str, Any]] = []

    # ── sales ──
    if SNAP_SALES.exists():
        s = pd.read_csv(SNAP_SALES)
        s["wn"] = s["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        s["brand_n"] = s["brand"].astype(str).apply(_norm_brand)
        s = s[s["brand_n"].isin(ACTIVE_BRANDS)]
        s["chan_n"] = s["channel"].astype(str).apply(_norm_chan)
        for (b, ch), g in s.groupby(["brand_n", "chan_n"]):
            prior  = g[g["wn"].isin(PRIOR)]["units_sold"].sum()
            latest = g[g["wn"] == latest_week]["units_sold"].sum()
            if prior > 0 and latest == 0:
                out.append({
                    "layer": "sales",
                    "brand": b,
                    "key":   ch,
                    "metric": "units_sold",
                    "prior_3wk": float(prior),
                    "latest":    float(latest),
                })

    # ── inventory ──
    if SNAP_INV.exists():
        inv = pd.read_csv(SNAP_INV)
        inv["wn"] = inv["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        inv["brand_n"] = inv["brand"].astype(str).apply(_norm_brand)
        inv = inv[inv["brand_n"].isin(ACTIVE_BRANDS)]
        # Normalise channel casing to dodge the same case-drift bug
        # that Check 6 surfaces — this check should not double-fire on it.
        inv["chan_n"] = inv["channel"].astype(str).apply(_norm_chan)
        for (b, ch), g in inv.groupby(["brand_n", "chan_n"]):
            prior  = g[g["wn"].isin(PRIOR)]["inventory_units"].sum()
            latest = g[g["wn"] == latest_week]["inventory_units"].sum()
            if prior > 0 and latest == 0:
                out.append({
                    "layer": "inventory",
                    "brand": b,
                    "key":   ch,
                    "metric": "inventory_units",
                    "prior_3wk": float(prior),
                    "latest":    float(latest),
                })

    # ── ams (business_ads_joined) ──
    if SNAP_AMS.exists():
        ams = pd.read_csv(SNAP_AMS)
        ams["wn"] = pd.to_numeric(ams["week"], errors="coerce").astype("Int64")
        ams["brand_n"] = ams["brand"].astype(str).apply(_norm_brand)
        ams = ams[ams["brand_n"].isin(ACTIVE_BRANDS)]
        for col, label in [("Spend", "ad_spend"),
                           ("attributed_sales", "attributed_sales"),
                           ("gmv", "amazon_gmv"),
                           ("sessions", "sessions")]:
            if col not in ams.columns:
                continue
            for b, g in ams.groupby("brand_n"):
                prior  = pd.to_numeric(g[g["wn"].isin(PRIOR)][col], errors="coerce").fillna(0).sum()
                latest = pd.to_numeric(g[g["wn"] == latest_week][col], errors="coerce").fillna(0).sum()
                if prior > 0 and latest == 0:
                    out.append({
                        "layer": "ams_trend",
                        "brand": b,
                        "key":   "(brand-total)",
                        "metric": label,
                        "prior_3wk": float(prior),
                        "latest":    float(latest),
                    })

    return pd.DataFrame(out).sort_values(["layer", "brand", "key"]).reset_index(drop=True) if out else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────
# CHECK 8 — MASTER COMPLETENESS
# ─────────────────────────────────────────────────────────────────────────
def check_master_completeness() -> pd.DataFrame:
    """sku_master.xlsx hygiene check.  Flags:
      - rows with empty Model (these are SKIPPED by master_lookups,
        so any ASIN linked only to such a row gets dropped silently —
        which is exactly what bit the 5 newly-added Nexlev rows)
      - duplicate ASINs
      - duplicate FBA SKUs
      - rows with empty Brand
    """
    if not MASTER_FILE.exists():
        return pd.DataFrame([{"issue": "MASTER FILE MISSING", "key": "—", "count": 0}])
    m = pd.read_excel(MASTER_FILE)
    m.columns = m.columns.str.strip()
    out: List[Dict[str, Any]] = []

    def _blank(s):
        return s.isna() | (s.astype(str).str.strip().str.lower().isin(["", "nan", "none"]))

    if "Model" in m.columns and "ASIN" in m.columns:
        bad = m[_blank(m["Model"]) & ~_blank(m["ASIN"])]
        for _, r in bad.iterrows():
            out.append({
                "issue":  "EMPTY MODEL — ASIN won't resolve (master_lookups skips this row)",
                "asin":   str(r.get("ASIN", "")).strip(),
                "sku":    str(r.get("FBA SKU", "")).strip(),
                "brand":  str(r.get("Brand", "")).strip(),
            })

    if "Brand" in m.columns and "ASIN" in m.columns:
        bad = m[_blank(m["Brand"]) & ~_blank(m["ASIN"])]
        for _, r in bad.iterrows():
            out.append({
                "issue":  "EMPTY BRAND",
                "asin":   str(r.get("ASIN", "")).strip(),
                "sku":    str(r.get("FBA SKU", "")).strip(),
                "brand":  "",
            })

    if "ASIN" in m.columns:
        asin_s = m["ASIN"].astype(str).str.strip()
        dup = asin_s[asin_s.duplicated(keep=False) & asin_s.ne("") & ~asin_s.str.lower().isin(["nan", "none"])]
        for a, _ in dup.value_counts().items():
            out.append({
                "issue":  f"DUPLICATE ASIN ({dup.value_counts()[a]}× in master)",
                "asin":   a,
                "sku":    "",
                "brand":  "",
            })

    if "FBA SKU" in m.columns:
        sku_s = m["FBA SKU"].astype(str).str.strip()
        dup = sku_s[sku_s.duplicated(keep=False) & sku_s.ne("") & ~sku_s.str.lower().isin(["nan", "none"])]
        for s, _ in dup.value_counts().items():
            out.append({
                "issue":  f"DUPLICATE SKU ({dup.value_counts()[s]}× in master)",
                "asin":   "",
                "sku":    s,
                "brand":  "",
            })

    return pd.DataFrame(out)


# ─────────────────────────────────────────────────────────────────────────
# CHECK 9 — BRAND NAME CONSISTENCY ACROSS SNAPSHOTS
# ─────────────────────────────────────────────────────────────────────────
def check_brand_name_consistency() -> pd.DataFrame:
    """A single brand should be spelled the same way across every
    snapshot.  Catches: 'Audio Array' vs 'audio array' vs 'Audio_Array'
    sneaking past joins that key on exact-match brand strings.
    """
    sources = {
        "sales":     SNAP_SALES,
        "inventory": SNAP_INV,
        "ams":       SNAP_AMS,
    }
    seen: Dict[str, set] = {}
    for label, path in sources.items():
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, usecols=lambda c: c.lower() == "brand")
        except Exception:
            df = pd.read_csv(path)
        if "brand" not in {c.lower() for c in df.columns}:
            continue
        col = next(c for c in df.columns if c.lower() == "brand")
        vals = df[col].dropna().astype(str).str.strip().unique()
        seen[label] = set(vals)

    # Group by lowercase to find casing/space drift
    all_vals = set().union(*seen.values()) if seen else set()
    groups: Dict[str, list] = {}
    for v in all_vals:
        key = _norm_brand(v)
        groups.setdefault(key, []).append(v)

    out: List[Dict[str, Any]] = []
    for canonical, variants in groups.items():
        if len(set(variants)) > 1 and canonical not in IGNORED_BRANDS:
            for v in sorted(set(variants)):
                appears_in = [src for src, s in seen.items() if v in s]
                out.append({
                    "canonical": canonical,
                    "variant_seen": v,
                    "appears_in": ", ".join(appears_in),
                })
    return pd.DataFrame(out)


# ─────────────────────────────────────────────────────────────────────────
# CHECK 10 — LATEST WEEK PRESENCE PER BRAND × LAYER
# ─────────────────────────────────────────────────────────────────────────
def check_latest_week_presence(latest_week: int) -> pd.DataFrame:
    """Every active brand should have rows in every layer for the latest
    week.  If a brand drops out of a layer entirely (e.g., operator
    forgot to upload one brand's inventory file), flag it.
    """
    out: List[Dict[str, Any]] = []

    def _brands_in_layer(path: Path, week_col: str, week_val: int) -> set:
        if not path.exists():
            return set()
        df = pd.read_csv(path)
        if df[week_col].dtype == object:
            # expand=False → returns Series (default expand=True returns
            # a DataFrame, which breaks the boolean mask below)
            wn = df[week_col].astype(str).str.extract(r"(\d+)", expand=False)
            wn = pd.to_numeric(wn, errors="coerce")
        else:
            wn = pd.to_numeric(df[week_col], errors="coerce")
        sub = df[wn == week_val]
        return {_norm_brand(b) for b in sub["brand"].dropna().astype(str).unique()}

    layers = [
        ("sales",     SNAP_SALES, "week"),
        ("inventory", SNAP_INV,   "week"),
        ("ams_trend", SNAP_AMS,   "week"),
    ]
    for layer, path, wcol in layers:
        present = _brands_in_layer(path, wcol, latest_week)
        missing = ACTIVE_BRANDS - present
        # Note: Tonor has no ads-side rows when its biz file is the only
        # source — that's expected per the operator rule and we don't
        # flag here.  But sales + inventory MUST have all four brands.
        if layer == "ams_trend":
            # Tonor's ads land via the AudioArray account; the brand
            # presence in ams_trend is contingent on master re-tagging
            # working.  We DO want it flagged if missing.
            pass
        for b in sorted(missing):
            out.append({
                "layer": layer,
                "brand": b,
                "week":  latest_week,
                "note":  f"MISSING — no rows for {b} in {layer} for W{latest_week}",
            })
    return pd.DataFrame(out)


# ─────────────────────────────────────────────────────────────────────────
# CHECK 11 — SALES RAW vs SNAPSHOT
# ─────────────────────────────────────────────────────────────────────────
def check_raw_vs_snapshot_sales(latest_week: int) -> pd.DataFrame:
    """Reconcile raw sales xlsx files (amazon_sales.xlsx +
    other_channels.xlsx sheets) against weekly_sales_snapshot.csv per
    (week, brand) for the latest 4 weeks.  Catches: sales ETL silently
    dropping rows where SKU isn't canonical in master (groupby on
    NaN-model keys).  Mirrors the inventory raw-vs-snapshot check.
    """
    RAW = ROOT / "data" / "raw" / "sales"
    if not (RAW.exists() and SNAP_SALES.exists()):
        return pd.DataFrame()

    folder_to_brand = {
        "Audio_Array":    "Audio Array",
        "Nexlev":         "Nexlev",
        "Tonor":          "Tonor",
        "White_Mulberry": "White Mulberry",
    }
    weeks_of_interest = {latest_week - 3, latest_week - 2, latest_week - 1, latest_week}

    raw_rows = []
    for week_dir in RAW.glob("Week *"):
        wnum = _wnum(week_dir.name)
        if wnum not in weeks_of_interest:
            continue
        for brand_dir in week_dir.iterdir():
            if not brand_dir.is_dir() or brand_dir.name not in folder_to_brand:
                continue
            brand = folder_to_brand[brand_dir.name]
            # 3P Amazon source: prefer operator's amazon_sales.xlsx when
            # present, else fall back to the cron-pulled Seller Sales
            # (SP-API).xlsx.  Mirror sales_auto_etl's fallback so this
            # audit doesn't false-flag a "snapshot has extra units"
            # diff just because the SP-API file is the canonical source
            # this week.
            amz = brand_dir / "amazon_sales.xlsx"
            sp_seller = brand_dir / "Seller Sales (SP-API).xlsx"
            amazon_source = amz if amz.exists() else (sp_seller if sp_seller.exists() else None)
            if amazon_source is not None:
                try:
                    df = pd.read_excel(amazon_source)
                    cols = {c.lower().strip(): c for c in df.columns}
                    u = cols.get("units ordered") or cols.get("units_ordered")
                    if u:
                        raw_rows.append({
                            "week": wnum, "brand": brand,
                            "raw_units": float(pd.to_numeric(df[u], errors="coerce").fillna(0).sum()),
                        })
                except Exception:
                    pass
            # SP-API 1P sales: when present + non-empty, it is the
            # canonical 1P raw source.  Mirror sales_auto_etl: count
            # SP-API units AND skip the "1p Sales" sheet in
            # other_channels.xlsx (shadowed by SP-API).
            sp_sales = brand_dir / "Vendor Sales (SP-API).xlsx"
            sp_owns_1p = False
            if sp_sales.exists():
                try:
                    spd = pd.read_excel(sp_sales)
                    if "Qty" in spd.columns:
                        sp_units = float(pd.to_numeric(spd["Qty"], errors="coerce").fillna(0).sum())
                        if sp_units > 0:
                            raw_rows.append({
                                "week": wnum, "brand": brand,
                                "raw_units": sp_units,
                            })
                            sp_owns_1p = True
                except Exception:
                    pass
            oc = brand_dir / "other_channels.xlsx"
            if oc.exists():
                try:
                    xl = pd.ExcelFile(oc)
                    for sh in xl.sheet_names:
                        if sp_owns_1p and sh.strip().lower() in {"1p sales", "1p", "amazon 1p", "amazon 1p sales"}:
                            continue  # shadowed by SP-API canonical
                        d = pd.read_excel(oc, sheet_name=sh)
                        cols = {c.lower().strip(): c for c in d.columns}
                        u = cols.get("units sold") or cols.get("units_sold") or cols.get("qty") or cols.get("units")
                        if u:
                            raw_rows.append({
                                "week": wnum, "brand": brand,
                                "raw_units": float(pd.to_numeric(d[u], errors="coerce").fillna(0).sum()),
                            })
                except Exception:
                    pass

    if not raw_rows:
        return pd.DataFrame()
    raw_agg = pd.DataFrame(raw_rows).groupby(["week", "brand"], as_index=False).agg(raw_units=("raw_units", "sum"))

    snap = pd.read_csv(SNAP_SALES)
    snap["wn"] = snap["week"].astype(str).str.extract(r"(\d+)", expand=False)
    snap["wn"] = pd.to_numeric(snap["wn"], errors="coerce").astype("Int64")
    snap = snap[snap["wn"].isin(weeks_of_interest)]
    snap = snap[snap["brand"].isin(folder_to_brand.values())]
    snap_agg = snap.groupby(["wn", "brand"], as_index=False).agg(snap_units=("units_sold", "sum")).rename(columns={"wn": "week"})
    snap_agg["week"] = snap_agg["week"].astype(int)
    raw_agg["week"] = raw_agg["week"].astype(int)

    m = raw_agg.merge(snap_agg, on=["week", "brand"], how="outer").fillna(0)
    m["delta"] = (m["snap_units"] - m["raw_units"]).round(0).astype(int)
    # Per-(brand × week) tolerance: ignore <=5 units OR <=1% drift —
    # operator's raw exports routinely have 1-2 unit rounding noise
    # between amazon_sales.xlsx column sums and what aggregation
    # produces in the snapshot.  Real ETL drops (e.g., the W22 master-
    # alignment regression) still surface — those dropped tens or
    # hundreds of units, well above this tolerance.
    raw_safe = m["raw_units"].replace(0, 1)
    pct_drift = (m["delta"].abs() / raw_safe)
    bad = m[(m["delta"].abs() > 5) & (pct_drift > 0.01)].copy()
    for c in ("raw_units", "snap_units"):
        bad[c] = bad[c].round(0).astype(int)
    bad["note"] = bad["delta"].apply(
        lambda d: "ROWS LOST (ETL drop)" if d < 0 else "EXTRA IN SNAP (stale)"
    )
    return bad.sort_values(["week", "brand"]).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────
def check_cross_route_inventory_consistency(latest_week: int) -> pd.DataFrame:
    """For any (model × week), the two routes that show Amazon-side
    stock must show the SAME number.  If they diverge, the channel
    filter rules have drifted apart — exactly the class of bug where
    AMS Trend was summing AMPM + B2B-AMPM but Amazon+1P was summing
    AMPM only, showing 345 vs 314 for ST-01 W22.

    Pairs checked:
      A) /amazon-sales-trend load_inventory(model)
         vs
         /ams-trend load_inventory_snapshot() inventory_total_amazon
            summed across that model's ASINs

      B) /sales-trend load_inventory(brand, model)  (total stock)
         vs
         inventory_model_snapshot per-(brand,model) sum  (total stock)
    """
    out: list[Dict[str, Any]] = []

    # Pair A: Amazon+1P trend ↔ AMS Trend
    try:
        from weekly_app.routes.AM_sales_trend import load_inventory as ams_p_load
        from weekly_app.routes.ams_trend import load_inventory_snapshot as ams_trend_load
        am1p = ams_p_load(latest_week) or {}
        piv  = ams_trend_load()
        piv_w = piv[piv["week"] == latest_week] if not piv.empty else pd.DataFrame()
        if not piv_w.empty:
            ams_per_model = (
                piv_w.groupby("Model")["inventory_total_amazon"].sum().astype(int).to_dict()
            )
            all_models = set(am1p.keys()) | set(ams_per_model.keys())
            for m in sorted(all_models):
                v_a = int(am1p.get(m, 0))
                v_b = int(ams_per_model.get(m, 0))
                if v_a != v_b:
                    out.append({
                        "pair": "amazon+1p_vs_ams_trend",
                        "key":  m,
                        "amazon+1p_inventory":   v_a,
                        "ams_trend_total_amazon": v_b,
                        "delta":                 v_b - v_a,
                        "note":                  "AMAZON-SIDE RULE DRIFTED between the two routes",
                    })
    except Exception as e:
        out.append({
            "pair": "amazon+1p_vs_ams_trend",
            "key":  "(load failed)",
            "amazon+1p_inventory": 0,
            "ams_trend_total_amazon": 0,
            "delta": 0,
            "note":  f"loader raised: {e!r}",
        })

    # Pair B: Sales Trend ↔ Inventory Dashboard total
    try:
        from weekly_app.routes.sales_trend import load_inventory as st_load
        st_map = st_load(latest_week)  # {(brand, model): units}
        if SNAP_INV.exists():
            inv = pd.read_csv(SNAP_INV)
            inv["wn"] = inv["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
            inv_w = inv[inv["wn"] == latest_week].copy()
            inv_w["brand_l"] = inv_w["brand"].astype(str).str.strip().str.lower()
            inv_w["model_u"] = inv_w["model"].astype(str).str.strip().str.upper()
            dash_per_bm = (
                inv_w.groupby(["brand_l", "model_u"])["inventory_units"].sum().astype(int).to_dict()
            )
            # Sales Trend excludes Fossil; reconcile against non-Fossil rows only
            all_keys = {k for k in dash_per_bm.keys() if k[0] != "fossil"} | set(st_map.keys())
            for (b, m) in sorted(all_keys):
                v_s = int(st_map.get((b, m), 0))
                v_d = int(dash_per_bm.get((b, m), 0))
                if v_s != v_d:
                    out.append({
                        "pair": "sales_trend_vs_inv_dashboard",
                        "key":  f"{b} / {m}",
                        "sales_trend_inv":    v_s,
                        "inv_dashboard_total": v_d,
                        "delta":              v_d - v_s,
                        "note":               "TOTAL-STOCK RULE DRIFTED between the two routes",
                    })
    except Exception as e:
        out.append({
            "pair": "sales_trend_vs_inv_dashboard",
            "key":  "(load failed)",
            "sales_trend_inv": 0,
            "inv_dashboard_total": 0,
            "delta": 0,
            "note":  f"loader raised: {e!r}",
        })

    return pd.DataFrame(out)


def check_cross_route_sales_consistency(latest_week: int) -> pd.DataFrame:
    """For each (ASIN × week), Amazon-side GMV (Amazon 3P + 1P
    combined) should match between:
      - AMS Trend `gmv` column  (sourced from business_ads_joined.csv,
                                  which already carries 3P + 1P per
                                  business_report_derive)
      - Sales Trend Amazon + "1p Sales" channel rows summed per ASIN
                                 (sourced from weekly_sales_snapshot.csv)

    Both should land on the same number.  Tolerance ₹10/ASIN guards
    against rounding noise but flags any real drift.
    """
    TOLERANCE_RS = 10.0
    AMAZON_SIDE_SALES_CHANNELS = {"amazon", "1p sales"}
    out: list[Dict[str, Any]] = []
    try:
        if not (SNAP_SALES.exists() and SNAP_AMS.exists()):
            return pd.DataFrame()
        s = pd.read_csv(SNAP_SALES)
        s["wn"] = s["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        s["chan_n"] = s["channel"].astype(str).str.strip().str.lower()
        s = s[(s["wn"] == latest_week) & (s["chan_n"].isin(AMAZON_SIDE_SALES_CHANNELS))]
        if s.empty:
            return pd.DataFrame()
        s["asin"] = s["asin"].astype(str).str.strip()
        s_per_asin = s.groupby("asin")["gross_sales"].sum().to_dict()

        a = pd.read_csv(SNAP_AMS)
        a["wn"] = pd.to_numeric(a["week"], errors="coerce").astype("Int64")
        a = a[a["wn"] == latest_week]
        a["asin"] = a["asin"].astype(str).str.strip()
        a_per_asin = a.groupby("asin")["gmv"].sum().to_dict()

        all_asins = set(s_per_asin) | set(a_per_asin)
        for asin in sorted(all_asins):
            if not asin or asin in ("nan", "__SB__"):
                continue
            v_s = float(s_per_asin.get(asin, 0.0))
            v_a = float(a_per_asin.get(asin, 0.0))
            delta = round(v_a - v_s, 2)
            if abs(delta) > TOLERANCE_RS:
                out.append({
                    "asin":                     asin,
                    "sales_trend_amzn+1p_gmv":  round(v_s, 2),
                    "ams_trend_gmv":            round(v_a, 2),
                    "delta_rs":                 delta,
                    "note": "AMAZON+1P GMV drifts between sales_snapshot and business_ads_joined",
                })
    except Exception as e:
        out.append({
            "asin": "(load failed)", "sales_trend_amazon_gmv": 0,
            "ams_trend_gmv": 0, "delta_rs": 0,
            "note": f"loader raised: {e!r}",
        })
    return pd.DataFrame(out)


def check_cross_route_ads_consistency(latest_week: int) -> pd.DataFrame:
    """For each (brand × week), ad spend on Main Dashboard (per-brand
    KPI, summed across that brand's rows in business_ads_joined) should
    equal AMS Trend's per-ASIN ad_spend summed within the brand.

    Both sources read from the SAME file (business_ads_joined.csv) so
    any drift = a real bug in one of the routes — flag at ≥ ₹1.
    """
    TOLERANCE_RS = 1.0
    out: list[Dict[str, Any]] = []
    try:
        if not SNAP_AMS.exists():
            return pd.DataFrame()
        a = pd.read_csv(SNAP_AMS)
        a["wn"] = pd.to_numeric(a["week"], errors="coerce").astype("Int64")
        a = a[a["wn"] == latest_week]
        if a.empty:
            return pd.DataFrame()
        a["brand_n"] = a["brand"].astype(str).apply(_norm_brand)
        # Pull spend per brand from the raw file directly (this is the
        # truth-source both routes read).  If both routes apply the
        # same filter and aggregation, they should both land here.
        per_brand = a.groupby("brand_n")["Spend"].sum().round(2).to_dict()

        # Now ask each route what it would report for the same brand.
        from weekly_app.routes.ams_trend import load_ams_data
        ams_df = load_ams_data()  # excludes Fossil, lowercased brand
        ams_df_w = ams_df[ams_df["week"] == latest_week]
        ams_per_brand = ams_df_w.groupby("brand")["ad_spend"].sum().round(2).to_dict()

        # Main Dashboard's spend is computed inline; replicate the same
        # filter (Fossil-exclude not applied in dashboard) to compare.
        for brand in sorted(set(per_brand) | set(ams_per_brand)):
            if brand in ("(unmapped)", ""):
                continue
            v_file = float(per_brand.get(brand, 0.0))
            v_ams  = float(ams_per_brand.get(brand, 0.0))
            delta = round(v_ams - v_file, 2)
            if abs(delta) > TOLERANCE_RS:
                out.append({
                    "brand":               brand,
                    "raw_file_spend":      v_file,
                    "ams_trend_spend":     v_ams,
                    "delta_rs":            delta,
                    "note": "ADS SPEND drift between raw business_ads_joined and AMS Trend route",
                })
    except Exception as e:
        out.append({
            "brand": "(load failed)", "raw_file_spend": 0,
            "ams_trend_spend": 0, "delta_rs": 0,
            "note": f"loader raised: {e!r}",
        })
    return pd.DataFrame(out)


def check_cross_route_sessions_consistency(latest_week: int) -> pd.DataFrame:
    """For each (ASIN × week), `sessions` should match between
    AMS Trend (load_ams_data) and Amazon+1P trend (load_business).
    Both pull from business_ads_joined.csv — any drift = a real
    route processing bug.  Tolerance: 1 session.
    """
    TOLERANCE = 1.0
    out: list[Dict[str, Any]] = []
    try:
        if not SNAP_AMS.exists():
            return pd.DataFrame()
        a = pd.read_csv(SNAP_AMS)
        a["wn"] = pd.to_numeric(a["week"], errors="coerce").astype("Int64")
        a = a[a["wn"] == latest_week].copy()
        if a.empty:
            return pd.DataFrame()
        # AMS Trend route excludes Fossil per operator rule; mirror
        # that filter here so the audit doesn't flag Fossil ASINs that
        # are correctly absent from the route output.
        a["brand_n"] = a["brand"].astype(str).apply(_norm_brand)
        a = a[a["brand_n"] != "fossil"]
        a["asin"] = a["asin"].astype(str).str.strip()
        a_per_asin = a.groupby("asin")["sessions"].sum().to_dict()

        # Pull what AMS Trend's route loader would emit
        from weekly_app.routes.ams_trend import load_ams_data
        ams = load_ams_data()
        ams_w = ams[ams["week"] == latest_week]
        ams_per_asin = ams_w.groupby("asin")["sessions"].sum().to_dict() if not ams_w.empty else {}

        # Amazon+1P trend reads via load_business; it pivots per-(model)
        # not per-(asin), so we reconcile at ASIN level using the file
        # source directly — drift here means AMS Trend route mangled it.
        for asin in sorted(set(a_per_asin) | set(ams_per_asin)):
            if not asin or asin in ("nan", "__SB__"):
                continue
            v_file = float(a_per_asin.get(asin, 0.0))
            v_ams  = float(ams_per_asin.get(asin, 0.0))
            delta = round(v_ams - v_file, 2)
            if abs(delta) > TOLERANCE:
                out.append({
                    "asin": asin,
                    "file_sessions": v_file,
                    "ams_trend_sessions": v_ams,
                    "delta": delta,
                    "note": "SESSIONS drift between business_ads_joined and AMS Trend route",
                })
    except Exception as e:
        out.append({
            "asin": "(load failed)", "file_sessions": 0,
            "ams_trend_sessions": 0, "delta": 0,
            "note": f"loader raised: {e!r}",
        })
    return pd.DataFrame(out)


def check_cross_route_units_consistency(latest_week: int) -> pd.DataFrame:
    """For each (ASIN × week), Amazon-side units (Amazon 3P + 1P) should
    match between AMS Trend.units and Sales Trend Amazon + "1p Sales"
    channels summed per ASIN.  Same Amazon-side scope as Check 13 but
    for units instead of GMV.  Tolerance: 1 unit.
    """
    TOLERANCE = 1.0
    AMAZON_SIDE_SALES_CHANNELS = {"amazon", "1p sales"}
    out: list[Dict[str, Any]] = []
    try:
        if not (SNAP_SALES.exists() and SNAP_AMS.exists()):
            return pd.DataFrame()
        s = pd.read_csv(SNAP_SALES)
        s["wn"] = s["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        s["chan_n"] = s["channel"].astype(str).str.strip().str.lower()
        s = s[(s["wn"] == latest_week) & (s["chan_n"].isin(AMAZON_SIDE_SALES_CHANNELS))]
        if s.empty:
            return pd.DataFrame()
        s["asin"] = s["asin"].astype(str).str.strip()
        s_per_asin = s.groupby("asin")["units_sold"].sum().to_dict()

        a = pd.read_csv(SNAP_AMS)
        a["wn"] = pd.to_numeric(a["week"], errors="coerce").astype("Int64")
        a = a[a["wn"] == latest_week]
        a["asin"] = a["asin"].astype(str).str.strip()
        a_per_asin = a.groupby("asin")["units"].sum().to_dict()

        for asin in sorted(set(s_per_asin) | set(a_per_asin)):
            if not asin or asin in ("nan", "__SB__"):
                continue
            v_s = float(s_per_asin.get(asin, 0.0))
            v_a = float(a_per_asin.get(asin, 0.0))
            delta = round(v_a - v_s, 2)
            if abs(delta) > TOLERANCE:
                out.append({
                    "asin":                 asin,
                    "sales_trend_units":    v_s,
                    "ams_trend_units":      v_a,
                    "delta":                delta,
                    "note": "AMAZON+1P UNITS drift between sales_snapshot and business_ads_joined",
                })
    except Exception as e:
        out.append({
            "asin": "(load failed)", "sales_trend_units": 0,
            "ams_trend_units": 0, "delta": 0,
            "note": f"loader raised: {e!r}",
        })
    return pd.DataFrame(out)


def check_off_master_asins_sales() -> pd.DataFrame:
    """Sales-side analogue of check_off_master_asins.

    Walks every brand × week sales raw file (amazon_sales.xlsx + every
    sheet in other_channels.xlsx) and flags ASINs whose ASIN AND raw
    SKU are both absent from sku_master.xlsx.  These rows survive
    sales_auto_etl's master-fill (because there's nothing to fill from)
    so they land in weekly_sales_snapshot with empty/unmapped model →
    Sales Trend can show them but they don't roll up under their
    intended brand/model.

    Latest-4 weeks only so this is a near-term action list, not a
    historical log.
    """
    RAW = ROOT / "data" / "raw" / "sales"
    master_path = ROOT / "data" / "master" / "sku_master.xlsx"
    if not (RAW.exists() and master_path.exists()):
        return pd.DataFrame()

    m = pd.read_excel(master_path)
    m.columns = m.columns.str.strip()
    master_asins = set(m["ASIN"].astype(str).str.strip()) if "ASIN" in m.columns else set()
    master_skus  = set(m["FBA SKU"].astype(str).str.strip()) if "FBA SKU" in m.columns else set()
    if "Original SKU" in m.columns:
        master_skus |= set(m["Original SKU"].astype(str).str.strip())
    master_asins.discard(""); master_asins.discard("nan")
    master_skus.discard("");  master_skus.discard("nan")

    # Find latest week from folder names → take latest 4
    week_dirs = []
    for p in RAW.iterdir():
        if p.is_dir() and p.name.lower().startswith("week"):
            try:
                week_dirs.append((int(p.name.replace("Week", "").strip()), p))
            except Exception:
                pass
    if not week_dirs:
        return pd.DataFrame()
    week_dirs.sort()
    weeks_of_interest = {w for w, _ in week_dirs[-4:]}

    rows: list[Dict[str, Any]] = []
    for wnum, wdir in week_dirs:
        if wnum not in weeks_of_interest:
            continue
        for brand_dir in wdir.iterdir():
            if not brand_dir.is_dir():
                continue
            brand_norm = _norm_brand(brand_dir.name)

            # amazon_sales.xlsx — single sheet, (Child) ASIN + SKU + Units Ordered
            amz = brand_dir / "amazon_sales.xlsx"
            if amz.exists():
                try:
                    df = pd.read_excel(amz)
                    df.columns = [c.strip() for c in df.columns]
                    cl = {c.lower(): c for c in df.columns}
                    a_col = cl.get("(child) asin") or cl.get("child asin") or cl.get("asin")
                    s_col = cl.get("sku")
                    u_col = cl.get("units ordered") or cl.get("units_ordered")
                    if a_col:
                        sub = df[[c for c in (a_col, s_col, u_col) if c]].copy()
                        sub.columns = ["asin"] + (["sku"] if s_col else []) + (["units"] if u_col else [])
                        sub["asin"] = sub["asin"].astype(str).str.strip()
                        if "sku" in sub.columns:
                            sub["sku"] = sub["sku"].astype(str).str.strip()
                        else:
                            sub["sku"] = ""
                        sub["units"] = pd.to_numeric(sub.get("units", 0), errors="coerce").fillna(0)
                        mask = (
                            sub["asin"].ne("")
                            & ~sub["asin"].isin(master_asins)
                            & ~sub["sku"].isin(master_skus)
                        )
                        for _, r in sub[mask].iterrows():
                            rows.append({
                                "week_num":     wnum,
                                "brand_folder": brand_norm,
                                "source":       "amazon_sales",
                                "channel":      "amazon",
                                "asin":         r["asin"],
                                "raw_sku":      r["sku"],
                                "units":        int(r["units"]),
                            })
                except Exception as e:
                    rows.append({
                        "week_num": wnum, "brand_folder": brand_norm,
                        "source": "amazon_sales", "channel": "—",
                        "asin": "(read failed)", "raw_sku": str(e)[:80],
                        "units": 0,
                    })

            # other_channels.xlsx — each sheet is one channel (1p Sales, B2B, Blinkit, etc.)
            oc = brand_dir / "other_channels.xlsx"
            if oc.exists():
                try:
                    xl = pd.ExcelFile(oc)
                    for sh in xl.sheet_names:
                        try:
                            d = pd.read_excel(oc, sheet_name=sh)
                        except Exception:
                            continue
                        d.columns = [c.strip() for c in d.columns]
                        cl = {c.lower(): c for c in d.columns}
                        a_col = cl.get("asin") or cl.get("(child) asin")
                        s_col = cl.get("sku")
                        u_col = cl.get("qty") or cl.get("units") or cl.get("units sold") or cl.get("units_sold")
                        if not a_col:
                            continue
                        sub = d[[c for c in (a_col, s_col, u_col) if c]].copy()
                        sub.columns = ["asin"] + (["sku"] if s_col else []) + (["units"] if u_col else [])
                        sub["asin"] = sub["asin"].astype(str).str.strip()
                        if "sku" in sub.columns:
                            sub["sku"] = sub["sku"].astype(str).str.strip()
                        else:
                            sub["sku"] = ""
                        sub["units"] = pd.to_numeric(sub.get("units", 0), errors="coerce").fillna(0)
                        mask = (
                            sub["asin"].ne("")
                            & ~sub["asin"].isin(master_asins)
                            & ~sub["sku"].isin(master_skus)
                        )
                        for _, r in sub[mask].iterrows():
                            rows.append({
                                "week_num":     wnum,
                                "brand_folder": brand_norm,
                                "source":       "other_channels",
                                "channel":      sh,
                                "asin":         r["asin"],
                                "raw_sku":      r["sku"],
                                "units":        int(r["units"]),
                            })
                except Exception:
                    pass

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    # Roll up: same ASIN in multiple weeks/sheets gets one summary row.
    rolled = out.groupby(
        ["asin", "raw_sku", "brand_folder", "source", "channel"],
        as_index=False,
    ).agg(weeks_seen=("week_num", "nunique"), total_units=("units", "sum"))
    rolled = rolled[rolled["total_units"] > 0]
    return rolled.sort_values("total_units", ascending=False).reset_index(drop=True)


def check_ams_gt_sales_per_model(latest_week: int) -> pd.DataFrame:
    """Per (brand, model, week), AMS Trend GMV must NOT exceed Sales
    Trend's Amazon+1P-channel GMV — AMS scope is a subset (Amazon+1P
    channels only).  When it's higher, business_ads_joined is carrying
    rows that don't reconcile back to weekly_sales_snapshot.

    Common root causes:
      • model=NaN bucket from untagged-in-master ASINs (now filtered
        in step4, so this should be 0 going forward).
      • Child-ASIN grain mismatch (business_report has multiple child
        variants, sales_snapshot has just the parent).

    Tolerance: Rs 5,000 OR 5% per (brand, model, week).  Catches the
    AM-W45 / AI-04 RED / AI-14 class of bugs that brand-rollup #17
    nets out and misses.
    """
    TOL_RS  = 5000.0
    TOL_PCT = 0.05

    try:
        sales = pd.read_csv(SNAP_SALES)
    except Exception:
        return pd.DataFrame()
    sales["wn"] = sales["week"].astype(str).str.extract(r"(\d+)").astype(int)
    # Scope to latest week ONLY.  Older weeks predate the SP-API 1P
    # refactor — their weekly_sales_snapshot 1P channel comes from
    # operator manual files (often partial), while business_ads_joined
    # always re-derives 1P from business_report (Amazon's full 1P view).
    # Comparing the two for W<24 surfaces a known data-coverage gap
    # rather than a route bug, so we only fail-loud on the current week.
    weeks_of_interest = {latest_week}
    sales = sales[sales["wn"].isin(weeks_of_interest)]
    sales = sales[sales["brand"].astype(str).str.lower() != "fossil"]
    chan = sales["channel"].astype(str).str.lower().str.strip()
    sales = sales[chan.isin(["amazon", "1p sales"])]
    sales["model_n"] = sales["model"].astype(str).str.upper().str.strip()
    sales["brand_n"] = sales["brand"].astype(str).str.strip()
    sales_agg = (
        sales.groupby(["wn", "brand_n", "model_n"], as_index=False)
             .agg(sales_gmv=("gross_sales", "sum"))
    )

    BA_PATH = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
    if not BA_PATH.exists():
        return pd.DataFrame()
    ba = pd.read_csv(BA_PATH)
    ba = ba[ba["week"].isin(weeks_of_interest)]
    ba = ba[ba["brand"].astype(str).str.lower() != "fossil"]
    ba["model_n"] = ba["model"].astype(str).str.upper().str.strip()
    ba["brand_n"] = ba["brand"].astype(str).str.strip()
    ams_agg = (
        ba.groupby(["week", "brand_n", "model_n"], as_index=False)
          .agg(ams_gmv=("gmv", "sum"))
          .rename(columns={"week": "wn"})
    )

    m = sales_agg.merge(ams_agg, on=["wn", "brand_n", "model_n"], how="outer").fillna(0)
    m["delta"] = m["ams_gmv"] - m["sales_gmv"]
    base = m["sales_gmv"].replace(0, 1)
    m["pct"] = m["delta"] / base
    bad = m[(m["delta"] > TOL_RS) & (m["pct"] > TOL_PCT)].copy()
    if bad.empty:
        return pd.DataFrame()
    bad = bad.rename(columns={"wn": "week", "brand_n": "brand", "model_n": "model"})
    bad["sales_gmv"] = bad["sales_gmv"].round(0).astype(int)
    bad["ams_gmv"]   = bad["ams_gmv"].round(0).astype(int)
    bad["delta"]     = bad["delta"].round(0).astype(int)
    bad["pct"]       = (bad["pct"] * 100).round(1)
    return bad.sort_values("delta", ascending=False)[
        ["week", "brand", "model", "sales_gmv", "ams_gmv", "delta", "pct"]
    ].reset_index(drop=True)


def check_cross_route_brand_totals(latest_week: int) -> pd.DataFrame:
    """Per-brand sales (latest week) should be identical between
    Main Dashboard's brand KPI and Sales Trend's brand sums.
    Both routes read weekly_sales_snapshot but apply different code
    paths.  Tolerance: ₹1.
    """
    TOLERANCE_RS = 1.0
    out: list[Dict[str, Any]] = []
    try:
        if not SNAP_SALES.exists():
            return pd.DataFrame()
        s = pd.read_csv(SNAP_SALES)
        s["wn"] = s["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        s = s[s["wn"] == latest_week].copy()
        if s.empty:
            return pd.DataFrame()
        s["brand_n"] = s["brand"].astype(str).apply(_norm_brand)
        # Sales Trend route excludes Fossil; mirror that for apples-to-apples
        s_st = s[s["brand_n"] != "fossil"]
        st_per_brand = s_st.groupby("brand_n")["gross_sales"].sum().round(2).to_dict()

        # Main Dashboard's per-brand KPI — pull it via its route's loader
        # to catch any custom filter/aggregation drift.
        from weekly_app.routes.AM_sales_trend import load_sales as am1p_load_sales
        ams_s = am1p_load_sales()
        ams_s = ams_s[ams_s["week_num"] == latest_week]
        # AM_sales_trend already drops Fossil; group by brand
        am_per_brand = ams_s.groupby("brand")["sales"].sum().round(2).to_dict()

        for brand in sorted(set(st_per_brand) | set(am_per_brand)):
            if not brand or brand == "fossil":
                continue
            v_st = float(st_per_brand.get(brand, 0.0))
            v_am = float(am_per_brand.get(brand, 0.0))
            delta = round(v_am - v_st, 2)
            if abs(delta) > TOLERANCE_RS:
                out.append({
                    "brand":               brand,
                    "snap_total_sales":    v_st,
                    "am1p_route_sales":    v_am,
                    "delta_rs":            delta,
                    "note": "BRAND TOTAL drift between snapshot and Amazon+1P route loader",
                })
    except Exception as e:
        out.append({
            "brand": "(load failed)", "snap_total_sales": 0,
            "am1p_route_sales": 0, "delta_rs": 0,
            "note": f"loader raised: {e!r}",
        })
    return pd.DataFrame(out)


def check_category_coverage() -> pd.DataFrame:
    """Flag when a snapshot's category coverage drops below threshold.

    Caught the 2026-06-17 inventory filter bug — raw Inventory Snapshot
    files don't carry category columns and the ETL wasn't backfilling
    from sku_master, leaving snapshot category_l0 at ~50% populated.
    UI filter-by-category silently returned blank for most picks.

    Per-snapshot thresholds reflect how complete master's coverage is
    at each level — L2 is intrinsically sparser in master itself.
    """
    # Thresholds calibrated to master's actual coverage.  L0 is almost
    # always set in master (incl. Fossil) — anything <90% means the ETL
    # isn't backfilling.  L1 is intrinsically sparse for Fossil watches
    # (operator hasn't filled it in master), so the threshold for files
    # that include Fossil is lower than inventory (which excludes Fossil
    # from many channel rows).  L2 is too sparse in master to alarm on.
    SPECS = [
        # (snapshot file, level, min_pct)
        ("data/processed/weekly_sales_snapshot.csv",        "category_l0", 90.0),
        ("data/processed/weekly_sales_snapshot.csv",        "category_l1", 60.0),
        ("data/processed/inventory_model_snapshot.csv",     "category_l0", 90.0),
        ("data/processed/inventory_model_snapshot.csv",     "category_l1", 80.0),
        ("data/ams_weekly_data/processed_ads/business_ads_joined.csv", "category_l0", 90.0),
        ("data/ams_weekly_data/processed_ads/business_ads_joined.csv", "category_l1", 40.0),
    ]
    out: list[Dict[str, Any]] = []
    for path_str, col, min_pct in SPECS:
        path = ROOT / path_str
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
        except Exception as e:
            out.append({"snapshot": path_str, "column": col, "coverage_pct": 0, "min_required_pct": min_pct, "note": f"load failed: {e!r}"})
            continue
        if col not in df.columns:
            continue
        clean = df[col].astype(str).str.strip()
        populated = (~clean.isin(["", "nan", "None", "<NA>"]) & df[col].notna()).sum()
        pct = round(100.0 * populated / max(1, len(df)), 1)
        if pct < min_pct:
            out.append({
                "snapshot":         path_str.split("/")[-1],
                "column":           col,
                "coverage_pct":     pct,
                "min_required_pct": min_pct,
                "note":             "Category coverage below threshold — UI filter will miss rows",
            })
    return pd.DataFrame(out)


def check_sp_api_ingestion(latest_week: int) -> pd.DataFrame:
    """Flag when an SP-API auto-pull file has > 0 raw units for the
    latest week but the corresponding snapshot channel for that
    brand+week is empty (or < 50% of the raw total).

    Catches the failure mode where the cron successfully writes a new
    SP-API file to disk + git, but no downstream ETL has been updated
    to ingest it.  W26 hit this silently for two layers:
       - Seller Sales (SP-API).xlsx   → snapshot Amazon channel = 0
       - Seller FBA Inventory (SP-API).xlsx → snapshot Amazon channel = 0
    Each cost ~₹76 L / 26,615 units of visibility before being caught.

    Pairs checked (raw file → snapshot channel):
       Seller Sales (SP-API).xlsx        → weekly_sales_snapshot "Amazon"
       Vendor Sales (SP-API).xlsx        → weekly_sales_snapshot "1p Sales"
       Seller FBA Inventory (SP-API).xlsx → inventory_model_snapshot "Amazon"
       Vendor SOH (SP-API).xlsx (1p rows) → inventory_model_snapshot "1P"
    """
    out: list[Dict[str, Any]] = []
    folder_to_brand = {
        "Audio_Array":    "Audio Array",
        "Nexlev":         "Nexlev",
        "Tonor":          "Tonor",
        "White_Mulberry": "White Mulberry",
        "Fossil":         "Fossil",
    }

    sales_snap = None
    inv_snap   = None
    if SNAP_SALES.exists():
        sales_snap = pd.read_csv(SNAP_SALES)
        sales_snap["wn"] = (
            sales_snap["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        )
    if SNAP_INV.exists():
        inv_snap = pd.read_csv(SNAP_INV)
        inv_snap["wn"] = (
            inv_snap["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        )

    sales_dir = ROOT / "data" / "raw" / "sales" / f"Week {latest_week}"
    inv_dir   = ROOT / "data" / "raw" / "inventory" / f"Week {latest_week}"

    def _flag(layer, brand, raw_file, expected_channel, raw_units, snap_units):
        out.append({
            "layer":            layer,
            "brand":            brand,
            "raw_file":         raw_file,
            "expected_channel": expected_channel,
            "raw_units":        int(raw_units),
            "snapshot_units":   int(snap_units),
            "delta":            int(snap_units - raw_units),
            "note": ("SP-API file has rows but snapshot missed them — "
                     "ETL ingestion path needs the new source"),
        })

    def _sum_snap_sales(brand_name: str, channel: str) -> float:
        if sales_snap is None:
            return 0.0
        m = ((sales_snap["wn"] == latest_week)
             & sales_snap["brand"].astype(str).str.strip().eq(brand_name)
             & sales_snap["channel"].astype(str).str.strip().str.lower().eq(channel.lower()))
        return float(pd.to_numeric(sales_snap.loc[m, "units_sold"],
                                   errors="coerce").fillna(0).sum())

    def _sum_snap_inv(brand_name: str, channel: str) -> float:
        if inv_snap is None:
            return 0.0
        m = ((inv_snap["wn"] == latest_week)
             & inv_snap["brand"].astype(str).str.strip().str.lower().eq(brand_name.lower())
             & inv_snap["channel"].astype(str).str.strip().str.lower().eq(channel.lower()))
        return float(pd.to_numeric(inv_snap.loc[m, "inventory_units"],
                                   errors="coerce").fillna(0).sum())

    def _read_raw(file, units_lower_col: str, channel_filter: str | None = None) -> float:
        try:
            df = pd.read_excel(file)
        except Exception:
            return 0.0
        df.columns = [c.strip().lower() for c in df.columns]
        if units_lower_col not in df.columns:
            return 0.0
        if channel_filter is not None and "channel" in df.columns:
            df = df[df["channel"].astype(str).str.strip().str.lower() == channel_filter.lower()]
        return float(pd.to_numeric(df[units_lower_col], errors="coerce").fillna(0).sum())

    for folder, brand in folder_to_brand.items():
        # 3P Seller Sales → snapshot Amazon
        f = sales_dir / folder / "Seller Sales (SP-API).xlsx"
        if f.exists():
            raw = _read_raw(f, "units ordered")
            if raw > 0:
                snap = _sum_snap_sales(brand, "Amazon")
                if snap < 0.5 * raw:
                    _flag("sales", brand, "Seller Sales (SP-API).xlsx", "Amazon", raw, snap)

        # 1P Vendor Sales → snapshot 1p Sales
        f = sales_dir / folder / "Vendor Sales (SP-API).xlsx"
        if f.exists():
            raw = _read_raw(f, "qty")
            if raw > 0:
                snap = _sum_snap_sales(brand, "1p Sales")
                if snap < 0.5 * raw:
                    _flag("sales", brand, "Vendor Sales (SP-API).xlsx", "1p Sales", raw, snap)

        # 3P Seller FBA Inventory → snapshot Amazon
        f = inv_dir / folder / "Seller FBA Inventory (SP-API).xlsx"
        if f.exists():
            raw = _read_raw(f, "inventory")
            if raw > 0:
                snap = _sum_snap_inv(brand, "Amazon")
                if snap < 0.5 * raw:
                    _flag("inventory", brand, "Seller FBA Inventory (SP-API).xlsx", "Amazon", raw, snap)

        # 1P Vendor SOH (only the 1p channel rows) → snapshot 1P
        f = inv_dir / folder / "Vendor SOH (SP-API).xlsx"
        if f.exists():
            raw = _read_raw(f, "qty", channel_filter="1p")
            if raw > 0:
                snap = _sum_snap_inv(brand, "1P")
                if snap < 0.5 * raw:
                    _flag("inventory", brand, "Vendor SOH (SP-API).xlsx", "1P", raw, snap)

    return pd.DataFrame(out)


def check_ams_vs_sales_brand(latest_week: int) -> pd.DataFrame:
    """Per-brand W{latest} GMV in business_ads_joined.csv (AMS Trend)
    must match weekly_sales_snapshot.csv Amazon-side gross_sales
    (Sales Trend's Amazon+1P slice).  Both represent the same money;
    any drift means the ETL chains disagree about a brand.

    Catches the class of bug where one pipeline ingests a source and
    the other doesn't, or where one parser handles a column format
    the other doesn't (e.g. W26 WM tripped this with ₹1.65 L missed
    by the AMS chain due to a currency-string parse failure).

    Tolerance: ₹1.  Fossil excluded (it's not on the dashboard).
    """
    TOL_RS = 1.0
    out: list[Dict[str, Any]] = []
    BIZ_JOINED = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
    if not (SNAP_SALES.exists() and BIZ_JOINED.exists()):
        return pd.DataFrame()
    try:
        s = pd.read_csv(SNAP_SALES)
        s["wn"] = s["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        s = s[s["wn"] == latest_week].copy()
        amz_side = s[s["channel"].astype(str).str.lower().isin(["amazon", "1p sales"])]
        amz_side["brand_n"] = amz_side["brand"].astype(str).apply(_norm_brand)
        amz_side = amz_side[amz_side["brand_n"] != "fossil"]
        sales_per_brand = (amz_side.groupby("brand_n")["gross_sales"]
                                    .sum().round(2).to_dict())

        ba = pd.read_csv(BIZ_JOINED)
        baw = ba[ba["week"] == latest_week]
        baw = baw.assign(brand_n=baw["brand"].astype(str).apply(_norm_brand))
        baw = baw[baw["brand_n"] != "fossil"]
        ams_per_brand = (baw.groupby("brand_n")["gmv"]
                           .sum().round(2).to_dict())

        for brand in sorted(set(sales_per_brand) | set(ams_per_brand)):
            if not brand or brand == "fossil":
                continue
            v_st = float(sales_per_brand.get(brand, 0.0))
            v_am = float(ams_per_brand.get(brand, 0.0))
            delta = round(v_am - v_st, 2)
            if abs(delta) > TOL_RS:
                out.append({
                    "brand":                brand,
                    "sales_trend_amz_side": v_st,
                    "ams_trend_gmv":        v_am,
                    "delta_rs":             delta,
                    "note": ("AMS Trend GMV ≠ Sales Trend Amazon-side gross_sales — "
                             "one chain is missing a 3P/1P source or column format"),
                })
    except Exception as e:
        out.append({
            "brand": "(load failed)",
            "sales_trend_amz_side": 0.0,
            "ams_trend_gmv":        0.0,
            "delta_rs":             0.0,
            "note": f"loader raised: {e!r}",
        })
    return pd.DataFrame(out)


def check_ams_vs_sales_brand_history(latest_week: int) -> pd.DataFrame:
    """Check 24's all-weeks companion: per-(brand, week) drift between
    business_ads_joined.csv and weekly_sales_snapshot.csv Amazon+1P.

    Catches historical bugs where a brand's rows are systematically
    re-tagged.  Example: 2026-07-13 sku_master blank-ASIN + drop_duplicates
    phantom-survivor collapsed ~25 unlaunched-AA-model rows into ONE row
    tagged brand=Audio Array; every Fossil ASIN with unresolved
    child_asin then joined to that survivor, silently re-tagging ~₹5.28
    Cr of Fossil GMV as Audio Array across W14-W25.  Check 24's latest-
    week-only scope missed it entirely (W26+W27 were already clean
    because Fossil started shipping business_report from W26); memory
    reference_ams_trend_aa_fossil_mistag_incident.md.

    Tolerance intentionally looser than Check 24 (₹10K AND >2% relative)
    to avoid false alarms on legitimate small drifts in older weeks that
    predate SP-API 1P ingest coverage.  Both thresholds must fire.
    """
    TOL_RS = 10_000.0
    TOL_PCT = 2.0
    out: list[Dict[str, Any]] = []
    BIZ_JOINED = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
    if not (SNAP_SALES.exists() and BIZ_JOINED.exists()):
        return pd.DataFrame()
    try:
        s = pd.read_csv(SNAP_SALES)
        s["wn"] = s["week"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
        amz = s[s["channel"].astype(str).str.lower().isin(["amazon", "1p sales"])].copy()
        amz["brand_n"] = amz["brand"].astype(str).apply(_norm_brand)
        amz = amz[amz["brand_n"] != "fossil"]
        sales = amz.groupby(["wn", "brand_n"])["gross_sales"].sum().round(2)

        ba = pd.read_csv(BIZ_JOINED)
        ba = ba.assign(brand_n=ba["brand"].astype(str).apply(_norm_brand))
        ba = ba[ba["brand_n"] != "fossil"]
        ams = ba.groupby(["week", "brand_n"])["gmv"].sum().round(2)

        # Restrict comparison to weeks where the ADS side has ANY data.
        # business_ads_joined.csv is a rolling 12-week window (~W17-W28
        # today), so sales-side rows for weeks below that (W4-W16) have
        # no ads counterpart and would flag as -100% delta — pure scope
        # artifact, not a mistag.  Only compare in the overlap so the
        # brand-mistag gate downstream doesn't fire on noise.
        ads_weeks = {int(wn) for (wn, _) in ams.index if pd.notna(wn)}
        if not ads_weeks:
            return pd.DataFrame()

        all_keys = set()
        for (wn, brand) in sales.index:
            if pd.notna(wn) and int(wn) in ads_weeks:
                all_keys.add((int(wn), str(brand)))
        for (wn, brand) in ams.index:
            if pd.notna(wn):
                all_keys.add((int(wn), str(brand)))

        for wn, brand in sorted(all_keys):
            v_st = float(sales.get((wn, brand), 0.0))
            v_am = float(ams.get((wn, brand), 0.0))
            delta = round(v_am - v_st, 2)
            pct = (delta / v_st * 100) if v_st > 0 else (100.0 if v_am > 0 else 0.0)
            if abs(delta) >= TOL_RS and abs(pct) >= TOL_PCT:
                out.append({
                    "week":                f"W{int(wn)}",
                    "brand":               brand,
                    "sales_trend_amz_side": v_st,
                    "ams_trend_gmv":       v_am,
                    "delta_rs":            delta,
                    "delta_pct":           round(pct, 2),
                    "note": ("AMS Trend brand-week GMV drifts from Sales Trend Amazon+1P "
                             ">= 2% AND >= Rs 10,000 — possible brand mis-tag (see "
                             "Fossil-to-AA pattern of 2026-07-13) OR missing/duplicate "
                             "source ingest for this brand-week"),
                })
    except Exception as e:
        out.append({
            "week": "-", "brand": "(load failed)",
            "sales_trend_amz_side": 0.0, "ams_trend_gmv": 0.0,
            "delta_rs": 0.0, "delta_pct": 0.0,
            "note": f"loader raised: {e!r}",
        })
    return pd.DataFrame(out)


def check_brand_magnitude_regression(latest_week: int) -> pd.DataFrame:
    """Flag when a brand's value for a key metric drops >50% (or spikes
    >2x) vs the median of the prior 4 weeks.  Catches the AMS partial-pull
    class of incident — where ads spend for a brand-week landed at Rs 1L
    instead of the historical Rs 8L baseline, but was non-zero so the
    "went_to_zero" check missed it.

    Metrics watched (latest-week brand totals):
      • Sales GMV (weekly_sales_snapshot)
      • Sales units (weekly_sales_snapshot)
      • Inventory total SOH (inventory_model_snapshot)
      • Ad spend (business_ads_joined)
      • Attributed sales (business_ads_joined)
      • Sessions (business_ads_joined)

    Tolerances:
      • Drop >50% or spike >100% vs prior-4w median
      • Skip brands where prior-4w median is below a small threshold
        (avoid noise from low-volume tail brands / new launches)
    """
    DROP_THRESHOLD  = 0.50   # latest < 50% of baseline → flag drop
    SPIKE_THRESHOLD = 2.00   # latest > 2x baseline → flag spike
    MIN_BASELINE = {
        "sales_gmv":      100_000,   # Rs 1L+ baseline only
        "sales_units":    20,
        "inventory_soh":  500,
        "ad_spend":       50_000,
        "attributed":     100_000,
        "sessions":       5_000,
    }
    prior_weeks = {latest_week - 4, latest_week - 3, latest_week - 2, latest_week - 1}

    out: list[Dict[str, Any]] = []

    def _flag(brand: str, metric: str, latest_val: float, baseline: float) -> None:
        if baseline < MIN_BASELINE.get(metric, 0):
            return
        if latest_val < baseline * DROP_THRESHOLD:
            out.append({
                "brand":     brand,
                "metric":    metric,
                "latest":    round(latest_val, 0),
                "prior_4w_median": round(baseline, 0),
                "delta_pct": round((latest_val - baseline) / baseline * 100, 1),
                "note":      "DROP > 50% vs prior 4-week median",
            })
        elif latest_val > baseline * SPIKE_THRESHOLD:
            out.append({
                "brand":     brand,
                "metric":    metric,
                "latest":    round(latest_val, 0),
                "prior_4w_median": round(baseline, 0),
                "delta_pct": round((latest_val - baseline) / baseline * 100, 1),
                "note":      "SPIKE > 2x vs prior 4-week median",
            })

    # Sales (GMV + units) per brand×week
    if SNAP_SALES.exists():
        try:
            s = pd.read_csv(SNAP_SALES)
            s["wn"] = pd.to_numeric(
                s["week"].astype(str).str.extract(r"(\d+)", expand=False),
                errors="coerce"
            ).astype("Int64")
            agg = (
                s.groupby(["brand", "wn"], as_index=False)
                 .agg(gmv=("gross_sales", "sum"), units=("units_sold", "sum"))
            )
            for brand in agg["brand"].dropna().unique():
                bdf = agg[agg["brand"] == brand]
                latest_row = bdf[bdf["wn"] == latest_week]
                prior      = bdf[bdf["wn"].isin(prior_weeks)]
                if latest_row.empty or prior.empty:
                    continue
                _flag(brand, "sales_gmv",   float(latest_row["gmv"].iloc[0]),   float(prior["gmv"].median()))
                _flag(brand, "sales_units", float(latest_row["units"].iloc[0]), float(prior["units"].median()))
        except Exception as e:
            out.append({"brand": "(load failed)", "metric": "sales", "latest": 0, "prior_4w_median": 0, "delta_pct": 0, "note": f"sales load raised: {e!r}"})

    # Inventory total SOH per brand×week
    if SNAP_INV.exists():
        try:
            i = pd.read_csv(SNAP_INV)
            i["wn"] = pd.to_numeric(
                i["week"].astype(str).str.extract(r"(\d+)", expand=False),
                errors="coerce"
            ).astype("Int64")
            agg = (
                i.groupby(["brand", "wn"], as_index=False)
                 .agg(soh=("inventory_units", "sum"))
            )
            for brand in agg["brand"].dropna().unique():
                bdf = agg[agg["brand"] == brand]
                latest_row = bdf[bdf["wn"] == latest_week]
                prior      = bdf[bdf["wn"].isin(prior_weeks)]
                if latest_row.empty or prior.empty:
                    continue
                _flag(brand, "inventory_soh", float(latest_row["soh"].iloc[0]), float(prior["soh"].median()))
        except Exception as e:
            out.append({"brand": "(load failed)", "metric": "inventory", "latest": 0, "prior_4w_median": 0, "delta_pct": 0, "note": f"inventory load raised: {e!r}"})

    # Ads (spend / attributed / sessions) per brand×week
    BA_PATH = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
    if BA_PATH.exists():
        try:
            b = pd.read_csv(BA_PATH)
            b["wn"] = pd.to_numeric(b["week"], errors="coerce").astype("Int64")
            agg = (
                b.groupby(["brand", "wn"], as_index=False)
                 .agg(spend=("Spend", "sum"), attributed=("attributed_sales", "sum"), sessions=("sessions", "sum"))
            )
            for brand in agg["brand"].dropna().unique():
                bdf = agg[agg["brand"] == brand]
                latest_row = bdf[bdf["wn"] == latest_week]
                prior      = bdf[bdf["wn"].isin(prior_weeks)]
                if latest_row.empty or prior.empty:
                    continue
                _flag(brand, "ad_spend",   float(latest_row["spend"].iloc[0]),      float(prior["spend"].median()))
                _flag(brand, "attributed", float(latest_row["attributed"].iloc[0]), float(prior["attributed"].median()))
                _flag(brand, "sessions",   float(latest_row["sessions"].iloc[0]),   float(prior["sessions"].median()))
        except Exception as e:
            out.append({"brand": "(load failed)", "metric": "ads", "latest": 0, "prior_4w_median": 0, "delta_pct": 0, "note": f"ads load raised: {e!r}"})

    return pd.DataFrame(out)


def check_derivative_freshness() -> pd.DataFrame:
    """Flag when a downstream snapshot is older than the file it derives
    from — means the ETL chain wasn't fully re-run after a source change.

    Catches the class of incident from 2026-06-16 where weekly_sales_snapshot
    was regenerated mid-debug but business_ads_joined wasn't, leaving the
    AMS layer briefly out of sync with Sales layer.  Check 19 surfaced it
    indirectly via per-model GMV drift; this check makes the cause obvious.

    Tolerance: 5 seconds (filesystem mtime jitter; consecutive writes
    inside an ETL run can land within the same second).
    """
    TOL_SEC = 5
    chain = [
        # (parent file, child file, label)
        (ROOT / "data" / "processed" / "weekly_sales_snapshot.csv",
         ROOT / "data" / "ams_weekly_data" / "ams_weekly_fact" / "ams_weekly_fact.csv",
         "weekly_sales_snapshot → ams_weekly_fact (biz_ads_weekly_etl needs re-run)"),
        (ROOT / "data" / "ams_weekly_data" / "ams_weekly_fact" / "ams_weekly_fact.csv",
         ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv",
         "ams_weekly_fact → business_ads_joined (step4 needs re-run)"),
        (ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv",
         ROOT / "data" / "ams_weekly_data" / "ams_weekly_fact" / "ams_weekly_fact_with_category.csv",
         "business_ads_joined → ams_weekly_fact_with_category (step5 needs re-run)"),
        (ROOT / "data" / "processed" / "inventory_model_snapshot.csv",
         ROOT / "data" / "processed" / "inventory_ams_snapshot.csv",
         "inventory_model_snapshot → inventory_ams_snapshot (inv_snap needs re-run)"),
    ]
    out: list[Dict[str, Any]] = []
    for parent, child, label in chain:
        if not parent.exists() or not child.exists():
            continue
        p_mt = parent.stat().st_mtime
        c_mt = child.stat().st_mtime
        lag = p_mt - c_mt  # positive = parent newer = child stale
        if lag > TOL_SEC:
            out.append({
                "parent":    str(parent.relative_to(ROOT)),
                "child":     str(child.relative_to(ROOT)),
                "lag_sec":   round(lag, 1),
                "note":      label,
            })
    return pd.DataFrame(out)


def check_inventory_snapshot_freshness() -> pd.DataFrame:
    """Flag when any weekly snapshot CSV that carries a `week` column
    fails to contain the latest raw week folder.

    Existed silently for weeks: the weekly-sync ran ETLs under
    `|| true`, so a TypeError inside the ETL was swallowed and the cron
    reported success while snapshot CSVs stayed frozen at an earlier
    week's data.  Check 20 (derivative freshness) only compares
    child-vs-parent mtime, so when both stayed stale together, nothing
    tripped.

    Rule: max(week) inside each weekly snapshot CSV must equal the max
    `Week NN` folder present under `data/raw/inventory/` (which is the
    fastest-updated week folder — sales/inventory share the same weekly
    cadence).
    """
    raw_dir = ROOT / "data" / "raw" / "inventory"
    if not raw_dir.exists():
        return pd.DataFrame()
    raw_weeks = []
    for p in raw_dir.iterdir():
        if p.is_dir():
            m = re.match(r"[Ww]eek\s*(\d+)", p.name)
            if m:
                raw_weeks.append(int(m.group(1)))
    if not raw_weeks:
        return pd.DataFrame()
    latest_raw_week = max(raw_weeks)

    # Every weekly snapshot with a `week` column belongs here.  Point-
    # in-time snapshots (returns/margin/inbound/reviews) don't carry
    # `week`, so they're covered by check_pointintime_snapshot_freshness.
    watched = [
        ROOT / "data" / "processed" / "inventory_model_snapshot.csv",
        ROOT / "data" / "processed" / "inventory_ams_snapshot.csv",
        ROOT / "data" / "processed" / "weekly_sales_snapshot.csv",
    ]

    out: list[Dict[str, Any]] = []
    for path in watched:
        if not path.exists():
            out.append({
                "file":              str(path.relative_to(ROOT)),
                "latest_raw_week":   latest_raw_week,
                "snapshot_max_week": None,
                "gap_weeks":         None,
                "note":              "snapshot missing — ETL never ran or failed silently",
            })
            continue
        try:
            df = pd.read_csv(path, usecols=["week"])
        except Exception as e:
            out.append({
                "file":              str(path.relative_to(ROOT)),
                "latest_raw_week":   latest_raw_week,
                "snapshot_max_week": None,
                "gap_weeks":         None,
                "note":              f"snapshot unreadable: {e}",
            })
            continue
        snap_weeks = (df["week"].astype(str)
                                 .str.extract(r"(\d+)")[0]
                                 .dropna().astype(int))
        if snap_weeks.empty:
            snap_max = None
            gap = None
        else:
            snap_max = int(snap_weeks.max())
            gap = latest_raw_week - snap_max
        if snap_max is None or snap_max < latest_raw_week:
            out.append({
                "file":              str(path.relative_to(ROOT)),
                "latest_raw_week":   latest_raw_week,
                "snapshot_max_week": snap_max,
                "gap_weeks":         gap,
                "note":              "snapshot did not ingest the latest raw week — ETL likely failed on the last cron run",
            })
    return pd.DataFrame(out)


def check_pointintime_snapshot_freshness() -> pd.DataFrame:
    """Flag point-in-time snapshot CSVs (no `week` column, replaced
    entirely each run) that are stale relative to a known-live anchor.

    Anchor: inventory_model_snapshot.csv — the load-bearing snapshot,
    already fail-loud in the workflow.  If a sibling snapshot is more
    than a day older than the anchor, its ETL likely failed silently on
    the last cron run.

    Complements Check 25 for CSVs like returns/margin/inbound/reviews
    that don't expose a week column.
    """
    anchor = ROOT / "data" / "processed" / "inventory_model_snapshot.csv"
    if not anchor.exists():
        return pd.DataFrame()
    anchor_mt = anchor.stat().st_mtime
    STALE_SEC = 24 * 3600  # >1 day older than anchor → suspect

    siblings = [
        ROOT / "data" / "processed" / "returns_snapshot.csv",
        ROOT / "data" / "processed" / "margin_snapshot.csv",
        ROOT / "data" / "processed" / "inbound_snapshot.csv",
        ROOT / "data" / "processed" / "reviews_snapshot.csv",
    ]
    out: list[Dict[str, Any]] = []
    for path in siblings:
        if not path.exists():
            out.append({
                "file":               str(path.relative_to(ROOT)),
                "anchor":             str(anchor.relative_to(ROOT)),
                "lag_hours":          None,
                "note":               "snapshot missing — ETL never ran or failed silently",
            })
            continue
        lag = anchor_mt - path.stat().st_mtime
        if lag > STALE_SEC:
            out.append({
                "file":               str(path.relative_to(ROOT)),
                "anchor":             str(anchor.relative_to(ROOT)),
                "lag_hours":          round(lag / 3600, 1),
                "note":               "snapshot older than anchor by >24h — ETL likely failed silently on the last cron run",
            })
    return pd.DataFrame(out)


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

    # Check registry: (sheet_name, label, fn_call, is_fail_loud).
    # is_fail_loud=False means the check produces informational output
    # only — issues are reported but don't cause non-zero exit.
    checks = [
        # Check 1 surfaces small chronic deltas (sub-1% per week, ~5-200
        # units) between raw and snapshot that the operator should
        # investigate but that don't represent a pipeline corruption
        # the cron should block on.  Info-level; output xlsx still
        # carries every row.
        ("1_raw_vs_snapshot",     "Raw vs Snapshot drift (diagnostic)",   check_raw_vs_snapshot_inventory(),       False),
        ("2_snapshot_vs_route",   "Snapshot vs Route loaders",            check_snapshot_vs_route(latest_week),    True),
        ("3_never_zero",          "Never-Zero column regression",         check_never_zero(latest_week),           True),
        ("4_channel_case_drift",  "Channel name case drift across weeks", check_channel_case_drift(),              True),
        # Check 5 — a brand×channel going to zero is often an
        # operational decision (e.g., paused on Blinkit) rather than a
        # bug.  Surface as diagnostic so the cron isn't blocked.
        ("5_went_to_zero",        "Brand×channel went to zero (diagnostic)", check_went_to_zero(latest_week),      False),
        # Operator-controlled data hygiene — sku_master is an operator
        # artefact; surface issues but don't block the cron's ads sync
        # on them.  The diagnostic xlsx still lists every row.
        ("6_master_completeness", "sku_master row hygiene (diagnostic)",  check_master_completeness(),             False),
        ("7_brand_name_consistency","Brand spelled inconsistently across snapshots", check_brand_name_consistency(), True),
        # Layer-presence is naturally noisy during WIP weeks — operator
        # may push sales raw before inventory raw, so one layer carries
        # the new week while another doesn't.  Info-level; the cron must
        # still be able to commit ads-only updates while operator works.
        ("8_latest_week_presence","Brand missing from a layer (diagnostic)", check_latest_week_presence(latest_week), False),
        ("9_brand_retags_info",   "(info) Brand re-tags by master",       check_brand_retag_diagnostics(),         False),
        ("10_off_master_asins",   "(info) Off-master ASINs in raw inv",   check_off_master_asins(),                False),
        ("11_raw_vs_snap_sales",  "Sales raw vs snapshot (per brand×wk)", check_raw_vs_snapshot_sales(latest_week),True),
        ("12_cross_route_inv",    "Cross-route inventory rule consistency", check_cross_route_inventory_consistency(latest_week), True),
        # Checks 13 + 16 compare weekly_sales_snapshot (operator-pushed,
        # frozen historical) against business_ads_joined (re-derived on
        # every cron run from business_report files).  Locally both
        # layers come from the same operator's-latest state and align,
        # but on the cron runner business_ads_joined regenerates with
        # the freshest attribution while weekly_sales_snapshot stays
        # whatever the operator last pushed — small per-ASIN drift is
        # expected and isn't a route bug, so these stay info-level and
        # don't fail the workflow.  Output xlsx still lists every drift
        # row for inspection.
        ("13_cross_route_sales",  "Cross-source sales drift (diagnostic)",  check_cross_route_sales_consistency(latest_week),     False),
        ("14_cross_route_ads",    "Cross-route ads rule consistency",       check_cross_route_ads_consistency(latest_week),       True),
        ("15_cross_route_sessions","Cross-route sessions consistency",      check_cross_route_sessions_consistency(latest_week),  True),
        ("16_cross_route_units",  "Cross-source units drift (diagnostic)",  check_cross_route_units_consistency(latest_week),     False),
        ("17_cross_route_brands", "Cross-route per-brand totals consistency",check_cross_route_brand_totals(latest_week),         True),
        # Operator action list: ASINs/SKUs in raw sales files that
        # don't resolve in sku_master.  These rows survive ETL but
        # roll up under a blank brand/model — operator should add
        # them to master so sales reconcile cleanly.
        ("18_off_master_sales",   "(info) Off-master ASINs in raw sales",  check_off_master_asins_sales(),                  False),
        # Check 19 catches the class of bug where business_ads_joined
        # carries GMV for ASINs that weekly_sales_snapshot doesn't —
        # AMS Trend then shows numbers HIGHER than Sales Trend for the
        # same (brand, model), which is impossible if AMS scope is a
        # subset.  Brand rollup checks (#17) net these out and miss them.
        ("19_ams_gt_sales_per_model", "AMS Trend GMV > Sales Trend GMV (per brand×model×wk)",
                                  check_ams_gt_sales_per_model(latest_week),                    True),
        # Check 20 surfaces the root cause that Check 19 catches indirectly:
        # a downstream snapshot has older mtime than its parent.  Means an
        # ETL step in the chain wasn't re-run after upstream changed.  When
        # this fires, just re-run the named ETL.
        ("20_derivative_freshness", "Derivative snapshot stale vs parent (re-run needed)",
                                  check_derivative_freshness(),                                  True),
        # Check 21 catches the AMS-partial-pull class of regression: a
        # brand's value for a key metric (sales / ads / inventory) drops
        # >50% or spikes >2x vs the prior 4-week median.  Catches partial
        # API pulls that "went_to_zero" misses because values are non-zero
        # but materially low.  Informational by default — surfaces in xlsx
        # for operator review.
        ("21_brand_magnitude_regression", "Brand metric > 50% drop or 2x spike vs prior 4w median",
                                  check_brand_magnitude_regression(latest_week),                 False),
        # Check 22: snapshot category_l0/l1/l2 coverage must stay above
        # the documented threshold.  Catches the 2026-06-17 inventory bug
        # where raw files lacked categories and the ETL didn't backfill,
        # silently breaking the UI's category-filter pickers.
        ("22_category_coverage", "Snapshot category_l0/l1 coverage < threshold (UI filter broken)",
                                  check_category_coverage(),                                     True),
        # Check 23 (new 2026-06-29) — early-warning for the "SP-API
        # auto-pull file exists but no downstream ETL ingests it" class
        # of bug.  W26 hit this twice silently (sales 3P Amazon +
        # inventory Amazon FBA) before this check existed.
        ("23_sp_api_ingestion", "SP-API file has rows but snapshot didn't ingest them",
                                  check_sp_api_ingestion(latest_week),                          True),
        # Check 24 (new 2026-06-29) — per-brand AMS Trend GMV must
        # equal Sales Trend Amazon-side gross_sales.  Both reflect the
        # same 3P + 1P money on Amazon; if they diverge by more than
        # ₹1 something silently dropped between the two pipelines.
        # W26 WM tripped this with a ₹1.65 L gap from a currency-string
        # parse failure in business_report_derive's 1P loader.
        ("24_ams_vs_sales_brand", "AMS Trend GMV vs Sales Trend Amazon-side per brand",
                                  check_ams_vs_sales_brand(latest_week),                        True),
        # Check 25 (new 2026-07-03) — catch the class of failure where a
        # weekly ETL step run under `|| true` swallows its error and the
        # cron reports success while the snapshot CSV stays frozen at
        # an earlier week's data.  Compares each inventory CSV's
        # max(week) against the max Week NN folder under
        # data/raw/inventory/.
        ("25_inv_snapshot_freshness", "Inventory snapshot behind latest raw week (silent ETL failure)",
                                  check_inventory_snapshot_freshness(),                         True),
        # Check 26 (new 2026-07-03) — same silent-failure class as
        # Check 25, but for point-in-time snapshots
        # (returns/margin/inbound/reviews) that don't carry a `week`
        # column.  Compares each sibling's mtime against
        # inventory_model_snapshot.csv (the fail-loud anchor).
        ("26_pit_snapshot_freshness", "Point-in-time snapshot stale vs inventory anchor (silent ETL failure)",
                                  check_pointintime_snapshot_freshness(),                       True),
        # Check 27 (new 2026-07-13) — Check 24's all-weeks companion.
        # Catches historical brand-mis-tag class of bug where Check 24's
        # latest-week-only scope wouldn't fire.  Triggered by the sku_
        # master phantom-NaN-survivor issue that dumped ~Rs 5.28 Cr of
        # Fossil GMV into brand=Audio Array across W14-W25 for 4 weeks
        # before operator spotted the eyeball-wrong 12.9 Cr / 7.88% TACOS
        # on AMS Trend UI.  See reference_ams_trend_aa_fossil_mistag_incident.
        # Tolerance: Rs 10K AND >= 2% (both must fire) — looser than
        # Check 24 to avoid noise on legitimate historical drifts.
        ("27_ams_vs_sales_brand_history", "AMS Trend GMV vs Sales Trend Amazon+1P per brand (ALL weeks)",
                                  check_ams_vs_sales_brand_history(latest_week),                True),
    ]

    print()
    fail_loud_issues = 0
    info_count = 0
    for sheet, label, df, is_fail_loud in checks:
        # Special case Check 2 — it always returns a row per checked
        # loader (success or fail); count only the failures.
        if sheet == "2_snapshot_vs_route":
            n = int((df["delta"].abs() > UNIT_TOLERANCE).sum()) if not df.empty and "delta" in df.columns else 0
        else:
            n = len(df)
        marker = "⚠" if (is_fail_loud and n > 0) else ("ℹ" if n > 0 else "✓")
        tag = "" if is_fail_loud else " (info)"
        print(f"  {marker}  {sheet:<26} {label:<48}{tag}: {n:>4}")
        if is_fail_loud:
            fail_loud_issues += n
        else:
            info_count += n

    print()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        for sheet, label, df, _ in checks:
            (df if not df.empty
                else pd.DataFrame([{"note": f"clean — {label}"}])
            ).to_excel(xl, sheet_name=sheet, index=False)

    print(f"📁 Output: {OUT}")
    print()

    if fail_loud_issues == 0:
        print(f"✅ Pipeline integrity: clean.  ({info_count} informational row(s))")
        return 0
    print(f"⚠ Pipeline integrity: {fail_loud_issues} fail-loud issue(s) — see audit xlsx.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
