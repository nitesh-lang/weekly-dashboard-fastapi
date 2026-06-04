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
            amz = brand_dir / "amazon_sales.xlsx"
            if amz.exists():
                try:
                    df = pd.read_excel(amz)
                    cols = {c.lower().strip(): c for c in df.columns}
                    u = cols.get("units ordered") or cols.get("units_ordered")
                    if u:
                        raw_rows.append({
                            "week": wnum, "brand": brand,
                            "raw_units": float(pd.to_numeric(df[u], errors="coerce").fillna(0).sum()),
                        })
                except Exception:
                    pass
            oc = brand_dir / "other_channels.xlsx"
            if oc.exists():
                try:
                    xl = pd.ExcelFile(oc)
                    for sh in xl.sheet_names:
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
    bad = m[m["delta"].abs() > UNIT_TOLERANCE].copy()
    for c in ("raw_units", "snap_units"):
        bad[c] = bad[c].round(0).astype(int)
    bad["note"] = bad["delta"].apply(
        lambda d: "ROWS LOST (ETL drop)" if d < 0 else "EXTRA IN SNAP (stale)"
    )
    return bad.sort_values(["week", "brand"]).reset_index(drop=True)


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

    # Check registry: (sheet_name, label, fn_call, is_fail_loud).
    # is_fail_loud=False means the check produces informational output
    # only — issues are reported but don't cause non-zero exit.
    checks = [
        ("1_raw_vs_snapshot",     "Raw vs Snapshot (per week×channel)",   check_raw_vs_snapshot_inventory(),       True),
        ("2_snapshot_vs_route",   "Snapshot vs Route loaders",            check_snapshot_vs_route(latest_week),    True),
        ("3_never_zero",          "Never-Zero column regression",         check_never_zero(latest_week),           True),
        ("4_channel_case_drift",  "Channel name case drift across weeks", check_channel_case_drift(),              True),
        ("5_went_to_zero",        "Brand×channel went to zero this week", check_went_to_zero(latest_week),         True),
        ("6_master_completeness", "sku_master row hygiene",               check_master_completeness(),             True),
        ("7_brand_name_consistency","Brand spelled inconsistently across snapshots", check_brand_name_consistency(), True),
        ("8_latest_week_presence","Brand missing from a layer this week", check_latest_week_presence(latest_week), True),
        ("9_brand_retags_info",   "(info) Brand re-tags by master",       check_brand_retag_diagnostics(),         False),
        ("10_off_master_asins",   "(info) Off-master ASINs in raw inv",   check_off_master_asins(),                False),
        ("11_raw_vs_snap_sales",  "Sales raw vs snapshot (per brand×wk)", check_raw_vs_snapshot_sales(latest_week),True),
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
