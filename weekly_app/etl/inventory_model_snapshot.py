# ============================================================
# INVENTORY MODEL SNAPSHOT – STRICT MODEL LEVEL
# (WEEK + BRAND + MODEL ONLY)
# ============================================================

from pathlib import Path
import pandas as pd
import re

from weekly_app.etl._excel_safe import read_excel_safe

# ------------------------------------------------------------
# BASE PATHS
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_INV_DIR = BASE_DIR / "data" / "raw" / "inventory"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MASTER_FILE = BASE_DIR / "data" / "master" / "sku_master.xlsx"

OUT_FILE = PROCESSED_DIR / "inventory_model_snapshot.csv"


def _load_sku_nlc_map() -> dict:
    """{sku → nlc} from sku_master.xlsx — used as fallback when raw inventory rows
    have a blank nlc (Week 18+ exports stopped including it)."""
    try:
        m = read_excel_safe(MASTER_FILE)
    except Exception:
        return {}
    m.columns = [c.strip().lower() for c in m.columns]
    sku_col = "fba sku" if "fba sku" in m.columns else ("sku" if "sku" in m.columns else None)
    if not sku_col or "nlc" not in m.columns:
        return {}
    m[sku_col] = m[sku_col].astype(str).str.strip()
    m["nlc"] = pd.to_numeric(m["nlc"], errors="coerce").fillna(0)
    return dict(zip(m[sku_col], m["nlc"]))

# ============================================================
# ✅ SNAPSHOT SKIP CACHE
# Tracks the last mtime of every .xlsx file we processed.
# If nothing has changed on disk, run_inventory_etl() returns
# immediately without re-reading any files.
# On Render starter plan this saves several seconds on every
# server restart / cold start.
# ============================================================
_last_run_mtimes: dict = {}

def _get_dir_mtimes() -> dict:
    """Return {filepath: mtime} for all .xlsx files under RAW_INV_DIR."""
    mtimes = {}
    if not RAW_INV_DIR.exists():
        return mtimes
    for f in RAW_INV_DIR.rglob("*.xlsx"):
        try:
            mtimes[str(f)] = f.stat().st_mtime
        except Exception:
            pass
    return mtimes


# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def extract_week(value):
    if pd.isna(value):
        return None

    match = re.search(r"\d+", str(value))
    return f"Week {int(match.group())}" if match else None


def extract_brand(file_path: Path):
    ref = f"{file_path.parent.name} {file_path.stem}".lower()

    if "nexlev" in ref:
        return "Nexlev"
    if "white" in ref or "mulberry" in ref:
        return "White Mulberry"
    if "audio" in ref or "array" in ref:
        return "Audio Array"
    if "fossil" in ref:
        return "Fossil"
    if "tonor" in ref:
        return "Tonor"
    if "am" in ref:
        return "AMPM"

    return "Unknown"


def clean_model(val):
    if pd.isna(val):
        return None

    return (
        str(val)
        .strip()
        .upper()
    )


# ------------------------------------------------------------
# MAIN ETL
# ✅ FIXED 1: Skip entirely if output exists and no source
#             .xlsx files have changed since last run.
# ✅ FIXED 2: Vectorised clean_model using .str methods
#             instead of row-by-row apply(clean_model).
# ------------------------------------------------------------
def _load_master_category_maps() -> tuple[dict, dict, dict, dict]:
    """Build {ASIN → cat_l0/l1/l2} and {Model → cat_l0/l1/l2} lookups from
    sku_master.xlsx.  Raw Inventory Snapshot.xlsx files don't carry
    categories at all (operator only fills Brand/Model/SKU/ASIN/Qty/Channel),
    so without a master-fill the snapshot has 50% category coverage and
    UI filter-by-category returns blank for most picks.

    Returns four dicts: by ASIN for each category level + one Model dict
    that maps Model → first non-empty category triple (used when ASIN
    misses but Model resolves)."""
    asin_l0, asin_l1, asin_l2 = {}, {}, {}
    model_map: dict[str, tuple[str, str, str]] = {}
    try:
        from weekly_app.core.df_cache import load_excel_cached
        if not MASTER_FILE.exists():
            return asin_l0, asin_l1, asin_l2, model_map
        m = load_excel_cached(MASTER_FILE)
        m = m.copy()
        m.columns = m.columns.str.strip()
        if "ASIN" not in m.columns:
            return asin_l0, asin_l1, asin_l2, model_map
        def _clean(s: pd.Series) -> pd.Series:
            return s.astype(str).str.strip().replace({"nan": "", "None": "", "<NA>": ""})
        m["ASIN_n"]  = _clean(m["ASIN"])
        m["Model_n"] = _clean(m["Model"]).str.upper() if "Model" in m.columns else ""
        for c in ("category_l0", "category_l1", "category_l2"):
            if c not in m.columns:
                m[c] = ""
            m[c] = _clean(m[c])
        for _, r in m.iterrows():
            a = r["ASIN_n"]
            if a:
                if r["category_l0"]: asin_l0[a] = r["category_l0"]
                if r["category_l1"]: asin_l1[a] = r["category_l1"]
                if r["category_l2"]: asin_l2[a] = r["category_l2"]
            mk = r["Model_n"]
            if mk and mk not in model_map:
                trip = (r["category_l0"], r["category_l1"], r["category_l2"])
                if any(trip):
                    model_map[mk] = trip
    except Exception as _e:
        print(f"⚠ _load_master_category_maps failed: {_e!r}")
    return asin_l0, asin_l1, asin_l2, model_map


def run_inventory_etl():

    if not RAW_INV_DIR.exists():
        print("⚠ INVENTORY RAW DIR NOT FOUND – SKIPPING")
        return

    # ✅ FIX 1: Skip if output already exists and no files changed
    current_mtimes = _get_dir_mtimes()
    if OUT_FILE.exists() and current_mtimes == _last_run_mtimes:
        print("✅ INVENTORY ETL: no source changes detected — skipping (using existing snapshot)")
        return
    _last_run_mtimes.clear()
    _last_run_mtimes.update(current_mtimes)

    sku_nlc = _load_sku_nlc_map()
    _cat_l0, _cat_l1, _cat_l2, _cat_by_model = _load_master_category_maps()
    print(f"📥 Master category maps loaded: L0={len(_cat_l0)} ASINs, L1={len(_cat_l1)}, L2={len(_cat_l2)}, by-model={len(_cat_by_model)}")
    all_rows = []

    # ─────────────────────────────────────────────────────────
    # SP-API 1P canonical-source registry.
    #
    # When `Vendor SOH (SP-API).xlsx` is present for a given
    # (Week, Brand-folder), that file becomes the AUTHORITATIVE
    # source of 1P inventory rows for that brand+week.  Any 1P
    # channel rows coming from OTHER files in the same brand+week
    # folder (e.g. operator's manual `Inventory Snapshot.xlsx`)
    # are dropped to avoid double-counting.
    #
    # Safety net: if the SP-API file exists but is empty / unreadable
    # / has zero 1P rows, the manual rows are KEPT — empty SP-API
    # data must never silently zero out the dashboard's 1P inventory.
    sp_api_one_p_owns: set[tuple[str, str]] = set()
    for sp_file in RAW_INV_DIR.rglob("Vendor SOH (SP-API).xlsx"):
        try:
            _df = read_excel_safe(sp_file)
            _df.columns = [c.strip().lower() for c in _df.columns]
            if "channel" not in _df.columns or "qty" not in _df.columns:
                continue
            chan_n = _df["channel"].astype(str).str.strip().str.lower()
            one_p_qty = pd.to_numeric(
                _df.loc[chan_n == "1p", "qty"], errors="coerce"
            ).fillna(0).sum()
            if one_p_qty <= 0:
                # Empty/zero SP-API → fall back to manual; don't claim ownership.
                continue
        except Exception:
            continue
        week_label  = extract_week(sp_file.parent.parent.name)
        brand_folder = sp_file.parent.name
        if week_label and brand_folder:
            sp_api_one_p_owns.add((week_label, brand_folder))
    if sp_api_one_p_owns:
        print(f"   📡 SP-API 1P canonical source for: "
              f"{sorted(sp_api_one_p_owns)}")

    # --------------------------------------------------------
    # SCAN ALL XLSX FILES
    # --------------------------------------------------------
    for file in RAW_INV_DIR.rglob("*.xlsx"):

        try:
            df = read_excel_safe(file)
        except Exception:
            continue

        df.columns = [c.strip().lower() for c in df.columns]

        # SP-API Seller FBA Inventory files don't carry `channel` or
        # `qty` — they carry `inventory` (= afn_total − unsellable).
        # Translate them into the schema the rest of the loop expects
        # so Amazon FBA stock lands in inventory_model_snapshot.csv
        # (otherwise inventory_amazon stays 0 portfolio-wide and the
        # audit's never-zero check fires every week).
        if file.name == "Seller FBA Inventory (SP-API).xlsx":
            if "inventory" in df.columns and "model" in df.columns:
                df["qty"] = pd.to_numeric(df["inventory"], errors="coerce").fillna(0)
                df["channel"] = "AMAZON"
                # `type` column is informational downstream; tag explicitly.
                if "type" not in df.columns:
                    df["type"] = "FBA"
                # `week` column matches the per-week ETL grouping.
                if "week" not in df.columns and "Week" in df.columns:
                    df["week"] = df["Week"]

        # Bundle SKUs (e.g. "UB-05 (AM-S2+AA-21)") legitimately have no
        # ASIN — bundles are SKU-level constructs.  Downstream loaders
        # like ams_trend.load_inventory_snapshot drop blank-ASIN rows
        # before groupby(asin, Model, week) to avoid NaN-key collisions,
        # so bundle inventory was silently lost from the AMS Trend
        # snapshot — while AM_sales_trend (which only groups by Model)
        # still saw it, producing the cross-route drift audit Check 12
        # caught.  Synthesise a stable per-SKU ASIN here so both routes
        # carry the same numbers.
        if "asin" in df.columns and "sku" in df.columns:
            _blank_asin = (
                df["asin"].isna()
                | df["asin"].astype(str).str.strip().str.lower().isin(["", "nan", "none", "<na>"])
            )
            if _blank_asin.any():
                # pandas 2.2+ setitem guard — widen asin to object so a
                # BUNDLE_ string assignment into a float64/NaN column
                # doesn't raise LossySetitemError.
                df["asin"] = df["asin"].astype("object")
                df.loc[_blank_asin, "asin"] = (
                    "BUNDLE_" + df.loc[_blank_asin, "sku"].astype(str).str.strip()
                )

        if "model" not in df.columns or "qty" not in df.columns:
            continue

        # ------------------------
        # STANDARDIZE FIELDS
        # ✅ FIX 2: vectorised model cleaning — replaces apply(clean_model)
        # ------------------------
        df["model"] = (
            df["model"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace("NAN", pd.NA)
        )

        df["qty"] = (
            pd.to_numeric(df["qty"], errors="coerce")
            .fillna(0)
        )

        # NLC: prefer in-file value; fall back to sku_master by SKU (Week 18+
        # raw exports stopped populating the nlc column).
        in_file_nlc = (
            pd.to_numeric(df["nlc"], errors="coerce").fillna(0)
            if "nlc" in df.columns
            else pd.Series(0.0, index=df.index)
        )
        mapped_nlc = (
            df["sku"].astype(str).str.strip().map(sku_nlc).fillna(0)
            if "sku" in df.columns and sku_nlc
            else pd.Series(0.0, index=df.index)
        )
        df["nlc_resolved"] = in_file_nlc.where(in_file_nlc > 0, mapped_nlc)
        df["row_value"] = df["qty"] * df["nlc_resolved"]

        df["brand"] = extract_brand(file)

        # Folder layout: data/raw/inventory/<Week N>/<Brand>/<file>.xlsx
        # → file.parent is the brand folder, file.parent.parent is the week folder.
        folder_week = extract_week(file.parent.parent.name) or extract_week(file.parent.name)

        if "week" in df.columns:
            df["week"] = df["week"].apply(extract_week)
            if folder_week:
                df["week"] = df["week"].fillna(folder_week)
        else:
            df["week"] = folder_week

        # Only drop on week here — model is allowed to be NaN at this
        # point because Amazon FBA rows arrive with empty model/category
        # fields; the ASIN-first master alignment below fills Model from
        # sku_master via the ASIN lookup.  Drop unresolved-model rows
        # AFTER alignment instead (line further down).
        df = df.dropna(subset=["week"])

        if df.empty:
            continue

        # Preserve sku + asin per row.  Raw inventory files carry both;
        # values may be unreliable for Amazon FBA rows (model/category
        # blank) but the master alignment below resolves the truth.
        if "sku" in df.columns:
            df["sku"]  = df["sku"].astype(str).str.strip().replace({"nan":"","None":""})
        else:
            df["sku"] = ""
        if "asin" in df.columns:
            df["asin"] = df["asin"].astype(str).str.strip().replace({"nan":"","None":""})
        else:
            df["asin"] = ""
        for c in ("channel", "type"):
            if c not in df.columns:
                df[c] = ""
            df[c] = df[c].astype(str).str.strip().replace({"nan":"","None":""})

        # Normalise channel casing — operator's raw files sometimes type
        # "1p" / "Ampm" / "B2B - Ampm" instead of the canonical casing.
        # Without this, the same channel splits into multiple buckets
        # downstream (silent-zero bug surfaced by the audit: W20 stored
        # "1p" while every other week used "1P", so 1P inventory looked
        # like it went to zero in W20).
        CHANNEL_CANONICAL = {
            "1p": "1P", "amazon": "Amazon", "blinkit": "Blinkit",
            "ampm": "AMPM", "b2b - ampm": "B2B - AMPM",
            "b2b-ampm": "B2B - AMPM", "pipeline": "Pipeline",
            "open order": "Open Order", "ynt": "YNT",
        }
        df["channel"] = df["channel"].astype(str).str.strip().map(
            lambda s: CHANNEL_CANONICAL.get(s.lower(), s)
        )

        # ── SP-API 1P preference ──
        # If this file is NOT the SP-API Vendor SOH file for its
        # brand+week, but an SP-API file with non-zero 1P qty DOES
        # exist for that brand+week, drop the 1P channel rows from
        # this file — SP-API wins.  See `sp_api_one_p_owns` build
        # above for the canonical-source registry.
        is_sp_api_file = (file.name == "Vendor SOH (SP-API).xlsx")
        if not is_sp_api_file:
            brand_folder = file.parent.name
            week_label   = df["week"].dropna().iloc[0] if not df["week"].dropna().empty else None
            if week_label and (week_label, brand_folder) in sp_api_one_p_owns:
                before = len(df)
                df = df[df["channel"] != "1P"]
                dropped = before - len(df)
                if dropped:
                    print(f"   🔇 dropped {dropped} 1P row(s) from {file.relative_to(RAW_INV_DIR)} "
                          f"(SP-API owns 1P for {brand_folder} {week_label})")

        # ── Master alignment: ASIN → SKU → Model ──
        # Operator rule: ASIN is truth → SKU/Model/Brand come from master.
        # If ASIN doesn't match, fall back to raw SKU lookup against
        # master for Model/Brand (and backfill ASIN from master if the
        # raw row had none).  If neither matches, use the raw row's
        # Model to at least pin Brand from master.  Rows where Model
        # still can't be resolved get dropped after this block.
        from weekly_app.core.master_override import master_lookups, model_to_rec_map
        asin_rec, sku_rec = master_lookups()
        resolved = pd.Series(False, index=df.index)

        if asin_rec:
            m = df["asin"].isin(asin_rec)
            if m.any():
                df.loc[m, "sku"]   = df.loc[m, "asin"].map(lambda a: asin_rec[a]["sku"])
                df.loc[m, "model"] = df.loc[m, "asin"].map(lambda a: asin_rec[a]["model"])
                df.loc[m, "brand"] = df.loc[m, "asin"].map(lambda a: asin_rec[a]["brand"])
                resolved |= m

        if sku_rec:
            m = ~resolved & df["sku"].isin(sku_rec)
            if m.any():
                df.loc[m, "model"] = df.loc[m, "sku"].map(lambda s: sku_rec[s]["model"])
                df.loc[m, "brand"] = df.loc[m, "sku"].map(lambda s: sku_rec[s]["brand"])
                # Backfill ASIN from master where the source had none
                empty_asin = m & (df["asin"] == "")
                if empty_asin.any():
                    df.loc[empty_asin, "asin"] = df.loc[empty_asin, "sku"].map(
                        lambda s: sku_rec[s]["asin"] or ""
                    )
                resolved |= m

        # Last fallback: row's MODEL matches a master Model → pin Brand.
        # SKU/ASIN stay raw (one Model maps to many SKUs/ASINs).
        model_rec = model_to_rec_map()
        if model_rec:
            mu = df["model"].astype(str).str.strip().str.upper()
            m = ~resolved & mu.isin(model_rec)
            if m.any():
                df.loc[m, "brand"] = mu[m].map(lambda k: model_rec[k]["brand"])

        # Re-normalize after override (master values are already trimmed)
        df["model"] = df["model"].astype(str).str.strip().str.upper()

        # Now drop rows where Model still couldn't be resolved (ASIN not
        # in master AND raw row had no model — these have no canonical
        # identity downstream).  This catches the residual that the
        # earlier dropna(subset=["week","model"]) used to swallow before
        # master alignment ran.
        df = df[
            df["model"].astype(str).str.strip().ne("")
            & ~df["model"].astype(str).str.upper().isin(["NAN", "NONE", "<NA>"])
        ]
        if df.empty:
            continue

        # Carry category + nlc so consumers don't need to re-read raw xlsx.
        for c in ("category_l0", "category_l1", "category_l2"):
            if c not in df.columns:
                df[c] = ""
            df[c] = df[c].astype(str).str.strip().replace({"nan":"","None":""})

        # Propagate non-empty category from any row to ALL rows of the same
        # (brand, model).  Raw Inventory Snapshot.xlsx populates category
        # only on the 1P / primary-channel row — other channel/type rows
        # (AMPM, Amazon, Pipeline, YNT) leave it blank.  Result: only ~50%
        # of snapshot rows had category, and the UI's L0/L1/L2 filter would
        # silently miss most of a category's models.
        for c in ("category_l0", "category_l1", "category_l2"):
            df[c] = df.groupby(["brand", "model"])[c].transform(
                lambda s: s.replace("", pd.NA).ffill().bfill().fillna("")
            )

        # Master-fill categories when raw rows have empty values.  Try
        # ASIN lookup first, then fall back to Model lookup for rows
        # whose ASIN isn't in master.  Then propagate within (brand,model)
        # so all channel/type rows of the same model carry the same
        # category triple.
        df["_asin_n"]  = df["asin"].astype(str).str.strip()
        df["_model_n"] = df["model"].astype(str).str.strip().str.upper()
        for c, amap in (("category_l0", _cat_l0),
                        ("category_l1", _cat_l1),
                        ("category_l2", _cat_l2)):
            # Widen dtype so pandas 2.2+ StringDtype doesn't reject the
            # map result (which may contain NaN or non-string sentinels).
            df[c] = df[c].astype("object")
            # Treat NaN, "", "nan", "None", "<NA>" all as empty
            current = df[c].astype(str).str.strip()
            empty = current.isin(["", "nan", "None", "<NA>"]) | df[c].isna()
            if amap and empty.any():
                df.loc[empty, c] = df.loc[empty, "_asin_n"].map(amap).fillna("")
            # Fall back to model-based map for any rows still empty
            current = df[c].astype(str).str.strip()
            empty = current.isin(["", "nan", "None", "<NA>"]) | df[c].isna()
            if _cat_by_model and empty.any():
                idx = ("category_l0", "category_l1", "category_l2").index(c)
                df.loc[empty, c] = df.loc[empty, "_model_n"].map(
                    lambda k: _cat_by_model.get(k, ("", "", ""))[idx]
                ).fillna("")
        df = df.drop(columns=["_asin_n", "_model_n"])
        # Normalize and propagate within (brand, model) one last time so
        # any straggling empty rows pick up a sibling's value.
        for c in ("category_l0", "category_l1", "category_l2"):
            df[c] = df[c].astype(str).str.strip().replace({"nan": "", "None": "", "<NA>": ""})
            df[c] = df.groupby(["brand", "model"])[c].transform(
                lambda s: s.replace("", pd.NA).ffill().bfill().fillna("")
            )

        # Per-(SKU × ASIN × channel × type) aggregation — matches the
        # grain raw inventory files come at.
        model_grp = (
            df.groupby(
                ["week", "brand", "model", "sku", "asin", "channel", "type",
                 "category_l0", "category_l1", "category_l2"],
                as_index=False
            )
            .agg(
                inventory_units=("qty", "sum"),
                inventory_value=("row_value", "sum"),
                nlc=("nlc_resolved", "mean"),
            )
        )

        all_rows.append(model_grp)

    # --------------------------------------------------------
    # FINAL CONCAT
    # --------------------------------------------------------
    if not all_rows:
        print("⚠ NO VALID INVENTORY FILES FOUND")
        return

    final_df = pd.concat(all_rows, ignore_index=True)

    # --------------------------------------------------------
    # FINAL DEDUPE + CONSOLIDATION
    # (IMPORTANT – ensures 1 row per model)
    # --------------------------------------------------------
    final_df = (
        final_df
        .groupby(["week", "brand", "model", "sku", "asin", "channel", "type",
                  "category_l0", "category_l1", "category_l2"], as_index=False)
        .agg(
            inventory_units=("inventory_units", "sum"),
            inventory_value=("inventory_value", "sum"),
            nlc=("nlc", "mean"),
        )
    )
    final_df["inventory_value"] = final_df["inventory_value"].round(2)
    final_df["nlc"]             = final_df["nlc"].round(2)

    final_df = final_df.sort_values(
        ["week", "brand", "model", "sku", "channel"]
    )

    # --------------------------------------------------------
    # SAVE OUTPUT
    # --------------------------------------------------------
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Diagnostic: rows per week so the workflow log surfaces any
    # silent week-drop on the runner (e.g., an xlsx file that fails to
    # parse on Linux but works locally).
    week_counts = (
        final_df["week"].astype(str).str.extract(r"(\d+)")[0]
        .dropna().astype(int).value_counts().sort_index()
    )
    print("📊 Rows by week:")
    for w, n in week_counts.items():
        print(f"   W{w:>2}: {n:>5} rows")
    new_max = int(week_counts.index.max()) if not week_counts.empty else -1

    # Regression guard: never overwrite a snapshot with one that lost
    # recent weeks.  Protects against the runner-side scenario where a
    # specific xlsx silently fails to parse and the regenerated file is
    # missing W{N..N+K}.  Locally-committed snapshot is treated as the
    # high-water mark; if the regenerated one is older, keep the
    # existing file unchanged.
    if OUT_FILE.exists():
        try:
            existing = pd.read_csv(OUT_FILE, usecols=["week"], dtype=str)
            existing_weeks = (
                existing["week"].astype(str).str.extract(r"(\d+)")[0]
                .dropna().astype(int)
            )
            existing_max = int(existing_weeks.max())
            existing_counts = existing_weeks.value_counts()
            new_counts = week_counts

            # Per-week regression handling (2026-07-08 rewrite):
            # Three-tier defence against xlsx parse regressions:
            #
            # 1. HISTORICAL weeks (< max-1): preserve committed rows on
            #    any ≥30% drop.  Finalised weeks should never shrink;
            #    if they do, it's a Linux xlsx bug or master-override
            #    accident, not real data movement.
            #
            # 2. CURRENT / previous week (>= max-1) with CATASTROPHIC
            #    drop (fresh <60% of committed): preserve.  Normal
            #    mid-week refreshes add or refresh rows; a 40%+ drop
            #    is almost certainly a Linux openpyxl parse bomb (the
            #    exact bug that ate W27 inventory on 2026-07-07).
            #    Sanity gate + audit still catch this if it slips
            #    through, but preserving here means the operator's
            #    dashboard doesn't go dark even for one cron cycle.
            #
            # 3. CURRENT / previous week with moderate drop (60-70% of
            #    committed): warn, accept fresh — could be legitimate
            #    (channel retire, brand exit).
            if new_max < existing_max:
                print(
                    f"⚠  new max week W{new_max} < existing W{existing_max} "
                    f"— snapshot writing anyway; investigate stale week."
                )
            historical_boundary   = new_max - 1
            HISTORICAL_FLOOR      = 0.70   # historical weeks: <30% drop OK
            CATASTROPHE_FLOOR     = 0.60   # current/prev week: <40% drop OK
            weeks_preserved: list[int] = []
            existing_df = None
            for w in sorted(set(existing_counts.index) & set(new_counts.index)):
                old_n = int(existing_counts[w])
                new_n = int(new_counts[w])
                if old_n < 50 or new_n >= old_n * HISTORICAL_FLOOR:
                    continue
                drop_pct = (1 - new_n / old_n) * 100
                is_current_or_prev = w >= historical_boundary
                is_catastrophic    = new_n < old_n * CATASTROPHE_FLOOR
                if is_current_or_prev and not is_catastrophic:
                    print(
                        f"⚠  W{w} shrank {old_n} -> {new_n} rows ({drop_pct:.0f}%) "
                        f"— recent week, writing anyway (drop under catastrophe floor)."
                    )
                    continue
                # Preserve: either historical week regression OR
                # catastrophic current/prev-week drop (Linux xlsx bomb).
                if existing_df is None:
                    existing_df = pd.read_csv(OUT_FILE)
                old_rows = existing_df[
                    existing_df["week"].astype(str).str.extract(r"(\d+)")[0] == str(w)
                ]
                if old_rows.empty:
                    print(f"⚠  W{w}: could not locate old rows for preservation.")
                    continue
                final_df = final_df[
                    final_df["week"].astype(str).str.extract(r"(\d+)")[0] != str(w)
                ]
                for _c in final_df.columns:
                    if _c not in old_rows.columns:
                        old_rows[_c] = ""
                final_df = pd.concat(
                    [final_df, old_rows[final_df.columns]],
                    ignore_index=True,
                )
                weeks_preserved.append(w)
                kind = "CATASTROPHIC" if is_catastrophic and is_current_or_prev else "historical"
                print(
                    f"🛡  W{w} {kind} preservation: fresh had {new_n} rows "
                    f"({drop_pct:.0f}% drop), keeping committed {old_n} rows."
                )
            if weeks_preserved:
                print(f"🛡  Total weeks preserved: {weeks_preserved}")
        except Exception as e:
            print(f"⚠ regression-guard read failed, will write anyway: {e!r}")

    final_df.to_csv(OUT_FILE, index=False)

    print("✅ INVENTORY MODEL SNAPSHOT GENERATED")
    print(f"📦 Rows written: {len(final_df)}")
    print(f"📁 Output: {OUT_FILE}")


# ------------------------------------------------------------
# RUN
# ------------------------------------------------------------
if __name__ == "__main__":
    run_inventory_etl()