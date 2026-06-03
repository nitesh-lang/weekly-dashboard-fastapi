# ============================================================
# INVENTORY MODEL SNAPSHOT – STRICT MODEL LEVEL
# (WEEK + BRAND + MODEL ONLY)
# ============================================================

from pathlib import Path
import pandas as pd
import re

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
        m = pd.read_excel(MASTER_FILE)
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
    all_rows = []

    # --------------------------------------------------------
    # SCAN ALL XLSX FILES
    # --------------------------------------------------------
    for file in RAW_INV_DIR.rglob("*.xlsx"):

        try:
            df = pd.read_excel(file)
        except Exception:
            continue

        df.columns = [c.strip().lower() for c in df.columns]

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

        # ASIN is the only key we trust from the raw file.  SKU and
        # Model values in the raw inventory exports are unreliable
        # (Amazon FBA rows arrive with model/category empty; non-FBA
        # rows can use fulfillment-center prefix SKUs that differ from
        # master).  Wipe SKU + Model + Brand here and let the master
        # lookup below repopulate them canonically from ASIN.
        if "asin" in df.columns:
            df["asin"] = df["asin"].astype(str).str.strip().replace({"nan":"","None":""})
        else:
            df["asin"] = ""

        # Keep raw sku as a *fallback lookup key only* (not a trusted value).
        raw_sku = (
            df["sku"].astype(str).str.strip().replace({"nan":"","None":""})
            if "sku" in df.columns else pd.Series([""] * len(df), index=df.index)
        )

        # Wipe trusted-value columns; master will repopulate.
        df["sku"]   = ""
        df["model"] = ""
        df["brand"] = ""

        for c in ("channel", "type"):
            if c not in df.columns:
                df[c] = ""
            df[c] = df[c].astype(str).str.strip().replace({"nan":"","None":""})

        # ── Master alignment: ASIN → SKU (fallback key only) ──
        # Operator rule: trust ONLY ASIN from the raw file.  SKU/Model/
        # Brand all come from sku_master via ASIN lookup.  If ASIN is
        # missing on the raw row, use raw SKU as a lookup key (NOT as a
        # value) to find the canonical record in master.  Rows that
        # can't be resolved get dropped after this block.
        from weekly_app.core.master_override import master_lookups
        asin_rec, sku_rec = master_lookups()
        resolved = pd.Series(False, index=df.index)

        if asin_rec:
            m = df["asin"].isin(asin_rec)
            if m.any():
                df.loc[m, "sku"]   = df.loc[m, "asin"].map(lambda a: asin_rec[a]["sku"])
                df.loc[m, "model"] = df.loc[m, "asin"].map(lambda a: asin_rec[a]["model"])
                df.loc[m, "brand"] = df.loc[m, "asin"].map(lambda a: asin_rec[a]["brand"])
                resolved |= m

        # Fallback: row had no ASIN but raw SKU resolves in master.
        # Master gives us canonical SKU/Model/Brand AND the ASIN.
        if sku_rec:
            m = ~resolved & raw_sku.isin(sku_rec)
            if m.any():
                df.loc[m, "sku"]   = raw_sku[m].map(lambda s: sku_rec[s]["sku"] or s)
                df.loc[m, "model"] = raw_sku[m].map(lambda s: sku_rec[s]["model"])
                df.loc[m, "brand"] = raw_sku[m].map(lambda s: sku_rec[s]["brand"])
                df.loc[m, "asin"]  = raw_sku[m].map(lambda s: sku_rec[s]["asin"] or "")
                resolved |= m

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

    final_df.to_csv(OUT_FILE, index=False)

    print("✅ INVENTORY MODEL SNAPSHOT GENERATED")
    print(f"📦 Rows written: {len(final_df)}")
    print(f"📁 Output: {OUT_FILE}")


# ------------------------------------------------------------
# RUN
# ------------------------------------------------------------
if __name__ == "__main__":
    run_inventory_etl()