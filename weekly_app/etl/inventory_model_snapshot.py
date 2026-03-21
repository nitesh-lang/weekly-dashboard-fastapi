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

OUT_FILE = PROCESSED_DIR / "inventory_model_snapshot.csv"

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

        df["brand"] = extract_brand(file)

        if "week" in df.columns:
            df["week"] = df["week"].apply(extract_week)
        else:
            df["week"] = extract_week(file.parent.name)

        df = df.dropna(subset=["week", "model"])

        if df.empty:
            continue

        # ------------------------
        # STRICT MODEL AGGREGATION
        # ------------------------
        model_grp = (
            df.groupby(
                ["week", "brand", "model"],
                as_index=False
            )
            .agg(
                inventory_units=("qty", "sum")
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
        .groupby(["week", "brand", "model"], as_index=False)
        .agg(inventory_units=("inventory_units", "sum"))
    )

    final_df["inventory_value"] = 0

    # Optional: sort clean output
    final_df = final_df.sort_values(
        ["week", "brand", "model"]
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