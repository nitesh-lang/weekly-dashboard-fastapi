# ============================================================
# AMS MODEL SNAPSHOT – AUTO ETL (MODEL-BASED, HARDENED)
# ============================================================
# Purpose:
# - Read AMS weekly Excel files (latest week only)
# - Aggregate at Brand + Model level
# - Produce ams_model_snapshot.csv for UI + downstream joins
#
# HARDENING NOTES:
# - Auto-detect model-like column (model / sku / item name etc.)
# - Auto-detect units column (AMS varies heavily)
# - Never silently drop valid Week data
# - Strict logging for skipped brands/files
# ============================================================

from pathlib import Path
import pandas as pd
import re
import traceback

# ============================================================
# PATH CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

AMS_DIR_CANDIDATES = [
    BASE_DIR / "data" / "ams",
    BASE_DIR / "data" / "raw" / "ams",
]

OUT_FILE = PROCESSED_DIR / "ams_model_snapshot.csv"

# ============================================================
# NORMALIZERS
# ============================================================

def normalize_model(x):
    if x is None:
        return None
    return str(x).strip().upper()

def normalize_brand(x):
    return str(x).strip().lower().replace("_", " ")

# ============================================================
# RESOLVE AMS ROOT DIR
# ============================================================

def resolve_ams_dir():
    for d in AMS_DIR_CANDIDATES:
        if d.exists() and d.is_dir():
            print(f"✅ AMS DIR FOUND → {d}")
            return d
    return None

# ============================================================
# MODEL COLUMN RESOLUTION
# ============================================================

def resolve_model_column(df: pd.DataFrame):
    candidates = [
        "model",
        "model ",
        "model name",
        "item name",
        "sku",
        "advertised sku",
        "advertised_sku",
        "child asin",
        "asin",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None

# ============================================================
# UNITS COLUMN RESOLUTION (CRITICAL FIX)
# ============================================================

def resolve_units_column(df: pd.DataFrame):
    candidates = [
        "units_ordered",
        "units ordered",
        "units",
        "total units",
        "7 day total units",
        "14 day total units",
        "orders",
        "total orders",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None

# ============================================================
# MAIN ETL
# ============================================================

def run_ams_model_etl():
    print("🚀 AMS MODEL ETL STARTED (MODEL-BASED)")

    ams_dir = resolve_ams_dir()
    if not ams_dir:
        raise RuntimeError("❌ AMS DIRECTORY NOT FOUND")

    records = []

    # --------------------------------------------------------
    # DETECT WEEK FOLDERS
    # --------------------------------------------------------
    week_dirs = []
    for d in ams_dir.iterdir():
        if d.is_dir():
            m = re.search(r"\d+", d.name)
            if m:
                week_dirs.append((int(m.group()), d))

    if not week_dirs:
        raise RuntimeError("❌ AMS DIR FOUND BUT NO WEEK FOLDERS")

    # 🔒 LATEST WEEK ONLY
    week_dirs = [max(week_dirs, key=lambda x: x[0])]

    # --------------------------------------------------------
    # PROCESS WEEK
    # --------------------------------------------------------
    for week_no, week_dir in week_dirs:
        week = int(week_no)
        print(f"\n📥 Processing AMS Week {week}")

        for brand_dir in [d for d in week_dir.iterdir() if d.is_dir()]:
            brand = normalize_brand(brand_dir.name)

            try:
                files = list(brand_dir.rglob("*.xlsx"))
                if not files:
                    print(f"⚠ No Excel files for brand: {brand}")
                    continue

                df_list = []
                for f in files:
                    try:
                        df_list.append(pd.read_excel(f))
                    except Exception:
                        print(f"⚠ Failed reading {f}")
                        traceback.print_exc()

                if not df_list:
                    continue

                df = pd.concat(df_list, ignore_index=True)

            except Exception:
                print(f"❌ Failed reading AMS Excel for Week {week} / {brand}")
                traceback.print_exc()
                continue

            # ------------------------------------------------
            # NORMALIZE COLUMNS
            # ------------------------------------------------
            df.columns = [c.strip().lower() for c in df.columns]

            # ------------------------------------------------
            # SALES (TRUTH)
            # ------------------------------------------------
            if "ordered_product_sales" in df.columns:
                df["ordered_product_sales"] = pd.to_numeric(
                    df["ordered_product_sales"], errors="coerce"
                ).fillna(0)
            else:
                df["ordered_product_sales"] = 0

            # ------------------------------------------------
            # UNITS (FIXED)
            # ------------------------------------------------
            unit_col = resolve_units_column(df)
            if unit_col:
                df["units"] = pd.to_numeric(
                    df[unit_col], errors="coerce"
                ).fillna(0)
            else:
                print(f"⚠ Units column not found (Week {week} / {brand}) → default 0")
                df["units"] = 0

            # ------------------------------------------------
            # MODEL COLUMN (FIXED)
            # ------------------------------------------------
            model_col = resolve_model_column(df)
            if not model_col:
                print(f"⚠ No model column (Week {week} / {brand}) — skipped")
                continue

            df["model"] = df[model_col].apply(normalize_model)
            df = df[df["model"].notna() & (df["model"] != "")]

            if df.empty:
                print(f"⚠ Empty after model normalization (Week {week} / {brand})")
                continue

            # ------------------------------------------------
            # STANDARD RENAMES
            # ------------------------------------------------
            df = df.rename(columns={
                "sessions - total": "sessions",
                "featured offer percentage": "buybox_pct",
            })

            # ------------------------------------------------
            # TYPE SAFETY
            # ------------------------------------------------
            df["brand"] = brand
            df["sessions"] = pd.to_numeric(
                df.get("sessions", 0), errors="coerce"
            ).fillna(0)
            df["buybox_pct"] = pd.to_numeric(
                df.get("buybox_pct", 0), errors="coerce"
            ).fillna(0)

            # ------------------------------------------------
            # AGGREGATION
            # ------------------------------------------------
            agg = (
                df.groupby(["brand", "model"], as_index=False)
                .agg({
                    "sessions": "sum",
                    "units": "sum",
                    "buybox_pct": "mean",
                    "ordered_product_sales": "sum",
                })
            )

            if agg.empty:
                continue

            # ------------------------------------------------
            # DERIVED METRICS
            # ------------------------------------------------
            agg["gmv"] = agg["ordered_product_sales"]
            agg["conversion_pct"] = agg.apply(
                lambda x: x["units"] / x["sessions"]
                if x["sessions"] > 0 else None,
                axis=1,
            )

            agg["week"] = week

            records.append(
                agg[
                    [
                        "week",
                        "brand",
                        "model",
                        "sessions",
                        "units",
                        "gmv",
                        "buybox_pct",
                        "conversion_pct",
                    ]
                ]
            )

    # --------------------------------------------------------
    # FINALIZE
    # --------------------------------------------------------
    if not records:
        print("\n❌ AMS ETL completed — no usable data found")
        return

    out = pd.concat(records, ignore_index=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)

    print(f"\n✅ AMS MODEL SNAPSHOT WRITTEN → {OUT_FILE}")
    print(f"📊 Rows written: {len(out)}")

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    run_ams_model_etl()
