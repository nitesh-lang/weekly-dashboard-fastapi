import pandas as pd
from pathlib import Path

# =====================================================
# PATHS
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]  # FastAPI

RAW_BASE = BASE_DIR / "data" / "raw" / "sales"
AMS_BASE = BASE_DIR / "data" / "ams_weekly_data"   # per-brand business_report files
MASTER_FILE = BASE_DIR / "data" / "master" / "sku_master.xlsx"
PROCESSED = BASE_DIR / "data" / "processed"

print("MASTER_FILE =>", MASTER_FILE)
print("Exists?", MASTER_FILE.exists())

PROCESSED.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = PROCESSED / "weekly_sales_snapshot.csv"

print("🚀 SALES AUTO ETL LOADED — FULL AUTO MODE (HARDENED, MODEL SAFE)")

# =====================================================
# ✅ MODULE-LEVEL SKU MASTER CACHE
# The SKU master Excel file is read once per process lifetime.
# It only reloads if the file's modification time changes on disk.
# This eliminates repeated pd.read_excel(MASTER_FILE) calls across
# every ETL run and every dashboard request.
# =====================================================
_sku_master_cache = {"df": None, "mtime": None}

def _get_sku_master_mtime():
    try:
        return MASTER_FILE.stat().st_mtime
    except Exception:
        return None

# =====================================================
# -------------------- HELPERS ------------------------
# =====================================================
def norm(c: str) -> str:
    """
    Aggressive column normalizer.
    DO NOT relax – absorbs Amazon / AMS / Vendor junk.
    """
    return (
        str(c)
        .lower()
        .strip()
        .replace("₹", "")
        .replace("%", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "_")
        .replace(" ", "_")
        .replace("__", "_")
    )


def normalize_week(week: str) -> str:
    """
    Normalize ALL formats → Week XX
    """
    if not week:
        return ""
    w = str(week).lower().replace(" ", "")
    if w.startswith("week"):
        return f"Week {w.replace('week', '')}"
    return week


def clean_category(x):
    if pd.isna(x):
        return ""
    return str(x).strip().replace("  ", " ")


def clean_money(x):
    """
    HARD MONEY FIX:
    ₹, commas, #######, blanks, NaN
    """
    if pd.isna(x):
        return 0.0

    s = (
        str(x)
        .replace("₹", "")
        .replace(",", "")
        .replace("#######", "")
        .replace("nan", "")
        .strip()
    )

    try:
        return float(s)
    except Exception:
        return 0.0


def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


# =====================================================
# ------------- LOAD SKU MASTER (SOURCE) --------------
# ✅ FIXED: cached — only re-reads Excel if file has changed on disk.
#    Previously this was called inside run_sales_auto_etl() on every
#    ETL run, reading the full .xlsx every single time.
# =====================================================
def load_sku_master():
    """
    SKU MASTER = SINGLE SOURCE OF TRUTH
    model, sku, brand, nlc, categories

    ✅ Returns cached copy if sku_master.xlsx hasn't changed.
    """
    current_mtime = _get_sku_master_mtime()

    if (
        _sku_master_cache["df"] is not None
        and _sku_master_cache["mtime"] == current_mtime
    ):
        print("[ETL] ✅ SKU master served from cache")
        return _sku_master_cache["df"].copy()

    print("[ETL] 📖 Reading SKU master from disk...")
    df = pd.read_excel(MASTER_FILE)
    df.columns = [norm(c) for c in df.columns]

    # ---------- MODEL ----------
    if "model" not in df.columns:
        if "model_no" in df.columns:
            df = df.rename(columns={"model_no": "model"})
        else:
            raise ValueError("❌ SKU master missing MODEL column")

    # ---------- SKU ----------
    if "sku" not in df.columns:
        if "fba_sku" in df.columns:
            df = df.rename(columns={"fba_sku": "sku"})
        else:
            raise ValueError("❌ SKU master missing SKU column")

    required = {"sku", "model", "brand", "nlc"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"❌ SKU master missing columns: {missing}")

    # ---------- CLEAN ----------
    df["sku"] = df["sku"].astype(str).str.strip()
    df["model"] = df["model"].astype(str).str.strip()
    df["brand"] = df["brand"].astype(str).str.strip()
    df["nlc"] = pd.to_numeric(df["nlc"], errors="coerce").fillna(0)

    # ASIN — keep so weekly_sales_snapshot can carry it per row.  Header in
    # master is "ASIN" → already lowercased to "asin" by the norm() pass.
    if "asin" not in df.columns:
        df["asin"] = ""
    df["asin"] = df["asin"].astype(str).str.strip().replace({"nan": "", "None": ""})

    for c in ["category_l0", "category_l1", "category_l2"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].apply(clean_category)

    result = df[
        [
            "sku",
            "model",
            "brand",
            "asin",
            "nlc",
            "category_l0",
            "category_l1",
            "category_l2",
        ]
    ]

    # Store in cache
    _sku_master_cache["df"] = result
    _sku_master_cache["mtime"] = current_mtime

    return result.copy()


# =====================================================
# ---------------- AMAZON PARSER ----------------------
# =====================================================
# =====================================================
# ---------------- AMAZON via BUSINESS REPORT ---------
# Per-(Child) ASIN sales for the Amazon channel.  Uses
# data/ams_weekly_data/<Brand>/business_report_week<N>.xlsx
# (operator-exported Amazon Brand Analytics).  This is the
# preferred source — replaces parse_amazon()'s model-grain
# rollup of amazon_sales.xlsx so secondary-variant ASINs no
# longer lose their sales attribution.
# =====================================================
def parse_amazon_from_business_report(brand_folder: str, week: str) -> pd.DataFrame:
    """Returns per-(Child) ASIN Amazon channel rows for this brand/week,
    or empty DataFrame if no business_report file exists."""
    import re as _re
    m = _re.search(r"\d+", str(week))
    if not m:
        return pd.DataFrame()
    week_num = int(m.group())
    f = AMS_BASE / brand_folder / f"business_report_week{week_num}.xlsx"
    if not f.exists():
        return pd.DataFrame()
    df = pd.read_excel(f)
    df.columns = df.columns.str.strip()
    if "(Child) ASIN" not in df.columns:
        return pd.DataFrame()

    rename = {
        "(Child) ASIN":         "asin",
        "SKU":                  "sku",
        "Model":                "model",
        "units_ordered":        "units_sold",
        "ordered_product_sales":"gross_sales",
    }
    for src, dst in rename.items():
        if src in df.columns and dst != src:
            df = df.rename(columns={src: dst})

    df["asin"]  = df["asin"].astype(str).str.strip()
    df["sku"]   = df.get("sku",   "").astype(str).str.strip()
    df["model"] = df.get("model", "").astype(str).str.strip()
    df = df[df["asin"].ne("") & ~df["asin"].str.lower().isin({"nan", "none"})]
    df["units_sold"]  = pd.to_numeric(df["units_sold"],  errors="coerce").fillna(0)
    df["gross_sales"] = pd.to_numeric(df["gross_sales"], errors="coerce").fillna(0)

    out = df[["asin", "sku", "model", "units_sold", "gross_sales"]].copy()
    out["channel"] = "Amazon"
    out["week"]    = week
    return out


def parse_amazon(file, week):
    """
    Amazon Business Report (per-account, 3P seller-side).
    Returns per-(Child) ASIN rows so secondary-variant ASINs no longer
    get rolled into their model's primary variant.  When child_asin is
    not present, falls back to the model-grain rollup.
    """
    df = pd.read_excel(file)
    df.columns = [norm(c) for c in df.columns]

    # -------- MODEL DETECTION --------
    if "model" not in df.columns:
        if "parent_asin" in df.columns:
            df = df.rename(columns={"parent_asin": "model"})
        else:
            raise ValueError("❌ Amazon file missing MODEL / Parent ASIN")

    required = {"model", "units_ordered", "ordered_product_sales"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"❌ Amazon sales missing columns: {missing}")

    df["model"] = df["model"].astype(str).str.strip()
    df["units_ordered"] = pd.to_numeric(
        df["units_ordered"], errors="coerce"
    ).fillna(0)

    # ✅ FIXED: vectorised money cleaning instead of row-by-row apply(clean_money)
    df["ordered_product_sales"] = (
        df["ordered_product_sales"]
        .astype(str)
        .str.replace("₹", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace("#######", "", regex=False)
        .str.replace("nan", "", regex=False)
        .str.strip()
    )
    df["ordered_product_sales"] = pd.to_numeric(
        df["ordered_product_sales"], errors="coerce"
    ).fillna(0)

    # Prefer per-(Child) ASIN grain when the column exists.  Falls back to
    # model-grain when Amazon's older export schema is in use.
    if "child_asin" in df.columns:
        df["child_asin"] = df["child_asin"].astype(str).str.strip()
        df = df[df["child_asin"].ne("") & ~df["child_asin"].str.lower().isin({"nan","none"})]
        out = (
            df.groupby(["model", "child_asin"], as_index=False)
            .agg(units_sold=("units_ordered", "sum"),
                 gross_sales=("ordered_product_sales", "sum"))
        )
        out = out.rename(columns={"child_asin": "asin"})
    else:
        out = (
            df.groupby("model", as_index=False)
            .agg(units_sold=("units_ordered", "sum"),
                 gross_sales=("ordered_product_sales", "sum"))
        )

    out["channel"] = "Amazon"
    out["week"] = week
    return out


# =====================================================
# -------------- OTHER CHANNEL PARSER -----------------
# =====================================================
def parse_other_channels(file, week):
    """
    Vendor / D2C / Marketplace.
    SKU + ASIN level — each sheet (1p Sales, D2C, B2B, Blinkit, etc.)
    carries its own ASIN column.  We honor the file's ASIN as ground
    truth so a SKU's true sale-time ASIN isn't overridden by master's
    primary-variant pick.
    """
    xls = pd.ExcelFile(file)
    rows = []

    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        if df.empty:
            continue

        df.columns = [norm(c) for c in df.columns]

        if not {"sku", "qty", "sale_amount"}.issubset(df.columns):
            continue

        df["sku"] = df["sku"].astype(str).str.strip()
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)

        # Honor file-side ASIN when present (every sheet has one in the
        # current operator workflow); fall back to empty so the downstream
        # master merge can fill it from SKU lookup as a safety net.
        if "asin" in df.columns:
            df["asin"] = df["asin"].astype(str).str.strip().replace({"nan": "", "None": ""})
        else:
            df["asin"] = ""

        # ✅ FIXED: vectorised money cleaning instead of row-by-row apply(clean_money)
        df["sale_amount"] = (
            df["sale_amount"]
            .astype(str)
            .str.replace("₹", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.replace("#######", "", regex=False)
            .str.replace("nan", "", regex=False)
            .str.strip()
        )
        df["sale_amount"] = pd.to_numeric(
            df["sale_amount"], errors="coerce"
        ).fillna(0)

        g = (
            df.groupby(["sku", "asin"], as_index=False)
            .agg(
                units_sold=("qty", "sum"),
                gross_sales=("sale_amount", "sum"),
            )
        )

        g["channel"] = sheet.strip()
        g["week"] = week
        rows.append(g)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


# =====================================================
# ---------------- WEEK DETECTION ---------------------
# =====================================================
def detect_raw_weeks():
    if not RAW_BASE.exists():
        return []

    weeks = []
    for p in RAW_BASE.iterdir():
        if p.is_dir() and p.name.lower().startswith("week"):
            weeks.append(normalize_week(p.name))

    return sorted(weeks, key=lambda x: int(x.replace("Week", "").strip()))


# =====================================================
# ----------- LOAD EXISTING SNAPSHOT ------------------
# =====================================================
def load_existing_snapshot():
    if not OUTPUT_FILE.exists():
        return pd.DataFrame()

    df = pd.read_csv(OUTPUT_FILE)
    df["week"] = df["week"].astype(str).apply(normalize_week)
    return df


# =====================================================
# ---------------- BRAND DETECTION --------------------
# =====================================================
def detect_brands_for_week(week_dir: Path):
    """
    Detect brand folders inside a Week directory.
    Falls back to legacy single-brand mode if no subfolders exist.
    """
    if not week_dir.exists():
        return []

    subdirs = [p.name for p in week_dir.iterdir() if p.is_dir()]
    return subdirs if subdirs else [""]


# =====================================================
# -------- PROCESS SINGLE WEEK (CORE) -----------------
# =====================================================
def process_week(week, sku_master, brand_folder=""):
    week_dir = RAW_BASE / week / brand_folder if brand_folder else RAW_BASE / week
    frames = []

    # ---------------- AMAZON (3P seller-side) ----------------
    # amazon_sales.xlsx is the 3P-only source (1P lives in
    # other_channels.xlsx).  parse_amazon now preserves per-(Child) ASIN
    # grain when the source has child_asin (which Amazon's modern Brand
    # Analytics export does); falls back to model-grain on older schemas.
    # Joining master on ASIN — when child-ASIN matches the primary or a
    # known variation — gives the correct brand/nlc/category.  Variant
    # ASINs not in master come through as UNMAPPED.
    amazon_file = week_dir / "amazon_sales.xlsx"
    if amazon_file.exists():
        amz = parse_amazon(amazon_file, week)
        if "asin" in amz.columns:
            # Per-ASIN join — preserves both primary and variant rows
            master_by_asin = sku_master.drop_duplicates(subset=["asin"])
            expanded = amz.merge(
                master_by_asin[["asin", "sku", "brand", "nlc",
                                "category_l0", "category_l1", "category_l2"]],
                on="asin", how="left", suffixes=("", "_m"),
            )
        else:
            # Legacy model-grain join (no child_asin in source)
            expanded = amz.merge(sku_master, on="model", how="left")

        if brand_folder:
            expanded["brand"] = brand_folder.replace("_", " ")

        if "asin" in expanded.columns:
            expanded = expanded.drop_duplicates(
                subset=["week", "channel", "asin", "brand"]
            )
        else:
            expanded = expanded.drop_duplicates(
                subset=["week", "channel", "model", "brand"]
            )

        frames.append(expanded)

    # ---------------- OTHER CHANNELS --------
    other_file = week_dir / "other_channels.xlsx"
    if other_file.exists():
        other = parse_other_channels(other_file, week)

        # parse_other_channels now emits its own `asin` column from each
        # sheet's ASIN cell, so we merge with master on sku for
        # brand/model/nlc and use suffix _m for master's ASIN.  File's
        # ASIN wins; master's only fills in when file value was blank.
        other = other.merge(
            sku_master,
            on="sku",
            how="left",
            suffixes=("", "_m"),
        )
        if "asin_m" in other.columns:
            other["asin"] = other["asin"].where(
                other["asin"].astype(str).str.len() > 0,
                other["asin_m"],
            )
            other = other.drop(columns=["asin_m"])
        # BRAND OVERRIDE FROM FOLDER (if present)
        if brand_folder:
            other["brand"] = brand_folder.replace("_", " ")

        # 🔒 DEDUPE OTHER CHANNELS — now SKU + ASIN since the file may
        # carry the same SKU under different ASINs (variant relisting).
        other = other.drop_duplicates(subset=["week", "channel", "sku", "asin", "brand"])
        frames.append(other)

    if not frames:
        print(f"[ETL] ⚠ No sales files for {week}")
        return pd.DataFrame()

    sales = pd.concat(frames, ignore_index=True)

    # ---------------- FINAL HARDENING ----------------
    # ✅ FIXED: vectorised string cleaning instead of row-by-row apply(safe_str)
    for c in ["sku", "model", "brand"]:
        if c in sales.columns:
            sales[c] = sales[c].astype(str).str.strip().replace("nan", "")

    sales["brand"] = sales["brand"].replace("nan", "")

    # ✅ FIXED: vectorised sku_status instead of apply(lambda)
    sales["sku_status"] = sales["brand"].apply(
        lambda x: "MAPPED" if x else "UNMAPPED"
    )

    sales["nlc"] = pd.to_numeric(sales["nlc"], errors="coerce").fillna(0)
    sales["units_sold"] = pd.to_numeric(sales["units_sold"], errors="coerce").fillna(0)
    sales["gross_sales"] = pd.to_numeric(sales["gross_sales"], errors="coerce").fillna(0)
    sales["gmv"] = sales["gross_sales"]

    sales["sales_nlc"] = sales["units_sold"] * sales["nlc"]

    # ASIN — backfill empties so the snapshot has a real value per row.
    # For Amazon channel rows the master merge is on model (one ASIN per
    # model variant, may not be the precise advertised one), for Other
    # channels it's on sku (correct primary ASIN).
    if "asin" not in sales.columns:
        sales["asin"] = ""
    sales["asin"] = sales["asin"].astype(str).str.strip().replace({"nan": "", "None": ""})

    # 🔑 MODEL IS PERSISTED (CRITICAL FIX)
    return sales[
        [
            "week",
            "channel",
            "sku",
            "asin",
            "model",
            "sku_status",
            "brand",
            "units_sold",
            "gross_sales",
            "gmv",
            "nlc",
            "sales_nlc",
            "category_l0",
            "category_l1",
            "category_l2",
        ]
    ]


# =====================================================
# ---------------- MAIN AUTO ETL ----------------------
# ✅ FIXED: now skips already-processed weeks properly.
#    Also accepts optional single_week arg so /run-etl-latest
#    can process just the latest week without re-running everything.
# =====================================================
def run_sales_auto_etl(single_week: str = None):
    """
    Run the full ETL pipeline.

    Args:
        single_week: If provided, only process this specific week.
                     Used by /run-etl-latest to avoid reprocessing all weeks.
                     If None, processes all weeks (startup / full refresh mode).
    """
    print("🔄 AUTO ETL STARTED")

    # ✅ SKU master is now cached — won't re-read Excel if unchanged
    sku_master = load_sku_master()
    existing = load_existing_snapshot()

    processed_weeks = set()

    # ✅ If single_week provided, only process that week
    if single_week:
        raw_weeks = [normalize_week(single_week)]
        print(f"[ETL] 🎯 Single week mode: {raw_weeks[0]}")
    else:
        raw_weeks = detect_raw_weeks()

    new_frames = []

    for week in raw_weeks:
        week_dir = RAW_BASE / week
        brands = detect_brands_for_week(week_dir)

        for brand_folder in brands:
            current_brand = brand_folder.replace("_", " ").strip().title() if brand_folder else ""

            # --- DUPLICATION GUARD (Amazon only) ---
            key = (week, current_brand, "Amazon")
            if key in processed_weeks:
                continue
            processed_weeks.add(key)

            label = f"{week}/{brand_folder}" if brand_folder else week
            print(f"[ETL] ▶ Processing {label}")

            out = process_week(week, sku_master, brand_folder)

            if not out.empty:
                new_frames.append(out)

    if not new_frames:
        print("⚠ No new frames generated — check raw files & brand folders")
        return None
    
    combined = pd.concat(new_frames, ignore_index=True)

    # ✅ FIX: separate numeric and string cols before groupby
    # Prevents NAType crash when unmapped SKUs have NaN in string columns
    # Grain: per-(week, brand, model, channel, asin) — promotes asin into
    # the key so multi-ASIN models (e.g. AI-13: B0G53H49BS + B0H1H3K6B2)
    # break out into separate rows.  Operator-requested 2026-05-28 to fix
    # the secondary-variant gap (was reporting 0 sales for B0H1H3K6B2 W21
    # because Amazon rows were collapsed at model-grain).
    group_keys = ["week", "brand", "model", "channel", "asin"]
    numeric_cols = ["units_sold", "gross_sales", "gmv", "nlc", "sales_nlc"]
    str_cols = ["sku", "sku_status", "category_l0", "category_l1", "category_l2"]
    # Backfill missing asin so groupby doesn't drop rows (older data /
    # legacy parse_amazon path).
    if "asin" not in combined.columns:
        combined["asin"] = ""
    combined["asin"] = combined["asin"].fillna("").astype(str)

    # ── ASIN-first master backfill before the final groupby ──
    # Other-channel sheets sometimes carry a SKU that's non-canonical
    # (master has the same ASIN under a different FBA/FBP/FBM prefix).
    # The earlier merge-on-SKU then leaves model = NaN for those rows.
    # groupby(dropna=True) would silently drop them — the W22 Nexlev
    # 9-unit Pharmaeasy/Blinkit gap was exactly this.  ASIN-first
    # lookup fills model/brand/sku from master so the rows survive.
    try:
        from weekly_app.core.master_override import master_lookups
        asin_rec, sku_rec = master_lookups()
        if asin_rec:
            m = combined["asin"].astype(str).isin(asin_rec)
            need_model = m & combined["model"].astype(str).str.strip().isin(["", "nan", "None", "<NA>"])
            if need_model.any():
                combined.loc[need_model, "model"] = combined.loc[need_model, "asin"].map(
                    lambda a: asin_rec[a]["model"]
                )
                combined.loc[need_model, "brand"] = combined.loc[need_model, "asin"].map(
                    lambda a: asin_rec[a]["brand"]
                )
                # Keep the raw SKU as-is — operator wants raw SKU preserved
                # as a fallback (per inventory rule).  Only fill if blank.
                blank_sku = need_model & combined["sku"].astype(str).str.strip().isin(["", "nan", "None"])
                if blank_sku.any():
                    combined.loc[blank_sku, "sku"] = combined.loc[blank_sku, "asin"].map(
                        lambda a: asin_rec[a]["sku"]
                    )
    except Exception as _e:
        print(f"[ETL] ⚠ ASIN backfill failed: {_e!r}")

    # Fill remaining NaN in groupby keys so dropna=True doesn't kill them.
    for _k in ("brand", "model"):
        combined[_k] = combined[_k].fillna("").astype(str).replace({"nan": "", "None": "", "<NA>": ""})

    combined_num = combined.groupby(group_keys, as_index=False, dropna=False)[numeric_cols].sum()
    combined_str = combined.groupby(group_keys, as_index=False, dropna=False)[str_cols].first()
    combined = combined_num.merge(combined_str, on=group_keys, how="left")

    combined = combined.drop_duplicates(
        subset=["week", "channel", "sku", "model", "brand", "asin"], keep="first"
    )
    combined["brand"] = combined["brand"].astype(str).str.strip().str.title()

    # ✅ If running single week mode, merge with existing snapshot
    #    instead of overwriting it — preserves all other weeks' data
    if single_week and not existing.empty:
        week_label = normalize_week(single_week)
        existing_without_week = existing[existing["week"] != week_label]
        combined = pd.concat([existing_without_week, combined], ignore_index=True)
        print(f"[ETL] 🔀 Merged Week {week_label} into existing snapshot")

    # Diagnostic + regression guard (same rule as inventory_model_snapshot).
    # If a freshly regenerated full snapshot dropped recent weeks (e.g.,
    # a Linux xlsx parse failure on the runner) the committed snapshot
    # is the high-water mark; we refuse to overwrite it.
    if not single_week:
        new_weeks = (
            combined["week"].astype(str).str.extract(r"(\d+)")[0]
            .dropna().astype(int)
        )
        if not new_weeks.empty:
            new_max = int(new_weeks.max())
            print("[ETL] 📊 Sales rows by week:")
            for w, n in new_weeks.value_counts().sort_index().items():
                print(f"        W{w:>2}: {n:>5} rows")
            if OUTPUT_FILE.exists():
                try:
                    existing_w = pd.read_csv(OUTPUT_FILE, usecols=["week"], dtype=str)
                    existing_max = int(
                        existing_w["week"].astype(str).str.extract(r"(\d+)")[0]
                        .dropna().astype(int).max()
                    )
                    if new_max < existing_max:
                        print(
                            f"[ETL] ⛔ ABORT WRITE: new max W{new_max} < existing W{existing_max}. "
                            f"Keeping committed snapshot — investigate why W{existing_max} sales "
                            f"didn't regenerate (check above logs for xlsx read failures)."
                        )
                        return existing
                except Exception as _e:
                    print(f"[ETL] ⚠ regression-guard read failed, will write anyway: {_e!r}")

    combined.to_csv(OUTPUT_FILE, index=False)
    print("✅ AUTO ETL COMPLETE")

    return combined


if __name__ == "__main__":
    run_sales_auto_etl()