import pandas as pd
from pathlib import Path

# =====================================================
# PATHS
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]  # FastAPI

RAW_BASE = BASE_DIR / "data" / "raw" / "sales"
MASTER_FILE = BASE_DIR / "data" / "master" / "sku_master.xlsx"
PROCESSED = BASE_DIR / "data" / "processed"

print("MASTER_FILE =>", MASTER_FILE)
print("Exists?", MASTER_FILE.exists())



PROCESSED.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = PROCESSED / "weekly_sales_snapshot.csv"

print("🚀 SALES AUTO ETL LOADED — FULL AUTO MODE (HARDENED, MODEL SAFE)")

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
# =====================================================
def load_sku_master():
    """
    SKU MASTER = SINGLE SOURCE OF TRUTH
    model, sku, brand, nlc, categories
    """
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

    for c in ["category_l0", "category_l1", "category_l2"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].apply(clean_category)

    return df[
        [
            "sku",
            "model",
            "brand",
            "nlc",
            "category_l0",
            "category_l1",
            "category_l2",
        ]
    ]


# =====================================================
# ---------------- AMAZON PARSER ----------------------
# =====================================================
def parse_amazon(file, week):
    """
    Amazon Business Report
    MODEL LEVEL aggregation (B2C only)
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
    df["ordered_product_sales"] = df["ordered_product_sales"].apply(clean_money)

    out = (
        df.groupby("model", as_index=False)
        .agg(
            units_sold=("units_ordered", "sum"),
            gross_sales=("ordered_product_sales", "sum"),
        )
    )

    out["channel"] = "Amazon"
    out["week"] = week

    return out


# =====================================================
# -------------- OTHER CHANNEL PARSER -----------------
# =====================================================
def parse_other_channels(file, week):
    """
    Vendor / D2C / Marketplace
    SKU LEVEL ONLY
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
        df["sale_amount"] = df["sale_amount"].apply(clean_money)

        g = (
            df.groupby("sku", as_index=False)
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

    # ---------------- AMAZON ----------------
    amazon_file = week_dir / "amazon_sales.xlsx"
    if amazon_file.exists():
        amazon_model = parse_amazon(amazon_file, week)

        expanded = amazon_model.merge(
            sku_master,
            on="model",
            how="left",
        )
        # BRAND OVERRIDE FROM FOLDER (if present)
        if brand_folder:
            expanded["brand"] = brand_folder.replace("_", " ")

        # 🔒 DEDUPE AMAZON MODEL FANOUT (CRITICAL)
        expanded = expanded.drop_duplicates(subset=["week", "channel", "model", "brand"]
        )


        frames.append(expanded)

    # ---------------- OTHER CHANNELS --------
    other_file = week_dir / "other_channels.xlsx"
    if other_file.exists():
        other = parse_other_channels(other_file, week)

        other = other.merge(
            sku_master,
            on="sku",
            how="left",
        )
        # BRAND OVERRIDE FROM FOLDER (if present)
        if brand_folder:
            other["brand"] = brand_folder.replace("_", " ")


        # 🔒 DEDUPE OTHER CHANNELS (SKU-LEVEL)
        other = other.drop_duplicates(subset=["week", "channel", "sku", "brand"])
        frames.append(other)

    if not frames:
        print(f"[ETL] ⚠ No sales files for {week}")
        return pd.DataFrame()

    sales = pd.concat(frames, ignore_index=True)

    # ---------------- FINAL HARDENING ----------------
    for c in ["sku", "model", "brand"]:
        if c in sales.columns:
            sales[c] = sales[c].apply(safe_str)

    sales["brand"] = sales["brand"].replace("nan", "")

    sales["sku_status"] = sales["brand"].apply(
        lambda x: "MAPPED" if x else "UNMAPPED"
    )

    sales["nlc"] = pd.to_numeric(sales["nlc"], errors="coerce").fillna(0)
    sales["units_sold"] = pd.to_numeric(sales["units_sold"], errors="coerce").fillna(0)
    sales["gross_sales"] = pd.to_numeric(sales["gross_sales"], errors="coerce").fillna(0)
    sales["gmv"] = sales["gross_sales"]

    sales["sales_nlc"] = sales["units_sold"] * sales["nlc"]

    # 🔑 MODEL IS PERSISTED (CRITICAL FIX)
    return sales[
        [
            "week",
            "channel",
            "sku",
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
# =====================================================
def run_sales_auto_etl():
    print("🔄 AUTO ETL STARTED")

    sku_master = load_sku_master()
    existing = load_existing_snapshot()

    processed_weeks = set()


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

            if existing.empty or "week" not in existing.columns or "brand" not in existing.columns:
                existing_week = existing.iloc[0:0]
            else:
                existing_week = existing[
                    (existing["week"] == week)
                    & (existing["brand"] == current_brand)
                ]

            has_amazon = (
                not existing_week.empty
                and (existing_week["channel"] == "Amazon").any()
                and week in processed_weeks

            )
            if has_amazon:
                continue

            label = f"{week}/{brand_folder}" if brand_folder else week
            print(f"[ETL] ▶ Processing {label}")

            out = process_week(week, sku_master, brand_folder)

            if not out.empty:
                new_frames.append(out)

    if not new_frames:
        print("⚠ No new frames generated — check raw files & brand folders")
        return None

    combined = pd.concat(new_frames, ignore_index=True)
    combined = combined.groupby(["week","brand","model","channel"], as_index=False).sum()
    combined = combined.drop_duplicates(subset=["week","channel","sku","model","brand"], keep="first")
    combined["brand"] = combined["brand"].astype(str).str.strip().str.title()



    combined.to_csv(OUTPUT_FILE, index=False)
    print("✅ AUTO ETL COMPLETE")

    return combined


if __name__ == "__main__":
    run_sales_auto_etl()
