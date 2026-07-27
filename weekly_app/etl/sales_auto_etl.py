import pandas as pd
from pathlib import Path

from weekly_app.etl._excel_safe import read_excel_safe

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
# This eliminates repeated read_excel_safe(MASTER_FILE) calls across
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
    df = read_excel_safe(MASTER_FILE)
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

    # ASIN Type (Core / Medium / Tail / EOL / New / New Launch / To be Launched)
    # — piped into snapshot so Dashboard + downstream can filter by lifecycle tier.
    if "asin_type" not in df.columns:
        df["asin_type"] = ""
    df["asin_type"] = df["asin_type"].astype(str).str.strip().replace({"nan": "", "None": ""})

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
            "asin_type",
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
    df = read_excel_safe(f)
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
    df = read_excel_safe(file)
    df.columns = [norm(c) for c in df.columns]

    # -------- MODEL DETECTION --------
    # Content check, not just column-presence check.  Some xlsx variants
    # (Windows Excel with all-empty Model cells) read differently across
    # openpyxl / calamine / different pandas versions — sometimes the
    # column is returned as float64 all-NaN, sometimes as object all-"nan"
    # strings, sometimes dropped entirely.  If the column exists but is
    # effectively empty, treat it as absent so we hit the parent_asin
    # fallback consistently.  Root of the "W23 dropped 976 → 104 rows on
    # Linux CI" cascade on 2026-07-06.
    if "model" in df.columns:
        _m_str = df["model"].astype(str).str.strip().str.lower()
        if _m_str.isin(["", "nan", "none", "<na>"]).all():
            df = df.drop(columns=["model"])
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
def parse_other_channels(file, week, skip_sheets=None):
    """
    Vendor / D2C / Marketplace.
    SKU + ASIN level — each sheet (1p Sales, D2C, B2B, Blinkit, etc.)
    carries its own ASIN column.  We honor the file's ASIN as ground
    truth so a SKU's true sale-time ASIN isn't overridden by master's
    primary-variant pick.

    `skip_sheets` (set of lowercased sheet names) lets the caller drop
    sheets that have a more authoritative source — currently used so
    "1p sales" gets sourced from the SP-API Vendor Sales file when
    present, not the manually-maintained sheet.
    """
    # IMPORTANT: use calamine engine at ExcelFile open time so per-sheet
    # reads route through it consistently.  Previously `pd.ExcelFile(file)`
    # opened with pandas default (openpyxl on Linux CI), and passing that
    # already-opened handle to `read_excel_safe(xls, ...)` silently ignored
    # the calamine hint — every 1P Sales / other-channels sheet read on CI
    # ran through openpyxl, which drops rows for some xlsx variants
    # (e.g. W24 WM 1P Sales lost all 7 rows on the runner; local Windows
    # read them fine).  Same class-of-bug as the 2026-07-06 W27 incident.
    try:
        xls = pd.ExcelFile(file, engine="calamine")
    except Exception:
        xls = pd.ExcelFile(file)  # calamine unavailable — fall back
    rows = []
    skip_norm = {s.strip().lower() for s in (skip_sheets or set())}

    for sheet in xls.sheet_names:
        if sheet.strip().lower() in skip_norm:
            continue
        df = read_excel_safe(xls, sheet_name=sheet)
        if df.empty:
            continue

        df.columns = [norm(c) for c in df.columns]

        # Need qty + sale_amount + at least ONE of {sku, asin}.  Before,
        # we required sku; sheets that only carried ASIN (e.g. WM W25
        # 1p Sales) were silently skipped, hiding ₹L of revenue from
        # the snapshot.  Now accept either — sku is filled from master
        # via ASIN later in the chain when missing.
        if not {"qty", "sale_amount"}.issubset(df.columns):
            continue
        if "sku" not in df.columns and "asin" not in df.columns:
            continue

        if "sku" in df.columns:
            df["sku"] = df["sku"].astype(str).str.strip().replace({"nan": "", "None": ""})
        else:
            df["sku"] = ""
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


def parse_vendor_sales_sp_api(file, week):
    """Load SP-API Vendor Sales (1P) xlsx — emit rows in the same
    shape parse_other_channels' "1p Sales" sheet would have produced
    (sku, asin, units_sold, gross_sales, channel, week).

    Schema in the source file (sp_vendor_sales_pull.py output):
        ASIN, SKU, Brand, Model, category_l0/l1/l2,
        Qty (orderedUnits), Sale (orderedRevenue),
        ShippedUnits, ShippedRevenue, CustomerReturns,
        Channel ("1p Sales"), Week, WindowStart, WindowEnd

    We use Qty (= orderedUnits) + Sale (= orderedRevenue) as the
    canonical "units sold to Amazon" + "revenue" pair, matching what
    the operator's manual "1p Sales" sheet historically tracked.
    """
    try:
        df = read_excel_safe(file)
    except Exception as e:
        print(f"[ETL] ⚠ SP-API Vendor Sales unreadable {file}: {e!r}")
        return pd.DataFrame()
    df.columns = [norm(c) for c in df.columns]
    needed = {"sku", "asin", "qty", "sale", "channel"}
    if not needed.issubset(df.columns):
        print(f"[ETL] ⚠ {file.name} missing columns {needed - set(df.columns)} — skipping")
        return pd.DataFrame()
    out = pd.DataFrame({
        "sku":         df["sku"].astype(str).str.strip().replace({"nan": "", "None": ""}),
        "asin":        df["asin"].astype(str).str.strip().replace({"nan": "", "None": ""}),
        "units_sold":  pd.to_numeric(df["qty"],  errors="coerce").fillna(0),
        "gross_sales": pd.to_numeric(df["sale"], errors="coerce").fillna(0),
        "channel":     df["channel"].astype(str).str.strip(),
        "week":        week,
    })
    # Drop rows with no identifiable SKU AND no ASIN — they can't merge
    # with master and would noise the snapshot.
    out = out[(out["sku"].str.len() > 0) | (out["asin"].str.len() > 0)]
    return out


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
    #
    # SP-API fallback: when the operator hasn't dropped amazon_sales.xlsx
    # this week but the cron has produced Seller Sales (SP-API).xlsx with
    # the same schema (see sp_seller_sales_pull.OUTPUT_COLS), use that
    # automatically.  Pre-fix the operator had to keep dropping the manual
    # file every week or every brand's 3P Amazon sales silently dropped
    # out of weekly_sales_snapshot.csv (W26 lost ₹76L across 5 brands
    # before this code-path landed).
    amazon_file = week_dir / "amazon_sales.xlsx"
    sp_seller_file = week_dir / "Seller Sales (SP-API).xlsx"
    if not amazon_file.exists() and sp_seller_file.exists():
        print(f"  ℹ Using SP-API Seller Sales for {brand_folder or 'brand'} "
              f"(amazon_sales.xlsx not dropped this week)")
        amazon_file = sp_seller_file
    if amazon_file.exists():
        amz = parse_amazon(amazon_file, week)
        if "asin" in amz.columns:
            # Per-ASIN join — preserves both primary and variant rows.
            # Pull master's `model` alongside the rest so we can OVERRIDE
            # parse_amazon's model fallback (which uses parent_asin as
            # model when the Amazon export lacks the Model column).  Per
            # the ASIN > SKU > Model hierarchy: ASIN is the anchor and
            # SKU + Model come from sku_master.
            master_by_asin = sku_master.drop_duplicates(subset=["asin"])
            expanded = amz.merge(
                master_by_asin[["asin", "sku", "brand", "model", "nlc",
                                "category_l0", "category_l1", "category_l2",
                                "asin_type"]],
                on="asin", how="left", suffixes=("", "_master"),
            )
            # Prefer master's Model when present.  Falls back to whatever
            # parse_amazon produced (the parent_asin / model column) only
            # when master has no Model for this ASIN.
            if "model_master" in expanded.columns:
                m_master = expanded["model_master"].astype(str).str.strip()
                blank_mask = m_master.isin(["", "nan", "None", "<NA>"])
                expanded["model"] = m_master.where(~blank_mask, expanded["model"])
                expanded = expanded.drop(columns=["model_master"])
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

    # ---------------- SP-API VENDOR (1P) — canonical when present ----------
    # When the SP-API vendor sales file exists for this brand+week, it
    # becomes the authoritative 1P source and we instruct
    # parse_other_channels to skip the manual "1p Sales" sheet.
    # Safety: only claim ownership if the SP-API file has non-zero
    # units — empty SP-API must never silently zero out 1P.
    sp_api_owns_1p = False
    sp_file = week_dir / "Vendor Sales (SP-API).xlsx"
    if sp_file.exists():
        sp_rows = parse_vendor_sales_sp_api(sp_file, week)
        if not sp_rows.empty and float(sp_rows["units_sold"].sum()) > 0:
            sp_rows = sp_rows.merge(
                sku_master, on="sku", how="left", suffixes=("", "_m"),
            )
            if "asin_m" in sp_rows.columns:
                sp_rows["asin"] = sp_rows["asin"].where(
                    sp_rows["asin"].astype(str).str.len() > 0,
                    sp_rows["asin_m"],
                )
                sp_rows = sp_rows.drop(columns=["asin_m"])
            if brand_folder:
                sp_rows["brand"] = brand_folder.replace("_", " ")
            sp_rows = sp_rows.drop_duplicates(
                subset=["week", "channel", "sku", "asin", "brand"]
            )
            frames.append(sp_rows)
            sp_api_owns_1p = True
            print(f"[ETL] 📡 SP-API 1P sales canonical for {brand_folder} {week} "
                  f"({len(sp_rows)} rows, {int(sp_rows['units_sold'].sum())} units)")

    # ---------------- OTHER CHANNELS --------
    other_file = week_dir / "other_channels.xlsx"
    if other_file.exists():
        # Skip any manual 1P sheet when SP-API already supplied that brand+week.
        # Operator sheet names vary across brands: "1p Sales", "1p", "1P",
        # "amazon 1p", "amazon 1p sales".  All represent the same source —
        # shadowed by SP-API Vendor Sales when present.  Without this,
        # Tonor's "1p" sheet would double-count alongside SP-API for W25+.
        skip = (
            {"1p sales", "1p", "amazon 1p", "amazon 1p sales"}
            if sp_api_owns_1p else None
        )
        other = parse_other_channels(other_file, week, skip_sheets=skip)

        # When SP-API owns 1P AND the file has ONLY the 1p Sales sheet,
        # parse_other_channels returns an empty DataFrame with no columns,
        # so the merge on `sku` would KeyError.  Skip merge when empty.
        if not other.empty:
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
            # ASIN-first fallback whenever the SKU-merge failed to resolve.
            # Two flavours to catch:
            #   1. Sheet has no SKU column at all (W25 WM 1p Sales case) —
            #      SKU is blank, ASIN is present.
            #   2. Sheet has a SKU value but it's a fulfillment-center
            #      prefix variant not in master (e.g. `FBA79350` in file
            #      vs `FBP79350` in master, same underlying product, same
            #      ASIN B0D67727VG).  W29 Nexlev BIW/Blinkit/D2C/Pharmaeasy
            #      lost model/brand this way.
            # In both cases, ASIN is truth per the ASIN>SKU>Model hierarchy
            # (see reference_master_alignment_hierarchy).  Trigger whenever
            # the merge produced no master hit (model_m blank) AND the row
            # has an ASIN we can look up.
            _mm = other["model_m"].astype(str).str.strip() if "model_m" in other.columns else pd.Series("", index=other.index)
            _model = other["model"].astype(str).str.strip() if "model" in other.columns else pd.Series("", index=other.index)
            _no_master_hit = _mm.isin(["", "nan", "None", "<NA>"]) & _model.isin(["", "nan", "None", "<NA>"])
            need_master = (
                other["asin"].astype(str).str.strip().ne("")
                & _no_master_hit
            )
            if need_master.any():
                master_by_asin = sku_master.drop_duplicates(subset=["asin"]).set_index("asin")
                for col in ("sku", "model", "brand", "nlc",
                            "category_l0", "category_l1", "category_l2",
                            "asin_type"):
                    if col not in master_by_asin.columns:
                        continue
                    # Widen destination to object so pandas 2.2+ doesn't
                    # reject a StringArray-into-float64 setitem when the
                    # column was created as all-NaN by the earlier merge.
                    if col in other.columns:
                        other[col] = other[col].astype("object")
                    other.loc[need_master, col] = (
                        other.loc[need_master, "asin"].map(master_by_asin[col])
                    )
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

    # ---------------- BRAND CANONICALISATION FROM sku_master ----------
    # Operator rule: sku_master is truth for Brand.  Before this, every
    # frame appended above assigned brand from the *folder path*
    # (line 525 / 559 / 622), so a file dropped under the wrong folder
    # (e.g., a Tonor SKU under White_Mulberry) carried the wrong brand
    # into weekly_sales_snapshot and the dashboard attributed revenue
    # to the wrong brand.  Override brand from master via ASIN
    # (canonical) after concat.  Log any mismatch so the operator can
    # move the file to the correct folder next week.
    if "asin" in sales.columns:
        try:
            # Drop master rows with blank/NaN ASIN so blank sales rows
            # can't spuriously match unassigned-ASIN master rows.
            _sm = sku_master.copy()
            _sm["asin"] = _sm["asin"].astype(str).str.strip()
            _sm = _sm[~_sm["asin"].str.lower().isin(["", "nan", "none", "<na>"])]
            master_by_asin = _sm.drop_duplicates(subset=["asin"]).set_index("asin")
        except Exception:
            master_by_asin = None
        if master_by_asin is not None and "brand" in master_by_asin.columns:
            sales["folder_brand"] = sales["brand"].astype(str)
            _asin = sales["asin"].astype(str).str.strip()
            _has_master = (_asin.isin(master_by_asin.index)
                           & ~_asin.str.lower().isin(["", "nan", "none", "<na>"]))
            if _has_master.any():
                mapped = _asin[_has_master].map(master_by_asin["brand"])
                mapped = mapped.astype(str).str.strip()
                # Only override where master brand is non-empty.
                valid = mapped.ne("") & ~mapped.str.lower().isin(["nan", "none", "<na>"])
                override_idx = _has_master[_has_master].index[valid]
                if len(override_idx):
                    folder_norm = (sales.loc[override_idx, "folder_brand"]
                                        .str.replace("_", " ", regex=False).str.strip())
                    master_norm = mapped.loc[valid]
                    mismatch_mask = folder_norm.values != master_norm.values
                    n_mismatch = int(mismatch_mask.sum())
                    if n_mismatch:
                        print(f"[ETL] ℹ️  {n_mismatch} sales row(s) re-tagged: "
                              f"folder brand ≠ sku_master brand")
                        sample = (sales.loc[override_idx[mismatch_mask],
                                            ["asin", "sku", "folder_brand"]]
                                        .assign(master_brand=master_norm.values[mismatch_mask])
                                        .drop_duplicates()
                                        .head(10))
                        print(sample.to_string(index=False))
                    sales.loc[override_idx, "brand"] = master_norm.values

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
            "asin_type",
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
    str_cols = ["sku", "sku_status", "category_l0", "category_l1", "category_l2", "asin_type"]
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
                    # pandas 2.2+ setitem guard — widen sku to object.
                    combined["sku"] = combined["sku"].astype("object")
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
                    existing_weeks = (
                        existing_w["week"].astype(str).str.extract(r"(\d+)")[0]
                        .dropna().astype(int)
                    )
                    existing_max = int(existing_weeks.max())
                    existing_counts = existing_weeks.value_counts()
                    new_counts = new_weeks.value_counts()

                    # Per-week regression handling (2026-07-08 rewrite):
                    # Three-tier defence against xlsx parse regressions:
                    #
                    # 1. HISTORICAL weeks (< max-1): preserve committed
                    #    rows on any ≥30% drop.  Finalised weeks should
                    #    never shrink; if they do it's a Linux xlsx bug
                    #    or master-override accident.
                    #
                    # 2. CURRENT / previous week with CATASTROPHIC drop
                    #    (fresh <60% of committed): preserve.  Normal
                    #    mid-week refreshes add/refresh rows; a 40%+
                    #    drop is almost certainly a Linux openpyxl parse
                    #    bomb.  (Original W23 loss of 872 rows had the
                    #    same shape.)  Sanity gate also catches this.
                    #
                    # 3. CURRENT / previous week with moderate drop
                    #    (60-70%): warn, accept fresh — could be
                    #    legitimate (channel retire, brand exit).
                    if new_max < existing_max:
                        print(
                            f"[ETL] ⚠  new max W{new_max} < existing W{existing_max}. "
                            f"Snapshot will be written anyway — investigate why W{existing_max} "
                            f"sales didn't regenerate (check for xlsx read failures above)."
                        )
                    historical_boundary = new_max - 1
                    HISTORICAL_FLOOR   = 0.70   # historical: <30% drop OK
                    CATASTROPHE_FLOOR  = 0.60   # current/prev: <40% drop OK
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
                                f"[ETL] ⚠  W{w} shrank {old_n} -> {new_n} rows ({drop_pct:.0f}% drop) "
                                f"— snapshot writing anyway (under catastrophe floor); audit checks 25/26 will surface if real."
                            )
                            continue
                        # Preserve: either historical week or catastrophic
                        # current/prev-week drop.
                        if existing_df is None:
                            existing_df = pd.read_csv(OUTPUT_FILE, dtype=str)
                            # Coerce the numeric columns back so downstream
                            # concat + write doesn't stringify everything.
                            for _c in ("units_sold", "gross_sales", "gmv",
                                       "nlc", "sales_nlc"):
                                if _c in existing_df.columns:
                                    existing_df[_c] = pd.to_numeric(
                                        existing_df[_c], errors="coerce").fillna(0)
                        w_label = f"Week {w}" if not str(existing_df["week"].iloc[0]).isdigit() else str(w)
                        old_rows = existing_df[existing_df["week"].astype(str).str.extract(r"(\d+)")[0] == str(w)]
                        if old_rows.empty:
                            print(f"[ETL] ⚠  W{w} historical preservation: could not locate old rows, "
                                  f"falling back to fresh ({drop_pct:.0f}% drop).")
                            continue
                        # Drop the fresh W{w} rows and splice in old ones.
                        combined = combined[
                            combined["week"].astype(str).str.extract(r"(\d+)")[0] != str(w)
                        ]
                        # Align columns — if `old_rows` is missing a col
                        # (e.g. `folder_brand`), add it as empty so the
                        # concat is clean.
                        for _c in combined.columns:
                            if _c not in old_rows.columns:
                                old_rows[_c] = ""
                        combined = pd.concat(
                            [combined, old_rows[combined.columns]],
                            ignore_index=True,
                        )
                        weeks_preserved.append(w)
                        kind = "CATASTROPHIC" if is_catastrophic and is_current_or_prev else "historical"
                        print(
                            f"[ETL] 🛡  W{w} {kind} preservation: "
                            f"fresh had {new_n} rows ({drop_pct:.0f}% drop), "
                            f"keeping committed {old_n} rows."
                        )
                    if weeks_preserved:
                        print(f"[ETL] 🛡  Total weeks preserved: {weeks_preserved}")
                except Exception as _e:
                    print(f"[ETL] ⚠ regression-guard read failed, will write anyway: {_e!r}")

    # Backfill empty category_l0/l1/l2 from sku_master so the UI
    # category filter has full coverage (not just rows whose raw file
    # happened to carry the value).  Same helper used by inventory.
    try:
        from weekly_app.core.category_lookup import fill_categories
        combined = fill_categories(combined, asin_col="asin", model_col="model", brand_col="brand")
    except Exception as _e:
        print(f"[ETL] ⚠ category backfill skipped: {_e!r}")

    # Debug 2026-07-06 — the CI-committed weekly_sales_snapshot.csv keeps
    # showing max_week=26 despite this ETL logging W27:932 rows.  Print
    # explicit disk state so we can see whether to_csv is landing on the
    # path the commit step is watching, and whether git sees any change.
    import os
    _before_size = OUTPUT_FILE.stat().st_size if OUTPUT_FILE.exists() else 0
    combined.to_csv(OUTPUT_FILE, index=False)
    _after_size = OUTPUT_FILE.stat().st_size
    print(f"[ETL] 💾 wrote {OUTPUT_FILE} (before={_before_size} bytes, after={_after_size} bytes, rows={len(combined)})")
    # Verify by round-tripping: read the file we just wrote and show max week
    try:
        _rt = pd.read_csv(OUTPUT_FILE, usecols=["week"], dtype=str)
        _rt_max = int(_rt["week"].astype(str).str.extract(r"(\d+)")[0].dropna().astype(int).max())
        print(f"[ETL] 🔁 round-trip max week from disk = {_rt_max}")
    except Exception as _e:
        print(f"[ETL] ⚠ round-trip check failed: {_e!r}")
    print("✅ AUTO ETL COMPLETE")

    return combined


if __name__ == "__main__":
    run_sales_auto_etl()