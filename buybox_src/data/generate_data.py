import os, json
import pandas as pd

BASE       = "./data"
SKU_MASTER = "./data/sku_master.xlsx"

CONFIGS = {
    "Nexlev": {
        "months": {
            "Jan": {"biz": "Nexlev/Jan/business_report.csv",  "ads": "Nexlev/Jan/Sp&Sd.xlsx", "p1": "Nexlev/Jan/1Psales.csv"},
            "Feb": {"biz": "Nexlev/Feb/business_report.csv",  "ads": "Nexlev/Feb/Sp&Sd.xlsx", "p1": "Nexlev/Feb/1Psales.csv"},
            "Mar": {"biz": "Nexlev/Mar/business_report.csv",  "ads": "Nexlev/Mar/Sp&Sd.xlsx", "p1": "Nexlev/Mar/1Psales.csv"},
            "Apr": {"biz": "Nexlev/Apr/business_report.xlsx",  "ads": "Nexlev/Apr/Sp&Sd.xlsx", "p1": "Nexlev/Apr/1Psales.csv"},
        }
    },
    "Audio Array": {
        "months": {
            "Jan": {"biz": "Audio array/Jan/business_report.csv",  "ads": "Audio array/Jan/Sp&Sd.xlsx", "p1": "Audio array/Jan/1Psales.csv"},
            "Feb": {"biz": "Audio array/Feb/business_report.csv",  "ads": "Audio array/Feb/Sp&Sd.xlsx", "p1": "Audio array/Feb/1Psales.csv"},
            "Mar": {"biz": "Audio array/Mar/business_report.csv",  "ads": "Audio array/Mar/Sp&Sd.xlsx", "p1": "Audio array/Mar/1Psales.csv"},
            "Apr": {"biz": "Audio array/Apr/business_report.xlsx",  "ads": "Audio array/Apr/Sp&Sd.xlsx", "p1": "Audio array/Apr/1Psales.csv"},
        }
    },
    "Tonor": {
        "months": {
            "Jan": {"biz": "Tonor/Jan/business_report.csv", "ads": "Tonor/Jan/Sp&Sd.xlsx", "p1": "Tonor/Jan/1Psales.csv"},
            "Feb": {"biz": "Tonor/Feb/business_report.xlsx", "ads": "Tonor/Feb/Sp&Sd.xlsx", "p1": "Tonor/Feb/1PSales.csv"},
            "Mar": {"biz": "Tonor/Mar/business_report.xlsx", "ads": "Tonor/Mar/Sp&Sd.xlsx", "p1": "Tonor/Mar/1Psales.csv"},
            "Apr": {"biz": "Tonor/Apr/business_report.xlsx", "ads": "Tonor/Apr/Sp&Sd.xlsx", "p1": "Tonor/Apr/1Psales.csv"},
        }
    },
    "White Mulberry": {
        "months": {
            "Jan": {"biz": "White Mulberry/Jan/business_report.csv", "ads": "White Mulberry/Jan/Sp&Sd.xlsx", "p1": "White Mulberry/Jan/1Psales.csv"},
            "Feb": {"biz": "White Mulberry/Feb/business_report.xlsx", "ads": "White Mulberry/Feb/Sp&Sd.xlsx", "p1": "White Mulberry/Feb/1Psales.csv"},
            "Mar": {"biz": "White Mulberry/Mar/business_report.xlsx", "ads": "White Mulberry/Mar/Sp&Sd.xlsx", "p1": "White Mulberry/Mar/1Psales.csv"},
            "Apr": {"biz": "White Mulberry/Apr/business_report.xlsx", "ads": "White Mulberry/Apr/Sp&Sd.xlsx", "p1": "White Mulberry/Apr/1Psales.csv"},
        }
    },
}

def safe_float(v):
    try: return float(str(v).replace(",","").replace("%","").replace("$","").replace("₹","").replace("(","").replace(")","").strip())
    except: return 0.0

def read_csv_safe(path):
    for enc in ["utf-8","latin-1","cp1252"]:
        try:
            df = pd.read_csv(path, encoding=enc, dtype=str)
            df.columns = [c.strip() for c in df.columns]
            return df
        except: pass
    return pd.DataFrame()

def read_xlsx_safe(path):
    try:
        df = pd.read_excel(path, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        return df
    except: return pd.DataFrame()

def read_file(path):
    if not os.path.exists(path): return pd.DataFrame()
    if path.endswith('.xlsx') or path.endswith('.xls'):
        return read_xlsx_safe(path)
    return read_csv_safe(path)

def read_p1(path):
    if not os.path.exists(path): return pd.DataFrame()
    for enc in ["utf-8","latin-1","cp1252"]:
        try:
            df = pd.read_csv(path, header=1, encoding=enc, dtype=str)
            df.columns = [c.strip() for c in df.columns]
            return df
        except: pass
    return pd.DataFrame()

def col(df, *names):
    for n in names:
        for c in df.columns:
            if n.lower() in c.lower(): return c
    return None

# ── SKU Master ────────────────────────────────────────────────────────────────
sku_df = read_xlsx_safe(SKU_MASTER)
sku_lookup = {}
asin_c  = col(sku_df,"child asin","asin")
fba_c   = col(sku_df,"fba sku","fba_sku")
model_c = col(sku_df,"model")
# category_l0 = mainCat (Home & Kitchen / HPC etc)
# category_l1 = specific category (Fabric Care, Trimmers etc)
cat_l0_c = next((c for c in sku_df.columns if c.strip().lower()=="category_l0"), None)
cat_l1_c = next((c for c in sku_df.columns if c.strip().lower()=="category_l1"), None)
nlc_c   = col(sku_df,"nlc","net landed")
brand_c = col(sku_df,"brand")

def clean_str(v):
    """Strip whitespace; convert pandas nan/NaT/None strings to empty string."""
    s = str(v).strip() if v is not None else ""
    return "" if s.lower() in ("nan", "nat", "none") else s

for _, row in sku_df.iterrows():
    asin = clean_str(row.get(asin_c, "") if asin_c else "").upper()
    if not asin: continue
    nlc      = safe_float(row.get(nlc_c, 0)) if nlc_c else 0
    main_cat = clean_str(row.get(cat_l0_c, "")) if cat_l0_c else ""
    cat_l1   = clean_str(row.get(cat_l1_c, "")) if cat_l1_c else ""
    # Brand-specific category rule:
    # Nexlev  → use category_l1 (specific subcategory)
    # All others (Audio Array, Tonor, White Mulberry, Fossil) → use category_l0 (broad)
    brand_val = clean_str(row.get(brand_c, "")) if brand_c else ""
    if brand_val == "Nexlev":
        category = cat_l1 if cat_l1 else main_cat
    else:
        category = main_cat if main_cat else cat_l1
    # Skip this row if ASIN already mapped with richer data (duplicate rows in SKU master)
    existing = sku_lookup.get(asin)
    if existing and (existing["category"] or existing["mainCat"]) and not category:
        continue
    sku_lookup[asin] = {
        "fba_sku":  clean_str(row.get(fba_c,  "")) if fba_c   else "",
        "model":    clean_str(row.get(model_c, "")) if model_c else "",
        "category": category,
        "mainCat":  main_cat,
        "dp":       round(nlc / 1.18, 2) if nlc else 0,
    }
print(f"SKU master: {len(sku_lookup)} ASINs loaded")

# ── Main Loop ─────────────────────────────────────────────────────────────────
all_rows = []

for brand_name, cfg in CONFIGS.items():
    for month, files in cfg["months"].items():
        biz_path = os.path.join(BASE, files["biz"])
        ads_path = os.path.join(BASE, files["ads"])
        p1_path  = os.path.join(BASE, files["p1"])

        # ── Business Report ──────────────────────────────────────────────────
        biz_df = read_file(biz_path)
        biz_lookup = {}
        if not biz_df.empty:
            # Use (Child) ASIN — Amazon's business report format
            asin_col  = col(biz_df,"child) asin","child asin") or col(biz_df,"asin")
            sess_col  = col(biz_df,"sessions - total")
            units_col = col(biz_df,"units ordered")
            bb_col    = col(biz_df,"featured offer percentage")
            rev_col   = col(biz_df,"ordered product sales")
            title_col = col(biz_df,"title")
            if asin_col:
                for _, row in biz_df.iterrows():
                    asin = str(row.get(asin_col,"")).strip().upper()
                    if not asin or asin=="NAN" or asin=="(CHILD) ASIN": continue
                    bb_raw = str(row.get(bb_col,"0")).replace("%","") if bb_col else "0"
                    rev   = safe_float(row.get(rev_col,0)) if rev_col else 0
                    units = safe_float(row.get(units_col,0)) if units_col else 0
                    sess  = safe_float(row.get(sess_col,0)) if sess_col else 0
                    bb_val = safe_float(bb_raw)
                    # CSV files have "60.67%" → bb_raw="60.67" → value > 1 → divide by 100
                    # XLSX files have 0.6067 (already decimal) → value ≤ 1 → use as-is
                    bb    = bb_val / 100 if bb_val > 1 else bb_val
                    title = str(row.get(title_col,"")).strip() if title_col else ""
                    if asin in biz_lookup:
                        # ASIN appears multiple times (parent+child rows) — sum numeric fields, keep largest bb/title
                        biz_lookup[asin]["sessions"] += sess
                        biz_lookup[asin]["units_3p"] += units
                        biz_lookup[asin]["rev_3p"]   += rev
                        if bb > biz_lookup[asin]["bb_pct"]:
                            biz_lookup[asin]["bb_pct"] = bb
                        if title and not biz_lookup[asin]["title"]:
                            biz_lookup[asin]["title"] = title
                    else:
                        biz_lookup[asin] = {
                            "sessions": sess,
                            "units_3p": units,
                            "bb_pct":   bb,
                            "rev_3p":   rev,
                            "title":    title,
                        }

        # ── Ads (read SP+SD+SB; use SP_SD_Combined only if it has rows) ────────
        ads_lookup = {}
        if os.path.exists(ads_path):
            try:
                xl = pd.ExcelFile(ads_path)
                combined = xl.parse("SP_SD_Combined", dtype=str) if "SP_SD_Combined" in xl.sheet_names else pd.DataFrame()
                combined.columns = [c.strip() for c in combined.columns]
                combined_asin_col = col(combined, "advertised asin", "asin") if not combined.empty else None
                combined_has_data = bool(
                    combined_asin_col
                    and combined[combined_asin_col].astype(str).str.strip().str.upper().ne("NAN").any()
                )
                sheets_to_read = ["SP_SD_Combined"] if combined_has_data else [s for s in xl.sheet_names if s in ("SP","SD","SB")]
                for sheet in sheets_to_read:
                    ads_df = xl.parse(sheet, dtype=str)
                    if ads_df.empty: continue
                    ads_df.columns = [c.strip() for c in ads_df.columns]
                    asin_col  = col(ads_df,"advertised asin","asin")
                    spend_col = col(ads_df,"spend")
                    sales_col = col(ads_df,"14 day total sales","7 day total sales","sales")
                    impr_col  = col(ads_df,"impressions")
                    click_col = col(ads_df,"clicks")
                    units_col2= col(ads_df,"14 day total units","7 day total units")
                    if not asin_col: continue
                    for _, row in ads_df.iterrows():
                        asin = str(row.get(asin_col,"")).strip().upper()
                        if not asin or asin=="NAN": continue
                        spend = safe_float(row.get(spend_col,0)) if spend_col else 0
                        sales = safe_float(row.get(sales_col,0)) if sales_col else 0
                        impr  = safe_float(row.get(impr_col,0)) if impr_col else 0
                        clicks= safe_float(row.get(click_col,0)) if click_col else 0
                        orders= safe_float(row.get(units_col2,0)) if units_col2 else 0
                        if asin in ads_lookup:
                            ads_lookup[asin]["spend"]  += spend
                            ads_lookup[asin]["sales"]  += sales
                            ads_lookup[asin]["impr"]   += impr
                            ads_lookup[asin]["clicks"] += clicks
                            ads_lookup[asin]["orders"] += orders
                        else:
                            ads_lookup[asin] = {"spend":spend,"sales":sales,"impr":impr,"clicks":clicks,"orders":orders}
            except Exception as e:
                print(f"  Ads error {ads_path}: {e}")

        # ── 1P Sales ──────────────────────────────────────────────────────────
        p1_lookup = {}
        p1_df = read_p1(p1_path)
        if not p1_df.empty:
            p1_asin = col(p1_df,"asin")
            p1_rev  = col(p1_df,"ordered revenue","revenue")
            p1_units= col(p1_df,"ordered units","units")
            if p1_asin:
                for _, row in p1_df.iterrows():
                    asin = str(row.get(p1_asin,"")).strip().upper()
                    if not asin or asin=="NAN": continue
                    p1_lookup[asin] = {
                        "rev_1p":   safe_float(row.get(p1_rev,0)) if p1_rev else 0,
                        "units_1p": safe_float(row.get(p1_units,0)) if p1_units else 0,
                    }

        # ── Merge ─────────────────────────────────────────────────────────────
        all_asins = set(biz_lookup)|set(ads_lookup)|set(p1_lookup)
        cnt = 0
        for asin in all_asins:
            sku  = sku_lookup.get(asin, {})
            biz  = biz_lookup.get(asin, {})
            ads  = ads_lookup.get(asin, {})
            p1   = p1_lookup.get(asin, {})

            ad_spend  = ads.get("spend", 0)
            ad_sales  = ads.get("sales", 0)
            rev_3p    = biz.get("rev_3p", 0)
            rev_1p    = p1.get("rev_1p", 0)
            # TotalNetSalesValue = total revenue (Amazon's "Ordered Product Sales" already
            # includes ad-attributed sales — ad_sales is a SUBSET of rev_3p, not added to it).
            # For 1P: Rev1P is separate revenue stream; combined mode shows both together.
            net_sales = rev_3p + rev_1p
            _ri = lambda v: round(v) if v == v else 0  # NaN != NaN
            impr      = _ri(ads.get("impr", 0))
            clicks    = _ri(ads.get("clicks", 0))
            ams_orders= _ri(ads.get("orders", 0))
            units_3p  = biz.get("units_3p", 0)
            units_1p  = p1.get("units_1p", 0)
            sessions  = biz.get("sessions", 0)

            # ACOS  = Ad Spend / Ad Sales           (efficiency of ads only)
            # TACOS = Ad Spend / Total Net Sales    (ad spend vs ALL sales, ad + organic)
            acos  = round(ad_spend/ad_sales, 4) if ad_sales > 0 else 0
            tacos = round(ad_spend/net_sales, 4) if net_sales > 0 else 0
            cac   = round(ad_spend/ams_orders, 2) if ams_orders > 0 else 0
            cvr   = round((units_3p+units_1p)/sessions, 4) if sessions > 0 else 0
            org_sales = max(0, net_sales - ad_sales)
            org_pct   = round(org_sales/net_sales, 4) if net_sales > 0 else 0

            def _s(v):
                """Ensure nan-string values become empty string in output."""
                s = str(v).strip() if v else ""
                return "" if s.lower() in ("nan","nat","none") else s

            all_rows.append({
                "Brand":             brand_name,
                "ASIN":              asin,
                "Title":             biz.get("title", ""),
                "Month":             month,
                "fbaSku":            _s(sku.get("fba_sku","")),
                "model":             _s(sku.get("model","")),
                "category":          _s(sku.get("category","")),
                "mainCat":           _s(sku.get("mainCat","")),
                "DP":                sku.get("dp", 0),
                "Sessions":          sessions,
                "BuyboxPct":         biz.get("bb_pct", 0),
                "NetUnits":          units_3p + units_1p,
                "Units1P":           units_1p,
                "Units3P":           units_3p,
                "TotalNetSalesValue":round(net_sales, 2),
                "Rev1P":             round(rev_1p, 2),
                "Rev3P":             round(rev_3p, 2),
                "TotalAdsSpend":     round(ad_spend, 2),
                "TotalAdsSales":     round(ad_sales, 2),
                "Impressions":       impr,
                "Clicks":            clicks,
                "AmsOrders":         ams_orders,
                "ACOS":              acos,
                "TACOS":             tacos,
                "CAC":               cac,
                "ConversionPct":     cvr,
                "OrganiSales":       round(org_sales, 2),
                "OrganicPct":        org_pct,
            })
            cnt += 1

        print(f"  {brand_name} {month}: {cnt} ASINs | BB:{len([v for v in biz_lookup.values() if v['bb_pct']>0])} with BB data")

print(f"\nTotal rows: {len(all_rows)}")
for b in CONFIGS:
    cnt = sum(1 for r in all_rows if r["Brand"]==b)
    bb  = sum(1 for r in all_rows if r["Brand"]==b and r["BuyboxPct"]>0)
    print(f"  {b}: {cnt} rows, {bb} with BuyboxPct")

out_path = "./src/raw_data.json"
os.makedirs("./src", exist_ok=True)

# Clean NaN/Infinity values → 0 so the JSON is valid (browsers can't parse NaN/Infinity in JSON)
import math
def clean(obj):
    if isinstance(obj, dict):
        return {k: clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return 0
    return obj

all_rows_clean = clean(all_rows)
with open(out_path,"w") as f:
    json.dump(all_rows_clean, f, indent=2, allow_nan=False)
print(f"\nWritten to {out_path}")
