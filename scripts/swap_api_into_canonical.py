"""
One-off migration — swap the API-pulled SP/SD sheets into the canonical
ads_report_week<N>.xlsx files, preserving each file's existing SB sheet
(which has the L0-attributed ASIN-level data from sb_ingest.py).

Source:    data/ams_weekly_data/<Brand>/ads_report_week<N>_api.xlsx
Target:    data/ams_weekly_data/<Brand>/ads_report_week<N>.xlsx
Backup:    target.xlsx.bak.<timestamp>  (created if doesn't exist)

API column → operator column rename map:
  asin             → Advertised ASIN
  campaignName     → Campaign Name
  Spend, Impressions, Clicks  → unchanged
  attributed_sales → 14 Day Total Sales (₹)
  ams_orders       → 14 Day Total Units (#)
"""
from __future__ import annotations
import shutil
import sys
import datetime as dt
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
AMS_BASE = ROOT / "data" / "ams_weekly_data"

API_TO_OPERATOR = {
    "asin":             "Advertised ASIN",
    "campaignName":     "Campaign Name",
    "attributed_sales": "14 Day Total Sales (₹)",
    "ams_orders":       "14 Day Total Units (#)",
    # Spend / Clicks / Impressions keep their names
}


def normalize_api_sheet(df: pd.DataFrame, sheet: str) -> pd.DataFrame:
    """Rename API columns to operator schema. Add Portfolio name (blank)
    and Brand columns to match what step3 / sb_ingest write."""
    if df.empty:
        return df
    df = df.rename(columns=API_TO_OPERATOR)
    # Ensure all numeric columns step3 expects exist
    for c in ("Spend", "Clicks", "Impressions",
              "14 Day Total Sales (₹)", "14 Day Total Units (#)"):
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    if "Portfolio name" not in df.columns:
        df["Portfolio name"] = ""
    if "Advertised ASIN" not in df.columns:
        return pd.DataFrame()  # nothing useful
    df["Advertised ASIN"] = df["Advertised ASIN"].astype(str).str.strip()
    df = df[df["Advertised ASIN"].ne("") & df["Advertised ASIN"].ne("nan")]
    # Re-aggregate at the Campaign Name × ASIN level so the file is tidy
    keys = ["Portfolio name", "Campaign Name", "Advertised ASIN"] if "Campaign Name" in df.columns \
           else ["Portfolio name", "Advertised ASIN"]
    nums = ["Spend", "Clicks", "Impressions",
            "14 Day Total Sales (₹)", "14 Day Total Units (#)"]
    return df.groupby(keys, as_index=False)[nums].sum()


def swap_one(brand_dir: Path, week: int) -> dict:
    api_path = brand_dir / f"ads_report_week{week}_api.xlsx"
    canon    = brand_dir / f"ads_report_week{week}.xlsx"
    out = {"brand": brand_dir.name, "week": week, "swapped": False, "note": ""}
    if not api_path.exists():
        out["note"] = "no api file"; return out

    # Read API SP + SD
    api_xl = pd.ExcelFile(api_path)
    api_sp = pd.read_excel(api_path, sheet_name="SP") if "SP" in api_xl.sheet_names else pd.DataFrame()
    api_sd = pd.read_excel(api_path, sheet_name="SD") if "SD" in api_xl.sheet_names else pd.DataFrame()
    new_sp = normalize_api_sheet(api_sp, "SP")
    new_sd = normalize_api_sheet(api_sd, "SD")

    # Read canonical to preserve SB sheet (if present)
    existing_sb = pd.DataFrame()
    if canon.exists():
        try:
            cx = pd.ExcelFile(canon)
            if "SB" in cx.sheet_names:
                existing_sb = pd.read_excel(canon, sheet_name="SB")
        except Exception as e:
            out["note"] = f"could not read canonical: {e}"
            # fall through — we'll still write API SP/SD, no SB

        # Timestamped backup so we never lose the operator-exported version
        ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        bak = canon.with_suffix(f".xlsx.bak.{ts}")
        try:
            shutil.copy2(canon, bak)
        except Exception:
            pass

    # Write SP, SD, SB to canonical.  ALWAYS emit SP + SD sheets even
    # when API returned nothing for that week — downstream consumers
    # (business_ads_weekly_etl) call pd.read_excel(sheet_name="SP")
    # blindly, so a missing sheet crashes the workflow.  An empty
    # sheet with the standard column headers is the safe sentinel.
    EMPTY_COLS = [
        "Portfolio name", "Campaign Name", "Advertised ASIN",
        "Spend", "Clicks", "Impressions",
        "14 Day Total Sales (₹)", "14 Day Total Units (#)",
    ]
    sheets = {
        "SP": new_sp if not new_sp.empty else pd.DataFrame(columns=EMPTY_COLS),
        "SD": new_sd if not new_sd.empty else pd.DataFrame(columns=EMPTY_COLS),
    }
    if not existing_sb.empty:
        sheets["SB"] = existing_sb

    # ── Regression guard ──
    # ads_reports_pull's coverage check should prevent this, but if a
    # partial-window fragment slips through (or operator runs swap by
    # hand) we refuse to overwrite a canonical file whose SP+SD spend
    # exceeds the new file's by more than 30%.  Better to keep stale
    # canonical than corrupt it with a fragment.  Empty canonical
    # (first-time write) always proceeds.
    DEGRADE_TOLERANCE = 0.70
    if canon.exists():
        try:
            old_xl = pd.ExcelFile(canon)
            old_spend = 0.0
            for sh in ("SP", "SD"):
                if sh in old_xl.sheet_names:
                    od = pd.read_excel(canon, sheet_name=sh)
                    if "Spend" in od.columns:
                        old_spend += pd.to_numeric(od["Spend"], errors="coerce").fillna(0).sum()
            new_spend = (
                pd.to_numeric(sheets["SP"].get("Spend", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                + pd.to_numeric(sheets["SD"].get("Spend", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
            )
            if old_spend > 1000 and new_spend < old_spend * DEGRADE_TOLERANCE:
                out["note"] = (
                    f"⛔ refused swap: SP+SD spend regressed ₹{int(old_spend):,} → "
                    f"₹{int(new_spend):,} ({(1-new_spend/old_spend)*100:.0f}% drop) — "
                    f"partial-window fragment; keeping canonical."
                )
                print(f"  {out['note']}")
                return out
        except Exception as e:
            print(f"  ⚠ regression-guard read failed, will write anyway: {e!r}")

    with pd.ExcelWriter(canon, engine="openpyxl", mode="w") as w:
        for name, sdf in sheets.items():
            sdf.to_excel(w, sheet_name=name, index=False)

    out["swapped"] = True
    out["sheets"]  = list(sheets.keys())
    out["rows"]    = {n: len(sdf) for n, sdf in sheets.items()}
    return out


def main():
    if not AMS_BASE.exists():
        print(f"⚠ {AMS_BASE} not found"); sys.exit(1)
    n_done = 0
    n_skipped = 0
    for brand_dir in sorted(AMS_BASE.iterdir()):
        if not brand_dir.is_dir() or brand_dir.name in ("processed_ads", "ams_weekly_fact"):
            continue
        for api_path in sorted(brand_dir.glob("ads_report_week*_api.xlsx")):
            wname = api_path.name.replace("_api.xlsx", "").replace("ads_report_week", "")
            try:
                week = int(wname)
            except ValueError:
                continue
            r = swap_one(brand_dir, week)
            if r["swapped"]:
                n_done += 1
                sheets = ",".join(r.get("sheets", []))
                rows   = r.get("rows", {})
                print(f"  ✅ {brand_dir.name:<16} W{week:<2}  sheets={sheets:<12}  rows={rows}")
            else:
                n_skipped += 1
                print(f"  ⊘ {brand_dir.name:<16} W{week:<2}  {r['note']}")
    print(f"\n✅ swapped {n_done}, skipped {n_skipped}")


if __name__ == "__main__":
    main()
