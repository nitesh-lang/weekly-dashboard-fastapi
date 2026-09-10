"""ONE-OFF (10/09/26): fill margins on the GIF'26 Deal Input Template.

Operator's brief:
  * price + referral fee come from the DEAL SHEET, not the brand master
      - SP            = "Compliant price"
      - referral %    = "Ref"      (margin WITHOUT waiver)
      - referral %    = "Ref" - "Waiver"  (margin WITH waiver)
  * everything else (FOB, freight, duty, FBA fee, closing, returns, coupon,
    SMM, overhead, finance, GST) stays as the brand master + Edit Parameters
  * AMS given twice: the sheet's 15% assumption AND the tool's actual 3-month
    TACOS per ASIN

Writes <input file>_filled.xlsx next to the source with the two template
columns populated plus the actual-TACOS variants and a working column set.
"""
from __future__ import annotations

import glob
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from weekly_app.etl.margin_snapshot import MARGIN_TOOL_MASTERS, _global_params  # noqa: E402
from margin_src.services.calculator_factory import get_calculator              # noqa: E402
from margin_src.core.ams_tacos import tacos_for                                 # noqa: E402

SHEET_AMS = 15.0          # operator: keep the sheet assumption at 15%


def _norm(v) -> str:
    return str(v).strip().upper()


def main() -> None:
    hits = glob.glob(str(Path.home() / "Downloads" / "GIF*Deal Input Template.xlsx"))
    hits = [h for h in hits if "~$" not in h]
    if not hits:
        raise SystemExit("GIF'26 Deal Input Template.xlsx not found in Downloads")
    src = Path(hits[0])
    deal = pd.read_excel(src, "Deal Template").dropna(how="all")
    print(f"template: {src.name} — {len(deal)} rows")

    # brand masters + their Edit Parameters
    masters: dict[str, tuple[pd.DataFrame, dict]] = {}
    for brand, path in MARGIN_TOOL_MASTERS.items():
        if brand in ("Nexlev", "White Mulberry") and path.exists():
            xls = pd.ExcelFile(path)
            masters[brand] = (xls.parse(xls.sheet_names[0]), _global_params(xls))
            print(f"  master {brand}: {len(masters[brand][0])} rows, "
                  f"usd {masters[brand][1].get('usd_rate')}")

    calc = get_calculator("cambium")
    out_rows, misses, tac_hits = [], [], 0

    for _, d in deal.iterrows():
        brand = str(d.get("Brand", "")).strip()
        asin = _norm(d.get("Deal ASIN(s"))
        sku = _norm(d.get("SKUs"))
        sp = pd.to_numeric(d.get("Compliant price"), errors="coerce")
        price_src = "Compliant price"
        if not (sp > 0):
            # No compliant price on the row — a deal price must be at or below
            # the recent selling prices, so fall back to the LOWEST of the
            # August / Prime / May columns and say so in the output.
            cands = {c: pd.to_numeric(d.get(c), errors="coerce")
                     for c in ("August", "Prime ", "May ")}
            cands = {k: v for k, v in cands.items() if pd.notna(v) and v > 0}
            if cands:
                price_src = min(cands, key=cands.get).strip() + " (lowest — no compliant price given)"
                sp = min(cands.values())
        ref = pd.to_numeric(d.get("Ref"), errors="coerce")
        waiver = pd.to_numeric(d.get("Waiver"), errors="coerce")
        if brand not in masters or not (sp > 0):
            misses.append((brand, asin, "no master / no price"))
            continue
        mdf, params = masters[brand]

        acol = next((c for c in mdf.columns if str(c).strip().lower() == "asin"), None)
        scol = next((c for c in mdf.columns if str(c).strip().lower() in ("sku", "fba sku")), None)
        row = pd.DataFrame()
        if acol:
            row = mdf[mdf[acol].map(_norm) == asin]
        if row.empty and scol:
            row = mdf[mdf[scol].map(_norm) == sku]
        if row.empty:
            misses.append((brand, asin, "not in master"))
            continue
        base = row.iloc[0].copy()

        # actual 3-month TACOS for this ASIN (falls back to the sheet's 15%)
        try:
            t = tacos_for(asin, 3)
            actual_ams = (t or {}).get("window", {}).get("tacos_pct")
        except Exception:
            actual_ams = None
        if actual_ams is not None:
            tac_hits += 1

        def run(ref_pct: float, ams_pct: float) -> dict:
            r = base.copy()
            r["BAU Deal SP"] = float(sp)
            r["Referral Fee %"] = float(ref_pct)
            r["AMS %"] = float(ams_pct)
            return calc.calculate_single(r, params)

        ref_no = float(ref) if pd.notna(ref) else float(base.get("Referral Fee %") or 0)
        ref_wv = max(ref_no - (float(waiver) if pd.notna(waiver) else 0.0), 0.0)

        m_no_sheet = run(ref_no, SHEET_AMS)
        m_wv_sheet = run(ref_wv, SHEET_AMS)
        m_no_act = run(ref_no, actual_ams) if actual_ams is not None else m_no_sheet
        m_wv_act = run(ref_wv, actual_ams) if actual_ams is not None else m_wv_sheet

        out_rows.append({
            "Brand": brand, "ASIN": asin, "SKU": sku, "Model": d.get("Model"),
            "Price used": float(sp), "Price source": price_src,
            "Ref %": ref_no, "Waiver %": float(waiver) if pd.notna(waiver) else 0.0,
            "Ref % after waiver": ref_wv,
            "AMS % (sheet)": SHEET_AMS,
            "AMS % (actual 3-mo TACOS)": actual_ams,
            "margin Without Waiver": m_no_sheet["net_margin_pct"],
            "margin with Waiver": m_wv_sheet["net_margin_pct"],
            "Rs Without Waiver": m_no_sheet["net_margin"],
            "Rs with Waiver": m_wv_sheet["net_margin"],
            "margin Without Waiver (actual TACOS)": m_no_act["net_margin_pct"],
            "margin with Waiver (actual TACOS)": m_wv_act["net_margin_pct"],
            "Rs Without Waiver (actual TACOS)": m_no_act["net_margin"],
            "Rs with Waiver (actual TACOS)": m_wv_act["net_margin"],
            "DP": m_wv_sheet["dp"],
            "Gross margin %": m_wv_sheet["gross_margin_pct"],
        })

    res = pd.DataFrame(out_rows)
    dst = src.with_name(src.stem + "_filled.xlsx")
    with pd.ExcelWriter(dst, engine="openpyxl") as xw:
        res.to_excel(xw, "Margins", index=False)
        if misses:
            pd.DataFrame(misses, columns=["brand", "asin", "why"]).to_excel(
                xw, "Unmatched", index=False)
    print(f"\npriced {len(res)} rows ({tac_hits} with actual TACOS), "
          f"{len(misses)} unmatched -> {dst.name}")
    if not res.empty:
        for b, g in res.groupby("Brand"):
            print(f"  {b}: median margin w/o waiver {g['margin Without Waiver'].median():.1f}% "
                  f"| with waiver {g['margin with Waiver'].median():.1f}% "
                  f"| with waiver @ actual TACOS {g['margin with Waiver (actual TACOS)'].median():.1f}%")
            neg = g[g["margin with Waiver"] < 0]
            if len(neg):
                print(f"     {len(neg)} SKU(s) still negative WITH the waiver: "
                      f"{', '.join(neg['Model'].astype(str).head(6))}")


if __name__ == "__main__":
    main()
