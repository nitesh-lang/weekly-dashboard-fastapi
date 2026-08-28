"""Variation Performance — sales + ads rolled up by Amazon variation family.

Keepa's export (data/master/keepa_variations.csv) gives the family structure:
each row is an ASIN with its `Variation ASINs` (the siblings/children of the
family).  Every member of a family usually has its own row carrying the same
member list, so families are deduplicated by member-set, not by row.

For each family the endpoint joins:
  * weekly_sales_snapshot.csv  -> GMV + units per member ASIN
  * business_ads_joined.csv    -> Spend + attributed (AMS) sales per member
and derives ACOS (spend / AMS sales) and TACOS (spend / GMV) at both the
family and member grain — the "is this variation pulling its weight" view.

Members missing from either source simply contribute zero — a listed
variation that never sold or was never advertised is exactly the signal the
operator is looking for, so it is shown, never dropped
(feedback_never_drop_rows_silently).
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Query

from weekly_app.core.json_utils import clean_nan

router = APIRouter(prefix="/api/variation-performance", tags=["variation-performance"])

KEEPA_CSV = Path("data/master/keepa_variations.csv")
SALES_CSV = Path("data/processed/weekly_sales_snapshot.csv")
AMS_CSV = Path("data/ams_weekly_data/processed_ads/business_ads_joined.csv")

_ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")

# Keepa brand spellings -> snapshot brand spellings.
_BRAND_CANON = {"nexlev": "Nexlev", "tonor": "Tonor",
                "audio array": "Audio Array", "white mulberry": "White Mulberry"}


def _wn(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.extract(r"(\d+)", expand=False),
                         errors="coerce")


# ── Keepa family loader (cached — the file changes only when the operator
#    drops a fresh export) ────────────────────────────────────────────────
_cache: Optional[tuple] = None   # (mtime, families)


def _load_families() -> list[dict]:
    global _cache
    if not KEEPA_CSV.exists():
        return []
    mtime = KEEPA_CSV.stat().st_mtime
    if _cache and _cache[0] == mtime:
        return _cache[1]

    k = pd.read_csv(KEEPA_CSV, low_memory=False)

    # Keepa gives every variation its OWN row, and the sibling lists are
    # per-row snapshots that drift out of sync (row A lists {A,B}, row B
    # lists {A,B,C}).  Exact-set dedupe therefore splits one real family
    # into several rows and multi-counts the shared members' revenue.
    # Merge by CONNECTED COMPONENTS instead: any two rows that share a
    # member are the same family (union-find over member ASINs).
    parent_of: dict[str, str] = {}

    def find(x: str) -> str:
        while parent_of.get(x, x) != x:
            parent_of[x] = parent_of.get(parent_of[x], parent_of[x])
            x = parent_of[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent_of[rb] = ra

    entries: dict[str, dict] = {}   # per row-ASIN identity metadata
    for _, r in k.iterrows():
        asin = str(r.get("ASIN") or "").strip().upper()
        if not _ASIN_RE.match(asin):
            continue
        kids = [x.strip().upper() for x in
                re.split(r"[,;\s]+", str(r.get("Variation ASINs") or ""))]
        kids = [x for x in kids if _ASIN_RE.match(x)]
        parent_of.setdefault(asin, asin)
        for c in kids:
            parent_of.setdefault(c, c)
            union(asin, c)
        brand_raw = str(r.get("Brand") or "").strip()
        entries[asin] = {
            "title": str(r.get("Title") or "")[:120],
            "brand": _BRAND_CANON.get(brand_raw.lower(), brand_raw),
            "rank": r.get("Sales Rank: Current"),
            "rating": r.get("Reviews: Rating"),
            "rating_count": r.get("Reviews: Rating Count"),
            "has_kids": bool(kids),
        }

    groups: dict[str, set] = {}
    for m in parent_of:
        groups.setdefault(find(m), set()).add(m)

    fams = []
    for members in groups.values():
        fam_entries = {m: entries[m] for m in members if m in entries}
        if not fam_entries:
            continue
        # Representative identity: the row with children listed and the most
        # reviews (the canonical listing).  Request-time may still prefer the
        # top-selling member's title for display.
        rep = max(fam_entries.items(),
                  key=lambda kv: (kv[1]["has_kids"],
                                  float(kv[1].get("rating_count") or 0)))
        fams.append({
            "parent": rep[0],
            "title": rep[1]["title"],
            "brand": rep[1]["brand"],
            "rank": rep[1]["rank"],
            "rating": rep[1]["rating"],
            "rating_count": rep[1]["rating_count"],
            "members": sorted(members),
            "entries": fam_entries,
        })
    _cache = (mtime, fams)
    return fams


@router.get("")
@router.get("/")
def variation_performance(
    weeks: List[int] = Query([], description="Week numbers; empty = latest week"),
    brands: List[str] = Query([], description="Brand filter; empty = all"),
    include_inactive: bool = Query(False, description="Also return families with zero GMV and zero spend in the window"),
):
    fams = _load_families()
    if not fams:
        return clean_nan({"error": "keepa_variations.csv missing — drop the Keepa "
                          "export at data/master/keepa_variations.csv",
                          "families": [], "weeks": [], "brands": []})

    s = pd.read_csv(SALES_CSV, usecols=["week", "brand", "asin", "model",
                                        "gmv", "units_sold"])
    s["wn"] = _wn(s["week"])
    s = s.dropna(subset=["wn"])
    s["wn"] = s["wn"].astype(int)
    all_weeks = sorted(s["wn"].unique().tolist(), reverse=True)
    sel_weeks = [w for w in weeks if w in set(all_weeks)] or all_weeks[:1]

    a = pd.DataFrame()
    if AMS_CSV.exists():
        a = pd.read_csv(AMS_CSV, usecols=["brand", "asin", "child_asin", "week",
                                          "Spend", "attributed_sales"])
        a["wn"] = _wn(a["week"])
        a = a.dropna(subset=["wn"])
        a["wn"] = a["wn"].astype(int)
        a = a[a["wn"].isin(sel_weeks)]

    sw = s[s["wn"].isin(sel_weeks)].copy()
    sw["asin"] = sw["asin"].fillna("").astype(str).str.strip().str.upper()

    # Per-ASIN sales aggregates for the window
    sales_by_asin = sw.groupby("asin").agg(
        gmv=("gmv", "sum"), units=("units_sold", "sum")).to_dict("index")
    model_by_asin = (sw[sw["model"].notna()]
                     .drop_duplicates("asin").set_index("asin")["model"].to_dict())

    # Per-ASIN ads aggregates — an AMS row is credited to its child_asin when
    # present (the actually-advertised variation), else its asin.
    ads_by_asin: dict = {}
    if not a.empty:
        eff = a["child_asin"].fillna("").astype(str).str.strip().str.upper()
        base = a["asin"].fillna("").astype(str).str.strip().str.upper()
        a["_asin"] = eff.where(eff.str.len() == 10, base)
        ads_by_asin = a.groupby("_asin").agg(
            spend=("Spend", "sum"), ams_sales=("attributed_sales", "sum")).to_dict("index")

    brand_filter = {b.strip().lower() for b in brands if b and b.strip()}
    out = []
    for f in fams:
        if brand_filter and f["brand"].lower() not in brand_filter:
            continue
        rows = []
        for m in f["members"]:
            sa = sales_by_asin.get(m, {})
            ad = ads_by_asin.get(m, {})
            gmv = float(sa.get("gmv", 0) or 0)
            spend = float(ad.get("spend", 0) or 0)
            ams = float(ad.get("ams_sales", 0) or 0)
            rows.append({
                "asin": m,
                # In a union-merged family almost every member's own Keepa
                # row listed children — badging them all says nothing.  The
                # badge marks only the family's canonical listing.
                "is_parent": m == f["parent"],
                "model": model_by_asin.get(m, ""),
                "gmv": round(gmv), "units": int(sa.get("units", 0) or 0),
                "spend": round(spend), "ams_sales": round(ams),
                "acos": round(spend / ams * 100, 1) if ams > 0 else None,
                "tacos": round(spend / gmv * 100, 1) if gmv > 0 else None,
            })
        gmv_t = sum(r["gmv"] for r in rows)
        spend_t = sum(r["spend"] for r in rows)
        ams_t = sum(r["ams_sales"] for r in rows)
        if not include_inactive and gmv_t == 0 and spend_t == 0:
            continue
        rows.sort(key=lambda r: (-r["gmv"], -r["spend"]))
        out.append({
            "parent_asin": f["parent"], "title": f["title"], "brand": f["brand"],
            "rank": f["rank"], "rating": f["rating"],
            "rating_count": f["rating_count"],
            "member_count": len(rows),
            "active_members": sum(1 for r in rows if r["gmv"] > 0),
            "gmv": gmv_t, "units": sum(r["units"] for r in rows),
            "spend": spend_t, "ams_sales": ams_t,
            "acos": round(spend_t / ams_t * 100, 1) if ams_t > 0 else None,
            "tacos": round(spend_t / gmv_t * 100, 1) if gmv_t > 0 else None,
            "ams_share": round(ams_t / gmv_t * 100, 1) if gmv_t > 0 else None,
            "members": rows,
        })
    out.sort(key=lambda x: -x["gmv"])

    total = len([f for f in fams
                 if not brand_filter or f["brand"].lower() in brand_filter])
    all_brands = sorted({f["brand"] for f in fams if f["brand"]})
    return clean_nan({
        "weeks": all_weeks, "selected_weeks": sorted(sel_weeks, reverse=True),
        "brands": all_brands, "selected_brands": sorted(brand_filter),
        "families": out,
        "family_count": total,
        "inactive_hidden": 0 if include_inactive else total - len(out),
        "generated_at": int(time.time()),
    })
