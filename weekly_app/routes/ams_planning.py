"""
AMS Planning endpoint — assembles a per-ASIN planning view across all brands.

Read-only columns (sourced):
  Brand, Model, BAU, ASIN, ASIN Type, Variation, Variation ASINs,
  Category L0, Category L1                            <-- sku_master.xlsx
  SOH (AMPM inventory units)                          <-- inventory snapshot
  Inventory Status (derived from SOH)

Editable columns (persisted to JSON):
  AMS Required, Remarks                               <-- ams_planning_notes.json

Notes are keyed by FBA SKU because that's the most stable identifier the
operator works with (one SKU = one ASIN = one planning row).  Other
identifiers (model_no, ASIN) can change with re-listings.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from threading import Lock
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

router = APIRouter()

SKU_MASTER  = Path("data/master/sku_master.xlsx")
INVENTORY   = Path("data/processed/inventory_model_snapshot.csv")
RAW_INV_DIR = Path("data/raw/inventory")
NOTES_FILE  = Path("data/processed/ams_planning_notes.json")

# SOH thresholds for Inventory Status — tune per operator preference.
SOH_LOW          = 50
SOH_HEALTHY_MAX  = 500   # > this is "Overstocked"

# AMPM channel synonyms (matches inventory_dashboard.py mapping).
AMPM_CHANNELS = {"ampm", "b2b-ampm", "b2b - ampm"}

_write_lock = Lock()
_payload_cache: dict = {"mtime": None, "body": None}


# ─────────────────────────────────────────────────────────────────────────
# Notes persistence (small JSON, single-writer expected, lock-guarded)
# ─────────────────────────────────────────────────────────────────────────

def _load_notes() -> dict:
    if not NOTES_FILE.exists():
        return {}
    try:
        return json.loads(NOTES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_notes(notes: dict) -> None:
    NOTES_FILE.parent.mkdir(parents=True, exist_ok=True)
    NOTES_FILE.write_text(json.dumps(notes, indent=2, ensure_ascii=False), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────
# SOH (AMPM inventory) lookup — built from the latest week of the raw inventory.
# The processed model snapshot doesn't preserve channel info, so we read the
# raw inventory directly (same source the inventory dashboard uses).
# ─────────────────────────────────────────────────────────────────────────

def _latest_week_num(weeks: list[str]) -> int:
    nums = []
    for w in weeks:
        try:    nums.append(int(str(w).replace("Week", "").strip()))
        except: pass
    return max(nums) if nums else -1


def _build_soh_lookup() -> dict[str, int]:
    """Return {sku → AMPM units on hand at the latest available week}."""
    try:
        # Reuse the inventory dashboard's loader so we get the same data the
        # rest of the app sees.  Imports here to avoid circular dependency at module level.
        from weekly_app.routes.inventory_dashboard import load_all_inventory
        df = load_all_inventory()
    except Exception:
        return {}
    if df is None or df.empty:
        return {}

    weeks = df["week"].dropna().astype(str).str.strip().unique().tolist()
    latest_week_num = _latest_week_num(weeks)
    if latest_week_num < 0:
        return {}
    latest_label = f"Week {latest_week_num}"
    sub = df[df["week"].astype(str).str.strip() == latest_label].copy()

    # Filter to AMPM channels only — "SOH" by operator definition.
    sub["_ch"] = sub["channel"].astype(str).str.strip().str.lower()
    sub = sub[sub["_ch"].isin(AMPM_CHANNELS)]
    if sub.empty:
        return {}

    # Aggregate units by SKU (one SKU can appear in multiple channel rows).
    sub["sku_key"] = sub["sku"].astype(str).str.strip()
    grp = sub.groupby("sku_key", as_index=True)["inventory_units"].sum()
    return {k: int(v) for k, v in grp.items() if k}


# ─────────────────────────────────────────────────────────────────────────
# Build the planning payload
# ─────────────────────────────────────────────────────────────────────────

def _inventory_status(soh: int) -> str:
    if soh <= 0:                  return "Out of Stock"
    if soh <= SOH_LOW:            return "Low"
    if soh <= SOH_HEALTHY_MAX:    return "Healthy"
    return "Overstocked"


def _build_payload() -> dict:
    if not SKU_MASTER.exists():
        return {"rows": [], "brands": [], "row_count": 0, "available": False}

    master = pd.read_excel(SKU_MASTER)
    master.columns = master.columns.str.strip()

    # Defensive column rename so the shape is stable for the JSON output.
    rename_map = {
        "FBA SKU":       "sku",
        "Original SKU":  "original_sku",
        "ASIN":          "asin",
        "Brand":         "brand",
        "Model":         "model",
        "BAU":           "bau",
        "ASIN Type":     "asin_type",
        "Variation":     "variation",
        "Variation ASINs": "variation_asins",
        "category_l0":   "category_l0",
        "category_l1":   "category_l1",
        "category_l2":   "category_l2",
        "NLC":           "nlc",
    }
    master = master.rename(columns={k: v for k, v in rename_map.items() if k in master.columns})

    # Coerce to strings for the text-ish columns (Excel sometimes stores as numbers/NaNs)
    for c in ["sku", "asin", "brand", "model", "asin_type", "variation",
              "variation_asins", "category_l0", "category_l1"]:
        if c in master.columns:
            master[c] = master[c].astype(str).str.strip()
            master[c] = master[c].replace({"nan": "", "None": ""})

    # Drop rows without a SKU — they're useless for planning.
    master = master[master.get("sku", "").astype(str).str.len() > 0]

    soh_lookup = _build_soh_lookup()
    notes = _load_notes()

    # Fossil isn't part of AMS — exclude it from the planning grid entirely.
    if "brand" in master.columns:
        master = master[master["brand"].astype(str).str.strip().str.lower() != "fossil"]

    rows = []
    for _, r in master.iterrows():
        sku = r.get("sku") or ""
        soh = int(soh_lookup.get(sku, 0))
        note = notes.get(sku, {})
        rows.append({
            "sku":               sku,
            "brand":             r.get("brand", "") or "",
            "model":             r.get("model", "") or "",
            "bau":               (r.get("bau") if pd.notna(r.get("bau")) else None),
            "asin":              r.get("asin", "") or "",
            "category_l0":       r.get("category_l0", "") or "",
            "category_l1":       r.get("category_l1", "") or "",
            "asin_type":         r.get("asin_type", "") or "",
            "variation":         r.get("variation", "") or "",
            "variation_asins":   r.get("variation_asins", "") or "",
            "soh":               soh,
            "inventory_status":  _inventory_status(soh),
            "ams_required":      note.get("ams_required", ""),
            "remarks":           note.get("remarks", ""),
        })

    brands = sorted({r["brand"] for r in rows if r["brand"]})
    return {
        "rows":      rows,
        "brands":    brands,
        "row_count": len(rows),
        "available": True,
    }


@router.get("/api/ams-planning")
def get_ams_planning():
    """Re-build the payload if either sku_master or notes file changed."""
    # mtime-aware cache so we don't re-read 907-row Excel on every request.
    master_mtime = SKU_MASTER.stat().st_mtime if SKU_MASTER.exists() else 0
    notes_mtime  = NOTES_FILE.stat().st_mtime if NOTES_FILE.exists() else 0
    cur = (master_mtime, notes_mtime)
    if _payload_cache["mtime"] == cur and _payload_cache["body"] is not None:
        return Response(content=_payload_cache["body"], media_type="application/json")

    payload = _build_payload()
    # Use pandas-compatible JSON encoder via plain json.dumps; non-NaN already cleaned.
    body = json.dumps(payload, ensure_ascii=False)
    _payload_cache["mtime"] = cur
    _payload_cache["body"]  = body
    return Response(content=body, media_type="application/json")


# ─────────────────────────────────────────────────────────────────────────
# Note upsert — single SKU at a time so concurrent edits to different SKUs
# don't clobber each other.
# ─────────────────────────────────────────────────────────────────────────

class NoteIn(BaseModel):
    sku:           str
    ams_required:  Optional[str] = None
    remarks:       Optional[str] = None


@router.post("/api/ams-planning/notes")
def upsert_note(note: NoteIn):
    sku = (note.sku or "").strip()
    if not sku:
        raise HTTPException(status_code=400, detail="sku is required")
    with _write_lock:
        notes = _load_notes()
        existing = notes.get(sku, {})
        # Only patch fields that were provided (None means "don't change").
        if note.ams_required is not None:
            existing["ams_required"] = note.ams_required
        if note.remarks is not None:
            existing["remarks"] = note.remarks
        existing["updated_at"] = int(time.time())
        notes[sku] = existing
        _save_notes(notes)
        # Invalidate read cache so next GET sees the change immediately.
        _payload_cache["mtime"] = None
    return JSONResponse({"ok": True, "sku": sku, "note": existing})
