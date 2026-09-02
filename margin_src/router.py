"""NexLev Margin Calculator — merged into the Weekly monorepo 2026-09-02.

Adapted from nitesh-lang/nexlev-margin-calculator main.py.  What changed:
  * FastAPI app -> APIRouter included by weekly_app.main (guarded import).
  * Its whole auth subsystem (login/enrol/reset/admin, users.db, usage.db)
    is GONE — the weekly session is the identity (weekly SSO); anonymous
    visitors are redirected by weekly's AuthGuard ("/margin" is a guarded
    prefix there).
  * All file paths are absolute under margin_src/ (the service CWD is the
    repo root).  Brand master sheets persist to THIS repo via the GitHub
    contents API ([skip ci]); Render redeploys keep the committed copy.
Calculator logic, routes and templates are otherwise byte-for-byte the
original — do not "improve" formulas here.
"""
from pathlib import Path

from fastapi import APIRouter, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from typing import Dict

import pandas as pd
from io import BytesIO
from datetime import datetime
import uuid
import os
import base64
import httpx

from margin_src.services.calculator import calculate_single
from margin_src.services.calculator_factory import get_calculator
from margin_src.core.session_store import SESSION_STORE
from margin_src.core.templates_instance import templates
from margin_src.core.category_insights import router as category_router

BASE_DIR = Path(__file__).resolve().parent
AUDIT_FILE = str(BASE_DIR / "audit_param_changes.csv")

router = APIRouter()
router.include_router(category_router)

# ==================================================
# AUDIT HELPERS (UNCHANGED LOGIC)
# ==================================================
def audit_log(session_id, scope, name, old, new, sku=""):
    if old == new:
        return

    row = pd.DataFrame([{
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "session_id": session_id,
        "scope": scope,
        "parameter": name,
        "sku": sku,
        "old_value": old,
        "new_value": new
    }])

    if os.path.exists(AUDIT_FILE):
        row.to_csv(AUDIT_FILE, mode="a", header=False, index=False)
    else:
        row.to_csv(AUDIT_FILE, index=False)

# ==================================================
# UI ROUTE
# ==================================================
@router.get("/margin", response_class=HTMLResponse)
def margin_home(request: Request):
    return templates.TemplateResponse(request=request, name="margin.html")

# ==================================================
# UPLOAD & INITIALIZE SESSION
# ==================================================
@router.post("/margin/upload")
async def upload_excel(request: Request, file: UploadFile):
    content = await file.read()
    buffer = BytesIO(content)
    xls = pd.ExcelFile(buffer)

    # --------------------------------------------------
    # READ FORM DATA (FASTAPI ASYNC SAFE)
    # --------------------------------------------------
    form = await request.form()
    calc_mode = form.get("calc_mode")

    # --------------------------------------------------
    # SELECT MASTER SHEET (AUTO-DETECT SAFE)
    # --------------------------------------------------
    # --------------------------------------------------
    # STRICT SHEET SELECTION (NO FALLBACK)
    # --------------------------------------------------
    if calc_mode == "audio_array_ccb":
        # Audio Array CCB file
        df = pd.read_excel(xls, sheet_name="Main")
        # Margin Structure sheet exists but is read later if needed
        calc_mode = "audio_array_ccb"
    else:
        # Cambium / NexLev file
        df = pd.read_excel(xls, sheet_name="Master")
        calc_mode = "cambium"

    # --------------------------------------------------
    # MASTER SHEET NORMALIZATION
    # --------------------------------------------------
    df.columns = df.columns.str.strip()
    df["SKU"] = df["SKU"].astype(str).str.strip()
    df["Model"] = df["Model"].astype(str).str.strip()

    # --------------------------------------------------
    # 🔒 HARD ENFORCE SMM FROM MASTER ONLY
    # --------------------------------------------------
    if "SMM" in df.columns and "BAU Deal SP" in df.columns:
        df["SMM"] = df["SMM"].fillna(0)
        df["SMM Cost"] = (df["SMM"] / 100) * df["BAU Deal SP"]
    else:
        df["SMM"] = 0
        df["SMM Cost"] = 0

    
    # ==================================================
    # 🔵 AUDIO ARRAY CCB – AUTO PARAM LOAD FROM MAIN SHEET
    # (CB ONLY – CAMBIUM COMPLETELY UNTOUCHED)
    # ==================================================
    def extract_cb_params_from_main(main_df):
        # CB parameters are GLOBAL and taken from ROW 1 (index 0)
        first = main_df.iloc[0]

        def safe(col, default=0.0):
            if col in main_df.columns and pd.notna(first[col]):
                try:
                    return float(first[col])
                except Exception:
                    return default
            return default

        # EXACT header mapping as per CB Main sheet
        return {
            "ccb_sp": safe("CCB SP"),
            "fee_structure_pct": safe("Fee Structure %"),
            "promo_waiver_pct": safe("Promotional Waiver on Fee Structure"),
            "ams_pct": safe("AMS"),
            "business_overhead_pct": safe("Business Overhead"),
            "finance_cost_pct": safe("Finance Cost"),
            "coupon_pct": safe("Coupon"),
        }

# --------------------------------------------------
    # EDIT PARAMETERS (GLOBAL)
    # --------------------------------------------------
    if calc_mode == "audio_array_ccb":
        params = pd.DataFrame([], columns=["parameter", "value"])
    else:
        params_raw = pd.read_excel(xls, sheet_name="Edit Parameters", header=None)
        params = params_raw.iloc[:, :2].copy()
        params.columns = ["parameter", "value"]

    params["parameter"] = params["parameter"].astype(str).str.strip()
    params["value"] = (
        params["value"]
        .astype(str)
        .str.replace("₹", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    params["value"] = pd.to_numeric(params["value"], errors="coerce")

    def get_pct(name, default):
        val = params.loc[params["parameter"] == name, "value"]
        if val.empty:
            return default
        v = float(val.iloc[0])
        return v * 100 if v <= 1 else v

    def get_val(name, default):
        val = params.loc[params["parameter"] == name, "value"]
        return float(val.iloc[0]) if not val.empty else default

    global_params = {
        "usd_rate": get_val("Dollar Conversion Rate", 88.0),
        "surcharge_pct": get_pct("Surcharge", 10.0),
        "gst_default_pct": get_pct("GST %", 18.0),
        "smm_pct": 0.0,
        "overhead_pct": get_pct("Business Overhead %", 5.0),
        "finance_pct": get_pct("Cost of Finance %", 3.0),
    }

    # CB MODE: edit parameters are SKU-wise (NO GLOBAL MERGE)

    session_id = str(uuid.uuid4())

    # --------------------------------------------------
    # ✅ ENRICH DF USING MARGIN ENGINE
    # --------------------------------------------------
    calc_rows = []
    calculator = get_calculator(calc_mode)

    for _, _row in df.iterrows():
        out = calculator.calculate_single(_row, _row.to_dict() if calc_mode == "audio_array_ccb" else global_params)
        calc_rows.append(out)

    calc_df = pd.DataFrame(calc_rows)

    # --------------------------------------------------
    # MAP OUTPUTS (ENGINE SAFE)
# NOTE: audio_array_ccb KPIs are final at upload time
    # --------------------------------------------------
    if calc_mode == "audio_array_ccb":
        df["DP"] = calc_df["dp"]
        df["CCB DP"] = calc_df["ccb_dp"]
        df["CCB NLC"] = calc_df["ccb_nlc"]
        df["Gross Margin"] = calc_df["gross_margin"]
        df["Gross Margin %"] = calc_df["gross_margin_pct"]
        df["Net Margin"] = calc_df["net_margin"]
        df["Net Margin %"] = calc_df["net_margin_pct"]
    else:
        df["DP"] = calc_df["dp"]
        df["Gross Margin"] = calc_df["gross_margin"]
        df["Gross Margin %"] = calc_df["gross_margin_pct"]
        df["Net Margin"] = calc_df["net_margin"]
        df["Net Margin %"] = calc_df["net_margin_pct"]

    SESSION_STORE[session_id] = {
        "calc_mode": calc_mode,
        "df": df,
        "params": global_params if calc_mode != "audio_array_ccb" else {},
        "audit_prev_values": {},
        "audit_session_id": str(uuid.uuid4())
    }


    # --------------------------------------------------
    # 🔁 AUTO-CALCULATE FIRST SKU (TRIGGER FOR UI)
    # --------------------------------------------------
    first_sku = df["SKU"].iloc[0] if not df.empty else None
    summary = {}
    if first_sku:
        r0 = df[df["SKU"] == first_sku].iloc[0]
        out0 = calculator.calculate_single(r0, r0.to_dict() if calc_mode == "audio_array_ccb" else global_params)
        summary = {
            "dp": round(out0.get("dp", 0), 2),
            "gross_margin": round(out0.get("gross_margin", 0), 2),
            "gross_margin_pct": round(out0.get("gross_margin_pct", 0), 2),
            "net_margin": round(out0.get("net_margin", 0), 2),
            "net_margin_pct": round(out0.get("net_margin_pct", 0), 2),
        }

    raw_rows = df.fillna("").to_dict(orient="records")
    safe_rows = [
        {k: (str(v) if isinstance(v, (dict, list)) else v) for k, v in rec.items()}
        for rec in raw_rows
    ]
    return {
        "session_id": session_id,
        "rows": safe_rows,
        "summary": summary,
        "default_sku": first_sku
    }

# ==================================================
# GITHUB PERSISTENCE CONFIG (3 BRANDS)
# ==================================================
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")
GITHUB_REPO   = os.getenv("GITHUB_REPO", "nitesh-lang/weekly-dashboard-fastapi")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")

BRAND_FILE_MAP = {
    "audio_array":    str(BASE_DIR / "input/Audio Array/Audio Array Master Sheet.xlsx"),
    "nexlev":         str(BASE_DIR / "input/Nexlev/NexLev Master Sheet.xlsx"),
    "white_mulberry": str(BASE_DIR / "input/White Mulberry/WM Master Sheet.xlsx"),
}

def _repo_path(local_path: str) -> str:
    """Repo-relative path (forward slashes) for the GitHub contents API."""
    return "margin_src/" + Path(local_path).relative_to(BASE_DIR).as_posix()

async def get_github_sha(file_path: str):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    async with httpx.AsyncClient() as client:
        res = await client.get(url, headers=headers)
        if res.status_code == 200:
            return res.json().get("sha")
    return None

async def push_brand_to_github(file_path: str, file_bytes: bytes) -> bool:
    encoded = base64.b64encode(file_bytes).decode("utf-8")
    sha = await get_github_sha(file_path)
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "message": f"data(margin): {file_path} via app upload [skip ci]",
        "content": encoded,
        "branch": GITHUB_BRANCH,
        **({"sha": sha} if sha else {})
    }
    async with httpx.AsyncClient() as client:
        res = await client.put(url, headers=headers, json=payload)
    return res.status_code in (200, 201)


@router.get("/margin/master-status")
def master_status():
    return {brand: os.path.exists(path) for brand, path in BRAND_FILE_MAP.items()}


@router.post("/margin/upload-brand")
async def upload_brand(file: UploadFile, brand: str):
    if brand not in BRAND_FILE_MAP:
        return {"error": f"Unknown brand: {brand}"}
    content = await file.read()
    local_path = BRAND_FILE_MAP[brand]
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(content)
    github_ok = False
    if GITHUB_TOKEN and GITHUB_REPO:
        try:
            github_ok = await push_brand_to_github(_repo_path(local_path), content)
        except Exception as e:
            print(f"⚠️ GitHub push failed for {brand}: {e}")
    return {"brand": brand, "saved": True, "github_saved": github_ok}


@router.post("/margin/load-brand")
async def load_brand(brand: str):
    if brand not in BRAND_FILE_MAP:
        return {"error": f"Unknown brand: {brand}"}
    local_path = BRAND_FILE_MAP[brand]
    if not os.path.exists(local_path):
        return {"error": f"No master sheet found for {brand}. Please upload one."}

    calc_mode = "cambium"

    with open(local_path, "rb") as f:
        content = f.read()
    buffer = BytesIO(content)
    xls = pd.ExcelFile(buffer)

    df = pd.read_excel(xls, sheet_name="Master")
    params_raw = pd.read_excel(xls, sheet_name="Edit Parameters", header=None)
    p = params_raw.iloc[:, :2].copy()
    p.columns = ["parameter", "value"]
    p["parameter"] = p["parameter"].astype(str).str.strip()
    p["value"] = pd.to_numeric(
        p["value"].astype(str)
        .str.replace("₹","",regex=False).str.replace("%","",regex=False)
        .str.replace(",","",regex=False).str.strip(), errors="coerce"
    )
    def _pct(name, default):
        val = p.loc[p["parameter"]==name,"value"]
        if val.empty: return default
        v = float(val.iloc[0]); return v*100 if v<=1 else v
    def _val(name, default):
        val = p.loc[p["parameter"]==name,"value"]
        return float(val.iloc[0]) if not val.empty else default
    global_params = {
        "usd_rate": _val("Dollar Conversion Rate", 88.0),
        "surcharge_pct": _pct("Surcharge", 10.0),
        "gst_default_pct": _pct("GST %", 18.0),
        "smm_pct": 0.0,
        "overhead_pct": _pct("Business Overhead %", 5.0),
        "finance_pct": _pct("Cost of Finance %", 3.0),
    }

    df.columns = df.columns.str.strip()
    df["SKU"] = df["SKU"].astype(str).str.strip()
    df["Model"] = df["Model"].astype(str).str.strip()
    if "SMM" in df.columns and "BAU Deal SP" in df.columns:
        df["SMM"] = df["SMM"].fillna(0)
        df["SMM Cost"] = (df["SMM"]/100)*df["BAU Deal SP"]
    else:
        df["SMM"] = 0; df["SMM Cost"] = 0

    calculator = get_calculator(calc_mode)
    calc_rows = [calculator.calculate_single(r, r.to_dict() if calc_mode=="audio_array_ccb" else global_params) for _,r in df.iterrows()]
    calc_df = pd.DataFrame(calc_rows)

    if calc_mode == "audio_array_ccb":
        df["DP"]=calc_df["dp"]; df["CCB DP"]=calc_df["ccb_dp"]; df["CCB NLC"]=calc_df["ccb_nlc"]
        df["Gross Margin"]=calc_df["gross_margin"]; df["Gross Margin %"]=calc_df["gross_margin_pct"]
        df["Net Margin"]=calc_df["net_margin"]; df["Net Margin %"]=calc_df["net_margin_pct"]
    else:
        df["DP"]=calc_df["dp"]; df["Gross Margin"]=calc_df["gross_margin"]
        df["Gross Margin %"]=calc_df["gross_margin_pct"]
        df["Net Margin"]=calc_df["net_margin"]; df["Net Margin %"]=calc_df["net_margin_pct"]

    session_id = str(uuid.uuid4())
    SESSION_STORE[session_id] = {
        "calc_mode": calc_mode, "df": df, "params": global_params,
        "audit_prev_values": {}, "audit_session_id": str(uuid.uuid4())
    }

    first_sku = df["SKU"].iloc[0] if not df.empty else None
    summary = {}
    if first_sku:
        r0 = df[df["SKU"]==first_sku].iloc[0]
        out0 = calculator.calculate_single(r0, r0.to_dict() if calc_mode=="audio_array_ccb" else global_params)
        summary = {
            "dp": round(out0.get("dp",0),2), "gross_margin": round(out0.get("gross_margin",0),2),
            "gross_margin_pct": round(out0.get("gross_margin_pct",0),2),
            "net_margin": round(out0.get("net_margin",0),2), "net_margin_pct": round(out0.get("net_margin_pct",0),2),
        }

    raw_rows = df.fillna("").to_dict(orient="records")
    safe_rows = [
        {k: (str(v) if isinstance(v, (dict, list)) else v) for k, v in rec.items()}
        for rec in raw_rows
    ]
    return {
        "session_id": session_id, "brand": brand,
        "rows": safe_rows,
        "summary": summary, "default_sku": first_sku, "source": "disk"
    }
# ==================================================
# GET GLOBAL PARAMETERS
# ==================================================
@router.get("/margin/params")
def get_params(session_id: str, sku: str | None = None):
    session = SESSION_STORE.get(session_id)
    if not session:
        return {"error": "Invalid session"}

    # --------------------------------------------------
    # 🔵 AUDIO ARRAY CB → SKU-WISE EDIT PARAMS
    # --------------------------------------------------
    if session["calc_mode"] == "audio_array_ccb":
        df = session["df"]
        if sku and sku in df["SKU"].astype(str).tolist():
            r = df[df["SKU"] == sku].iloc[0]
        else:
            r = df.iloc[0]

        return {
            "ccb_sp": float(r.get("CCB SP", 0) or 0),
            "fee_structure_pct": float(r.get("Fee Structure %", 0) or 0),
            "promo_waiver_pct": float(r.get("Promotional Waiver on Fee Structure", 0) or 0),
            "ams_pct": float(r.get("AMS", 0) or 0),
            "business_overhead_pct": float(r.get("Business Overhead", 0) or 0),
            "finance_cost_pct": float(r.get("Finance Cost", 0) or 0),
            "coupon_pct": float(r.get("Coupon", 0) or 0),
        }

    # --------------------------------------------------
    # CAMBIUM (UNCHANGED)
    # --------------------------------------------------
    return session["params"]

# ==================================================
# UPDATE GLOBAL PARAMETER (AUDIT SAFE)
# ==================================================
@router.post("/margin/params/update")
def update_param(request: Request, session_id: str, key: str, value: float, sku: str | None = None):
    session = SESSION_STORE.get(session_id)
    if not session:
        return {"error": "Invalid session"}
    if not session:
        return {"error": "Invalid session"}

    params = session["params"]
    prev = params.get(key)

    audit_log(
        session["audit_session_id"],
        scope="GLOBAL",
        name=key,
        old=prev,
        new=value
    )


    
    # --------------------------------------------------
    # 🔵 AUDIO ARRAY CB → PARAM UPDATE IS SKU-WISE
    # --------------------------------------------------
    if session["calc_mode"] == "audio_array_ccb":
        if not sku:
            return {"error": "SKU required for CB parameter update"}

        FIELD_MAP = {
            "ccb_sp": "CCB SP",
            "fee_structure_pct": "Fee Structure %",
            "promo_waiver_pct": "Promotional Waiver on Fee Structure",
            "ams_pct": "AMS",
            "business_overhead_pct": "Business Overhead",
            "finance_cost_pct": "Finance Cost",
            "coupon_pct": "Coupon",
        }

        if key not in FIELD_MAP:
            return {"error": "Invalid CB parameter"}

        df = session["df"]
        idx = df.index[df["SKU"] == sku]
        if len(idx) == 0:
            return {"error": "SKU not found"}

        prev = df.at[idx[0], FIELD_MAP[key]]
        df.at[idx[0], FIELD_MAP[key]] = value

        audit_log(
            session["audit_session_id"],
            scope="CB-SKU",
            name=key,
            old=prev,
            new=value,
            sku=sku
        )

        calculator = get_calculator(session["calc_mode"])
        r = df[df["SKU"] == sku].iloc[0]
        out = calculator.calculate_single(r, r.to_dict() if session["calc_mode"] == "audio_array_ccb" else session["params"])

        return {
            "status": "updated",
            "summary": {
                "dp": round(out.get("dp", 0), 2),
                "gross_margin": round(out.get("gross_margin", 0), 2),
                "gross_margin_pct": round(out.get("gross_margin_pct", 0), 2),
                "net_margin": round(out.get("net_margin", 0), 2),
                "net_margin_pct": round(out.get("net_margin_pct", 0), 2),
            }
        }

    # --------------------------------------------------
    # CAMBIUM (GLOBAL PARAM UPDATE)
    # --------------------------------------------------
    if session["calc_mode"] != "audio_array_ccb":
    
        params[key] = 0.0 if key == "smm_pct" else value

    # 🔁 RECOMPUTE ALL ROWS FOR BOTH CAMBIUM & AUDIO ARRAY CB
    calc_mode = session["calc_mode"]
    calculator = get_calculator(calc_mode)

    new_rows = []
    for _, _row in session["df"].iterrows():
        out = calculator.calculate_single(_row, _row.to_dict() if calc_mode == "audio_array_ccb" else params)
        new_rows.append(out)

    calc_df = pd.DataFrame(new_rows)

    if calc_mode == "audio_array_ccb":
        session["df"]["DP"] = calc_df["dp"]
        session["df"]["CCB DP"] = calc_df.get("ccb_dp", 0)
        session["df"]["CCB NLC"] = calc_df.get("ccb_nlc", 0)
        session["df"]["Gross Margin"] = calc_df["gross_margin"]
        session["df"]["Gross Margin %"] = calc_df["gross_margin_pct"]
        session["df"]["Net Margin"] = calc_df["net_margin"]
        session["df"]["Net Margin %"] = calc_df["net_margin_pct"]
    else:
        session["df"]["DP"] = calc_df["dp"]
        session["df"]["Gross Margin"] = calc_df["gross_margin"]
        session["df"]["Gross Margin %"] = calc_df["gross_margin_pct"]
        session["df"]["Net Margin"] = calc_df["net_margin"]
        session["df"]["Net Margin %"] = calc_df["net_margin_pct"]

    
    # 🔁 RESOLVE SELECTED SKU FROM REQUEST (UI BINDING FIX)
    selected_sku = None
    try:
        selected_sku = request.query_params.get("sku")
    except Exception:
        selected_sku = None

# 🔁 RETURN UPDATED SUMMARY (FIRST SKU)
    df2 = session["df"]
    summary = {}
    target_sku = selected_sku if selected_sku in df2["SKU"].astype(str).tolist() else (df2["SKU"].iloc[0] if not df2.empty else None)
    if target_sku:
        r0 = df2[df2["SKU"] == target_sku].iloc[0]
        out0 = calculator.calculate_single(r0, r0.to_dict() if calc_mode == "audio_array_ccb" else params)
        summary = {
            "dp": round(out0.get("dp", 0), 2),
            "gross_margin": round(out0.get("gross_margin", 0), 2),
            "gross_margin_pct": round(out0.get("gross_margin_pct", 0), 2),
            "net_margin": round(out0.get("net_margin", 0), 2),
            "net_margin_pct": round(out0.get("net_margin_pct", 0), 2),
        }

    return {"status": "updated", "summary": summary}


# ==================================================
# MODEL & SKU FETCH
# ==================================================
@router.get("/margin/models")
def get_models(session_id: str):
    df = SESSION_STORE[session_id]["df"]
    return sorted(df["Model"].dropna().unique().tolist())

@router.get("/margin/skus")
def get_skus(session_id: str, model: str):
    df = SESSION_STORE[session_id]["df"]
    return (
        df[df["Model"] == model]["SKU"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

# ==================================================
# SKU FIELD MAP
# ==================================================
SKU_FIELDS = {
    "fob_usd": "Latest FOB",
    "freight_usd": "Freight+Clearance",
    "cbm": "CBM per unit",
    "duty_pct": "Import Duty %",
    "bau_sp": "BAU Deal SP",
    "mrp": "MRP",
    "fixed_closing_fee": "Fixed Closing Fee",
    "referral_pct": "Referral Fee %",
    "ams_pct": "AMS %",
    "coupon_pct": "Coupon %",
    "coupon_flat": "Coupon Flat",
    "returns_pct": "Returns Loss %",
    "fulfillment_fee": "Fulfillment Fees",
    "additional_cost": "Additional Cost",
}

# ==================================================
# GET SKU INPUTS
# ==================================================
@router.get("/margin/sku")
def get_sku(session_id: str, sku: str):
    session = SESSION_STORE.get(session_id)
    if not session:
        return {"error": "Invalid session"}
    if not session:
        return {}

    df = session["df"]
    row = df[df["SKU"] == sku]
    if row.empty:
        return {}

    r = row.iloc[0]
    return {k: float(r.get(v, 0) or 0) for k, v in SKU_FIELDS.items()}

# ==================================================
# UPDATE SKU FIELD (AUDIT SAFE)
# ==================================================
@router.post("/margin/sku/update")
def update_sku_field(session_id: str, sku: str, field: str, value: float):
    session = SESSION_STORE.get(session_id)
    if not session:
        return {"error": "Invalid session"}
    if not session:
        return {"error": "Invalid session"}

    if field not in SKU_FIELDS:
        return {"error": "Invalid field"}

    df = session["df"]
    idx = df.index[df["SKU"] == sku]
    if len(idx) == 0:
        return {"error": "SKU not found"}

    col = SKU_FIELDS[field]
    prev = df.at[idx[0], col]

    audit_log(
        session["audit_session_id"],
        scope="SKU",
        name=field,
        old=prev,
        new=value,
        sku=sku
    )

    df.at[idx[0], col] = value

    # 🔁 IMMEDIATE RECALC FOR THIS SKU
    calculator = get_calculator(session["calc_mode"])
    r = df[df["SKU"] == sku].iloc[0]
    out = calculator.calculate_single(r, r.to_dict() if session["calc_mode"] == "audio_array_ccb" else session["params"])

    return {
        "status": "updated",
        "summary": {
            "dp": round(out.get("dp", 0), 2),
            "gross_margin": round(out.get("gross_margin", 0), 2),
            "gross_margin_pct": round(out.get("gross_margin_pct", 0), 2),
            "net_margin": round(out.get("net_margin", 0), 2),
            "net_margin_pct": round(out.get("net_margin_pct", 0), 2),
        }
    }

# ==================================================
# CALCULATION ENGINE (FINAL SOURCE OF TRUTH)
# ==================================================
# Cambium-only calculation endpoint
@router.post("/margin/calculate")
def calculate_margin(session_id: str, sku: str):
    # CB MODE ENABLED FOR LIVE CALCULATION
    session = SESSION_STORE.get(session_id)
    if not session:
        return {"error": "Invalid session"}
    if not session:
        return {"error": "Invalid session"}

    df = session["df"]
    params = session["params"]

    row = df[df["SKU"] == sku]
    if row.empty:
        return {"error": "SKU not found"}

    r = row.iloc[0]

    
    calculator = get_calculator(session["calc_mode"])
    out = calculator.calculate_single(r, r.to_dict() if session["calc_mode"] == "audio_array_ccb" else params)

    return {
        "dp": round(out.get("dp", 0), 2),
        "gross_margin": round(out.get("gross_margin", 0), 2),
        "gross_margin_pct": round(out.get("gross_margin_pct", 0), 2),
        "net_margin": round(out.get("net_margin", 0), 2),
        "net_margin_pct": round(out.get("net_margin_pct", 0), 2)
    }


# ==================================================
# EXPORT MARGIN DATA (EXCEL)
# ==================================================
@router.get("/margin/export")
def export_margin(session_id: str):
    session = SESSION_STORE.get(session_id)
    if not session:
        return {"error": "Invalid session"}
    if not session:
        return {"error": "Invalid session"}

    df = session["df"].copy()

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Margin Data")

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=margin_export.xlsx"}
    )


# ==================================================
# CB GLOBAL PARAM FIX COMPLETE – CAMBIUM LOGIC UNCHANGED
# ==================================================
