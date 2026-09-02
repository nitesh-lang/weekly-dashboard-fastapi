from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import HTMLResponse
import pandas as pd
from io import BytesIO

from margin_src.core.session_store import SESSION_STORE
from margin_src.core.templates_instance import templates

router = APIRouter()


@router.get("/margin/category-insights", response_class=HTMLResponse)
def category_page(request: Request, session_id: str | None = None):

    df = None

    if session_id and session_id in SESSION_STORE:
        df = SESSION_STORE[session_id].get("df")

    if df is None or df.empty:
        return templates.TemplateResponse(
            request=request, name="category_insights.html",
            context={"data": None, "summary": None}
        )

    df = df.copy()

    for col in ["BAU Deal SP", "Gross Margin", "Net Margin"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Category_L0"] = df["Category - L1"] if "Category - L1" in df.columns else "Unmapped"
    df["Category_L1"] = df["Category - L2"] if "Category - L2" in df.columns else "Unmapped"

    total_sp = df["BAU Deal SP"].sum()
    total_gm = df["Gross Margin"].sum()
    total_nm = df["Net Margin"].sum()

    summary = {
        "total_sp": round(total_sp, 2),
        "gross_margin": round(total_gm, 2),
        "gross_pct": round((total_gm / total_sp * 100) if total_sp else 0, 2),
        "net_margin": round(total_nm, 2),
        "net_pct": round((total_nm / total_sp * 100) if total_sp else 0, 2),
    }

    grouped = []

    for l0, g0 in df.groupby("Category_L0"):
        l0_sp = g0["BAU Deal SP"].sum()
        l0_gm = g0["Gross Margin"].sum()
        l0_nm = g0["Net Margin"].sum()

        l1_rows = []
        for l1, g1 in g0.groupby("Category_L1"):
            l1_sp = g1["BAU Deal SP"].sum()
            l1_gm = g1["Gross Margin"].sum()
            l1_nm = g1["Net Margin"].sum()

            l1_rows.append({
                "name": l1,
                "total_sp": round(l1_sp, 2),
                "gross_margin": round(l1_gm, 2),
                "net_margin": round(l1_nm, 2),
                "gross_pct": round((l1_gm / l1_sp * 100) if l1_sp else 0, 2),
                "net_pct": round((l1_nm / l1_sp * 100) if l1_sp else 0, 2),
                "sku_count": int(g1["SKU"].nunique()) if "SKU" in g1.columns else 0,
            })

        grouped.append({
            "name": l0,
            "total_sp": round(l0_sp, 2),
            "gross_margin": round(l0_gm, 2),
            "net_margin": round(l0_nm, 2),
            "gross_pct": round((l0_gm / l0_sp * 100) if l0_sp else 0, 2),
            "net_pct": round((l0_nm / l0_sp * 100) if l0_sp else 0, 2),
            "sku_count": int(g0["SKU"].nunique()) if "SKU" in g0.columns else 0,
            "l1": l1_rows,
        })

    return templates.TemplateResponse(
        request=request, name="category_insights.html",
        context={"data": grouped, "summary": summary}
    )


@router.post("/margin/category-insights", response_class=HTMLResponse)
async def category_upload(request: Request, file: UploadFile = File(...)):
    contents = await file.read()
    df = pd.read_excel(BytesIO(contents))
    df.columns = df.columns.str.strip()

    SESSION_STORE["_temp"] = {"df": df}
    return category_page(request, session_id="_temp")