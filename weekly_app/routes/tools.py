from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")


TOOLS = [
    {
        "section": "Dashboards",
        "items": [
            {"brand": "Cambium", "label": "Ad Pilot Dashboard",
             "href": "https://adpilot-by-cambium.onrender.com/"},
            # Migrated 2026-08-28: now mounted in THIS service at /sales —
            # no cross-domain hop; the standalone Render services are suspended.
            {"brand": "Cambium", "label": "Sales Dashboard",
             "href": "/sales/login"},
        ],
    },
    {
        "section": "Calculators",
        "items": [
            {"brand": "Cambium", "label": "Margin Calculator",
             "href": "https://nexlev-margin-calculator.onrender.com"},
            {"brand": "Audio Array", "label": "CB Margin Calculator",
             "href": "https://cb-margin-calculator-python.onrender.com/cb/margin"},
        ],
    },
    {
        "section": "Trackers & Reports",
        "items": [
            {"brand": "Cambium", "label": "OrderPilot",
             "href": "https://orderpilot-web.onrender.com/login"},
            {"brand": "Cambium", "label": "AM Replenishment Tool",
             "href": "https://am-replenishment-1.onrender.com/"},
            {"brand": "Cambium", "label": "Buybox Report",
             "href": "https://buybox-report.onrender.com/"},
            {"brand": "Cambium", "label": "Hygiene Validator",
             "href": "https://hygiene-validator.onrender.com/"},
            {"brand": "Cambium", "label": "Ops Trackers (Shipments + Vendor)",
             "href": "https://cambium-trackers.onrender.com/"},
        ],
    },
    {
        "section": "CRM",
        "items": [
            {"brand": "Cambium", "label": "Cambium Hub",
             "href": "https://cambium-hub.onrender.com/sign-in?redirect_url=%2F"},
        ],
    },
]


@router.get("/tools", response_class=HTMLResponse)
def tools_page(request: Request):
    return templates.TemplateResponse(
        "tools.html",
        {"request": request, "sections": TOOLS, "active_nav": "tools"},
    )
