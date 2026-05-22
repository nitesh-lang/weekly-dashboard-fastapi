"""Login / logout / forgot-password / reset-password routes."""
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment, FileSystemLoader

from weekly_app.core import auth_users, security

router = APIRouter()
_env = Environment(loader=FileSystemLoader("weekly_app/templates"), cache_size=0)


def _render(name: str, **ctx) -> HTMLResponse:
    return HTMLResponse(_env.get_template(name).render(**ctx))


def _safe_next(value: str | None) -> str | None:
    """Allow only same-origin redirects: must start with '/' and not '//'."""
    if not value:
        return None
    if not value.startswith("/") or value.startswith("//"):
        return None
    return value


# =====================================================
# LOGIN
# =====================================================
# React SPA owns `GET /login`; the JSON POST handler stays for /api/login.
# Old Jinja form kept in templates/ as fallback (not registered).
def login_page(
    request: Request,
    error: str | None = None,
    info: str | None = None,
    next: str | None = None,
):
    if request.session.get("user_email"):
        return RedirectResponse(_safe_next(next) or "/dashboard", status_code=303)
    return _render("login.html", error=error, info=info, next=next or "")


@router.post("/login")
def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    next: str | None = Form(None),
):
    user = auth_users.find_user(email)
    if not user or not security.verify_password(password, user.get("password_hash", "")):
        return _render(
            "login.html",
            error="Invalid email or password.",
            info=None,
            next=next or "",
        )
    request.session["user_email"] = user["email"]
    return RedirectResponse(_safe_next(next) or "/dashboard", status_code=303)


# =====================================================
# LOGOUT
# =====================================================
@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login?info=Logged+out.", status_code=303)


# =====================================================
# FORGOT PASSWORD
# =====================================================
@router.get("/forgot-password", response_class=HTMLResponse)
def forgot_get(
    request: Request,
    error: str | None = None,
    info: str | None = None,
):
    return _render("forgot_password.html", error=error, info=info)


@router.post("/forgot-password")
def forgot_post(request: Request, email: str = Form(...)):
    norm = email.strip().lower()
    user = auth_users.find_user(norm)
    if user:
        token = security.make_reset_token(norm)
        url = str(request.base_url).rstrip("/") + f"/reset-password?token={token}"
        # SMTP not configured yet — log to server console for now.
        print(f"\n🔐 PASSWORD RESET REQUESTED for {norm}")
        print(f"🔗 Reset URL (valid 30 min): {url}\n")
    # Same response whether the email exists or not (no enumeration).
    return _render(
        "forgot_password.html",
        info="If that email is registered, a reset link has been issued.",
        error=None,
    )


# =====================================================
# RESET PASSWORD
# =====================================================
@router.get("/reset-password", response_class=HTMLResponse)
def reset_get(
    request: Request,
    token: str | None = None,
    error: str | None = None,
):
    if not token:
        return _render(
            "reset_password.html",
            token=None,
            email=None,
            error="Missing reset token.",
        )
    email = security.verify_reset_token(token)
    if not email:
        return _render(
            "reset_password.html",
            token=None,
            email=None,
            error="Reset link is invalid or expired.",
        )
    return _render("reset_password.html", token=token, email=email, error=error)


@router.post("/reset-password")
def reset_post(
    request: Request,
    token: str = Form(...),
    password: str = Form(...),
    confirm: str = Form(...),
):
    email = security.verify_reset_token(token)
    if not email:
        return _render(
            "reset_password.html",
            token=None,
            email=None,
            error="Reset link is invalid or expired.",
        )
    if password != confirm:
        return _render(
            "reset_password.html",
            token=token,
            email=email,
            error="Passwords do not match.",
        )
    if len(password) < 8:
        return _render(
            "reset_password.html",
            token=token,
            email=email,
            error="Password must be at least 8 characters.",
        )

    auth_users.update_password(email, security.hash_password(password))
    request.session["user_email"] = email
    return RedirectResponse("/dashboard", status_code=303)
