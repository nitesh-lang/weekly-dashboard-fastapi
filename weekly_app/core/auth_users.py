"""File-backed user store.

Path is configurable via the `USERS_FILE` env var so Render can mount
a persistent disk and point at it (e.g. `USERS_FILE=/var/data/users.json`).
Without that, the container's local `data/users.json` gets wiped on
every deploy and only the bootstrap admin (auto-reseeded on startup)
survives.

Local dev leaves USERS_FILE unset → falls back to `data/users.json`.

Schema:
{
  "users": [
    {
      "email":               "info@cambiumretail.com",
      "password_hash":       "$2b$...",
      "role":                "admin",
      "tabs":                ["/sales-trend", ...],
      "created_at":          "2026-04-30T...Z",
      "password_updated_at": null
    }
  ]
}

Notes:
- _load() is wrapped in a short-lived in-memory cache (~30s) because the
  Google-Drive virtual filesystem on Windows occasionally throws
  `OSError: [WinError 433] A device which does not exist was specified`
  when stat'd from a child process.  Without the cache, every login
  request rolled the dice on that flake.  Auto-invalidated on _save().
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

# Persistent location on Render comes via env var; local dev uses the
# repo-relative path.  Anything calling auth_users sees the same
# interface — only the storage location moves.
USERS_FILE = Path(os.environ.get("USERS_FILE", "data/users.json"))
_CACHE_TTL_SECONDS = 30.0

# Single source of truth for the bootstrap account. seed_user.py and the
# startup auto-seed in main.py both use these.
INITIAL_USER_EMAIL = "info@cambiumretail.com"
INITIAL_USER_PASSWORD = "Cambium@109"  # initial only — change via /reset-password

# ─────────────────────────────────────────────────────────────────────
# Role-based access control (RBAC).
#
# - admin     full access, can manage users + see every tab
# - operator  full data access (view + export + edit notes), no admin
# - viewer    view-only — export/copy/edit are hidden + 403'd
#
# Per-user `tabs` is an allowlist of route paths (e.g. "/sales-trend").
# Empty list = "all tabs allowed for this role"; admins ignore it
# entirely.  Tab gating is enforced primarily by the React shell
# (nav/route hiding) and again by /api/me's tabs field; APIs are
# auth-gated but not tab-gated to keep the surface simple.
# ─────────────────────────────────────────────────────────────────────
VALID_ROLES = ("admin", "operator", "viewer")
DEFAULT_ROLE = "admin"  # legacy users without a role field

# Canonical tab catalog — must stay in sync with NAV_GROUPS on the
# frontend.  Anything not listed here can't be assigned to a user; the
# admin UI uses this list to render checkboxes.
KNOWN_TABS = (
    "/dashboard",
    "/insights",
    "/sales-trend",
    "/amazon-sales-trend",
    "/category-sales",
    "/margin-snapshot",
    "/inventory-dashboard",
    "/ams-trend",
    "/ams-planning",
    "/variation-performance",
    "/keepa-upload",
    "/ams-poor-performers",
    "/no-sales-last-week",
    "/dead-stock",
    "/returns",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# Cache: (loaded_at_monotonic, data).  None means "never loaded".
_cache: Optional[tuple] = None


def _load_uncached() -> Dict[str, Any]:
    """Single attempt to load + parse the users file.  Retries the FS call
    a couple of times to ride out the Google-Drive WinError 433 hiccup.
    Returns {"users": []} only on hard parse error, never on FS flake."""
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            if not USERS_FILE.exists():
                return {"users": []}
            return json.loads(USERS_FILE.read_text(encoding="utf-8"))
        except OSError as e:
            last_exc = e
            time.sleep(0.05 * (attempt + 1))   # 50ms, 100ms backoff
        except Exception:
            return {"users": []}
    # All retries failed — surface the FS error so the caller can decide.
    raise last_exc if last_exc else RuntimeError("users.json unreadable")


def _load() -> Dict[str, Any]:
    global _cache
    now = time.monotonic()
    if _cache is not None and (now - _cache[0]) < _CACHE_TTL_SECONDS:
        return _cache[1]
    try:
        data = _load_uncached()
    except Exception:
        # If the file is genuinely unreadable but we have a cached copy,
        # return the stale one rather than 500ing.  Better than nothing.
        if _cache is not None:
            return _cache[1]
        return {"users": []}
    _cache = (now, data)
    return data


def _save(data: Dict[str, Any]) -> None:
    global _cache
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    USERS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    # Invalidate cache so the next _load() picks up the freshly written file.
    _cache = (time.monotonic(), data)


def find_user(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None
    norm = email.strip().lower()
    for u in _load().get("users", []):
        if u.get("email", "").strip().lower() == norm:
            return u
    return None


def create_user(
    email: str,
    password_hash: str,
    role: str = "operator",
    tabs: Optional[list] = None,
) -> Dict[str, Any]:
    data = _load()
    norm = email.strip().lower()
    if any(u.get("email", "").strip().lower() == norm for u in data["users"]):
        raise ValueError(f"User already exists: {norm}")
    if role not in VALID_ROLES:
        raise ValueError(f"Invalid role: {role!r}. Must be one of {VALID_ROLES}")
    user = {
        "email": norm,
        "password_hash": password_hash,
        "role": role,
        "tabs": list(tabs) if tabs is not None else [],
        "created_at": _now_iso(),
        "password_updated_at": None,
    }
    data["users"].append(user)
    _save(data)
    return user


def get_role(email: str) -> str:
    """Returns the user's role.  Legacy users without a role get DEFAULT_ROLE
    (admin) — preserves backwards compatibility with the pre-RBAC bootstrap."""
    u = find_user(email)
    if not u:
        return DEFAULT_ROLE
    return u.get("role") or DEFAULT_ROLE


def get_tabs(email: str) -> list:
    """Returns the per-user tabs allowlist.  Empty list = no restriction
    (every tab the role allows is visible)."""
    u = find_user(email)
    if not u:
        return []
    return list(u.get("tabs") or [])


def list_users() -> list:
    """All users, with the password_hash stripped — admin-UI consumption."""
    out = []
    for u in _load().get("users", []):
        out.append({
            "email": u.get("email", ""),
            "role":  u.get("role") or DEFAULT_ROLE,
            "tabs":  list(u.get("tabs") or []),
            "created_at":          u.get("created_at"),
            "password_updated_at": u.get("password_updated_at"),
        })
    return out


def update_user(
    email: str,
    role: Optional[str] = None,
    tabs: Optional[list] = None,
) -> bool:
    """Update role and/or tabs on an existing user.  Returns True on
    success, False if the user wasn't found.  Validates role."""
    if role is not None and role not in VALID_ROLES:
        raise ValueError(f"Invalid role: {role!r}. Must be one of {VALID_ROLES}")
    data = _load()
    norm = email.strip().lower()
    for u in data["users"]:
        if u.get("email", "").strip().lower() == norm:
            if role is not None: u["role"] = role
            if tabs is not None: u["tabs"] = list(tabs)
            _save(data)
            return True
    return False


def delete_user(email: str) -> bool:
    data = _load()
    norm = email.strip().lower()
    before = len(data["users"])
    data["users"] = [u for u in data["users"]
                     if u.get("email", "").strip().lower() != norm]
    if len(data["users"]) == before:
        return False
    _save(data)
    return True


def count_admins() -> int:
    return sum(1 for u in _load().get("users", [])
               if (u.get("role") or DEFAULT_ROLE) == "admin")


def update_password(email: str, password_hash: str) -> bool:
    data = _load()
    norm = email.strip().lower()
    for u in data["users"]:
        if u.get("email", "").strip().lower() == norm:
            u["password_hash"] = password_hash
            u["password_updated_at"] = _now_iso()
            _save(data)
            return True
    return False


def ensure_initial_user() -> bool:
    """Create the bootstrap user if absent. Idempotent — safe to call on
    every startup. Returns True if a user was created, False if it already
    existed.
    """
    from weekly_app.core import security  # local import to avoid circular
    if find_user(INITIAL_USER_EMAIL):
        return False
    create_user(
        INITIAL_USER_EMAIL,
        security.hash_password(INITIAL_USER_PASSWORD),
        role="admin",
    )
    return True
