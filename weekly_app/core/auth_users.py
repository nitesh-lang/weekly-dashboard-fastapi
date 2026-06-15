"""Dual-backend user store.

When `DATABASE_URL` is set (Render Postgres attached), every operation
goes through Postgres.  Otherwise the legacy JSON file at
`data/users.json` is used — keeps local dev frictionless (no DB to run
locally).

Why two backends instead of always Postgres?
- Render's container disk is ephemeral; the JSON file gets wiped on
  every deploy and only the bootstrap admin survives.  Postgres
  persists.
- Local dev shouldn't require a running Postgres.  Falling back to
  JSON keeps `python weekly_app/main.py` zero-config.

The public surface (find_user / create_user / update_user / etc.) is
identical to the original file-only API so `api.py`, `admin.py`, and
`auth.py` import unchanged.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

from weekly_app.core import db

USERS_FILE = Path("data/users.json")
_CACHE_TTL_SECONDS = 30.0

# Single source of truth for the bootstrap account.
INITIAL_USER_EMAIL = "info@cambiumretail.com"
INITIAL_USER_PASSWORD = "Cambium@109"  # initial only — change via /reset-password

# ─────────────────────────────────────────────────────────────────────
# Role-based access control (RBAC).
# ─────────────────────────────────────────────────────────────────────
VALID_ROLES = ("admin", "operator", "viewer")
DEFAULT_ROLE = "admin"  # legacy users without a role field

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
    "/ams-poor-performers",
    "/no-sales-last-week",
    "/dead-stock",
    "/returns",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ─────────────────────────────────────────────────────────────────────
# JSON backend (local dev only) — kept verbatim from the pre-Postgres
# implementation so behaviour off-Render is unchanged.
# ─────────────────────────────────────────────────────────────────────
_json_cache: Optional[tuple] = None


def _json_load_uncached() -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            if not USERS_FILE.exists():
                return {"users": []}
            return json.loads(USERS_FILE.read_text(encoding="utf-8"))
        except OSError as e:
            last_exc = e
            time.sleep(0.05 * (attempt + 1))
        except Exception:
            return {"users": []}
    raise last_exc if last_exc else RuntimeError("users.json unreadable")


def _json_load() -> Dict[str, Any]:
    global _json_cache
    now = time.monotonic()
    if _json_cache is not None and (now - _json_cache[0]) < _CACHE_TTL_SECONDS:
        return _json_cache[1]
    try:
        data = _json_load_uncached()
    except Exception:
        if _json_cache is not None:
            return _json_cache[1]
        return {"users": []}
    _json_cache = (now, data)
    return data


def _json_save(data: Dict[str, Any]) -> None:
    global _json_cache
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    USERS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    _json_cache = (time.monotonic(), data)


# ─────────────────────────────────────────────────────────────────────
# Postgres backend — used on Render when DATABASE_URL is set.
# All helpers normalise email to lowercase to match the JSON behaviour.
# ─────────────────────────────────────────────────────────────────────
def _pg_row_to_user(row) -> Dict[str, Any]:
    if not row:
        return {}
    email, pw_hash, role, tabs, created_at, pw_updated = row
    return {
        "email":               email,
        "password_hash":       pw_hash,
        "role":                role or DEFAULT_ROLE,
        "tabs":                list(tabs or []),
        "created_at":          created_at.isoformat() if created_at else None,
        "password_updated_at": pw_updated.isoformat() if pw_updated else None,
    }


def _pg_find(email: str) -> Optional[Dict[str, Any]]:
    norm = email.strip().lower()
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT email, password_hash, role, tabs, created_at, password_updated_at "
                "FROM users WHERE email = %s",
                (norm,),
            )
            row = cur.fetchone()
    return _pg_row_to_user(row) if row else None


def _pg_list() -> List[Dict[str, Any]]:
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT email, password_hash, role, tabs, created_at, password_updated_at "
                "FROM users ORDER BY created_at"
            )
            rows = cur.fetchall()
    return [_pg_row_to_user(r) for r in rows]


def _pg_create(email: str, password_hash: str, role: str, tabs: list) -> Dict[str, Any]:
    norm = email.strip().lower()
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (email, password_hash, role, tabs) "
                "VALUES (%s, %s, %s, %s::jsonb) "
                "RETURNING email, password_hash, role, tabs, created_at, password_updated_at",
                (norm, password_hash, role, json.dumps(list(tabs))),
            )
            row = cur.fetchone()
        conn.commit()
    return _pg_row_to_user(row)


def _pg_update(email: str, role: Optional[str], tabs: Optional[list]) -> bool:
    sets, params = [], []
    if role is not None:
        sets.append("role = %s"); params.append(role)
    if tabs is not None:
        sets.append("tabs = %s::jsonb"); params.append(json.dumps(list(tabs)))
    if not sets:
        return True  # noop
    params.append(email.strip().lower())
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE users SET {', '.join(sets)} WHERE email = %s",
                tuple(params),
            )
            ok = cur.rowcount > 0
        conn.commit()
    return ok


def _pg_update_password(email: str, password_hash: str) -> bool:
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET password_hash = %s, password_updated_at = NOW() "
                "WHERE email = %s",
                (password_hash, email.strip().lower()),
            )
            ok = cur.rowcount > 0
        conn.commit()
    return ok


def _pg_delete(email: str) -> bool:
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE email = %s", (email.strip().lower(),))
            ok = cur.rowcount > 0
        conn.commit()
    return ok


def _pg_count_admins() -> int:
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
            (n,) = cur.fetchone()
    return int(n)


# ─────────────────────────────────────────────────────────────────────
# Public API — dispatches to the right backend based on DATABASE_URL.
# Signatures match the original JSON-only implementation so callers
# (api.py / admin.py / auth.py) don't change.
# ─────────────────────────────────────────────────────────────────────
def find_user(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None
    if db.is_enabled():
        return _pg_find(email)
    norm = email.strip().lower()
    for u in _json_load().get("users", []):
        if u.get("email", "").strip().lower() == norm:
            return u
    return None


def create_user(
    email: str,
    password_hash: str,
    role: str = "operator",
    tabs: Optional[list] = None,
) -> Dict[str, Any]:
    norm = email.strip().lower()
    if role not in VALID_ROLES:
        raise ValueError(f"Invalid role: {role!r}. Must be one of {VALID_ROLES}")
    tabs_list = list(tabs) if tabs is not None else []

    if db.is_enabled():
        # Same uniqueness contract as the JSON path.
        if _pg_find(norm):
            raise ValueError(f"User already exists: {norm}")
        return _pg_create(norm, password_hash, role, tabs_list)

    data = _json_load()
    if any(u.get("email", "").strip().lower() == norm for u in data["users"]):
        raise ValueError(f"User already exists: {norm}")
    user = {
        "email": norm, "password_hash": password_hash,
        "role": role, "tabs": tabs_list,
        "created_at": _now_iso(), "password_updated_at": None,
    }
    data["users"].append(user)
    _json_save(data)
    return user


def get_role(email: str) -> str:
    u = find_user(email)
    if not u:
        return DEFAULT_ROLE
    return u.get("role") or DEFAULT_ROLE


def get_tabs(email: str) -> list:
    u = find_user(email)
    if not u:
        return []
    return list(u.get("tabs") or [])


def list_users() -> list:
    if db.is_enabled():
        return [
            {k: u[k] for k in ("email", "role", "tabs", "created_at", "password_updated_at")}
            for u in _pg_list()
        ]
    out = []
    for u in _json_load().get("users", []):
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
    if role is not None and role not in VALID_ROLES:
        raise ValueError(f"Invalid role: {role!r}. Must be one of {VALID_ROLES}")
    if db.is_enabled():
        return _pg_update(email, role, tabs)

    data = _json_load()
    norm = email.strip().lower()
    for u in data["users"]:
        if u.get("email", "").strip().lower() == norm:
            if role is not None: u["role"] = role
            if tabs is not None: u["tabs"] = list(tabs)
            _json_save(data)
            return True
    return False


def delete_user(email: str) -> bool:
    if db.is_enabled():
        return _pg_delete(email)
    data = _json_load()
    norm = email.strip().lower()
    before = len(data["users"])
    data["users"] = [u for u in data["users"]
                     if u.get("email", "").strip().lower() != norm]
    if len(data["users"]) == before:
        return False
    _json_save(data)
    return True


def count_admins() -> int:
    if db.is_enabled():
        return _pg_count_admins()
    return sum(1 for u in _json_load().get("users", [])
               if (u.get("role") or DEFAULT_ROLE) == "admin")


def update_password(email: str, password_hash: str) -> bool:
    if db.is_enabled():
        return _pg_update_password(email, password_hash)
    data = _json_load()
    norm = email.strip().lower()
    for u in data["users"]:
        if u.get("email", "").strip().lower() == norm:
            u["password_hash"] = password_hash
            u["password_updated_at"] = _now_iso()
            _json_save(data)
            return True
    return False


def ensure_initial_user() -> bool:
    """Create the bootstrap user if absent.  Idempotent — safe to call
    on every startup.  Returns True if a user was created."""
    from weekly_app.core import security  # local import to avoid circular
    if find_user(INITIAL_USER_EMAIL):
        return False
    create_user(
        INITIAL_USER_EMAIL,
        security.hash_password(INITIAL_USER_PASSWORD),
        role="admin",
    )
    return True


def migrate_json_to_pg() -> int:
    """One-time bootstrap migration.

    Runs on Render startup: if Postgres is enabled and the users table
    is empty, copy whatever the local JSON file holds.  Saves the
    operator the manual step of re-creating their bootstrap account in
    a fresh DB.  Idempotent — returns 0 once the table has users.
    """
    if not db.is_enabled():
        return 0
    try:
        if _pg_list():   # already populated
            return 0
        if not USERS_FILE.exists():
            return 0
        data = _json_load_uncached()
        users = data.get("users", [])
        copied = 0
        for u in users:
            try:
                _pg_create(
                    u.get("email", ""),
                    u.get("password_hash", ""),
                    u.get("role") or DEFAULT_ROLE,
                    list(u.get("tabs") or []),
                )
                copied += 1
            except Exception as e:
                print(f"[auth] migration skipped {u.get('email')}: {e!r}")
        if copied:
            print(f"[auth] migrated {copied} user(s) from data/users.json into Postgres")
        return copied
    except Exception as e:
        print(f"[auth] migration failed: {e!r}")
        return 0
