"""Seed the users table with the Cambium roster on first boot.

Idempotent: if a user already exists, we don't touch their password or role
(so an admin who's already changed their password isn't reset). New users
are inserted with must_reset_password=True + a shared bootstrap password
they change on first login.
"""
from __future__ import annotations

import bcrypt
from sqlalchemy import text

from .database import engine

DEFAULT_PASSWORD = "Cambium@109"

ROSTER: list[tuple[str, str, str]] = [
    # (email, full_name, role)
    ("info@cambiumretail.com", "Info · Admin", "admin"),
    ("nitesh@cambiumretail.com", "Nitesh", "admin"),
    ("ceo@cambiumretail.com", "CEO", "viewer"),
    ("vishwanath@cambiumretail.com", "Vishwanath", "viewer"),
    ("zaid@cambiumretail.com", "Zaid", "viewer"),
    ("kanwal@cambiumretail.com", "Kanwal", "viewer"),
    ("unmesha@cambiumretail.com", "Unmesha", "viewer"),
    ("hazique@cambiumretail.com", "Hazique", "viewer"),
    ("sagar@cambiumretail.com", "Sagar Maharana", "viewer"),
]


def seed_users() -> None:
    with engine.begin() as conn:
        existing = {
            row[0]
            for row in conn.execute(text("SELECT email FROM users")).fetchall()
        }
        inserted = 0
        for email, full_name, role in ROSTER:
            e = email.strip().lower()
            if e in existing:
                continue
            pw_hash = bcrypt.hashpw(DEFAULT_PASSWORD.encode(), bcrypt.gensalt())
            # info@ keeps its existing password (already set via env in main.py's
            # older auth), so treat it as no-reset-required. Everyone else must
            # change on first login.
            must_reset = e != "info@cambiumretail.com"
            conn.execute(
                text(
                    "INSERT INTO users (email, full_name, password_hash, role, "
                    "is_active, must_reset_password) "
                    "VALUES (:e, :n, :h, :r, TRUE, :m)"
                ),
                {"e": e, "n": full_name, "h": pw_hash, "r": role, "m": must_reset},
            )
            inserted += 1
        if inserted:
            print(f"[users] seeded {inserted} new user(s)")
