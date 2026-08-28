"""Brand registry.

Each brand entry pairs a display key with:
  - the services module that owns its business logic (ported unchanged
    from the standalone dashboards — nothing removed)
  - the planning-file directory (per-brand ASIN plan)
  - the SP-API accounts to fetch daily sales from
    · seller  → GET_SALES_AND_TRAFFIC_REPORT (3P orders)
    · vendor  → GET_VENDOR_SALES_REPORT (1P orders)

Same-account, multiple-brand pulls are fine: each brand's build_rows
filters incoming rows against its OWN planning ASINs, so a row that
lives in "Audio Array" seller account but belongs to a Nexlev ASIN
lands in Nexlev's ledger only, and vice versa.
"""
from __future__ import annotations

import functools
import importlib
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import List

BASE_DIR = Path(__file__).resolve().parent.parent


# ── Planning-file caching (transparent) ───────────────────
# The two ported services modules read the ASIN Planning Excel file on every
# call to load_planning_main / load_planning_category. That's the biggest
# per-request cost. We wrap them with a small TTL cache keyed by month so the
# 6-8 planning workbooks in the folder are read at most every 15 minutes.
_PLANNING_TTL_SEC = 900


def _install_planning_cache(mod: ModuleType) -> None:
    if getattr(mod, "_planning_cache_installed", False):
        return
    for name in ("load_planning_main", "load_planning_category"):
        fn = getattr(mod, name, None)
        if fn is None or not callable(fn):
            continue
        cache: dict[str, tuple[float, object]] = {}

        @functools.wraps(fn)
        def wrapper(ref_date, _fn=fn, _cache=cache):
            key = str(ref_date)[:10]  # date-only key
            now = time.time()
            hit = _cache.get(key)
            if hit and (now - hit[0]) < _PLANNING_TTL_SEC:
                return hit[1]
            val = _fn(ref_date)
            # Some planning workbooks have duplicate normalized column names
            # (Nexlev Aug 2026 Category sheet had two `perdaygoal` columns).
            # Pandas returns a DataFrame for a bracket-lookup on a duplicate
            # name — services then crash on `df[col] = df[col] * x`. Drop
            # duplicates (keep first) so the ported code sees a Series.
            if hasattr(val, "columns"):
                dup_mask = val.columns.duplicated()
                if dup_mask.any():
                    val = val.loc[:, ~dup_mask]
            _cache[key] = (now, val)
            return val

        setattr(mod, name, wrapper)
    mod._planning_cache_installed = True  # type: ignore[attr-defined]


@dataclass
class SpAccount:
    """One Amazon account whose orders feed a brand's daily sales."""

    label: str                       # e.g. "Nexlev" — matches build_rows' `account` arg
    kind: str                        # "seller" | "vendor"
    lwa_client_id_env: str
    lwa_client_secret_env: str
    refresh_token_env: str


@dataclass
class Brand:
    key: str                         # url-safe (matches session var)
    label: str                       # display name
    services_module: str             # dotted path
    planning_dir: Path
    sp_accounts: List[SpAccount]

    def load_services(self) -> ModuleType:
        mod = importlib.import_module(self.services_module)
        # The ported services modules define PLANNING_FOLDER as a
        # CWD-relative path ("data/planning/<brand>") — correct only when
        # the process happens to start in backend/. Pin it to this Brand's
        # absolute planning_dir so planning resolution is CWD-independent
        # (required when the app is mounted inside another service, e.g.
        # the Weekly dashboard, whose CWD is its own repo root).
        if hasattr(mod, "PLANNING_FOLDER"):
            mod.PLANNING_FOLDER = str(self.planning_dir)
        _install_planning_cache(mod)
        return mod


# ── SP account presets (reused across brands) ─────────────
_SELLER_NEXLEV = SpAccount(
    label="Nexlev",
    kind="seller",
    lwa_client_id_env="SP_LWA_CLIENT_ID",
    lwa_client_secret_env="SP_LWA_CLIENT_SECRET",
    refresh_token_env="SP_REFRESH_TOKEN_NEXLEV",
)
_SELLER_VIOMI = SpAccount(
    label="Viomi By Cambium",
    kind="seller",
    lwa_client_id_env="SP_LWA_CLIENT_ID",
    lwa_client_secret_env="SP_LWA_CLIENT_SECRET",
    refresh_token_env="SP_REFRESH_TOKEN_VIOMI",
)
_SELLER_CAMBIUM = SpAccount(
    label="Cambium Retail",
    kind="seller",
    lwa_client_id_env="SP_LWA_CLIENT_ID",
    lwa_client_secret_env="SP_LWA_CLIENT_SECRET",
    refresh_token_env="SP_REFRESH_TOKEN_CAMBIUMRETAIL",
)
_SELLER_AUDIOARRAY = SpAccount(
    label="Audio Array",
    kind="seller",
    lwa_client_id_env="SP_LWA_CLIENT_ID",
    lwa_client_secret_env="SP_LWA_CLIENT_SECRET",
    refresh_token_env="SP_REFRESH_TOKEN_AUDIOARRAY",
)
_VENDOR_AUDIOARRAY = SpAccount(
    label="Vendor Central",
    kind="vendor",
    lwa_client_id_env="SP_LWA_CLIENT_ID_AUDIOARRAY",
    lwa_client_secret_env="SP_LWA_CLIENT_SECRET_AUDIOARRAY",
    refresh_token_env="SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY",
)


BRANDS: dict[str, Brand] = {
    "nexlev": Brand(
        key="nexlev",
        label="Nexlev",
        services_module="app.services.nexlev",
        planning_dir=BASE_DIR / "data" / "planning" / "nexlev",
        sp_accounts=[
            _SELLER_NEXLEV,
            _SELLER_VIOMI,
            _SELLER_CAMBIUM,
            _SELLER_AUDIOARRAY,
        ],
    ),
    "audio_array": Brand(
        key="audio_array",
        label="Audio Array",
        services_module="app.services.audio_array",
        planning_dir=BASE_DIR / "data" / "planning" / "audio_array",
        sp_accounts=[
            _VENDOR_AUDIOARRAY,
            _SELLER_AUDIOARRAY,
            _SELLER_VIOMI,
            _SELLER_CAMBIUM,
        ],
    ),
}


def get_brand(key: str) -> Brand | None:
    return BRANDS.get(key)


def list_brands() -> list[dict]:
    return [{"key": b.key, "label": b.label} for b in BRANDS.values()]
