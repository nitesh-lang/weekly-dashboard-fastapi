"""Phase 1 config — loads .env + brands.json into a single object."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent.parent
ENV_PATH = HERE / ".env"
BRANDS_PATH = HERE / "brands.json"

MASTER_PATH = PROJECT_ROOT / "data" / "master" / "sku_master.xlsx"
OUTPUT_ROOT = PROJECT_ROOT / "data" / "raw_phase1" / "ads"

ADS_API_ENDPOINTS = {
    # India profiles live on the EU regional endpoint (no in-country host).
    "IN": "https://advertising-api-eu.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "NA": "https://advertising-api.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"


@dataclass
class AdsProfile:
    profile_id_env: str
    source_brand: str
    merge_into: Optional[str]
    profile_id: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class Config:
    client_id: str
    client_secret: str
    refresh_token: str
    region: str
    marketplace_id: str
    currency: str
    brand_display_names: dict[str, str]
    ads_profiles: list[AdsProfile] = field(default_factory=list)

    @property
    def ads_endpoint(self) -> str:
        return ADS_API_ENDPOINTS[self.region]


def load_config() -> Config:
    if not ENV_PATH.exists():
        raise FileNotFoundError(
            f"Missing {ENV_PATH}. Copy .env.example to .env and fill in values."
        )
    load_dotenv(ENV_PATH, override=True)

    required = {
        "ADS_API_CLIENT_ID": "client_id",
        "ADS_API_CLIENT_SECRET": "client_secret",
        "ADS_API_REFRESH_TOKEN": "refresh_token",
    }
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            f"Missing required env vars in {ENV_PATH}: {', '.join(missing)}"
        )

    region = (os.getenv("ADS_API_REGION") or "IN").upper()
    if region not in ADS_API_ENDPOINTS:
        raise ValueError(f"ADS_API_REGION={region} not in {list(ADS_API_ENDPOINTS)}")

    raw = json.loads(BRANDS_PATH.read_text(encoding="utf-8"))

    profiles: list[AdsProfile] = []
    for entry in raw["ads_profiles"]:
        env_key = entry["profile_id_env"]
        profiles.append(
            AdsProfile(
                profile_id_env=env_key,
                source_brand=entry["source_brand"],
                merge_into=entry.get("merge_into"),
                profile_id=os.getenv(env_key) or None,
                notes=entry.get("notes"),
            )
        )

    return Config(
        client_id=os.environ["ADS_API_CLIENT_ID"],
        client_secret=os.environ["ADS_API_CLIENT_SECRET"],
        refresh_token=os.environ["ADS_API_REFRESH_TOKEN"],
        region=region,
        marketplace_id=raw["marketplace_id"],
        currency=raw["currency"],
        brand_display_names=raw["brand_display_names"],
        ads_profiles=profiles,
    )
