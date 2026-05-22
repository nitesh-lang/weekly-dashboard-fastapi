"""ASIN → brand mapping from data/master/sku_master.xlsx (read-only)."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import MASTER_PATH


@dataclass
class AsinBrandMap:
    asin_to_brand: dict[str, str]      # ASIN → canonical brand key (e.g. "nexlev")
    asin_to_display: dict[str, str]    # ASIN → display name (e.g. "Nexlev")
    known_brands: set[str]             # canonical keys covered

    def resolve(self, asin: str) -> tuple[str | None, str | None]:
        a = (asin or "").strip().upper()
        return self.asin_to_brand.get(a), self.asin_to_display.get(a)


def _canonical_key(display: str) -> str:
    return display.strip().lower().replace(" ", "_")


def load_master_map(
    brand_display_names: dict[str, str],
    *,
    path: Path | None = None,
) -> AsinBrandMap:
    target_path = Path(path) if path else MASTER_PATH
    if not target_path.exists():
        raise FileNotFoundError(f"Master not found: {target_path}")

    df = pd.read_excel(target_path, dtype=str)
    needed = {"ASIN", "Brand"}
    missing = needed - set(df.columns)
    if missing:
        raise RuntimeError(f"sku_master.xlsx missing columns: {sorted(missing)}")

    df = df[["ASIN", "Brand"]].copy()
    df["ASIN"] = df["ASIN"].fillna("").str.strip().str.upper()
    df["Brand"] = df["Brand"].fillna("").str.strip()
    df = df[df["ASIN"] != ""]

    allowed_displays = {v.strip(): k for k, v in brand_display_names.items()}

    asin_to_brand: dict[str, str] = {}
    asin_to_display: dict[str, str] = {}
    for asin, brand_display in zip(df["ASIN"], df["Brand"]):
        if not brand_display:
            continue
        key = allowed_displays.get(brand_display)
        if key is None:
            continue  # brand not in our 4-brand scope (e.g. Fossil) → drop
        asin_to_brand[asin] = key
        asin_to_display[asin] = brand_display

    return AsinBrandMap(
        asin_to_brand=asin_to_brand,
        asin_to_display=asin_to_display,
        known_brands=set(brand_display_names.keys()),
    )
