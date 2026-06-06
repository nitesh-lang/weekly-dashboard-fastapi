"""Single source of truth for channel-bucket mappings.

Any route that filters or rolls up inventory by channel MUST import
from here.  This prevents the class of bug where two routes silently
disagree on a business rule (e.g., whether B2B-AMPM counts as Amazon-
side stock).  When the operator's rule changes, edit ONE constant here
and every consuming route updates together.

Operator rule documented 2026-06-06.
"""
from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────
# Amazon-side inventory.  Used by:
#   - /amazon-sales-trend  (AM_sales_trend.load_inventory)
#   - /ams-trend           (ams_trend.load_inventory_snapshot
#                            → inventory_total_amazon column)
#
# Plain AMPM only — B2B-AMPM is allocated to wholesale and does NOT
# count.  YNT (dispatch), Blinkit (marketplace) and Pipeline / Open
# Order (in-transit POs) are non-Amazon channels.
# ─────────────────────────────────────────────────────────────────────
AMAZON_SIDE_CHANNELS: set[str] = {"ampm", "amazon", "1p"}


# ─────────────────────────────────────────────────────────────────────
# Vendor POs in flight to operator's warehouse.  Displayed on AMS Trend
# in its own column (pipeline_orders) for visibility but NEVER added
# into inventory_total_amazon.
# ─────────────────────────────────────────────────────────────────────
PIPELINE_CHANNELS: set[str] = {"pipeline", "pipeline order", "open order"}


# ─────────────────────────────────────────────────────────────────────
# Total physical stock (every storage location whether sellable now or
# in transit).  Used by /sales-trend's inventory column and matches the
# /inventory-dashboard's total per (brand, model) sum.
#
# No filter set is published because the union of all channels is the
# safe default — adding a new channel in the future won't accidentally
# exclude it.  Routes that want "total stock" should NOT filter.
# ─────────────────────────────────────────────────────────────────────
def is_amazon_side(channel: str) -> bool:
    return str(channel or "").strip().lower() in AMAZON_SIDE_CHANNELS


def is_pipeline(channel: str) -> bool:
    return str(channel or "").strip().lower() in PIPELINE_CHANNELS
