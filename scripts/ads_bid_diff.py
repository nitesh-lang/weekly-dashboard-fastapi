"""Diff the two most recent snapshots per profile from
`scripts/ads_bid_snapshot.py` and flag bid-change bursts.

Detects the class of event we're hunting: 100s of keyword bids changing
in a single interval (Nexlev W26 saw 602 keyword bids change at 02:26 IST
Jul 2 by an unidentified actor — Amazon's IN Ads Console doesn't expose
the `Changed by` field, so this script is our own audit trail).

For each profile:
  - Loads latest.json.gz + the most recent *timestamped* snapshot that
    is NOT latest (so we always compare "this run" vs "previous run").
  - Diffs by primary key per collection:
        campaigns   : campaignId
        adGroups    : adGroupId
        keywords    : keywordId
        targets     : targetId
  - Categorises each change as:
        bid_changed     (bid or defaultBid delta)
        state_changed   (ENABLED → PAUSED etc.)
        budget_changed  (campaign budget delta)
        added / removed (row appeared / disappeared)
  - If the total for a profile exceeds BURST_THRESHOLD, marks the run
    as a burst and dumps every change into a
    data/processed/ads_bid_diffs/BURST_<profile>_<ts>.xlsx for you to
    attach to an Amazon Ads support ticket.

Every run also writes a per-profile summary CSV so a mild change stream
is still visible.

Exit code: 0 always (the workflow uses the presence of BURST_*.xlsx to
decide whether to open an alert issue).
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SNAP_BASE = ROOT / "data" / "processed" / "ads_bid_snapshots"
DIFF_BASE = ROOT / "data" / "processed" / "ads_bid_diffs"

# A "burst" is >= this many total changes in one interval for one
# profile.  Nexlev's 02:26 IST event was 602 keyword bid changes; a
# healthy run rarely has more than ~10-20 organic edits per profile
# per 30 min.
BURST_THRESHOLD = 50


def _load_snapshot(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def _two_most_recent(profile_dir: Path) -> tuple[Path | None, Path | None]:
    """Return (previous, current) snapshot paths — sorted by ts filename."""
    snaps = sorted(profile_dir.glob("*.json.gz"), reverse=True)
    if len(snaps) < 2:
        return None, None
    return snaps[1], snaps[0]


def _index(rows: list[dict], key: str) -> dict:
    return {str(r.get(key)): r for r in rows if r.get(key) is not None}


def _diff_collection(
    prev: list[dict], curr: list[dict], key: str,
    numeric_fields: list[str], state_field: str | None = None,
) -> list[dict]:
    """Row-level diff.  Returns a list of change records."""
    prev_idx = _index(prev, key)
    curr_idx = _index(curr, key)

    changes: list[dict] = []

    # Additions
    for k in curr_idx.keys() - prev_idx.keys():
        c = curr_idx[k]
        changes.append({
            "kind":         "added",
            "entity_id":    k,
            "campaign_id":  c.get("campaignId"),
            "ad_group_id":  c.get("adGroupId"),
            "field":        "*",
            "from_value":   None,
            "to_value":     "row_created",
            "snapshot_current": c,
        })
    # Removals
    for k in prev_idx.keys() - curr_idx.keys():
        p = prev_idx[k]
        changes.append({
            "kind":         "removed",
            "entity_id":    k,
            "campaign_id":  p.get("campaignId"),
            "ad_group_id":  p.get("adGroupId"),
            "field":        "*",
            "from_value":   "row_existed",
            "to_value":     None,
            "snapshot_prev": p,
        })
    # Common → check numeric + state fields
    for k in prev_idx.keys() & curr_idx.keys():
        p, c = prev_idx[k], curr_idx[k]
        for fld in numeric_fields:
            pv, cv = p.get(fld), c.get(fld)
            if pv is None and cv is None:
                continue
            if pv != cv:
                kind = "bid_changed" if fld in ("bid", "defaultBid") else "budget_changed"
                changes.append({
                    "kind":         kind,
                    "entity_id":    k,
                    "campaign_id":  c.get("campaignId"),
                    "ad_group_id":  c.get("adGroupId"),
                    "field":        fld,
                    "from_value":   pv,
                    "to_value":     cv,
                })
        if state_field:
            ps, cs = p.get(state_field), c.get(state_field)
            if ps != cs:
                changes.append({
                    "kind":         "state_changed",
                    "entity_id":    k,
                    "campaign_id":  c.get("campaignId"),
                    "ad_group_id":  c.get("adGroupId"),
                    "field":        state_field,
                    "from_value":   ps,
                    "to_value":     cs,
                })
    return changes


def _label_change(row: dict, keyword_by_id: dict, target_by_id: dict) -> dict:
    """Enrich with human-readable labels (keyword text, target expression)."""
    out = dict(row)
    kid = row.get("entity_id")
    if kid in keyword_by_id:
        kw = keyword_by_id[kid]
        out["keyword_text"] = kw.get("keywordText")
        out["match_type"]   = kw.get("matchType")
    if kid in target_by_id:
        t = target_by_id[kid]
        # target expression is a list of {type,value}; flatten to text
        expr = t.get("expression") or []
        out["target_expression"] = "; ".join(
            f"{x.get('type')}={x.get('value')}" for x in expr
        )
    return out


def _diff_profile(prev: dict, curr: dict) -> tuple[list[dict], dict]:
    """Return (change_records, per-collection totals)."""
    changes = []
    changes += _diff_collection(
        prev.get("campaigns", []), curr.get("campaigns", []),
        key="campaignId",
        numeric_fields=[],  # campaigns don't have a bid directly
        state_field="state",
    )
    # Budget lives under campaign.budget.amount (v3) — pull out for diffing
    def _budget_amount(c: dict) -> float | None:
        b = c.get("budget") or {}
        v = b.get("budget") if "budget" in b else b.get("amount")
        try:
            return float(v) if v is not None else None
        except Exception:
            return None
    prev_camp_bud = {str(c.get("campaignId")): _budget_amount(c) for c in prev.get("campaigns", [])}
    curr_camp_bud = {str(c.get("campaignId")): _budget_amount(c) for c in curr.get("campaigns", [])}
    for k in prev_camp_bud.keys() & curr_camp_bud.keys():
        if prev_camp_bud[k] != curr_camp_bud[k]:
            changes.append({
                "kind":        "budget_changed",
                "entity_id":   k,
                "campaign_id": k,
                "ad_group_id": None,
                "field":       "budget.amount",
                "from_value":  prev_camp_bud[k],
                "to_value":    curr_camp_bud[k],
            })

    changes += _diff_collection(
        prev.get("adGroups", []), curr.get("adGroups", []),
        key="adGroupId",
        numeric_fields=["defaultBid"],
        state_field="state",
    )
    changes += _diff_collection(
        prev.get("keywords", []), curr.get("keywords", []),
        key="keywordId",
        numeric_fields=["bid"],
        state_field="state",
    )
    changes += _diff_collection(
        prev.get("targets", []), curr.get("targets", []),
        key="targetId",
        numeric_fields=["bid"],
        state_field="state",
    )

    # Enrich with keyword text + target expression from current snapshot
    kw_by_id     = {str(k.get("keywordId")): k for k in curr.get("keywords", [])}
    target_by_id = {str(t.get("targetId")):  t for t in curr.get("targets", [])}
    enriched = [_label_change(c, kw_by_id, target_by_id) for c in changes]

    totals = {
        "total":           len(enriched),
        "bid_changed":     sum(1 for c in enriched if c["kind"] == "bid_changed"),
        "state_changed":   sum(1 for c in enriched if c["kind"] == "state_changed"),
        "budget_changed":  sum(1 for c in enriched if c["kind"] == "budget_changed"),
        "added":           sum(1 for c in enriched if c["kind"] == "added"),
        "removed":         sum(1 for c in enriched if c["kind"] == "removed"),
    }
    return enriched, totals


def main() -> int:
    DIFF_BASE.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if not SNAP_BASE.exists():
        print(f"No snapshots directory at {SNAP_BASE} — run ads_bid_snapshot.py first.")
        return 0

    burst_files: list[Path] = []
    summary_rows: list[dict] = []

    for profile_dir in sorted(SNAP_BASE.iterdir()):
        if not profile_dir.is_dir():
            continue
        pid = profile_dir.name
        prev_path, curr_path = _two_most_recent(profile_dir)
        if not (prev_path and curr_path):
            print(f"[{pid}] skipping — need at least 2 snapshots")
            continue

        curr = _load_snapshot(curr_path)
        prev = _load_snapshot(prev_path)
        changes, totals = _diff_profile(prev, curr)

        print(f"[{pid}] {curr.get('profile_label')} — total {totals['total']} "
              f"(bid {totals['bid_changed']}, state {totals['state_changed']}, "
              f"budget {totals['budget_changed']}, +{totals['added']} -{totals['removed']})")

        summary_rows.append({
            "profile_id":     pid,
            "profile_label":  curr.get("profile_label"),
            "prev_snapshot":  prev.get("captured_at"),
            "curr_snapshot":  curr.get("captured_at"),
            **totals,
        })

        # Always write the change list (empty CSV is fine — it's traceable).
        out_csv = DIFF_BASE / f"{pid}_{now}.csv"
        pd.DataFrame(changes).to_csv(out_csv, index=False)

        if totals["total"] >= BURST_THRESHOLD:
            burst_path = DIFF_BASE / f"BURST_{pid}_{now}.xlsx"
            pd.DataFrame(changes).to_excel(burst_path, index=False)
            burst_files.append(burst_path)
            print(f"  ⚠ BURST detected — {totals['total']} changes → {burst_path.name}")

    # Summary row for the current run
    if summary_rows:
        pd.DataFrame(summary_rows).to_csv(DIFF_BASE / f"summary_{now}.csv", index=False)

    if burst_files:
        # Emit a marker file the workflow can grep for to decide whether
        # to open an alert issue.
        (DIFF_BASE / f"BURST_MARKER_{now}.txt").write_text(
            "\n".join(str(p.relative_to(ROOT)) for p in burst_files),
            encoding="utf-8",
        )
        print(f"\n{len(burst_files)} burst(s) detected — workflow should alert")
    return 0


if __name__ == "__main__":
    sys.exit(main())
