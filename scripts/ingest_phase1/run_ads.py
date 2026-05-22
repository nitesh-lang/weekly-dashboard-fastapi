"""CLI entry for Phase 1 Ads ingestion.

Usage:
  python -m scripts.ingest_phase1.run_ads --test-auth
  python -m scripts.ingest_phase1.run_ads --list-profiles
  python -m scripts.ingest_phase1.run_ads --week 19 --year 2026
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

# Windows cp1252 console can't print check-marks / arrows. Force UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass

import pandas as pd

from .ads_api import OUTPUT_COLUMNS, fetch_reports_for_profile, list_profiles, week_bounds
from .auth import token_diagnostics
from .config import OUTPUT_ROOT, Config, load_config
from .master_split import AsinBrandMap, load_master_map


def _brand_folder_name(cfg: Config, brand_key: str) -> str:
    """Match existing data/ams_weekly_data/<Folder> naming: 'Audio_Array', 'White_Mulberry'."""
    display = cfg.brand_display_names.get(brand_key, brand_key)
    return display.strip().replace(" ", "_")


def _split_by_brand(
    sheet: str,
    df: pd.DataFrame,
    profile_source_brand: str,
    profile_merge_into: str | None,
    cfg: Config,
    master: AsinBrandMap,
) -> tuple[dict[str, pd.DataFrame], int]:
    """
    Returns ({brand_key: df}, unassigned_count).
    Brand routing rules:
      1. If row has an ASIN we know in master → master brand wins.
      2. Else if profile has merge_into → row goes to merge_into brand.
      3. Else if profile source_brand is one of our 4 brands → goes there.
      4. Else → Unassigned.
    """
    target_brands = set(cfg.brand_display_names.keys())
    routed: dict[str, list[dict]] = defaultdict(list)
    unassigned: list[dict] = []

    fallback_brand: str | None = None
    if profile_merge_into and profile_merge_into in target_brands:
        fallback_brand = profile_merge_into
    elif profile_source_brand in target_brands:
        fallback_brand = profile_source_brand

    for row in df.to_dict(orient="records"):
        asin = (row.get("Advertised ASIN") or "").strip().upper()
        brand_key, _ = master.resolve(asin) if asin else (None, None)

        if brand_key in target_brands:
            routed[brand_key].append(row)
            continue
        if fallback_brand:
            # Sheet has no ASIN (e.g. SB campaign-level) — use the profile's brand.
            if not asin:
                routed[fallback_brand].append(row)
                continue
            # Sheet has an ASIN but master doesn't know it — drop to Unassigned,
            # operator needs to fill master before we can route confidently.
            unassigned.append({**row, "_source_profile": profile_source_brand, "_sheet": sheet})
            continue
        unassigned.append({**row, "_source_profile": profile_source_brand, "_sheet": sheet})

    out: dict[str, pd.DataFrame] = {}
    for brand_key, rows in routed.items():
        out[brand_key] = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    if unassigned:
        out["__unassigned__"] = pd.DataFrame(unassigned)
    return out, len(unassigned)


def _write_brand_xlsx(
    out_dir: Path,
    week: int,
    cfg: Config,
    sheets_by_brand: dict[str, dict[str, pd.DataFrame]],
) -> list[Path]:
    """sheets_by_brand: {brand_key: {sheet_name: df}}"""
    written: list[Path] = []
    for brand_key, sheets in sheets_by_brand.items():
        folder = out_dir / _brand_folder_name(cfg, brand_key)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"ads_report_week{week}.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            wrote_any = False
            for sheet in ("SP", "SD", "SB"):
                df = sheets.get(sheet, pd.DataFrame(columns=OUTPUT_COLUMNS))
                df.to_excel(writer, sheet_name=sheet, index=False)
                wrote_any = True
            if not wrote_any:
                pd.DataFrame(columns=OUTPUT_COLUMNS).to_excel(writer, sheet_name="SP", index=False)
        written.append(path)
    return written


def _write_unassigned(out_dir: Path, week: int, frames: list[pd.DataFrame]) -> Path | None:
    if not frames:
        return None
    folder = out_dir / "Unassigned"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"ads_report_week{week}.xlsx"
    combined = pd.concat(frames, ignore_index=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet, group in combined.groupby("_sheet"):
            group.drop(columns=["_sheet"], errors="ignore").to_excel(
                writer, sheet_name=sheet, index=False
            )
    return path


def cmd_test_auth(cfg: Config) -> int:
    info = token_diagnostics(cfg)
    print(f"✓ Auth OK. Access token minted, expires in {info['expires_in_seconds']} seconds.")
    print(f"  mint latency: {info['mint_latency_ms']} ms")
    print(f"  token prefix: {info['token_prefix']}")
    return 0


def cmd_list_profiles(cfg: Config) -> int:
    profiles = list_profiles(cfg)
    if not profiles:
        print("No profiles visible to this token.")
        return 1
    header = f"{'profile_id':<14} {'country':<8} {'currency':<9} {'account_id':<14} {'account_type':<14} name"
    print(header)
    print("-" * len(header))
    for p in profiles:
        acct = p.get("accountInfo", {}) or {}
        print(
            f"{str(p.get('profileId','')):<14} "
            f"{str(p.get('countryCode','')):<8} "
            f"{str(p.get('currencyCode','')):<9} "
            f"{str(acct.get('id','')):<14} "
            f"{str(acct.get('type','')):<14} "
            f"{acct.get('name','')}"
        )
    return 0


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def cmd_run_week(
    cfg: Config,
    week: int,
    year: int,
    *,
    start_override: str | None = None,
    end_override: str | None = None,
    only_profile: str | None = None,
) -> int:
    if start_override and end_override:
        start = _parse_iso_date(start_override)
        end = _parse_iso_date(end_override)
        print(f"Week {week} of {year} -> {start} to {end} (operator override)")
    else:
        start, end = week_bounds(week, year)
        print(f"Week {week} of {year} -> {start} to {end} (Sun-Sat default)")

    profiles_with_id = [p for p in cfg.ads_profiles if p.profile_id]
    if only_profile:
        profiles_with_id = [p for p in profiles_with_id if p.source_brand == only_profile]
        if not profiles_with_id:
            valid = sorted({p.source_brand for p in cfg.ads_profiles})
            print(f"✗ --profile {only_profile!r} not found. Valid: {valid}")
            return 2
    if not profiles_with_id:
        print("✗ No profile IDs filled in .env — run --list-profiles first.")
        return 2

    print(f"Loading sku_master.xlsx → ASIN/brand map…")
    master = load_master_map(cfg.brand_display_names)
    print(f"  master has {len(master.asin_to_brand)} ASINs across {sorted(set(master.asin_to_brand.values()))}")

    week_dir = OUTPUT_ROOT / f"Week {week}"
    week_dir.mkdir(parents=True, exist_ok=True)

    sheets_by_brand: dict[str, dict[str, pd.DataFrame]] = defaultdict(dict)
    unassigned_frames: list[pd.DataFrame] = []

    for prof in profiles_with_id:
        label = prof.source_brand
        print(f"\n--- Profile: {label} (id={prof.profile_id}) ---")
        if prof.notes:
            print(f"    note: {prof.notes}")
        sheets = fetch_reports_for_profile(
            cfg, prof.profile_id, start=start, end=end, profile_label=label
        )
        for sheet_name, df in sheets.items():
            routed, n_unassigned = _split_by_brand(
                sheet_name, df, prof.source_brand, prof.merge_into, cfg, master
            )
            if n_unassigned:
                print(f"  ⚠ {n_unassigned} {sheet_name} rows from {label} → Unassigned (ASIN not in sku_master)")
            for brand_key, brand_df in routed.items():
                if brand_key == "__unassigned__":
                    unassigned_frames.append(brand_df)
                    continue
                existing = sheets_by_brand[brand_key].get(sheet_name)
                if existing is None:
                    sheets_by_brand[brand_key][sheet_name] = brand_df
                else:
                    sheets_by_brand[brand_key][sheet_name] = pd.concat(
                        [existing, brand_df], ignore_index=True
                    )

    written = _write_brand_xlsx(week_dir, week, cfg, sheets_by_brand)
    unassigned_path = _write_unassigned(week_dir, week, unassigned_frames)

    print("\n=== Output ===")
    for p in written:
        try:
            rel = p.relative_to(OUTPUT_ROOT.parent.parent)
        except ValueError:
            rel = p
        print(f"  ✓ {rel}")
    if unassigned_path:
        try:
            rel = unassigned_path.relative_to(OUTPUT_ROOT.parent.parent)
        except ValueError:
            rel = unassigned_path
        print(f"  ⚠ {rel}  (review these — ASINs not in master)")
    print(f"\nDone. {len(written)} brand files + {'1' if unassigned_path else '0'} unassigned file.")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 1 Ads ingestion (local, isolated).")
    p.add_argument("--test-auth", action="store_true", help="Verify LWA refresh-token works.")
    p.add_argument("--list-profiles", action="store_true", help="List Ads API profiles visible to the token.")
    p.add_argument("--week", type=int, help="Week number used in output filename (ads_report_weekN.xlsx).")
    p.add_argument("--year", type=int, default=2026, help="Year (default: 2026).")
    p.add_argument("--start", help="Override start date (YYYY-MM-DD). Use with --end.")
    p.add_argument("--end", help="Override end date (YYYY-MM-DD). Use with --start.")
    p.add_argument("--profile", help="Run only this source_brand (e.g. nexlev, audio_array, white_mulberry, cambium_retail).")
    args = p.parse_args(argv)
    if bool(args.start) ^ bool(args.end):
        p.error("--start and --end must be used together")

    try:
        cfg = load_config()
    except (FileNotFoundError, RuntimeError, ValueError) as e:
        print(f"✗ Config error: {e}", file=sys.stderr)
        return 2

    if args.test_auth:
        return cmd_test_auth(cfg)
    if args.list_profiles:
        return cmd_list_profiles(cfg)
    if args.week is not None:
        return cmd_run_week(
            cfg, args.week, args.year,
            start_override=args.start, end_override=args.end,
            only_profile=args.profile,
        )

    p.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
