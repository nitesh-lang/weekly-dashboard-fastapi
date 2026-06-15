# Vendor SOH Pull — Notes for the Weekly Claude Agent

## What this script does

`scripts/sp_vendor_soh_pull.py` pulls **1P (Vendor Central) sellable on-hand inventory per ASIN** from Amazon SP-API for two vendor accounts and drops per-brand xlsx files into the current week's inventory folder.

| Vendor account | Brands it covers | Feeds (in AM_Replenishment app) |
|---|---|---|
| `AUDIOARRAY` (Cambium Retail Pvt Ltd - Mumbai - cb) | Audio Array + Tonor | CB Replenishment |
| `WHITEMULBERRY` (Clicktech vendor entity) | White Mulberry | Clicktech Replenishment |

Output paths (auto-computed from snapshot date using Sun-Sat week convention):

```
data/raw/inventory/Week {NN}/Audio_Array/Vendor SOH (SP-API).xlsx
data/raw/inventory/Week {NN}/Tonor/Vendor SOH (SP-API).xlsx
data/raw/inventory/Week {NN}/White_Mulberry/Vendor SOH (SP-API).xlsx
data/raw/inventory/Week {NN}/_audit/vendor_soh_unmatched_<acct>.csv
```

## Schema produced

Matches the `channel == "1p"` slice of the existing `Inventory Snapshot.xlsx`:

```
SKU, ASIN, Brand, Model, category_l0, category_l1, category_l2,
NLC, Qty, Channel (always "1p"), Type, Week,
OpenPOUnits, Aged90PlusUnits, UnsellableUnits, SellableCost, SnapshotDate
```

`Qty` is `sellableOnHandInventoryUnits` from the Vendor Inventory Report. The "extra" columns (`OpenPOUnits` etc) are ops visibility — the AM_Replenishment service only reads `channel=="1p"` + `Qty`.

## Required env vars in `.env`

```
SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY
SP_API_VENDOR_REFRESH_TOKEN_WHITEMULBERRY
SP_LWA_CLIENT_ID_AUDIOARRAY      SP_LWA_CLIENT_SECRET_AUDIOARRAY
SP_LWA_CLIENT_ID_WHITEMULBERRY   SP_LWA_CLIENT_SECRET_WHITEMULBERRY
```

The LWA Client ID + Secret are the **AdPilot Production** SP-API app credentials. The vendor refresh tokens were issued from AdPilot, so any shared `SP_LWA_CLIENT_ID` (like "AM Replenishment Inbound") will fail with `unauthorized_client`. Always use the per-account override.

## Usage

```bash
python scripts/sp_vendor_soh_pull.py                  # both accounts, last Saturday
python scripts/sp_vendor_soh_pull.py --account AUDIOARRAY
python scripts/sp_vendor_soh_pull.py --account WHITEMULBERRY
python scripts/sp_vendor_soh_pull.py --date 2026-06-13   # force a specific Saturday
```

Default `--date` is the most recent Saturday (with 48h margin). The script writes outputs into the week folder computed from the date.

## Critical quirk: Amazon's data lag

`GET_VENDOR_INVENTORY_REPORT` has a **~3-day publication lag**, not 48h. A Sun/Mon morning request asking for Saturday's data will FATAL with `"The report data for the requested date range is not yet available."`

The script auto-walks back up to 3 days on this error. So a Monday run targeting Saturday will silently fall back to Friday. The output's `SnapshotDate` column shows the actual date Amazon returned.

**To reliably get Saturday's SOH, run on Tuesday or later.** If the weekly cron runs Sun 07:00 IST, expect Friday data on cycles when Amazon hasn't published Saturday yet.

## Scheduling recommendation

Two options:

1. **Single Sun cron** — accept Friday-as-fallback. Simpler, but you lose the Sat→Sun delta.
2. **Separate Tue 07:00 IST cron** — gives reliable Saturday SOH. Recommended for inventory accuracy.

If the existing Sun 07:00 IST cron already runs other things (ads + business_report_derive), adding vendor SOH there is fine for ops continuity — just know SnapshotDate will often be Fri.

## Integration with the manual `Inventory Snapshot.xlsx`

The existing per-brand `Inventory Snapshot.xlsx` (built manually) has 8 channel slices: Amazon, 1p, AMPM, B2B-AMPM, Pipeline, Open Order, YNT, Blinkit. This SP-API script **only produces the 1p slice**.

Decision the operator/weekly agent needs to make:

- **(a) Keep both in parallel** for one or two cycles. Compare `Vendor SOH (SP-API).xlsx` against the manual 1p rows in `Inventory Snapshot.xlsx`. If they reconcile, cut over.
- **(b) Replace** — modify `run_inventory_etl.py` (or the snapshot builder) to consume `Vendor SOH (SP-API).xlsx` instead of the manual 1p slice.

Until cutover, the `Vendor SOH (SP-API).xlsx` files sit beside `Inventory Snapshot.xlsx` without affecting downstream services. The downstream AM_Replenishment app still reads `Inventory_snapshot_<brand>.xlsx` (a different file, in a different repo).

## Known issues

- **Unmatched ASINs**: For Audio Array, ~340 ASINs in the vendor report (holding ~6,300 units) aren't in `sku_master.xlsx`. Those land in `_audit/vendor_soh_unmatched_audioarray.csv`. Triage candidates: legacy ASINs, retired SKUs, or genuine master gaps.
- **WM has almost no data**: The Clicktech vendor account holds ~20 legacy ASINs with ~1 sellable unit total, no current sku_master overlap. Could mean the active WM brand isn't being supplied via Clicktech right now.

## Token attribution (so future debugging is faster)

- LWA app: **AdPilot Production**
- Vendor refresh tokens issued from: AdPilot Production self-authorization in Solution Provider Portal
- Region: EU (`sellingpartnerapi-eu.amazon.com`)
- Marketplace: `A21TJRUUN4KGV` (amazon.in)
- Report type: `GET_VENDOR_INVENTORY_REPORT` (not real-time — the real-time variant only gives `highlyAvailableInventory` per hour, not full sellable SOH)
- Date window cap: 7 days max between `dataStartTime` and `dataEndTime`
