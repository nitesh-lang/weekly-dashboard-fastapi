# Weekly Data Landing Checklist

Run after every weekly ETL refresh, **before** declaring the dashboard
ready for the operator. Mirrors what `scripts/audit_week_landing.py`
checks automatically — keep this in sync with that script.

## 0. Pre-flight

- [ ] All raw uploads in place
  - [ ] `data/raw/sales/Week N/<brand>/amazon_sales.xlsx`
  - [ ] `data/raw/sales/Week N/<brand>/other_channels.xlsx`
  - [ ] `data/raw/inventory/Week N/<brand>/Inventory Snapshot.xlsx`
  - [ ] `data/ams_weekly_data/<brand>/ads_report_weekN.xlsx`
  - [ ] `data/ams_weekly_data/<brand>/business_report_weekN.xlsx`
  - [ ] PO file at `data/raw/inbound/In_Transit_PO*.xlsx`
- [ ] No Excel lock files (`~$*`) — close every workbook before ETL
- [ ] `data/master/sku_master.xlsx` up to date for the new week

## 1. Run the ETL chain

```bash
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.business_ads_weekly_etl
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.step3_ads_aggregation
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.step4_join_business_ads
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.step5_add_category_mapping
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.inventory_model_snapshot
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.inventory_snapshot_etl
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.sales_auto_etl
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.inbound_snapshot
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.margin_snapshot
PYTHONIOENCODING=utf-8 python -m weekly_app.etl.returns_snapshot
```

- [ ] All 10 ETLs exit 0
- [ ] No `KeyError` / `FutureWarning` that blocks output

## 2. Run inventory anomaly audit

```bash
PYTHONIOENCODING=utf-8 python scripts/audit_week21_inventory.py
```
*(rename the week constant inside or copy the script for the new week)*

- [ ] Cross-brand SKU / ASIN / Model collisions: **0**
- [ ] Brand mismatches (file vs master): **0**
- [ ] Orphan SKUs (not in master): triage every entry
- [ ] Orphan ASINs (not in master): triage every entry
- [ ] SKU↔ASIN drift: **0** (a non-zero count is a real data corruption)
- [ ] SKU↔Model drift: triage (often case-only, fixable with the casing script)
- [ ] Within-file multi-Model: **0** (run `fix_inventory_model_casing.py`)
- [ ] Whitespace anomalies: **0** (server trims but source should be clean)

## 3. Run week-landing audit

```bash
PYTHONIOENCODING=utf-8 python scripts/audit_week_landing.py
```

Output: `data/processed/week_landing_audit.xlsx`

### 3.1 Freshness (Sheet 1)
- [ ] `weekly_sales_snapshot` latest week = **W{N}**
- [ ] `inventory_model_snapshot` latest week = **W{N}**
- [ ] `inventory_ams_snapshot` latest week = **W{N}**

### 3.2 Sales drops to 0 (Sheet 2)
- [ ] Each model is explainable (out of stock / discontinued / source file missing)
- [ ] **No surprise zeros** for top-revenue models

### 3.3 Sales WoW swings ≥ ±50% (Sheet 3)
- [ ] Each swing explainable (seasonality, promo, stock-out)
- [ ] Cross-check the top 5 against the source sales file

### 3.4 Inventory drops to 0 by type (Sheet 4)
- [ ] No "warehouse" (AMPM) model dropped due to data missing — verify via raw file
- [ ] Marketplace drops match operator's known FBA depletions
- [ ] 1P drops match Vendor Central state

### 3.5 AMPM brand swings (Sheet 5)
- [ ] Every brand within ±15% WoW — beyond that, suspect raw-file truncation
- [ ] Total AMPM units delta < 10% portfolio-wide

### 3.6 Master coverage (Sheets 6a, 6b)
- [ ] SKUs unsold in last 12 weeks → flag for retire / archive / restock
- [ ] ASINs absent from returns + inbound + margin → flag for cleanup

### 3.7 Cross-snapshot orphans (Sheets 7a, 7b)
- [ ] Add each orphan ASIN to master, OR remove from snapshot source
- [ ] Add each orphan SKU to master

### 3.8 Casing dupes (Sheet 8)
- [ ] **0** within-snapshot casing variants (run `fix_inventory_model_casing.py`
      if any appear)

## 4. Server-side sanity (only if anomalies suspected)

- [ ] `df_cache.py` cache hits look reasonable in logs
- [ ] `/api/health` returns 200
- [ ] Open `/inventory` — Week 21 stock matches operator's expectation
- [ ] Open `/sales-trend` — totals close to ETL totals printed to console
- [ ] Open `/ams-trend` — last 4 weeks default loads, picker shows all weeks

## 5. Commit + push

```bash
git add data/master data/raw/sales data/raw/inventory \
        data/ams_weekly_data data/processed/*.csv
git commit -m "data: Week N refresh"
git push origin main
```

- [ ] Render deploy triggers
- [ ] Production `/health` 200
- [ ] Production `/api/ams/trend` returns Week N in `default_weeks`

## 6. Post-deploy

- [ ] Operator notified (Week N is live)
- [ ] If any audit category showed > 0, write a follow-up note for next week
