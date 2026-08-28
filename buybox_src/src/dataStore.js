// Live-binding holders for the dashboard datasets.
//
// nexlev_ads_dashboard.jsx used to `import` these JSON files directly, which
// baked ~16 MB of data (and every ASIN) into the public bundle. They are now
// decrypted at login and pushed in here instead.
//
// Load order matters: App.jsx only dynamically imports the dashboard AFTER
// setDatasets() has run, so the dashboard's module-scope constants (the
// ASIN->title lookup, the BSR snapshot table) see fully populated data.

export let RAW_DATA = [];
export let BSR_RAW = [];
export let BSR_SNAPSHOTS = [];
export let JUNE_DIFF = [];
// ASIN -> { fbaSku, model, category, mainCat, dp, nlc }. Holds cost prices, so
// it is encrypted alongside the datasets rather than inlined in the dashboard.
export let ASIN_MASTER = {};

export function setDatasets(payload) {
  RAW_DATA = payload.raw_data ?? [];
  BSR_RAW = payload.bsr_data ?? [];
  BSR_SNAPSHOTS = payload.bsr_snapshots ?? [];
  JUNE_DIFF = payload.june_diff ?? [];
  ASIN_MASTER = payload.asin_master ?? {};
}

export function clearDatasets() {
  RAW_DATA = [];
  BSR_RAW = [];
  BSR_SNAPSHOTS = [];
  JUNE_DIFF = [];
  ASIN_MASTER = {};
}
