/*
 * Build src/june_diff.json — per-brand aggregates for June 2026, first-cron
 * pull (2026-07-03) vs latest refresh (working-tree raw_data.json).
 *
 * The "Refresh Diff" tab in the dashboard reads this to show operators
 * what Amazon actually changed between the initial cron and the follow-up
 * refresh (cancellations, returns, matured SP attribution, SB re-attribution).
 *
 * Data sources
 *   Jul-3 snapshot: git show 79fe813:src/raw_data.json
 *                   (2026-07-04 rupee-parse fix commit, so 1P/3P rupees
 *                    are trustworthy; underlying data is Jul-3 pull)
 *   Latest:         src/raw_data.json in the working tree — must be
 *                   rebuilt before this script runs (npm run build does
 *                   the correct order: generate_raw_data → this).
 *
 * Extend for future months by looping `MONTHS_TO_DIFF` and adding one
 * entry per (month, first-cron-commit) pair.
 */
const fs   = require("fs");
const path = require("path");

const REPO = path.resolve(__dirname, "..");

// month key -> static snapshot file capturing the first-cron aggregate.
// Files live under src/snapshots/ so the build works in Render's shallow
// clone (git history is not available in CI).  Regenerate the snapshot by
// running: `git show <ref>:src/raw_data.json > src/snapshots/<slug>.json`
const MONTH_SNAPSHOTS = {
  Jun: {
    firstPullFile:  "src/snapshots/jul3_raw_data.json",
    firstPullLabel: "First cron · Jul 3",
    firstPullDate:  "2026-07-03",
  },
};

const METRICS = [
  { key: "units_3p", label: "Units Ordered (3P)",  from: "Units3P",         fmt: "int",   note: "Δ = post-close cancellations / returns" },
  { key: "rev_3p",   label: "Sales · 3P (₹)",       from: "Rev3P",           fmt: "money", note: "Δ = order-value adjustments after month close" },
  { key: "units_1p", label: "Units Ordered (1P)",  from: "Units1P",         fmt: "int",   note: "AA + Tonor only via API; Nexlev/WM 1P sources vary" },
  { key: "rev_1p",   label: "Sales · 1P (₹)",       from: "Rev1P",           fmt: "money", note: "AA + Tonor only via API; Nexlev/WM 1P sources vary" },
  { key: "spend",    label: "Ad Spend (₹)",         from: "TotalAdsSpend",   fmt: "money", note: "Locked at month-end; drift = report consolidation" },
  { key: "impr",     label: "Impressions",         from: "Impressions",     fmt: "int",   note: "Drift = SD/SB report consolidation" },
  { key: "clicks",   label: "Clicks",              from: "Clicks",          fmt: "int",   note: "Drift = SD/SB report consolidation" },
  { key: "ads_sales",label: "Ads Attributed Sales (₹)", from: "TotalAdsSales", fmt: "money", note: "Δ = 14-day SP attribution maturing after month close" },
  { key: "orders",   label: "AMS Orders",          from: "AmsOrders",       fmt: "int",   note: "Δ = attributed orders after month close" },
];

function loadJsonFromDisk(relPath) {
  return JSON.parse(fs.readFileSync(path.join(REPO, relPath), "utf8"));
}

function aggregateByBrand(rows, monthKey) {
  const byBrand = {};
  for (const row of rows) {
    if (row.Month !== monthKey) continue;
    const b = row.Brand || "Unknown";
    if (!byBrand[b]) {
      byBrand[b] = { asinCount: 0 };
      for (const m of METRICS) byBrand[b][m.key] = 0;
    }
    byBrand[b].asinCount += 1;
    for (const m of METRICS) {
      const v = Number(row[m.from]);
      if (Number.isFinite(v)) byBrand[b][m.key] += v;
    }
  }
  return byBrand;
}

function round(v, decimals = 2) {
  if (!Number.isFinite(v)) return 0;
  const mult = Math.pow(10, decimals);
  return Math.round(v * mult) / mult;
}

function buildDiff(monthKey) {
  const snap = MONTH_SNAPSHOTS[monthKey];
  if (!snap) throw new Error(`No snapshot config for month ${monthKey}`);

  const oldRows = loadJsonFromDisk(snap.firstPullFile);
  const newRows = loadJsonFromDisk("src/raw_data.json");

  const oldAgg = aggregateByBrand(oldRows, monthKey);
  const newAgg = aggregateByBrand(newRows, monthKey);

  const allBrands = Array.from(new Set([...Object.keys(oldAgg), ...Object.keys(newAgg)])).sort();

  const brands = {};
  for (const brand of allBrands) {
    const o = oldAgg[brand] || {};
    const n = newAgg[brand] || {};
    const brandRow = {
      brand,
      asinCount: { first: o.asinCount || 0, latest: n.asinCount || 0 },
      metrics: [],
    };
    for (const m of METRICS) {
      const first = round(o[m.key] || 0);
      const latest = round(n[m.key] || 0);
      const delta = round(latest - first);
      const pct = first !== 0 ? round((delta / first) * 100, 2) : null;
      brandRow.metrics.push({
        key: m.key,
        label: m.label,
        fmt: m.fmt,
        note: m.note,
        first,
        latest,
        delta,
        pct,
      });
    }
    brands[brand] = brandRow;
  }

  return {
    month: monthKey,
    firstPull: {
      ref: snap.firstPullRef,
      label: snap.firstPullLabel,
      date: snap.firstPullDate,
    },
    latest: {
      label: `Latest refresh · ${new Date().toISOString().slice(0, 10)}`,
      date: new Date().toISOString().slice(0, 10),
    },
    metrics: METRICS.map(({ key, label, fmt, note }) => ({ key, label, fmt, note })),
    brands,
    generatedAt: new Date().toISOString(),
  };
}

function main() {
  const out = { months: {} };
  for (const monthKey of Object.keys(MONTH_SNAPSHOTS)) {
    out.months[monthKey] = buildDiff(monthKey);
    const b = out.months[monthKey].brands;
    const brandList = Object.keys(b).join(", ");
    console.log(`June diff: brands=[${brandList}]`);
  }
  const outPath = path.join(REPO, "src", "june_diff.json");
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log(`Written ${outPath}`);
}

main();
