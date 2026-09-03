const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

const ROOT = path.resolve(__dirname, "..");
const DATA = path.join(ROOT, "data");
const OUT = path.join(ROOT, "src", "raw_data.json");

// 3P sales are NET of GST everywhere in this report — the manual-era
// process divided console gross by 1.18 and monthly_seller_sales_pull.py
// does the same, so the whole trendline is ex-GST BY DESIGN (operator,
// 2026-09-03: "3p sales from sp-api should be minus 1.18, when we were
// doing manually we were minusing 1.18"). Do NOT gross these up; the
// console reads ~18% higher than this report and that is expected.
const GST_RATE = 0.18; // used only for the informational RevB2B field basis note

// Brands the report covers.  Fossil deliberately omitted — not a
// buybox-report brand (it is in sku_master.xlsx but not on the dashboard).
const BRANDS = ["Nexlev", "Audio Array", "Tonor", "White Mulberry"];

const MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const MONTH_NUM = MONTH_NAMES.reduce((acc, m, i) => { acc[m] = i + 1; return acc; }, {});

// Bare month folders ("Jan", "Jul") pre-date the multi-year data and are all
// 2026.  Anything added since carries its year, so this never needs bumping.
const LEGACY_YEAR = 2026;

// Periods are DISCOVERED by scanning data/<Brand>/, never hardcoded -- that is
// what makes 2027 a zero-code-change year: drop the folders in and they appear.
//
// Three folder spellings live side by side directly under the brand:
//   "2026-07"  YYYY-MM stamp, written by the monthly API cron
//   "Jul"      bare month, old operator export -> LEGACY_YEAR
//   "Jul25"    month + 2-digit year, manual back-fill -> 2025
// A month can have both a stamp folder and a name folder; the API files win and
// the operator files are the fallback, so we keep BOTH dirs for that period.
function discoverPeriods(brand) {
  const brandDir = path.join(DATA, BRAND_FOLDERS[brand]);
  if (!fs.existsSync(brandDir)) return [];

  const byStamp = new Map();
  const addDir = (year, monthNum, dir) => {
    const stamp = `${year}-${String(monthNum).padStart(2, "0")}`;
    if (!byStamp.has(stamp)) {
      byStamp.set(stamp, {
        stamp,
        year,
        month: MONTH_NAMES[monthNum - 1],
        label: `${MONTH_NAMES[monthNum - 1]} ${year}`,
        dirs: [],
      });
    }
    byStamp.get(stamp).dirs.push(dir);
  };

  for (const entry of fs.readdirSync(brandDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const name = entry.name;
    const dir = path.join(brandDir, name);

    const stamp = name.match(/^(\d{4})-(\d{2})$/);
    if (stamp) {
      addDir(Number(stamp[1]), Number(stamp[2]), dir);
      continue;
    }
    // "Jul25" / "Jul2025"
    const suffixed = name.match(/^([A-Za-z]{3})(\d{2}|\d{4})$/);
    if (suffixed && MONTH_NUM[titleCaseMonth(suffixed[1])]) {
      const raw = Number(suffixed[2]);
      addDir(raw < 100 ? 2000 + raw : raw, MONTH_NUM[titleCaseMonth(suffixed[1])], dir);
      continue;
    }
    if (MONTH_NUM[titleCaseMonth(name)]) {
      addDir(LEGACY_YEAR, MONTH_NUM[titleCaseMonth(name)], dir);
    }
  }

  return [...byStamp.values()].sort((a, b) => a.stamp.localeCompare(b.stamp));
}

// Folder casing drifts between operators ("jul", "Jul", "JUL").
function titleCaseMonth(text) {
  const t = String(text || "");
  return t.charAt(0).toUpperCase() + t.slice(1).toLowerCase();
}

// Legacy operator-exported folder layout (Sp&Sd.xlsx + business_report.xlsx etc.)
//
// Folder names MUST match the Brand strings in data/master/sku_master.xlsx
// exactly, case included.  The three Python pullers derive their output
// folder straight from that Brand column, and GitHub Actions runs them on
// ubuntu-latest where the filesystem is case-sensitive — a mismatch there
// silently writes to a second folder this generator never reads.
const BRAND_FOLDERS = {
  Nexlev: "Nexlev",
  "Audio Array": "Audio Array",
  Tonor: "Tonor",
  "White Mulberry": "White Mulberry",
};

// New API-produced folder layout: data/<Brand>/<YYYY-MM>/ads_*.csv
// Lives alongside the legacy folders.  Writer is monthly_buybox_pull.py
// (the GitHub Actions monthly cron).
const API_BRAND_FOLDERS = {
  Nexlev: "Nexlev",
  "Audio Array": "Audio Array",
  Tonor: "Tonor",
  "White Mulberry": "White Mulberry",
};



function cleanText(value) {
  if (value === null || value === undefined) return "";
  const text = String(value).trim();
  return /^(nan|nat|none)$/i.test(text) ? "" : text;
}

function safeFloat(value) {
  const text = cleanText(value).replace(/[^0-9.\-]/g, "").trim();
  const parsed = Number.parseFloat(text);
  return Number.isFinite(parsed) ? parsed : 0;
}

// Percentages reach us on two different scales depending on the source:
//   operator business_report.xlsx -> 0..1 fraction  (0.3771 = 37.71%)
//   operator export, %-formatted  -> "37.71%"       (safeFloat -> 37.71)
//   SP-API sales_seller.csv       -> 0..100 percent (71.43  = 71.43%)
// The dashboard treats these fields as a 0..1 fraction throughout
// (fmt "pct" renders v * 100), so normalise everything to that here.
// A value of exactly 1 is read as 100%, which is what every source we
// have actually means by it; a literal 1% would be indistinguishable,
// but that is far rarer than a full-buy-box ASIN.
function normalisePct(value) {
  const n = safeFloat(value);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return n > 1 ? n / 100 : n;
}

function col(headers, ...names) {
  return headers.find((header) => names.some((name) => header.toLowerCase().includes(name.toLowerCase())));
}

function splitCsvRow(line) {
  const cells = [];
  let cell = "";
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];
    if (char === "\"") {
      if (inQuotes && next === "\"") {
        cell += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      cells.push(cell);
      cell = "";
    } else {
      cell += char;
    }
  }
  cells.push(cell);
  return cells.map(cleanText);
}

function rowsToObjects(rows, headerIndex = 0) {
  const headers = (rows[headerIndex] || []).map(cleanText);
  return rows.slice(headerIndex + 1).map((row) => {
    const out = {};
    headers.forEach((header, index) => {
      if (header) out[header] = row[index] ?? "";
    });
    return out;
  });
}

function readCsv(filePath, headerIndex = 0) {
  if (!fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, "utf8").replace(/^\uFEFF/, "");
  const rows = raw.split(/\r?\n/).filter(Boolean).map(splitCsvRow);
  return rowsToObjects(rows, headerIndex);
}

function readXlsx(filePath, sheetName = null, headerIndex = 0) {
  if (!fs.existsSync(filePath)) return [];
  const workbook = XLSX.readFile(filePath, { raw: false });
  const target = sheetName && workbook.SheetNames.includes(sheetName) ? sheetName : workbook.SheetNames[0];
  const rows = XLSX.utils.sheet_to_json(workbook.Sheets[target], { header: 1, defval: "" });
  return rowsToObjects(rows, headerIndex);
}

function readFile(filePath, headerIndex = 0) {
  return /\.(xlsx|xls)$/i.test(filePath) ? readXlsx(filePath, null, headerIndex) : readCsv(filePath, headerIndex);
}

function firstExisting(paths) {
  return paths.find((filePath) => fs.existsSync(filePath)) || paths[0];
}

function loadSkuMaster() {
  // Single source of truth = data/master/sku_master.xlsx (the same file
  // monthly_buybox_pull.py / sb_attribution.py read).  Falls back to
  // the legacy data/sku_master.xlsx if the canonical one isn't there.
  const canonical = path.join(DATA, "master", "sku_master.xlsx");
  const legacy    = path.join(DATA, "sku_master.xlsx");
  const masterPath = fs.existsSync(canonical) ? canonical : legacy;
  const rows = readXlsx(masterPath);
  const headers = Object.keys(rows[0] || {});
  const asinCol = col(headers, "child asin", "asin");
  const fbaCol = col(headers, "fba sku", "fba_sku");
  const modelCol = col(headers, "model");
  const nlcCol = col(headers, "nlc", "net landed");
  const brandCol = col(headers, "brand");
  const cat0Col = headers.find((header) => header.trim().toLowerCase() === "category_l0");
  const cat1Col = headers.find((header) => header.trim().toLowerCase() === "category_l1");
  const map = new Map();

  rows.forEach((row) => {
    const asin = cleanText(row[asinCol]).toUpperCase();
    if (!asin) return;
    const mainCat = cleanText(row[cat0Col]);
    const cat1 = cleanText(row[cat1Col]);
    const brand = cleanText(row[brandCol]);
    map.set(asin, {
      fbaSku: cleanText(row[fbaCol]),
      model: cleanText(row[modelCol]),
      category: brand === "Nexlev" ? cat1 || mainCat : mainCat || cat1,
      mainCat,
      // Clean, un-mixed levels straight off the sheet. `category` above is a
      // brand-dependent blend of the two and cannot be split back apart, so
      // the dashboard's L0 / L1 filters read these instead.
      catL0: mainCat,
      catL1: cat1,
      dp: safeFloat(row[nlcCol]) ? Number((safeFloat(row[nlcCol]) / 1.18).toFixed(2)) : 0,
    });
  });

  return map;
}

function readBusiness(filePath) {
  const rows = readFile(filePath);
  const headers = Object.keys(rows[0] || {});
  const asinCol = col(headers, "child) asin", "child asin", "asin");
  const sessCol = col(headers, "sessions - total");
  const unitsCol = col(headers, "units ordered");
  const unitsB2bCol = col(headers, "units ordered - b2b");
  const bbCol = col(headers, "featured offer percentage");
  const revCol = col(headers, "ordered product sales");
  const revB2bCol = col(headers, "ordered product sales - b2b");
  const titleCol = col(headers, "title");
  const map = new Map();

  rows.forEach((row) => {
    const asin = cleanText(row[asinCol]).toUpperCase();
    if (!asin || asin === "(CHILD) ASIN") return;
    const current = map.get(asin) || { sessions: 0, units3p: 0, rev3p: 0, bbPct: 0, title: "" };
    current.sessions += safeFloat(row[sessCol]);
    current.units3p += safeFloat(row[unitsCol]);
    current.rev3p += safeFloat(row[revCol]);
    // B2B recorded separately — NEVER added into the 3P totals (operator
    // rule 2026-09-03: "3p sales is only ordered sales, not adding b2b").
    current.unitsB2b = (current.unitsB2b || 0) + (unitsB2bCol ? safeFloat(row[unitsB2bCol]) : 0);
    current.revB2b = (current.revB2b || 0) + (revB2bCol ? safeFloat(row[revB2bCol]) : 0);
    current.bbPct = Math.max(current.bbPct, normalisePct(row[bbCol]));
    current.title ||= cleanText(row[titleCol]);
    map.set(asin, current);
  });

  return map;
}

// Read API-produced per-ASIN ads CSVs from data/<Brand>/<YYYY-MM>/.
// Pools SP + SD + SB-attributed into a single map per ASIN â€” same
// shape readAds() returns from the operator-exported Sp&Sd.xlsx.
// Returns null when none of the 3 CSVs exist (caller then falls back
// to the legacy xlsx path).
function readAdsApi(dir) {
  if (!dir) return null;
  const candidates = ["ads_sp.csv", "ads_sd.csv", "ads_sb_attributed.csv"]
    .map((name) => path.join(dir, name))
    .filter((p) => fs.existsSync(p));
  if (candidates.length === 0) return null;

  const map = new Map();
  candidates.forEach((csvPath) => {
    const rows = readCsv(csvPath, 0);
    if (!rows.length) return;
    const headers = Object.keys(rows[0]);
    const asinCol  = col(headers, "asin");
    if (!asinCol) return;
    // SP / SD use friendly capitalised names; SB-attributed uses
    // lowercase.  col() does a case-insensitive substring match so
    // these picks cover both schemas with one set of lookups.
    const spendCol  = col(headers, "spend");
    const salesCol  = col(headers, "14 day total sales", "attributed_sales", "sales");
    const imprCol   = col(headers, "impressions");
    const clickCol  = col(headers, "clicks");
    const ordersCol = col(headers, "14 day total units", "ams_orders", "orders");
    const isSbAttr = csvPath.toLowerCase().includes("sb_attributed");
    rows.forEach((row) => {
      let asin = cleanText(row[asinCol]).toUpperCase();
      if (!asin) {
        // SB residue the L0-L4 cascade could pin to a BRAND but not an
        // ASIN (L5_campaign_kw / L6_account rows, added 2026-09-02).
        // Dropping these made every brand's ad totals read LOW vs the
        // ads console; folding them into one visible synthetic row per
        // brand keeps totals truthful without inventing ASIN data.
        if (!isSbAttr) return;
        asin = "SB-UNATTRIBUTED";
      }
      const current = map.get(asin) || { spend: 0, sales: 0, impressions: 0, clicks: 0, orders: 0 };
      current.spend       += safeFloat(row[spendCol]);
      current.sales       += safeFloat(row[salesCol]);
      current.impressions += safeFloat(row[imprCol]);
      current.clicks      += safeFloat(row[clickCol]);
      current.orders      += safeFloat(row[ordersCol]);
      map.set(asin, current);
    });
  });
  return map;
}


// Read the API-produced sales_seller.csv from data/<Brand>/<YYYY-MM>/
// and expose it in the same shape readBusiness() returns (sessions,
// units3p, rev3p, bbPct, title) so build() can treat it as a drop-in
// replacement for the operator's business_report.xlsx.  The CSV
// columns (SKU / (Child) ASIN / Units Ordered / Ordered Product Sales
// / Sessions / Page Views / Buy Box Percentage) mirror the operator's
// amazon_sales.xlsx by design (see monthly_seller_sales_pull.py).
function readBusinessApi(dir) {
  if (!dir) return null;
  const p = path.join(dir, "sales_seller.csv");
  if (!fs.existsSync(p)) return null;
  const rows = readCsv(p, 0);
  if (!rows.length) return null;
  const headers = Object.keys(rows[0]);
  const asinCol  = col(headers, "(child) asin", "child asin", "asin");
  const unitsCol = col(headers, "units ordered");
  const unitsB2bCol = col(headers, "units ordered b2b");
  const revCol   = col(headers, "ordered product sales");
  const revB2bCol = col(headers, "ordered product sales b2b");
  const sessCol  = col(headers, "sessions");
  const bbCol    = col(headers, "buy box percentage", "featured offer percentage");
  const map = new Map();
  rows.forEach((row) => {
    const asin = cleanText(row[asinCol]).toUpperCase();
    if (!asin) return;
    const current = map.get(asin) || { sessions: 0, units3p: 0, rev3p: 0, bbPct: 0, title: "" };
    current.sessions += safeFloat(row[sessCol]);
    current.units3p  += safeFloat(row[unitsCol]);
    current.rev3p    += safeFloat(row[revCol]);
    // B2B recorded separately — never added into the 3P totals.
    current.unitsB2b = (current.unitsB2b || 0) + (unitsB2bCol ? safeFloat(row[unitsB2bCol]) : 0);
    current.revB2b   = (current.revB2b || 0) + (revB2bCol ? safeFloat(row[revB2bCol]) : 0);
    // SP-API reports buyBoxPercentage on a 0..100 scale (71.43 = 71.43%);
    // normalisePct() brings it onto the 0..1 scale the UI expects.
    current.bbPct     = Math.max(current.bbPct, normalisePct(row[bbCol]));
    map.set(asin, current);
  });
  return map;
}

// Read the API-produced sales_vendor.csv from data/<Brand>/<YYYY-MM>/
// and expose it in the same shape readP1() returns.  Column names in
// the API output differ (Qty / Sale) from the operator's 1Psales.csv
// (Ordered Units / Ordered Revenue) — mapping is done here.
function read1PApi(dir) {
  if (!dir) return null;
  const p = path.join(dir, "sales_vendor.csv");
  if (!fs.existsSync(p)) return null;
  const rows = readCsv(p, 0);
  if (!rows.length) return null;
  const headers  = Object.keys(rows[0]);
  const asinCol  = col(headers, "asin");
  // API writes "Sale" / "Qty" (ordered revenue / ordered units under
  // MANUFACTURING view).  Fall back to legacy names for safety.
  const revCol   = col(headers, "sale", "ordered revenue", "revenue");
  const unitsCol = col(headers, "qty", "ordered units", "units");
  const map = new Map();
  rows.forEach((row) => {
    const asin = cleanText(row[asinCol]).toUpperCase();
    if (!asin) return;
    map.set(asin, {
      rev1p:   safeFloat(row[revCol]),
      units1p: safeFloat(row[unitsCol]),
      title:   "",
    });
  });
  return map;
}


function readAds(filePath) {
  const map = new Map();
  if (!fs.existsSync(filePath)) return map;
  const workbook = XLSX.readFile(filePath, { raw: false });
  const combinedRows = workbook.SheetNames.includes("SP_SD_Combined")
    ? rowsToObjects(XLSX.utils.sheet_to_json(workbook.Sheets["SP_SD_Combined"], { header: 1, defval: "" }))
    : [];
  const combinedHeaders = Object.keys(combinedRows[0] || {});
  const combinedAsinCol = col(combinedHeaders, "advertised asin", "asin");
  const combinedHasData = combinedRows.some((row) => cleanText(row[combinedAsinCol]).toUpperCase() !== "");
  const sheets = combinedHasData
    ? ["SP_SD_Combined"]
    : workbook.SheetNames.filter((name) => ["SP", "SD", "SB"].includes(name));

  sheets.forEach((sheetName) => {
    const rows = rowsToObjects(XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { header: 1, defval: "" }));
    const headers = Object.keys(rows[0] || {});
    const asinCol = col(headers, "advertised asin", "asin");
    const spendCol = col(headers, "spend");
    const salesCol = col(headers, "14 day total sales", "7 day total sales", "sales");
    const imprCol = col(headers, "impressions");
    const clickCol = col(headers, "clicks");
    const ordersCol = col(headers, "14 day total units", "7 day total units", "orders");
    rows.forEach((row) => {
      const asin = cleanText(row[asinCol]).toUpperCase();
      if (!asin) return;
      const current = map.get(asin) || { spend: 0, sales: 0, impressions: 0, clicks: 0, orders: 0 };
      current.spend += safeFloat(row[spendCol]);
      current.sales += safeFloat(row[salesCol]);
      current.impressions += safeFloat(row[imprCol]);
      current.clicks += safeFloat(row[clickCol]);
      current.orders += safeFloat(row[ordersCol]);
      map.set(asin, current);
    });
  });

  return map;
}

function readP1(filePath) {
  const rows = readCsv(filePath, 1);
  const headers = Object.keys(rows[0] || {});
  const asinCol = col(headers, "asin");
  const revCol = col(headers, "ordered revenue", "revenue");
  const unitsCol = col(headers, "ordered units", "units");
  const titleCol = col(headers, "product title", "title");
  const map = new Map();
  rows.forEach((row) => {
    const asin = cleanText(row[asinCol]).toUpperCase();
    if (!asin) return;
    map.set(asin, {
      rev1p: safeFloat(row[revCol]),
      units1p: safeFloat(row[unitsCol]),
      // Title fallback for months where the business report hasn't landed yet.
      title: cleanText(row[titleCol]),
    });
  });
  return map;
}

function build() {
  const skuMaster = loadSkuMaster();
  const allRows = [];

  BRANDS.forEach((brand) => {
    discoverPeriods(brand).forEach((period) => {
      const month = period.month;
      // A period can span more than one folder only if the same month is filed
      // under two spellings; pick the first that actually holds each file.
      const inDirs = (names) =>
        firstExisting(period.dirs.flatMap((d) => names.map((n) => path.join(d, n))));

      const bizPath = inDirs(["business_report.csv", "business_report.xlsx"]);
      const adsPath = inDirs(["Sp&Sd.xlsx"]);
      const p1Path = inDirs(["1Psales.csv", "1PSales.csv", "1Psales.xlsx", "1PSales.xlsx"]);
      const apiDir = period.dirs.find((d) =>
        ["sales_seller.csv", "sales_vendor.csv", "ads_sp.csv", "ads_sd.csv", "ads_sb.csv", "ads_sb_attributed.csv"]
          .some((n) => fs.existsSync(path.join(d, n)))
      );

      // Prefer the API-produced CSVs written by the monthly cron.  Falls back
      // to operator-exported xlsx/csv for historical months that pre-date the
      // API automation.  Same pattern for 3P sales (sales_seller.csv
      // ← business_report.xlsx), ads (ads_*.csv ← Sp&Sd.xlsx), and
      // 1P vendor sales (sales_vendor.csv ← 1Psales.csv).
      const apiBiz = readBusinessApi(apiDir);
      const business = apiBiz && apiBiz.size > 0 ? apiBiz : readBusiness(bizPath);
      const apiAds = readAdsApi(apiDir);
      const ads = apiAds && apiAds.size > 0 ? apiAds : readAds(adsPath);
      const adsSource = apiAds && apiAds.size > 0 ? "api" : "xlsx";
      const api1P = read1PApi(apiDir);
      const p1 = api1P && api1P.size > 0 ? api1P : readP1(p1Path);
      const asins = new Set([...business.keys(), ...ads.keys(), ...p1.keys()]);
      let count = 0;

      asins.forEach((asin) => {
        const sku = skuMaster.get(asin) || {};
        const biz = business.get(asin) || {};
        const ad = ads.get(asin) || {};
        const p = p1.get(asin) || {};
        const adSpend = ad.spend || 0;
        const adSales = ad.sales || 0;
        const rev3p = biz.rev3p || 0; // net of GST by design — see GST note at top
        const rev1p = p.rev1p || 0;
        const units3p = biz.units3p || 0;
        const units1p = p.units1p || 0;
        const netSales = rev3p + rev1p;
        const sessions = biz.sessions || 0;
        const amsOrders = ad.orders || 0;
        const organicSales = Math.max(0, netSales - adSales);

        allRows.push({
          Brand: brand,
          ASIN: asin,
          Title: biz.title || p.title || "",
          // Month carries the year ("Jul 2026") so Jul 2025 and Jul 2026 are
          // distinct values everywhere the dashboard groups or compares by
          // month -- there is no bare "Jul" left to collide.
          Month: period.label,
          MonthShort: month,
          Year: period.year,
          // "2026-07" -- sorts chronologically as a plain string.
          Period: period.stamp,
          fbaSku: sku.fbaSku || "",
          model: sku.model || "",
          category: sku.category || "",
          mainCat: sku.mainCat || "",
          catL0: sku.catL0 || "",
          catL1: sku.catL1 || "",
          DP: sku.dp || 0,
          Sessions: sessions,
          BuyboxPct: biz.bbPct || 0,
          NetUnits: units3p + units1p,
          Units1P: units1p,
          Units3P: units3p,
          UnitsB2B: biz.unitsB2b || 0,
          RevB2B: Number((biz.revB2b || 0).toFixed(2)),  // net basis, same as Rev3P
          TotalNetSalesValue: Number(netSales.toFixed(2)),
          Rev1P: Number(rev1p.toFixed(2)),
          Rev3P: Number(rev3p.toFixed(2)),
          TotalAdsSpend: Number(adSpend.toFixed(2)),
          TotalAdsSales: Number(adSales.toFixed(2)),
          Impressions: ad.impressions || 0,
          Clicks: ad.clicks || 0,
          AmsOrders: amsOrders,
          ACOS: adSales > 0 ? Number((adSpend / adSales).toFixed(4)) : 0,
          TACOS: netSales > 0 ? Number((adSpend / netSales).toFixed(4)) : 0,
          CAC: amsOrders > 0 ? Number((adSpend / amsOrders).toFixed(2)) : 0,
          ConversionPct: sessions > 0 ? Number(((units3p + units1p) / sessions).toFixed(4)) : 0,
          OrganiSales: Number(organicSales.toFixed(2)),
          OrganicPct: netSales > 0 ? Number((organicSales / netSales).toFixed(4)) : 0,
        });
        count += 1;
      });

      console.log(`${brand} ${month}: ${count} ASINs  (ads source: ${adsSource})`);
    });
  });

  // Title backfill.  The SP-API sales_seller.csv carries no product title
  // (GET_SALES_AND_TRAFFIC_REPORT doesn't return one), so every month
  // sourced from the API would otherwise render with a blank product name.
  // Reuse the title we already know for that ASIN from any other month.
  const titleByAsin = new Map();
  allRows.forEach((row) => {
    if (row.Title && !titleByAsin.has(row.ASIN)) titleByAsin.set(row.ASIN, row.Title);
  });
  let backfilled = 0;
  let stillBlank = 0;
  allRows.forEach((row) => {
    if (row.Title) return;
    const known = titleByAsin.get(row.ASIN);
    if (known) {
      row.Title = known;
      backfilled += 1;
    } else {
      stillBlank += 1;
    }
  });
  console.log(`Titles: ${backfilled} backfilled from other months, ${stillBlank} still blank (ASIN seen in no month with a title)`);

  // Guard the buy-box scale regression that shipped in July 2026: the two
  // readers disagreed about whether the source was a fraction or a percent,
  // so Jan-Jun rendered 100x too small and Jul 100x too big.
  const outOfRange = allRows.filter((row) => row.BuyboxPct > 1).length;
  if (outOfRange > 0) {
    console.error(`WARNING: ${outOfRange} rows have BuyboxPct > 1 — a source is emitting percents that normalisePct() did not catch.`);
  }

  fs.writeFileSync(OUT, JSON.stringify(allRows, null, 2));
  console.log(`Written ${allRows.length} rows to ${OUT}`);
}

build();

