const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

const ROOT = path.resolve(__dirname, "..");
const DATA = path.join(ROOT, "data");
const OUT = path.join(ROOT, "src", "raw_data.json");

const CONFIGS = {
  Nexlev: ["Jan", "Feb", "Mar", "Apr", "May"],
  "Audio Array": ["Jan", "Feb", "Mar", "Apr", "May"],
  Tonor: ["Jan", "Feb", "Mar", "Apr", "May"],
  "White Mulberry": ["Jan", "Feb", "Mar", "Apr", "May"],
};

const BRAND_FOLDERS = {
  Nexlev: "Nexlev",
  "Audio Array": "Audio array",
  Tonor: "Tonor",
  "White Mulberry": "White Mulberry",
};

function cleanText(value) {
  if (value === null || value === undefined) return "";
  const text = String(value).trim();
  return /^(nan|nat|none)$/i.test(text) ? "" : text;
}

function safeFloat(value) {
  const text = cleanText(value).replace(/[₹$,%",]/g, "").replace(/[()]/g, "").trim();
  const parsed = Number.parseFloat(text);
  return Number.isFinite(parsed) ? parsed : 0;
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
  const rows = readXlsx(path.join(DATA, "sku_master.xlsx"));
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
  const bbCol = col(headers, "featured offer percentage");
  const revCol = col(headers, "ordered product sales");
  const titleCol = col(headers, "title");
  const map = new Map();

  rows.forEach((row) => {
    const asin = cleanText(row[asinCol]).toUpperCase();
    if (!asin || asin === "(CHILD) ASIN") return;
    const current = map.get(asin) || { sessions: 0, units3p: 0, rev3p: 0, bbPct: 0, title: "" };
    current.sessions += safeFloat(row[sessCol]);
    current.units3p += safeFloat(row[unitsCol]);
    current.rev3p += safeFloat(row[revCol]);
    current.bbPct = Math.max(current.bbPct, safeFloat(row[bbCol]) / 100);
    current.title ||= cleanText(row[titleCol]);
    map.set(asin, current);
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
  const map = new Map();
  rows.forEach((row) => {
    const asin = cleanText(row[asinCol]).toUpperCase();
    if (!asin) return;
    map.set(asin, { rev1p: safeFloat(row[revCol]), units1p: safeFloat(row[unitsCol]) });
  });
  return map;
}

function build() {
  const skuMaster = loadSkuMaster();
  const allRows = [];

  Object.entries(CONFIGS).forEach(([brand, months]) => {
    months.forEach((month) => {
      const folder = path.join(DATA, BRAND_FOLDERS[brand], month);
      const bizPath = firstExisting([
        path.join(folder, "business_report.csv"),
        path.join(folder, "business_report.xlsx"),
      ]);
      const adsPath = path.join(folder, "Sp&Sd.xlsx");
      const p1Path = firstExisting([
        path.join(folder, "1Psales.csv"),
        path.join(folder, "1PSales.csv"),
      ]);

      const business = readBusiness(bizPath);
      const ads = readAds(adsPath);
      const p1 = readP1(p1Path);
      const asins = new Set([...business.keys(), ...ads.keys(), ...p1.keys()]);
      let count = 0;

      asins.forEach((asin) => {
        const sku = skuMaster.get(asin) || {};
        const biz = business.get(asin) || {};
        const ad = ads.get(asin) || {};
        const p = p1.get(asin) || {};
        const adSpend = ad.spend || 0;
        const adSales = ad.sales || 0;
        const rev3p = biz.rev3p || 0;
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
          Title: biz.title || "",
          Month: month,
          fbaSku: sku.fbaSku || "",
          model: sku.model || "",
          category: sku.category || "",
          mainCat: sku.mainCat || "",
          DP: sku.dp || 0,
          Sessions: sessions,
          BuyboxPct: biz.bbPct || 0,
          NetUnits: units3p + units1p,
          Units1P: units1p,
          Units3P: units3p,
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

      console.log(`${brand} ${month}: ${count} ASINs`);
    });
  });

  fs.writeFileSync(OUT, JSON.stringify(allRows, null, 2));
  console.log(`Written ${allRows.length} rows to ${OUT}`);
}

build();
