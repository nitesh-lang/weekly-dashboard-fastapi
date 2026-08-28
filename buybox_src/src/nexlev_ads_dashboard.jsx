import React, { useState, useMemo, useCallback, useTransition, useRef, useEffect, useDeferredValue } from "react";
// Datasets arrive decrypted at login rather than baked into the bundle -- see
// src/dataStore.js. This module is only imported once they are populated.
import { RAW_DATA, BSR_RAW, BSR_SNAPSHOTS, JUNE_DIFF, ASIN_MASTER } from "./dataStore";

// Months are derived from the data, never hardcoded, so a new year needs no
// code change.  Values are year-qualified ("Jul 2026") and ordered oldest ->
// newest by the row's Period stamp ("2026-07"), which sorts as a plain string.
const MONTH_SEQUENCE = (() => {
  const byLabel = new Map();
  RAW_DATA.forEach((row) => {
    if (row.Month && !byLabel.has(row.Month)) byLabel.set(row.Month, row.Period || row.Month);
  });
  return [...byLabel.entries()]
    .sort((a, b) => String(a[1]).localeCompare(String(b[1])))
    .map(([label]) => label);
})();

const MONTH_INDEX = MONTH_SEQUENCE.reduce((acc, month, index) => {
  acc[month] = index;
  return acc;
}, {});

// "Jul 2026" -> 2026.  Used to group the Month dropdown under year headers.
function monthYearOf(label) {
  const m = String(label).match(/(\d{4})$/);
  return m ? m[1] : "";
}

// ── ASIN Master: FBA SKU · Model · Category · DP · NLC ──────────────────────
// Moved into the encrypted payload (src/asin_master.json): it carries dealer
// price and net landed cost, which must not sit in a publicly fetchable chunk.

function splitCsvRow(line) {
  const cells = [];
  let cell = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      cells.push(cell);
      cell = "";
      continue;
    }

    cell += char;
  }

  cells.push(cell);
  return cells.map((value) => value.trim());
}

function sortMonths(months) {
  return [...months].sort((a, b) => (MONTH_INDEX[a] ?? 999) - (MONTH_INDEX[b] ?? 999));
}

function parseCsvNumber(value) {
  if (value === null || value === undefined || value === "") return 0;
  const cleaned = String(value).replace(/[₹,%"]/g, "").replace(/,/g, "").trim();
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseCsvPercent(value) {
  if (!value) return 0;
  return parseCsvNumber(value) / 100;
}

function inferAudioCategory(title = "") {
  const text = title.toLowerCase();

  if (text.includes("headphone")) return { category: "Headphones", mainCat: "Audio Equipment" };
  if (text.includes("sound bar") || text.includes("soundbar") || text.includes("speaker")) return { category: "Speakers", mainCat: "Audio Equipment" };
  if (text.includes("microphone") || text.includes("mic ")) return { category: "Microphones", mainCat: "Audio Equipment" };
  if (text.includes("mixer") || text.includes("interface") || text.includes("soundcard")) return { category: "Mixers & Interfaces", mainCat: "Audio Equipment" };
  if (text.includes("bundle")) return { category: "Studio Bundles", mainCat: "Audio Equipment" };
  if (text.includes("cable") || text.includes("stand") || text.includes("adapter")) return { category: "Audio Accessories", mainCat: "Audio Equipment" };

  return { category: "Audio Gear", mainCat: "Audio Equipment" };
}

function inferAudioModel(title = "") {
  const match = title.match(/\b(?:AH|AM|AI|AA|AC|SB|PB)-[A-Z0-9-]+\b/i);
  return match ? match[0].toUpperCase() : "AA";
}

function parseAudioArrayCsv(raw, month) {
  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];

  const headers = splitCsvRow(lines[0]);
  const headerIndex = Object.fromEntries(headers.map((header, index) => [header, index]));

  return lines.slice(1).map((line) => {
    const values = splitCsvRow(line);
    const title = values[headerIndex.Title] || "Audio Array Product";
    const categoryMeta = inferAudioCategory(title);
    const units = parseCsvNumber(values[headerIndex["Units Ordered"]]);
    const orders = parseCsvNumber(values[headerIndex["Total Order Items"]]);

    return {
      ASIN: values[headerIndex["(Parent) ASIN"]] || values[headerIndex["(Child) ASIN"]],
      Title: title,
      Sessions: parseCsvNumber(values[headerIndex["Sessions - Total"]]),
      BuyboxPct: parseCsvPercent(values[headerIndex["Featured Offer Percentage"]]),
      NetUnits: units,
      TotalNetSalesValue: parseCsvNumber(values[headerIndex["Ordered Product Sales"]]),
      Impressions: 0,
      Clicks: 0,
      TotalAdsSpend: 0,
      TotalAdsSales: 0,
      AmsOrders: 0,
      Brand: "Audio Array",
      Month: month,
      OrganiSales: orders || units,
      ACOS: 0,
      TACOS: 0,
      CAC: 0,
      ConversionPct: parseCsvPercent(values[headerIndex["Unit Session Percentage"]]),
      AttributedPct: 0,
      OrganicPct: 1,
      fbaSku: "—",
      model: inferAudioModel(title),
      category: categoryMeta.category,
      mainCat: categoryMeta.mainCat,
      dp: null,
      nlc: null,
    };
  }).filter((row) => row.ASIN);
}

const AUDIO_ARRAY_MASTER = Object.fromEntries(
  RAW_DATA
    .filter((row) => row.Brand === "Audio Array" && typeof row.Title === "string" && row.Title.trim())
    .map((row) => {
      const categoryMeta = inferAudioCategory(row.Title);
      return [
        row.ASIN,
        {
          fbaSku: row.fbaSku || "—",
          model: row.model || inferAudioModel(row.Title),
          category: row.category || categoryMeta.category,
          mainCat: row.mainCat || categoryMeta.mainCat,
          dp: row.dp ?? null,
          nlc: row.nlc ?? null,
        },
      ];
    })
);

const MASTER_DATA = { ...ASIN_MASTER, ...AUDIO_ARRAY_MASTER };
let ACTIVE_MASTER_DATA = {};

const THEME = {
  pageBg: "#E5E7EB",
  shellBg: "#F3F4F6",
  panelBg: "#F8FAFC",
  panelBgAlt: "#EEF2F7",
  surface: "#FFFFFF",
  surfaceMuted: "#F3F4F6",
  surfaceSoft: "#E5E7EB",
  surfaceTint: "#EDF2F7",
  border: "#CBD5E1",
  borderStrong: "#94A3B8",
  text: "#111111",
  textSoft: "#334155",
  textMuted: "#64748B",
  textFaint: "#94A3B8",
  headerBg: "#D1D5DB",
  headerBgActive: "#C4CAD3",
  headerText: "#111111",
  headerShadow: "0 6px 16px rgba(15,23,42,0.10)",
  stickyShadow: "12px 0 18px -18px rgba(15,23,42,0.16)",
  hover: "#E5E7EB",
};

// ── Column definitions ─────────────────────────────────────────────────────
const COLS = [
  { key:"ASIN",            label:"ASIN",           fmt:"str",  w:132 },
  { key:"fbaSku",          label:"FBA SKU",        fmt:"str",  w:90,  master:true },
  { key:"model",           label:"Model",          fmt:"str",  w:72,  master:true },
  { key:"catL0",           label:"Category L0",    fmt:"str",  w:130, master:true },
  { key:"catL1",           label:"Category L1",    fmt:"str",  w:150, master:true },
  // Product column removed
  { key:"Sessions",        label:"Sessions",       fmt:"num",  w:80  },
  { key:"BuyboxPct",       label:"Buybox%",        fmt:"pct",  w:78  },
  { key:"NetUnits",        label:"Net Units",      fmt:"num",  w:72  },
  { key:"TotalNetSalesValue",label:"Net Sales",    fmt:"inr",  w:100 },
  { key:"salesContribPct", label:"Sales %",        fmt:"pct2", w:80  },
  { key:"dp",              label:"DP ₹",           fmt:"inr2", w:80,  master:true },
  { key:"Impressions",     label:"Impressions",    fmt:"num",  w:92  },
  { key:"Clicks",          label:"Clicks",         fmt:"num",  w:62  },
  { key:"AmsOrders",       label:"AMS Orders",     fmt:"num",  w:85  },
  { key:"OrganiSales",     label:"Organic Sales",  fmt:"inr",  w:95  },
  { key:"TotalAdsSpend",   label:"Ad Spend",      fmt:"inr",  w:90  },
  { key:"TotalAdsSales",   label:"Ad Sales",      fmt:"inr",  w:90  },
  { key:"ACOS",            label:"ACOS",           fmt:"pct",  w:72  },
  { key:"TACOS",           label:"TACOS",          fmt:"pct",  w:72  },
  { key:"CAC",             label:"CAC ₹",          fmt:"inr",  w:80  },
  { key:"ConversionPct",   label:"CVR%",           fmt:"pct",  w:76  },
  { key:"OrganicPct",      label:"Organic%",       fmt:"pct",  w:84  },
];

const STICKY_COLUMN_COUNT = 4;
const STICKY_LEFT_OFFSETS = COLS.slice(0, STICKY_COLUMN_COUNT).reduce((acc, col, index) => {
  acc[col.key] = COLS.slice(0, index).reduce((sum, current) => sum + current.w, 0);
  return acc;
}, {});

const SMART_VIEW_TABS = [
  {
    key: "low-buybox",
    label: "Low Buybox",
    description: "Buybox under 70%",
    matches: (row) => (row.BuyboxPct ?? 0) > 0 && (row.BuyboxPct ?? 0) < 0.7,
  },
  {
    key: "high-acos",
    label: "High ACOS",
    description: "ACOS above 35%",
    matches: (row) => (row.TotalAdsSales ?? 0) > 0 && (row.ACOS ?? 0) > 0.35,
  },
  {
    key: "high-spend-low-sales",
    label: "High Spend Low Sales",
    description: "High spend with weak ad return",
    matches: (row) => (row.TotalAdsSpend ?? 0) >= 5000 && (row.TotalAdsSales ?? 0) <= (row.TotalAdsSpend ?? 0) * 1.5,
  },
  {
    key: "high-sessions-low-units",
    label: "High Sessions Low Units",
    description: "Traffic high, units low",
    matches: (row) => (row.Sessions ?? 0) >= 100 && (row.NetUnits ?? 0) <= 3,
  },
  {
    key: "zero-ams-orders",
    label: "Zero AMS Orders",
    description: "No AMS orders",
    matches: (row) => (row.AmsOrders ?? 0) === 0,
  },
];

function inr(n) {
  if (!n) return "₹0";
  if (n >= 1e7) return "₹" + (n/1e7).toFixed(2) + "Cr";
  if (n >= 1e5) return "₹" + (n/1e5).toFixed(1) + "L";
  if (n >= 1e3) return "₹" + (n/1e3).toFixed(1) + "K";
  return "₹" + n.toLocaleString("en-IN", { maximumFractionDigits: 0 });
}

function getMasterValue(row, key) {
  // ASIN_MASTER above is a frozen hand-written snapshot from before
  // data/master/sku_master.xlsx became the single source of truth.  It used
  // to take precedence over the live row, so for its 48 Nexlev ASINs the
  // dashboard rendered the snapshot and ignored the master: every one of
  // them showed a stale DP (B0DVC9PRSD read Rs 4,250.85 against the
  // master's Rs 8,608.47), 36 showed a stale FBA SKU, 4 a stale Model.
  // Editing sku_master.xlsx changed nothing on screen for those ASINs.
  //
  // The live row wins now.  generate_raw_data.js already resolves fbaSku,
  // model, category, mainCat and DP straight out of sku_master.xlsx, so the
  // snapshot is only a fallback for fields the live row doesn't carry.
  const master = ACTIVE_MASTER_DATA[row.ASIN] ?? {};
  const pick = (...values) =>
    values.find((v) => v !== undefined && v !== null && v !== "") ?? null;

  if (key === "fbaSku") {
    return pick(row.fbaSku, master.fbaSku, row.ASIN);
  }

  if (key === "model") {
    return pick(row.model, master.model);
  }

  if (key === "catL0" || key === "catL1") {
    // Levels come only from sku_master.xlsx -- never from the old blended
    // `category` field, and never from a title guess.
    return pick(row[key], master[key]);
  }

  if (key === "dp" || key === "nlc") {
    // generate_raw_data.js emits DP capitalised; the snapshot uses lowercase.
    return pick(row[key], row[key.toUpperCase()], master[key]);
  }

  return pick(row[key], master[key]);
}

// category_l0 / category_l1 exactly as they sit in sku_master.xlsx, kept
// apart instead of blended.  `catL0` / `catL1` come from generate_raw_data.js;
// `mainCat` / `category` are the older mixed fields and serve only as a
// fallback for ASINs with no master row, whose values inferAudioCategory()
// guessed from the title.
function categoryLevels(row, masterData = ACTIVE_MASTER_DATA) {
  const master = masterData[row.ASIN] ?? {};
  const clean = (v) => (v && v !== "nan") ? String(v).trim() : "";
  return {
    l0: clean(row.catL0) || clean(master.catL0),
    l1: clean(row.catL1) || clean(master.catL1),
  };
}
const getCategoryL0 = (row, masterData) => categoryLevels(row, masterData).l0;
const getCategoryL1 = (row, masterData) => categoryLevels(row, masterData).l1;

function fmtCell(row, col) {
  let v = col.master ? getMasterValue(row, col.key) : row[col.key];
  if (v === null || v === undefined || v === 0 || v === "") {
    if (col.fmt === "str" || col.fmt === "title") return v || "—";
    return null;
  }
  switch (col.fmt) {
    case "num":   return Math.round(v).toLocaleString("en-IN");
    case "inr":   return "₹" + v.toLocaleString("en-IN", { maximumFractionDigits: 0 });
    case "inr2":  return "₹" + v.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    case "pct":   return (v * 100).toFixed(1) + "%";
    case "pct2":  return (v * 100).toFixed(2) + "%";
    case "title": return typeof v === "string" && v.length > 45 ? v.slice(0, 45) + "…" : (v || "—");
    default: return v;
  }
}

function pctColor(key, v) {
  if (!v) return "#374151";
  if (key === "BuyboxPct")     return v >= 0.9 ? "#34D399" : v >= 0.6 ? "#FBBF24" : "#F87171";
  if (key === "ACOS")          return v < 0.15 ? "#34D399" : v < 0.30 ? "#FBBF24" : "#F87171";
  if (key === "TACOS")         return v < 0.10 ? "#34D399" : v < 0.20 ? "#FBBF24" : "#F87171";
  if (key === "ConversionPct") return v > 0.05 ? "#34D399" : v > 0.02 ? "#FBBF24" : "#F87171";
  if (key === "OrganicPct")    return v > 0.6  ? "#34D399" : v > 0.3  ? "#FBBF24" : "#F87171";
  return "#60A5FA";
}

function compareValues(a, b, direction = -1) {
  const aEmpty = a === null || a === undefined || a === "";
  const bEmpty = b === null || b === undefined || b === "";

  if (aEmpty && bEmpty) return 0;
  if (aEmpty) return 1;
  if (bEmpty) return -1;

  if (typeof a === "number" && typeof b === "number") {
    return direction * (b - a);
  }

  return direction * String(b).localeCompare(String(a), undefined, { numeric: true, sensitivity: "base" });
}

function csvSafe(value) {
  if (value === null || value === undefined) return "";
  const text = String(value);
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function downloadBlob(filename, blob) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function exportRowsToCsv(filename, rows, columns) {
  const csv = [
    columns.join(","),
    ...rows.map((row) => columns.map((column) => csvSafe(row[column])).join(",")),
  ].join("\n");

  downloadBlob(filename, new Blob([csv], { type: "text/csv;charset=utf-8;" }));
}

function exportSvgNode(svgNode, filename) {
  if (!svgNode) return;
  const serializer = new XMLSerializer();
  const svgMarkup = serializer.serializeToString(svgNode);
  downloadBlob(filename, new Blob([svgMarkup], { type: "image/svg+xml;charset=utf-8;" }));
}

function getAmazonProductUrl(asin) {
  if (!asin) return "#";
  return `https://www.amazon.in/dp/${encodeURIComponent(asin)}`;
}

// ── KPI Card ───────────────────────────────────────────────────────────────
function KpiCard({ label, value, sub, accent, trend }) {
  const isPos = trend > 0;
  const isNeg = trend < 0;
  return (
    <div style={{
      background: THEME.surface, border: `1px solid ${THEME.border}`,
      borderRadius: 10, padding: "10px 16px", minWidth: 140, flex: "0 0 auto",
      borderTop: `2px solid ${accent}`, display:"flex", flexDirection:"column", gap:2,
    }}>
      <div style={{ fontSize: 9, color: THEME.textMuted, fontFamily: "'DM Mono',monospace", letterSpacing: 1.2, textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 800, color: THEME.text, letterSpacing: -0.5, lineHeight: 1.2 }}>{value}</div>
      {trend !== undefined && trend !== null && (
        <div style={{ fontSize: 10, display:"flex", alignItems:"center", gap:2,
          color: isPos ? "#059669" : isNeg ? "#DC2626" : THEME.textMuted, fontWeight:700 }}>
          {isPos ? "▲" : isNeg ? "▼" : "—"} {Math.abs(trend).toFixed(1)}%
        </div>
      )}
    </div>
  );
}

function Panel({ title, subtitle, right, children }) {
  const hasHeader = title || subtitle || right;
  return (
    <section style={{ borderRadius: 22, border: `1px solid ${THEME.border}`, background: `linear-gradient(180deg, ${THEME.surface}, ${THEME.panelBg})`, boxShadow: "0 18px 48px rgba(15,23,42,0.06)", overflow: "hidden" }}>
      {hasHeader && (
        <div style={{ padding: "18px 20px 12px", display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", alignItems: "flex-start" }}>
          <div>
            {title && <div style={{ fontSize: 16, fontWeight: 700, color: THEME.text }}>{title}</div>}
            {subtitle && <div style={{ fontSize: 12, color: THEME.textMuted, marginTop: 4 }}>{subtitle}</div>}
          </div>
          {right}
        </div>
      )}
      <div style={{ padding: hasHeader ? "0 20px 20px" : "20px" }}>{children}</div>
    </section>
  );
}

function formatMetricValue(value, type) {
  if (!value) return type === "currency" ? "₹0" : "0";
  if (type === "currency") return inr(value);
  return Math.round(value).toLocaleString("en-IN");
}

function fmtDiffValue(v, fmt) {
  if (v == null || !Number.isFinite(v)) return "—";
  if (fmt === "money") return inr(v);
  return Math.round(v).toLocaleString("en-IN");
}

function fmtDiffDelta(v, fmt) {
  if (v == null || !Number.isFinite(v)) return "—";
  const sign = v > 0 ? "+" : v < 0 ? "−" : "";
  const abs = Math.abs(v);
  if (fmt === "money") return `${sign}${inr(abs)}`;
  return `${sign}${Math.round(abs).toLocaleString("en-IN")}`;
}

function diffColor(delta, pct, metricKey) {
  if (delta === 0 || delta == null) return "#94A3B8";
  const badWhenNeg = ["units_3p", "rev_3p", "units_1p", "rev_1p", "ads_sales", "orders"];
  const badWhenPos = ["spend"];
  if (badWhenNeg.includes(metricKey) && delta < 0) return "#D97706";
  if (badWhenPos.includes(metricKey) && delta > 0) return "#D97706";
  if (metricKey === "ads_sales" && delta > 0) return "#059669";
  return "#0F766E";
}

function RefreshDiffPage({ data, onBack }) {
  const monthKey = "Jun";
  const monthData = data?.months?.[monthKey];
  if (!monthData) {
    return (
      <div style={{ padding: 40, fontFamily: "'Inter','Segoe UI',sans-serif" }}>
        <button onClick={onBack} style={{ background: THEME.surfaceMuted, color: THEME.textSoft, border: `1px solid ${THEME.border}`, borderRadius: 8, padding: "7px 12px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>← Back</button>
        <div style={{ marginTop: 16 }}>No diff data available for {monthKey}.</div>
      </div>
    );
  }

  const brands = Object.values(monthData.brands);

  return (
    <div style={{ fontFamily: "'Inter','Segoe UI',sans-serif", background: THEME.pageBg, minHeight: "100vh", color: THEME.text }}>
      <div style={{ background: THEME.surface, borderBottom: `1px solid ${THEME.border}`, padding: "14px 24px", display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <button onClick={onBack} style={{ background: THEME.surfaceMuted, color: THEME.textSoft, border: `1px solid ${THEME.border}`, borderRadius: 8, padding: "7px 12px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>← Back</button>
          <div>
            <div style={{ fontSize: 18, fontWeight: 800, color: THEME.text, letterSpacing: -0.3 }}>Refresh Diff · June 2026</div>
            <div style={{ fontSize: 10, color: THEME.textFaint, fontFamily: "'DM Mono',monospace", letterSpacing: 0.8 }}>
              How June data shifted between the first cron and the latest re-refresh
            </div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 20, alignItems: "center" }}>
          <div style={{ textAlign: "right" }}>
            <div style={{ fontSize: 10, color: THEME.textFaint, fontFamily: "'DM Mono',monospace", textTransform: "uppercase", letterSpacing: 0.8 }}>{monthData.firstPull.label}</div>
            <div style={{ fontSize: 12, color: THEME.textSoft, fontWeight: 600 }}>{monthData.firstPull.date}</div>
          </div>
          <div style={{ fontSize: 18, color: THEME.textFaint }}>→</div>
          <div style={{ textAlign: "right" }}>
            <div style={{ fontSize: 10, color: THEME.textFaint, fontFamily: "'DM Mono',monospace", textTransform: "uppercase", letterSpacing: 0.8 }}>{monthData.latest.label}</div>
            <div style={{ fontSize: 12, color: "#0F766E", fontWeight: 700 }}>{monthData.latest.date}</div>
          </div>
        </div>
      </div>

      <div style={{ padding: "18px 24px 8px" }}>
        <div style={{ background: "#FEF3C7", border: "1px solid #FCD34D", borderRadius: 10, padding: "10px 14px", fontSize: 12, color: "#78350F", lineHeight: 1.55 }}>
          <b>Why this matters:</b> Amazon keeps updating sales &amp; ads reports for weeks after a month closes — 3P orders get cancelled/returned, SP ads keep attributing sales inside the 14-day window, and SB/SD reports re-consolidate. This tab shows what actually changed between the first cron pull and the latest refresh so you know which June numbers to trust.
        </div>
      </div>

      <div style={{ padding: "8px 24px 32px", display: "grid", gap: 20 }}>
        {brands.map((b) => (
          <section key={b.brand} style={{ borderRadius: 16, border: `1px solid ${THEME.border}`, background: THEME.surface, boxShadow: "0 8px 24px rgba(15,23,42,0.05)", overflow: "hidden" }}>
            <div style={{ padding: "14px 20px", borderBottom: `1px solid ${THEME.border}`, display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 10 }}>
              <div>
                <div style={{ fontSize: 16, fontWeight: 800, color: THEME.text }}>{b.brand}</div>
                <div style={{ fontSize: 11, color: THEME.textMuted, marginTop: 2 }}>
                  {b.asinCount.first === b.asinCount.latest
                    ? `${b.asinCount.latest} ASINs (unchanged)`
                    : `${b.asinCount.first} → ${b.asinCount.latest} ASINs`}
                </div>
              </div>
            </div>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr style={{ background: THEME.headerBg }}>
                    <th style={{ padding: "10px 14px", textAlign: "left", fontFamily: "'DM Mono',monospace", fontSize: 9.5, fontWeight: 500, letterSpacing: 0.7, textTransform: "uppercase", color: THEME.headerText }}>Metric</th>
                    <th style={{ padding: "10px 14px", textAlign: "right", fontFamily: "'DM Mono',monospace", fontSize: 9.5, fontWeight: 500, letterSpacing: 0.7, textTransform: "uppercase", color: THEME.headerText }}>{monthData.firstPull.label}</th>
                    <th style={{ padding: "10px 14px", textAlign: "right", fontFamily: "'DM Mono',monospace", fontSize: 9.5, fontWeight: 500, letterSpacing: 0.7, textTransform: "uppercase", color: THEME.headerText }}>Latest</th>
                    <th style={{ padding: "10px 14px", textAlign: "right", fontFamily: "'DM Mono',monospace", fontSize: 9.5, fontWeight: 500, letterSpacing: 0.7, textTransform: "uppercase", color: THEME.headerText }}>Δ</th>
                    <th style={{ padding: "10px 14px", textAlign: "right", fontFamily: "'DM Mono',monospace", fontSize: 9.5, fontWeight: 500, letterSpacing: 0.7, textTransform: "uppercase", color: THEME.headerText }}>Δ %</th>
                    <th style={{ padding: "10px 14px", textAlign: "left", fontFamily: "'DM Mono',monospace", fontSize: 9.5, fontWeight: 500, letterSpacing: 0.7, textTransform: "uppercase", color: THEME.headerText }}>What it means</th>
                  </tr>
                </thead>
                <tbody>
                  {b.metrics.map((m, i) => {
                    const color = diffColor(m.delta, m.pct, m.key);
                    const rowBg = i % 2 === 0 ? THEME.surface : THEME.panelBg;
                    return (
                      <tr key={m.key} style={{ borderTop: `1px solid ${THEME.border}`, background: rowBg }}>
                        <td style={{ padding: "9px 14px", fontWeight: 600, color: THEME.text }}>{m.label}</td>
                        <td style={{ padding: "9px 14px", textAlign: "right", fontFamily: "'DM Mono',monospace", color: THEME.textSoft }}>{fmtDiffValue(m.first, m.fmt)}</td>
                        <td style={{ padding: "9px 14px", textAlign: "right", fontFamily: "'DM Mono',monospace", color: THEME.text, fontWeight: 700 }}>{fmtDiffValue(m.latest, m.fmt)}</td>
                        <td style={{ padding: "9px 14px", textAlign: "right", fontFamily: "'DM Mono',monospace", color, fontWeight: 700 }}>{fmtDiffDelta(m.delta, m.fmt)}</td>
                        <td style={{ padding: "9px 14px", textAlign: "right", fontFamily: "'DM Mono',monospace", color, fontWeight: 700 }}>{m.pct == null ? "—" : `${m.pct > 0 ? "+" : ""}${m.pct}%`}</td>
                        <td style={{ padding: "9px 14px", fontSize: 11, color: THEME.textMuted }}>{m.note}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>
        ))}

        <div style={{ fontSize: 11, color: THEME.textFaint, textAlign: "center", padding: "4px 0" }}>
          Generated {new Date(monthData.generatedAt).toLocaleString("en-IN", { timeZone: "Asia/Kolkata" })} · rebuilds automatically on every dashboard deploy
        </div>
      </div>
    </div>
  );
}

function MiniMonthBars({ jan = 0, feb = 0, colorA = "#38BDF8", colorB = "#A78BFA", type = "number" }) {
  const max = Math.max(jan, feb, 1);
  const bars = [
    { label: "Jan", value: jan, color: colorA },
    { label: "Feb", value: feb, color: colorB },
  ];

  return (
    <div style={{ display: "grid", gap: 10 }}>
      {bars.map((bar) => (
        <div key={bar.label} style={{ display: "grid", gridTemplateColumns: "38px 1fr auto", gap: 10, alignItems: "center" }}>
          <div style={{ fontSize: 10, color: THEME.textMuted, fontFamily: "'DM Mono',monospace" }}>{bar.label}</div>
          <div style={{ height: 10, borderRadius: 999, background: THEME.surfaceSoft, overflow: "hidden" }}>
            <div style={{ width: `${(bar.value / max) * 100}%`, minWidth: bar.value ? 8 : 0, height: "100%", borderRadius: 999, background: `linear-gradient(90deg, ${bar.color}, ${bar.color}CC)` }} />
          </div>
          <div style={{ fontSize: 12, color: THEME.text, fontWeight: 600 }}>{formatMetricValue(bar.value, type)}</div>
        </div>
      ))}
    </div>
  );
}

function TrendLineChart({ points, color = "#38BDF8", valueType = "number", svgRef }) {
  const width = 520;
  const height = 180;
  const padding = 18;
  const values = points.map((point) => point.value);
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const range = Math.max(max - min, 1);

  const coords = points.map((point, index) => {
    const x = padding + (index * (width - padding * 2)) / Math.max(points.length - 1, 1);
    const y = height - padding - ((point.value - min) / range) * (height - padding * 2);
    return { ...point, x, y };
  });

  const path = coords.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");

  return (
    <div>
      <svg ref={svgRef} viewBox={`0 0 ${width} ${height}`} style={{ width: "100%", height: 200 }}>
        <defs>
          <linearGradient id="trendFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.35" />
            <stop offset="100%" stopColor={color} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        {[0, 1, 2, 3].map((tick) => {
          const y = padding + (tick * (height - padding * 2)) / 3;
          return <line key={tick} x1={padding} x2={width - padding} y1={y} y2={y} stroke={THEME.border} strokeWidth="1" />;
        })}
        <path d={`${path} L ${coords.at(-1)?.x ?? padding} ${height - padding} L ${coords[0]?.x ?? padding} ${height - padding} Z`} fill="url(#trendFill)" />
        <path d={path} fill="none" stroke={color} strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
        {coords.map((point) => (
          <g key={point.label}>
            <circle cx={point.x} cy={point.y} r="5" fill={THEME.surface} stroke={color} strokeWidth="2.5" />
            <text x={point.x} y={height - 2} textAnchor="middle" fill={THEME.textMuted} fontSize="10" fontFamily="'DM Mono', monospace">{point.label}</text>
            <text x={point.x} y={point.y - 10} textAnchor="middle" fill={THEME.text} fontSize="10">{formatMetricValue(point.value, valueType)}</text>
          </g>
        ))}
      </svg>
    </div>
  );
}


// ── MultiSelect pill filter component ──────────────────────────────────────
function MultiSelectFilter({ label, selected, setSelected, opts, onClear, isActive, allLabel }) {
  const [open, setOpen] = React.useState(false);
  const ref = React.useRef(null);
  React.useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);
  const toggle = (v) => {
    setSelected(prev => prev.includes(v) ? prev.filter(x => x !== v) : [...prev, v]);
  };
  // The chip label ("CATEGORY L0") and the plural used in the summary text
  // ("All Categorys") are separate: two category filters share one plural.
  const plural = allLabel ?? `${label}s`;
  const displayLabel = selected.length === 0
    ? `All ${plural}`
    : selected.length === 1
    ? selected[0]
    : `${selected.length} ${plural}`;
  return (
    <div ref={ref} style={{ position:"relative" }}>
      <div
        onClick={() => setOpen(o => !o)}
        style={{
          display:"flex", gap:6, alignItems:"center", padding:"7px 10px", borderRadius:8,
          background: isActive ? "#EFF6FF" : "#FFFFFF",
          border: isActive ? "1px solid #3B82F6" : "1px solid #E2E8F0",
          cursor:"pointer", userSelect:"none", minWidth:100
        }}
      >
        <span style={{ fontSize:10, color: isActive ? "#2563EB" : "#64748B", fontFamily:"'DM Mono',monospace", letterSpacing:0.8, textTransform:"uppercase", fontWeight:600, whiteSpace:"nowrap" }}>{label}</span>
        <span style={{ fontSize:12, color: isActive ? "#1D4ED8" : "#0F172A", whiteSpace:"nowrap", fontWeight:500 }}>{displayLabel}</span>
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke={isActive?"#3B82F6":"#94A3B8"} strokeWidth="2" style={{ marginLeft:2, flexShrink:0 }}><polyline points={open?"18 15 12 9 6 15":"6 9 12 15 18 9"}/></svg>
      </div>
      {open && (
        <div style={{
          position:"absolute", top:"calc(100% + 4px)", left:0, zIndex:200,
          background:"#FFFFFF", border:"1px solid #E2E8F0", borderRadius:10,
          minWidth:190, boxShadow:"0 8px 24px rgba(15,23,42,0.12)", padding:"6px 0",
          maxHeight:320, display:"flex", flexDirection:"column"
        }}>
          <div
            onClick={() => { setSelected([]); }}
            style={{ padding:"7px 14px", fontSize:12, color: selected.length===0?"#2563EB":"#475569", cursor:"pointer", fontWeight: selected.length===0?700:500 }}
          >All {plural}</div>
          <div style={{ height:1, background:"#E2E8F0", margin:"4px 0" }}/>
          <div style={{ overflowY:"auto", flex:1 }}>
          {opts.map((o, index) => (
            <React.Fragment key={o.v}>
            {/* Group header, e.g. the year above its months.  Clicking it
                selects that whole group, which is how you jump to "just 2025". */}
            {o.g && o.g !== opts[index - 1]?.g && (
              <div
                onClick={() => setSelected(opts.filter(x => x.g === o.g).map(x => x.v))}
                style={{
                  padding:"8px 14px 4px", fontSize:10, letterSpacing:1,
                  color:"#94A3B8", fontWeight:700, textTransform:"uppercase",
                  cursor:"pointer", borderTop: index === 0 ? "none" : "1px solid #F1F5F9",
                  marginTop: index === 0 ? 0 : 4,
                }}
                title={`Show only ${o.g}`}
              >{o.g}</div>
            )}
            <div
              onClick={() => toggle(o.v)}
              style={{
                padding: o.g ? "7px 14px 7px 22px" : "7px 14px",
                fontSize:12, cursor:"pointer",
                color: selected.includes(o.v) ? "#2563EB" : "#0F172A",
                fontWeight: selected.includes(o.v) ? 700 : 500,
                display:"flex", alignItems:"center", gap:8
              }}
            >
              <div style={{
                width:14, height:14, borderRadius:4, flexShrink:0,
                border: selected.includes(o.v) ? "2px solid #2563EB" : "2px solid #CBD5E1",
                background: selected.includes(o.v) ? "#2563EB" : "transparent",
                display:"flex", alignItems:"center", justifyContent:"center"
              }}>
                {selected.includes(o.v) && <svg width="8" height="8" viewBox="0 0 12 12"><polyline points="2,6 5,9 10,3" stroke="white" strokeWidth="2" fill="none"/></svg>}
              </div>
              {o.l}
            </div>
            </React.Fragment>
          ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Main Dashboard ─────────────────────────────────────────────────────────
// ── Module-level constants (stable across renders) ─────────────────────────
const TABLE_PAGE_SIZE = 25;

// Returns display-ready metric values for a row based on the active data mode.
// "biz" = Business Report  →  Rev3P, Units3P, Sessions, BuyboxPct
// "p1"  = 1P Sales         →  Rev1P, Units1P, Sessions/BB hidden (shown as null)
function getMetrics(row, mode) {
  // "all" = combined (Rev1P+Rev3P), "biz" = Amazon Sales 3P only, "p1" = 1P only
  if (mode === "all") {
    const rev   = row.TotalNetSalesValue || 0;
    const units = row.NetUnits || 0;
    const spend = row.TotalAdsSpend || 0;
    const adSales = row.TotalAdsSales || 0;
    const tacos = rev > 0 ? spend / rev : 0;
    const acos  = adSales > 0 ? spend / adSales : 0;
    const cac   = (row.AmsOrders || 0) > 0 ? spend / row.AmsOrders : 0;
    const orgSales = Math.max(0, rev - adSales);
    return {
      revenue: rev, units,
      sessions: row.Sessions || 0,
      buybox: row.BuyboxPct || 0,
      adSpend: spend, adSales, acos, tacos, cac,
      amsOrders: row.AmsOrders || 0,
      impressions: row.Impressions || 0,
      clicks: row.Clicks || 0,
      orgSales, orgPct: rev > 0 ? orgSales / rev : 0,
      cvr: (row.Sessions || 0) > 0 ? units / row.Sessions : 0,
    };
  }
  if (mode === "p1") {
    const rev   = row.Rev1P   || 0;
    const units = row.Units1P || 0;
    const spend = row.TotalAdsSpend || 0;
    const adSales = row.TotalAdsSales || 0;
    const tacos = rev > 0 ? spend / rev : 0;
    const acos  = adSales > 0 ? spend / adSales : 0;
    const cac   = (row.AmsOrders || 0) > 0 ? spend / row.AmsOrders : 0;
    return {
      revenue:   rev,
      units,
      sessions:  null,   // not available in 1P
      buybox:    null,   // not available in 1P
      adSpend:   spend,
      adSales,
      acos,
      tacos,
      cac,
      amsOrders: row.AmsOrders || 0,
      impressions: row.Impressions || 0,
      clicks:    row.Clicks || 0,
      orgSales:  null,
      orgPct:    null,
      cvr:       null,
    };
  }
  // default: "biz" — Business Report
  const rev   = row.Rev3P   || 0;
  const units = row.Units3P || 0;
  const spend = row.TotalAdsSpend || 0;
  const adSales = row.TotalAdsSales || 0;
  const tacos = rev > 0 ? spend / rev : 0;
  const acos  = adSales > 0 ? spend / adSales : 0;
  const cac   = (row.AmsOrders || 0) > 0 ? spend / row.AmsOrders : 0;
  const orgSales = Math.max(0, rev - adSales);
  const orgPct   = rev > 0 ? orgSales / rev : 0;
  const cvr = (row.Sessions || 0) > 0 ? units / row.Sessions : 0;
  return {
    revenue:    rev,
    units,
    sessions:   row.Sessions   || 0,
    buybox:     row.BuyboxPct  || 0,
    adSpend:    spend,
    adSales,
    acos,
    tacos,
    cac,
    amsOrders:  row.AmsOrders  || 0,
    impressions: row.Impressions || 0,
    clicks:     row.Clicks     || 0,
    orgSales,
    orgPct,
    cvr,
  };
}

// ── BSR Tracker Page ─────────────────────────────────────────────────────────
function getBsrTier(bsr) {
  if (!bsr) return { label:"No data", color:"#9CA3AF", bg:"#F3F4F6", emoji:"—" };
  if (bsr <= 1000)   return { label:"Top 1K",   color:"#059669", bg:"#D1FAE5", emoji:"🏆" };
  if (bsr <= 5000)   return { label:"Top 5K",   color:"#0284C7", bg:"#DBEAFE", emoji:"🥇" };
  if (bsr <= 10000)  return { label:"Top 10K",  color:"#7C3AED", bg:"#EDE9FE", emoji:"🥈" };
  if (bsr <= 50000)  return { label:"Top 50K",  color:"#D97706", bg:"#FEF3C7", emoji:"🥉" };
  if (bsr <= 100000) return { label:"Top 100K", color:"#EA580C", bg:"#FFEDD5", emoji:"📈" };
  return               { label:"100K+",         color:"#DC2626", bg:"#FEE2E2", emoji:"📉" };
}

const BRAND_META = {
  "Audio Array":    { accent:"#7C3AED", light:"#F5F3FF", text:"#6D28D9" },
  "Nexlev":         { accent:"#2563EB", light:"#EFF6FF", text:"#1D4ED8" },
  "nexlev":         { accent:"#2563EB", light:"#EFF6FF", text:"#1D4ED8" },
  "TONOR":          { accent:"#059669", light:"#ECFDF5", text:"#047857" },
  "Tonor":          { accent:"#059669", light:"#ECFDF5", text:"#047857" },
  "White Mulberry": { accent:"#D97706", light:"#FFFBEB", text:"#B45309" },
};

// ── Real per-ASIN daily history ────────────────────────────────────────
// Every chart below used to fake its own history: it plotted the 365/90/30-day
// AVERAGES as if they were points in time, and manufactured the rating-count
// line from fixed multipliers (0.70 / 0.88 / 0.95 of today's value). Both were
// inventions. An average is not a past value, and a fixed multiplier can only
// slope one way -- so an ASIN whose review count FELL was still drawn as a
// clean rise. The published snapshots hold the real thing, one row per pull.
const _histCache = new Map();
const buildAsinHistory = (asin, brand) => {
  const ck = `${brand}::${asin}`;
  if (_histCache.has(ck)) return _histCache.get(ck);
  const snaps = BSR_SNAPSHOTS || {};
  const out = [];
  Object.keys(snaps).sort().forEach(date => {
    const byBrand = snaps[date] || {};
    // Prefer the row's own brand; fall back to a scan because master data
    // re-tags some ASINs (an AudioArray-account product filed under Tonor).
    let hit = (byBrand[brand] || []).find(r => r.asin === asin);
    if (!hit) {
      for (const b of Object.keys(byBrand)) {
        hit = (byBrand[b] || []).find(r => r.asin === asin);
        if (hit) break;
      }
    }
    if (!hit) return;
    out.push({
      date,
      bsr:          hit.bsr || null,
      rating:       hit.rating ?? null,
      reviews:      hit.reviews ?? null,
      monthly_sold: hit.monthly_sold ?? null,
    });
  });
  _histCache.set(ck, out);
  return out;
};
// Up to `n` evenly spaced dates, so a 76-day axis stays readable.
const axisDates = (hist, n = 5) => {
  if (hist.length <= 1) return hist.map(p => p.date);
  const k = Math.min(n, hist.length);
  return Array.from({ length: k }, (_, i) => hist[Math.round(i * (hist.length - 1) / (k - 1))].date);
};

function BsrGraphModal({ row, onClose }) {
  const T = THEME;
  const bc = BRAND_META[row.brand] || { accent:"#6B7280", light:"#F9FAFB", text:"#4B5563" };
  const tier = getBsrTier(row.bsr);
  const trendColor = row.trend==="up"?"#059669":row.trend==="down"?"#DC2626":"#6B7280";
  const trendLabel = row.trend==="up"?"↑ Improving":row.trend==="down"?"↓ Declining":"→ Stable";
  const stars = row.rating ? Math.round(row.rating) : 0;

  // Hover state for tooltips
  const [hover, setHover] = useState(null); // { chart, label, values }

  // Chart geometry (Keepa-like proportions)
  const W = 760, chartH = 150, ratingH = 110, salesH = 100;
  const PAD_L = 60, PAD_R = 60, PAD_T = 14, PAD_B = 26;
  const plotW = W - PAD_L - PAD_R;

  // ── Real measured history (see buildAsinHistory above) ──
  const hist = useMemo(() => buildAsinHistory(row.asin, row.brand), [row.asin, row.brand]);
  // X is positioned by real date, not by index, so a gap in the pulls reads as
  // a gap instead of being silently closed up into an even step.
  const tOf = d => new Date(d + "T00:00:00").getTime();
  const tMin = hist.length ? tOf(hist[0].date) : 0;
  const tSpan = hist.length ? (tOf(hist[hist.length-1].date) - tMin) || 1 : 1;
  const xOf = d => hist.length <= 1 ? PAD_L + plotW/2 : PAD_L + ((tOf(d) - tMin) / tSpan) * plotW;
  const ticks = axisDates(hist);

  const bsrData    = hist.filter(p => p.bsr).map(p => ({ date:p.date, label:formatDateShort(p.date), val:p.bsr }));
  const ratingData = hist.filter(p => p.rating).map(p => ({ date:p.date, label:formatDateShort(p.date), val:p.rating }));
  const rcData     = hist.filter(p => p.reviews).map(p => ({ date:p.date, label:formatDateShort(p.date), val:p.reviews }));

  const bsrMax = bsrData.length ? Math.max(...bsrData.map(p=>p.val)) : 1;
  const bsrMin = bsrData.length ? Math.min(...bsrData.map(p=>p.val)) : 0;
  const bsrSpan = (bsrMax - bsrMin) || 1;
  const bsrX = i => xOf(bsrData[i].date);
  // Flipped: lower rank = higher on chart (like Keepa)
  const bsrY = v => PAD_T + ((v - bsrMin) / bsrSpan) * (chartH - PAD_T - PAD_B);
  const bsrPath = bsrData.map((p,i)=>`${i===0?"M":"L"}${bsrX(i)},${bsrY(p.val)}`).join(" ");
  const bsrArea = bsrData.length>0 ? `${bsrPath} L${bsrX(bsrData.length-1)},${chartH-PAD_B} L${bsrX(0)},${chartH-PAD_B} Z` : "";
  // Dots would merge into a smear once there are more than a couple of dozen
  // days, so they shrink; the invisible hover target stays full size.
  const dotR = bsrData.length > 24 ? 1.6 : 4;

  // ── Rating chart (1 to 5 scale) ──
  const ratingY = v => PAD_T + ((5 - v) / 4) * (ratingH - PAD_T - PAD_B);
  const ratingPath = ratingData.map((p,i)=>`${i===0?"M":"L"}${xOf(p.date)},${ratingY(p.val)}`).join(" ");
  // Rating count is scaled between its own min and max -- a 0-based axis would
  // flatten a 1,503 -> 1,859 move into a straight line.
  const rcMax = rcData.length ? Math.max(...rcData.map(p=>p.val)) : 1;
  const rcMin = rcData.length ? Math.min(...rcData.map(p=>p.val)) : 0;
  const rcSpan = (rcMax - rcMin) || 1;
  const rcY = v => PAD_T + (1 - (v - rcMin) / rcSpan) * (ratingH - PAD_T - PAD_B);
  const rcPath = rcData.map((p,i)=>`${i===0?"M":"L"}${xOf(p.date)},${rcY(p.val)}`).join(" ");

  // ── Monthly Sold bars ──
  const monthlyVal = row.monthly_sold || 0;
  const salesMax = Math.max(monthlyVal * 1.2, 10);
  const salesY = v => PAD_T + (1 - v / salesMax) * (salesH - PAD_T - PAD_B);

  // Keepa-style color palette
  const K_BSR    = "#2E9442";  // Keepa green for rank
  const K_RATING = "#17A6A6";  // Keepa teal for rating
  const K_RC     = "#9ACD32";  // Keepa yellow-green for rating count
  const K_SOLD   = "#E8A33D";  // Keepa amber for sold

  const fmtRank = v => v ? `#${v.toLocaleString("en-IN")}` : "—";

  return (
    <div onClick={onClose} style={{ position:"fixed", inset:0, background:"rgba(15,23,42,0.72)", display:"flex", alignItems:"center", justifyContent:"center", zIndex:9999, padding:16 }}>
      <div onClick={e=>e.stopPropagation()}
        style={{ background:"#fff", borderRadius:14, width:820, maxWidth:"98vw", maxHeight:"92vh", overflow:"auto", boxShadow:"0 30px 90px rgba(0,0,0,0.35)", fontFamily:"'Inter','Segoe UI',sans-serif" }}>

        {/* ── Header ── */}
        <div style={{ padding:"14px 18px", borderBottom:`1px solid #E5E7EB`, display:"flex", alignItems:"center", gap:10, flexWrap:"wrap", background:"#fff" }}>
          <a href={`https://www.amazon.in/dp/${row.asin}`} target="_blank" rel="noreferrer"
            style={{ fontFamily:"'DM Mono',monospace", fontSize:13, fontWeight:700, color:"#2563EB", textDecoration:"none" }}>
            {row.asin}
          </a>
          <span style={{ background:bc.light, color:bc.text, borderRadius:20, padding:"2px 10px", fontSize:10, fontWeight:700 }}>{row.brand}</span>
          <span style={{ background:tier.bg, color:tier.color, borderRadius:20, padding:"2px 9px", fontSize:10, fontWeight:700 }}>{tier.emoji} {tier.label}</span>
          <span style={{ fontSize:10, color:"#6B7280", fontFamily:"'DM Mono',monospace", marginLeft:6 }}>
            {hist.length > 1 ? `${hist.length} DAILY PULLS · ${formatDateShort(hist[0].date)} → ${formatDateShort(hist[hist.length-1].date)}` : hist.length === 1 ? `SINGLE PULL · ${formatDateShort(hist[0].date)}` : "NO HISTORY"}
          </span>
          <button onClick={onClose} style={{ marginLeft:"auto", border:"none", background:"transparent", fontSize:20, cursor:"pointer", color:"#6B7280", lineHeight:1 }}>✕</button>
        </div>

        {/* ── Three charts stacked ── */}
        <div style={{ padding:"0 16px", background:"#fff" }}>

          {/* Legend row */}
          <div style={{ padding:"10px 6px 6px", display:"flex", gap:14, flexWrap:"wrap", alignItems:"center", fontSize:11 }}>
            <span style={{ display:"flex", alignItems:"center", gap:5 }}><span style={{ width:12, height:3, background:K_BSR, borderRadius:2 }}/><span style={{ color:"#374151", fontWeight:600 }}>Sales Rank</span></span>
            <span style={{ display:"flex", alignItems:"center", gap:5 }}><span style={{ width:12, height:3, background:K_RATING, borderRadius:2 }}/><span style={{ color:"#374151", fontWeight:600 }}>Rating</span></span>
            <span style={{ display:"flex", alignItems:"center", gap:5 }}><span style={{ width:12, height:3, background:K_RC, borderRadius:2 }}/><span style={{ color:"#374151", fontWeight:600 }}>Rating Count</span></span>
            <span style={{ display:"flex", alignItems:"center", gap:5 }}><span style={{ width:10, height:10, background:K_SOLD, borderRadius:2 }}/><span style={{ color:"#374151", fontWeight:600 }}>Monthly Sold</span></span>
            <a href={`https://keepa.com/#!product/12-${row.asin}`} target="_blank" rel="noreferrer"
              style={{ marginLeft:"auto", background:"#F3F4F6", color:"#374151", fontSize:10, fontWeight:700, padding:"5px 10px", borderRadius:7, textDecoration:"none", border:"1px solid #E5E7EB" }}>
              🔗 Open Full Keepa Chart →
            </a>
          </div>

          {/* ═══ Chart 1: Sales Rank ═══ */}
          <div style={{ position:"relative", marginBottom:4 }}>
            <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#9CA3AF", fontWeight:700, padding:"2px 0 4px 6px", letterSpacing:0.5 }}>SALES RANK</div>
            <svg width="100%" viewBox={`0 0 ${W} ${chartH}`} style={{ display:"block", background:"#FAFAFA", borderRadius:6, border:"1px solid #E5E7EB" }}>
              <defs>
                <linearGradient id="bsrFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%"   stopColor={K_BSR} stopOpacity="0.28"/>
                  <stop offset="100%" stopColor={K_BSR} stopOpacity="0.02"/>
                </linearGradient>
              </defs>
              {/* Horizontal gridlines */}
              {[0,0.25,0.5,0.75,1].map((f,i)=>(
                <g key={i}>
                  <line x1={PAD_L} y1={PAD_T+f*(chartH-PAD_T-PAD_B)} x2={W-PAD_R} y2={PAD_T+f*(chartH-PAD_T-PAD_B)}
                    stroke="#E5E7EB" strokeWidth="1" strokeDasharray={i===0||i===4?"0":"3,3"}/>
                  <text x={PAD_L-6} y={PAD_T+f*(chartH-PAD_T-PAD_B)+3} textAnchor="end" fontSize="9" fill="#6B7280" fontFamily="'DM Mono',monospace">
                    {fmtRank(Math.round(bsrMin + f*bsrSpan))}
                  </text>
                </g>
              ))}
              {/* Vertical gridlines (at the labelled dates) */}
              {ticks.map((d,i)=>(
                <line key={i} x1={xOf(d)} y1={PAD_T} x2={xOf(d)} y2={chartH-PAD_B}
                  stroke="#F3F4F6" strokeWidth="1"/>
              ))}
              {/* Area */}
              <path d={bsrArea} fill="url(#bsrFill)"/>
              {/* Line */}
              <path d={bsrPath} fill="none" stroke={K_BSR} strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"/>
              {/* Points */}
              {bsrData.map((p,i)=>(
                <g key={i} style={{ cursor:"pointer" }}
                   onMouseEnter={()=>setHover({ chart:"bsr", label:p.label, value:fmtRank(p.val), x:bsrX(i), y:bsrY(p.val) })}
                   onMouseLeave={()=>setHover(null)}>
                  <circle cx={bsrX(i)} cy={bsrY(p.val)} r={dotR} fill={K_BSR} stroke="#fff" strokeWidth={dotR>2?2:0}/>
                  <circle cx={bsrX(i)} cy={bsrY(p.val)} r="10" fill="transparent"/>
                </g>
              ))}
              {/* X axis labels — real dates */}
              {ticks.map((d,i)=>(
                <text key={i} x={xOf(d)} y={chartH-8} textAnchor={i===0?"start":i===ticks.length-1?"end":"middle"} fontSize="10" fill="#6B7280" fontFamily="'DM Mono',monospace">{formatDateShort(d)}</text>
              ))}
              {/* Tooltip */}
              {hover && hover.chart==="bsr" && (
                <g>
                  <rect x={hover.x-50} y={hover.y-40} width={100} height={30} rx={5} fill="#111827" opacity="0.95"/>
                  <text x={hover.x} y={hover.y-24} textAnchor="middle" fontSize="9" fill="#9CA3AF">{hover.label}</text>
                  <text x={hover.x} y={hover.y-12} textAnchor="middle" fontSize="11" fill="#fff" fontWeight="700" fontFamily="'DM Mono',monospace">{hover.value}</text>
                </g>
              )}
            </svg>
          </div>

          {/* ═══ Chart 2: Rating + Rating Count ═══ */}
          <div style={{ position:"relative", marginBottom:4 }}>
            <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#9CA3AF", fontWeight:700, padding:"2px 0 4px 6px", letterSpacing:0.5 }}>RATING &amp; RATING COUNT</div>
            <svg width="100%" viewBox={`0 0 ${W} ${ratingH}`} style={{ display:"block", background:"#FAFAFA", borderRadius:6, border:"1px solid #E5E7EB" }}>
              {/* Horizontal gridlines (rating scale) */}
              {[1,2,3,4,5].map((r,i)=>(
                <g key={r}>
                  <line x1={PAD_L} y1={ratingY(r)} x2={W-PAD_R} y2={ratingY(r)}
                    stroke="#E5E7EB" strokeWidth="1" strokeDasharray={r===1||r===5?"0":"3,3"}/>
                  <text x={PAD_L-6} y={ratingY(r)+3} textAnchor="end" fontSize="9" fill="#17A6A6" fontFamily="'DM Mono',monospace" fontWeight="600">{r}★</text>
                </g>
              ))}
              {/* Rating Count axis on right */}
              {rcData.length > 0 && [0, 0.5, 1].map((f,i)=>(
                <text key={i} x={W-PAD_R+6} y={PAD_T+(1-f)*(ratingH-PAD_T-PAD_B)+3} textAnchor="start" fontSize="9" fill="#7CB342" fontFamily="'DM Mono',monospace" fontWeight="600">
                  {Math.round(rcMin + rcSpan*f).toLocaleString("en-IN")}
                </text>
              ))}
              {/* Rating line — one measured point per pull */}
              {ratingData.length > 0 && (
                <g>
                  <path d={ratingPath} fill="none" stroke={K_RATING} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  {ratingData.map((p,i)=>(
                    <g key={i} style={{ cursor:"pointer" }}
                       onMouseEnter={()=>setHover({ chart:"rc", label:p.label, value:`${p.val.toFixed(1)}★`, x:xOf(p.date), y:ratingY(p.val) })}
                       onMouseLeave={()=>setHover(null)}>
                      <circle cx={xOf(p.date)} cy={ratingY(p.val)} r={dotR} fill={K_RATING} stroke="#fff" strokeWidth={dotR>2?1.5:0}/>
                      <circle cx={xOf(p.date)} cy={ratingY(p.val)} r="9" fill="transparent"/>
                    </g>
                  ))}
                </g>
              )}
              {/* Rating Count — measured, not extrapolated */}
              {rcData.length > 1 && (
                <g>
                  <path d={rcPath} fill="none" stroke={K_RC} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  {rcData.map((p,i)=>(
                    <g key={i} style={{ cursor:"pointer" }}
                       onMouseEnter={()=>setHover({ chart:"rc", label:p.label, value:`${p.val.toLocaleString("en-IN")} ratings`, x:xOf(p.date), y:rcY(p.val) })}
                       onMouseLeave={()=>setHover(null)}>
                      <circle cx={xOf(p.date)} cy={rcY(p.val)} r={dotR} fill={K_RC} stroke="#fff" strokeWidth={dotR>2?1.5:0}/>
                      <circle cx={xOf(p.date)} cy={rcY(p.val)} r="10" fill="transparent"/>
                    </g>
                  ))}
                </g>
              )}
              {/* X labels — real dates */}
              {ticks.map((d,i)=>(
                <text key={i} x={xOf(d)} y={ratingH-8} textAnchor={i===0?"start":i===ticks.length-1?"end":"middle"} fontSize="10" fill="#6B7280" fontFamily="'DM Mono',monospace">{formatDateShort(d)}</text>
              ))}
              {/* Tooltip for rating count */}
              {hover && hover.chart==="rc" && (
                <g>
                  <rect x={hover.x-55} y={hover.y-40} width={110} height={30} rx={5} fill="#111827" opacity="0.95"/>
                  <text x={hover.x} y={hover.y-24} textAnchor="middle" fontSize="9" fill="#9CA3AF">{hover.label}</text>
                  <text x={hover.x} y={hover.y-12} textAnchor="middle" fontSize="11" fill="#fff" fontWeight="700" fontFamily="'DM Mono',monospace">{hover.value}</text>
                </g>
              )}
              {/* Coverage note — every point below is a measured pull */}
              <text x={PAD_L} y={ratingH-2} fontSize="8" fill="#9CA3AF" fontStyle="italic">
                {rcData.length > 1 ? `Measured — ${rcData.length} pulls` : "Not enough history yet"}
              </text>
            </svg>
          </div>

          {/* ═══ Chart 3: Monthly Sold bars ═══ */}
          <div style={{ position:"relative", marginBottom:10 }}>
            <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#9CA3AF", fontWeight:700, padding:"2px 0 4px 6px", letterSpacing:0.5 }}>MONTHLY SOLD</div>
            <svg width="100%" viewBox={`0 0 ${W} ${salesH}`} style={{ display:"block", background:"#FAFAFA", borderRadius:6, border:"1px solid #E5E7EB" }}>
              {/* Gridlines */}
              {[0, 0.5, 1].map((f,i)=>(
                <g key={i}>
                  <line x1={PAD_L} y1={PAD_T+f*(salesH-PAD_T-PAD_B)} x2={W-PAD_R} y2={PAD_T+f*(salesH-PAD_T-PAD_B)}
                    stroke="#E5E7EB" strokeWidth="1" strokeDasharray={i===0||i===2?"0":"3,3"}/>
                  <text x={PAD_L-6} y={PAD_T+f*(salesH-PAD_T-PAD_B)+3} textAnchor="end" fontSize="9" fill="#6B7280" fontFamily="'DM Mono',monospace">
                    {Math.round((1-f)*salesMax).toLocaleString("en-IN")}
                  </text>
                </g>
              ))}
              {/* Bar for current monthly sold */}
              {monthlyVal > 0 ? (
                <g>
                  <rect x={PAD_L + plotW*0.35} y={salesY(monthlyVal)} width={plotW*0.3}
                    height={salesH - PAD_B - salesY(monthlyVal)}
                    fill={K_SOLD} rx="3"/>
                  <text x={PAD_L + plotW*0.5} y={salesY(monthlyVal)-6} textAnchor="middle" fontSize="12" fill="#374151" fontWeight="700" fontFamily="'DM Mono',monospace">
                    {monthlyVal}+ units/mo
                  </text>
                </g>
              ) : (
                <text x={W/2} y={salesH/2} textAnchor="middle" fontSize="11" fill="#9CA3AF" fontStyle="italic">No monthly sales data available</text>
              )}
              {/* Drops indicator */}
              {row.drops_30d > 0 && (
                <g>
                  <text x={W-PAD_R-6} y={PAD_T+10} textAnchor="end" fontSize="10" fill="#DC2626" fontWeight="700" fontFamily="'DM Mono',monospace">
                    ↓ {row.drops_30d} drops in last 30d
                  </text>
                </g>
              )}
              <text x={PAD_L + plotW*0.5} y={salesH-8} textAnchor="middle" fontSize="10" fill="#6B7280" fontFamily="'DM Mono',monospace">Current month</text>
            </svg>
          </div>
        </div>

        {/* ── Keepa-style data strip at bottom ── */}
        <div style={{ padding:"10px 18px", background:"#F9FAFB", borderTop:`1px solid #E5E7EB`, display:"flex", alignItems:"center", gap:16, flexWrap:"wrap" }}>
          <div style={{ display:"flex", alignItems:"center", gap:0 }}>
            <div style={{ width:8, height:8, borderRadius:"50%", background:K_BSR, marginRight:7 }} />
            <span style={{ fontSize:10, color:"#6B7280", fontWeight:700, marginRight:10 }}>SALES RANK</span>
            {[["Current",row.bsr],["30d avg",row.bsr_30d],["90d avg",row.bsr_90d],["365d avg",row.bsr_365d]].map(([l,v],i)=>(
              <div key={l} style={{ marginRight:16 }}>
                <div style={{ fontSize:13, fontWeight:800, color:i===0?tier.color:"#374151", fontFamily:"'DM Mono',monospace" }}>
                  {v?`#${v.toLocaleString("en-IN")}`:"—"}
                </div>
                <div style={{ fontSize:9, color:"#9CA3AF", textTransform:"uppercase", letterSpacing:0.5 }}>{l}</div>
              </div>
            ))}
          </div>
          <div style={{ width:"1px", background:"#E5E7EB", alignSelf:"stretch" }} />
          <div style={{ fontSize:11, fontWeight:700, color:trendColor }}>{trendLabel}</div>
          <div style={{ width:"1px", background:"#E5E7EB", alignSelf:"stretch" }} />
          <span style={{ fontSize:11, color:K_BSR, fontWeight:700, fontFamily:"'DM Mono',monospace" }}>↓ {row.drops_30d || 0} drops/mo</span>
        </div>

        {/* ── Bottom: Rating · Rating Count · Monthly sold · Actions ── */}
        <div style={{ padding:"12px 18px", display:"flex", gap:20, alignItems:"center", background:"#fff", borderTop:`1px solid #E5E7EB`, flexWrap:"wrap" }}>
          <div>
            <div style={{ fontSize:9, color:"#9CA3AF", marginBottom:3, textTransform:"uppercase", letterSpacing:0.5, fontWeight:700 }}>Rating</div>
            <div style={{ display:"flex", alignItems:"center", gap:3 }}>
              {[1,2,3,4,5].map(s=>(
                <div key={s} style={{ width:12, height:12, background:s<=stars?"#F59E0B":"#E5E7EB", clipPath:"polygon(50% 0%,61% 35%,98% 35%,68% 57%,79% 91%,50% 70%,21% 91%,32% 57%,2% 35%,39% 35%)" }} />
              ))}
              <span style={{ fontSize:13, fontWeight:700, color:"#111827", marginLeft:5 }}>{row.rating||"—"}</span>
            </div>
          </div>
          <div style={{ width:"1px", background:"#E5E7EB", alignSelf:"stretch" }} />
          <div>
            <div style={{ fontSize:9, color:"#9CA3AF", marginBottom:3, textTransform:"uppercase", letterSpacing:0.5, fontWeight:700 }}>Rating Count</div>
            <div style={{ fontSize:13, fontWeight:700, color:"#111827", fontFamily:"'DM Mono',monospace" }}>{row.reviews?.toLocaleString("en-IN")||"—"}</div>
          </div>
          <div style={{ width:"1px", background:"#E5E7EB", alignSelf:"stretch" }} />
          <div>
            <div style={{ fontSize:9, color:"#9CA3AF", marginBottom:3, textTransform:"uppercase", letterSpacing:0.5, fontWeight:700 }}>Monthly Sold</div>
            <div style={{ fontSize:13, fontWeight:700, color:"#111827", fontFamily:"'DM Mono',monospace" }}>{row.monthly_sold?`${row.monthly_sold}+`:"—"}</div>
          </div>
          <div style={{ marginLeft:"auto", display:"flex", gap:8 }}>
            <a href={`https://keepa.com/#!product/12-${row.asin}`} target="_blank" rel="noreferrer"
              style={{ background:"#F3F4F6", color:"#374151", borderRadius:8, padding:"7px 14px", fontSize:11, fontWeight:700, textDecoration:"none", border:"1px solid #E5E7EB" }}>
              Keepa →
            </a>
            <a href={`https://www.amazon.in/dp/${row.asin}`} target="_blank" rel="noreferrer"
              style={{ background:bc.accent, color:"#fff", borderRadius:8, padding:"7px 14px", fontSize:11, fontWeight:700, textDecoration:"none" }}>
              View on Amazon →
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// BsrFullPage — Keepa-style full-page chart view (opens on double-click)
// ═══════════════════════════════════════════════════════════════════════
function BsrFullPage({ row, onBack }) {
  const bc = BRAND_META[row.brand] || { accent:"#6B7280", light:"#F9FAFB", text:"#4B5563" };
  const tier = getBsrTier(row.bsr);
  const trendColor = row.trend==="up"?"#059669":row.trend==="down"?"#DC2626":"#6B7280";
  const trendLabel = row.trend==="up"?"↑ Improving":row.trend==="down"?"↓ Declining":"→ Stable";
  const stars = row.rating ? Math.round(row.rating) : 0;

  // Hover crosshair state (index of x-position user is hovering)
  const [hoverIdx, setHoverIdx] = useState(null);
  const [rangeMode, setRangeMode] = useState("all"); // day/week/month/3m/year/all
  const [visible, setVisible] = useState({ bsr:true, rating:true, rc:true, sold:true });

  // ── Build data series from the real pulls (see buildAsinHistory) ──
  const hist = useMemo(() => buildAsinHistory(row.asin, row.brand), [row.asin, row.brand]);
  const allPoints = hist.map(h => ({
    date: h.date,
    label: formatDateShort(h.date),
    xLabel: formatDateShort(h.date),
    val_bsr: h.bsr, val_rating: h.rating, val_rc: h.reviews, val_sold: h.monthly_sold,
  }));
  // The range buttons now cut on real elapsed days instead of on array index,
  // which is what they always looked like they did.
  const RANGE_DAYS = { day:1, week:7, month:30, "3m":90, year:365, all:null };
  const lastT = allPoints.length ? new Date(allPoints[allPoints.length-1].date + "T00:00:00").getTime() : 0;
  const cutDays = RANGE_DAYS[rangeMode];
  const points = cutDays == null ? allPoints : allPoints.filter(pt => {
    const days = (lastT - new Date(pt.date + "T00:00:00").getTime()) / 86400000;
    return days <= cutDays;
  });

  // Chart geometry
  const W = 1120, plotL = 80, plotR = 80;
  const chartW = W - plotL - plotR;
  const H_BSR = 240, H_MID = 190, H_SOLD = 150;
  const PAD_T = 20, PAD_B = 28;

  // X positions — by real date, so an unpulled day leaves a real gap
  const tAt = i => new Date(points[i].date + "T00:00:00").getTime();
  const t0 = points.length ? tAt(0) : 0;
  const tW = points.length ? (tAt(points.length-1) - t0) || 1 : 1;
  const xAt = i => points.length > 1 ? plotL + ((tAt(i) - t0) / tW) * chartW : plotL + chartW / 2;
  // Only a handful of the dates get printed, else 76 labels overlap into mush.
  const tickIdx = points.length <= 1 ? points.map((_,i)=>i)
    : Array.from({ length: Math.min(6, points.length) }, (_, k) =>
        Math.round(k * (points.length - 1) / (Math.min(6, points.length) - 1)));
  const dotR = points.length > 24 ? 1.8 : 4;

  // ── BSR series ──
  const bsrVals = points.map(p => p.val_bsr).filter(v => v);
  const bsrMax = bsrVals.length ? Math.max(...bsrVals) : 1;
  const bsrMin = bsrVals.length ? Math.min(...bsrVals) : 0;
  const bsrSpan = (bsrMax - bsrMin) || 1;
  const bsrY = v => PAD_T + ((v - bsrMin) / bsrSpan) * (H_BSR - PAD_T - PAD_B);
  const bsrPath = points.filter(p=>p.val_bsr).map((p,i)=>{
    const realI = points.indexOf(p);
    return `${i===0?"M":"L"}${xAt(realI)},${bsrY(p.val_bsr)}`;
  }).join(" ");

  // ── Rating count series ──
  const rcVals = points.map(p => p.val_rc).filter(v => v);
  const rcMax = rcVals.length ? Math.max(...rcVals) : 1;
  const rcMin = rcVals.length ? Math.min(...rcVals) : 0;
  const rcSpan = (rcMax - rcMin) || 1;
  const rcY = v => PAD_T + (1 - (v - rcMin) / rcSpan) * (H_MID - PAD_T - PAD_B);
  const rcPath = points.filter(p=>p.val_rc).map((p,i)=>{
    const realI = points.indexOf(p);
    return `${i===0?"M":"L"}${xAt(realI)},${rcY(p.val_rc)}`;
  }).join(" ");
  const ratingYFn = v => PAD_T + ((5 - v) / 4) * (H_MID - PAD_T - PAD_B);
  const ratingPath = points.filter(p=>p.val_rating).map((p,i)=>{
    const realI = points.indexOf(p);
    return `${i===0?"M":"L"}${xAt(realI)},${ratingYFn(p.val_rating)}`;
  }).join(" ");

  // ── Sold bars ──
  const soldMax = Math.max((row.monthly_sold || 0) * 1.2, 10);
  const soldY = v => PAD_T + (1 - v / soldMax) * (H_SOLD - PAD_T - PAD_B);

  // Keepa palette
  const K_BSR    = "#2E9442";
  const K_RATING = "#17A6A6";
  const K_RC     = "#9ACD32";
  const K_SOLD   = "#E8A33D";

  // Format helpers
  const fmtRank = v => v ? `#${v.toLocaleString("en-IN")}` : "—";
  const fmtNum  = v => v ? v.toLocaleString("en-IN") : "—";

  // Generic chart shell (gridlines + hover crosshair + x axis)
  const ChartShell = ({ height, yLabelLeft, yLabelRight, yTicksLeft=5, yTicksRight=3, minLeft, maxLeft, colorLeft, minRight=0, maxRight, colorRight, children, crosshairData }) => {
    const h = height;
    return (
      <svg width="100%" viewBox={`0 0 ${W} ${h}`} preserveAspectRatio="none"
        style={{ display:"block", background:"#fff", border:"1px solid #E5E7EB" }}
        onMouseMove={e => {
          const svg = e.currentTarget;
          const pt = svg.getBoundingClientRect();
          const x = ((e.clientX - pt.left) / pt.width) * W;
          if (x < plotL || x > W - plotR) { setHoverIdx(null); return; }
          // Nearest point by pixel distance -- points are no longer evenly spaced.
          let idx = 0;
          for (let i = 1; i < points.length; i++) {
            if (Math.abs(xAt(i) - x) < Math.abs(xAt(idx) - x)) idx = i;
          }
          setHoverIdx(points.length ? idx : null);
        }}
        onMouseLeave={()=>setHoverIdx(null)}>
        {/* Horizontal gridlines */}
        {Array.from({length:yTicksLeft},(_,i)=>i).map(i=>{
          const f = i/(yTicksLeft-1);
          const y = PAD_T + f * (h - PAD_T - PAD_B);
          const val = minLeft !== undefined ? Math.round(minLeft + (maxLeft - minLeft) * f) : null;
          return (
            <g key={`gl-${i}`}>
              <line x1={plotL} y1={y} x2={W-plotR} y2={y} stroke="#F3F4F6" strokeWidth="1" strokeDasharray={i===0||i===yTicksLeft-1?"0":"2,3"}/>
              {val !== null && <text x={plotL-8} y={y+3} textAnchor="end" fontSize="10" fill={colorLeft} fontFamily="'DM Mono',monospace" fontWeight="600">{yLabelLeft ? yLabelLeft(val, f) : val}</text>}
            </g>
          );
        })}
        {/* Right axis labels */}
        {maxRight !== undefined && Array.from({length:yTicksRight},(_,i)=>i).map(i=>{
          const f = i/(yTicksRight-1);
          const y = PAD_T + f * (h - PAD_T - PAD_B);
          const val = Math.round(minRight + (maxRight - minRight) * (1-f));
          return <text key={`gr-${i}`} x={W-plotR+8} y={y+3} textAnchor="start" fontSize="10" fill={colorRight} fontFamily="'DM Mono',monospace" fontWeight="600">{yLabelRight ? yLabelRight(val) : val}</text>;
        })}
        {/* Vertical gridlines at the labelled dates */}
        {tickIdx.map(i=>(
          <line key={`vg-${i}`} x1={xAt(i)} y1={PAD_T} x2={xAt(i)} y2={h-PAD_B} stroke="#F3F4F6" strokeWidth="1"/>
        ))}
        {children}
        {/* X axis labels — real dates, thinned to fit */}
        {tickIdx.map((i,k)=>(
          <text key={`x-${i}`} x={xAt(i)} y={h-10} textAnchor={k===0?"start":k===tickIdx.length-1?"end":"middle"} fontSize="11" fill="#6B7280" fontFamily="'DM Mono',monospace">{points[i].label}</text>
        ))}
        {/* Crosshair */}
        {hoverIdx !== null && (
          <g pointerEvents="none">
            <line x1={xAt(hoverIdx)} y1={PAD_T} x2={xAt(hoverIdx)} y2={h-PAD_B} stroke="#9CA3AF" strokeWidth="1" strokeDasharray="4,3"/>
          </g>
        )}
      </svg>
    );
  };

  const hoveredPoint = hoverIdx !== null ? points[hoverIdx] : null;

  return (
    <div style={{ fontFamily:"'Inter','Segoe UI',sans-serif", background:"#F5F5F5", minHeight:"100vh" }}>

      {/* Top bar — ASIN info + back */}
      <div style={{ background:"#fff", borderBottom:"1px solid #E5E7EB", padding:"12px 24px", display:"flex", alignItems:"center", gap:12, flexWrap:"wrap" }}>
        <button onClick={onBack} style={{ background:"#F3F4F6", color:"#374151", border:"1px solid #E5E7EB", borderRadius:8, padding:"7px 14px", fontSize:12, fontWeight:700, cursor:"pointer" }}>← Back to Table</button>
        <a href={`https://www.amazon.in/dp/${row.asin}`} target="_blank" rel="noreferrer"
          style={{ fontFamily:"'DM Mono',monospace", fontSize:15, fontWeight:700, color:"#2563EB", textDecoration:"none" }}>
          {row.asin}
        </a>
        <span style={{ background:bc.light, color:bc.text, borderRadius:20, padding:"3px 11px", fontSize:11, fontWeight:700 }}>{row.brand}</span>
        <span style={{ background:tier.bg, color:tier.color, borderRadius:20, padding:"3px 10px", fontSize:11, fontWeight:700 }}>{tier.emoji} {tier.label}</span>
        {row.model && row.model !== "—" && <span style={{ fontSize:12, color:"#6B7280" }}>Model: <strong style={{ color:"#111827" }}>{row.model}</strong></span>}
        {row.catL0 && row.catL0 !== "—" && <span style={{ fontSize:12, color:"#6B7280" }}>L0: <strong style={{ color:"#111827" }}>{row.catL0}</strong></span>}
        {row.catL1 && row.catL1 !== "—" && <span style={{ fontSize:12, color:"#6B7280" }}>L1: <strong style={{ color:"#111827" }}>{row.catL1}</strong></span>}
        <div style={{ marginLeft:"auto", display:"flex", gap:8 }}>
          <a href={`https://keepa.com/#!product/12-${row.asin}`} target="_blank" rel="noreferrer"
            style={{ background:"#F3F4F6", color:"#374151", borderRadius:8, padding:"7px 14px", fontSize:12, fontWeight:700, textDecoration:"none", border:"1px solid #E5E7EB" }}>
            🔗 Open in Keepa
          </a>
          <a href={`https://www.amazon.in/dp/${row.asin}`} target="_blank" rel="noreferrer"
            style={{ background:bc.accent, color:"#fff", borderRadius:8, padding:"7px 14px", fontSize:12, fontWeight:700, textDecoration:"none" }}>
            View on Amazon →
          </a>
        </div>
      </div>

      {/* Main content: charts (left) + legend sidebar (right) */}
      <div style={{ display:"grid", gridTemplateColumns:"1fr 260px", gap:16, padding:16 }}>

        {/* Charts column */}
        <div style={{ background:"#fff", borderRadius:10, border:"1px solid #E5E7EB", padding:"14px 16px" }}>

          {/* Page title + range selector + hover indicator */}
          <div style={{ display:"flex", alignItems:"center", gap:12, marginBottom:12, flexWrap:"wrap" }}>
            <div style={{ fontSize:14, fontWeight:700, color:"#111827" }}>Keepa-style Chart View</div>
            <span style={{ fontSize:11, color:"#6B7280" }}>Hover anywhere on the charts to see values</span>
            <div style={{ marginLeft:"auto", display:"flex", gap:4, background:"#F9FAFB", borderRadius:8, padding:4, border:"1px solid #E5E7EB" }}>
              {[
                { k:"day",   l:"Day" },
                { k:"week",  l:"Week" },
                { k:"month", l:"Month" },
                { k:"3m",    l:"3 Months" },
                { k:"year",  l:"Year" },
                { k:"all",   l:"All" },
              ].map(r => (
                <button key={r.k} onClick={()=>setRangeMode(r.k)}
                  style={{ padding:"4px 10px", fontSize:10, fontWeight:700, borderRadius:6, cursor:"pointer",
                           background: rangeMode === r.k ? "#2563EB" : "transparent",
                           color: rangeMode === r.k ? "#fff" : "#6B7280",
                           border: "none" }}>
                  {r.l}
                </button>
              ))}
            </div>
          </div>

          {/* ══ CHART 1: Sales Rank ══ */}
          <div style={{ marginBottom:4 }}>
            <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#9CA3AF", fontWeight:700, padding:"0 0 4px 6px", letterSpacing:0.5, display:"flex", alignItems:"center", gap:8 }}>
              <span style={{ width:10, height:10, borderRadius:"50%", background:K_BSR, display:"inline-block" }}/>
              SALES RANK
              <span style={{ color:"#6B7280", marginLeft:4 }}>(lower is better)</span>
            </div>
            <ChartShell height={H_BSR}
              minLeft={bsrMin} maxLeft={bsrMax} colorLeft="#6B7280"
              yTicksLeft={6}
              yLabelLeft={(v, f) => fmtRank(Math.round(bsrMin + (bsrMax-bsrMin)*f))}>
              {visible.bsr && (
                <>
                  <defs>
                    <linearGradient id="bsrFillFP" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%"   stopColor={K_BSR} stopOpacity="0.22"/>
                      <stop offset="100%" stopColor={K_BSR} stopOpacity="0.02"/>
                    </linearGradient>
                  </defs>
                  {/* Area */}
                  {bsrVals.length > 1 && (() => {
                    const pts = points.filter(p=>p.val_bsr);
                    const first = points.indexOf(pts[0]);
                    const last = points.indexOf(pts[pts.length-1]);
                    return <path d={`${bsrPath} L${xAt(last)},${H_BSR-PAD_B} L${xAt(first)},${H_BSR-PAD_B} Z`} fill="url(#bsrFillFP)"/>;
                  })()}
                  <path d={bsrPath} fill="none" stroke={K_BSR} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
                  {points.map((p,i)=>p.val_bsr && (
                    <circle key={i} cx={xAt(i)} cy={bsrY(p.val_bsr)} r={hoverIdx===i?6:dotR} fill={K_BSR} stroke="#fff" strokeWidth={dotR>2||hoverIdx===i?2:0}/>
                  ))}
                </>
              )}
            </ChartShell>
          </div>

          {/* ══ CHART 2: Rating (left axis) + Rating Count (right axis) ══ */}
          <div style={{ marginTop:10, marginBottom:4 }}>
            <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#9CA3AF", fontWeight:700, padding:"0 0 4px 6px", letterSpacing:0.5, display:"flex", alignItems:"center", gap:12 }}>
              <span style={{ display:"flex", alignItems:"center", gap:5 }}>
                <span style={{ width:10, height:10, borderRadius:"50%", background:K_RATING, display:"inline-block" }}/>
                RATING <span style={{ color:"#6B7280" }}>(1–5★, left axis)</span>
              </span>
              <span style={{ display:"flex", alignItems:"center", gap:5 }}>
                <span style={{ width:10, height:10, borderRadius:"50%", background:K_RC, display:"inline-block" }}/>
                RATING COUNT <span style={{ color:"#6B7280" }}>(right axis, measured)</span>
              </span>
            </div>
            <ChartShell height={H_MID}
              minLeft={1} maxLeft={5} colorLeft={K_RATING} yTicksLeft={5}
              yLabelLeft={(v)=>`${v}★`}
              minRight={rcMin} maxRight={rcMax} colorRight={K_RC} yTicksRight={3}
              yLabelRight={(v)=>fmtNum(v)}>
              {/* Rating — measured per pull */}
              {visible.rating && ratingPath && (
                <g>
                  <path d={ratingPath} fill="none" stroke={K_RATING} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
                  {points.map((p,i)=> p.val_rating ? (
                    <circle key={i} cx={xAt(i)} cy={ratingYFn(p.val_rating)} r={hoverIdx===i?6:dotR} fill={K_RATING} stroke="#fff" strokeWidth={dotR>2||hoverIdx===i?2:0}/>
                  ) : null)}
                </g>
              )}
              {/* Rating Count trend */}
              {visible.rc && rcVals.length > 1 && (
                <g>
                  <path d={rcPath} fill="none" stroke={K_RC} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
                  {points.map((p,i)=>p.val_rc && (
                    <circle key={i} cx={xAt(i)} cy={rcY(p.val_rc)} r={hoverIdx===i?6:dotR} fill={K_RC} stroke="#fff" strokeWidth={dotR>2||hoverIdx===i?2:0}/>
                  ))}
                </g>
              )}
            </ChartShell>
          </div>

          {/* ══ CHART 3: Monthly Sold ══ */}
          <div style={{ marginTop:10 }}>
            <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#9CA3AF", fontWeight:700, padding:"0 0 4px 6px", letterSpacing:0.5, display:"flex", alignItems:"center", gap:8 }}>
              <span style={{ width:10, height:10, borderRadius:2, background:K_SOLD, display:"inline-block" }}/>
              MONTHLY SOLD
              {row.drops_30d ? <span style={{ marginLeft:"auto", color:"#DC2626", fontWeight:700 }}>↓ {row.drops_30d} drops in last 30d</span> : null}
            </div>
            <ChartShell height={H_SOLD}
              minLeft={0} maxLeft={soldMax} colorLeft="#6B7280" yTicksLeft={4}
              yLabelLeft={(v)=>fmtNum(v)}>
              {visible.sold && row.monthly_sold && (
                <g>
                  <rect
                    x={xAt(points.length-1) - 30}
                    y={soldY(row.monthly_sold)}
                    width={60}
                    height={H_SOLD - PAD_B - soldY(row.monthly_sold)}
                    fill={K_SOLD} rx="3"/>
                  <text x={xAt(points.length-1)} y={soldY(row.monthly_sold)-7} textAnchor="middle" fontSize="13" fill="#374151" fontWeight="700" fontFamily="'DM Mono',monospace">
                    {row.monthly_sold}+
                  </text>
                </g>
              )}
              {!row.monthly_sold && (
                <text x={W/2} y={H_SOLD/2} textAnchor="middle" fontSize="12" fill="#9CA3AF" fontStyle="italic">No monthly sales data available</text>
              )}
            </ChartShell>
          </div>

          {/* Keepa-style stat line under charts */}
          <div style={{ marginTop:14, padding:"10px 14px", background:"#F9FAFB", border:"1px solid #E5E7EB", borderRadius:8, display:"flex", alignItems:"center", gap:18, flexWrap:"wrap", fontSize:11 }}>
            <div style={{ display:"flex", alignItems:"center", gap:5 }}>
              <span style={{ width:8, height:8, borderRadius:"50%", background:K_BSR }}/>
              <span style={{ color:"#6B7280", fontWeight:700 }}>Sales Rank</span>
            </div>
            {[["Current",row.bsr],["30d avg",row.bsr_30d],["90d avg",row.bsr_90d],["365d avg",row.bsr_365d]].map(([l,v],i)=>(
              <div key={l}>
                <div style={{ fontSize:13, fontWeight:800, color:i===0?tier.color:"#374151", fontFamily:"'DM Mono',monospace" }}>
                  {fmtRank(v)}
                </div>
                <div style={{ fontSize:9, color:"#9CA3AF", textTransform:"uppercase", letterSpacing:0.5, fontWeight:600 }}>{l}</div>
              </div>
            ))}
            <div style={{ width:"1px", background:"#E5E7EB", alignSelf:"stretch" }}/>
            <span style={{ fontSize:11, fontWeight:700, color:trendColor }}>{trendLabel}</span>
          </div>
        </div>

        {/* ══════════════ Right Sidebar (Keepa-style) ══════════════ */}
        <div style={{ background:"#fff", borderRadius:10, border:"1px solid #E5E7EB", padding:"14px 14px", height:"fit-content", position:"sticky", top:16 }}>

          {/* Legend toggles */}
          <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#6B7280", fontWeight:700, letterSpacing:0.8, marginBottom:10 }}>CHART SERIES</div>
          {[
            { key:"bsr",    label:"Sales Rank",     color:K_BSR,    has:true },
            { key:"rating", label:"Rating",         color:K_RATING, has:!!row.rating },
            { key:"rc",     label:"Rating Count",   color:K_RC,     has:!!row.reviews },
            { key:"sold",   label:"Monthly Sold",   color:K_SOLD,   has:!!row.monthly_sold },
          ].map(item => (
            <div key={item.key} onClick={()=> item.has && setVisible(v => ({ ...v, [item.key]: !v[item.key] }))}
              style={{ display:"flex", alignItems:"center", gap:8, padding:"6px 4px", cursor: item.has ? "pointer" : "default", opacity: item.has ? 1 : 0.4, fontSize:12 }}>
              <span style={{ width:14, height:14, borderRadius:"50%", background: visible[item.key] && item.has ? item.color : "transparent", border:`2px solid ${item.color}` }}/>
              <span style={{ color: visible[item.key] && item.has ? "#111827" : "#9CA3AF", fontWeight:600, textDecoration: !visible[item.key] ? "line-through" : "none" }}>
                {item.label}
              </span>
              {!item.has && <span style={{ marginLeft:"auto", fontSize:9, color:"#DC2626", fontWeight:700 }}>NO DATA</span>}
            </div>
          ))}

          {/* Not available notice */}
          <div style={{ marginTop:14, padding:"10px 12px", background:"#FEF3C7", border:"1px solid #FCD34D", borderRadius:8, fontSize:10, color:"#92400E" }}>
            <div style={{ fontWeight:700, marginBottom:4 }}>ℹ Not in our data</div>
            <div style={{ lineHeight:1.5 }}>
              Buy Box price, offer counts, category sub-ranks, and true daily history aren't included in the Keepa CSV export. Click <strong>🔗 Open in Keepa</strong> above for the full chart.
            </div>
          </div>

          {/* Hover tooltip info panel */}
          <div style={{ marginTop:14, padding:"12px", background:"#F9FAFB", border:"1px solid #E5E7EB", borderRadius:8 }}>
            <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#6B7280", fontWeight:700, letterSpacing:0.8, marginBottom:8 }}>
              {hoveredPoint ? hoveredPoint.label.toUpperCase() : "HOVER A CHART"}
            </div>
            {hoveredPoint ? (
              <div style={{ display:"flex", flexDirection:"column", gap:6 }}>
                <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
                  <span style={{ display:"flex", alignItems:"center", gap:5, fontSize:11 }}>
                    <span style={{ width:8, height:8, borderRadius:"50%", background:K_BSR }}/>
                    Sales Rank
                  </span>
                  <span style={{ fontSize:12, fontWeight:700, fontFamily:"'DM Mono',monospace", color:"#111827" }}>
                    {fmtRank(hoveredPoint.val_bsr)}
                  </span>
                </div>
                <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
                  <span style={{ display:"flex", alignItems:"center", gap:5, fontSize:11 }}>
                    <span style={{ width:8, height:8, borderRadius:"50%", background:K_RATING }}/>
                    Rating
                  </span>
                  <span style={{ fontSize:12, fontWeight:700, fontFamily:"'DM Mono',monospace", color:"#111827" }}>
                    {hoveredPoint.val_rating ? `${hoveredPoint.val_rating}★` : "—"}
                  </span>
                </div>
                <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
                  <span style={{ display:"flex", alignItems:"center", gap:5, fontSize:11 }}>
                    <span style={{ width:8, height:8, borderRadius:"50%", background:K_RC }}/>
                    Rating Count
                  </span>
                  <span style={{ fontSize:12, fontWeight:700, fontFamily:"'DM Mono',monospace", color:"#111827" }}>
                    {fmtNum(hoveredPoint.val_rc)}
                  </span>
                </div>
                <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
                  <span style={{ display:"flex", alignItems:"center", gap:5, fontSize:11 }}>
                    <span style={{ width:8, height:8, borderRadius:2, background:K_SOLD }}/>
                    Monthly Sold
                  </span>
                  <span style={{ fontSize:12, fontWeight:700, fontFamily:"'DM Mono',monospace", color:"#111827" }}>
                    {hoveredPoint.val_sold ? `${hoveredPoint.val_sold}+` : "—"}
                  </span>
                </div>
              </div>
            ) : (
              <div style={{ fontSize:11, color:"#9CA3AF", fontStyle:"italic" }}>Move your cursor over a chart to see values at each snapshot.</div>
            )}
          </div>

          {/* Key stats summary */}
          <div style={{ marginTop:14, padding:"12px", background:"#F0FDF4", border:"1px solid #BBF7D0", borderRadius:8 }}>
            <div style={{ fontSize:10, fontFamily:"'DM Mono',monospace", color:"#059669", fontWeight:700, letterSpacing:0.8, marginBottom:8 }}>SUMMARY</div>
            <div style={{ display:"flex", flexDirection:"column", gap:8, fontSize:12 }}>
              <div style={{ display:"flex", justifyContent:"space-between" }}>
                <span style={{ color:"#374151" }}>Rating</span>
                <span style={{ display:"flex", alignItems:"center", gap:3 }}>
                  {[1,2,3,4,5].map(s=>(
                    <div key={s} style={{ width:10, height:10, background:s<=stars?"#F59E0B":"#E5E7EB", clipPath:"polygon(50% 0%,61% 35%,98% 35%,68% 57%,79% 91%,50% 70%,21% 91%,32% 57%,2% 35%,39% 35%)" }}/>
                  ))}
                  <span style={{ fontWeight:700, color:"#111827", marginLeft:4 }}>{row.rating||"—"}</span>
                </span>
              </div>
              <div style={{ display:"flex", justifyContent:"space-between" }}>
                <span style={{ color:"#374151" }}>Drops / mo</span>
                <span style={{ fontWeight:700, color:"#059669", fontFamily:"'DM Mono',monospace" }}>↓ {row.drops_30d || 0}</span>
              </div>
              <div style={{ display:"flex", justifyContent:"space-between" }}>
                <span style={{ color:"#374151" }}>FBA SKU</span>
                <span style={{ fontWeight:700, color:"#111827", fontFamily:"'DM Mono',monospace", fontSize:11 }}>{row.fbaSku || "—"}</span>
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
}

// ── Daily BSR snapshot storage helpers (localStorage-backed) ────────────────
const BSR_STORAGE_KEY = "bsr_daily_snapshots_v1";
const todayStr = () => {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
};
const formatDateShort = (iso) => {
  // "2026-04-23" -> "23 Apr"
  if (!iso) return "";
  const [, m, d] = iso.split("-");
  const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  return `${parseInt(d,10)} ${months[parseInt(m,10)-1]}`;
};
// Infer rank direction from the 30-day average (lower rank = better, so a
// current rank BELOW the average is improving). Lives here because the rows
// come from bsr_snapshots.json, which carries no `trend` field -- without this
// every row read as "Stable" and the Rising/Falling KPIs were stuck on 0.
const inferTrend = (bsr, bsr30) => {
  if (!bsr || !bsr30) return "stable";
  const diff = (bsr30 - bsr) / bsr30;
  if (diff > 0.1) return "up";
  if (diff < -0.1) return "down";
  return "stable";
};
// Load snapshots — PRIMARY source is baked-in BSR_SNAPSHOTS (from build_bsr_snapshots.py).
// This means every team member sees the same data automatically after each git push.
// localStorage is no longer used (upload buttons removed).
const loadSnapshots = () => {
  try {
    return BSR_SNAPSHOTS || {};
  } catch { return {}; }
};
// Kept as no-op stub so any remaining references don't crash; we never write snapshots anymore.
const saveSnapshots = (_snaps) => {};
// Detect brand from filename: "Audio Array_BSR.csv" -> "Audio Array"
const detectBrandFromFilename = (filename) => {
  const name = filename.toLowerCase();
  if (name.includes("audio")) return "Audio Array";
  if (name.includes("nexlev")) return "Nexlev";
  if (name.includes("tonor")) return "Tonor";
  if (name.includes("mulberry") || name.includes("white")) return "White Mulberry";
  return null;
};

// Extract ISO date (YYYY-MM-DD) from a filename. Handles multiple formats:
//   "2026-04-23.csv"     -> "2026-04-23"
//   "26-04-23.csv"       -> "2026-04-23"  (YY-MM-DD assumed 20YY)
//   "Nexlev_2026-04-23"  -> "2026-04-23"
//   "23-04-2026.csv"     -> "2026-04-23"  (DD-MM-YYYY)
// Returns null if no valid date found.
const extractDateFromFilename = (filename) => {
  if (!filename) return null;
  const name = filename.replace(/\.csv$/i, "");
  // Try YYYY-MM-DD
  let m = name.match(/(\d{4})[-_](\d{2})[-_](\d{2})/);
  if (m) {
    const [, y, mo, d] = m;
    if (+mo >= 1 && +mo <= 12 && +d >= 1 && +d <= 31) return `${y}-${mo}-${d}`;
  }
  // Try DD-MM-YYYY
  m = name.match(/(\d{2})[-_](\d{2})[-_](\d{4})/);
  if (m) {
    const [, d, mo, y] = m;
    if (+mo >= 1 && +mo <= 12 && +d >= 1 && +d <= 31) return `${y}-${mo}-${d}`;
  }
  // Try YY-MM-DD (2-digit year, e.g. "26-04-23" -> 2026-04-23)
  m = name.match(/(?:^|[^0-9])(\d{2})[-_](\d{2})[-_](\d{2})(?:$|[^0-9])/);
  if (m) {
    const [, y, mo, d] = m;
    if (+mo >= 1 && +mo <= 12 && +d >= 1 && +d <= 31) return `20${y}-${mo}-${d}`;
  }
  return null;
};

// ── ASIN → Title lookup built once from RAW_DATA (Nexlev has titles) ────────
const ASIN_TITLE_MAP = (() => {
  const map = {};
  try {
    RAW_DATA.forEach(r => {
      if (r.ASIN && r.Title && !map[r.ASIN]) map[r.ASIN] = r.Title;
    });
  } catch {}
  return map;
})();

// ── Compact Keepa-style hover popup: BSR + Rating + Reviews on one card ────
function BsrHoverChart({ row, x, y, title }) {
  if (!row) return null;

  // Card dimensions & positioning (flips to left/top edge if near window edge)
  const CARD_W = 380;
  const CARD_H = 260;
  const margin = 14;
  let left = x + margin;
  let top  = y + margin;
  if (typeof window !== "undefined") {
    if (left + CARD_W > window.innerWidth - 8)  left = Math.max(8, x - CARD_W - margin);
    if (top  + CARD_H > window.innerHeight - 8) top  = Math.max(8, y - CARD_H - margin);
  }

  // Chart geometry
  const W = CARD_W, PAD_L = 38, PAD_R = 12, PAD_T = 8, PAD_B = 16;
  const H1 = 80;  // BSR panel
  const H2 = 48;  // Rating panel
  const H3 = 48;  // Reviews panel
  const plotW = W - PAD_L - PAD_R;

  // Real pulls, oldest → newest. No useMemo: this component returns early
  // above, so a hook here would be conditional -- and buildAsinHistory caches.
  const pts = buildAsinHistory(row.asin, row.brand).map(h => ({
    label: formatDateShort(h.date), date: h.date, bsr: h.bsr, rating: h.rating, rc: h.reviews,
  }));

  const tOf = d => new Date(d + "T00:00:00").getTime();
  const t0 = pts.length ? tOf(pts[0].date) : 0;
  const tW = pts.length ? (tOf(pts[pts.length-1].date) - t0) || 1 : 1;
  const xAt = i => pts.length <= 1 ? PAD_L + plotW/2 : PAD_L + ((tOf(pts[i].date) - t0) / tW) * plotW;
  // 380px of card cannot carry 76 dots or 76 labels — show the line, and mark
  // only the ends.
  const showDots = pts.length <= 12;
  const endIdx = pts.length ? [0, pts.length - 1] : [];

  // BSR (inverted: lower rank = higher on chart like Keepa)
  const bsrVals = pts.map(p => p.bsr).filter(v => v != null && v > 0);
  const bsrMax = bsrVals.length ? Math.max(...bsrVals) : 1;
  const bsrMin = bsrVals.length ? Math.min(...bsrVals) : 0;
  const bsrSpan = (bsrMax - bsrMin) || 1;
  const bsrY = v => PAD_T + ((v - bsrMin) / bsrSpan) * (H1 - PAD_T - PAD_B);
  const bsrPath = pts.map((p,i) => p.bsr ? `${i===0||!pts[i-1]?.bsr?"M":"L"}${xAt(i)},${bsrY(p.bsr)}` : "").filter(Boolean).join(" ");

  // Rating (1-5 scale, fixed)
  const ratingY = v => PAD_T + ((5 - v) / 4) * (H2 - PAD_T - PAD_B);
  const ratingPath = pts.map((p,i) => p.rating ? `${i===0||!pts[i-1]?.rating?"M":"L"}${xAt(i)},${ratingY(p.rating)}` : "").filter(Boolean).join(" ");

  // Reviews — measured per pull, scaled min..max so small moves stay visible
  const rcVals = pts.map(p => p.rc).filter(v => v != null);
  const rcMax = rcVals.length ? Math.max(...rcVals) : 1;
  const rcMin = rcVals.length ? Math.min(...rcVals) : 0;
  const rcSpan = (rcMax - rcMin) || 1;
  const rcY = v => PAD_T + (1 - (v - rcMin) / rcSpan) * (H3 - PAD_T - PAD_B);
  const rcPath = rcVals.length ? pts.map((p,i) => p.rc ? `${i===0||!pts[i-1]?.rc?"M":"L"}${xAt(i)},${rcY(p.rc)}` : "").filter(Boolean).join(" ") : "";

  // Keepa palette
  const K_BSR    = "#2E9442";
  const K_RATING = "#17A6A6";
  const K_RC     = "#9ACD32";
  const BG_GRID  = "#F3F4F6";
  const TEXT_DIM = "#6B7280";

  const fmt = (v, p="#") => v ? `${p}${v.toLocaleString("en-IN")}` : "—";

  return (
    <div style={{
      position:"fixed", left, top, zIndex:9998,
      width:CARD_W,
      background:"#fff", border:"1px solid #D1D5DB", borderRadius:10,
      boxShadow:"0 16px 40px rgba(15,23,42,0.18), 0 2px 6px rgba(15,23,42,0.08)",
      fontFamily:"'Inter','Segoe UI',sans-serif",
      pointerEvents:"none",
      overflow:"hidden",
    }}>
      {/* Title strip */}
      <div style={{ padding:"7px 12px", borderBottom:"1px solid #E5E7EB", background:"#F9FAFB", display:"flex", gap:8, alignItems:"center" }}>
        {row.image ? (
          <img src={row.image} alt="" loading="lazy"
            onError={(e) => { e.currentTarget.style.display = "none"; }}
            style={{ width:36, height:36, objectFit:"contain", borderRadius:4, background:"#fff", border:"1px solid #E5E7EB", flexShrink:0 }} />
        ) : null}
        <div style={{ minWidth:0, flex:1 }}>
          <div style={{ fontSize:11, fontWeight:700, color:"#111827", overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}
            title={title || row.title || row.asin}>
            {title || row.title || row.asin}
          </div>
          <div style={{ fontSize:9, color:TEXT_DIM, fontFamily:"'DM Mono',monospace", marginTop:1 }}>
            {row.asin} · {row.brand}
          </div>
        </div>
      </div>

      {/* Legend row */}
      <div style={{ display:"flex", gap:10, padding:"6px 12px", background:"#fff", fontSize:9, fontFamily:"'DM Mono',monospace", fontWeight:700 }}>
        <span style={{ color:K_BSR }}>● Sales Rank {fmt(row.bsr)}</span>
        <span style={{ color:K_RATING }}>● ★ {row.rating ?? "—"}</span>
        <span style={{ color:K_RC }}>● Reviews {row.reviews?.toLocaleString("en-IN") ?? "—"}</span>
      </div>

      {/* ── BSR panel ── */}
      <svg width={W} height={H1} style={{ display:"block" }}>
        <rect x={PAD_L} y={PAD_T} width={plotW} height={H1-PAD_T-PAD_B} fill={BG_GRID} rx={2} />
        {/* y-axis label: rank */}
        <text x={6} y={PAD_T+10} fontSize={8} fontFamily="DM Mono,monospace" fill={TEXT_DIM}>#{bsrMin || "—"}</text>
        <text x={6} y={H1-PAD_B} fontSize={8} fontFamily="DM Mono,monospace" fill={TEXT_DIM}>#{bsrMax || "—"}</text>
        {/* line */}
        {bsrPath && <path d={bsrPath} fill="none" stroke={K_BSR} strokeWidth={2} />}
        {/* dots — only when the series is short enough to read */}
        {showDots && pts.map((p,i) => p.bsr ? <circle key={i} cx={xAt(i)} cy={bsrY(p.bsr)} r={2.5} fill={K_BSR} /> : null)}
        {/* x labels — first and last real date */}
        {endIdx.map((i,k) => (
          <text key={i} x={xAt(i)} y={H1-3} fontSize={8} fontFamily="DM Mono,monospace" fill={TEXT_DIM}
            textAnchor={pts.length <= 1 ? "middle" : k===0 ? "start" : "end"}>{pts[i].label}</text>
        ))}
      </svg>

      {/* ── Rating panel ── */}
      <svg width={W} height={H2} style={{ display:"block", borderTop:"1px dashed #E5E7EB" }}>
        <text x={6} y={PAD_T+8} fontSize={8} fontFamily="DM Mono,monospace" fill={TEXT_DIM}>5★</text>
        <text x={6} y={H2-PAD_B+2} fontSize={8} fontFamily="DM Mono,monospace" fill={TEXT_DIM}>1★</text>
        {ratingPath && <path d={ratingPath} fill="none" stroke={K_RATING} strokeWidth={2} />}
        {showDots && pts.map((p,i) => p.rating ? <circle key={i} cx={xAt(i)} cy={ratingY(p.rating)} r={2.5} fill={K_RATING} /> : null)}
      </svg>

      {/* ── Reviews panel ── */}
      <svg width={W} height={H3} style={{ display:"block", borderTop:"1px dashed #E5E7EB" }}>
        <text x={6} y={PAD_T+8} fontSize={8} fontFamily="DM Mono,monospace" fill={TEXT_DIM}>{fmt(rcMax, "")}</text>
        <text x={6} y={H3-PAD_B+2} fontSize={8} fontFamily="DM Mono,monospace" fill={TEXT_DIM}>{fmt(rcMin, "")}</text>
        {rcPath && <path d={rcPath} fill="none" stroke={K_RC} strokeWidth={2} />}
        {showDots && pts.map((p,i) => p.rc ? <circle key={i} cx={xAt(i)} cy={rcY(p.rc)} r={2.5} fill={K_RC} /> : null)}
      </svg>

      {/* Footer hint */}
      <div style={{ padding:"5px 12px", borderTop:"1px solid #E5E7EB", fontSize:9, color:TEXT_DIM, fontFamily:"'DM Mono',monospace", background:"#F9FAFB", textAlign:"center" }}>
        {pts.length > 1 ? `${pts.length} pulls · ${pts[0].label} → ${pts[pts.length-1].label}` : "click for details · double-click for full chart"}
      </div>
    </div>
  );
}

// ── Month-view mini calendar popup ──────────────────────────────────────
// Props:
//   value       — currently selected ISO date (YYYY-MM-DD) or null
//   uploadedSet — Set of ISO dates that have uploads (highlighted)
//   onPick(iso) — callback when a date is clicked
//   onClose()   — callback when user clicks outside
function MiniCalendar({ value, uploadedSet, onPick, onClose }) {
  const todayISO = todayStr();
  const [cursor, setCursor] = useState(() => {
    // Start viewing the month of the selected date, or today's month
    const base = value || todayISO;
    const [y, m] = base.split("-").map(Number);
    return { year: y, month: m - 1 }; // JS month is 0-indexed
  });

  const wrapRef = useRef(null);
  useEffect(() => {
    const handler = (e) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target)) onClose?.();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [onClose]);

  const monthNames = ["January","February","March","April","May","June","July","August","September","October","November","December"];
  const daysShort  = ["S","M","T","W","T","F","S"];

  const { year, month } = cursor;
  const firstDay = new Date(year, month, 1);
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const startWeekday = firstDay.getDay(); // 0 = Sunday

  // Build grid cells
  const cells = [];
  for (let i = 0; i < startWeekday; i++) cells.push(null); // blanks before day 1
  for (let d = 1; d <= daysInMonth; d++) cells.push(d);

  const toISO = (d) => `${year}-${String(month+1).padStart(2,"0")}-${String(d).padStart(2,"0")}`;

  const goMonth = (delta) => {
    setCursor(c => {
      const nm = c.month + delta;
      if (nm < 0) return { year: c.year - 1, month: 11 };
      if (nm > 11) return { year: c.year + 1, month: 0 };
      return { year: c.year, month: nm };
    });
  };

  return (
    <div ref={wrapRef}
      style={{
        position:"absolute", top:"110%", left:0, zIndex:500,
        background:"#fff", border:"1px solid #D1D5DB", borderRadius:10,
        boxShadow:"0 16px 40px rgba(15,23,42,0.18), 0 2px 6px rgba(15,23,42,0.08)",
        padding:12, width:272,
        fontFamily:"'Inter','Segoe UI',sans-serif",
      }}>
      {/* Header: month nav */}
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:10 }}>
        <button onClick={() => goMonth(-1)}
          style={{ background:"none", border:"1px solid #E5E7EB", borderRadius:6, padding:"4px 8px", cursor:"pointer", fontSize:12, color:"#4B5563", fontWeight:700 }}>‹</button>
        <div style={{ fontSize:13, fontWeight:800, color:"#111827" }}>
          {monthNames[month]} {year}
        </div>
        <button onClick={() => goMonth(1)}
          style={{ background:"none", border:"1px solid #E5E7EB", borderRadius:6, padding:"4px 8px", cursor:"pointer", fontSize:12, color:"#4B5563", fontWeight:700 }}>›</button>
      </div>

      {/* Day-of-week header */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(7, 1fr)", gap:2, marginBottom:4 }}>
        {daysShort.map((d, i) => (
          <div key={i} style={{ textAlign:"center", fontSize:9, fontFamily:"'DM Mono',monospace", fontWeight:700, color:"#9CA3AF", letterSpacing:1 }}>{d}</div>
        ))}
      </div>

      {/* Date grid */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(7, 1fr)", gap:2 }}>
        {cells.map((d, i) => {
          if (d === null) return <div key={i} />;
          const iso = toISO(d);
          const isToday    = iso === todayISO;
          const isSelected = iso === value;
          const hasUpload  = uploadedSet.has(iso);
          return (
            <button key={i}
              onClick={() => { if (hasUpload) onPick(iso); }}
              disabled={!hasUpload}
              style={{
                position:"relative",
                padding:"6px 0", borderRadius:6,
                fontSize:11, fontWeight:700,
                background: isSelected ? "#2563EB" : isToday ? "#EFF6FF" : "#fff",
                color:      isSelected ? "#fff" : hasUpload ? "#111827" : "#D1D5DB",
                border:     isSelected ? "1px solid #2563EB" : isToday ? "1px solid #93C5FD" : "1px solid transparent",
                cursor:     hasUpload ? "pointer" : "not-allowed",
                transition:"all 0.1s",
              }}
              title={hasUpload ? `${iso} — click to view` : `${iso} — no data`}
            >
              {d}
              {hasUpload && (
                <span style={{
                  position:"absolute", bottom:2, left:"50%", transform:"translateX(-50%)",
                  width:4, height:4, borderRadius:"50%",
                  background: isSelected ? "#fff" : "#2563EB",
                }}/>
              )}
            </button>
          );
        })}
      </div>

      {/* Footer: legend */}
      <div style={{ display:"flex", gap:12, marginTop:10, paddingTop:8, borderTop:"1px solid #F3F4F6", fontSize:9, fontFamily:"'DM Mono',monospace", color:"#9CA3AF" }}>
        <span style={{ display:"inline-flex", alignItems:"center", gap:4 }}>
          <span style={{ width:5, height:5, borderRadius:"50%", background:"#2563EB" }}/> has data
        </span>
        <span style={{ display:"inline-flex", alignItems:"center", gap:4 }}>
          <span style={{ width:8, height:8, borderRadius:3, background:"#EFF6FF", border:"1px solid #93C5FD" }}/> today
        </span>
      </div>
    </div>
  );
}

// ── Upload date picker dialog ───────────────────────────────────────────
// Shown AFTER user picks folder, when multiple dates are detected.
// Lets user choose: specific date, all dates, or cancel.
function UploadDatePicker({ pendingUpload, existingDates, onCancel, onConfirm }) {
  const { filesByDate, dateOptions } = pendingUpload;
  const today = todayStr();

  // Default selection: today if present, else the latest date
  const [selected, setSelected] = useState(() => {
    const set = new Set();
    if (dateOptions.includes(today)) set.add(today);
    else if (dateOptions.length > 0) set.add(dateOptions[0]);
    return set;
  });

  const toggle = (date) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(date)) next.delete(date);
      else next.add(date);
      return next;
    });
  };

  const selectAll  = () => setSelected(new Set(dateOptions));
  const selectNone = () => setSelected(new Set());
  const selectToday = () => {
    if (dateOptions.includes(today)) setSelected(new Set([today]));
  };
  const selectLatest = () => {
    if (dateOptions.length > 0) setSelected(new Set([dateOptions[0]]));
  };

  const totalFiles = [...selected].reduce((s,d) => s + (filesByDate[d]?.length || 0), 0);

  return (
    <div onClick={onCancel}
      style={{ position:"fixed", inset:0, background:"rgba(15,23,42,0.65)", display:"flex", alignItems:"center", justifyContent:"center", zIndex:9998, padding:16 }}>
      <div onClick={e => e.stopPropagation()}
        style={{
          background:"#fff", borderRadius:14, width:520, maxWidth:"96vw", maxHeight:"90vh", overflow:"hidden",
          boxShadow:"0 30px 90px rgba(0,0,0,0.35)", fontFamily:"'Inter','Segoe UI',sans-serif", display:"flex", flexDirection:"column",
        }}>

        {/* Header */}
        <div style={{ padding:"14px 18px", borderBottom:"1px solid #E5E7EB", background:"#F9FAFB" }}>
          <div style={{ fontSize:15, fontWeight:800, color:"#111827" }}>
            📅 Choose dates to upload
          </div>
          <div style={{ fontSize:11, color:"#6B7280", marginTop:3 }}>
            Found <b>{dateOptions.length}</b> date{dateOptions.length!==1?"s":""} across <b>{Object.values(filesByDate).flat().length}</b> files. Pick which ones to import.
          </div>
        </div>

        {/* Quick actions */}
        <div style={{ display:"flex", gap:6, padding:"10px 18px", borderBottom:"1px solid #F3F4F6", flexWrap:"wrap" }}>
          {dateOptions.includes(today) && (
            <button onClick={selectToday}
              style={{ background:"#EFF6FF", color:"#1D4ED8", border:"1px solid #93C5FD", borderRadius:6, padding:"5px 10px", fontSize:11, fontWeight:700, cursor:"pointer" }}>
              ⚡ Today only
            </button>
          )}
          <button onClick={selectLatest}
            style={{ background:"#F9FAFB", color:"#374151", border:"1px solid #D1D5DB", borderRadius:6, padding:"5px 10px", fontSize:11, fontWeight:700, cursor:"pointer" }}>
            ★ Latest date only
          </button>
          <button onClick={selectAll}
            style={{ background:"#F9FAFB", color:"#374151", border:"1px solid #D1D5DB", borderRadius:6, padding:"5px 10px", fontSize:11, fontWeight:700, cursor:"pointer" }}>
            All dates
          </button>
          <button onClick={selectNone}
            style={{ background:"#F9FAFB", color:"#6B7280", border:"1px solid #D1D5DB", borderRadius:6, padding:"5px 10px", fontSize:11, fontWeight:700, cursor:"pointer" }}>
            Clear
          </button>
        </div>

        {/* Date list */}
        <div style={{ flex:1, overflowY:"auto", padding:"6px 0", minHeight:120, maxHeight:380 }}>
          {dateOptions.map(date => {
            const isSel    = selected.has(date);
            const isToday  = date === today;
            const willOverwrite = existingDates.has(date);
            const files = filesByDate[date] || [];
            const brandsFound = new Set();
            files.forEach(f => {
              let b = detectBrandFromFilename(f.name);
              if (!b && f.webkitRelativePath) b = detectBrandFromFilename(f.webkitRelativePath);
              if (b) brandsFound.add(b);
            });
            return (
              <label key={date}
                style={{
                  display:"flex", alignItems:"center", gap:12,
                  padding:"10px 18px", cursor:"pointer",
                  borderLeft: isSel ? "3px solid #2563EB" : "3px solid transparent",
                  background: isSel ? "#EFF6FF" : "transparent",
                  transition:"all 0.1s",
                }}>
                <input type="checkbox" checked={isSel} onChange={() => toggle(date)}
                  style={{ width:16, height:16, cursor:"pointer", accentColor:"#2563EB" }} />
                <div style={{ flex:1, minWidth:0 }}>
                  <div style={{ display:"flex", alignItems:"center", gap:8, flexWrap:"wrap" }}>
                    <span style={{ fontSize:13, fontWeight:800, color:"#111827" }}>
                      {formatDateShort(date)} <span style={{ fontFamily:"'DM Mono',monospace", color:"#9CA3AF", fontWeight:600, fontSize:11 }}>({date})</span>
                    </span>
                    {isToday && (
                      <span style={{ background:"#DCFCE7", color:"#166534", padding:"1px 7px", borderRadius:10, fontSize:9, fontWeight:800 }}>TODAY</span>
                    )}
                    {willOverwrite && (
                      <span style={{ background:"#FEF3C7", color:"#92400E", padding:"1px 7px", borderRadius:10, fontSize:9, fontWeight:800 }}>
                        ⚠ WILL OVERWRITE
                      </span>
                    )}
                  </div>
                  <div style={{ fontSize:10, color:"#6B7280", marginTop:2, fontFamily:"'DM Mono',monospace" }}>
                    {files.length} file{files.length!==1?"s":""}
                    {brandsFound.size > 0 && ` · ${[...brandsFound].join(", ")}`}
                  </div>
                </div>
              </label>
            );
          })}
        </div>

        {/* Footer: action buttons */}
        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", gap:10, padding:"12px 18px", borderTop:"1px solid #E5E7EB", background:"#F9FAFB" }}>
          <div style={{ fontSize:11, color:"#6B7280" }}>
            {selected.size === 0 ? (
              <span style={{ color:"#DC2626" }}>Select at least one date</span>
            ) : (
              <>Importing <b style={{ color:"#111827" }}>{totalFiles} files</b> across <b style={{ color:"#111827" }}>{selected.size} date{selected.size!==1?"s":""}</b></>
            )}
          </div>
          <div style={{ display:"flex", gap:8 }}>
            <button onClick={onCancel}
              style={{ background:"#fff", color:"#374151", border:"1px solid #D1D5DB", borderRadius:8, padding:"8px 14px", fontSize:12, fontWeight:700, cursor:"pointer" }}>
              Cancel
            </button>
            <button onClick={() => onConfirm([...selected])}
              disabled={selected.size === 0}
              style={{
                background: selected.size === 0 ? "#9CA3AF" : "linear-gradient(135deg,#2563EB,#1D4ED8)",
                color:"#fff", border:"none", borderRadius:8, padding:"8px 16px",
                fontSize:12, fontWeight:800,
                cursor: selected.size === 0 ? "not-allowed" : "pointer",
                boxShadow: selected.size === 0 ? "none" : "0 2px 6px rgba(37,99,235,0.3)",
              }}>
              Import {selected.size > 0 ? `→ ${selected.size}` : ""}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function BsrTrackerPage({ onBack }) {
  const T = THEME;
  const [bsrBrand,   setBsrBrand]   = useState("All");
  const [sortCol,    setSortCol]    = useState("bsr");   // column key currently sorted
  const [sortDir,    setSortDir]    = useState("asc");   // "asc" | "desc"
  const [bsrSearch,  setBsrSearch]  = useState("");
  const [bsrPageNum, setBsrPageNum] = useState(1);
  const [modalRow,   setModalRow]   = useState(null);
  const [fullPageRow, setFullPageRow] = useState(null); // double-click opens Keepa-style full page
  const [toast,        setToast]        = useState(null); // { type, msg }
  const [quickFilter,  setQuickFilter]  = useState(null); // null | 'top1k' | 'rising' | 'falling' | 'problem' | 'zero_sales'

  // ── Hover popup (Keepa-style mini chart on title hover) ────────────────
  const [hoverRow, setHoverRow] = useState(null);
  const [hoverPos, setHoverPos] = useState({ x: 0, y: 0 });

  // ── Upload picker dialog: after user selects folder, we scan files,
  //     group by detected date, and show this dialog for user to choose.
  //     pendingUpload = { filesByDate: { "2026-04-23": [File, File, ...] }, dateOptions: [...] }
  const [pendingUpload, setPendingUpload] = useState(null);

  // ── Daily snapshots: { "2026-04-23": { "Audio Array": [rows], "Nexlev": [rows], ... } }
  const [snapshots, setSnapshots] = useState(() => loadSnapshots());
  // Currently-viewed date (default: most recent uploaded date, or null = use built-in BSR_RAW)
  const availableDates = useMemo(() => Object.keys(snapshots).sort().reverse(), [snapshots]);
  const [viewDate, setViewDate] = useState(() => {
    const snaps = loadSnapshots();
    const today = todayStr();
    if (snaps[today]) return today;          // prefer today if uploaded
    const keys = Object.keys(snaps).sort().reverse();
    return keys[0] || null;                  // else latest
  });
  // Compare mode: separate toggle — when ON shows compare view
  const [compareMode, setCompareMode] = useState(false);
  const [compareDate, setCompareDate] = useState(null);
  const [calendarOpen, setCalendarOpen] = useState(false);

  // Persist whenever snapshots change
  useEffect(() => { saveSnapshots(snapshots); }, [snapshots]);

  // Legacy compatibility: uploadedData/uploadTimestamps derived from current viewDate
  const uploadedData = viewDate && snapshots[viewDate] ? snapshots[viewDate] : {};
  const uploadTimestamps = useMemo(() => {
    if (!viewDate || !snapshots[viewDate]) return {};
    const out = {};
    Object.keys(snapshots[viewDate]).forEach(b => { out[b] = new Date(viewDate + "T12:00:00"); });
    return out;
  }, [viewDate, snapshots]);
  const PAGE_SIZE = 20;

  // Column definitions for sortable header (compare mode inserts 2 extra columns)
  const COLUMNS = useMemo(() => {
    const cmpOn = compareMode && compareDate;
    const base = [
      { key:"rank",         label:"#",              width:36,  sortable:false, align:"left" },
      { key:"image",        label:"IMG",            width:48,  sortable:false, align:"center" },
      { key:"title",        label:"TITLE",          width:240, sortable:true,  align:"left", type:"string" },
      { key:"asin",         label:"ASIN",           width:118, sortable:true,  align:"left", type:"string" },
      { key:"brand",        label:"BRAND",          width:120, sortable:true,  align:"left", type:"string" },
      { key:"bsr",          label: cmpOn ? `RANK · ${formatDateShort(viewDate)}` : "CURRENT RANK", width:130, sortable:true, align:"left", type:"number" },
    ];
    if (cmpOn) {
      base.push({ key:"bsr_B",         label:`RANK · ${formatDateShort(compareDate)}`, width:130, sortable:true, align:"left", type:"number" });
      base.push({ key:"bsr_delta",     label:"Δ RANK",     width:110, sortable:true, align:"left", type:"number" });
      base.push({ key:"bsr_delta_pct", label:"Δ %",        width:90,  sortable:true, align:"left", type:"number" });
    }
    base.push(
      { key:"trend",        label:"TREND",          width:110, sortable:true,  align:"left", type:"string" },
      { key:"rating",       label:"RATING",         width:120, sortable:true,  align:"left", type:"number" },
      { key:"reviews",      label:"RATING COUNT",   width:100, sortable:true,  align:"left", type:"number" },
      { key:"bsr_30d",      label:"30D AVG RANK",   width:125, sortable:true,  align:"left", type:"number" },
      { key:"drops_30d",    label:"FOOTFALL / 30D", width:150, sortable:true,  align:"left", type:"number" },
      { key:"monthly_sold", label:"MONTHLY SOLD",   width:110, sortable:true,  align:"left", type:"number" },
    );
    return base;
  }, [compareMode, compareDate, viewDate]);

  // Click handler: same column -> flip direction; new column -> set with sensible default
  const handleSort = (key) => {
    if (sortCol === key) {
      setSortDir(d => d === "asc" ? "desc" : "asc");
    } else {
      setSortCol(key);
      // Numbers default to desc (high first), strings default to asc (A→Z)
      // Exception: bsr defaults to asc (best/lowest rank first) since lower = better
      const col = COLUMNS.find(c => c.key === key);
      setSortDir(key === "bsr" || key === "bsr_30d" || col?.type === "string" ? "asc" : "desc");
    }
    setBsrPageNum(1);
  };

  const allBrands = ["All","Audio Array","Nexlev","Tonor","White Mulberry"];

  const data = useMemo(() => {
    // Start with built-in data, normalized
    const built = BSR_RAW.map(row => {
      const m = ASIN_MASTER[row.asin] || {};
      return { ...row,
        trend:    row.trend || inferTrend(row.bsr, row.bsr_30d),
        image:    row.image || "",
        // Last resort is the model code from sku_master. An export can carry no
        // title at all -- a "not found (404)" row, which is what a discontinued
        // or not-yet-launched ASIN returns -- and a nameless row is unusable.
        title:    row.title || ASIN_TITLE_MAP[row.asin] || m.model || "",
        model:    m.model    || "—",
        category: m.category || "—",
        fbaSku:   m.fbaSku   || "—",
        brand: row.brand === "nexlev" ? "Nexlev" : row.brand === "TONOR" ? "Tonor" : row.brand,
      };
    });
    // Replace any brand that has uploaded data with the new rows
    const uploadedBrands = Object.keys(uploadedData);
    let base;
    if (uploadedBrands.length === 0) {
      base = built;
    } else {
      const kept = built.filter(r => !uploadedBrands.includes(r.brand));
      const fresh = [];
      uploadedBrands.forEach(b => {
        (uploadedData[b] || []).forEach(row => {
          const m = ASIN_MASTER[row.asin] || {};
          fresh.push({
            ...row,
            brand: b,
            trend: row.trend || inferTrend(row.bsr, row.bsr_30d),
            image: row.image || "",
            title: row.title || ASIN_TITLE_MAP[row.asin] || m.model || "",
            model:    m.model    || "—",
            category: m.category || "—",
            fbaSku:   m.fbaSku   || "—",
          });
        });
      });
      base = [...kept, ...fresh];
    }

    // If compare mode is on, enrich each row with Date-B values
    if (compareMode && compareDate && snapshots[compareDate]) {
      const compSnap = snapshots[compareDate];
      const compMap = {};
      Object.keys(compSnap).forEach(b => {
        (compSnap[b] || []).forEach(r => { compMap[r.asin] = r; });
      });
      return base.map(r => {
        const cmp = compMap[r.asin];
        if (!cmp || !cmp.bsr || !r.bsr) {
          return { ...r, bsr_B: cmp?.bsr || null, bsr_delta: null, bsr_delta_pct: null };
        }
        const delta = r.bsr - cmp.bsr;  // negative = improved (lower rank is better)
        const pct = (delta / cmp.bsr) * 100;
        return { ...r, bsr_B: cmp.bsr, bsr_delta: delta, bsr_delta_pct: pct };
      });
    }
    return base;
  }, [uploadedData, compareMode, compareDate, snapshots]);

  const filtered = useMemo(() => {
    // Unranked products are still shown. An export can arrive with no sales
    // rank for a product that is perfectly fine and still has a rating and a
    // review count -- the 2026-08-05 export had zero ranks for all 98 White
    // Mulberry ASINs. Hiding those made the whole brand vanish from the tracker,
    // which reads as "we lost the products" rather than "Keepa gave us no rank".
    // Rank sorts to the bottom and renders as "No data"; nothing is invented.
    let rows = [...data];
    if (bsrBrand !== "All") rows = rows.filter(r => r.brand === bsrBrand);
    if (bsrSearch.trim()) {
      const q = bsrSearch.trim().toLowerCase();
      rows = rows.filter(r => r.asin.toLowerCase().includes(q) || r.brand.toLowerCase().includes(q) || r.model.toLowerCase().includes(q));
    }
    // Quick filter chips
    // r.bsr > 0 first: a null rank would sail through `null <= 1000`.
    if (quickFilter === "top1k")      rows = rows.filter(r => r.bsr > 0 && r.bsr <= 1000);
    if (quickFilter === "rising")     rows = rows.filter(r => r.trend === "up");
    if (quickFilter === "falling")    rows = rows.filter(r => r.trend === "down");
    if (quickFilter === "problem")    rows = rows.filter(r => (r.rating || 0) > 0 && r.rating < 4);
    if (quickFilter === "zero_sales") rows = rows.filter(r => !r.drops_30d || r.drops_30d === 0);
    const col = COLUMNS.find(c => c.key === sortCol);
    const mult = sortDir === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => {
      const av = a[sortCol];
      const bv = b[sortCol];
      // Push null/undefined/0 (for ranks) to the bottom regardless of direction
      const aMissing = av === null || av === undefined || av === "" || (col?.type === "number" && !av);
      const bMissing = bv === null || bv === undefined || bv === "" || (col?.type === "number" && !bv);
      if (aMissing && bMissing) return 0;
      if (aMissing) return 1;
      if (bMissing) return -1;
      if (col?.type === "string") {
        return String(av).localeCompare(String(bv)) * mult;
      }
      return (av - bv) * mult;
    });
  }, [data, bsrBrand, bsrSearch, sortCol, sortDir, quickFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const curPage    = Math.min(bsrPageNum, totalPages);
  const paged      = filtered.slice((curPage-1)*PAGE_SIZE, curPage*PAGE_SIZE);

  // Aggregate stats for KPI strip (across currently filtered brand)
  const stats = useMemo(() => {
    // TOTAL ASINS counts every product in the brand; the rank-based figures are
    // computed only over the ones that actually have a rank, so a missing rank
    // drags nothing towards zero.
    const scope = bsrBrand === "All" ? data : data.filter(r => r.brand === bsrBrand);
    const ranked = scope.filter(r => r.bsr > 0);
    const best = ranked.length ? Math.min(...ranked.map(r => r.bsr)) : null;
    const rising = scope.filter(r => r.trend === "up").length;
    const falling = scope.filter(r => r.trend === "down").length;
    const top1k = ranked.filter(r => r.bsr <= 1000).length;
    const avgDrops = scope.length ? Math.round(scope.reduce((s, r) => s + (r.drops_30d || 0), 0) / scope.length) : 0;
    const totalMonthly = scope.reduce((s, r) => s + (r.monthly_sold || 0), 0);
    const problem = scope.filter(r => (r.rating || 0) > 0 && r.rating < 4).length;
    const zeroSales = scope.filter(r => !r.drops_30d || r.drops_30d === 0).length;
    return { total: scope.length, best, rising, falling, top1k, avgDrops, totalMonthly, problem, zeroSales };
  }, [data, bsrBrand]);

  // Auto-dismiss toast after 3 seconds
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 3000);
    return () => clearTimeout(t);
  }, [toast]);

  // Parse a Keepa CSV into BSR rows. Handles both old and new column sets:
  //   Old: "Sales Rank: Current", "Sales Rank: 30/90/365 days avg.",
  //        "Sales Rank: Drops last 30 days", "Reviews: Rating", "Reviews: Rating Count",
  //        "ASIN", "Brand", "Monthly Sold"
  //   New: adds "Image" (semicolon-separated URLs), "Title",
  //        and uses "Sales Rank: 30 days drop %" instead of drop count.
  const parseKeepaCsv = (text) => {
    const parseRow = (line) => {
      const out = []; let cur = ""; let q = false;
      for (let i = 0; i < line.length; i++) {
        const c = line[i];
        if (c === '"') {
          if (q && line[i+1] === '"') { cur += '"'; i++; }
          else q = !q;
        } else if (c === "," && !q) { out.push(cur); cur = ""; }
        else cur += c;
      }
      out.push(cur);
      return out.map(s => s.trim());
    };
    const lines = text.replace(/\r/g, "").split("\n").filter(l => l.trim());
    if (lines.length < 2) return [];
    const headers = parseRow(lines[0]);
    const idx = (name) => headers.findIndex(h => h.toLowerCase().includes(name.toLowerCase()));
    const iImg    = idx("Image");
    const iTitle  = idx("Title");
    const iCurr   = idx("Sales Rank: Current");
    const i30     = idx("Sales Rank: 30 days avg");
    const i90     = idx("Sales Rank: 90");
    const i365    = idx("Sales Rank: 365");
    // Both old ("Drops last 30 days") and new ("30 days drop %") map to drops_30d.
    // If header has "drop %", we'll treat the value as a percentage string.
    const iDrops    = idx("Drops last 30");
    const iDropsPct = idx("30 days drop %");
    const iDropsCol = iDrops >= 0 ? iDrops : iDropsPct;
    const dropsIsPct = iDrops < 0 && iDropsPct >= 0;
    const iRating = idx("Reviews: Rating");
    const iRevCnt = idx("Rating Count");
    const iAsin   = idx("ASIN");
    const iMonth  = idx("Monthly Sold");
    const toNum = (v) => {
      if (v === undefined || v === null) return null;
      const s = String(v).replace(/[",#%]/g, "").replace(/[\u00A0\s]/g, "").trim();
      if (!s || s === "-" || s.toLowerCase() === "n/a") return null;
      const n = parseFloat(s);
      return isNaN(n) ? null : n;
    };
    const rows = [];
    for (let li = 1; li < lines.length; li++) {
      const cols = parseRow(lines[li]);
      const asin = (cols[iAsin] || "").trim();
      if (!asin || asin.length < 8) continue;
      // Image: first URL before ";" (Keepa returns multiple joined by semicolon)
      const imgRaw  = iImg >= 0 ? (cols[iImg] || "").trim() : "";
      const image   = imgRaw ? imgRaw.split(";")[0].trim() : "";
      const title   = iTitle >= 0 ? (cols[iTitle] || "").trim() : "";
      const bsr     = toNum(cols[iCurr]);
      const bsr30   = toNum(cols[i30]);
      const bsr90   = toNum(cols[i90]);
      const bsr365  = toNum(cols[i365]);
      const dropsRaw = iDropsCol >= 0 ? toNum(cols[iDropsCol]) : null;
      const rating  = toNum(cols[iRating]);
      const reviews = toNum(cols[iRevCnt]);
      const monthly = toNum(cols[iMonth]);
      const trend = inferTrend(bsr, bsr30);
      rows.push({
        asin,
        image,
        title,
        bsr: bsr || 0,
        bsr_30d: bsr30,
        bsr_90d: bsr90,
        bsr_365d: bsr365,
        // Keep original number and whether it's a percentage — UI can decide how to render
        drops_30d: dropsRaw || 0,
        drops_is_pct: dropsIsPct,
        rating: rating,
        reviews: reviews,
        monthly_sold: monthly,
        trend,
      });
    }
    return rows;
  };

  const handleBrandUpload = (brand, file) => {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const rows = parseKeepaCsv(e.target.result);
        if (rows.length === 0) {
          setToast({ type:"error", msg:`⚠️ No valid rows found in ${file.name}` });
          return;
        }
        const date = todayStr();
        setSnapshots(prev => ({
          ...prev,
          [date]: { ...(prev[date] || {}), [brand]: rows }
        }));
        setViewDate(date);
        setCompareDate(null);
        setBsrPageNum(1);
        setToast({ type:"success", msg:`✅ ${brand} · ${formatDateShort(date)} — ${rows.length} ASINs loaded` });
      } catch (err) {
        setToast({ type:"error", msg:`❌ Failed to parse: ${err.message}` });
      }
    };
    reader.onerror = () => setToast({ type:"error", msg:`❌ Could not read ${file.name}` });
    reader.readAsText(file);
  };

  // Step 1: Scan picked files, group by detected date, open picker dialog.
  // User chooses which date(s) to upload, then processUploadForDates runs.
  const handleFolderUpload = (fileList) => {
    const files = Array.from(fileList || []).filter(f => f.name.toLowerCase().endsWith(".csv"));
    if (files.length === 0) {
      setToast({ type:"error", msg:"⚠️ No CSV files found" });
      return;
    }

    // Group files by detected date. Files with no parseable date fall into "today".
    const filesByDate = {};
    const noDateFiles = [];
    files.forEach(file => {
      const dateFromName = extractDateFromFilename(file.name);
      const dateFromPath = file.webkitRelativePath ? extractDateFromFilename(file.webkitRelativePath) : null;
      const date = dateFromName || dateFromPath;
      if (date) {
        (filesByDate[date] ||= []).push(file);
      } else {
        noDateFiles.push(file);
      }
    });

    // If we have files with no date, bucket them under today
    if (noDateFiles.length > 0) {
      const today = todayStr();
      (filesByDate[today] ||= []).push(...noDateFiles);
    }

    const dateOptions = Object.keys(filesByDate).sort().reverse();
    if (dateOptions.length === 0) {
      setToast({ type:"error", msg:"⚠️ No dates detected in files" });
      return;
    }

    // If only 1 date detected, just process it silently — no dialog needed
    if (dateOptions.length === 1) {
      processUploadForDates(filesByDate, [dateOptions[0]]);
      return;
    }

    // Multiple dates → show picker dialog
    setPendingUpload({ filesByDate, dateOptions });
  };

  // Step 2: Actually read + parse files for chosen dates and save to snapshots.
  const processUploadForDates = (filesByDate, chosenDates) => {
    const allJobs = [];
    chosenDates.forEach(date => {
      (filesByDate[date] || []).forEach(file => {
        allJobs.push({ date, file });
      });
    });
    if (allJobs.length === 0) {
      setToast({ type:"error", msg:"⚠️ No files to upload" });
      return;
    }

    // brandsByDate = { "2026-04-23": { "Audio Array": [rows], ... }, ... }
    const brandsByDate = {};
    const errors = [];
    let remaining = allJobs.length;

    allJobs.forEach(({ date, file }) => {
      // Detect brand from filename or folder path
      let brand = detectBrandFromFilename(file.name);
      if (!brand && file.webkitRelativePath) {
        brand = detectBrandFromFilename(file.webkitRelativePath);
      }
      if (!brand) {
        errors.push(`No brand detected: ${file.webkitRelativePath || file.name}`);
        if (--remaining === 0) finalize();
        return;
      }

      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const rows = parseKeepaCsv(e.target.result);
          if (rows.length > 0) {
            brandsByDate[date] ||= {};
            // If multiple files match the same brand+date, prefer the one WITH images
            const existing = brandsByDate[date][brand];
            const newHasImages = rows.some(r => r.image);
            const existingHasImages = existing ? existing.some(r => r.image) : false;
            if (!existing ||
                (newHasImages && !existingHasImages) ||
                (newHasImages === existingHasImages && rows.length > existing.length)) {
              brandsByDate[date][brand] = rows;
            }
          } else {
            errors.push(`No rows in ${file.name}`);
          }
        } catch (err) {
          errors.push(`${file.name}: ${err.message}`);
        }
        if (--remaining === 0) finalize();
      };
      reader.onerror = () => {
        errors.push(`Could not read ${file.name}`);
        if (--remaining === 0) finalize();
      };
      reader.readAsText(file);
    });

    const finalize = () => {
      const dates = Object.keys(brandsByDate);
      if (dates.length === 0) {
        setToast({ type:"error", msg:`❌ Upload failed. ${errors[0] || ""}` });
        return;
      }
      // Overwrite existing snapshots for each date (user confirmed overwrite)
      setSnapshots(prev => {
        const next = { ...prev };
        dates.forEach(date => {
          next[date] = { ...(next[date] || {}), ...brandsByDate[date] };
        });
        return next;
      });

      // Jump view to today if today was uploaded, else latest uploaded date
      const today = todayStr();
      const target = dates.includes(today) ? today : dates.sort().reverse()[0];
      setViewDate(target);
      setCompareDate(null);
      setBsrPageNum(1);

      // Build summary toast
      const totalBrands  = dates.reduce((s,d) => s + Object.keys(brandsByDate[d]).length, 0);
      const totalRows    = dates.reduce((s,d) => s + Object.values(brandsByDate[d]).reduce((x,r)=>x+r.length,0), 0);
      const dateLabel    = dates.length === 1 ? formatDateShort(dates[0]) : `${dates.length} dates`;
      setToast({
        type:"success",
        msg:`✅ ${dateLabel} · ${totalBrands} file${totalBrands>1?"s":""} · ${totalRows} ASINs${errors.length?` (${errors.length} skipped)`:""}`
      });
    };
  };

  const deleteSnapshot = (date) => {
    if (!window.confirm(`Delete all BSR data for ${formatDateShort(date)}?`)) return;
    setSnapshots(prev => {
      const next = { ...prev };
      delete next[date];
      return next;
    });
    if (viewDate === date) {
      const remaining = Object.keys(snapshots).filter(d => d !== date).sort().reverse();
      setViewDate(remaining[0] || null);
    }
    if (compareDate === date) setCompareDate(null);
    setToast({ type:"success", msg:`🗑️ Deleted ${formatDateShort(date)}` });
  };

  const exportCsv = () => {
    const header = ["Rank","ASIN","Brand","Title","Current Rank","30d Avg","90d Avg","365d Avg","Trend","Rating","Rating Count","Drops 30d","Monthly Sold","Image"];
    const lines = [header.join(",")];
    filtered.forEach((r, i) => {
      const vals = [
        i + 1, r.asin, r.brand, r.title || "",
        r.bsr || "", r.bsr_30d || "", r.bsr_90d || "", r.bsr_365d || "",
        r.trend || "", r.rating || "", r.reviews || "", r.drops_30d || "", r.monthly_sold || "",
        r.image || "",
      ];
      lines.push(vals.map(v => {
        const s = String(v);
        return s.includes(",") ? `"${s.replace(/"/g, '""')}"` : s;
      }).join(","));
    });
    const blob = new Blob([lines.join("\n")], { type:"text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    const today = new Date().toISOString().slice(0,10);
    const brandTag = bsrBrand === "All" ? "all" : bsrBrand.toLowerCase().replace(/\s+/g, "_");
    a.download = `bsr_${brandTag}_${today}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    setToast({ type:"success", msg:`📥 Exported ${filtered.length} rows` });
  };

  // Double-click opens full Keepa-like page
  if (fullPageRow) {
    return <BsrFullPage row={fullPageRow} onBack={()=>setFullPageRow(null)} />;
  }

  return (
    <div style={{ fontFamily:"'Inter','Segoe UI',sans-serif", background:T.pageBg, minHeight:"100vh", color:T.text }}>

      {modalRow && <BsrGraphModal row={modalRow} onClose={()=>setModalRow(null)} />}

      {/* Keepa-style hover popup on ASIN */}
      {hoverRow && (
        <BsrHoverChart
          row={hoverRow}
          x={hoverPos.x}
          y={hoverPos.y}
          title={ASIN_TITLE_MAP[hoverRow.asin] || null}
        />
      )}

      {/* Top Header — matches Smart View */}
      <div style={{ background:T.surface, borderBottom:`1px solid ${T.border}`, padding:"0 24px", display:"flex", alignItems:"center", justifyContent:"space-between", minHeight:64 }}>
        <div style={{ display:"flex", alignItems:"center", gap:10 }}>
          <button onClick={onBack} style={{ background:T.surfaceMuted, color:T.textSoft, border:`1px solid ${T.border}`, borderRadius:8, padding:"6px 12px", fontSize:11, fontWeight:700, cursor:"pointer" }}>← Back</button>
          <div style={{ width:32, height:32, borderRadius:8, background:"linear-gradient(135deg,#38BDF8,#2563EB)", display:"flex", alignItems:"center", justifyContent:"center", marginLeft:4 }}>
            <span style={{ color:"#fff", fontSize:13, fontWeight:800 }}>📊</span>
          </div>
          <div>
            <div style={{ fontSize:15, fontWeight:700, color:T.text, letterSpacing:-0.4 }}>BSR Tracker</div>
            <div style={{ fontSize:9, color:T.textFaint, fontFamily:"'DM Mono',monospace", letterSpacing:1 }}>CLICK = PREVIEW · DOUBLE-CLICK = FULL CHART</div>
          </div>
        </div>
        <div style={{ display:"flex", gap:10, alignItems:"center" }}>
          {(() => {
            const ts = Object.values(uploadTimestamps);
            const latest = ts.length ? new Date(Math.max(...ts.map(d => d.getTime()))) : null;
            const display = latest
              ? latest.toLocaleString("en-IN", { day:"2-digit", month:"short", hour:"2-digit", minute:"2-digit" })
              : "from bsr_data.json";
            const isStale = !latest; // uploaded data is fresh; JSON fallback might be stale
            return (
              <div style={{ display:"flex", flexDirection:"column", alignItems:"flex-end", lineHeight:1.2 }}>
                <span style={{ fontSize:8, color:T.textFaint, fontFamily:"'DM Mono',monospace", letterSpacing:1, fontWeight:700 }}>LAST UPDATED</span>
                <span style={{ fontSize:10, color: isStale ? "#DC2626" : "#059669", fontFamily:"'DM Mono',monospace", fontWeight:700 }}>
                  {display}
                </span>
              </div>
            );
          })()}
          <span style={{ fontSize:10, color:T.textFaint, fontFamily:"'DM Mono',monospace" }}>{filtered.length} ASINs</span>
        </div>
      </div>

      {/* ── DAILY SNAPSHOTS CALENDAR BAR (view-only — data baked in from repo) ── */}
      <div style={{ background:T.surface, borderBottom:`1px solid ${T.border}`, padding:"10px 24px" }}>
        <div style={{ display:"flex", gap:12, alignItems:"center", flexWrap:"wrap" }}>

          {/* ── Date picker group ─────────────────────────────────────── */}
          <div style={{ display:"flex", alignItems:"center", gap:8, flexWrap:"wrap" }}>

            {/* Calendar popover trigger */}
            <div style={{ position:"relative" }}>
              <button onClick={() => setCalendarOpen(o => !o)}
                style={{
                  display:"inline-flex", alignItems:"center", gap:7,
                  background: calendarOpen ? "#EFF6FF" : T.surface,
                  color: calendarOpen ? "#1D4ED8" : T.text,
                  border: `1px solid ${calendarOpen ? "#93C5FD" : T.border}`,
                  padding:"8px 14px", borderRadius:8, fontSize:11, fontWeight:700, cursor:"pointer",
                  whiteSpace:"nowrap", minWidth:160, justifyContent:"space-between",
                }}>
                <span style={{ display:"inline-flex", alignItems:"center", gap:7 }}>
                  <span style={{ fontSize:14 }}>📅</span>
                  {viewDate ? (
                    <span>
                      <span style={{ color:T.textFaint, fontWeight:600, marginRight:5 }}>Viewing:</span>
                      <span style={{ color:"#1D4ED8", fontFamily:"'DM Mono',monospace" }}>{formatDateShort(viewDate)}</span>
                      {viewDate === todayStr() && (
                        <span style={{ background:"#DCFCE7", color:"#166534", padding:"1px 6px", borderRadius:10, fontSize:9, marginLeft:6 }}>TODAY</span>
                      )}
                    </span>
                  ) : (
                    <span style={{ color:T.textFaint, fontStyle:"italic" }}>No data yet</span>
                  )}
                </span>
                <span style={{ fontSize:9, color:T.textFaint }}>▼</span>
              </button>

              {calendarOpen && (
                <MiniCalendar
                  value={viewDate}
                  uploadedSet={new Set(availableDates)}
                  onPick={(iso) => {
                    setViewDate(iso);
                    setBsrPageNum(1);
                    setCalendarOpen(false);
                  }}
                  onClose={() => setCalendarOpen(false)}
                />
              )}
            </div>

            {/* Today quick jump */}
            <button
              onClick={() => {
                const t = todayStr();
                if (availableDates.includes(t)) {
                  setViewDate(t);
                  setBsrPageNum(1);
                  setToast({ type:"success", msg:`📅 Jumped to Today (${formatDateShort(t)})` });
                } else {
                  setToast({ type:"error", msg:`⚠️ No data uploaded for today yet` });
                }
              }}
              disabled={!availableDates.includes(todayStr())}
              style={{
                background: viewDate === todayStr() ? "#DCFCE7" : availableDates.includes(todayStr()) ? "#fff" : T.surfaceMuted,
                color:      viewDate === todayStr() ? "#166534" : availableDates.includes(todayStr()) ? T.text : T.textFaint,
                border: `1px solid ${viewDate === todayStr() ? "#86EFAC" : T.border}`,
                padding:"8px 14px", borderRadius:8, fontSize:11, fontWeight:800,
                cursor: availableDates.includes(todayStr()) ? "pointer" : "not-allowed",
                whiteSpace:"nowrap",
              }}>
              ⚡ Today
            </button>

            {/* Compare toggle */}
            {availableDates.length >= 2 && (
              <button
                onClick={() => {
                  if (compareMode) {
                    setCompareMode(false);
                    setCompareDate(null);
                  } else {
                    // Pick the most recent date that isn't the current view
                    const other = availableDates.find(d => d !== viewDate);
                    setCompareDate(other || null);
                    setCompareMode(true);
                  }
                }}
                style={{
                  background: compareMode ? "#FEF3C7" : T.surface,
                  color:      compareMode ? "#92400E" : T.text,
                  border: `1px solid ${compareMode ? "#F59E0B" : T.border}`,
                  padding:"8px 14px", borderRadius:8, fontSize:11, fontWeight:700, cursor:"pointer",
                  whiteSpace:"nowrap",
                }}>
                {compareMode ? "✕ Exit Compare" : "🔍 Compare Dates"}
              </button>
            )}

            {/* Record count badge */}
            <span style={{ fontSize:10, color:T.textFaint, fontFamily:"'DM Mono',monospace", fontWeight:700 }}>
              {availableDates.length} {availableDates.length === 1 ? "date" : "dates"} available
            </span>
          </div>
        </div>

        {/* ── Compare Mode banner + Date B picker ──────────────────────── */}
        {compareMode && (
          <div style={{ marginTop:8, padding:"8px 12px", background:"#FFFBEB", border:"1px solid #FCD34D", borderRadius:8, display:"flex", gap:12, alignItems:"center", flexWrap:"wrap" }}>
            <span style={{ fontSize:11, fontWeight:800, color:"#92400E" }}>🔍 Compare Mode</span>
            <span style={{ fontSize:11, color:"#92400E" }}>
              <span style={{ color:"#2563EB", fontWeight:800 }}>{formatDateShort(viewDate)}</span>
              {" vs "}
            </span>
            <select
              value={compareDate || ""}
              onChange={e => setCompareDate(e.target.value || null)}
              style={{ background:"#fff", border:"1px solid #F59E0B", borderRadius:6, padding:"4px 10px", fontSize:11, color:"#92400E", fontWeight:800, cursor:"pointer", outline:"none" }}>
              <option value="">— pick date —</option>
              {availableDates.filter(d => d !== viewDate).map(d => (
                <option key={d} value={d}>{formatDateShort(d)}</option>
              ))}
            </select>
            <span style={{ fontSize:10, color:"#92400E", fontStyle:"italic" }}>
              green ↑ = rank improved · red ↓ = rank worsened (lower rank = better on Amazon)
            </span>
          </div>
        )}
      </div>

      {/* Filter Bar — matches Smart View */}
      <div style={{ background:T.shellBg, borderBottom:`1px solid ${T.border}`, padding:"12px 24px", display:"flex", gap:10, alignItems:"center", flexWrap:"wrap" }}>
        <div style={{ position:"relative", flex:"0 0 auto" }}>
          <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", color:T.textFaint, fontSize:13 }}>⌕</span>
          <input
            placeholder="Search ASIN, brand, model…"
            value={bsrSearch}
            onChange={e=>{setBsrSearch(e.target.value); setBsrPageNum(1);}}
            style={{ background: bsrSearch?"#EFF6FF":T.surface, border: bsrSearch?"1px solid #3B82F6":`1px solid ${T.border}`, borderRadius:8, color:T.text, padding:"9px 14px 9px 34px", fontSize:12, width:260, outline:"none", transition:"all 0.15s" }}
          />
        </div>
        {/* Brand pill filter */}
        <div style={{ display:"flex", gap:6, alignItems:"center", padding:"7px 12px", borderRadius:8, background: bsrBrand!=="All" ? "#EFF6FF" : T.surface, border:`1px solid ${bsrBrand!=="All" ? "#3B82F6" : T.border}` }}>
          <span style={{ fontSize:10, color: bsrBrand!=="All" ? "#2563EB" : T.textFaint, fontFamily:"'DM Mono',monospace", fontWeight:700, letterSpacing:1 }}>BRAND</span>
          <select value={bsrBrand} onChange={e=>{setBsrBrand(e.target.value); setBsrPageNum(1);}}
            style={{ background:"transparent", border:"none", color: bsrBrand!=="All" ? "#1D4ED8" : T.text, padding:"1px 20px 1px 2px", fontSize:12, fontWeight:600, outline:"none", appearance:"none", cursor:"pointer", backgroundImage:`url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 24 24' fill='none' stroke='%2394A3B8' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E")`, backgroundRepeat:"no-repeat", backgroundPosition:"right 4px center" }}>
            {allBrands.map(b=>(
              <option key={b} value={b} style={{ color:"#0F172A", background:"#FFFFFF" }}>{b === "All" ? "All Brands" : b}</option>
            ))}
          </select>
        </div>
        <div style={{ marginLeft:"auto", display:"flex", gap:8, alignItems:"center" }}>
          <span style={{ fontSize:11, color: bsrBrand!=="All" ? "#2563EB" : T.textFaint, fontFamily:"'DM Mono',monospace" }}>
            {filtered.length} rows
          </span>
          {/* Export lives up here beside the row count as well as in the bottom
              bar: the bottom bar is pinned to the window edge, which puts it
              below the fold on a long table and made it impossible to find. */}
          <button onClick={exportCsv} title="Download the rows currently shown, as CSV"
            style={{ background:T.surface, border:`1px solid ${T.border}`, borderRadius:8, padding:"7px 12px", fontSize:11, color:T.textSoft, fontWeight:700, cursor:"pointer", whiteSpace:"nowrap" }}>
            📥 Export CSV
          </button>
          <div style={{ display:"flex", gap:6, alignItems:"center", padding:"7px 10px", borderRadius:8, background:T.surface, border:`1px solid ${T.border}` }}>
            <span style={{ fontSize:10, color:T.textFaint, fontFamily:"'DM Mono',monospace", fontWeight:600 }}>SORT</span>
            <select value={`${sortCol}_${sortDir}`} onChange={e=>{ const [k, d] = e.target.value.split("_"); setSortCol(k); setSortDir(d); setBsrPageNum(1); }}
              style={{ background:"transparent", border:"none", color:T.text, padding:"1px 20px 1px 2px", fontSize:12, outline:"none", appearance:"none", cursor:"pointer", backgroundImage:`url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 24 24' fill='none' stroke='%2394A3B8' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E")`, backgroundRepeat:"no-repeat", backgroundPosition:"right 4px center" }}>
              <option value="bsr_asc">Current Rank ↑</option>
              <option value="bsr_desc">Current Rank ↓</option>
              <option value="drops_30d_desc">Footfall (high)</option>
              <option value="rating_desc">Rating (high)</option>
              <option value="reviews_desc">Rating Count (high)</option>
              <option value="monthly_sold_desc">Monthly Sold (high)</option>
            </select>
          </div>
        </div>
      </div>

      <div style={{ padding:"16px 24px" }}>

        {/* KPI Strip — clickable cards that act as filters */}
        <div style={{ display:"flex", gap:10, marginBottom:10, overflowX:"auto", paddingBottom:2 }}>
          {(() => {
            const cards = [
              { key:null,         label:"Total ASINs",         value:stats.total.toLocaleString("en-IN"),                          accent:"#38BDF8", hint:"Show all" },
              { key:"best",       label:"Best Rank",           value:stats.best ? `#${stats.best.toLocaleString("en-IN")}` : "—", accent:"#10B981", disabled:true },
              { key:"top1k",      label:"Top 1K",              value:stats.top1k.toLocaleString("en-IN"),                          accent:"#F59E0B" },
              { key:"rising",     label:"Rising ↑",            value:stats.rising.toLocaleString("en-IN"),                         accent:"#059669" },
              { key:"falling",    label:"Falling ↓",           value:stats.falling.toLocaleString("en-IN"),                        accent:"#DC2626" },
              { key:null,         label:"Avg Drops/mo",        value:stats.avgDrops.toLocaleString("en-IN"),                       accent:"#8B5CF6", disabled:true },
              { key:null,         label:"Total Monthly Sold",  value:stats.totalMonthly.toLocaleString("en-IN"),                   accent:"#EC4899", disabled:true },
            ];
            return cards.map((c, i) => {
              const clickable = c.key !== undefined && !c.disabled;
              const isActive = clickable && c.key !== null && quickFilter === c.key;
              const isAll = clickable && c.key === null;
              const showAsActive = isActive || (isAll && quickFilter === null);
              return (
                <div
                  key={`${c.label}-${i}`}
                  onClick={clickable ? () => { setQuickFilter(isActive ? null : c.key); setBsrPageNum(1); } : undefined}
                  style={{
                    background: T.surface,
                    border: showAsActive && !isAll ? `1px solid ${c.accent}` : `1px solid ${T.border}`,
                    boxShadow: showAsActive && !isAll ? `0 0 0 3px ${c.accent}22` : "none",
                    borderRadius: 10, padding: "10px 16px", minWidth: 140, flex: "0 0 auto",
                    borderTop: `2px solid ${c.accent}`,
                    display: "flex", flexDirection: "column", gap: 2,
                    cursor: clickable ? "pointer" : "default",
                    opacity: c.disabled ? 0.9 : 1,
                    transition: "all 0.15s",
                  }}
                  title={c.disabled ? "" : (isActive ? "Click to clear filter" : (c.hint || `Filter by ${c.label}`))}
                >
                  <div style={{ fontSize:9, color:T.textMuted, fontFamily:"'DM Mono',monospace", letterSpacing:1.2, textTransform:"uppercase", display:"flex", justifyContent:"space-between", alignItems:"center" }}>
                    <span>{c.label}</span>
                    {clickable && !c.disabled && <span style={{ fontSize:8, color: showAsActive ? c.accent : T.textFaint, fontWeight:700 }}>{isActive ? "●" : ""}</span>}
                  </div>
                  <div style={{ fontSize:18, fontWeight:800, color:T.text, letterSpacing:-0.5, lineHeight:1.2 }}>{c.value}</div>
                </div>
              );
            });
          })()}
        </div>

        {/* Quick filter chips */}
        <div style={{ display:"flex", gap:6, marginBottom:12, alignItems:"center", flexWrap:"wrap" }}>
          <span style={{ fontSize:9, color:T.textFaint, fontFamily:"'DM Mono',monospace", fontWeight:700, letterSpacing:1, marginRight:4 }}>QUICK FILTER</span>
          {[
            { key:null,         label:"All",            count:stats.total,     color:"#64748B" },
            { key:"top1k",      label:"🏆 Top 1K",      count:stats.top1k,     color:"#F59E0B" },
            { key:"rising",     label:"↑ Rising",       count:stats.rising,    color:"#059669" },
            { key:"falling",    label:"↓ Falling",      count:stats.falling,   color:"#DC2626" },
            { key:"problem",    label:"⚠ Low Rating",   count:stats.problem,   color:"#EA580C" },
            { key:"zero_sales", label:"💤 Zero Sales",  count:stats.zeroSales, color:"#9CA3AF" },
          ].map(chip => {
            const active = quickFilter === chip.key;
            return (
              <button key={chip.label}
                onClick={() => { setQuickFilter(chip.key); setBsrPageNum(1); }}
                style={{
                  padding:"5px 11px", borderRadius:999, fontSize:11, fontWeight:700, cursor:"pointer", transition:"all 0.15s",
                  background: active ? chip.color : T.surface,
                  color: active ? "#fff" : T.textSoft,
                  border: `1px solid ${active ? chip.color : T.border}`,
                  whiteSpace:"nowrap",
                }}>
                {chip.label} <span style={{ opacity:0.75, fontSize:10, marginLeft:3 }}>{chip.count.toLocaleString("en-IN")}</span>
              </button>
            );
          })}
          {quickFilter !== null && (
            <button onClick={() => { setQuickFilter(null); setBsrPageNum(1); }}
              style={{ marginLeft:"auto", padding:"5px 10px", borderRadius:7, fontSize:10, fontWeight:700, cursor:"pointer",
                       background:T.surfaceSoft, border:`1px solid ${T.border}`, color:T.textMuted }}>
              ✕ Clear filter
            </button>
          )}
        </div>

        {/* Table */}
        <div style={{ borderRadius:12, border:`1px solid ${T.border}`, overflow:"hidden", background:T.surface }}>
          <div style={{ overflowX:"auto", overflowY:"auto", maxHeight:"calc(100vh - 360px)" }}>
            <table style={{ width:"100%", borderCollapse:"separate", borderSpacing:0, fontSize:12, tableLayout:"fixed", minWidth:1100 }}>
              <colgroup>
                {COLUMNS.map(c => <col key={c.key} style={{ width:c.width }} />)}
              </colgroup>
              <thead>
                <tr>
                  {COLUMNS.map(c => {
                    const isActive = sortCol === c.key;
                    const arrow = !c.sortable ? "" : isActive ? (sortDir === "asc" ? " ↑" : " ↓") : " ⇅";
                    return (
                      <th
                        key={c.key}
                        onClick={c.sortable ? () => handleSort(c.key) : undefined}
                        style={{
                          padding:"9px 10px",
                          textAlign:c.align,
                          fontSize:10,
                          fontWeight:700,
                          color: isActive ? "#2563EB" : T.textMuted,
                          background: T.headerBg,
                          borderBottom:`1px solid ${T.border}`,
                          whiteSpace:"nowrap",
                          overflow:"hidden",
                          position:"sticky",
                          top:0,
                          zIndex:2,
                          cursor: c.sortable ? "pointer" : "default",
                          userSelect:"none",
                          boxShadow: `inset 0 -1px 0 ${T.border}`,
                        }}
                        title={c.sortable ? `Sort by ${c.label}` : ""}
                      >
                        {c.label}<span style={{ fontSize:9, opacity: isActive ? 1 : 0.45 }}>{arrow}</span>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
          {paged.map((row, i) => {
            const bc   = BRAND_META[row.brand] || { accent:"#6B7280", light:"#F9FAFB", text:"#4B5563" };
            const tier = getBsrTier(row.bsr);
            const trendColor = row.trend==="up"?"#059669":row.trend==="down"?"#DC2626":"#9CA3AF";
            const trendLabel = row.trend==="up"?"↑ Improving":row.trend==="down"?"↓ Declining":"→ Stable";
            // Drops can be either a count (old Keepa export) or a % (new export).
            // Keepa's "30 days drop %" is: negative = BSR improved (fewer drops = sales down? actually
            // Keepa shows this as % change in rank — negative = rank got better. We treat negative/bigger-negative as good.)
            const dropVal = row.drops_30d || 0;
            let footfallColor, footfallLabel, footfallText;
            if (row.drops_is_pct) {
              // Percentage: negative = rank improved (good), positive = rank worsened (bad)
              footfallColor = dropVal < -10 ? "#059669" : dropVal > 10 ? "#DC2626" : "#D97706";
              footfallLabel = dropVal < -10 ? "rank ↑" : dropVal > 10 ? "rank ↓" : "stable";
              footfallText  = `${dropVal > 0 ? "+" : ""}${dropVal}%`;
            } else {
              // Count: higher = more sales = good
              footfallColor = dropVal >= 20 ? "#059669" : dropVal >= 8 ? "#D97706" : "#DC2626";
              footfallLabel = dropVal >= 20 ? "sells daily" : dropVal >= 8 ? "few/week" : "slow";
              footfallText  = `${dropVal} drops`;
            }
            const stars = row.rating ? Math.round(row.rating) : 0;
            const globalRank = (curPage-1)*PAGE_SIZE + i + 1;
            return (
              <tr key={row.asin}
                onClick={()=>setModalRow(row)}
                onDoubleClick={()=>{ setModalRow(null); setFullPageRow(row); }}
                style={{ borderBottom:`1px solid ${T.surfaceSoft}`, background:i%2===0?T.surface:T.surfaceSoft, cursor:"pointer" }}>

                {/* # */}
                <td style={{ padding:"9px 8px", fontSize:10, color:T.textFaint, fontWeight:600 }}>{globalRank}</td>

                {/* Image thumbnail */}
                <td style={{ padding:"6px 4px", textAlign:"center" }}>
                  {row.image ? (
                    <img src={row.image} alt="" loading="lazy"
                      onError={(e) => { e.currentTarget.style.display = "none"; }}
                      style={{ width:38, height:38, objectFit:"contain", borderRadius:4, background:"#F9FAFB", border:`1px solid ${T.border}` }} />
                  ) : (
                    <div style={{ width:38, height:38, borderRadius:4, background:T.surfaceMuted, border:`1px dashed ${T.border}`, display:"inline-flex", alignItems:"center", justifyContent:"center", fontSize:14, color:T.textFaint }}>📦</div>
                  )}
                </td>

                {/* Title — truncated in cell, hover opens Keepa popup */}
                <td style={{ padding:"9px 10px", maxWidth:240 }}
                  onMouseEnter={(e) => { setHoverRow(row); setHoverPos({ x: e.clientX, y: e.clientY }); }}
                  onMouseMove={(e) => setHoverPos({ x: e.clientX, y: e.clientY })}
                  onMouseLeave={() => setHoverRow(null)}>
                  <div title={row.title || row.asin}
                    style={{
                      fontSize:11, fontWeight:600, color:T.text,
                      overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
                      maxWidth:230, cursor:"pointer",
                      borderBottom: row.title ? "1px dashed #93C5FD" : "none",
                    }}>
                    {row.title || <span style={{ color:T.textFaint, fontStyle:"italic" }}>— no title —</span>}
                  </div>
                </td>

                {/* ASIN */}
                <td style={{ padding:"9px 10px" }}>
                  <span style={{ fontFamily:"'DM Mono',monospace", fontSize:11, color:"#2563EB", fontWeight:700 }}>{row.asin}</span>
                </td>

                {/* Brand */}
                <td style={{ padding:"9px 10px" }}>
                  <span style={{ background:bc.light, color:bc.text, borderRadius:20, padding:"2px 8px", fontSize:10, fontWeight:700, whiteSpace:"nowrap" }}>{row.brand}</span>
                </td>

                {/* Current Rank — may be absent; getBsrTier() already labels that "No data" */}
                <td style={{ padding:"9px 10px" }}>
                  <div style={{ fontSize:13, fontWeight:800, color:tier.color, fontFamily:"monospace" }}>
                    {row.bsr > 0 ? `#${row.bsr.toLocaleString("en-IN")}` : "—"}
                  </div>
                  <div style={{ fontSize:9, color:tier.color, fontWeight:600, marginTop:1 }}>{tier.emoji} {tier.label}</div>
                </td>

                {/* Compare-mode cells */}
                {compareMode && compareDate && (
                  <>
                    {/* Rank on Date B */}
                    <td style={{ padding:"9px 10px", fontSize:12, fontWeight:700, color:T.textSoft, fontFamily:"monospace" }}>
                      {row.bsr_B ? `#${row.bsr_B.toLocaleString("en-IN")}` : "—"}
                    </td>
                    {/* Δ Rank (negative = improved because lower rank is better) */}
                    <td style={{ padding:"9px 10px" }}>
                      {row.bsr_delta === null || row.bsr_delta === undefined ? (
                        <span style={{ color:T.textFaint, fontSize:11 }}>—</span>
                      ) : (() => {
                        const improved = row.bsr_delta < 0;
                        const color = row.bsr_delta === 0 ? "#6B7280" : improved ? "#059669" : "#DC2626";
                        const arrow = row.bsr_delta === 0 ? "→" : improved ? "↑" : "↓";
                        return (
                          <span style={{ fontSize:12, fontWeight:800, color, fontFamily:"monospace" }}>
                            {arrow} {Math.abs(row.bsr_delta).toLocaleString("en-IN")}
                          </span>
                        );
                      })()}
                    </td>
                    {/* Δ % */}
                    <td style={{ padding:"9px 10px" }}>
                      {row.bsr_delta_pct === null || row.bsr_delta_pct === undefined ? (
                        <span style={{ color:T.textFaint, fontSize:11 }}>—</span>
                      ) : (() => {
                        const improved = row.bsr_delta_pct < 0;
                        const color = Math.abs(row.bsr_delta_pct) < 0.5 ? "#6B7280" : improved ? "#059669" : "#DC2626";
                        return (
                          <span style={{ fontSize:11, fontWeight:700, color, fontFamily:"monospace" }}>
                            {row.bsr_delta_pct > 0 ? "+" : ""}{row.bsr_delta_pct.toFixed(1)}%
                          </span>
                        );
                      })()}
                    </td>
                  </>
                )}

                {/* Trend */}
                <td style={{ padding:"9px 10px" }}>
                  <span style={{ fontSize:11, fontWeight:700, color:trendColor }}>{trendLabel}</span>
                </td>

                {/* Rating */}
                <td style={{ padding:"9px 10px" }}>
                  <div style={{ display:"flex", gap:1, alignItems:"center" }}>
                    {[1,2,3,4,5].map(s=>(
                      <div key={s} style={{ width:9, height:9, background:s<=stars?"#F59E0B":"#E5E7EB", clipPath:"polygon(50% 0%,61% 35%,98% 35%,68% 57%,79% 91%,50% 70%,21% 91%,32% 57%,2% 35%,39% 35%)", flexShrink:0 }} />
                    ))}
                    <span style={{ fontSize:11, fontWeight:700, color:T.text, marginLeft:3 }}>{row.rating||"—"}</span>
                  </div>
                </td>

                {/* Reviews */}
                <td style={{ padding:"9px 10px", fontSize:12, fontWeight:700, color:T.text }}>
                  {row.reviews?.toLocaleString("en-IN")||"—"}
                </td>

                {/* 30d avg rank */}
                <td style={{ padding:"9px 10px", fontSize:12, fontWeight:700, color:T.textSoft, fontFamily:"monospace" }}>
                  {row.bsr_30d ? `#${row.bsr_30d.toLocaleString("en-IN")}` : "—"}
                </td>

                {/* Footfall */}
                <td style={{ padding:"9px 10px" }}>
                  <span style={{ fontSize:11, fontWeight:700, color:footfallColor }}>{footfallText}</span>
                  <span style={{ fontSize:9, color:T.textMuted, marginLeft:4 }}>({footfallLabel})</span>
                </td>

                {/* Monthly Sold */}
                <td style={{ padding:"9px 10px", fontSize:12, fontWeight:700, color:T.text }}>
                  {row.monthly_sold ? `${row.monthly_sold}+` : "—"}
                </td>
              </tr>
            );
          })}
          {paged.length === 0 && (
            <tr><td colSpan={COLUMNS.length} style={{ padding:40, textAlign:"center", color:T.textMuted, fontSize:13 }}>No ASINs match your filters</td></tr>
          )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Fixed Bottom toolbar: Export · Pagination (upload moved to top bar) */}
        <div style={{ position:"fixed", left:0, right:0, bottom:0, zIndex:90,
                      padding:"10px 24px", borderTop:`1px solid ${T.border}`,
                      display:"flex", alignItems:"center", gap:12, flexWrap:"wrap",
                      background:T.surface, boxShadow:"0 -4px 14px rgba(15,23,42,0.06)" }}>
          {/* Export */}
          <button onClick={exportCsv}
            style={{ background:T.panelBg, border:`1px solid ${T.border}`, borderRadius:7, padding:"6px 12px", fontSize:11, color:T.textSoft, fontWeight:700, cursor:"pointer", whiteSpace:"nowrap" }}>
            📥 Export CSV
          </button>

          {/* Pagination — pushed to the right */}
          {totalPages > 1 && (
            <div style={{ marginLeft:"auto", display:"flex", alignItems:"center", gap:14 }}>
              <button onClick={()=>setBsrPageNum(p=>Math.max(1,p-1))} disabled={curPage===1}
                style={{ border:`1px solid ${T.border}`, borderRadius:8, padding:"6px 14px", fontSize:11, background:T.surface, color:T.text, cursor:curPage===1?"not-allowed":"pointer", opacity:curPage===1?0.4:1, fontWeight:700 }}>← Prev</button>
              <span style={{ fontSize:12, color:T.textSoft, fontFamily:"'DM Mono',monospace" }}>
                Page {curPage} of {totalPages}
              </span>
              <button onClick={()=>setBsrPageNum(p=>Math.min(totalPages,p+1))} disabled={curPage===totalPages}
                style={{ border:`1px solid ${T.border}`, borderRadius:8, padding:"6px 14px", fontSize:11, background:T.surface, color:T.text, cursor:curPage===totalPages?"not-allowed":"pointer", opacity:curPage===totalPages?0.4:1, fontWeight:700 }}>Next →</button>
            </div>
          )}
        </div>

        {/* Spacer so table bottom doesn't hide behind fixed toolbar */}
        <div style={{ height:60 }} />

        {/* Toast notification */}
        {toast && (
          <div style={{ position:"fixed", bottom:80, right:24, zIndex:999,
                        background: toast.type === "success" ? "#059669" : "#DC2626",
                        color:"#fff", padding:"10px 16px", borderRadius:10,
                        fontSize:12, fontWeight:700, boxShadow:"0 8px 24px rgba(0,0,0,0.15)",
                        animation:"slideInUp 0.2s ease-out" }}>
            {toast.msg}
          </div>
        )}
        <style>{`@keyframes slideInUp { from { transform:translateY(20px); opacity:0; } to { transform:translateY(0); opacity:1; } }`}</style>
      </div>
    </div>
  );
}

export default function Dashboard({ onLogout }) {
  const [dataRows] = useState(RAW_DATA);
  const [masterData] = useState(MASTER_DATA);
  const [brand, setBrand]   = useState([]);
  const [month, setMonth]   = useState([]);
  const [catL0, setCatL0]   = useState([]); // multi-select array (category_l0)
  const [catL1, setCatL1]   = useState([]); // multi-select array (category_l1)
  const [dataMode, setDataMode] = useState("all"); // "all" = combined, "biz" = Amazon Sales (3P), "p1" = 1P Sales
  const [compareAsin, setCompareAsin] = useState("");
  const [compareM1, setCompareM1] = useState("Jan");
  const [compareM2, setCompareM2] = useState("Feb");
  const [aiOpen, setAiOpen] = useState(false);
  const [aiMessages, setAiMessages] = useState([]);
  const [aiInput, setAiInput] = useState("");
  const [aiLoading, setAiLoading] = useState(false);
  const aiEndRef = React.useRef(null);
  const [detailAsin, setDetailAsin] = useState("");
  const [compareTabAsin, setCompareTabAsin] = useState("");
  const [modelTabKey, setModelTabKey] = useState("");
  const [selectedModelMonth, setSelectedModelMonth] = useState("All");
  const [topTenView, setTopTenView] = useState("ASIN");
  const [search, setSearch] = useState("");
  const [debSearch, setDebSearch] = useState("");
  const [sortKey, setSortKey] = useState("NetUnits");
  const [sortDir, setSortDir] = useState(-1);
  const [showExportMenu, setShowExportMenu] = useState(false);
  const [tableScrollTop, setTableScrollTop] = useState(0);
  const [ctTab, setCtTab] = useState("Overview");
  const [ctM1, setCtM1] = useState("Jan");
  const [ctM2, setCtM2] = useState("Feb");
  const [ctAsin, setCtAsin] = useState("");
  const [smartViewTab, setSmartViewTab] = useState("low-buybox");
  const [ctTop10View, setCtTop10View] = useState("ASIN");
  const [ctTop10Brand, setCtTop10Brand] = useState("");
  const [ctTop10MonthMode, setCtTop10MonthMode] = useState("selected");
  const [ctSearch, setCtSearch] = useState("");
  const [smartViewPage, setSmartViewPage] = useState(false);
  const [bsrPage, setBsrPage] = useState(false);
  const [analyticsPage, setAnalyticsPage] = useState(false);
  const [refreshDiffPage, setRefreshDiffPage] = useState(false);
  const [tablePage, setTablePage] = useState(1);
  const [isSearchPending, startT] = useTransition();
  const timer               = useRef(null);
  const tableScrollerRef    = useRef(null);
  const detailChartRef      = useRef(null);
  const comparisonChartRef  = useRef(null);
  const deferredSearch      = useDeferredValue(debSearch);

  // Sync module-level lookup used by getMasterValue / fmtCell helpers.
  // useMemo ensures this runs during render in a deterministic order,
  // before any child reads from ACTIVE_MASTER_DATA.
  useMemo(() => { ACTIVE_MASTER_DATA = masterData; }, [masterData]);

  useEffect(() => {
    if (compareTabAsin) {
      // Use URL hash params first, then fall back to global compareM1/M2 selection
      setCtM1(hashParams.m1 || compareM1 || "Jan");
      setCtM2(hashParams.m2 || compareM2 || "Feb");
      setCtAsin(compareTabAsin);
      setCtTab("Overview");
    }
  }, [compareTabAsin, compareM1, compareM2]);

  useEffect(() => {
    if (!compareTabAsin) {
      return;
    }
    // Track ctAsin, not compareTabAsin.  compareTabAsin is only ever set from
    // the hash, so picking a different ASIN in the ASIN Compare tab (which sets
    // ctAsin) used to leave the URL pointing at the ASIN you arrived on: the
    // page showed one product and the address bar named another, so sharing,
    // bookmarking or reloading silently snapped back to the wrong one.
    // activectAsin resolves the same way in the render path.
    const asinForHash = ctAsin || compareTabAsin;
    const nextHash = `#compare/${encodeURIComponent(asinForHash)}?m1=${ctM1}&m2=${ctM2}`;
    if (window.location.hash !== nextHash) {
      window.history.replaceState(null, "", `${window.location.pathname}${window.location.search}${nextHash}`);
    }
  }, [compareTabAsin, ctAsin, ctM1, ctM2]);

  useEffect(() => {
    const syncDetailFromHash = () => {
      const hash = window.location.hash;
      // Parse #asin/<ASIN>?m1=Jan&m2=Feb
      const asinMatch = hash.match(/^#asin\/([^?]+)(?:\?m1=([^&]+)&m2=(.+))?$/);
      // Parse #compare/<ASIN>?m1=Jan&m2=Feb
      const compareMatch = hash.match(/^#compare\/([^?]+)(?:\?m1=([^&]+)&m2=(.+))?$/);
      // Parse #model/<MODEL>
      const modelMatch = hash.match(/^#model\/(.+)$/);
      const smartViewMatch = hash.match(/^#smart-views(?:\/([^?]+))?$/);

      if (asinMatch) {
        setDetailAsin(decodeURIComponent(asinMatch[1]));
        if (asinMatch[2]) setCompareM1(asinMatch[2]);
        if (asinMatch[3]) setCompareM2(asinMatch[3]);
      } else {
        setDetailAsin("");
      }

      if (compareMatch) {
        setCompareTabAsin(decodeURIComponent(compareMatch[1]));
        if (compareMatch[2]) setCompareM1(compareMatch[2]);
        if (compareMatch[3]) setCompareM2(compareMatch[3]);
      } else {
        setCompareTabAsin("");
      }

      if (modelMatch) {
        setModelTabKey(decodeURIComponent(modelMatch[1]));
        setSelectedModelMonth("All");
      } else {
        setModelTabKey("");
      }

      if (smartViewMatch) {
        const nextTab = smartViewMatch[1] || SMART_VIEW_TABS[0].key;
        setSmartViewPage(true);
        setSmartViewTab(nextTab);
        setBsrPage(false);
        setAnalyticsPage(false);
        setRefreshDiffPage(false);
      } else if (hash === "#bsr-tracker") {
        setBsrPage(true);
        setSmartViewPage(false);
        setAnalyticsPage(false);
        setRefreshDiffPage(false);
      } else if (hash === "#analytics") {
        setAnalyticsPage(true);
        setSmartViewPage(false);
        setBsrPage(false);
        setRefreshDiffPage(false);
      } else if (hash === "#refresh-diff") {
        setRefreshDiffPage(true);
        setSmartViewPage(false);
        setBsrPage(false);
        setAnalyticsPage(false);
      } else {
        setSmartViewPage(false);
        setBsrPage(false);
        setAnalyticsPage(false);
        setRefreshDiffPage(false);
      }
    };

    syncDetailFromHash();
    window.addEventListener("hashchange", syncDetailFromHash);

    return () => window.removeEventListener("hashchange", syncDetailFromHash);
  }, []);

  const brands = useMemo(() => [...new Set(dataRows.map((row) => row.Brand))].sort(), [dataRows]);
  const months = ["All", ...MONTH_SEQUENCE];
  const brandScopedRows = useMemo(
    () => dataRows.filter((row) => brand.length === 0 || brand.includes(row.Brand)),
    [dataRows, brand]
  );
  const catsL0 = useMemo(() => [...new Set(brandScopedRows.map((row) => getCategoryL0(row, masterData)).filter(Boolean))].sort(), [brandScopedRows, masterData]);
  // L1 is scoped by the L0 picks too, so choosing a parent narrows the child
  // list instead of leaving 400 unrelated sub-categories on screen.
  const catsL1 = useMemo(() => [...new Set(
    brandScopedRows
      .filter((row) => catL0.length === 0 || catL0.includes(getCategoryL0(row, masterData)))
      .map((row) => getCategoryL1(row, masterData))
      .filter(Boolean)
  )].sort(), [brandScopedRows, masterData, catL0]);

  const onSearch = useCallback(e => {
    const v = e.target.value; setSearch(v);
    setTablePage(1);
    clearTimeout(timer.current);
    timer.current = setTimeout(() => startT(() => setDebSearch(v)), 250);
  }, []);

  const handleSort = k => { if (sortKey === k) setSortDir(d => -d); else { setSortKey(k); setSortDir(-1); } };

  const enriched = useMemo(() => {
    const seen = new Set();
    const out = [];
    for (let i = 0; i < dataRows.length; i++) {
      const r = dataRows[i];
      const key = r.ASIN + "|" + r.Month + "|" + r.Brand;
      if (seen.has(key)) continue;
      seen.add(key);
      const m = masterData[r.ASIN];
      let row = r;
      if (m) {
        // Merge master data but never overwrite a populated field with a blank/null value.
        const merged = Object.assign(Object.create(null), r);
        for (const k of Object.keys(m)) {
          const mv = m[k];
          const rv = r[k];
          const hasValue = mv !== null && mv !== undefined && mv !== "" && mv !== "nan";
          if (hasValue || (rv === null || rv === undefined || rv === "" || rv === "nan")) {
            merged[k] = mv;
          }
        }
        row = merged;
      }
      // ── Force-recompute ACOS and TACOS from raw values ──────────────────
      // This overrides any precomputed (and potentially buggy) values from raw_data.json.
      // ACOS  = AdSpend / AdSales
      // TACOS = AdSpend / TotalNetSales  (Net Sales already includes ad sales + organic)
      const adSpend = row.TotalAdsSpend || 0;
      const adSales = row.TotalAdsSales || 0;
      const netSales = row.TotalNetSalesValue || 0;
      row = Object.assign(Object.create(null), row, {
        ACOS:  adSales > 0  ? adSpend / adSales  : 0,
        TACOS: netSales > 0 ? adSpend / netSales : 0,
      });
      out.push(row);
    }
    return out;

  }, [dataRows, masterData]);

  const filtered = useMemo(() => {
    const q = deferredSearch.toLowerCase();
    return enriched.filter(r =>
      (brand.length === 0 || brand.includes(r.Brand)) &&
      (month.length === 0 || month.includes(r.Month)) &&
      (catL0.length === 0 || catL0.includes(getCategoryL0(r, masterData))) &&
      (catL1.length === 0 || catL1.includes(getCategoryL1(r, masterData))) &&
      (!q || (r.ASIN||"").toLowerCase().includes(q) || (typeof r.Title==="string"&&r.Title.toLowerCase().includes(q)) || (r.fbaSku||"").toLowerCase().includes(q) || (r.model||"").toLowerCase().includes(q))
    );
  }, [enriched, brand, month, catL0, catL1, deferredSearch, masterData]);

  const aggregatedRows = useMemo(() => {
    if (month.length <= 1) return filtered;

    const grouped = new Map();
    filtered.forEach((row) => {
      const key = `${row.Brand}|${row.ASIN}`;
      const current = grouped.get(key);
      if (!current) {
        grouped.set(key, {
          ...row,
          Month: month.join("+"),
          Sessions: row.Sessions ?? 0,
          BuyboxPctWeightedTotal: (row.BuyboxPct ?? 0) * (row.Sessions ?? 0),
          BuyboxPctWeight: row.Sessions ?? 0,
          NetUnits: row.NetUnits ?? 0,
          Units1P: row.Units1P ?? 0,
          Units3P: row.Units3P ?? 0,
          TotalNetSalesValue: row.TotalNetSalesValue ?? 0,
          Rev1P: row.Rev1P ?? 0,
          Rev3P: row.Rev3P ?? 0,
          TotalAdsSpend: row.TotalAdsSpend ?? 0,
          TotalAdsSales: row.TotalAdsSales ?? 0,
          Impressions: row.Impressions ?? 0,
          Clicks: row.Clicks ?? 0,
          AmsOrders: row.AmsOrders ?? 0,
          OrganiSales: row.OrganiSales ?? 0,
        });
        return;
      }

      current.Sessions += row.Sessions ?? 0;
      current.BuyboxPctWeightedTotal += (row.BuyboxPct ?? 0) * (row.Sessions ?? 0);
      current.BuyboxPctWeight += row.Sessions ?? 0;
      current.NetUnits += row.NetUnits ?? 0;
      current.Units1P += row.Units1P ?? 0;
      current.Units3P += row.Units3P ?? 0;
      current.TotalNetSalesValue += row.TotalNetSalesValue ?? 0;
      current.Rev1P += row.Rev1P ?? 0;
      current.Rev3P += row.Rev3P ?? 0;
      current.TotalAdsSpend += row.TotalAdsSpend ?? 0;
      current.TotalAdsSales += row.TotalAdsSales ?? 0;
      current.Impressions += row.Impressions ?? 0;
      current.Clicks += row.Clicks ?? 0;
      current.AmsOrders += row.AmsOrders ?? 0;
      current.OrganiSales += row.OrganiSales ?? 0;
    });

    return [...grouped.values()].map((row) => {
      const buyboxPct = row.BuyboxPctWeight > 0 ? row.BuyboxPctWeightedTotal / row.BuyboxPctWeight : row.BuyboxPct ?? 0;
      const totalSales = row.TotalNetSalesValue ?? 0;
      const adSales = row.TotalAdsSales ?? 0;
      const adSpend = row.TotalAdsSpend ?? 0;
      const units = row.NetUnits ?? 0;
      const sessions = row.Sessions ?? 0;
      const organicSales = Math.max(0, totalSales - adSales);

      const { BuyboxPctWeightedTotal, BuyboxPctWeight, ...cleanRow } = row;
      return {
        ...cleanRow,
        BuyboxPct: buyboxPct,
        ACOS: adSales > 0 ? adSpend / adSales : 0,
        TACOS: totalSales > 0 ? adSpend / totalSales : 0,
        CAC: (row.AmsOrders ?? 0) > 0 ? adSpend / row.AmsOrders : 0,
        ConversionPct: sessions > 0 ? units / sessions : 0,
        OrganicPct: totalSales > 0 ? organicSales / totalSales : 0,
      };
    });
  }, [filtered, month]);

  const comparePool = useMemo(() => {
    return enriched.filter((row) =>
      (brand.length === 0 || brand.includes(row.Brand)) &&
      (!deferredSearch || (row.ASIN || "").toLowerCase().includes(deferredSearch.toLowerCase()) || String(row.Title || "").toLowerCase().includes(deferredSearch.toLowerCase()))
    );
  }, [enriched, brand, deferredSearch]);

  const compareOptions = useMemo(() => {
    const map = new Map();
    comparePool.forEach((row) => {
      if (!map.has(row.ASIN)) map.set(row.ASIN, row);
    });
    return [...map.values()].sort((a, b) => (b.TotalNetSalesValue ?? 0) - (a.TotalNetSalesValue ?? 0));
  }, [comparePool]);

  const activeCompareAsin = useMemo(() => {
    if (compareAsin && compareOptions.some((row) => row.ASIN === compareAsin)) return compareAsin;
    return compareOptions[0]?.ASIN ?? "";
  }, [compareAsin, compareOptions]);

  const compareRows = useMemo(() => {
    return comparePool
      .filter((row) => row.ASIN === activeCompareAsin)
      .sort((a, b) => months.indexOf(a.Month) - months.indexOf(b.Month));
  }, [comparePool, activeCompareAsin]);

  const comparison = useMemo(() => {
    const m1row = compareRows.find((row) => row.Month === compareM1) ?? null;
    const m2row = compareRows.find((row) => row.Month === compareM2) ?? null;
    const selectedMeta = compareOptions.find((row) => row.ASIN === activeCompareAsin) ?? m1row ?? m2row ?? null;
    return { m1: m1row, m2: m2row, selectedMeta };
  }, [compareRows, compareOptions, activeCompareAsin, compareM1, compareM2]);

  const comparisonCards = useMemo(() => ([
    { label: "Net Units", jan: comparison.m1?.NetUnits ?? 0, feb: comparison.m2?.NetUnits ?? 0, type: "number", colorA: "#34D399", colorB: "#2DD4BF" },
    { label: "Net Sales", jan: comparison.m1?.TotalNetSalesValue ?? 0, feb: comparison.m2?.TotalNetSalesValue ?? 0, type: "currency", colorA: "#A78BFA", colorB: "#F472B6" },
    { label: "Sessions", jan: comparison.m1?.Sessions ?? 0, feb: comparison.m2?.Sessions ?? 0, type: "number", colorA: "#38BDF8", colorB: "#60A5FA" },
    { label: "Buybox", jan: (comparison.m1?.BuyboxPct ?? 0) * 100, feb: (comparison.m2?.BuyboxPct ?? 0) * 100, type: "number", colorA: "#FBBF24", colorB: "#FB7185" },
    { label: "Ad Sales", jan: comparison.m1?.TotalAdsSales ?? 0, feb: comparison.m2?.TotalAdsSales ?? 0, type: "currency", colorA: "#EC4899", colorB: "#FB7185" },
    { label: "Ad Spend", jan: comparison.m1?.TotalAdsSpend ?? 0, feb: comparison.m2?.TotalAdsSpend ?? 0, type: "currency", colorA: "#FB7185", colorB: "#EF4444" },
    { label: "Clicks", jan: comparison.m1?.Clicks ?? 0, feb: comparison.m2?.Clicks ?? 0, type: "number", colorA: "#0EA5E9", colorB: "#2563EB" },
    { label: "AMS Orders", jan: comparison.m1?.AmsOrders ?? 0, feb: comparison.m2?.AmsOrders ?? 0, type: "number", colorA: "#8B5CF6", colorB: "#7C3AED" },
    { label: "ACOS", jan: (comparison.m1?.ACOS ?? 0) * 100, feb: (comparison.m2?.ACOS ?? 0) * 100, type: "number", colorA: "#F59E0B", colorB: "#F97316" },
    { label: "TACOS", jan: (comparison.m1?.TACOS ?? 0) * 100, feb: (comparison.m2?.TACOS ?? 0) * 100, type: "number", colorA: "#10B981", colorB: "#059669" },
    { label: "Organic Sales", jan: comparison.m1?.OrganiSales ?? 0, feb: comparison.m2?.OrganiSales ?? 0, type: "currency", colorA: "#38BDF8", colorB: "#60A5FA" },
    { label: "Impressions", jan: comparison.m1?.Impressions ?? 0, feb: comparison.m2?.Impressions ?? 0, type: "number", colorA: "#A78BFA", colorB: "#8B5CF6" },
  ]), [comparison]);

  const brandMonthTotals = useMemo(() => {
    const sourceRows = enriched.filter((row) => brand.length === 0 || brand.includes(row.Brand));
    const summarizeMonth = (targetMonth) => sourceRows
      .filter((row) => row.Month === targetMonth)
      .reduce((acc, row) => ({
        sessions: acc.sessions + (row.Sessions ?? 0),
        units: acc.units + (row.NetUnits ?? 0),
        sales: acc.sales + (row.TotalNetSalesValue ?? 0),
        spend: acc.spend + (row.TotalAdsSpend ?? 0),
        adsSales: acc.adsSales + (row.TotalAdsSales ?? 0),
        orders: acc.orders + (row.AmsOrders ?? 0),
      }), { sessions: 0, units: 0, sales: 0, spend: 0, adsSales: 0, orders: 0 });

    return {
      m1: summarizeMonth(compareM1),
      m2: summarizeMonth(compareM2),
    };
  }, [enriched, brand, compareM1, compareM2]);

  const detailRows = useMemo(
    () => enriched.filter((row) => row.ASIN === detailAsin).sort((a, b) => months.indexOf(a.Month) - months.indexOf(b.Month)),
    [enriched, detailAsin]
  );

  // Model tab: all rows matching this model key across all months
  const modelTabRows = useMemo(
    () => enriched.filter((row) => (getMasterValue(row, "model") || row.model || "") === modelTabKey)
           .sort((a, b) => months.indexOf(a.Month) - months.indexOf(b.Month)),
    [enriched, modelTabKey]
  );
  const modelTabMeta = modelTabRows[0] ?? null;
  const detailMeta = detailRows[0] ?? null;
  // Use compareM1/compareM2 directly — no hardcoded Jan/Feb fallback
  const detailM1 = detailRows.find((row) => row.Month === compareM1) ?? null;
  const detailM2 = detailRows.find((row) => row.Month === compareM2) ?? null;
  const detailGrowth = useMemo(() => {
    const metrics = [
      { key: "sales", label: "Sales Growth", jan: detailM1?.TotalNetSalesValue ?? 0, feb: detailM2?.TotalNetSalesValue ?? 0 },
      { key: "ads", label: "Ad Sales Growth", jan: detailM1?.TotalAdsSales ?? 0, feb: detailM2?.TotalAdsSales ?? 0 },
      { key: "units", label: "Units Growth", jan: detailM1?.NetUnits ?? 0, feb: detailM2?.NetUnits ?? 0 },
      { key: "buybox", label: "Buybox Growth", jan: detailM1?.BuyboxPct ?? 0, feb: detailM2?.BuyboxPct ?? 0 },
    ];

    return metrics.map((metric) => {
      const delta = metric.feb - metric.jan;
      const growthPct = metric.jan > 0 ? (delta / metric.jan) * 100 : metric.feb > 0 ? 100 : 0;
      return { ...metric, delta, growthPct };
    });
  }, [detailM1, detailM2]);

  const compareTabRows = useMemo(
    () => enriched.filter((row) => row.ASIN === compareTabAsin).sort((a, b) => months.indexOf(a.Month) - months.indexOf(b.Month)),
    [enriched, compareTabAsin]
  );
  // Parse compareM1/M2 from URL hash if present
  // hashParams reads from compareM1/compareM2 state which are kept in sync by the hash listener
  const hashParams = useMemo(() => {
    return { m1: compareM1 || "Jan", m2: compareM2 || "Feb" };
  }, [compareM1, compareM2]);
  const compareTabMeta = compareTabRows[0] ?? null;
  const compareTabM1 = compareTabRows.find((row) => row.Month === hashParams.m1) ?? null;
  const compareTabM2 = compareTabRows.find((row) => row.Month === hashParams.m2) ?? null;
  const compareTabMetrics = useMemo(() => ([
    { key: "sales", label: "Net Sales", jan: compareTabM1?.TotalNetSalesValue ?? 0, feb: compareTabM2?.TotalNetSalesValue ?? 0, type: "currency", colorA: "#2563EB", colorB: "#7C3AED" },
    { key: "units", label: "Net Units", jan: compareTabM1?.NetUnits ?? 0, feb: compareTabM2?.NetUnits ?? 0, type: "number", colorA: "#10B981", colorB: "#059669" },
    { key: "sessions", label: "Sessions", jan: compareTabM1?.Sessions ?? 0, feb: compareTabM2?.Sessions ?? 0, type: "number", colorA: "#06B6D4", colorB: "#0284C7" },
    { key: "buybox", label: "Buybox %", jan: (compareTabM1?.BuyboxPct ?? 0) * 100, feb: (compareTabM2?.BuyboxPct ?? 0) * 100, type: "number", colorA: "#F59E0B", colorB: "#F97316" },
    { key: "adsSales", label: "Ad Sales", jan: compareTabM1?.TotalAdsSales ?? 0, feb: compareTabM2?.TotalAdsSales ?? 0, type: "currency", colorA: "#EC4899", colorB: "#FB7185" },
    { key: "adsSpend", label: "Ad Spend", jan: compareTabM1?.TotalAdsSpend ?? 0, feb: compareTabM2?.TotalAdsSpend ?? 0, type: "currency", colorA: "#FB7185", colorB: "#EF4444" },
    { key: "clicks", label: "Clicks", jan: compareTabM1?.Clicks ?? 0, feb: compareTabM2?.Clicks ?? 0, type: "number", colorA: "#0EA5E9", colorB: "#2563EB" },
    { key: "orders", label: "AMS Orders", jan: compareTabM1?.AmsOrders ?? 0, feb: compareTabM2?.AmsOrders ?? 0, type: "number", colorA: "#8B5CF6", colorB: "#7C3AED" },
    { key: "impressions", label: "Impressions", jan: compareTabM1?.Impressions ?? 0, feb: compareTabM2?.Impressions ?? 0, type: "number", colorA: "#38BDF8", colorB: "#60A5FA" },
    { key: "acos", label: "ACOS", jan: (compareTabM1?.ACOS ?? 0) * 100, feb: (compareTabM2?.ACOS ?? 0) * 100, type: "number", colorA: "#F59E0B", colorB: "#F97316" },
    { key: "tacos", label: "TACOS", jan: (compareTabM1?.TACOS ?? 0) * 100, feb: (compareTabM2?.TACOS ?? 0) * 100, type: "number", colorA: "#10B981", colorB: "#059669" },
    { key: "organic", label: "Organic Sales", jan: compareTabM1?.OrganiSales ?? 0, feb: compareTabM2?.OrganiSales ?? 0, type: "currency", colorA: "#34D399", colorB: "#2DD4BF" },
    { key: "cac", label: "CAC ₹", jan: compareTabM1?.CAC ?? 0, feb: compareTabM2?.CAC ?? 0, type: "currency", colorA: "#A78BFA", colorB: "#8B5CF6" },
    { key: "cvr", label: "Conversion %", jan: (compareTabM1?.ConversionPct ?? 0) * 100, feb: (compareTabM2?.ConversionPct ?? 0) * 100, type: "number", colorA: "#34D399", colorB: "#10B981" },
    { key: "organicpct", label: "Organic %", jan: (compareTabM1?.OrganicPct ?? 0) * 100, feb: (compareTabM2?.OrganicPct ?? 0) * 100, type: "number", colorA: "#0EA5E9", colorB: "#2563EB" },
  ].map((metric) => {
    const delta = metric.feb - metric.jan;
    const growthPct = metric.jan > 0 ? (delta / metric.jan) * 100 : metric.feb > 0 ? 100 : 0;
    return { ...metric, delta, growthPct };
  })), [compareTabM1, compareTabM2]);


  // Remap metric fields per row based on active data mode.
  // Defined here (before smartViewSortedRows) so it's available to all table renders.
  const applyDataMode = React.useCallback((row) => {
    if (dataMode === "all") return row; // combined — row already has TotalNetSalesValue = Rev1P+Rev3P
    if (dataMode === "biz") {
      // Amazon Sales mode — show only 3P revenue/units
      const m = getMetrics(row, "biz");
      return Object.assign(Object.create(null), row, {
        NetUnits:            row.Units3P || 0,
        TotalNetSalesValue:  row.Rev3P   || 0,
        ACOS:                (row.TotalAdsSales||0) > 0 ? (row.TotalAdsSpend||0)/(row.TotalAdsSales||0) : 0,
        TACOS:               (row.Rev3P||0) > 0 ? (row.TotalAdsSpend||0)/(row.Rev3P||0) : 0,
        OrganiSales:         Math.max(0,(row.Rev3P||0)-(row.TotalAdsSales||0)),
        OrganicPct:          (row.Rev3P||0) > 0 ? Math.max(0,(row.Rev3P||0)-(row.TotalAdsSales||0))/(row.Rev3P||0) : 0,
        ConversionPct:       (row.Sessions||0) > 0 ? (row.Units3P||0)/row.Sessions : 0,
      });
    }
    const m = getMetrics(row, "p1");
    return Object.assign(Object.create(null), row, {
      Sessions:            null,
      BuyboxPct:           null,
      NetUnits:            m.units,
      Units3P:             null,
      Units1P:             m.units,
      TotalNetSalesValue:  m.revenue,
      Rev3P:               null,
      Rev1P:               m.revenue,
      TotalAdsSpend:       m.adSpend,
      TotalAdsSales:       m.adSales,
      ACOS:                m.acos,
      TACOS:               m.tacos,
      CAC:                 m.cac,
      AmsOrders:           m.amsOrders,
      Impressions:         m.impressions,
      Clicks:              m.clicks,
      OrganiSales:         null,
      OrganicPct:          null,
      ConversionPct:       null,
    });
  }, [dataMode]);

  // Monthly sales contribution % — Option B: each ASIN's combined (1P+3P) sales
  // as a share of the FULL Brand + Month total, computed across the entire
  // dataset (NOT the filtered/visible rows). This keeps each row's % stable
  // regardless of search box, category, or smart-view filters.
  // Denominator respects the active data mode so it matches the visible Net Sales.
  const contribBrandMonthTotals = useMemo(() => {
    const totals = new Map();
    for (const r of enriched.map(applyDataMode)) {
      const g = `${r.Brand}||${r.Month}`;
      totals.set(g, (totals.get(g) || 0) + (r.TotalNetSalesValue || 0));
    }
    return totals;
  }, [enriched, applyDataMode]);

  const withSalesContrib = React.useCallback((rows) => {
    return rows.map((r) => {
      const denom = contribBrandMonthTotals.get(`${r.Brand}||${r.Month}`) || 0;
      return Object.assign(Object.create(null), r, {
        salesContribPct: denom > 0 ? (r.TotalNetSalesValue || 0) / denom : null,
      });
    });
  }, [contribBrandMonthTotals]);

  const sorted = useMemo(() => {
    const col = COLS.find(c => c.key === sortKey);
    const isMaster = col?.master;
    return [...aggregatedRows].sort((a, b) => {
      const av = isMaster ? getMasterValue(a, sortKey) : (a[sortKey] ?? null);
      const bv = isMaster ? getMasterValue(b, sortKey) : (b[sortKey] ?? null);
      return compareValues(av, bv, sortDir);
    });
  }, [aggregatedRows, sortKey, sortDir]);

  const visibleRows = sorted;
  const totalPages = Math.max(1, Math.ceil(visibleRows.length / TABLE_PAGE_SIZE));
  const currentPage = Math.min(tablePage, totalPages);
  const pagedRows = withSalesContrib(visibleRows.map(applyDataMode)).slice((currentPage - 1) * TABLE_PAGE_SIZE, currentPage * TABLE_PAGE_SIZE);
  const rankingMetricKey = useMemo(() => {
    const currentCol = COLS.find((column) => column.key === sortKey);
    return currentCol && currentCol.fmt !== "str" && currentCol.fmt !== "title" ? sortKey : "TotalNetSalesValue";
  }, [sortKey]);
  const topTenRows = useMemo(() => {
    const groups = new Map();

    filtered.forEach((row) => {
      const masterModel = getMasterValue(row, "model") || "Unmapped";
      const groupKey = topTenView === "ASIN" ? row.ASIN : masterModel;
      if (!groupKey) return;

      const current = groups.get(groupKey) ?? {
        key: groupKey,
        asin: row.ASIN,
        model: masterModel,
        title: row.Title,
        value: 0,
        brand: row.Brand,
      };

      const increment = Number(row[rankingMetricKey] ?? getMasterValue(row, rankingMetricKey) ?? 0) || 0;
      current.value += increment;
      groups.set(groupKey, current);
    });

    return [...groups.values()]
      .sort((a, b) => b.value - a.value)
      .slice(0, 10);
  }, [filtered, topTenView, rankingMetricKey]);
  const compareSelectedLabel = useMemo(() => {
    const row = compareOptions.find((item) => item.ASIN === activeCompareAsin) ?? null;
    if (!row) return "No ASIN selected";
    return `${row.ASIN} - ${String(row.Title || "Product").slice(0, 48)}`;
  }, [compareOptions, activeCompareAsin]);

  const activeSmartView = useMemo(
    () => SMART_VIEW_TABS.find((view) => view.key === smartViewTab) ?? SMART_VIEW_TABS[0],
    [smartViewTab]
  );

  const smartViewRows = useMemo(() => (
    enriched.filter((row) => {
      const q = deferredSearch.toLowerCase();
      return (
        (brand.length === 0 || brand.includes(row.Brand)) &&
        (month.length === 0 || month.includes(row.Month)) &&
        (catL0.length === 0 || catL0.includes(getCategoryL0(row, masterData))) &&
        (catL1.length === 0 || catL1.includes(getCategoryL1(row, masterData))) &&
        (!q || (row.ASIN || "").toLowerCase().includes(q) || String(row.Title || "").toLowerCase().includes(q) || (row.fbaSku || "").toLowerCase().includes(q) || (row.model || "").toLowerCase().includes(q)) &&
        activeSmartView.matches(row)
      );
    })
  ), [enriched, brand, month, catL0, catL1, deferredSearch, masterData, activeSmartView]);
  const smartViewSortedRows = useMemo(() => {
    const col = COLS.find((column) => column.key === sortKey);
    const isMaster = col?.master;
    const stamped = withSalesContrib([...smartViewRows].map(applyDataMode));
    return stamped.sort((a, b) => {
      const av = isMaster ? getMasterValue(a, sortKey) : (a[sortKey] ?? null);
      const bv = isMaster ? getMasterValue(b, sortKey) : (b[sortKey] ?? null);
      return compareValues(av, bv, sortDir);
    });
  }, [smartViewRows, sortKey, sortDir, dataMode, withSalesContrib]);
  const smartViewTotals = useMemo(() => smartViewRows.reduce((acc, row) => ({
    rows: acc.rows + 1,
    sales: acc.sales + (row.TotalNetSalesValue ?? 0),
    spend: acc.spend + (row.TotalAdsSpend ?? 0),
    sessions: acc.sessions + (row.Sessions ?? 0),
  }), { rows: 0, sales: 0, spend: 0, sessions: 0 }), [smartViewRows]);
  const smartViewAvgBuybox = smartViewRows.length > 0
    ? smartViewRows.reduce((sum, row) => sum + (row.BuyboxPct ?? 0), 0) / smartViewRows.length
    : 0;

  // Totals — recalculate based on active data mode
  const totals = useMemo(() => filtered.reduce((acc, r) => {
    const m = getMetrics(r, dataMode);
    return {
      sessions: acc.sessions + (m.sessions ?? 0),
      units:    acc.units    + m.units,
      sales:    acc.sales    + m.revenue,
      spend:    acc.spend    + m.adSpend,
      adsSales: acc.adsSales + m.adSales,
      orders:   acc.orders   + m.amsOrders,
    };
  }, { sessions:0, units:0, sales:0, spend:0, adsSales:0, orders:0 }), [filtered, dataMode]);

  const avgAcos  = totals.adsSales > 0 ? totals.spend / totals.adsSales : 0;
  const avgTacos = totals.sales > 0 ? totals.spend / totals.sales : 0;
  const activeFilterCount = [brand.length > 0 ? "b" : "", month.length > 0 ? "m" : "", catL0.length > 0 ? "c0" : "", catL1.length > 0 ? "c1" : "", debSearch].filter(Boolean).length;

  // Prev month totals for trend indicators
  const monthOrder = MONTH_SEQUENCE;
  const prevMonth = month.length === 1 ? monthOrder[monthOrder.indexOf(month[0]) - 1] : null;
  const prevFiltered = useMemo(() => prevMonth ? enriched.filter(r =>
    (brand.length === 0 || brand.includes(r.Brand)) && r.Month === prevMonth &&
    (catL0.length === 0 || catL0.includes(getCategoryL0(r, masterData))) &&
    (catL1.length === 0 || catL1.includes(getCategoryL1(r, masterData)))
  ) : [], [enriched, brand, prevMonth, catL0, catL1, masterData]);
  const prevTotals = useMemo(() => prevFiltered.reduce((acc, r) => {
    const m = getMetrics(r, dataMode);
    return {
      sessions: acc.sessions + (m.sessions ?? 0),
      units:    acc.units    + m.units,
      sales:    acc.sales    + m.revenue,
      spend:    acc.spend    + m.adSpend,
      adsSales: acc.adsSales + m.adSales,
      orders:   acc.orders   + m.amsOrders,
    };
  }, { sessions:0, units:0, sales:0, spend:0, adsSales:0, orders:0 }), [prevFiltered, dataMode]);
  const trendPct = (curr, prev) => prev > 0 ? ((curr - prev) / prev) * 100 : null;

  const ACCENT = "#3B82F6";
  const openAppTab = (hash) => {
    window.open(`${window.location.pathname}${window.location.search}${hash}`, "_blank", "noopener,noreferrer");
  };
  const openDetailPage = (asin) => {
    if (!asin) return;
    openAppTab(`#asin/${encodeURIComponent(asin)}?m1=${compareM1}&m2=${compareM2}`);
  };
  const openModelPage = (modelKey) => {
    if (!modelKey) return;
    openAppTab(`#model/${encodeURIComponent(modelKey)}`);
  };
  const openComparePage = (asin) => {
    if (!asin) return;
    openAppTab(`#compare/${encodeURIComponent(asin)}?m1=${compareM1}&m2=${compareM2}`);
  };
  const openSmartViewPage = (tabKey = SMART_VIEW_TABS[0].key) => {
    openAppTab(`#smart-views/${tabKey}`);
  };
  const openAnalyticsPage = () => {
    openAppTab("#analytics");
  };
  const switchSmartViewTab = (tabKey) => {
    setSmartViewTab(tabKey);
    // Use replaceState so switching tabs doesn't pile up history entries.
    // This way, one Back click always returns to dashboard regardless of how many tabs you clicked.
    const newUrl = `${window.location.pathname}${window.location.search}#smart-views/${tabKey}`;
    window.history.replaceState(null, "", newUrl);
  };
  const closeDetailPage = () => {
    // Clear hash directly — always returns to main dashboard in one click,
    // regardless of tab history. Fires hashchange event → React updates state → no reload.
    if (window.location.hash) {
      window.history.replaceState(null, "", window.location.pathname + window.location.search);
      // Manually dispatch hashchange since replaceState doesn't fire it
      window.dispatchEvent(new HashChangeEvent("hashchange"));
    }
  };
  const exportFilteredData = () => {
    const exportColumns = [
      "ASIN", "fbaSku", "model", "catL0", "catL1", "Title", "Brand", "Month",
      "Sessions", "BuyboxPct", "NetUnits", "TotalNetSalesValue", "Impressions", "Clicks",
      "AmsOrders", "OrganiSales", "TotalAdsSpend", "TotalAdsSales", "ACOS", "TACOS", "CAC", "ConversionPct",
    ];

    const sourceRows = smartViewPage ? smartViewRows : aggregatedRows;
    const rows = sourceRows.map((row) => ({
      ...row,
      fbaSku: getMasterValue(row, "fbaSku"),
      model: getMasterValue(row, "model"),
      catL0: getMasterValue(row, "catL0"),
      catL1: getMasterValue(row, "catL1"),
      mainCat: row.mainCat || getMasterValue(row, "mainCat"),
    }));
    const brandPart = brand || "all-brands";
    const monthPart = month || "all-months";
    const smartPart = smartViewPage ? `${activeSmartView.key}-` : "";
    exportRowsToCsv(`buybox-${smartPart}table-${brandPart}-${monthPart}.csv`, rows, exportColumns);
    setShowExportMenu(false);
  };

  const exportTopTenData = () => {
    const rows = topTenRows.map((item, index) => ({
      Rank: index + 1,
      View: topTenView,
      Key: item.key,
      ASIN: item.asin,
      Brand: item.brand,
      Title: item.title,
      Metric: COLS.find((column) => column.key === rankingMetricKey)?.label || rankingMetricKey,
      Value: item.value,
    }));
    exportRowsToCsv(`buybox-top10-${topTenView.toLowerCase()}.csv`, rows, ["Rank", "View", "Key", "ASIN", "Brand", "Title", "Metric", "Value"]);
    setShowExportMenu(false);
  };

  const exportDetailData = () => {
    if (!detailRows.length) return;
    const rows = detailRows.map((row) => ({
      Month: row.Month,
      ASIN: row.ASIN,
      Brand: row.Brand,
      Title: row.Title,
      FbaSku: getMasterValue(row, "fbaSku"),
      Model: getMasterValue(row, "model"),
      CategoryL0: getMasterValue(row, "catL0"),
      CategoryL1: getMasterValue(row, "catL1"),
      Sessions: row.Sessions,
      BuyboxPct: row.BuyboxPct,
      NetUnits: row.NetUnits,
      NetSales: row.TotalNetSalesValue,
      AdsSpend: row.TotalAdsSpend,
      AdsSales: row.TotalAdsSales,
      AmsOrders: row.AmsOrders,
      Impressions: row.Impressions,
      Clicks: row.Clicks,
    }));
    exportRowsToCsv(`buybox-asin-${detailMeta.ASIN}.csv`, rows, ["Month", "ASIN", "Brand", "Title", "FbaSku", "Model", "Category", "Sessions", "BuyboxPct", "NetUnits", "NetSales", "AdsSpend", "AdsSales", "AmsOrders", "Impressions", "Clicks"]);
  };

  const printCurrentView = () => {
    window.print();
    setShowExportMenu(false);
  };

  const analyticsTotals = useMemo(() => filtered.reduce((acc, row) => {
    const metrics = getMetrics(row, dataMode);
    acc.sales += metrics.revenue || 0;
    acc.units += metrics.units || 0;
    acc.sessions += metrics.sessions || 0;
    acc.spend += metrics.adSpend || 0;
    acc.adSales += metrics.adSales || 0;
    acc.orders += metrics.amsOrders || 0;
    acc.impressions += row.Impressions || 0;
    acc.clicks += row.Clicks || 0;
    return acc;
  }, {
    sales: 0,
    units: 0,
    sessions: 0,
    spend: 0,
    adSales: 0,
    orders: 0,
    impressions: 0,
    clicks: 0,
  }), [filtered, dataMode]);

  const analyticsBrandShare = useMemo(() => {
    const rows = filtered.reduce((acc, row) => {
      const sales = getMetrics(row, dataMode).revenue || 0;
      acc[row.Brand] = (acc[row.Brand] || 0) + sales;
      return acc;
    }, {});

    return Object.entries(rows)
      .map(([label, value]) => ({
        label,
        value,
        share: analyticsTotals.sales > 0 ? value / analyticsTotals.sales : 0,
      }))
      .sort((a, b) => b.value - a.value);
  }, [filtered, dataMode, analyticsTotals.sales]);

  const analyticsCategoryShare = useMemo(() => {
    const rows = filtered.reduce((acc, row) => {
      const key = getCategoryL0(row, masterData) || "Unmapped";
      const sales = getMetrics(row, dataMode).revenue || 0;
      acc[key] = (acc[key] || 0) + sales;
      return acc;
    }, {});

    return Object.entries(rows)
      .map(([label, value]) => ({
        label,
        value,
        share: analyticsTotals.sales > 0 ? value / analyticsTotals.sales : 0,
      }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 8);
  }, [filtered, dataMode, masterData, analyticsTotals.sales]);

  const analyticsMonthSeries = useMemo(() => {
    const available = MONTH_SEQUENCE.filter((monthLabel) => filtered.some((row) => row.Month === monthLabel));
    return available.map((monthLabel) => {
      const monthRows = filtered.filter((row) => row.Month === monthLabel);
      const total = monthRows.reduce((acc, row) => {
        const metrics = getMetrics(row, dataMode);
        acc.sales += metrics.revenue || 0;
        acc.units += metrics.units || 0;
        acc.spend += metrics.adSpend || 0;
        return acc;
      }, { sales: 0, units: 0, spend: 0 });
      return { month: monthLabel, ...total };
    });
  }, [filtered, dataMode]);

  const analyticsComparison = useMemo(() => {
    const availableMonths = analyticsMonthSeries.map((item) => item.month);
    const currentMonth = month.length === 1 && availableMonths.includes(month[0])
      ? month[0]
      : availableMonths[availableMonths.length - 1];
    const currentIndex = MONTH_SEQUENCE.indexOf(currentMonth);
    const previousMonth = currentIndex > 0
      ? [...availableMonths].reverse().find((item) => MONTH_SEQUENCE.indexOf(item) < currentIndex)
      : null;

    if (!currentMonth || !previousMonth) {
      return { currentMonth: currentMonth || "", previousMonth: previousMonth || "", movers: [] };
    }

    const asinMap = {};
    filtered.forEach((row) => {
      if (row.Month !== currentMonth && row.Month !== previousMonth) {
        return;
      }
      if (!asinMap[row.ASIN]) {
        asinMap[row.ASIN] = {
          asin: row.ASIN,
          title: row.Title,
          brand: row.Brand,
          months: {},
        };
      }
      asinMap[row.ASIN].months[row.Month] = getMetrics(row, dataMode).revenue || 0;
    });

    const movers = Object.values(asinMap)
      .map((item) => {
        const prev = item.months[previousMonth] || 0;
        const curr = item.months[currentMonth] || 0;
        const delta = curr - prev;
        const pctChange = prev > 0 ? (delta / prev) * 100 : curr > 0 ? 100 : 0;
        return { ...item, prev, curr, delta, pctChange };
      })
      .filter((item) => item.prev > 0 || item.curr > 0)
      .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))
      .slice(0, 8);

    return { currentMonth, previousMonth, movers };
  }, [analyticsMonthSeries, filtered, month, dataMode]);

  if (bsrPage) {
    return <BsrTrackerPage onBack={() => { window.location.hash = ""; }} />;
  }

  if (refreshDiffPage) {
    return <RefreshDiffPage data={JUNE_DIFF} onBack={() => { window.location.hash = ""; }} />;
  }

  if (analyticsPage) {
    const acos = analyticsTotals.adSales > 0 ? analyticsTotals.spend / analyticsTotals.adSales : 0;
    const tacos = analyticsTotals.sales > 0 ? analyticsTotals.spend / analyticsTotals.sales : 0;
    const conversionRate = analyticsTotals.sessions > 0 ? analyticsTotals.units / analyticsTotals.sessions : 0;
    const clickThroughRate = analyticsTotals.impressions > 0 ? analyticsTotals.clicks / analyticsTotals.impressions : 0;
    const maxBrandValue = analyticsBrandShare[0]?.value || 1;
    const maxCategoryValue = analyticsCategoryShare[0]?.value || 1;

    return (
      <div style={{ fontFamily:"'Inter','Segoe UI',sans-serif", background:THEME.pageBg, minHeight:"100vh", color:THEME.text }}>
        <div style={{ background:THEME.surface, borderBottom:`1px solid ${THEME.border}`, padding:"14px 24px", display:"flex", justifyContent:"space-between", alignItems:"center", gap:12, flexWrap:"wrap" }}>
          <div style={{ display:"flex", alignItems:"center", gap:12 }}>
            <button onClick={closeDetailPage} style={{ background:THEME.surfaceMuted, color:THEME.textSoft, border:`1px solid ${THEME.border}`, borderRadius:8, padding:"7px 12px", fontSize:11, fontWeight:700, cursor:"pointer" }}>
              ← Back
            </button>
            <div>
              <div style={{ fontSize:18, fontWeight:800, color:THEME.text, letterSpacing:-0.3 }}>Analytics</div>
              <div style={{ fontSize:10, color:THEME.textFaint, fontFamily:"'DM Mono',monospace", letterSpacing:0.8 }}>
                Cross-brand view for sales, ads, traffic, and movers
              </div>
            </div>
          </div>
          <div style={{ display:"flex", gap:8, alignItems:"center", flexWrap:"wrap" }}>
            <button onClick={exportFilteredData} style={{ background:THEME.panelBg, color:THEME.textSoft, border:`1px solid ${THEME.border}`, borderRadius:8, padding:"7px 12px", fontSize:11, fontWeight:600, cursor:"pointer" }}>
              Export CSV ↓
            </button>
          </div>
        </div>

        <div style={{ background: THEME.shellBg, borderBottom: `1px solid ${THEME.border}`, padding: "12px 24px", display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <MultiSelectFilter label="Brand" selected={brand} setSelected={(v)=>{setBrand(v);setCompareAsin("");setTablePage(1);}} opts={brands.map(b=>({v:b,l:b}))} isActive={brand.length>0} />
          <MultiSelectFilter label="Month" selected={month} setSelected={(v)=>{setMonth(v);setTablePage(1);}} opts={MONTH_SEQUENCE.map((monthLabel) => ({ v: monthLabel, l: monthLabel.replace(/ \d{4}$/, ""), g: monthYearOf(monthLabel) }))} isActive={month.length>0} />
          <MultiSelectFilter label="Category L0" allLabel="Categorys" selected={catL0} setSelected={(v)=>{setCatL0(v);setCompareAsin("");setTablePage(1);}} opts={catsL0.map(c=>({v:c,l:c}))} isActive={catL0.length>0} />
          <MultiSelectFilter label="Category L1" allLabel="Categorys" selected={catL1} setSelected={(v)=>{setCatL1(v);setCompareAsin("");setTablePage(1);}} opts={catsL1.map(c=>({v:c,l:c}))} isActive={catL1.length>0} />
          <div style={{ display:"flex", gap:0, borderRadius:8, border:`1px solid ${THEME.border}`, overflow:"hidden", flexShrink:0 }}>
            {[{v:"biz",l:"Amazon Sales"},{v:"p1",l:"1P Sales"}].map(({v,l}) => (
              <button key={v} onClick={() => setDataMode(prev => prev === v ? "all" : v)} style={{
                padding:"7px 13px", border:"none", fontSize:11, fontWeight:700,
                cursor:"pointer", transition:"all 0.15s", whiteSpace:"nowrap",
                background: dataMode===v ? "#2563EB" : THEME.surface,
                color: dataMode===v ? "#fff" : THEME.textSoft,
                borderRight: v==="biz" ? `1px solid ${THEME.border}` : "none",
              }}>{l}</button>
            ))}
          </div>
        </div>

        <div style={{ padding:"12px 24px", display:"flex", gap:10, overflowX:"auto", flexWrap:"nowrap", WebkitOverflowScrolling:"touch", background:THEME.surface, borderBottom:`1px solid ${THEME.surfaceSoft}` }}>
          <KpiCard label={dataMode === "p1" ? "1P Sales" : dataMode === "biz" ? "Amazon Sales" : "Net Sales"} value={inr(analyticsTotals.sales)} accent="#A78BFA" />
          <KpiCard label="Net Units" value={analyticsTotals.units.toLocaleString("en-IN")} accent="#34D399" />
          <KpiCard label="Sessions" value={dataMode === "p1" ? "N/A" : analyticsTotals.sessions.toLocaleString("en-IN")} accent="#38BDF8" />
          <KpiCard label="Ad Spend" value={inr(analyticsTotals.spend)} accent="#FBBF24" />
          <KpiCard label="Ad Sales" value={inr(analyticsTotals.adSales)} accent="#F9A8D4" />
          <KpiCard label="ACoS" value={`${(acos * 100).toFixed(1)}%`} accent={acos < 0.2 ? "#34D399" : acos < 0.35 ? "#FBBF24" : "#F87171"} />
          <KpiCard label="TACoS" value={`${(tacos * 100).toFixed(1)}%`} accent={tacos < 0.1 ? "#34D399" : tacos < 0.2 ? "#FBBF24" : "#F87171"} />
        </div>

        <div style={{ padding:"18px 24px 32px", display:"grid", gap:16 }}>
          <div style={{ display:"grid", gridTemplateColumns:"repeat(4, minmax(0, 1fr))", gap:12 }}>
            {[
              { label:"Impressions", value: analyticsTotals.impressions.toLocaleString("en-IN"), sub:"From SP + SD" },
              { label:"Clicks", value: analyticsTotals.clicks.toLocaleString("en-IN"), sub:`CTR ${(clickThroughRate * 100).toFixed(2)}%` },
              { label:"AMS Orders", value: analyticsTotals.orders.toLocaleString("en-IN"), sub:"Attributed orders" },
              { label:"CVR", value: `${(conversionRate * 100).toFixed(2)}%`, sub:"Units / Sessions" },
            ].map((item) => (
              <div key={item.label} style={{ background:THEME.surface, border:`1px solid ${THEME.border}`, borderRadius:14, padding:"16px" }}>
                <div style={{ fontSize:10, color:THEME.textMuted, fontFamily:"'DM Mono',monospace", letterSpacing:0.8 }}>{item.label}</div>
                <div style={{ marginTop:8, fontSize:28, fontWeight:800, color:THEME.text }}>{item.value}</div>
                <div style={{ marginTop:6, fontSize:12, color:THEME.textSoft }}>{item.sub}</div>
              </div>
            ))}
          </div>

          <div style={{ display:"grid", gridTemplateColumns:"1.1fr 1.1fr 0.8fr", gap:16, alignItems:"start" }}>
            <div style={{ background:THEME.surface, border:`1px solid ${THEME.border}`, borderRadius:16, padding:"16px" }}>
              <div style={{ fontSize:14, fontWeight:800, color:THEME.text, marginBottom:4 }}>Brand Share</div>
              <div style={{ fontSize:12, color:THEME.textMuted, marginBottom:14 }}>Revenue split across the current selection</div>
              <div style={{ display:"grid", gap:12 }}>
                {analyticsBrandShare.map((item) => (
                  <div key={item.label}>
                    <div style={{ display:"flex", justifyContent:"space-between", gap:12, marginBottom:6, fontSize:12 }}>
                      <span style={{ color:THEME.text, fontWeight:600 }}>{item.label}</span>
                      <span style={{ color:THEME.textSoft }}>{(item.share * 100).toFixed(1)}% · {inr(item.value)}</span>
                    </div>
                    <div style={{ height:10, borderRadius:999, background:THEME.surfaceMuted, overflow:"hidden" }}>
                      <div style={{ width:`${Math.max((item.value / maxBrandValue) * 100, 4)}%`, height:"100%", background:"linear-gradient(90deg, #2563EB 0%, #60A5FA 100%)" }} />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ background:THEME.surface, border:`1px solid ${THEME.border}`, borderRadius:16, padding:"16px" }}>
              <div style={{ fontSize:14, fontWeight:800, color:THEME.text, marginBottom:4 }}>Category Share</div>
              <div style={{ fontSize:12, color:THEME.textMuted, marginBottom:14 }}>Top categories by current sales mix</div>
              <div style={{ display:"grid", gap:12 }}>
                {analyticsCategoryShare.map((item) => (
                  <div key={item.label}>
                    <div style={{ display:"flex", justifyContent:"space-between", gap:12, marginBottom:6, fontSize:12 }}>
                      <span style={{ color:THEME.text, fontWeight:600 }}>{item.label}</span>
                      <span style={{ color:THEME.textSoft }}>{(item.share * 100).toFixed(1)}% · {inr(item.value)}</span>
                    </div>
                    <div style={{ height:10, borderRadius:999, background:THEME.surfaceMuted, overflow:"hidden" }}>
                      <div style={{ width:`${Math.max((item.value / maxCategoryValue) * 100, 4)}%`, height:"100%", background:"linear-gradient(90deg, #14B8A6 0%, #5EEAD4 100%)" }} />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ background:THEME.surface, border:`1px solid ${THEME.border}`, borderRadius:16, padding:"16px" }}>
              <div style={{ fontSize:14, fontWeight:800, color:THEME.text, marginBottom:4 }}>Funnel</div>
              <div style={{ fontSize:12, color:THEME.textMuted, marginBottom:14 }}>Traffic to revenue snapshot</div>
              <div style={{ display:"grid", gap:10 }}>
                {[
                  ["Impressions", analyticsTotals.impressions],
                  ["Clicks", analyticsTotals.clicks],
                  ["Sessions", analyticsTotals.sessions],
                  ["Units", analyticsTotals.units],
                  ["Ad Sales", analyticsTotals.adSales],
                  ["Ad Spend", analyticsTotals.spend],
                ].map(([label, value], index, list) => {
                  const maxValue = Number(list[0][1]) || 1;
                  const numericValue = Number(value) || 0;
                  return (
                    <div key={label}>
                      <div style={{ display:"flex", justifyContent:"space-between", gap:12, marginBottom:6, fontSize:12 }}>
                        <span style={{ color:THEME.text, fontWeight:600 }}>{label}</span>
                        <span style={{ color:THEME.textSoft }}>{label.includes("Sales") || label.includes("Spend") ? inr(numericValue) : numericValue.toLocaleString("en-IN")}</span>
                      </div>
                      <div style={{ height:10, borderRadius:999, background:THEME.surfaceMuted, overflow:"hidden" }}>
                        <div style={{ width:`${Math.max((numericValue / maxValue) * 100, 4)}%`, height:"100%", background:"linear-gradient(90deg, #4F46E5 0%, #818CF8 100%)" }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>

          <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:16, alignItems:"start" }}>
            <div style={{ background:THEME.surface, border:`1px solid ${THEME.border}`, borderRadius:16, padding:"16px" }}>
              <div style={{ fontSize:14, fontWeight:800, color:THEME.text, marginBottom:4 }}>Monthly Trend</div>
              <div style={{ fontSize:12, color:THEME.textMuted, marginBottom:14 }}>Current filtered totals by month</div>
              <div style={{ display:"grid", gap:12 }}>
                {analyticsMonthSeries.map((item) => {
                  const maxSales = analyticsMonthSeries.reduce((max, row) => Math.max(max, row.sales), 0) || 1;
                  return (
                    <div key={item.month}>
                      <div style={{ display:"flex", justifyContent:"space-between", gap:12, marginBottom:6, fontSize:12 }}>
                        <span style={{ color:THEME.text, fontWeight:700 }}>{item.month} 2026</span>
                        <span style={{ color:THEME.textSoft }}>{inr(item.sales)} · {item.units.toLocaleString("en-IN")} units</span>
                      </div>
                      <div style={{ height:12, borderRadius:999, background:THEME.surfaceMuted, overflow:"hidden" }}>
                        <div style={{ width:`${Math.max((item.sales / maxSales) * 100, 4)}%`, height:"100%", background:"linear-gradient(90deg, #7C3AED 0%, #C084FC 100%)" }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            <div style={{ background:THEME.surface, border:`1px solid ${THEME.border}`, borderRadius:16, padding:"16px" }}>
              <div style={{ fontSize:14, fontWeight:800, color:THEME.text, marginBottom:4 }}>Top Movers</div>
              <div style={{ fontSize:12, color:THEME.textMuted, marginBottom:14 }}>
                {analyticsComparison.previousMonth && analyticsComparison.currentMonth
                  ? `${analyticsComparison.previousMonth} vs ${analyticsComparison.currentMonth}`
                  : "Select at least two months to compare movers"}
              </div>
              <div style={{ display:"grid", gap:10 }}>
                {analyticsComparison.movers.length === 0 ? (
                  <div style={{ fontSize:12, color:THEME.textMuted }}>Not enough month data in the current filter to rank movers yet.</div>
                ) : analyticsComparison.movers.map((item) => (
                  <div key={item.asin} style={{ border:`1px solid ${THEME.border}`, borderRadius:12, padding:"12px 14px", background:THEME.panelBg }}>
                    <div style={{ display:"flex", justifyContent:"space-between", gap:12, alignItems:"start" }}>
                      <div>
                        <div style={{ fontSize:13, fontWeight:700, color:THEME.text }}>{item.asin}</div>
                        <div style={{ fontSize:12, color:THEME.textSoft }}>{item.brand} · {String(item.title || "").slice(0, 64)}</div>
                      </div>
                      <div style={{ textAlign:"right" }}>
                        <div style={{ fontSize:13, fontWeight:800, color:item.delta >= 0 ? "#059669" : "#DC2626" }}>
                          {item.delta >= 0 ? "+" : ""}{inr(item.delta)}
                        </div>
                        <div style={{ fontSize:12, color:THEME.textMuted }}>{item.pctChange >= 0 ? "+" : ""}{item.pctChange.toFixed(1)}%</div>
                      </div>
                    </div>
                    <div style={{ marginTop:8, fontSize:12, color:THEME.textSoft }}>
                      {analyticsComparison.previousMonth}: {inr(item.prev)} → {analyticsComparison.currentMonth}: {inr(item.curr)}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (smartViewPage) {
    return (
      <div style={{ fontFamily:"'Inter','Segoe UI',sans-serif", background:THEME.pageBg, minHeight:"100vh", color:THEME.text }}>
        <div style={{ background:THEME.surface, borderBottom:`1px solid ${THEME.border}`, padding:"14px 24px", display:"flex", justifyContent:"space-between", alignItems:"center", gap:12, flexWrap:"wrap" }}>
          <div style={{ display:"flex", alignItems:"center", gap:12 }}>
            <button onClick={closeDetailPage} style={{ background:THEME.surfaceMuted, color:THEME.textSoft, border:`1px solid ${THEME.border}`, borderRadius:8, padding:"7px 12px", fontSize:11, fontWeight:700, cursor:"pointer" }}>
              ← Back
            </button>
            <div>
              <div style={{ fontSize:18, fontWeight:800, color:THEME.text, letterSpacing:-0.3 }}>Smart Views</div>
              <div style={{ fontSize:10, color:THEME.textFaint, fontFamily:"'DM Mono',monospace", letterSpacing:0.8 }}>BUYBOX ACTION WORKBENCH</div>
            </div>
          </div>
          <div style={{ display:"flex", gap:8, alignItems:"center", flexWrap:"wrap" }}>
            <span style={{ fontSize:11, color:THEME.textMuted }}>{smartViewRows.length} matching rows</span>
            <button onClick={exportFilteredData} style={{ background:THEME.panelBg, border:`1px solid ${THEME.border}`, borderRadius:8, padding:"7px 12px", fontSize:11, color:THEME.textSoft, fontWeight:600, cursor:"pointer" }}>
              Export CSV
            </button>
          </div>
        </div>

        <div style={{ background:THEME.surface, borderBottom:`1px solid ${THEME.border}`, padding:"0 24px", display:"flex", gap:8, overflowX:"auto" }}>
          {SMART_VIEW_TABS.map((view) => (
            <button
              key={view.key}
              onClick={() => switchSmartViewTab(view.key)}
              title={view.description}
              style={{
                padding:"12px 16px",
                border:"none",
                borderBottom: smartViewTab === view.key ? "2px solid #2563EB" : "2px solid transparent",
                background:"transparent",
                color: smartViewTab === view.key ? "#1D4ED8" : THEME.textSoft,
                fontSize:12,
                fontWeight:700,
                cursor:"pointer",
                whiteSpace:"nowrap",
              }}
            >
              {view.label}
            </button>
          ))}
        </div>

        <div style={{ background:THEME.shellBg, borderBottom:`1px solid ${THEME.border}`, padding:"12px 24px", display:"flex", gap:10, alignItems:"center", flexWrap:"wrap" }}>
          <div style={{ position:"relative", flex:"0 0 auto" }}>
            <span style={{ position:"absolute", left:12, top:"50%", transform:"translateY(-50%)", color:THEME.textFaint, fontSize:13 }}>⌕</span>
            <input
              placeholder={`Search inside ${activeSmartView.label.toLowerCase()}…`}
              value={search}
              onChange={onSearch}
              style={{ background: search ? "#EFF6FF" : THEME.surface, border: search ? "1px solid #3B82F6" : `1px solid ${THEME.border}`, borderRadius:8, color:THEME.text, padding:"9px 14px 9px 34px", fontSize:12, width:260, outline:"none" }}
            />
          </div>
          <MultiSelectFilter label="Brand" selected={brand} setSelected={(v)=>{setBrand(v);setTablePage(1);}} opts={brands.map(b=>({v:b,l:b}))} isActive={brand.length>0} />
          <MultiSelectFilter label="Month" selected={month} setSelected={(v)=>{setMonth(v);setTablePage(1);}} opts={MONTH_SEQUENCE.map((monthLabel) => ({ v: monthLabel, l: monthLabel.replace(/ \d{4}$/, ""), g: monthYearOf(monthLabel) }))} isActive={month.length>0} />
          <MultiSelectFilter label="Category L0" allLabel="Categorys" selected={catL0} setSelected={(v)=>{setCatL0(v);setTablePage(1);}} opts={catsL0.map(c=>({v:c,l:c}))} isActive={catL0.length>0} />
          <MultiSelectFilter label="Category L1" allLabel="Categorys" selected={catL1} setSelected={(v)=>{setCatL1(v);setTablePage(1);}} opts={catsL1.map(c=>({v:c,l:c}))} isActive={catL1.length>0} />
          {/* Data Mode Toggle */}
          <div style={{ display:"flex", gap:0, borderRadius:8, border:`1px solid ${THEME.border}`, overflow:"hidden", flexShrink:0 }}>
            {[{v:"biz",l:"Amazon Sales"},{v:"p1",l:"1P Sales"}].map(({v,l}) => (
              <button key={v} onClick={() => setDataMode(prev => prev === v ? "all" : v)} style={{
                padding:"7px 13px", border:"none", fontSize:11, fontWeight:700,
                cursor:"pointer", transition:"all 0.15s", whiteSpace:"nowrap",
                background: dataMode===v ? "#2563EB" : THEME.surface,
                color: dataMode===v ? "#fff" : THEME.textSoft,
                borderRight: v==="biz" ? `1px solid ${THEME.border}` : "none",
              }}>{l}</button>
            ))}
          </div>
        </div>

        <div style={{ padding:"18px 24px 12px", display:"grid", gridTemplateColumns:"repeat(4, minmax(0, 1fr))", gap:12 }}>
          <KpiCard label="Matching Rows" value={smartViewTotals.rows.toLocaleString("en-IN")} accent="#2563EB" />
          <KpiCard label="Net Sales" value={inr(smartViewTotals.sales)} accent="#A78BFA" />
          <KpiCard label="Ad Spend" value={inr(smartViewTotals.spend)} accent="#FBBF24" />
          <KpiCard label="Avg Buybox" value={`${(smartViewAvgBuybox * 100).toFixed(1)}%`} accent="#34D399" />
        </div>

        <div style={{ padding:"0 24px 10px" }}>
          <div style={{ background:THEME.surface, border:`1px solid ${THEME.border}`, borderRadius:14, padding:"14px 16px" }}>
            <div style={{ fontSize:10, color:THEME.textMuted, fontFamily:"'DM Mono',monospace", letterSpacing:1, textTransform:"uppercase", marginBottom:8 }}>View Summary</div>
            <div style={{ fontSize:17, fontWeight:800, color:THEME.text, marginBottom:6 }}>{activeSmartView.label}</div>
            <div style={{ fontSize:13, color:THEME.textSoft, lineHeight:1.6 }}>{activeSmartView.description}. Use this view to spot products that need immediate buybox or ads action.</div>
          </div>
        </div>

        <div style={{ padding:"0 24px 32px", overflowX:"auto" }}>
          <div style={{ borderRadius:12, border:`1px solid ${THEME.border}`, overflow:"hidden", minWidth:1800, background:THEME.surface, boxShadow:"0 4px 16px rgba(15,23,42,0.04)" }}>
            <div style={{ overflow:"auto", maxHeight:"calc(100vh - 310px)" }}>
              <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11.5 }}>
                <thead>
                  <tr style={{ background:THEME.headerBg }}>
                    {COLS.map((col) => (
                      <th key={col.key} onClick={()=>handleSort(col.key)} style={{
                        padding:"10px 10px",
                        textAlign: col.fmt==="str"||col.fmt==="title"?"left":"right",
                        color: sortKey===col.key ? "#1D4ED8" : THEME.headerText,
                        fontFamily:"'DM Mono',monospace",
                        fontSize:9.5,
                        fontWeight:500,
                        letterSpacing:0.7,
                        textTransform:"uppercase",
                        cursor:"pointer",
                        whiteSpace:"nowrap",
                        userSelect:"none",
                        minWidth:col.w,
                        background: sortKey===col.key ? THEME.headerBgActive : THEME.headerBg,
                        borderBottom:`1px solid ${THEME.borderStrong}`,
                        position:"sticky",
                        top:0,
                        left:STICKY_LEFT_OFFSETS[col.key],
                        zIndex: STICKY_LEFT_OFFSETS[col.key] !== undefined ? 8 : 5,
                        borderRight: STICKY_LEFT_OFFSETS[col.key] !== undefined ? `1px solid ${THEME.borderStrong}` : undefined,
                        boxShadow:`inset 0 -1px 0 ${THEME.borderStrong}, ${THEME.headerShadow}`,
                      }}>
                        {col.label}{sortKey===col.key&&<span style={{marginLeft:3}}>{sortDir===-1?"↓":"↑"}</span>}
                      </th>
                    ))}
                    {/* Brand and Month columns removed */}
                    <th style={{ minWidth:190, padding:"10px 12px", color:THEME.headerText, fontFamily:"'DM Mono',monospace", fontSize:9.5, textTransform:"uppercase", position:"sticky", top:0, zIndex:5, background:THEME.headerBg, borderBottom:`1px solid ${THEME.borderStrong}`, boxShadow:`inset 0 -1px 0 ${THEME.borderStrong}, ${THEME.headerShadow}` }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {smartViewSortedRows.map((row, index) => {
                    const rowBackground = index % 2 === 0 ? THEME.surface : THEME.panelBg;
                    return (
                      <tr key={`${row.Brand}-${row.ASIN}-${row.Month}`} style={{ borderBottom:"1px solid #F1F5F9", background:rowBackground }}>
                        {COLS.map((col) => {
                          const v = col.master ? getMasterValue(row, col.key) : row[col.key];
                          const disp = fmtCell(row, col);
                          const isPct = col.fmt === "pct";
                          const accentColor = isPct ? pctColor(col.key, v) : col.key === "Title" ? THEME.text : THEME.textSoft;
                          const isEmpty = v === null || v === undefined || v === 0 || v === "";
                          return (
                            <td key={col.key} style={{
                              padding:"5px 10px",
                              textAlign: col.fmt==="str"||col.fmt==="title"?"left":"right",
                              fontFamily: col.key==="ASIN"||col.key==="fbaSku"||col.key==="model" ? "'DM Mono',monospace" : "inherit",
                              fontSize: col.key==="ASIN" ? 12 : col.key==="fbaSku" ? 10.8 : 11.5,
                              maxWidth: col.fmt==="title" ? 140 : undefined,
                              overflow:"hidden",
                              textOverflow:"ellipsis",
                              whiteSpace:"nowrap",
                              background: STICKY_LEFT_OFFSETS[col.key] !== undefined ? rowBackground : col.master ? (isEmpty ? THEME.surfaceMuted : "#F0F4FF") : "transparent",
                              color: isEmpty ? "#CBD5E1" : col.key==="ASIN" ? "#1D4ED8" : accentColor,
                              letterSpacing: col.key==="ASIN" ? 0.3 : 0,
                              fontWeight: col.key==="ASIN" ? 800 : col.key==="model" ? 600 : col.key==="TotalNetSalesValue" ? 700 : col.key==="dp" ? 600 : 400,
                              position: STICKY_LEFT_OFFSETS[col.key] !== undefined ? "sticky" : "static",
                              left: STICKY_LEFT_OFFSETS[col.key],
                              zIndex: STICKY_LEFT_OFFSETS[col.key] !== undefined ? 4 : 1,
                              borderRight: STICKY_LEFT_OFFSETS[col.key] !== undefined ? `1px solid ${THEME.border}` : undefined,
                              boxShadow: STICKY_LEFT_OFFSETS[col.key] === STICKY_LEFT_OFFSETS.category ? THEME.stickyShadow : undefined,
                            }}>
                              {col.key === "ASIN" && !isEmpty ? (
                                <a href={getAmazonProductUrl(row.ASIN)} target="_blank" rel="noreferrer" style={{ color:"#1D4ED8", fontWeight:800, textDecoration:"underline", textUnderlineOffset:"2px" }}>
                                  {disp}
                                </a>
                              ) : col.key === "model" && !isEmpty ? (
                                <span
                                  onClick={() => openModelPage(v)}
                                  title={`Open model ${v} detail`}
                                  style={{ color:"#7C3AED", cursor:"pointer", textDecoration:"underline", textUnderlineOffset:"2px", fontWeight:700 }}
                                >
                                  {disp}
                                </span>
                              ) : disp}
                            </td>
                          );
                        })}
                        {/* Brand and Month cells removed */}
                        <td style={{ padding:"5px 12px" }}>
                          <div style={{ display:"flex", gap:6, alignItems:"center", flexWrap:"wrap" }}>
                            <a
                              href={getAmazonProductUrl(row.ASIN)}
                              target="_blank"
                              rel="noreferrer"
                              style={{ padding:"5px 8px", borderRadius:999, background:"#EFF6FF", border:"1px solid #BFDBFE", color:"#1D4ED8", fontSize:10, fontWeight:700, textDecoration:"none", whiteSpace:"nowrap" }}
                            >
                              Open Amazon
                            </a>
                            <button
                              onClick={() => openDetailPage(row.ASIN)}
                              style={{ padding:"5px 8px", borderRadius:999, background:THEME.surface, border:`1px solid ${THEME.border}`, color:THEME.textSoft, fontSize:10, fontWeight:700, cursor:"pointer", whiteSpace:"nowrap" }}
                            >
                              Detail View
                            </button>
                            <button
                              onClick={() => openComparePage(row.ASIN)}
                              style={{ padding:"5px 8px", borderRadius:999, background:"#2563EB", border:"none", color:"#FFFFFF", fontSize:10, fontWeight:700, cursor:"pointer", whiteSpace:"nowrap" }}
                            >
                              Compare
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                  {smartViewSortedRows.length === 0 && (
                    <tr>
                      <td colSpan={COLS.length + 1} style={{ padding:"28px 18px", textAlign:"center", color:THEME.textMuted, background:THEME.surface }}>
                        No rows match this smart view with the current filters.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (compareTabMeta) {
    const activectAsin = ctAsin || compareTabMeta.ASIN;

    // All ASINs available
    const ctAllAsins = [...new Set(enriched.map(r => r.ASIN))].map(asin => {
      const row = enriched.find(r => r.ASIN === asin);
      return { asin, title: row?.Title || asin, brand: row?.Brand || "" };
    });

    const ctRows = enriched.filter(r => r.ASIN === activectAsin);
    const ctMonths = sortMonths([...new Set(ctRows.map((row) => row.Month))].filter(Boolean));
    const ctR1 = ctRows.find(r => r.Month === ctM1);
    const ctR2 = ctRows.find(r => r.Month === ctM2);
    const ctMeta = ctRows[0] || compareTabMeta;

    const pct = (a, b) => a > 0 ? (((b - a) / a) * 100).toFixed(1) : b > 0 ? "100.0" : "0.0";
    const growthColor = (a, b) => Number(pct(a,b)) >= 0 ? "#059669" : "#DC2626";
    const compareInsights = (() => {
      const metrics = [
        { key: "sales", label: "net sales", prev: ctR1?.TotalNetSalesValue ?? 0, curr: ctR2?.TotalNetSalesValue ?? 0, better: "up" },
        { key: "units", label: "net units", prev: ctR1?.NetUnits ?? 0, curr: ctR2?.NetUnits ?? 0, better: "up" },
        { key: "sessions", label: "sessions", prev: ctR1?.Sessions ?? 0, curr: ctR2?.Sessions ?? 0, better: "up" },
        { key: "buybox", label: "buybox", prev: ctR1?.BuyboxPct ?? 0, curr: ctR2?.BuyboxPct ?? 0, better: "up", format: "pct" },
        { key: "adsSales", label: "ads sales", prev: ctR1?.TotalAdsSales ?? 0, curr: ctR2?.TotalAdsSales ?? 0, better: "up" },
        { key: "adsSpend", label: "ads spend", prev: ctR1?.TotalAdsSpend ?? 0, curr: ctR2?.TotalAdsSpend ?? 0, better: "down" },
        { key: "organicSales", label: "organic sales", prev: ctR1?.OrganiSales ?? 0, curr: ctR2?.OrganiSales ?? 0, better: "up" },
        { key: "acos", label: "ACOS", prev: ctR1?.ACOS ?? 0, curr: ctR2?.ACOS ?? 0, better: "down", format: "pct" },
        { key: "tacos", label: "TACOS", prev: ctR1?.TACOS ?? 0, curr: ctR2?.TACOS ?? 0, better: "down", format: "pct" },
      ].map((metric) => {
        const delta = metric.curr - metric.prev;
        const pctChange = metric.prev > 0 ? (delta / metric.prev) * 100 : metric.curr > 0 ? 100 : 0;
        const improved = metric.better === "down" ? delta <= 0 : delta >= 0;
        return { ...metric, delta, pctChange, improved, magnitude: Math.abs(pctChange) };
      });

      const strongestPositive = [...metrics]
        .filter((metric) => metric.improved && metric.magnitude > 0)
        .sort((a, b) => b.magnitude - a.magnitude)[0];
      const strongestNegative = [...metrics]
        .filter((metric) => !metric.improved && metric.magnitude > 0)
        .sort((a, b) => b.magnitude - a.magnitude)[0];

      const formatMetricValue = (metric, value) => {
        if (metric.format === "pct") return `${(value * 100).toFixed(1)}%`;
        if (metric.key === "sales" || metric.key === "adsSales" || metric.key === "adsSpend" || metric.key === "organicSales") return inr(value);
        return value.toLocaleString("en-IN");
      };

      const highlights = [];
      if (strongestPositive) {
        highlights.push(`${strongestPositive.label} improved from ${formatMetricValue(strongestPositive, strongestPositive.prev)} to ${formatMetricValue(strongestPositive, strongestPositive.curr)} (${strongestPositive.pctChange >= 0 ? "+" : ""}${strongestPositive.pctChange.toFixed(1)}%).`);
      }
      if (strongestNegative) {
        highlights.push(`${strongestNegative.label} weakened from ${formatMetricValue(strongestNegative, strongestNegative.prev)} to ${formatMetricValue(strongestNegative, strongestNegative.curr)} (${strongestNegative.pctChange >= 0 ? "+" : ""}${strongestNegative.pctChange.toFixed(1)}%).`);
      }

      const overallSales = metrics.find((metric) => metric.key === "sales");
      const overallOrganic = metrics.find((metric) => metric.key === "organicSales");
      const overallBuybox = metrics.find((metric) => metric.key === "buybox");

      const summaryParts = [];
      if (overallSales) {
        summaryParts.push(`Net sales ${overallSales.delta >= 0 ? "rose" : "fell"} ${Math.abs(overallSales.pctChange).toFixed(1)}% from ${ctM1} to ${ctM2}.`);
      }
      if (overallOrganic && overallOrganic.magnitude > 0) {
        summaryParts.push(`Organic sales ${overallOrganic.delta >= 0 ? "moved up" : "dropped"} ${Math.abs(overallOrganic.pctChange).toFixed(1)}%.`);
      }
      if (overallBuybox && overallBuybox.magnitude > 0) {
        summaryParts.push(`Buybox ${overallBuybox.delta >= 0 ? "improved" : "slipped"} to ${(overallBuybox.curr * 100).toFixed(1)}%.`);
      }

      const improvedText = strongestPositive
        ? `${strongestPositive.label} improved from ${formatMetricValue(strongestPositive, strongestPositive.prev)} to ${formatMetricValue(strongestPositive, strongestPositive.curr)} (${strongestPositive.pctChange >= 0 ? "+" : ""}${strongestPositive.pctChange.toFixed(1)}%).`
        : "No major positive movement stood out in this comparison.";

      const worsenedText = strongestNegative
        ? `${strongestNegative.label} weakened from ${formatMetricValue(strongestNegative, strongestNegative.prev)} to ${formatMetricValue(strongestNegative, strongestNegative.curr)} (${strongestNegative.pctChange >= 0 ? "+" : ""}${strongestNegative.pctChange.toFixed(1)}%).`
        : "No major deterioration stood out in this comparison.";

      let actionText = "Keep monitoring this ASIN before making a pricing or ads change.";
      if ((metrics.find((metric) => metric.key === "organicSales")?.delta ?? 0) < 0 && (metrics.find((metric) => metric.key === "adsSales")?.delta ?? 0) > 0) {
        actionText = "Organic contribution is weakening while paid sales are rising, so review listing quality and buybox protection before increasing ad budgets further.";
      } else if ((metrics.find((metric) => metric.key === "buybox")?.delta ?? 0) < 0) {
        actionText = "Buybox slipped in the selected comparison, so check pricing, stock health, and seller competition first.";
      } else if ((metrics.find((metric) => metric.key === "adsSpend")?.delta ?? 0) > 0 && (metrics.find((metric) => metric.key === "sales")?.delta ?? 0) < 0) {
        actionText = "Ads spend is rising while net sales are under pressure, so tighten campaign targeting and review wasted spend.";
      } else if ((metrics.find((metric) => metric.key === "sales")?.delta ?? 0) > 0) {
        actionText = "Sales improved in the selected months, so preserve the current setup and look for similar ASINs to scale.";
      }

      return {
        title: `${ctM1} to ${ctM2} summary`,
        summary: summaryParts.join(" "),
        highlights,
        improvedText,
        worsenedText,
        actionText,
      };
    })();

    // Export filtered comparison CSV
    const exportCtCsv = () => {
      const rows = ctRows.filter(r => r.Month === ctM1 || r.Month === ctM2).map(r => ({
        Month: r.Month, ASIN: r.ASIN, Brand: r.Brand, Title: r.Title,
        Model: getMasterValue(r,"model"), Sessions: r.Sessions, NetUnits: r.NetUnits,
        NetSales: r.TotalNetSalesValue, AdsSpend: r.TotalAdsSpend, AdsSales: r.TotalAdsSales,
        Clicks: r.Clicks, AmsOrders: r.AmsOrders, BuyboxPct: r.BuyboxPct,
        ACOS: r.ACOS, TACOS: r.TACOS, Impressions: r.Impressions, CAC: r.CAC,
      }));
      exportRowsToCsv(`comparison-${activectAsin}-${ctM1}-vs-${ctM2}.csv`, rows,
        ["Month","ASIN","Brand","Title","Model","Sessions","NetUnits","NetSales","AdsSpend","AdsSales","Clicks","AmsOrders","BuyboxPct","ACOS","TACOS","Impressions","CAC"]);
    };

    const TABS = ["Overview","Sales","Ads","Traffic","Efficiency","Monthly Split","Top 10","ASIN Compare"];
    const TAB_ICONS = {"Overview":"📊","Sales":"💰","Ads":"📢","Traffic":"🚦","Efficiency":"⚡","Monthly Split":"📅","Top 10":"🏆","ASIN Compare":"🔍"};

    const MetricCard = ({label, v1, v2, fmt="num", pctKeys=[]}) => {
      const val1 = v1 ?? 0, val2 = v2 ?? 0;
      const g = pct(val1, val2);
      const isPos = Number(g) >= 0;
      const fmt1 = fmt==="cur" ? inr(val1) : fmt==="pct" ? `${(val1*100).toFixed(1)}%` : val1.toLocaleString("en-IN");
      const fmt2 = fmt==="cur" ? inr(val2) : fmt==="pct" ? `${(val2*100).toFixed(1)}%` : val2.toLocaleString("en-IN");
      return (
        <div style={{padding:"14px",borderRadius:14,
          background: isPos ? "rgba(16,185,129,0.04)" : "rgba(239,68,68,0.04)",
          border:`1px solid ${isPos?"rgba(16,185,129,0.2)":"rgba(239,68,68,0.2)"}`,
          borderLeft:`4px solid ${isPos?"#10B981":"#EF4444"}`}}>
          <div style={{fontSize:10,color:THEME.textMuted,marginBottom:4,fontFamily:"'DM Mono',monospace",textTransform:"uppercase",letterSpacing:0.8}}>{label}</div>
          <div style={{fontSize:20,fontWeight:800,color:isPos?"#059669":"#DC2626"}}>{isPos?"+":""}{g}%</div>
          <div style={{fontSize:11,color:THEME.textMuted,marginTop:4}}>
            <span style={{color:THEME.text,fontWeight:600}}>{ctM1}:</span> {fmt1} &nbsp;→&nbsp; <span style={{color:THEME.text,fontWeight:600}}>{ctM2}:</span> {fmt2}
          </div>
        </div>
      );
    };

    const renderTab = () => {
      if (ctTab === "Overview") return (
        <div style={{display:"grid",gap:16}}>
          <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:12}}>
            <MetricCard label="Net Sales" v1={ctR1?.TotalNetSalesValue} v2={ctR2?.TotalNetSalesValue} fmt="cur"/>
            <MetricCard label="Net Units" v1={ctR1?.NetUnits} v2={ctR2?.NetUnits}/>
            <MetricCard label="Sessions" v1={ctR1?.Sessions} v2={ctR2?.Sessions}/>
            <MetricCard label="Buybox %" v1={ctR1?.BuyboxPct} v2={ctR2?.BuyboxPct} fmt="pct"/>
            <MetricCard label="AMS Orders" v1={ctR1?.AmsOrders} v2={ctR2?.AmsOrders}/>
            <MetricCard label="Ad Sales" v1={ctR1?.TotalAdsSales} v2={ctR2?.TotalAdsSales} fmt="cur"/>
            <MetricCard label="Ad Spend" v1={ctR1?.TotalAdsSpend} v2={ctR2?.TotalAdsSpend} fmt="cur"/>
            <MetricCard label="ACOS" v1={ctR1?.ACOS} v2={ctR2?.ACOS} fmt="pct"/>
            <MetricCard label="TACOS" v1={ctR1?.TACOS} v2={ctR2?.TACOS} fmt="pct"/>
            <MetricCard label="Impressions" v1={ctR1?.Impressions} v2={ctR2?.Impressions}/>
            <MetricCard label="Clicks" v1={ctR1?.Clicks} v2={ctR2?.Clicks}/>
            <MetricCard label="CAC" v1={ctR1?.CAC} v2={ctR2?.CAC} fmt="cur"/>
            <MetricCard label="CVR %" v1={ctR1?.ConversionPct} v2={ctR2?.ConversionPct} fmt="pct"/>
            <MetricCard label="Organic %" v1={ctR1?.OrganicPct} v2={ctR2?.OrganicPct} fmt="pct"/>
            <MetricCard label="Organic Sales" v1={ctR1?.OrganiSales} v2={ctR2?.OrganiSales}/>
          </div>
          {/* Monthly split summary below */}
          <div style={{background:THEME.surface,border:`1px solid ${THEME.border}`,borderRadius:14,overflow:"hidden"}}>
            <div style={{display:"grid",gridTemplateColumns:`2fr repeat(${ctMonths.length},1fr)`,background:THEME.surfaceMuted,padding:"10px 16px",borderBottom:`1px solid ${THEME.border}`}}>
              <div style={{fontSize:10,color:THEME.textMuted,fontFamily:"'DM Mono',monospace",letterSpacing:0.8,fontWeight:700}}>METRIC</div>
              {ctMonths.map(m=><div key={m} style={{fontSize:10,color: m===ctM1||m===ctM2?"#2563EB":THEME.textMuted,fontFamily:"'DM Mono',monospace",letterSpacing:0.8,textAlign:"right",fontWeight: m===ctM1||m===ctM2?700:400}}>{m} 2026{m===ctM1?" ←":m===ctM2?" →":""}</div>)}
            </div>
            {[
              {label:"Net Sales",key:"TotalNetSalesValue",fmt:"cur"},
              {label:"Net Units",key:"NetUnits",fmt:"num"},
              {label:"Sessions",key:"Sessions",fmt:"num"},
              {label:"Ad Spend",key:"TotalAdsSpend",fmt:"cur"},
              {label:"ACOS",key:"ACOS",fmt:"pct"},
            ].map((metric,i)=>{
              const allMonths = sortMonths([...new Set(enriched.filter(r=>r.ASIN===activectAsin).map(r=>r.Month))]);
              const rows2 = enriched.filter(r=>r.ASIN===activectAsin);
              const vals = allMonths.map(m=>rows2.find(r=>r.Month===m)?.[metric.key]??null);
              const fmtV = (v) => v===null?"—":metric.fmt==="cur"?inr(v):metric.fmt==="pct"?`${(v*100).toFixed(1)}%`:v.toLocaleString("en-IN");
              return (
                <div key={metric.key} style={{display:"grid",gridTemplateColumns:`2fr repeat(${allMonths.length},1fr)`,padding:"9px 16px",background:i%2===0?THEME.surface:THEME.panelBg,borderBottom:`1px solid ${THEME.surfaceSoft}`,alignItems:"center"}}>
                  <div style={{fontSize:12,color:THEME.textSoft,fontWeight:600}}>{metric.label}</div>
                  {vals.map((v,vi)=>(
                    <div key={vi} style={{fontSize:12,fontWeight: allMonths[vi]===ctM1||allMonths[vi]===ctM2?700:500, color:v===null?"#CBD5E1":allMonths[vi]===ctM1||allMonths[vi]===ctM2?THEME.text:THEME.textFaint,textAlign:"right"}}>{fmtV(v)}</div>
                  ))}
                </div>
              );
            })}
          </div>
        </div>
      );
      if (ctTab === "Sales") return (
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12}}>
          <MetricCard label="Net Sales" v1={ctR1?.TotalNetSalesValue} v2={ctR2?.TotalNetSalesValue} fmt="cur"/>
          <MetricCard label="Net Units" v1={ctR1?.NetUnits} v2={ctR2?.NetUnits}/>
          <MetricCard label="AMS Orders" v1={ctR1?.AmsOrders} v2={ctR2?.AmsOrders}/>
          <MetricCard label="Ad Sales" v1={ctR1?.TotalAdsSales} v2={ctR2?.TotalAdsSales} fmt="cur"/>
          <MetricCard label="Organic Sales" v1={ctR1?.OrganiSales} v2={ctR2?.OrganiSales}/>
          <MetricCard label="Organic %" v1={ctR1?.OrganicPct} v2={ctR2?.OrganicPct} fmt="pct"/>
        </div>
      );
      if (ctTab === "Ads") return (
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12}}>
          <MetricCard label="Ad Spend" v1={ctR1?.TotalAdsSpend} v2={ctR2?.TotalAdsSpend} fmt="cur"/>
          <MetricCard label="Ad Sales" v1={ctR1?.TotalAdsSales} v2={ctR2?.TotalAdsSales} fmt="cur"/>
          <MetricCard label="ACOS" v1={ctR1?.ACOS} v2={ctR2?.ACOS} fmt="pct"/>
          <MetricCard label="TACOS" v1={ctR1?.TACOS} v2={ctR2?.TACOS} fmt="pct"/>
          <MetricCard label="Impressions" v1={ctR1?.Impressions} v2={ctR2?.Impressions}/>
          <MetricCard label="Clicks" v1={ctR1?.Clicks} v2={ctR2?.Clicks}/>
          <MetricCard label="AMS Orders" v1={ctR1?.AmsOrders} v2={ctR2?.AmsOrders}/>
          <MetricCard label="CAC" v1={ctR1?.CAC} v2={ctR2?.CAC} fmt="cur"/>
        </div>
      );
      if (ctTab === "Traffic") return (
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12}}>
          <MetricCard label="Sessions" v1={ctR1?.Sessions} v2={ctR2?.Sessions}/>
          <MetricCard label="Clicks" v1={ctR1?.Clicks} v2={ctR2?.Clicks}/>
          <MetricCard label="Impressions" v1={ctR1?.Impressions} v2={ctR2?.Impressions}/>
          <MetricCard label="CVR %" v1={ctR1?.ConversionPct} v2={ctR2?.ConversionPct} fmt="pct"/>
          <MetricCard label="Buybox %" v1={ctR1?.BuyboxPct} v2={ctR2?.BuyboxPct} fmt="pct"/>
        </div>
      );
      if (ctTab === "Efficiency") return (
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12}}>
          <MetricCard label="ACOS" v1={ctR1?.ACOS} v2={ctR2?.ACOS} fmt="pct"/>
          <MetricCard label="TACOS" v1={ctR1?.TACOS} v2={ctR2?.TACOS} fmt="pct"/>
          <MetricCard label="CAC" v1={ctR1?.CAC} v2={ctR2?.CAC} fmt="cur"/>
          <MetricCard label="CVR %" v1={ctR1?.ConversionPct} v2={ctR2?.ConversionPct} fmt="pct"/>
          <MetricCard label="Organic %" v1={ctR1?.OrganicPct} v2={ctR2?.OrganicPct} fmt="pct"/>
          <MetricCard label="Buybox %" v1={ctR1?.BuyboxPct} v2={ctR2?.BuyboxPct} fmt="pct"/>
        </div>
      );
      if (ctTab === "Monthly Split") {
        const allMonthRows = [...ctRows].sort((a,b) => (MONTH_INDEX[a.Month] ?? 999) - (MONTH_INDEX[b.Month] ?? 999));
        return (
          <div style={{display:"grid",gap:12}}>
            {allMonthRows.map(row => (
              <div key={row.Month} style={{background:THEME.surface,border:`1px solid ${THEME.border}`,borderRadius:16,padding:"16px"}}>
                <div style={{fontWeight:800,fontSize:15,color:THEME.text,marginBottom:12}}>{row.Month} 2026</div>
                <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:10,fontSize:12}}>
                  {[
                    ["Net Sales", inr(row.TotalNetSalesValue||0)],
                    ["Net Units", (row.NetUnits||0).toLocaleString("en-IN")],
                    ["Sessions", (row.Sessions||0).toLocaleString("en-IN")],
                    ["Ad Spend", inr(row.TotalAdsSpend||0)],
                    ["Ad Sales", inr(row.TotalAdsSales||0)],
                    ["ACOS", `${((row.ACOS||0)*100).toFixed(1)}%`],
                    ["TACOS", `${((row.TACOS||0)*100).toFixed(1)}%`],
                    ["Buybox", `${((row.BuyboxPct||0)*100).toFixed(1)}%`],
                    ["Clicks", (row.Clicks||0).toLocaleString("en-IN")],
                    ["AMS Orders", (row.AmsOrders||0).toLocaleString("en-IN")],
                  ].map(([label, val]) => (
                    <div key={label} style={{padding:"10px",background:THEME.panelBg,borderRadius:12}}>
                      <div style={{fontSize:10,color:THEME.textMuted,marginBottom:4,fontFamily:"'DM Mono',monospace"}}>{label}</div>
                      <div style={{fontWeight:700,color:THEME.text}}>{val}</div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        );
      }
      if (ctTab === "Top 10") {
        const monthSequence = ctMonths;
        const top10BrandOptions = ["", ...new Set(enriched.map((row) => row.Brand).filter(Boolean))];
        const top10SourceRows = enriched.filter((row) => !ctTop10Brand || row.Brand === ctTop10Brand);
        const top10Data = (() => {
          if (ctTop10View === "ASIN") {
            const asinMap = {};
            top10SourceRows.forEach((r) => {
              if (!asinMap[r.ASIN]) asinMap[r.ASIN] = { asin: r.ASIN, title: r.Title, brand: r.Brand, model: getMasterValue(r,"model"), months: {} };
              asinMap[r.ASIN].months[r.Month] = r;
            });
            return Object.values(asinMap)
              .filter((x) => x.months[ctM1] || x.months[ctM2])
              .sort((a,b) => (((b.months[ctM2]?.TotalNetSalesValue||0) + (b.months[ctM1]?.TotalNetSalesValue||0)) - ((a.months[ctM2]?.TotalNetSalesValue||0) + (a.months[ctM1]?.TotalNetSalesValue||0))))
              .slice(0, 10);
          }
          // Group by model
          const modelMap = {};
          top10SourceRows.forEach(r => {
            const model = getMasterValue(r,"model") || "Unmapped";
            if (!modelMap[model]) modelMap[model] = { key: model, brand: r.Brand, title: r.Title, asin: r.ASIN, months: {} };
            if (!modelMap[model].months[r.Month]) modelMap[model].months[r.Month] = { TotalNetSalesValue: 0, NetUnits: 0 };
            modelMap[model].months[r.Month].TotalNetSalesValue += r.TotalNetSalesValue || 0;
            modelMap[model].months[r.Month].NetUnits += r.NetUnits || 0;
          });
          return Object.values(modelMap)
            .sort((a,b)=>(((b.months[ctM2]?.TotalNetSalesValue||0) + (b.months[ctM1]?.TotalNetSalesValue||0)) - ((a.months[ctM2]?.TotalNetSalesValue||0) + (a.months[ctM1]?.TotalNetSalesValue||0))))
            .slice(0,10)
            .map(m => ({ asin: m.asin, title: m.title, brand: m.brand, model: m.key, months: m.months }));
        })();
        const showAllMonths = ctTop10MonthMode === "all";
        const visibleMonths = showAllMonths ? monthSequence : [ctM1, ctM2];
        const gridTemplateColumns = `2fr 1fr repeat(${visibleMonths.length},1fr) 1fr repeat(${visibleMonths.length},1fr)`;
        return (
          <div style={{display:"grid",gap:12}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
              <div style={{fontSize:13,color:"#64748B"}}>Top 10 by Net Sales — {showAllMonths ? monthSequence.join(", ") : `${ctM1} vs ${ctM2}`}</div>
              <div style={{display:"flex",gap:6,alignItems:"center",flexWrap:"wrap"}}>
                <div style={{ display:"flex", gap:6, alignItems:"center", padding:"6px 10px", borderRadius:999, background:THEME.surface, border:`1px solid ${THEME.border}` }}>
                  <span style={{ fontSize:10, color:THEME.textFaint, fontFamily:"'DM Mono',monospace", letterSpacing:0.8, textTransform:"uppercase", fontWeight:600 }}>Brand</span>
                  <select
                    value={ctTop10Brand}
                    onChange={(e)=>setCtTop10Brand(e.target.value)}
                    style={{ background:"transparent", border:"none", color:THEME.text, fontSize:12, outline:"none", cursor:"pointer", fontWeight:600 }}
                  >
                    {top10BrandOptions.map((brandOption) => (
                      <option key={brandOption || "all"} value={brandOption}>
                        {brandOption || "All Brands"}
                      </option>
                    ))}
                  </select>
                </div>
                <div style={{ display:"flex", gap:6, alignItems:"center", padding:"6px 10px", borderRadius:999, background:THEME.surface, border:`1px solid ${THEME.border}` }}>
                  <span style={{ fontSize:10, color:THEME.textFaint, fontFamily:"'DM Mono',monospace", letterSpacing:0.8, textTransform:"uppercase", fontWeight:600 }}>Months</span>
                  <select
                    value={ctTop10MonthMode}
                    onChange={(e)=>setCtTop10MonthMode(e.target.value)}
                    style={{ background:"transparent", border:"none", color:THEME.text, fontSize:12, outline:"none", cursor:"pointer", fontWeight:600 }}
                  >
                    <option value="selected">Selected Months</option>
                    <option value="all">All Months</option>
                  </select>
                </div>
                {["ASIN","Model"].map(v=>(
                  <button key={v} onClick={()=>setCtTop10View(v)} style={{padding:"6px 14px",borderRadius:999,border:`1px solid ${ctTop10View===v?THEME.borderStrong:THEME.border}`,background:ctTop10View===v?THEME.headerBg:THEME.surface,color:THEME.text,fontSize:11,fontWeight:700,cursor:"pointer"}}>
                    Top 10 {v}
                  </button>
                ))}
              </div>
            </div>
            <div style={{display:"grid",gridTemplateColumns:gridTemplateColumns,gap:0,background:THEME.headerBg,borderRadius:"12px 12px 0 0",padding:"10px 14px",fontSize:10,color:THEME.headerText,fontFamily:"'DM Mono',monospace",letterSpacing:0.8}}>
              {[
                ctTop10View==="ASIN"?"ASIN":"MODEL",
                "BRAND",
                ...visibleMonths.map((monthLabel) => `${monthLabel} SALES`),
                "GROWTH",
                ...visibleMonths.map((monthLabel) => `${monthLabel} UNITS`),
              ].map(h=><div key={h}>{h}</div>)}
            </div>
            {top10Data.map((item, i) => {
              const s1 = item.months?.[ctM1]?.TotalNetSalesValue||0;
              const s2 = item.months?.[ctM2]?.TotalNetSalesValue||0;
              const g = pct(s1, s2);
              const amazonUrl = getAmazonProductUrl(item.asin);
              return (
                <div key={item.asin+i} style={{display:"grid",gridTemplateColumns:gridTemplateColumns,gap:0,padding:"12px 14px",background:i%2===0?THEME.surface:THEME.panelBg,border:`1px solid ${THEME.border}`,borderTop:"none",fontSize:12,alignItems:"center"}}>
                  <div>
                    <a
                      href={amazonUrl}
                      target="_blank"
                      rel="noreferrer"
                      title={`Open ${item.asin} on Amazon.in`}
                      style={{fontWeight:700,color:"#1D4ED8",textDecoration:"underline",textUnderlineOffset:"2px"}}
                    >
                      {ctTop10View==="ASIN"?item.asin:item.model}
                    </a>
                    <div style={{fontSize:10,color:"#64748B"}}>
                      <a
                        href={amazonUrl}
                        target="_blank"
                        rel="noreferrer"
                        title={`Open ${item.asin} on Amazon.in`}
                        style={{color:"inherit",textDecoration:"none"}}
                      >
                        {ctTop10View==="ASIN"?String(item.title).slice(0,40):item.asin}
                      </a>
                    </div>
                  </div>
                  <div style={{fontSize:11,color:"#475569"}}>{item.brand}</div>
                  {visibleMonths.map((monthLabel) => (
                    <div key={`${item.asin}-${monthLabel}-sales`} style={{fontWeight:600}}>
                      {inr(item.months?.[monthLabel]?.TotalNetSalesValue || 0)}
                    </div>
                  ))}
                  <div style={{fontWeight:700,color:Number(g)>=0?"#059669":"#DC2626"}}>{Number(g)>=0?"+":""}{g}%</div>
                  {visibleMonths.map((monthLabel) => (
                    <div key={`${item.asin}-${monthLabel}-units`}>
                      {(item.months?.[monthLabel]?.NetUnits || 0).toLocaleString("en-IN")}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        );
      }
      if (ctTab === "ASIN Compare") {
        const filteredAsins = ctAllAsins.filter(a =>
          !ctSearch || a.asin.toLowerCase().includes(ctSearch.toLowerCase()) ||
          a.title.toLowerCase().includes(ctSearch.toLowerCase()) ||
          a.brand.toLowerCase().includes(ctSearch.toLowerCase())
        ).slice(0, 50);
        const allMonthsList = ctMonths;
        const selectedRows = enriched.filter(r => r.ASIN === activectAsin);
        const metrics = [
          { label:"Net Sales", key:"TotalNetSalesValue", fmt:"cur" },
          { label:"Net Units", key:"NetUnits", fmt:"num" },
          { label:"Sessions", key:"Sessions", fmt:"num" },
          { label:"Buybox %", key:"BuyboxPct", fmt:"pct" },
          { label:"Ad Sales", key:"TotalAdsSales", fmt:"cur" },
          { label:"Ad Spend", key:"TotalAdsSpend", fmt:"cur" },
          { label:"ACOS", key:"ACOS", fmt:"pct" },
          { label:"TACOS", key:"TACOS", fmt:"pct" },
          { label:"Clicks", key:"Clicks", fmt:"num" },
          { label:"AMS Orders", key:"AmsOrders", fmt:"num" },
          { label:"Impressions", key:"Impressions", fmt:"num" },
          { label:"CAC", key:"CAC", fmt:"cur" },
          { label:"CVR %", key:"ConversionPct", fmt:"pct" },
          { label:"Organic %", key:"OrganicPct", fmt:"pct" },
          { label:"Organic Sales", key:"OrganiSales", fmt:"cur" },
        ];
        const fmtVal = (val, fmt) => fmt==="cur" ? inr(val||0) : fmt==="pct" ? `${((val||0)*100).toFixed(1)}%` : (val||0).toLocaleString("en-IN");
        return (
          <div style={{display:"grid",gap:16}}>
            {/* Search */}
            <div style={{position:"relative",maxWidth:480}}>
              <input
                value={ctSearch}
                onChange={e=>{
                  const v = e.target.value;
                  // Typing or pasting a whole ASIN selects it outright.  The result
                  // row below looks almost identical to the "now showing" bar under
                  // it, so it was easy to type an ASIN, read the table, and never
                  // realise a click was still required to switch products.
                  const exact = ctAllAsins.find(a => a.asin.toLowerCase() === v.trim().toLowerCase());
                  if (exact) { setCtAsin(exact.asin); setCtSearch(""); return; }
                  setCtSearch(v);
                }}
                onKeyDown={e=>{
                  if (e.key === "Enter" && filteredAsins.length) {
                    setCtAsin(filteredAsins[0].asin);
                    setCtSearch("");
                  }
                }}
                placeholder="Search ASIN, product name, brand — press Enter or click a result"
                style={{width:"100%",padding:"10px 14px 10px 36px",border:`1px solid ${THEME.border}`,borderRadius:12,fontSize:12,outline:"none",background:THEME.panelBg,color:THEME.text}}
              />
              <span style={{position:"absolute",left:12,top:"50%",transform:"translateY(-50%)",color:THEME.textFaint,fontSize:14}}>⌕</span>
            </div>
            {/* ASIN list */}
            {ctSearch && (
              <div style={{background:THEME.surface,border:`1px solid ${THEME.border}`,borderRadius:12,overflow:"hidden",maxHeight:240,overflowY:"auto"}}>
                <div style={{padding:"7px 14px",background:THEME.surfaceMuted,borderBottom:`1px solid ${THEME.border}`,fontSize:10,letterSpacing:0.8,fontWeight:700,fontFamily:"'DM Mono',monospace",color:THEME.textMuted}}>
                  SEARCH RESULTS — CLICK ONE TO SWITCH
                </div>
                {filteredAsins.length === 0 && (
                  <div style={{padding:"12px 14px",fontSize:12,color:THEME.textMuted}}>No ASIN matches “{ctSearch}”.</div>
                )}
                {filteredAsins.map(a=>(
                  <div key={a.asin} onClick={()=>{ setCtAsin(a.asin); setCtSearch(""); }}
                    style={{padding:"10px 14px",cursor:"pointer",borderBottom:`1px solid ${THEME.surfaceSoft}`,background:activectAsin===a.asin?"#EFF6FF":THEME.surface,display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                    <div>
                      <div style={{fontWeight:700,color:"#1D4ED8",fontSize:12}}>{a.asin}</div>
                      <div style={{fontSize:11,color:"#64748B"}}>{String(a.title).slice(0,60)}</div>
                    </div>
                    <span style={{fontSize:11,color:THEME.textFaint,marginLeft:8}}>{a.brand}</span>
                  </div>
                ))}
              </div>
            )}
            {/* Selected ASIN info */}
            {activectAsin && (
              <div style={{background:THEME.headerBg,border:`1px solid ${THEME.borderStrong}`,borderRadius:12,padding:"12px 16px",display:"flex",gap:12,alignItems:"center",flexWrap:"wrap"}}>
                {/* Labelled so this can't be mistaken for another search result. */}
                <span style={{fontSize:10,letterSpacing:0.8,fontWeight:700,fontFamily:"'DM Mono',monospace",color:"#1D4ED8",background:"#EFF6FF",border:"1px solid #BFDBFE",borderRadius:6,padding:"3px 7px"}}>NOW SHOWING</span>
                <span style={{color:THEME.text,fontWeight:700,fontSize:13}}>{activectAsin}</span>
                <span style={{color:THEME.textSoft,fontSize:12}}>{String(ctMeta?.Title||"").slice(0,60)}</span>
                <span style={{color:THEME.textMuted,fontSize:11,marginLeft:"auto"}}>{ctMeta?.Brand}</span>
              </div>
            )}
            {/* All months side by side table */}
            {activectAsin && (
              <div style={{background:THEME.surface,border:`1px solid ${THEME.border}`,borderRadius:16,overflow:"hidden"}}>
                <div style={{display:"grid",gridTemplateColumns:`2fr repeat(${allMonthsList.length},1fr)`,background:THEME.surfaceMuted,padding:"10px 16px",gap:0,borderBottom:`1px solid ${THEME.border}`}}>
                  <div style={{fontSize:10,color:THEME.textMuted,fontFamily:"'DM Mono',monospace",letterSpacing:0.8,fontWeight:700}}>METRIC</div>
                  {allMonthsList.map(m=>(
                    <div key={m} style={{fontSize:10,color:THEME.textSoft,fontFamily:"'DM Mono',monospace",letterSpacing:0.8,textAlign:"right",fontWeight:700}}>{m} 2026</div>
                  ))}
                </div>
                {metrics.map((metric,i) => {
                  const vals = allMonthsList.map(m => selectedRows.find(r=>r.Month===m)?.[metric.key] ?? null);
                  return (
                    <div key={metric.key} style={{display:"grid",gridTemplateColumns:`2fr repeat(${allMonthsList.length},1fr)`,padding:"10px 16px",background:i%2===0?THEME.surface:THEME.panelBg,borderBottom:`1px solid ${THEME.surfaceSoft}`,alignItems:"center"}}>
                      <div style={{fontSize:12,color:THEME.textSoft,fontWeight:600}}>{metric.label}</div>
                      {vals.map((val,vi) => (
                        <div key={vi} style={{fontSize:12,fontWeight:700,color:val===null?"#CBD5E1":THEME.text,textAlign:"right"}}>
                          {val===null ? "—" : fmtVal(val, metric.fmt)}
                        </div>
                      ))}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        );
      }
    };

    return (
      <div style={{ fontFamily:"'Inter','Segoe UI',sans-serif", background:THEME.pageBg, minHeight:"100vh", color:THEME.text }}>
        {/* Header */}
        <div style={{ background:THEME.surface, borderBottom:`1px solid ${THEME.border}`, padding:"12px 24px", display:"flex", justifyContent:"space-between", alignItems:"center", flexWrap:"wrap", gap:12 }}>
          <div style={{display:"flex", alignItems:"center", gap:12}}>
            <button onClick={closeDetailPage} style={{ background:THEME.surfaceMuted, color:THEME.textSoft, border:`1px solid ${THEME.border}`, borderRadius:8, padding:"7px 12px", fontSize:11, fontWeight:700, cursor:"pointer", display:"flex", alignItems:"center", gap:6 }}>
              ← Back
            </button>
            <div>
              <div style={{ fontSize:16, fontWeight:800, letterSpacing:-0.3, color:THEME.text }}>Month Comparison</div>
              <div style={{ fontSize:10, color:THEME.textFaint, fontFamily:"'DM Mono',monospace", letterSpacing:0.8 }}>{ctMeta?.Brand || ""} · {getMasterValue(ctMeta,"model")||activectAsin}</div>
            </div>
          </div>
          <div style={{ display:"flex", gap:8, alignItems:"center", flexWrap:"wrap" }}>
            {/* Month filter */}
            <div style={{ display:"flex", gap:4, alignItems:"center", background:THEME.panelBg, padding:"7px 12px", borderRadius:8, border:`1px solid ${THEME.border}` }}>
              <select value={ctM1} onChange={e=>setCtM1(e.target.value)} style={{ background:"transparent", border:"none", color:THEME.text, fontSize:12, outline:"none", cursor:"pointer", fontWeight:600 }}>
                {ctMonths.map(m=><option key={m} value={m} style={{color:THEME.text,background:THEME.surface}}>{m} 2026</option>)}
              </select>
              <span style={{ color:THEME.textFaint, fontSize:10, fontFamily:"'DM Mono',monospace" }}>VS</span>
              <select value={ctM2} onChange={e=>setCtM2(e.target.value)} style={{ background:"transparent", border:"none", color:THEME.text, fontSize:12, outline:"none", cursor:"pointer", fontWeight:600 }}>
                {ctMonths.map(m=><option key={m} value={m} style={{color:THEME.text,background:THEME.surface}}>{m} 2026</option>)}
              </select>
            </div>
            <button onClick={exportCtCsv} style={{ background:THEME.panelBg, color:THEME.textSoft, border:`1px solid ${THEME.border}`, borderRadius:8, padding:"7px 12px", fontSize:11, fontWeight:600, cursor:"pointer" }}>
              Export CSV ↓
            </button>
          </div>
        </div>

        {/* Tabs with icons */}
        <div style={{ background:THEME.surface, borderBottom:`2px solid ${THEME.border}`, padding:"0 24px", display:"flex", gap:2, overflowX:"auto" }}>
          {TABS.map(t => (
            <button key={t} onClick={() => setCtTab(t)} style={{ background:"none", border:"none", color: ctTab===t ? THEME.text : THEME.textFaint, fontSize:11, fontWeight: ctTab===t ? 700 : 400, padding:"11px 14px", cursor:"pointer", borderBottom: ctTab===t ? "2px solid #2563EB" : "2px solid transparent", whiteSpace:"nowrap", display:"flex", alignItems:"center", gap:5, transition:"all 0.1s" }}>
              <span style={{fontSize:13}}>{TAB_ICONS[t]}</span> {t}
            </button>
          ))}
        </div>

        {/* ASIN info bar - truncated title */}
        <div style={{ background:THEME.surface, borderBottom:`1px solid ${THEME.border}`, padding:"10px 24px", display:"flex", justifyContent:"space-between", alignItems:"center", gap:8 }}>
          <div style={{ fontSize:13, fontWeight:700, color:THEME.text, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap", maxWidth:"60%", cursor:"default" }}
            title={ctMeta?.Title||""}>
            {ctMeta?.Title||""}
          </div>
          <div style={{ display:"flex", gap:6, flexShrink:0 }}>
            {[ctMeta?.ASIN, ctMeta?.Brand, getMasterValue(ctMeta,"model")||"—", ctMeta?.mainCat||"—"].filter(Boolean).map((v,i) => (
              <span key={i} style={{ padding:"3px 8px", borderRadius:6, background:THEME.surfaceMuted, color:THEME.textSoft, fontSize:11, fontWeight:600, whiteSpace:"nowrap" }}>{v}</span>
            ))}
          </div>
        </div>

        {/* Tab content */}
        <div style={{ padding:"24px" }}>
          {ctTab === "Overview" && (
            <div style={{ marginBottom:16, background:THEME.surface, border:`1px solid ${THEME.border}`, borderRadius:16, padding:"16px 18px", boxShadow:"0 10px 24px rgba(15,23,42,0.04)" }}>
              <div style={{ fontSize:10, color:THEME.textMuted, fontFamily:"'DM Mono',monospace", letterSpacing:1, textTransform:"uppercase", marginBottom:8 }}>Summary Insight</div>
              <div style={{ fontSize:18, fontWeight:800, color:THEME.text, marginBottom:8 }}>{compareInsights.title}</div>
              <div style={{ fontSize:13, color:THEME.textSoft, lineHeight:1.6, marginBottom:10 }}>{compareInsights.summary}</div>
              <div style={{ display:"grid", gap:8 }}>
                {[
                  { title: "What Improved", body: compareInsights.improvedText, tone: "#059669" },
                  { title: "What Worsened", body: compareInsights.worsenedText, tone: "#DC2626" },
                  { title: "What To Do Next", body: compareInsights.actionText, tone: "#2563EB" },
                ].map((item) => (
                  <div key={item.title} style={{ background:THEME.panelBg, borderRadius:10, padding:"10px 12px", border:`1px solid ${THEME.surfaceSoft}` }}>
                    <div style={{ fontSize:10, color:item.tone, fontFamily:"'DM Mono',monospace", letterSpacing:0.9, textTransform:"uppercase", marginBottom:4 }}>{item.title}</div>
                    <div style={{ fontSize:12, color:THEME.textSoft, lineHeight:1.55 }}>{item.body}</div>
                  </div>
                ))}
                {compareInsights.highlights.length > 0 && (
                  <div style={{ fontSize:11, color:THEME.textMuted, marginTop:2 }}>
                    Key highlight: {compareInsights.highlights[0]}
                  </div>
                )}
              </div>
            </div>
          )}
          {renderTab()}
        </div>
      </div>
    );
  }

  // ── MODEL DETAIL PAGE ────────────────────────────────────────────────────
  if (modelTabKey && modelTabMeta) {
    // Auto-derive available months from actual data (works for any number of months)
    const availableMonths = MONTH_SEQUENCE.filter(m => modelTabRows.some(r => r.Month === m));
    const selectedMonth = selectedModelMonth;
    const setSelectedMonth = setSelectedModelMonth;
    // One aggregated row per month (model may have multiple ASINs per month)
    const byMonth = availableMonths.map(mon => {
      const rows = modelTabRows.filter(r => r.Month === mon);
      if (!rows.length) return null;
      const sum = (key) => rows.reduce((acc, r) => acc + (r[key] || 0), 0);
      const wavg = (key, wKey) => {
        const totalW = sum(wKey);
        if (!totalW) return 0;
        return rows.reduce((acc, r) => acc + (r[key] || 0) * (r[wKey] || 0), 0) / totalW;
      };
      const adSpend = sum("TotalAdsSpend");
      const adSales = sum("TotalAdsSales");
      const netSales = sum("TotalNetSalesValue");
      const amsOrders = sum("AmsOrders");
      return {
        Month: mon,
        Sessions: sum("Sessions"),
        NetUnits: sum("NetUnits"),
        TotalNetSalesValue: netSales,
        TotalAdsSpend: adSpend,
        TotalAdsSales: adSales,
        Impressions: sum("Impressions"),
        Clicks: sum("Clicks"),
        AmsOrders: amsOrders,
        OrganiSales: sum("OrganiSales"),
        BuyboxPct: wavg("BuyboxPct","Sessions"),
        ACOS: adSales > 0 ? adSpend / adSales : 0,
        TACOS: netSales > 0 ? adSpend / netSales : 0,
        CAC: amsOrders > 0 ? adSpend / amsOrders : 0,
        ConversionPct: sum("Sessions") > 0 ? sum("NetUnits") / sum("Sessions") : 0,
        OrganicPct: netSales > 0 ? Math.max(0, netSales - adSales) / netSales : 0,
        asinCount: rows.length,
      };
    }).filter(Boolean);

    // Apply month filter for display
    const filteredByMonth = selectedMonth === "All" ? byMonth : byMonth.filter(r => r.Month === selectedMonth);
    const filteredModelRows = selectedMonth === "All" ? modelTabRows : modelTabRows.filter(r => r.Month === selectedMonth);

    // All unique ASINs for this model
    const modelAsins = [...new Set(modelTabRows.map(r => r.ASIN))];

    // Growth cards Jan→latest available
    const firstMon = byMonth[0];
    const lastMon = byMonth[byMonth.length - 1];
    const growthCards = [
      { label: "Sales Growth", jan: firstMon?.TotalNetSalesValue ?? 0, feb: lastMon?.TotalNetSalesValue ?? 0, fmt: "currency", accent: "#2563EB" },
      { label: "Ad Sales Growth", jan: firstMon?.TotalAdsSales ?? 0, feb: lastMon?.TotalAdsSales ?? 0, fmt: "currency", accent: "#EC4899" },
      { label: "Units Growth", jan: firstMon?.NetUnits ?? 0, feb: lastMon?.NetUnits ?? 0, fmt: "number", accent: "#10B981" },
      { label: "Buybox Growth", jan: (firstMon?.BuyboxPct ?? 0)*100, feb: (lastMon?.BuyboxPct ?? 0)*100, fmt: "pct", accent: "#F59E0B" },
    ].map(m => {
      const delta = m.feb - m.jan;
      const growthPct = m.jan > 0 ? (delta / m.jan) * 100 : m.feb > 0 ? 100 : 0;
      return { ...m, delta, growthPct };
    });

    // Summary totals across all months
    const totAdSpend  = filteredByMonth.reduce((a,r) => a + r.TotalAdsSpend, 0);
    const totAdSales  = filteredByMonth.reduce((a,r) => a + r.TotalAdsSales, 0);
    const totNetSales = filteredByMonth.reduce((a,r) => a + r.TotalNetSalesValue, 0);
    const totUnits    = filteredByMonth.reduce((a,r) => a + r.NetUnits, 0);
    const totOrders   = filteredByMonth.reduce((a,r) => a + r.AmsOrders, 0);
    const totClicks   = filteredByMonth.reduce((a,r) => a + r.Clicks, 0);
    const totImpr     = filteredByMonth.reduce((a,r) => a + r.Impressions, 0);
    const totSessions = filteredByMonth.reduce((a,r) => a + r.Sessions, 0);
    const overallACOS  = totAdSales  > 0 ? totAdSpend / totAdSales  : 0;
    const overallTACOS = totNetSales > 0 ? totAdSpend / totNetSales : 0;
    const overallCAC   = totOrders   > 0 ? totAdSpend / totOrders   : 0;
    const overallBB    = filteredByMonth.reduce((a,r,_,arr) => a + r.BuyboxPct / arr.length, 0);

    return (
      <div style={{ fontFamily:"'Inter','Segoe UI',sans-serif", background:`linear-gradient(180deg,${THEME.pageBg} 0%,${THEME.shellBg} 52%,${THEME.panelBg} 100%)`, minHeight:"100vh", color:THEME.text }}>

        {/* ── Header ── */}
        <div style={{ background:`linear-gradient(90deg,${THEME.headerBg} 0%,${THEME.surfaceSoft} 100%)`, padding:"16px 24px", borderBottom:`1px solid ${THEME.border}` }}>
          <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", gap:16, flexWrap:"wrap" }}>
            <div>
              <div style={{ fontSize:26, fontWeight:800, letterSpacing:-0.8, fontFamily:"'DM Mono',monospace" }}>
                {modelTabKey}
                <span style={{ marginLeft:12, fontSize:13, fontWeight:400, color:THEME.textMuted, letterSpacing:0 }}>Model Overview</span>
              </div>
              <div style={{ fontSize:11, color:THEME.textMuted, marginTop:4, fontFamily:"'DM Mono',monospace", letterSpacing:1.1 }}>
                {modelTabMeta.Brand} · {modelTabMeta.mainCat || modelTabMeta.category || "—"} · {modelAsins.length} ASIN{modelAsins.length !== 1 ? "s" : ""}
              </div>
            </div>
            <div style={{ display:"flex", alignItems:"center", gap:8 }}>
              {/* Month toggle pills */}
              <div style={{ display:"flex", gap:4, background:"#1E293B", borderRadius:10, padding:"4px" }}>
                {["All", ...availableMonths].map(m => (
                  <button
                    key={m}
                    onClick={() => setSelectedMonth(m)}
                    style={{
                      padding:"6px 14px", borderRadius:7, border:"none", fontSize:11, fontWeight:700,
                      cursor:"pointer", transition:"all 0.15s",
                      background: selectedMonth === m ? "#2563EB" : "transparent",
                      color: selectedMonth === m ? "#fff" : "#94A3B8",
                    }}
                  >{m}</button>
                ))}
              </div>
              <button
                onClick={() => { if (window.opener) window.close(); else window.history.back(); }}
                style={{ padding:"10px 18px", borderRadius:999, background:"#2563EB", border:"none", color:"#fff", fontSize:12, fontWeight:700, cursor:"pointer" }}
              >
                {window.opener ? "Close Tab" : "← Back"}
              </button>
            </div>
          </div>
        </div>

        <div style={{ padding:"24px" }}>

          {/* ── Summary KPI Cards ── */}
          <div style={{ display:"grid", gridTemplateColumns:"repeat(4,minmax(0,1fr))", gap:14, marginBottom:20 }}>
            {[
              { label:"Total Net Sales",  value: inr(totNetSales),   accent:"#2563EB" },
              { label:"Total Units",      value: totUnits.toLocaleString("en-IN"), accent:"#10B981" },
              { label:"Total Ads Spend",  value: inr(totAdSpend),    accent:"#EC4899" },
              { label:"Total Ads Sales",  value: inr(totAdSales),    accent:"#F97316" },
              { label:"Overall ACOS",     value: `${(overallACOS*100).toFixed(1)}%`,  accent:"#7C3AED" },
              { label:"Overall TACOS",    value: `${(overallTACOS*100).toFixed(1)}%`, accent:"#0EA5E9" },
              { label:"Avg Buybox %",     value: `${(overallBB*100).toFixed(1)}%`,    accent:"#F59E0B" },
              { label:"Overall CAC",      value: inr(overallCAC),    accent:"#64748B" },
            ].map(card => (
              <div key={card.label} style={{ background:"#fff", border:"1px solid #D8E1EC", borderLeft:`4px solid ${card.accent}`, borderRadius:14, padding:"14px 16px", boxShadow:"0 8px 20px rgba(15,23,42,0.05)" }}>
                <div style={{ fontSize:9.5, color:"#64748B", fontFamily:"'DM Mono',monospace", letterSpacing:1.3, textTransform:"uppercase", marginBottom:6 }}>{card.label}</div>
                <div style={{ fontSize:22, fontWeight:800, color:card.accent, letterSpacing:-0.5 }}>{card.value}</div>
              </div>
            ))}
          </div>

          {/* ── Growth Cards (first → last month) ── */}
          <div style={{ display:"grid", gridTemplateColumns:"repeat(4,minmax(0,1fr))", gap:14, marginBottom:20 }}>
            {growthCards.map(m => (
              <div key={m.label} style={{ padding:"14px", borderRadius:14, background:"#fff", border:"1px solid #D8E1EC", boxShadow:"0 8px 20px rgba(15,23,42,0.04)", borderLeft:`4px solid ${m.growthPct >= 0 ? "#10B981" : "#EF4444"}` }}>
                <div style={{ fontSize:10, color:"#64748B", marginBottom:6, fontFamily:"'DM Mono',monospace", letterSpacing:0.8, textTransform:"uppercase" }}>{m.label}</div>
                <div style={{ fontSize:22, fontWeight:800, color: m.growthPct >= 0 ? "#059669" : "#DC2626" }}>
                  {m.growthPct >= 0 ? "+" : ""}{m.growthPct.toFixed(1)}%
                </div>
                <div style={{ fontSize:11, color:"#64748B", marginTop:6 }}>
                  {byMonth[0]?.Month} {m.fmt === "currency" ? inr(m.jan) : m.fmt === "pct" ? `${m.jan.toFixed(1)}%` : m.jan.toLocaleString("en-IN")}
                  {" → "}
                  {byMonth[byMonth.length-1]?.Month} {m.fmt === "currency" ? inr(m.feb) : m.fmt === "pct" ? `${m.feb.toFixed(1)}%` : m.feb.toLocaleString("en-IN")}
                </div>
              </div>
            ))}
          </div>

          {/* ── Monthly Summary Breakdown ── */}
          <div style={{ background:"#fff", border:"1px solid #D8E1EC", borderRadius:16, boxShadow:"0 10px 24px rgba(15,23,42,0.05)", marginBottom:20, overflow:"hidden" }}>
            <div style={{ padding:"12px 18px", background:`linear-gradient(90deg,#EEF2FF,transparent)`, borderBottom:"1px solid #E2E8F0", fontSize:10, fontFamily:"'DM Mono',monospace", letterSpacing:0.9, textTransform:"uppercase", color:"#334155", fontWeight:700 }}>
              Monthly Breakdown — {modelTabKey}{selectedMonth !== "All" ? ` (${selectedMonth})` : ""}
            </div>
            <div style={{ overflowX:"auto" }}>
              <table style={{ width:"100%", borderCollapse:"collapse", fontSize:12 }}>
                <thead>
                  <tr style={{ background:"#F8FAFC" }}>
                    {["Month","ASINs","Sessions","Net Units","Net Sales","Ads Spend","Ads Sales","AMS Orders","Impressions","Clicks","Buybox %","ACOS","TACOS","CAC","CVR","Organic %"].map(h => (
                      <th key={h} style={{ padding:"9px 12px", textAlign: h==="Month"||h==="ASINs" ? "left" : "right", fontFamily:"'DM Mono',monospace", fontSize:9.5, textTransform:"uppercase", letterSpacing:0.8, color:"#64748B", borderBottom:"1px solid #E2E8F0", whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filteredByMonth.map((r, i) => (
                    <tr key={r.Month} style={{ background: i%2===0 ? "#fff" : "#F8FAFC", borderBottom:"1px solid #F1F5F9" }}>
                      <td style={{ padding:"8px 12px", fontWeight:700, fontFamily:"'DM Mono',monospace", fontSize:12, color:"#1E293B" }}>{r.Month} 2026</td>
                      <td style={{ padding:"8px 12px", textAlign:"left", color:"#64748B" }}>{r.asinCount}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right" }}>{r.Sessions.toLocaleString("en-IN")}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", fontWeight:700 }}>{r.NetUnits.toLocaleString("en-IN")}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", fontWeight:700, color:"#2563EB" }}>{inr(r.TotalNetSalesValue)}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", color:"#EC4899" }}>{inr(r.TotalAdsSpend)}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", color:"#F97316" }}>{inr(r.TotalAdsSales)}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right" }}>{r.AmsOrders.toLocaleString("en-IN")}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", color:"#94A3B8" }}>{r.Impressions.toLocaleString("en-IN")}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", color:"#94A3B8" }}>{r.Clicks.toLocaleString("en-IN")}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", color:"#F59E0B", fontWeight:700 }}>{(r.BuyboxPct*100).toFixed(1)}%</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", color: r.ACOS > 0.4 ? "#EF4444" : "#059669", fontWeight:600 }}>{(r.ACOS*100).toFixed(1)}%</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", color:"#7C3AED" }}>{(r.TACOS*100).toFixed(1)}%</td>
                      <td style={{ padding:"8px 12px", textAlign:"right" }}>{inr(r.CAC)}</td>
                      <td style={{ padding:"8px 12px", textAlign:"right" }}>{(r.ConversionPct*100).toFixed(2)}%</td>
                      <td style={{ padding:"8px 12px", textAlign:"right", color:"#10B981" }}>{(r.OrganicPct*100).toFixed(1)}%</td>
                    </tr>
                  ))}
                  {/* Totals row — show aggregate of currently visible months */}
                  {filteredByMonth.length > 1 && (
                  <tr style={{ background:"#EEF2FF", borderTop:"2px solid #C7D2FE", fontWeight:700 }}>
                    <td style={{ padding:"9px 12px", fontFamily:"'DM Mono',monospace", fontSize:12, color:"#1E293B" }}>TOTAL</td>
                    <td style={{ padding:"9px 12px", color:"#64748B" }}>—</td>
                    <td style={{ padding:"9px 12px", textAlign:"right" }}>{totSessions.toLocaleString("en-IN")}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right" }}>{totUnits.toLocaleString("en-IN")}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color:"#2563EB" }}>{inr(totNetSales)}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color:"#EC4899" }}>{inr(totAdSpend)}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color:"#F97316" }}>{inr(totAdSales)}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right" }}>{totOrders.toLocaleString("en-IN")}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color:"#94A3B8" }}>{totImpr.toLocaleString("en-IN")}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color:"#94A3B8" }}>{totClicks.toLocaleString("en-IN")}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color:"#F59E0B" }}>{(overallBB*100).toFixed(1)}%</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color: overallACOS > 0.4 ? "#EF4444" : "#059669" }}>{(overallACOS*100).toFixed(1)}%</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color:"#7C3AED" }}>{(overallTACOS*100).toFixed(1)}%</td>
                    <td style={{ padding:"9px 12px", textAlign:"right" }}>{inr(overallCAC)}</td>
                    <td style={{ padding:"9px 12px", textAlign:"right" }}>—</td>
                    <td style={{ padding:"9px 12px", textAlign:"right", color:"#10B981" }}>—</td>
                  </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* ── Per-ASIN Detail Table ── */}
          <div style={{ background:"#fff", border:"1px solid #D8E1EC", borderRadius:16, boxShadow:"0 10px 24px rgba(15,23,42,0.05)", overflow:"hidden" }}>
            <div style={{ padding:"12px 18px", background:`linear-gradient(90deg,#F0FDF4,transparent)`, borderBottom:"1px solid #E2E8F0", fontSize:10, fontFamily:"'DM Mono',monospace", letterSpacing:0.9, textTransform:"uppercase", color:"#334155", fontWeight:700 }}>
              All ASINs under {modelTabKey}{selectedMonth !== "All" ? ` — ${selectedMonth}` : ""} — Full Metrics
            </div>
            <div style={{ overflowX:"auto" }}>
              <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11.5 }}>
                <thead>
                  <tr style={{ background:"#F8FAFC" }}>
                    {["ASIN","Month","Title","Sessions","Net Units","Net Sales","Ads Spend","Ads Sales","AMS Orders","Buybox %","ACOS","TACOS","CAC","CVR","Organic %","Action"].map(h => (
                      <th key={h} style={{ padding:"9px 12px", textAlign: ["ASIN","Month","Title","Action"].includes(h) ? "left" : "right", fontFamily:"'DM Mono',monospace", fontSize:9.5, textTransform:"uppercase", letterSpacing:0.7, color:"#64748B", borderBottom:"1px solid #E2E8F0", whiteSpace:"nowrap", position:"sticky", top:0, background:"#F8FAFC", zIndex:2 }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filteredModelRows.map((r, i) => (
                    <tr key={`${r.Brand}-${r.ASIN}-${r.Month}`} style={{ background: i%2===0 ? "#fff" : "#F8FAFC", borderBottom:"1px solid #F1F5F9" }}>
                      <td style={{ padding:"6px 12px", fontFamily:"'DM Mono',monospace", fontSize:11, fontWeight:800, color:"#1D4ED8" }}>
                        <a href={getAmazonProductUrl(r.ASIN)} target="_blank" rel="noreferrer" style={{ color:"#1D4ED8", textDecoration:"underline", textUnderlineOffset:"2px" }}>{r.ASIN}</a>
                      </td>
                      <td style={{ padding:"6px 12px", fontFamily:"'DM Mono',monospace", fontWeight:700, color:"#475569", fontSize:11 }}>{r.Month}</td>
                      <td style={{ padding:"6px 12px", maxWidth:180, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap", color:"#334155" }} title={r.Title}>{r.Title || "—"}</td>
                      <td style={{ padding:"6px 12px", textAlign:"right" }}>{(r.Sessions||0).toLocaleString("en-IN")}</td>
                      <td style={{ padding:"6px 12px", textAlign:"right", fontWeight:700 }}>{(r.NetUnits||0).toLocaleString("en-IN")}</td>
                      <td style={{ padding:"6px 12px", textAlign:"right", fontWeight:700, color:"#2563EB" }}>{inr(r.TotalNetSalesValue||0)}</td>
                      <td style={{ padding:"6px 12px", textAlign:"right", color:"#EC4899" }}>{inr(r.TotalAdsSpend||0)}</td>
                      <td style={{ padding:"6px 12px", textAlign:"right", color:"#F97316" }}>{inr(r.TotalAdsSales||0)}</td>
                      <td style={{ padding:"6px 12px", textAlign:"right" }}>{(r.AmsOrders||0).toLocaleString("en-IN")}</td>
                      <td style={{ padding:"6px 12px", textAlign:"right", color:"#F59E0B", fontWeight:700 }}>{((r.BuyboxPct||0)*100).toFixed(1)}%</td>
                      <td style={{ padding:"6px 12px", textAlign:"right", color:(r.ACOS||0)>0.4?"#EF4444":"#059669", fontWeight:600 }}>{((r.ACOS||0)*100).toFixed(1)}%</td>
                      <td style={{ padding:"6px 12px", textAlign:"right", color:"#7C3AED" }}>{((r.TACOS||0)*100).toFixed(1)}%</td>
                      <td style={{ padding:"6px 12px", textAlign:"right" }}>{inr(r.CAC||0)}</td>
                      <td style={{ padding:"6px 12px", textAlign:"right" }}>{((r.ConversionPct||0)*100).toFixed(2)}%</td>
                      <td style={{ padding:"6px 12px", textAlign:"right", color:"#10B981" }}>{((r.OrganicPct||0)*100).toFixed(1)}%</td>
                      <td style={{ padding:"6px 12px" }}>
                        <button
                          onClick={() => openDetailPage(r.ASIN)}
                          style={{ padding:"4px 10px", borderRadius:999, background:"#EFF6FF", border:"1px solid #BFDBFE", color:"#1D4ED8", fontSize:10, fontWeight:700, cursor:"pointer", whiteSpace:"nowrap" }}
                        >
                          ASIN Detail
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

        </div>
      </div>
    );
  }

  if (detailMeta) {
    return (
      <div style={{ fontFamily: "'Inter', 'Segoe UI', sans-serif", background: `linear-gradient(180deg, ${THEME.pageBg} 0%, ${THEME.shellBg} 52%, ${THEME.panelBg} 100%)`, minHeight: "100vh", color: THEME.text }}>
        <div style={{ background:`linear-gradient(90deg, ${THEME.headerBg} 0%, ${THEME.surfaceSoft} 100%)`, color:THEME.text, padding:"16px 24px", borderBottom:`1px solid ${THEME.border}` }}>
          <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", gap:16, flexWrap:"wrap" }}>
            <div>
              <div style={{ fontSize:24, fontWeight:800, letterSpacing:-0.7 }}>{getMasterValue(detailMeta, "model") || detailMeta.ASIN} Drilldown</div>
              <div style={{ fontSize:11, color:THEME.textMuted, marginTop:4, fontFamily:"'DM Mono', monospace", letterSpacing:1.1 }}>ASIN DETAIL · SMART PRODUCT VIEW</div>
            </div>
            <div style={{ fontSize:11, fontFamily:"'DM Mono', monospace", color:THEME.textMuted }}>{detailMeta.Brand} · {compareM1} vs {compareM2}</div>
          </div>
        </div>
        <div style={{ padding:"10px 24px", background:THEME.surfaceMuted, borderBottom:`1px solid ${THEME.border}`, display:"flex", gap:12, flexWrap:"wrap" }}>
          {["Overview", "Sales Trend", "Ads", "Units", "Monthly Drill"].map((tabLabel, index) => (
            <div key={tabLabel} style={{ color:index === 0 ? THEME.text : THEME.textMuted, fontSize:11, fontWeight:700, padding:"4px 10px", borderRadius:999, background:index === 0 ? THEME.headerBg : "transparent", fontFamily:"'DM Mono', monospace", letterSpacing:0.7 }}>
              {tabLabel}
            </div>
          ))}
        </div>
        <div style={{ padding: "24px" }}>
        <Panel
          title={detailMeta.Title}
          subtitle={`${detailMeta.ASIN} • ${detailMeta.Brand} • ${detailMeta.mainCat || detailMeta.category || "Category not mapped"}`}
          right={
            <div style={{ display:"flex", gap:8, flexWrap:"wrap", justifyContent:"flex-end" }}>
              <button onClick={exportDetailData} style={{ background:"#FFFFFF", color:"#1D4ED8", border:"1px solid #BFDBFE", borderRadius:999, padding:"10px 14px", fontSize:12, fontWeight:700, cursor:"pointer" }}>
                Export ASIN CSV
              </button>
              <button onClick={() => exportSvgNode(detailChartRef.current, `buybox-asin-${detailMeta.ASIN}-sales-chart.svg`)} style={{ background:"#FFFFFF", color:"#0F172A", border:"1px solid #D8E1EC", borderRadius:999, padding:"10px 14px", fontSize:12, fontWeight:700, cursor:"pointer" }}>
                Export Chart
              </button>
              <button onClick={printCurrentView} style={{ background:"#FFFFFF", color:"#0F172A", border:"1px solid #D8E1EC", borderRadius:999, padding:"10px 14px", fontSize:12, fontWeight:700, cursor:"pointer" }}>
                Save PDF
              </button>
              <button onClick={closeDetailPage} style={{ background:"#2563EB", color:"#FFFFFF", border:"none", borderRadius:999, padding:"10px 16px", fontSize:12, fontWeight:700, cursor:"pointer" }}>
                {window.opener ? "Close tab" : "← Back to dashboard"}
              </button>
            </div>
          }
        >
          <div style={{ display:"grid", gridTemplateColumns:"repeat(5, minmax(0, 1fr))", gap:12, marginBottom:18 }}>
            <div style={{
              background: "linear-gradient(180deg, #FFFFFF, #F8FAFC)",
              border: "1px solid #D8E1EC",
              boxShadow: "0 12px 28px rgba(15,23,42,0.06)",
              borderRadius: 16,
              padding: "16px 18px",
              minWidth: 152,
              flex: "0 0 auto",
              borderLeft: "3px solid #2563EB",
            }}>
              <div style={{ fontSize: 9.5, color: "#64748B", fontFamily: "'DM Mono',monospace", letterSpacing: 1.4, textTransform: "uppercase", marginBottom: 8 }}>ASIN</div>
              <a
                href={getAmazonProductUrl(detailMeta.ASIN)}
                target="_blank"
                rel="noreferrer"
                style={{ fontSize: 23, fontWeight: 700, color: "#2563EB", letterSpacing: -1, lineHeight: 1, textDecoration: "underline", textUnderlineOffset: "4px" }}
                title={`Open ${detailMeta.ASIN} on Amazon`}
              >
                {detailMeta.ASIN}
              </a>
              <div style={{ fontSize: 10, color: "#94A3B8", marginTop: 6 }}>Open Amazon product page</div>
            </div>
            <KpiCard label="FBA SKU" value={getMasterValue(detailMeta, "fbaSku") || "—"} accent="#0EA5E9" />
            <KpiCard label="Brand" value={detailMeta.Brand} accent="#0EA5E9" />
            <KpiCard label="Model" value={getMasterValue(detailMeta, "model") || "—"} accent="#8B5CF6" />
            <KpiCard label="Category" value={[getMasterValue(detailMeta, "catL0"), getMasterValue(detailMeta, "catL1")].filter(Boolean).join(" › ") || "—"} accent="#F59E0B" />
          </div>

          <div style={{ display:"grid", gridTemplateColumns:"repeat(4, minmax(0, 1fr))", gap:12, marginBottom:16 }}>
            {detailGrowth.map((metric) => (
              <div key={metric.key} style={{ padding:"14px", borderRadius:18, background:"#FFFFFF", border:"1px solid #D8E1EC", boxShadow:"0 10px 24px rgba(15,23,42,0.04)", borderLeft:`4px solid ${metric.growthPct >= 0 ? "#10B981" : "#EF4444"}` }}>
                <div style={{ fontSize:10, color:"#64748B", marginBottom:8, fontFamily:"'DM Mono', monospace", letterSpacing:0.8, textTransform:"uppercase" }}>{metric.label}</div>
                <div style={{ fontSize:22, fontWeight:800, color:metric.growthPct >= 0 ? "#059669" : "#DC2626" }}>
                  {metric.growthPct >= 0 ? "+" : ""}{metric.growthPct.toFixed(1)}%
                </div>
                <div style={{ fontSize:11, color:"#64748B", marginTop:6 }}>
                  {detailM1?.Month ?? compareM1} {metric.key === "buybox" ? `${(metric.jan * 100).toFixed(1)}%` : metric.key === "units" ? (metric.jan || 0).toLocaleString("en-IN") : inr(metric.jan || 0).replace(".0L", "L")}
                  {" → "}
                  {detailM2?.Month ?? compareM2} {metric.key === "buybox" ? `${(metric.feb * 100).toFixed(1)}%` : metric.key === "units" ? (metric.feb || 0).toLocaleString("en-IN") : inr(metric.feb || 0).replace(".0L", "L")}
                </div>
              </div>
            ))}
          </div>

          <div style={{ display:"grid", gridTemplateColumns:"repeat(4, minmax(0, 1fr))", gap:12, marginBottom:16 }}>
            {[
              {
                title: "Traffic Module",
                accent: "#5B8DEF",
                rows: [
                  `${detailM1?.Month ?? compareM1} Sessions: ${(detailM1?.Sessions ?? 0).toLocaleString("en-IN")}`,
                  `${detailM2?.Month ?? compareM2} Sessions: ${(detailM2?.Sessions ?? 0).toLocaleString("en-IN")}`,
                  `Total Clicks: ${((detailM1?.Clicks ?? 0) + (detailM2?.Clicks ?? 0)).toLocaleString("en-IN")}`,
                ],
              },
              {
                title: "Sales Module",
                accent: "#16A34A",
                rows: [
                  `${detailM1?.Month ?? compareM1} Sales: ${inr(detailM1?.TotalNetSalesValue ?? 0)}`,
                  `${detailM2?.Month ?? compareM2} Sales: ${inr(detailM2?.TotalNetSalesValue ?? 0)}`,
                  `Ads Sales: ${inr((detailM1?.TotalAdsSales ?? 0) + (detailM2?.TotalAdsSales ?? 0))}`,
                ],
              },
              {
                title: "Unit Module",
                accent: "#8B5CF6",
                rows: [
                  `${detailM1?.Month ?? compareM1} Units: ${(detailM1?.NetUnits ?? 0).toLocaleString("en-IN")}`,
                  `${detailM2?.Month ?? compareM2} Units: ${(detailM2?.NetUnits ?? 0).toLocaleString("en-IN")}`,
                  `AMS Orders: ${((detailM1?.AmsOrders ?? 0) + (detailM2?.AmsOrders ?? 0)).toLocaleString("en-IN")}`,
                ],
              },
              {
                title: "Efficiency Module",
                accent: "#F59E0B",
                rows: [
                  `ACOS: ${((((detailM1?.TotalAdsSpend ?? 0) + (detailM2?.TotalAdsSpend ?? 0)) / Math.max((detailM1?.TotalAdsSales ?? 0) + (detailM2?.TotalAdsSales ?? 0), 1)) * 100).toFixed(1)}%`,
                  `TACOS: ${((((detailM1?.TotalAdsSpend ?? 0) + (detailM2?.TotalAdsSpend ?? 0)) / Math.max((detailM1?.TotalNetSalesValue ?? 0) + (detailM2?.TotalNetSalesValue ?? 0), 1)) * 100).toFixed(1)}%`,
                  `CAC: ${inr((((detailM1?.TotalAdsSpend ?? 0) + (detailM2?.TotalAdsSpend ?? 0)) / Math.max((detailM1?.AmsOrders ?? 0) + (detailM2?.AmsOrders ?? 0), 1)) || 0)}`,
                ],
              },
            ].map((module) => (
              <div key={module.title} style={{ background:"#FFFFFF", border:"1px solid #D8E1EC", borderRadius:18, overflow:"hidden", boxShadow:"0 12px 28px rgba(15,23,42,0.05)" }}>
                <div style={{ padding:"10px 14px", background:`linear-gradient(90deg, ${module.accent}18, transparent)`, borderBottom:`1px solid ${module.accent}22`, fontSize:10, fontFamily:"'DM Mono', monospace", letterSpacing:0.9, textTransform:"uppercase", color:"#334155", fontWeight:700 }}>{module.title}</div>
                <div style={{ padding:"14px", display:"grid", gap:8 }}>
                  {module.rows.map((text) => (
                    <div key={text} style={{ display:"flex", justifyContent:"space-between", gap:12, fontSize:12, color:"#0F172A" }}>
                      <span>{text.split(":")[0]}</span>
                      <strong style={{ color:module.accent }}>{text.split(": ").slice(1).join(": ")}</strong>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>

          <div style={{ display:"grid", gridTemplateColumns:"1.2fr 1fr", gap:16, alignItems:"start" }}>
            <div style={{ display:"grid", gap:16 }}>
              <div style={{ padding:"14px", borderRadius:18, background:"#FFFFFF", border:"1px solid #D8E1EC", boxShadow:"0 12px 28px rgba(15,23,42,0.05)" }}>
                <div style={{ color:"#334155", fontSize:10, marginBottom:10, fontFamily:"'DM Mono', monospace", letterSpacing:0.8, textTransform:"uppercase" }}>Month-wise net sales</div>
                <TrendLineChart
                  svgRef={detailChartRef}
                  valueType="currency"
                  color="#2563EB"
                  points={detailRows.map((row) => ({ label: row.Month, value: row.TotalNetSalesValue ?? 0 }))}
                />
              </div>
              <div style={{ display:"grid", gridTemplateColumns:"repeat(3, minmax(0, 1fr))", gap:12 }}>
                <div style={{ padding:"14px", borderRadius:18, background:"#FFFFFF", border:"1px solid #D8E1EC", boxShadow:"0 12px 28px rgba(15,23,42,0.05)" }}>
                  <div style={{ color:"#334155", fontSize:10, marginBottom:10, fontFamily:"'DM Mono', monospace", letterSpacing:0.8, textTransform:"uppercase" }}>Ad trend</div>
                  <MiniMonthBars jan={detailM1?.TotalAdsSales ?? 0} feb={detailM2?.TotalAdsSales ?? 0} type="currency" colorA="#F472B6" colorB="#FB7185" />
                </div>
                <div style={{ padding:"14px", borderRadius:18, background:"#FFFFFF", border:"1px solid #D8E1EC", boxShadow:"0 12px 28px rgba(15,23,42,0.05)" }}>
                  <div style={{ color:"#334155", fontSize:10, marginBottom:10, fontFamily:"'DM Mono', monospace", letterSpacing:0.8, textTransform:"uppercase" }}>Unit trend</div>
                  <MiniMonthBars jan={detailM1?.NetUnits ?? 0} feb={detailM2?.NetUnits ?? 0} colorA="#34D399" colorB="#10B981" />
                </div>
                <div style={{ padding:"14px", borderRadius:18, background:"#FFFFFF", border:"1px solid #D8E1EC", boxShadow:"0 12px 28px rgba(15,23,42,0.05)" }}>
                  <div style={{ color:"#334155", fontSize:10, marginBottom:10, fontFamily:"'DM Mono', monospace", letterSpacing:0.8, textTransform:"uppercase" }}>Buybox trend</div>
                  <MiniMonthBars jan={(detailM1?.BuyboxPct ?? 0) * 100} feb={(detailM2?.BuyboxPct ?? 0) * 100} colorA="#FBBF24" colorB="#F59E0B" />
                </div>
              </div>
            </div>
            <div style={{ display:"grid", gap:12 }}>
              {detailRows.map((row) => (
                <div key={row.Month} style={{ padding:"14px", borderRadius:18, background:"#FFFFFF", border:"1px solid #D8E1EC", boxShadow:"0 12px 28px rgba(15,23,42,0.05)" }}>
                  <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:10 }}>
                    <div style={{ fontSize:14, fontWeight:700, color:"#0F172A" }}>{row.Month} 2026</div>
                    <span style={{ padding:"4px 8px", borderRadius:999, background:"#EEF2FF", color:"#4338CA", fontSize:11, fontWeight:700 }}>{row.Brand}</span>
                  </div>
                  <div style={{ display:"grid", gridTemplateColumns:"repeat(2, minmax(0, 1fr))", gap:8, color:"#334155", fontSize:12 }}>
                    <div>Sessions: <strong>{(row.Sessions ?? 0).toLocaleString("en-IN")}</strong></div>
                    <div>Net Units: <strong>{(row.NetUnits ?? 0).toLocaleString("en-IN")}</strong></div>
                    <div>Net Sales: <strong>{inr(row.TotalNetSalesValue ?? 0)}</strong></div>
                    <div>Ads Sales: <strong>{inr(row.TotalAdsSales ?? 0)}</strong></div>
                    <div>Ads Spend: <strong>{inr(row.TotalAdsSpend ?? 0)}</strong></div>
                    <div>AMS Orders: <strong>{(row.AmsOrders ?? 0).toLocaleString("en-IN")}</strong></div>
                    <div>Clicks: <strong>{(row.Clicks ?? 0).toLocaleString("en-IN")}</strong></div>
                    <div>Impressions: <strong>{(row.Impressions ?? 0).toLocaleString("en-IN")}</strong></div>
                    <div>Buybox: <strong>{((row.BuyboxPct || 0) * 100).toFixed(1)}%</strong></div>
                    <div>Organic Units: <strong>{(row.OrganiSales ?? 0).toLocaleString("en-IN")}</strong></div>
                    <div>ACOS: <strong>{((row.ACOS || 0) * 100).toFixed(1)}%</strong></div>
                    <div>TACOS: <strong>{((row.TACOS || 0) * 100).toFixed(1)}%</strong></div>
                    <div>CVR: <strong>{((row.ConversionPct || 0) * 100).toFixed(2)}%</strong></div>
                    <div>Organic %: <strong>{((row.OrganicPct || 0) * 100).toFixed(1)}%</strong></div>
                    <div>CAC: <strong>{inr(row.CAC ?? 0)}</strong></div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </Panel>
        </div>
      </div>
    );
  }

  const aiSuggestions = [
    "Give me a full performance overview",
    "Which ASIN has the best ACOS?",
    "Top 5 ASINs by Net Sales",
    "Which brand spent most on ads?",
    "Compare all 4 brands: Nexlev, Audio Array, Tonor, White Mulberry",
    "Which ASINs have Buybox below 50%?",
    "Show me high TACOS ASINs to fix",
  ];

  const buildAiContext = () => {
    try {
      const brandTotals = brands.map(b => {
        const rows = enriched.filter(r => r.Brand === b);
        const sales = rows.reduce((s,r) => s+(r.TotalNetSalesValue||0),0);
        const spend = rows.reduce((s,r) => s+(r.TotalAdsSpend||0),0);
        const units = rows.reduce((s,r) => s+(r.NetUnits||0),0);
        const orders = rows.reduce((s,r) => s+(r.AmsOrders||0),0);
        const adsSales = rows.reduce((s,r) => s+(r.TotalAdsSales||0),0);
        return `${b}: Sales=₹${(sales/100000).toFixed(1)}L, Spend=₹${(spend/100000).toFixed(1)}L, Units=${units}, Orders=${orders}, AdsSales=₹${(adsSales/100000).toFixed(1)}L`;
      }).join("\n");
      const top10 = [...filtered].sort((a,b)=>(b.TotalNetSalesValue||0)-(a.TotalNetSalesValue||0)).slice(0,10).map(r=>
        `${r.ASIN}(${r.Brand},${r.Month}): Sales=₹${((r.TotalNetSalesValue||0)/1000).toFixed(0)}K, Units=${r.NetUnits||0}, ACOS=${((r.ACOS||0)*100).toFixed(1)}%, Buybox=${((r.BuyboxPct||0)*100).toFixed(1)}%, Sessions=${r.Sessions||0}`
      ).join("\n");
      return `You are an expert Amazon performance analyst AI for BrandIQ Hub dashboard.\n\nBRAND TOTALS:\n${brandTotals}\n\nTOP 10 ASINs (filter: Brand=${brand||"All"}, Month=${month.length===0?"All":month.join(",")}):\n${top10}\n\nOVERALL: Sessions=${totals.sessions.toLocaleString()}, Units=${totals.units.toLocaleString()}, Sales=₹${(totals.sales/10000000).toFixed(2)}Cr, Spend=₹${(totals.spend/100000).toFixed(1)}L, AvgACOS=${(avgAcos*100).toFixed(1)}%, AvgTACOS=${(avgTacos*100).toFixed(1)}%\n\nBe concise and business-focused. Use bullet points and bold key numbers with ₹ signs.`;
    } catch(err) {
      console.error("buildAiContext error:", err);
      return "You are an expert Amazon performance analyst AI for BrandIQ Hub dashboard. Data could not be loaded.";
    }
  };

  const sendAiMessage = async (text) => {
    const msg = text || aiInput.trim();
    if (!msg || aiLoading) return;
    setAiInput("");
    const newMessages = [...aiMessages, { role:"user", content:msg }];
    setAiMessages(newMessages);
    setAiLoading(true);
    try {
      const res = await fetch("https://openrouter.ai/api/v1/chat/completions", {
        method:"POST",
        headers:{"Content-Type":"application/json","Authorization":`Bearer ${import.meta.env.VITE_OPENROUTER_API_KEY}`},
        body:JSON.stringify({ model:"meta-llama/llama-3.1-8b-instruct:free", max_tokens:1000, messages:[{role:"system",content:buildAiContext()},...newMessages] }),
      });
      const data = await res.json();
      const reply = data.choices?.[0]?.message?.content || "Sorry, couldn't get a response.";
      setAiMessages([...newMessages, { role:"assistant", content:reply }]);
    } catch(e) {
      setAiMessages([...newMessages, { role:"assistant", content:"Error: " + e.message }]);
    }
    setAiLoading(false);
    setTimeout(() => aiEndRef.current?.scrollIntoView({behavior:"smooth"}), 100);
  };

  const fmtAi = (text) => text.split("\n").map((line,i) => {
    if (line.startsWith("## ")) return <div key={i} style={{fontWeight:800,fontSize:14,color:"#0F172A",margin:"8px 0 2px"}}>{line.slice(3)}</div>;
    if (line.startsWith("# ")) return <div key={i} style={{fontWeight:800,fontSize:15,color:"#1D4ED8",margin:"8px 0 2px"}}>{line.slice(2)}</div>;
    if (line.startsWith("- ")||line.startsWith("• ")) return <div key={i} style={{display:"flex",gap:6,margin:"2px 0"}}><span style={{color:"#2563EB",flexShrink:0}}>•</span><span dangerouslySetInnerHTML={{__html:line.slice(2).replace(/\*\*(.*?)\*\*/g,"<strong>$1</strong>")}}/></div>;
    if (!line.trim()) return <div key={i} style={{height:5}}/>;
    return <div key={i} style={{margin:"2px 0"}} dangerouslySetInnerHTML={{__html:line.replace(/\*\*(.*?)\*\*/g,"<strong>$1</strong>")}}/>;
  });

  return (
    <div style={{ fontFamily: "'Inter', 'Segoe UI', sans-serif", background: "#FFFFFF", minHeight: "100vh", color: "#0F172A" }}>

      {/* AI Floating Button + Chat */}
      <div style={{position:"fixed",bottom:24,right:24,zIndex:9999,display:"flex",flexDirection:"column",alignItems:"flex-end",gap:12}}>
        {aiOpen && (
          <div style={{width:380,background:"#FFFFFF",borderRadius:24,boxShadow:"0 24px 64px rgba(15,23,42,0.2)",border:"1px solid #E2E8F0",display:"flex",flexDirection:"column",overflow:"hidden",maxHeight:"80vh"}}>
            <div style={{background:"linear-gradient(135deg,#4338CA 0%,#2563EB 100%)",padding:"16px 18px",display:"flex",justifyContent:"space-between",alignItems:"center",flexShrink:0}}>
              <div style={{display:"flex",gap:10,alignItems:"center"}}>
                <div style={{width:36,height:36,borderRadius:999,background:"rgba(255,255,255,0.2)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:18}}>🤖</div>
                <div>
                  <div style={{color:"#fff",fontWeight:800,fontSize:14}}>BuyBox AI</div>
                  <div style={{color:"rgba(255,255,255,0.7)",fontSize:10,fontFamily:"'DM Mono',monospace",letterSpacing:0.8}}>AMAZON PERFORMANCE ANALYST</div>
                </div>
              </div>
              <button onClick={()=>setAiOpen(false)} style={{background:"rgba(255,255,255,0.2)",border:"none",borderRadius:999,color:"#fff",width:28,height:28,cursor:"pointer",fontSize:13,display:"flex",alignItems:"center",justifyContent:"center"}}>✕</button>
            </div>
            <div style={{flex:1,overflowY:"auto",padding:"14px",display:"flex",flexDirection:"column",gap:10,minHeight:0,maxHeight:380}}>
              {aiMessages.length===0 && (
                <div>
                  <div style={{background:"linear-gradient(135deg,#EEF2FF,#F0F9FF)",borderRadius:16,padding:"14px",fontSize:13,color:"#334155",marginBottom:10,border:"1px solid #C7D2FE"}}>
                    Hi! I have access to all your Amazon data — ASINs, brands, months, metrics. Ask me anything! 🚀
                  </div>
                  <div style={{fontSize:10,color:"#94A3B8",marginBottom:8,fontFamily:"'DM Mono',monospace",letterSpacing:0.8}}>TRY ASKING:</div>
                  <div style={{display:"flex",flexDirection:"column",gap:6}}>
                    {aiSuggestions.map(s=>(
                      <button key={s} onClick={()=>sendAiMessage(s)} style={{background:"#EEF2FF",border:"1px solid #C7D2FE",borderRadius:999,padding:"8px 14px",fontSize:12,color:"#4338CA",cursor:"pointer",textAlign:"left",fontWeight:600,transition:"all 0.15s"}}>{s}</button>
                    ))}
                  </div>
                </div>
              )}
              {aiMessages.map((m,i)=>(
                <div key={i} style={{display:"flex",justifyContent:m.role==="user"?"flex-end":"flex-start"}}>
                  <div style={{maxWidth:"88%",padding:"10px 14px",borderRadius:m.role==="user"?"18px 18px 4px 18px":"18px 18px 18px 4px",background:m.role==="user"?"linear-gradient(135deg,#4338CA,#2563EB)":"#F8FAFC",color:m.role==="user"?"#fff":"#0F172A",fontSize:13,lineHeight:1.55,border:m.role==="assistant"?"1px solid #E2E8F0":"none"}}>
                    {m.role==="assistant"?fmtAi(m.content):m.content}
                  </div>
                </div>
              ))}
              {aiLoading && (
                <div style={{display:"flex",gap:5,padding:"12px 14px",background:"#F8FAFC",borderRadius:"18px 18px 18px 4px",width:"fit-content",border:"1px solid #E2E8F0"}}>
                  {[0,1,2].map(i=><div key={i} style={{width:7,height:7,borderRadius:999,background:"#94A3B8",animation:`aiBounce 1s ease-in-out ${i*0.15}s infinite`}}/>)}
                </div>
              )}
              <div ref={aiEndRef}/>
            </div>
            {aiMessages.length>0 && (
              <div style={{padding:"6px 14px",borderTop:"1px solid #F1F5F9",flexShrink:0}}>
                <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
                  {aiSuggestions.slice(0,3).map(s=>(
                    <button key={s} onClick={()=>sendAiMessage(s)} style={{background:"#F8FAFC",border:"1px solid #E2E8F0",borderRadius:999,padding:"4px 10px",fontSize:11,color:"#475569",cursor:"pointer"}}>{s}</button>
                  ))}
                </div>
              </div>
            )}
            <div style={{padding:"12px 14px",borderTop:"1px solid #E2E8F0",display:"flex",gap:8,flexShrink:0}}>
              <input value={aiInput} onChange={e=>setAiInput(e.target.value)} onKeyDown={e=>e.key==="Enter"&&!e.shiftKey&&sendAiMessage()} placeholder="Ask about sales, ASINs, trends..." style={{flex:1,border:"1px solid #D8E1EC",borderRadius:999,padding:"9px 14px",fontSize:12,outline:"none",color:"#0F172A",background:"#F8FAFC"}}/>
              <button onClick={()=>sendAiMessage()} disabled={aiLoading||!aiInput.trim()} style={{background:"linear-gradient(135deg,#4338CA,#2563EB)",border:"none",borderRadius:999,width:36,height:36,cursor:aiInput.trim()?"pointer":"not-allowed",display:"flex",alignItems:"center",justifyContent:"center",opacity:aiInput.trim()?1:0.4,flexShrink:0}}>
                <span style={{color:"#fff",fontSize:16}}>↑</span>
              </button>
            </div>
          </div>
        )}
        <button onClick={()=>setAiOpen(o=>!o)} style={{width:56,height:56,borderRadius:999,background:"linear-gradient(135deg,#4338CA,#2563EB)",border:"none",boxShadow:"0 8px 28px rgba(67,56,202,0.45)",cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",fontSize:24,transition:"transform 0.2s"}}>
          {aiOpen?"✕":"🤖"}
        </button>
      </div>
      <style>{`@keyframes aiBounce{0%,100%{transform:translateY(0)}50%{transform:translateY(-5px)}}`}</style>
      <div style={{ background: THEME.surface, borderBottom: `1px solid ${THEME.border}`, padding: "0 24px", display: "flex", alignItems: "center", justifyContent: "space-between", minHeight: 64 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ width: 32, height: 32, borderRadius: 8, background: "linear-gradient(135deg,#38BDF8,#2563EB)", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span style={{ color: "#fff", fontSize: 13, fontWeight: 800 }}>N</span>
          </div>
          <div>
            <div style={{ fontSize: 15, fontWeight: 700, color: THEME.text, letterSpacing: -0.4 }}>BrandIQ Hub</div>
            <div style={{ fontSize: 9, color: THEME.textFaint, fontFamily: "'DM Mono',monospace", letterSpacing: 1 }}>AMAZON PERFORMANCE INTELLIGENCE</div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <button
            onClick={exportFilteredData}
            style={{ background:THEME.panelBg, border:`1px solid ${THEME.border}`, borderRadius:8, padding:"7px 12px", fontSize:11, color:THEME.textSoft, fontWeight:600, cursor:"pointer" }}
          >
            Export CSV
          </button>
          {onLogout && (
            <button
              onClick={onLogout}
              style={{ background:"#FEF2F2", border:"1px solid #FECACA", borderRadius:8, padding:"7px 12px", fontSize:11, color:"#B91C1C", fontWeight:700, cursor:"pointer" }}
            >
              Logout
            </button>
          )}
          <span style={{ fontSize: 10, color: THEME.textFaint, fontFamily: "'DM Mono',monospace" }}>{aggregatedRows.length} rows</span>
        </div>
      </div>

      {/* Filters */}
      <div style={{ background: THEME.shellBg, borderBottom: `1px solid ${THEME.border}`, padding: "12px 24px", display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
        <div style={{ position: "relative", flex: "0 0 auto" }}>
          <span style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: THEME.textFaint, fontSize: 13 }}>{isSearchPending ? "⟳" : "⌕"}</span>
          <input
            placeholder="Search ASIN, SKU, model, product…"
            value={search} onChange={onSearch}
            style={{ background: search?"#EFF6FF":THEME.surface, border: search?"1px solid #3B82F6":`1px solid ${THEME.border}`, borderRadius: 8, color: THEME.text, padding: "9px 14px 9px 34px", fontSize: 12, width: 260, outline: "none", transition:"all 0.15s" }}
          />
        </div>
        <MultiSelectFilter label="Brand" selected={brand} setSelected={(v)=>{setBrand(v);setCompareAsin("");setTablePage(1);}} opts={brands.map(b=>({v:b,l:b}))} isActive={brand.length>0} />
        <MultiSelectFilter label="Month" selected={month} setSelected={(v)=>{setMonth(v);setTablePage(1);}} opts={MONTH_SEQUENCE.map((monthLabel) => ({ v: monthLabel, l: monthLabel.replace(/ \d{4}$/, ""), g: monthYearOf(monthLabel) }))} isActive={month.length>0} />
        <MultiSelectFilter label="Category L0" allLabel="Categorys" selected={catL0} setSelected={(v)=>{setCatL0(v);setCompareAsin("");setTablePage(1);}} opts={catsL0.map(c=>({v:c,l:c}))} isActive={catL0.length>0} />
          <MultiSelectFilter label="Category L1" allLabel="Categorys" selected={catL1} setSelected={(v)=>{setCatL1(v);setCompareAsin("");setTablePage(1);}} opts={catsL1.map(c=>({v:c,l:c}))} isActive={catL1.length>0} />
        {/* Data Mode Toggle */}
        <div style={{ display:"flex", gap:0, borderRadius:8, border:`1px solid ${THEME.border}`, overflow:"hidden", flexShrink:0 }}>
          {[{v:"biz",l:"Amazon Sales"},{v:"p1",l:"1P Sales"}].map(({v,l}) => (
            <button key={v} onClick={() => setDataMode(prev => prev === v ? "all" : v)} style={{
              padding:"7px 13px", border:"none", fontSize:11, fontWeight:700,
              cursor:"pointer", transition:"all 0.15s", whiteSpace:"nowrap",
              background: dataMode===v ? "#2563EB" : THEME.surface,
              color: dataMode===v ? "#fff" : THEME.textSoft,
              borderRight: v==="biz" ? `1px solid ${THEME.border}` : "none",
            }}>{l}</button>
          ))}
        </div>
        <div style={{ marginLeft:"auto", display:"flex", gap:8, alignItems:"center" }}>
          <span style={{ fontSize:11, color: activeFilterCount > 0 ? "#2563EB" : THEME.textFaint, fontFamily:"'DM Mono',monospace" }}>
            {activeFilterCount > 0 ? `${activeFilterCount} filter${activeFilterCount>1?"s":""} active` : `${aggregatedRows.length} rows`}
          </span>
          <div style={{ display:"flex", gap:6, alignItems:"center", padding:"7px 10px", borderRadius:8, background:THEME.surface, border:`1px solid ${THEME.border}` }}>
            <span style={{ fontSize:10, color:THEME.textFaint, fontFamily:"'DM Mono',monospace", fontWeight:600 }}>SORT</span>
            <select value={sortKey} onChange={e=>{setSortKey(e.target.value); setTablePage(1);}} style={{ background:"transparent", border:"none", color:THEME.text, padding:"1px 20px 1px 2px", fontSize:12, outline:"none", appearance:"none", backgroundImage:"url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 24 24' fill='none' stroke='%2394A3B8' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E\")", backgroundRepeat:"no-repeat", backgroundPosition:"right 4px center" }}>
            {COLS.map(c=><option key={c.key} value={c.key}>{c.label}</option>)}
            </select>
          </div>
        </div>
      </div>
      <div style={{ background: THEME.shellBg, borderBottom: `1px solid ${THEME.border}`, padding: "0 24px 12px" }}>
        <div style={{ display:"flex", gap:10, alignItems:"center", flexWrap:"wrap" }}>
          <button
            onClick={() => openSmartViewPage()}
            style={{
              padding:"9px 16px",
              borderRadius:"12px 12px 0 0",
              border:"none",
              borderBottom:"2px solid #2563EB",
              background:THEME.surface,
              color:"#1D4ED8",
              fontSize:11,
              fontWeight:700,
              cursor:"pointer",
              boxShadow:"0 -1px 0 rgba(255,255,255,0.7) inset",
            }}
          >
            Smart View
          </button>
          <button
            onClick={() => openAppTab("#bsr-tracker")}
            style={{
              border:"none",
              borderBottom: bsrPage ? "2px solid #8B5CF6" : "2px solid transparent",
              background:THEME.surface,
              color: bsrPage ? "#7C3AED" : THEME.textMuted,
              fontSize:11,
              fontWeight:700,
              cursor:"pointer",
              padding:"7px 14px",
              borderRadius:0,
            }}
          >
            📊 BSR Tracker
          </button>
          <button
            onClick={openAnalyticsPage}
            style={{
              border:"none",
              borderBottom: analyticsPage ? "2px solid #14B8A6" : "2px solid transparent",
              background:THEME.surface,
              color: analyticsPage ? "#0F766E" : THEME.textMuted,
              fontSize:11,
              fontWeight:700,
              cursor:"pointer",
              padding:"7px 14px",
              borderRadius:0,
            }}
          >
            📈 Analytics
          </button>
          <button
            onClick={() => openAppTab("#refresh-diff")}
            style={{
              border:"none",
              borderBottom: refreshDiffPage ? "2px solid #F59E0B" : "2px solid transparent",
              background:THEME.surface,
              color: refreshDiffPage ? "#B45309" : THEME.textMuted,
              fontSize:11,
              fontWeight:700,
              cursor:"pointer",
              padding:"7px 14px",
              borderRadius:0,
            }}
          >
            🔄 Refresh Diff · June
          </button>
          <button
            onClick={() => openComparePage(activeCompareAsin)}
            disabled={!activeCompareAsin}
            style={{ background: activeCompareAsin ? "#2563EB" : THEME.surfaceSoft, border:"none", borderRadius:8, padding:"7px 14px", fontSize:11, color:activeCompareAsin ? "#FFFFFF" : THEME.textMuted, fontWeight:700, cursor:activeCompareAsin?"pointer":"not-allowed", whiteSpace:"nowrap" }}
          >
            Compare View →
          </button>
        </div>
      </div>

      {/* KPI Strip */}
      <div style={{ padding:"12px 24px", display:"flex", gap:10, overflowX:"auto", flexWrap:"nowrap", WebkitOverflowScrolling:"touch", background:THEME.surface, borderBottom:`1px solid ${THEME.surfaceSoft}` }}>
        <KpiCard label="Sessions" value={dataMode === "p1" ? "N/A" : totals.sessions.toLocaleString("en-IN")} accent="#38BDF8" trend={dataMode === "p1" ? null : trendPct(totals.sessions, prevTotals.sessions)} />
        <KpiCard label="Net Units"  value={totals.units.toLocaleString("en-IN")}        accent="#34D399" trend={trendPct(totals.units, prevTotals.units)} />
        <KpiCard label={dataMode === "p1" ? "1P Sales" : dataMode === "biz" ? "Amazon Sales" : "Net Sales"}  value={inr(totals.sales)}   accent="#A78BFA" trend={trendPct(totals.sales, prevTotals.sales)} />
        <KpiCard label="Ad Spend"  value={inr(totals.spend)}   accent="#FBBF24" trend={trendPct(totals.spend, prevTotals.spend)} />
        <KpiCard label="Ad Sales"  value={inr(totals.adsSales)} accent="#F9A8D4" trend={trendPct(totals.adsSales, prevTotals.adsSales)} />
        <KpiCard label="AMS Orders" value={totals.orders.toLocaleString("en-IN")}       accent="#FB923C" trend={trendPct(totals.orders, prevTotals.orders)} />
        <KpiCard label="Avg ACoS"   value={(avgAcos*100).toFixed(1)+"%"}  accent={avgAcos<0.2?"#34D399":avgAcos<0.35?"#FBBF24":"#F87171"} />
        <KpiCard label="Avg TACoS"  value={(avgTacos*100).toFixed(1)+"%"} accent={avgTacos<0.1?"#34D399":avgTacos<0.2?"#FBBF24":"#F87171"} />
      </div>

      {/* Table */}
      <div style={{ padding:"0 24px 32px", overflowX:"auto" }}>
        <div style={{ borderRadius:12, border:`1px solid ${THEME.border}`, overflow:"hidden", minWidth:1800, background:THEME.surface, boxShadow:"0 4px 16px rgba(15,23,42,0.04)", opacity: isSearchPending ? 0.6 : 1, transition:"opacity 0.15s" }}>
          <div ref={tableScrollerRef} style={{ overflow:"auto", maxHeight:"calc(100vh - 240px)" }}>
          <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11.5 }}>
            <thead>
              <tr style={{ background:THEME.headerBg }}>
                {COLS.map(col => (
                  <th key={col.key} onClick={()=>handleSort(col.key)} style={{
                    padding:"10px 10px", textAlign: col.fmt==="str"||col.fmt==="title"?"left":"right",
                    color: sortKey===col.key?"#1D4ED8":THEME.headerText,
                    fontFamily:"'DM Mono',monospace", fontSize:9.5, fontWeight:500,
                    letterSpacing:0.7, textTransform:"uppercase", cursor:"pointer",
                    whiteSpace:"nowrap", userSelect:"none", minWidth:col.w,
                    background: sortKey===col.key ? THEME.headerBgActive : THEME.headerBg,
                    borderBottom: `1px solid ${THEME.borderStrong}`,
                    position:"sticky", top:0,
                    left: STICKY_LEFT_OFFSETS[col.key],
                    zIndex: STICKY_LEFT_OFFSETS[col.key] !== undefined ? 8 : 5,
                    borderRight: STICKY_LEFT_OFFSETS[col.key] !== undefined ? `1px solid ${THEME.borderStrong}` : undefined,
                    boxShadow: `inset 0 -1px 0 ${THEME.borderStrong}, ${THEME.headerShadow}`,
                  }}>
                    {col.label}{sortKey===col.key&&<span style={{marginLeft:3}}>{sortDir===-1?"↓":"↑"}</span>}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {pagedRows.map((row, index) => {
                const i = (currentPage - 1) * TABLE_PAGE_SIZE + index;
                const rowBackground = i % 2 === 0 ? THEME.surface : THEME.panelBg;
                return (
                  <tr key={`${row.Brand}-${row.ASIN}-${row.Month}`} style={{ borderBottom:"1px solid #F1F5F9", background: rowBackground }}
                    onMouseEnter={e=>{
                      e.currentTarget.style.background=THEME.hover;
                      e.currentTarget.querySelectorAll("[data-sticky='true']").forEach((cell) => {
                        cell.style.background = THEME.hover;
                      });
                    }}
                    onMouseLeave={e=>{
                      e.currentTarget.style.background=rowBackground;
                      e.currentTarget.querySelectorAll("[data-sticky='true']").forEach((cell) => {
                        cell.style.background = rowBackground;
                      });
                    }}
                  >
                    {COLS.map(col => {
                      const v    = col.master ? getMasterValue(row, col.key) : row[col.key];
                      const disp = fmtCell(row, col);
                      const isPct = col.fmt === "pct";
                      const accentColor = isPct ? pctColor(col.key, v) : col.key === "Title" ? THEME.text : THEME.textSoft;
                      const isEmpty = v === null || v === undefined || v === 0 || v === "";

                      return (
                        <td key={col.key} data-sticky={STICKY_LEFT_OFFSETS[col.key] !== undefined ? "true" : "false"} style={{
                          padding:"5px 10px",
                          textAlign: col.fmt==="str"||col.fmt==="title"?"left":"right",
                          fontFamily: col.key==="ASIN"||col.key==="fbaSku"||col.key==="model" ? "'DM Mono',monospace" : "inherit",
                          fontSize: col.key==="ASIN" ? 12 : col.key==="fbaSku" ? 10.8 : 11.5,
                          maxWidth: col.fmt==="title" ? 140 : undefined,
                          overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
                          background: STICKY_LEFT_OFFSETS[col.key] !== undefined ? rowBackground : col.master ? (isEmpty?THEME.surfaceMuted:"#F0F4FF") : "transparent",
                          color: isEmpty ? "#CBD5E1" : col.key==="ASIN" ? "#1D4ED8" : accentColor,
                          letterSpacing: col.key==="ASIN" ? 0.3 : 0,
                          fontWeight: col.key==="ASIN" ? 800 : col.key==="model" ? 600 : col.key==="TotalNetSalesValue" ? 700 : col.key==="dp"||col.key==="nlc" ? 600 : 400,
                          position: STICKY_LEFT_OFFSETS[col.key] !== undefined ? "sticky" : "static",
                          left: STICKY_LEFT_OFFSETS[col.key],
                          zIndex: STICKY_LEFT_OFFSETS[col.key] !== undefined ? 4 : 1,
                          borderRight: STICKY_LEFT_OFFSETS[col.key] !== undefined ? `1px solid ${THEME.border}` : undefined,
                          boxShadow: STICKY_LEFT_OFFSETS[col.key] === STICKY_LEFT_OFFSETS.category ? THEME.stickyShadow : undefined,
                        }}>
                          {col.key === "ASIN" && !isEmpty ? (
                            <a
                              href={`https://www.amazon.in/dp/${encodeURIComponent(row.ASIN)}`}
                              target="_blank"
                              rel="noreferrer"
                              title={`Open ${row.ASIN} in new tab`}
                              style={{ color:"#1D4ED8", fontFamily:"inherit", fontSize:"inherit", cursor:"pointer", fontWeight:800, textDecoration:"underline", textUnderlineOffset:"2px" }}
                            >
                              {disp}
                            </a>
                          ) : col.key === "Title" && !isEmpty ? (
                            <button
                              onClick={null}
                              title={String(row.Title)}
                              style={{ background:"none", border:"none", padding:0, color:THEME.text, fontFamily:"inherit", fontSize:"inherit", cursor:"pointer", textAlign:"left", fontWeight:600, maxWidth:130, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap", display:"block" }}
                            >
                              {String(row.Title).split("|")[0].split(",")[0].trim().slice(0,32)}
                            </button>
                          ) : col.key === "model" && !isEmpty ? (
                            <span
                              onClick={() => openModelPage(v)}
                              title={`Open model ${v} detail`}
                              style={{ color:"#7C3AED", cursor:"pointer", textDecoration:"underline", textUnderlineOffset:"2px", fontWeight:700 }}
                            >
                              {disp}
                            </span>
                          ) : isPct && v > 0
                            ? <span style={{
                                display:"inline-block",
                                background: accentColor+"12",
                                border: `1px solid ${accentColor}33`,
                                borderRadius:4, padding:"1px 6px",
                                color: accentColor, fontSize:11,
                              }}>{disp}</span>
                            : (isEmpty ? <span style={{color:"#2D3139"}}>—</span> : disp)
                          }
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
              {visibleRows.length===0&&(
                <tr>
                  <td colSpan={COLS.length} style={{ padding:"42px 24px" }}>
                    <div style={{ maxWidth:420, margin:"0 auto", textAlign:"center", padding:"22px 24px", borderRadius:18, background:`linear-gradient(180deg, ${THEME.surface}, ${THEME.panelBg})`, border:"1px solid #BFDBFE" }}>
                      <div style={{ fontSize:12, color:"#2563EB", fontFamily:"'DM Mono',monospace", letterSpacing:1, textTransform:"uppercase", marginBottom:10 }}>No matching rows</div>
                      <div style={{ fontSize:20, fontWeight:700, color:THEME.text, marginBottom:8 }}>No results for this combination</div>
                      <div style={{ fontSize:13, color:THEME.textMuted, lineHeight:1.6, marginBottom:16 }}>
                        Try another brand, month or search term to find results.
                      </div>
                      <button
                        onClick={() => { setBrand([]); setMonth([]); setSearch(""); setDebSearch(""); }}
                        style={{ background:"linear-gradient(135deg,#2563EB,#38BDF8)", border:"none", borderRadius:999, color:"#EFF6FF", padding:"10px 16px", fontSize:12, fontWeight:700, cursor:"pointer" }}
                      >
                        Reset filters
                      </button>
                    </div>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
          </div>
        </div>
        {/* Pagination */}
        {totalPages > 1 && (
          <div style={{ display:"flex", justifyContent:"center", alignItems:"center", gap:16, padding:"12px 16px 16px", background:THEME.surface }}>
            <button
              onClick={() => setTablePage(p => Math.max(1, p-1))}
              disabled={currentPage === 1}
              style={{ padding:"7px 20px", border:`1px solid ${THEME.border}`, borderRadius:8, background:currentPage===1?THEME.panelBg:THEME.surface, color:currentPage===1?"#CBD5E1":THEME.textSoft, fontSize:12, fontWeight:600, cursor:currentPage===1?"not-allowed":"pointer" }}
            >
              ← Prev
            </button>
            <span style={{ fontSize:12, color:THEME.textMuted, fontFamily:"'DM Mono',monospace" }}>Page {currentPage} of {totalPages}</span>
            <button
              onClick={() => setTablePage(p => Math.min(totalPages, p+1))}
              disabled={currentPage === totalPages}
              style={{ padding:"7px 20px", border:`1px solid ${THEME.border}`, borderRadius:8, background:currentPage===totalPages?THEME.panelBg:THEME.headerBg, color:currentPage===totalPages?"#CBD5E1":THEME.text, fontSize:12, fontWeight:600, cursor:currentPage===totalPages?"not-allowed":"pointer" }}
            >
              Next →
            </button>
          </div>
        )}
      </div>

    </div>
  );
}

