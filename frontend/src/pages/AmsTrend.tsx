import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, sortWeeks } from "@/lib/utils";
import { ExportButtons } from "@/components/ExportButtons";
import { useSortedRows } from "@/lib/useSortedRows";
import { makeTextFilter } from "@/lib/useTextFilter";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { ColumnFilter } from "@/components/ColumnFilter";
import { NumberRangeFilter } from "@/components/NumberRangeFilter";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Download, ArrowUp, ArrowDown, ArrowUpDown, Megaphone } from "lucide-react";

interface AmsKpis {
    gmv?: number; sessions?: number; units?: number;
    ad_spend?: number; attributed_sales?: number;
    acos?: number | null; tacos?: number | null;
}
interface AmsRow {
    week?: number | string;
    SKU?: string;  Model?: string;  brand?: string;  asin?: string;
    category_l0?: string;  category_l1?: string;  category_l2?: string;
    sessions?: number;  GMV?: number;  gmv?: number;  units?: number;
    ad_spend?: number;
    contribution_to_sales_pct?: number;
    attributed_sales_pct?: number;  organic_sales_pct?: number;
    conversion_pct?: number;
    roas?: number;  acos?: number;  tacos?: number; TACOS?: number;
    cac?: number;  ams_orders?: number;  attributed_sales?: number;
    clicks?: number;  impressions?: number;
    buy_box_pct?: number;
    inventory_ampm?: number;  inventory_1p?: number;
    inventory_amazon?: number;  inventory_total_amazon?: number;
    pipeline_orders?: number;
    campaign_name?: string;
    [k: string]: any;
}
interface AmsData {
    kpis: AmsKpis;
    rows: AmsRow[];
    all_weeks?: number[];
    default_weeks?: number[];
}

/* Match Jinja formatters exactly. */
const safe = (v: any) => (v == null || v === "" ? "—" : v);
const fmt  = (v: any) => (v == null || isNaN(v) ? "—" : Number(v).toFixed(2));
/** Integer formatter — order / unit / click / impression / session counts
 *  should never display a decimal.  Rounds to nearest int + adds locale separators. */
const int  = (v: any) => (v == null || v === "" || isNaN(v) ? "—" : Math.round(Number(v)).toLocaleString("en-IN"));
const pct  = (v: any) => (v == null || isNaN(v) ? "—" : (Number(v) * 100).toFixed(1) + "%");
const roas = (v: any) => (v == null || isNaN(v) ? "—" : Number(v).toFixed(2) + "x");

const COLUMNS = [
    "Week", "SKU", "Model", "Brand", "ASIN",
    "Category L0", "Category L1", "Category L2",
    "Sessions", "GMV (₹)", "Units", "Ad Spend (₹)",
    "Contrib Sales %", "Attributed Sales %", "Organic Sales %",
    "Conversion %", "ROAS", "ACOS %", "TACOS %",
    "CAC", "AMS Orders", "Attributed Sales (₹)",
    "Clicks", "Impressions", "CPC (₹)", "Buybox %",
    "Inv AMPM", "Inv 1P", "Inv Amazon", "Total Amz Inv", "Pipeline Orders",
];
// CPC is computed at render-time so it has no raw key; null disables sorting there.
const SORT_KEYS: (string | null)[] = [
    "week", "SKU", "Model", "brand", "asin",
    "category_l0", "category_l1", "category_l2",
    "sessions", "gmv", "units", "ad_spend",
    "contribution_to_sales_pct", "attributed_sales_pct", "organic_sales_pct",
    "conversion_pct", "roas", "acos", "tacos",
    "cac", "ams_orders", "attributed_sales",
    "clicks", "impressions", null, "buy_box_pct",
    "inventory_ampm", "inventory_1p", "inventory_amazon", "inventory_total_amazon", "pipeline_orders",
];
const TEXT_COLS_IDX = new Set([0, 1, 2, 3, 4, 5, 6, 7]);

// Parallel to COLUMNS — assigns each metric column a tint utility class so
// AmsTrend matches the visual rhythm of the other modules.  null = no tint
// (identifiers + categories).
const TINT_KEYS: (string | null)[] = [
    null, null, null, null, null,
    null, null, null,                                // categories
    "col-sessions", "col-sales", "col-units", "col-summary",
    "col-pct", "col-pct", "col-pct",
    "col-conv", "col-conv", "col-conv", "col-conv",
    "col-summary", "col-units", "col-sales",
    "col-units", "col-units", "col-summary", "col-pct",
    "col-inv", "col-inv", "col-inv", "col-inv",
    "col-units",
];
// Indices where a 2-px group divider should appear (left edge of a new group)
const DIVIDE_AT = new Set([8, 12, 15, 19, 22, 26, 30]);

function MiniKpi({ label, value, accent = "#1e40af" }: { label: string; value: string; accent?: string }) {
    return (
        <Card className="p-4">
            <div
                className="text-[10.5px] font-semibold uppercase mb-2"
                style={{ letterSpacing: "0.14em", color: accent }}
            >
                {label}
            </div>
            <div
                className="tabular"
                style={{
                    fontSize: 22,
                    fontWeight: 600,
                    letterSpacing: "-0.014em",
                    lineHeight: 1.1,
                    color: "#0a0a0a",
                }}
            >
                {value}
            </div>
        </Card>
    );
}

export default function AmsTrend() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const [filter, setFilter] = useDebouncedUrlParam("q");

    // Filter pickers persist in the URL so reloads / shared links keep state.
    // Memoized on qsKey so the arrays don't churn references each render.
    const selSkus   = useMemo(() => params.getAll("skus").filter(Boolean),   [qsKey]);
    const selModels = useMemo(() => params.getAll("models").filter(Boolean), [qsKey]);
    const selAsins  = useMemo(() => params.getAll("asins").filter(Boolean),  [qsKey]);
    const selBrands = useMemo(() => params.getAll("brands").filter(Boolean), [qsKey]);

    function setMulti(name: string, values: string[]) {
        const next = new URLSearchParams(params);
        next.delete(name);
        values.forEach((v) => next.append(name, v));
        setParams(next, { replace: false });
    }
    function getRange(key: string): { min: number | null; max: number | null } {
        const min = params.get(`${key}_min`);
        const max = params.get(`${key}_max`);
        return {
            min: min != null && min !== "" && Number.isFinite(Number(min)) ? Number(min) : null,
            max: max != null && max !== "" && Number.isFinite(Number(max)) ? Number(max) : null,
        };
    }
    function setRange(key: string, next: { min: number | null; max: number | null }) {
        const u = new URLSearchParams(params);
        if (next.min != null) u.set(`${key}_min`, String(next.min)); else u.delete(`${key}_min`);
        if (next.max != null) u.set(`${key}_max`, String(next.max)); else u.delete(`${key}_max`);
        setParams(u, { replace: false });
    }
    const setSelSkus   = (v: string[]) => setMulti("skus",   v);
    const setSelModels = (v: string[]) => setMulti("models", v);
    const setSelAsins  = (v: string[]) => setMulti("asins",  v);
    const setSelBrands = (v: string[]) => setMulti("brands", v);

    // Categorical filters that have no top-of-page picker — driven from column-header filters only.
    const selL0     = useMemo(() => params.getAll("cat_l0").filter(Boolean), [qsKey]);
    const selL1     = useMemo(() => params.getAll("cat_l1").filter(Boolean), [qsKey]);
    const selL2     = useMemo(() => params.getAll("cat_l2").filter(Boolean), [qsKey]);
    const selWeeks  = useMemo(() => params.getAll("weeks").filter(Boolean),  [qsKey]);

    // Numeric range filters per column
    const rSessions    = useMemo(() => getRange("sessions"),    [qsKey]);
    const rGmv         = useMemo(() => getRange("gmv"),         [qsKey]);
    const rUnits       = useMemo(() => getRange("units"),       [qsKey]);
    const rAdSpend     = useMemo(() => getRange("spend"),       [qsKey]);
    const rContrib     = useMemo(() => getRange("contrib"),     [qsKey]);
    const rAttrPct     = useMemo(() => getRange("attr_pct"),    [qsKey]);
    const rOrgPct      = useMemo(() => getRange("org_pct"),     [qsKey]);
    const rConvPct     = useMemo(() => getRange("conv_pct"),    [qsKey]);
    const rRoas        = useMemo(() => getRange("roas"),        [qsKey]);
    const rAcos        = useMemo(() => getRange("acos"),        [qsKey]);
    const rTacos       = useMemo(() => getRange("tacos"),       [qsKey]);
    const rCac         = useMemo(() => getRange("cac"),         [qsKey]);
    const rOrders      = useMemo(() => getRange("orders"),      [qsKey]);
    const rAttrSales   = useMemo(() => getRange("attr_sales"),  [qsKey]);
    const rClicks      = useMemo(() => getRange("clicks"),      [qsKey]);
    const rImpr        = useMemo(() => getRange("impr"),        [qsKey]);
    const rBb          = useMemo(() => getRange("bb"),          [qsKey]);
    const rInvAmpm     = useMemo(() => getRange("inv_ampm"),    [qsKey]);
    const rInv1p       = useMemo(() => getRange("inv_1p"),      [qsKey]);
    const rInvAmz      = useMemo(() => getRange("inv_amz"),     [qsKey]);
    const rInvTotAmz   = useMemo(() => getRange("inv_tot_amz"), [qsKey]);
    const rPipeOrders  = useMemo(() => getRange("pipe_orders"), [qsKey]);

    // Pass week selection to the API so the operator can pull weeks outside
    // the default last-4 window.  Backend returns last 4 weeks when no
    // sel_weeks is provided, full list of weeks is in data.all_weeks for
    // the picker.
    const { data, isLoading, error } = useQuery<AmsData>({
        queryKey: ["ams-trend", selWeeks.join(",")],
        queryFn: () => {
            const qs = selWeeks.map((w) => `sel_weeks=${encodeURIComponent(w)}`).join("&");
            return api.get(`/api/ams/trend${qs ? `?${qs}` : ""}`);
        },
    });

    // Build unique lists for each picker from the loaded rows.
    // Weeks come from data.all_weeks (full set) so the picker can offer
    // weeks outside the default last-4 window the API returns.
    const { allSkus, allModels, allAsins, allBrands, allWeeks, allL0, allL1, allL2 } = useMemo(() => {
        const skus = new Set<string>(), models = new Set<string>(), asins = new Set<string>(), brands = new Set<string>();
        const l0 = new Set<string>(), l1 = new Set<string>(), l2 = new Set<string>();
        for (const r of data?.rows || []) {
            if (r.SKU)   skus.add(r.SKU);
            if (r.Model) models.add(r.Model);
            if (r.asin)  asins.add(r.asin);
            if (r.brand) brands.add(r.brand);
            if (r.category_l0) l0.add(r.category_l0);
            if (r.category_l1) l1.add(r.category_l1);
            if (r.category_l2) l2.add(r.category_l2);
        }
        return {
            allSkus:   Array.from(skus).sort(),
            allModels: Array.from(models).sort(),
            allAsins:  Array.from(asins).sort(),
            allBrands: Array.from(brands).sort(),
            allWeeks:  sortWeeks((data?.all_weeks || []).map(String)),
            allL0:     Array.from(l0).sort(),
            allL1:     Array.from(l1).sort(),
            allL2:     Array.from(l2).sort(),
        };
    }, [data]);

    const filtered = useMemo(() => {
        if (!data) return [];
        const matchText = makeTextFilter<any>(filter, ["SKU", "Model", "asin", "brand"]);
        const skuSet   = new Set(selSkus);
        const modelSet = new Set(selModels);
        const asinSet  = new Set(selAsins);
        const brandSet = new Set(selBrands);
        const wkSet    = new Set(selWeeks);
        const l0Set    = new Set(selL0);
        const l1Set    = new Set(selL1);
        const l2Set    = new Set(selL2);
        function inRange(v: any, rng: { min: number | null; max: number | null }, scale = 1): boolean {
            if (rng.min == null && rng.max == null) return true;
            if (v == null || v === "" || isNaN(Number(v))) return false;
            const num = Number(v) * scale;
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        return data.rows.filter((r) => {
            if (skuSet.size   && !skuSet.has(r.SKU   || "")) return false;
            if (modelSet.size && !modelSet.has(r.Model || "")) return false;
            if (asinSet.size  && !asinSet.has(r.asin  || "")) return false;
            if (brandSet.size && !brandSet.has(r.brand || "")) return false;
            if (wkSet.size    && !wkSet.has(String(r.week ?? ""))) return false;
            if (l0Set.size    && !l0Set.has(r.category_l0 || "")) return false;
            if (l1Set.size    && !l1Set.has(r.category_l1 || "")) return false;
            if (l2Set.size    && !l2Set.has(r.category_l2 || "")) return false;
            // Numeric — percentages stored as 0..1 in the API; UI shows ×100, so scale 100 in comparison.
            if (!inRange(r.sessions,    rSessions))    return false;
            if (!inRange((r as any).GMV ?? (r as any).gmv, rGmv))         return false;
            if (!inRange(r.units,       rUnits))       return false;
            if (!inRange(r.ad_spend,    rAdSpend))     return false;
            if (!inRange(r.contribution_to_sales_pct, rContrib,  100)) return false;
            if (!inRange(r.attributed_sales_pct,      rAttrPct,  100)) return false;
            if (!inRange(r.organic_sales_pct,         rOrgPct,   100)) return false;
            if (!inRange(r.conversion_pct,            rConvPct,  100)) return false;
            if (!inRange(r.roas,        rRoas))        return false;
            if (!inRange(r.acos,        rAcos,  100))  return false;
            if (!inRange(r.tacos ?? (r as any).TACOS, rTacos, 100)) return false;
            if (!inRange(r.cac,         rCac))         return false;
            if (!inRange(r.ams_orders,  rOrders))      return false;
            if (!inRange(r.attributed_sales, rAttrSales)) return false;
            if (!inRange(r.clicks,      rClicks))      return false;
            if (!inRange(r.impressions, rImpr))        return false;
            if (!inRange(r.buy_box_pct, rBb,    100))  return false;
            if (!inRange(r.inventory_ampm,         rInvAmpm))    return false;
            if (!inRange(r.inventory_1p,           rInv1p))      return false;
            if (!inRange(r.inventory_amazon,       rInvAmz))     return false;
            if (!inRange(r.inventory_total_amazon, rInvTotAmz))  return false;
            if (!inRange(r.pipeline_orders,        rPipeOrders)) return false;
            return matchText(r);
        });
    }, [data, filter, selSkus, selModels, selAsins, selBrands, selWeeks, selL0, selL1, selL2,
        rSessions, rGmv, rUnits, rAdSpend, rContrib, rAttrPct, rOrgPct, rConvPct,
        rRoas, rAcos, rTacos, rCac, rOrders, rAttrSales, rClicks, rImpr, rBb,
        rInvAmpm, rInv1p, rInvAmz, rInvTotAmz, rPipeOrders]);

    const { sorted, sort, onSort } = useSortedRows<AmsRow>(filtered, { key: "ad_spend", dir: "desc" });

    // KPIs derived from the *client-filtered* rows so picking Brands /
    // Models / SKUs / Categories collapses the headline numbers in step
    // with the table.  data.kpis was server-computed for the loaded
    // dataset only (week-filtered), so it ignored every other picker.
    // Exclude the "Grand Total" row the backend appends to data.rows.
    const filteredKpis = useMemo(() => {
        const real = filtered.filter(
            (r) => String(r.Model ?? "").toLowerCase() !== "grand total"
        );
        const sum = (k: keyof AmsRow) =>
            real.reduce((acc, r) => acc + (Number(r[k] as any) || 0), 0);
        const gmv      = sum("gmv");
        const units    = sum("units");
        const sessions = sum("sessions");
        const adSpend  = sum("ad_spend");
        const attrSales= sum("attributed_sales");
        return {
            gmv, units, sessions, ad_spend: adSpend, attributed_sales: attrSales,
            acos:  attrSales > 0 ? adSpend / attrSales : null,
            tacos: gmv > 0       ? adSpend / gmv      : null,
        };
    }, [filtered]);

    function cellsFor(r: AmsRow): string[] {
        const gmv  = r.GMV ?? r.gmv;
        const tacosVal = r.tacos ?? r.TACOS;
        const cpc  = (r.clicks ?? 0) > 0 && r.ad_spend != null ? r.ad_spend / r.clicks! : null;
        return [
            safe(r.week),
            safe(r.SKU || "—"),
            safe(r.Model || r.campaign_name || "SB Campaign"),
            safe(r.brand),
            safe(r.asin),
            safe(r.category_l0),
            safe(r.category_l1),
            safe(r.category_l2),
            int(r.sessions),
            // Currency uses Jinja's fmt() (decimal) — but for a SaaS feel, fmtINR
            // gives "₹ 1.23 L" / "Cr". Replicate the Jinja "to 2dp" exactly so totals
            // reconcile against the legacy view.
            fmt(gmv),
            int(r.units),
            fmt(r.ad_spend),
            pct(r.contribution_to_sales_pct),
            pct(r.attributed_sales_pct),
            pct(r.organic_sales_pct),
            pct(r.conversion_pct),
            roas(r.roas),
            pct(r.acos),
            pct(tacosVal),
            fmt(r.cac),
            int(r.ams_orders),
            fmt(r.attributed_sales),
            int(r.clicks),
            int(r.impressions),
            fmt(cpc),
            pct(r.buy_box_pct),
            int(r.inventory_ampm),
            int(r.inventory_1p),
            int(r.inventory_amazon),
            int(r.inventory_total_amazon),
            int(r.pipeline_orders),
        ].map(String);
    }

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Advertising · Amazon Ads</div>
                    <h1 className="text-2xl font-semibold tracking-tight">AMS Trend</h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        ASIN-level ad spend, attribution, ACOS, ROAS, conversion — all AMS metrics in one view.
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Weeks"  options={allWeeks}  selected={selWeeks}  onApply={(v) => setMulti("weeks", v)} />
                <MultiPicker label="Brands" options={allBrands} selected={selBrands} onApply={setSelBrands} />
                <MultiPicker label="Models" options={allModels} selected={selModels} onApply={setSelModels} />
                <MultiPicker label="SKUs"   options={allSkus}   selected={selSkus}   onApply={setSelSkus} />
                <MultiPicker label="ASINs"  options={allAsins}  selected={selAsins}  onApply={setSelAsins} />
            </div>

            {isLoading && <LoadingSkeleton rows={8} />}
            {error && <ErrorBlock error={error} />}

            {data && (
                <>
                    <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-3">
                        <MiniKpi label="GMV"              value={fmtINR(filteredKpis.gmv)}                                                              accent="#059669" />
                        <MiniKpi label="Units"            value={fmtInt(filteredKpis.units)}                                                            accent="#2563eb" />
                        <MiniKpi label="Sessions"         value={fmtInt(filteredKpis.sessions)}                                                         accent="#0891b2" />
                        <MiniKpi label="Ad Spend"         value={fmtINR(filteredKpis.ad_spend)}                                                         accent="#d97706" />
                        <MiniKpi label="Attributed Sales" value={fmtINR(filteredKpis.attributed_sales)}                                                 accent="#7c3aed" />
                        <MiniKpi label="ACOS"             value={filteredKpis.acos != null ? (filteredKpis.acos * 100).toFixed(1) + "%" : "—"}           accent="#be185d" />
                    </div>

                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={Megaphone}
                            iconColor="#7c3aed"
                            title="AMS Trend"
                            subtitle={`Full table · ${data.rows.length.toLocaleString()} rows`}
                            action={
                                <>
                                    <Input
                                        placeholder="Filter SKU / Model / ASIN / Brand…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    <ExportButtons
                                        rows={sorted as any}
                                        columns={SORT_KEYS.filter((k): k is string => k != null)}
                                        filename="ams-trend.xlsx"
                                    />
                                </>
                            }
                        />
                        <div className="overflow-auto max-h-[72vh]">
                            <table className="w-full text-[13px] border-separate border-spacing-0">
                                {/* Freeze first 4 identifier columns (Week, SKU, Model, Brand) */}
                                <thead className="sticky top-0 z-30">
                                    <tr>
                                        {COLUMNS.map((c, idx) => {
                                            const frozen = idx < 4;
                                            const left = [0, 70, 170, 280][idx] ?? 0;
                                            const style = frozen ? { position: "sticky" as const, left } : undefined;
                                            const lastFrozen = idx === 3;
                                            const isText = TEXT_COLS_IDX.has(idx);
                                            const sortKey = SORT_KEYS[idx];
                                            const sortable = sortKey != null;
                                            const active = sortable && sort?.key === sortKey;
                                            const dir = active ? sort!.dir : null;
                                            const tint = TINT_KEYS[idx];
                                            const divide = DIVIDE_AT.has(idx);
                                            // Per-column filter config — indexed by column position.
                                            type CatF = { values: string[]; selected: string[]; onChange: (v: string[]) => void };
                                            type NumF = { value: { min: number | null; max: number | null }; onChange: (v: { min: number | null; max: number | null }) => void; suffix?: string; presets?: number[] };
                                            const cats: Record<number, CatF> = {
                                                0:  { values: allWeeks,  selected: selWeeks,  onChange: (v) => setMulti("weeks", v) },
                                                1:  { values: allSkus,   selected: selSkus,   onChange: setSelSkus },
                                                2:  { values: allModels, selected: selModels, onChange: setSelModels },
                                                3:  { values: allBrands, selected: selBrands, onChange: setSelBrands },
                                                4:  { values: allAsins,  selected: selAsins,  onChange: setSelAsins },
                                                5:  { values: allL0,     selected: selL0,     onChange: (v) => setMulti("cat_l0", v) },
                                                6:  { values: allL1,     selected: selL1,     onChange: (v) => setMulti("cat_l1", v) },
                                                7:  { values: allL2,     selected: selL2,     onChange: (v) => setMulti("cat_l2", v) },
                                            };
                                            const nums: Record<number, NumF> = {
                                                8:  { value: rSessions,   onChange: (r) => setRange("sessions",    r), presets: [10, 100, 1000] },
                                                9:  { value: rGmv,        onChange: (r) => setRange("gmv",         r), presets: [1000, 10000, 100000] },
                                                10: { value: rUnits,      onChange: (r) => setRange("units",       r), presets: [1, 10, 50] },
                                                11: { value: rAdSpend,    onChange: (r) => setRange("spend",       r), presets: [100, 1000, 10000] },
                                                12: { value: rContrib,    onChange: (r) => setRange("contrib",     r), suffix: "%", presets: [1, 5, 10] },
                                                13: { value: rAttrPct,    onChange: (r) => setRange("attr_pct",    r), suffix: "%", presets: [10, 30, 50] },
                                                14: { value: rOrgPct,     onChange: (r) => setRange("org_pct",     r), suffix: "%", presets: [10, 30, 50] },
                                                15: { value: rConvPct,    onChange: (r) => setRange("conv_pct",    r), suffix: "%", presets: [1, 5, 10] },
                                                16: { value: rRoas,       onChange: (r) => setRange("roas",        r), presets: [1, 2, 5] },
                                                17: { value: rAcos,       onChange: (r) => setRange("acos",        r), suffix: "%", presets: [10, 30, 50] },
                                                18: { value: rTacos,      onChange: (r) => setRange("tacos",       r), suffix: "%", presets: [10, 30, 50] },
                                                19: { value: rCac,        onChange: (r) => setRange("cac",         r), presets: [10, 100, 500] },
                                                20: { value: rOrders,     onChange: (r) => setRange("orders",      r), presets: [1, 5, 10] },
                                                21: { value: rAttrSales,  onChange: (r) => setRange("attr_sales",  r), presets: [100, 1000, 10000] },
                                                22: { value: rClicks,     onChange: (r) => setRange("clicks",      r), presets: [10, 100, 1000] },
                                                23: { value: rImpr,       onChange: (r) => setRange("impr",        r), presets: [100, 1000, 10000] },
                                                25: { value: rBb,         onChange: (r) => setRange("bb",          r), suffix: "%", presets: [80, 90, 95] },
                                                26: { value: rInvAmpm,    onChange: (r) => setRange("inv_ampm",    r), presets: [1, 50, 500] },
                                                27: { value: rInv1p,      onChange: (r) => setRange("inv_1p",      r), presets: [1, 50, 500] },
                                                28: { value: rInvAmz,     onChange: (r) => setRange("inv_amz",     r), presets: [1, 50, 500] },
                                                29: { value: rInvTotAmz,  onChange: (r) => setRange("inv_tot_amz", r), presets: [1, 50, 500] },
                                                30: { value: rPipeOrders, onChange: (r) => setRange("pipe_orders", r), presets: [1, 5, 10] },
                                            };
                                            const cat = cats[idx];
                                            const num = nums[idx];
                                            const filterActive = (cat && cat.selected.length > 0) || (num && (num.value.min != null || num.value.max != null));
                                            return (
                                                <th
                                                    key={c}
                                                    style={style}
                                                    onClick={sortable ? () => onSort(sortKey!) : undefined}
                                                    className={
                                                        "px-3 py-2 text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b whitespace-nowrap bg-secondary " +
                                                        (isText ? "text-left" : "text-right") +
                                                        (frozen ? " z-40" : "") +
                                                        (lastFrozen ? " border-r-2 border-r-border" : "") +
                                                        (tint ? ` ${tint}` : "") +
                                                        (divide ? " col-divide-l" : "") +
                                                        (sortable ? " cursor-pointer select-none" : "") +
                                                        (filterActive ? " bg-primary/5" : "")
                                                    }
                                                >
                                                    <span className={"inline-flex items-center gap-1 " + (isText ? "" : "justify-end w-full")}>
                                                        {c}
                                                        {sortable && dir === "desc" && <ArrowDown className="h-3 w-3" />}
                                                        {sortable && dir === "asc"  && <ArrowUp   className="h-3 w-3" />}
                                                        {sortable && !dir            && <ArrowUpDown className="h-3 w-3 opacity-30" />}
                                                        {cat && (
                                                            <ColumnFilter
                                                                values={cat.values}
                                                                selected={cat.selected}
                                                                onChange={cat.onChange}
                                                            />
                                                        )}
                                                        {num && (
                                                            <NumberRangeFilter
                                                                value={num.value}
                                                                onChange={num.onChange}
                                                                suffix={num.suffix}
                                                                presets={num.presets}
                                                            />
                                                        )}
                                                    </span>
                                                </th>
                                            );
                                        })}
                                    </tr>
                                </thead>
                                <tbody>
                                    {sorted.map((r, i) => {
                                        const cells = cellsFor(r);
                                        const isTotal = String(r.Model ?? "").toLowerCase().includes("grand total");
                                        const rowBg = isTotal ? "bg-secondary/50" : "bg-background group-hover:bg-accent/40";
                                        return (
                                            <tr key={i} className={"group " + (isTotal ? "font-semibold" : "")}>
                                                {cells.map((val, idx) => {
                                                    const frozen = idx < 4;
                                                    const left = [0, 70, 170, 280][idx] ?? 0;
                                                    const style = frozen ? { position: "sticky" as const, left } : undefined;
                                                    const lastFrozen = idx === 3;
                                                    const tint = TINT_KEYS[idx];
                                                    const divide = DIVIDE_AT.has(idx);
                                                    return (
                                                        <td
                                                            key={idx}
                                                            style={style}
                                                            className={
                                                                "px-3 py-2 whitespace-nowrap border-b " + rowBg + " " +
                                                                (TEXT_COLS_IDX.has(idx) ? "text-left" : "text-right tabular") +
                                                                (frozen ? " z-10" : "") +
                                                                (lastFrozen ? " border-r-2 border-r-border" : "") +
                                                                (tint ? ` ${tint}` : "") +
                                                                (divide ? " col-divide-l" : "") +
                                                                (idx === 2 ? " font-medium" : "")
                                                            }
                                                        >
                                                            {val}
                                                        </td>
                                                    );
                                                })}
                                            </tr>
                                        );
                                    })}
                                </tbody>
                            </table>
                        </div>
                    </Card>
                </>
            )}
        </AppLayout>
    );
}
