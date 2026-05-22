import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, exportToCsv } from "@/lib/utils";
import { useSortedRows } from "@/lib/useSortedRows";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { SortableTh } from "@/components/SortableTh";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Download, AlertTriangle, Sliders } from "lucide-react";

interface AmsRow {
    week?: number | string;
    SKU?: string; Model?: string; brand?: string; asin?: string;
    category_l0?: string; category_l1?: string; category_l2?: string;
    sessions?: number; GMV?: number; gmv?: number; units?: number;
    ad_spend?: number;
    attributed_sales_pct?: number;
    conversion_pct?: number;
    roas?: number; acos?: number; tacos?: number; TACOS?: number;
    ams_orders?: number; attributed_sales?: number;
    clicks?: number; impressions?: number;
    [k: string]: any;
}
interface AmsData { kpis: any; rows: AmsRow[]; }

// Thresholds for "poor performer".  Tunable from the UI.
const DEFAULT_ACOS_MIN  = 0.50;   // ACOS > 50% = bleeding
const DEFAULT_ROAS_MAX  = 1.50;   // ROAS < 1.5x = under-water
const DEFAULT_SPEND_MIN = 500;    // ignore noise: only flag when ad spend >= ₹500

function numParam(p: URLSearchParams, name: string, fallback: number): number {
    const raw = p.get(name);
    if (raw == null) return fallback;
    const n = parseFloat(raw);
    return isFinite(n) ? n : fallback;
}

export default function AmsPoorPerformers() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const [filter, setFilter] = useDebouncedUrlParam("q");

    // URL-backed state — reload / share preserves operator config.
    const acosMin   = numParam(params, "acos_min",  DEFAULT_ACOS_MIN);
    const roasMax   = numParam(params, "roas_max",  DEFAULT_ROAS_MAX);
    const spendMin  = numParam(params, "spend_min", DEFAULT_SPEND_MIN);
    const selBrands = useMemo(() => params.getAll("brands").filter(Boolean), [qsKey]);
    const selAsins  = useMemo(() => params.getAll("asins").filter(Boolean),  [qsKey]);

    function setNum(name: string, value: number, def: number) {
        const next = new URLSearchParams(params);
        if (value === def) next.delete(name);
        else               next.set(name, String(value));
        setParams(next, { replace: true });   // replace so threshold-drags don't pollute history
    }
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
    const setAcosMin   = (v: number)   => setNum("acos_min",  v, DEFAULT_ACOS_MIN);
    const setRoasMax   = (v: number)   => setNum("roas_max",  v, DEFAULT_ROAS_MAX);
    const setSpendMin  = (v: number)   => setNum("spend_min", v, DEFAULT_SPEND_MIN);
    const setSelBrands = (v: string[]) => setMulti("brands",  v);
    const setSelAsins  = (v: string[]) => setMulti("asins",   v);

    // Column-header filter state
    const selWeeks   = useMemo(() => params.getAll("weeks").filter(Boolean),  [qsKey]);
    const selSkus    = useMemo(() => params.getAll("skus").filter(Boolean),   [qsKey]);
    const selModels  = useMemo(() => params.getAll("models").filter(Boolean), [qsKey]);
    const rSpend     = useMemo(() => getRange("spend"),  [qsKey]);
    const rAttr      = useMemo(() => getRange("attr"),   [qsKey]);
    const rOrders    = useMemo(() => getRange("orders"), [qsKey]);
    const rAcos      = useMemo(() => getRange("acos"),   [qsKey]);
    const rRoas      = useMemo(() => getRange("roas"),   [qsKey]);
    const rTacos     = useMemo(() => getRange("tacos"),  [qsKey]);
    const rConv      = useMemo(() => getRange("conv"),   [qsKey]);
    const rClicks    = useMemo(() => getRange("clicks"), [qsKey]);
    const rImpr      = useMemo(() => getRange("impr"),   [qsKey]);
    const rSessions  = useMemo(() => getRange("sess"),   [qsKey]);

    // Shares queryKey with /ams-trend so navigating between them reuses cache.
    const { data, isLoading, error } = useQuery<AmsData>({
        queryKey: ["ams-trend"],
        queryFn: () => api.get("/api/ams/trend"),
    });

    const { allBrands, allAsins, allWeeksU, allSkusU, allModelsU } = useMemo(() => {
        const brands = new Set<string>(), asins = new Set<string>();
        const weeks = new Set<string>(), skus = new Set<string>(), models = new Set<string>();
        for (const r of data?.rows || []) {
            if (r.brand) brands.add(r.brand);
            if (r.asin)  asins.add(r.asin);
            if (r.week)  weeks.add(String(r.week));
            if (r.SKU)   skus.add(r.SKU);
            if (r.Model) models.add(r.Model);
        }
        return {
            allBrands: Array.from(brands).sort(),
            allAsins:  Array.from(asins).sort(),
            allWeeksU: Array.from(weeks).sort(),
            allSkusU:  Array.from(skus).sort(),
            allModelsU: Array.from(models).sort(),
        };
    }, [data]);

    // Core poor-performer filter: row counts only when ad spend exceeds the
    // floor (skips zero-spend noise) AND either ACOS or ROAS is in the red.
    const poorRows = useMemo(() => {
        if (!data) return [];
        const brandSet = new Set(selBrands);
        const asinSet  = new Set(selAsins);
        const wkSet    = new Set(selWeeks);
        const skuSet   = new Set(selSkus);
        const modelSet = new Set(selModels);
        function inRange(v: any, rng: { min: number | null; max: number | null }, scale = 1): boolean {
            if (rng.min == null && rng.max == null) return true;
            if (v == null || v === "" || isNaN(Number(v))) return false;
            const num = Number(v) * scale;
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        return data.rows.filter((r) => {
            const spend = Number(r.ad_spend ?? 0);
            if (!isFinite(spend) || spend < spendMin) return false;

            const acos = r.acos != null ? Number(r.acos) : null;
            const roas = r.roas != null ? Number(r.roas) : null;
            const acosBad = acos != null && acos > acosMin;
            const roasBad = roas != null && roas < roasMax;
            if (!acosBad && !roasBad) return false;

            if (brandSet.size && !brandSet.has(r.brand || "")) return false;
            if (asinSet.size  && !asinSet.has(r.asin   || "")) return false;
            if (wkSet.size    && !wkSet.has(String(r.week ?? ""))) return false;
            if (skuSet.size   && !skuSet.has(r.SKU || ""))     return false;
            if (modelSet.size && !modelSet.has(r.Model || "")) return false;
            if (!inRange(r.ad_spend,          rSpend))     return false;
            if (!inRange(r.attributed_sales,  rAttr))      return false;
            if (!inRange(r.ams_orders,        rOrders))    return false;
            if (!inRange(r.acos,              rAcos, 100)) return false;
            if (!inRange(r.roas,              rRoas))      return false;
            if (!inRange(r.tacos ?? (r as any).TACOS, rTacos, 100)) return false;
            if (!inRange(r.conversion_pct,    rConv, 100)) return false;
            if (!inRange(r.clicks,            rClicks))    return false;
            if (!inRange(r.impressions,       rImpr))      return false;
            if (!inRange(r.sessions,          rSessions))  return false;
            return true;
        });
    }, [data, acosMin, roasMax, spendMin, selBrands, selAsins, selWeeks, selSkus, selModels,
        rSpend, rAttr, rOrders, rAcos, rRoas, rTacos, rConv, rClicks, rImpr, rSessions]);

    const searchFiltered = useMemo(() => {
        if (!filter.trim()) return poorRows;
        const q = filter.toLowerCase();
        return poorRows.filter((r) =>
            ((r.SKU || "") + " " + (r.Model || "") + " " + (r.asin || "") + " " + (r.brand || ""))
                .toLowerCase()
                .includes(q),
        );
    }, [poorRows, filter]);

    const { sorted, sort, onSort } = useSortedRows<AmsRow>(searchFiltered, { key: "ad_spend", dir: "desc" });

    // Totals across the flagged rows — what is this leak costing?
    const totals = useMemo(() => {
        let spend = 0, attrib = 0;
        for (const r of poorRows) {
            spend  += Number(r.ad_spend ?? 0);
            attrib += Number(r.attributed_sales ?? 0);
        }
        const blendedAcos = attrib > 0 ? spend / attrib : null;
        return { spend, attrib, blendedAcos, count: poorRows.length };
    }, [poorRows]);

    const safe = (v: any) => (v == null || v === "" ? "—" : v);
    const int  = (v: any) => (v == null || v === "" || isNaN(v) ? "—" : Math.round(Number(v)).toLocaleString("en-IN"));
    const pct  = (v: any) => (v == null || isNaN(v) ? "—" : (Number(v) * 100).toFixed(1) + "%");
    const roasFmt = (v: any) => (v == null || isNaN(v) ? "—" : Number(v).toFixed(2) + "x");

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2" style={{ color: "#b91c1c" }}>Advertising · Optimization Targets</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <AlertTriangle className="h-5 w-5" style={{ color: "#b91c1c" }} />
                        Ad Underperformers
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        ASINs with ACOS &gt; <strong style={{ color: "#0a0a0a" }}>{(acosMin * 100).toFixed(0)}%</strong>
                        {" "}or ROAS &lt; <strong style={{ color: "#0a0a0a" }}>{roasMax.toFixed(1)}x</strong>
                        {" "}and ad spend ≥ <strong style={{ color: "#0a0a0a" }}>{fmtINR(spendMin)}</strong>.
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Brands" options={allBrands} selected={selBrands} onApply={setSelBrands} />
                <MultiPicker label="ASINs"  options={allAsins}  selected={selAsins}  onApply={setSelAsins} />
            </div>

            {/* Threshold knobs */}
            <Card className="overflow-hidden">
                <SectionHeader icon={Sliders} iconColor="#6b7280" title="Thresholds" subtitle="Tune to widen or narrow the flag" />
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 p-4">
                    <label className="flex flex-col gap-1 text-[13px]">
                        <span className="font-medium">ACOS &gt; (fraction)</span>
                        <Input type="number" step="0.05" min="0" max="5"
                            value={acosMin} onChange={(e) => setAcosMin(parseFloat(e.target.value) || 0)} />
                        <span style={{ color: "#6b7280" }}>= {(acosMin * 100).toFixed(0)}%</span>
                    </label>
                    <label className="flex flex-col gap-1 text-[13px]">
                        <span className="font-medium">ROAS &lt; (x)</span>
                        <Input type="number" step="0.1" min="0" max="20"
                            value={roasMax} onChange={(e) => setRoasMax(parseFloat(e.target.value) || 0)} />
                        <span style={{ color: "#6b7280" }}>= {roasMax.toFixed(2)}x</span>
                    </label>
                    <label className="flex flex-col gap-1 text-[13px]">
                        <span className="font-medium">Ad spend ≥ (₹)</span>
                        <Input type="number" step="100" min="0"
                            value={spendMin} onChange={(e) => setSpendMin(parseFloat(e.target.value) || 0)} />
                        <span style={{ color: "#6b7280" }}>min spend to flag</span>
                    </label>
                </div>
            </Card>

            {isLoading && <LoadingSkeleton rows={8} />}
            {error && <ErrorBlock error={error} />}

            {data && (
                <>
                    {/* Summary KPI strip — what is the leak costing? */}
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                        {[
                            { label: "Flagged ASINs",      value: fmtInt(totals.count),                                                       accent: "#b91c1c", color: "#0a0a0a" },
                            { label: "Ad Spend (Flagged)", value: fmtINR(totals.spend),                                                       accent: "#b91c1c", color: "#b91c1c" },
                            { label: "Attributed Sales",   value: fmtINR(totals.attrib),                                                      accent: "#059669", color: "#0a0a0a" },
                            { label: "Blended ACOS",       value: totals.blendedAcos != null ? (totals.blendedAcos * 100).toFixed(1) + "%" : "—", accent: "#b91c1c", color: "#b91c1c" },
                        ].map((k) => (
                            <Card key={k.label} className="p-4">
                                <div
                                    className="text-[10.5px] font-semibold uppercase mb-2"
                                    style={{ letterSpacing: "0.14em", color: k.accent }}
                                >
                                    {k.label}
                                </div>
                                <div
                                    className="tabular"
                                    style={{
                                        fontSize: 22,
                                        fontWeight: 600,
                                        letterSpacing: "-0.014em",
                                        lineHeight: 1.1,
                                        color: k.color,
                                    }}
                                >
                                    {k.value}
                                </div>
                            </Card>
                        ))}
                    </div>

                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={AlertTriangle}
                            iconColor="#b91c1c"
                            title="Ad Underperformers"
                            subtitle={
                                searchFiltered.length !== poorRows.length
                                    ? `${poorRows.length.toLocaleString()} flagged · ${searchFiltered.length} shown`
                                    : `${poorRows.length.toLocaleString()} flagged`
                            }
                            action={
                                <>
                                    <Input
                                        placeholder="Filter SKU / Model / ASIN / Brand…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => {
                                            const cols = ["week", "SKU", "Model", "brand", "asin",
                                                "category_l0", "category_l1", "category_l2",
                                                "ad_spend", "attributed_sales", "ams_orders",
                                                "acos", "roas", "tacos", "conversion_pct",
                                                "clicks", "impressions", "sessions"];
                                            exportToCsv(sorted as any, cols, "ams-poor-performers.csv");
                                        }}
                                    >
                                        <Download className="h-3.5 w-3.5" />
                                        Export CSV
                                    </Button>
                                </>
                            }
                        />

                        {sorted.length === 0 ? (
                            <div className="p-10 text-center text-sm">
                                {poorRows.length === 0
                                    ? "Nothing flagged at the current thresholds — adjust ACOS / ROAS / Spend above to widen the net."
                                    : "No rows match the current text filter."}
                            </div>
                        ) : (
                            <div className="overflow-auto max-h-[72vh]">
                                <table className="w-full text-[14px] border-separate border-spacing-0">
                                    <thead className="sticky top-0 z-30">
                                        <tr>
                                            <SortableTh sortKey="week"      label="Week"    sort={sort} onSort={onSort} align="left"  stickyLeft={0}   minWidth={80}
                                                filterValues={allWeeksU}  filterSelected={selWeeks}  onFilterChange={(v) => setMulti("weeks", v)} />
                                            <SortableTh sortKey="SKU"       label="SKU"     sort={sort} onSort={onSort} align="left"  stickyLeft={80}  minWidth={120}
                                                filterValues={allSkusU}   filterSelected={selSkus}   onFilterChange={(v) => setMulti("skus", v)} />
                                            <SortableTh sortKey="Model"     label="Model"   sort={sort} onSort={onSort} align="left"  stickyLeft={200} minWidth={140}
                                                filterValues={allModelsU} filterSelected={selModels} onFilterChange={(v) => setMulti("models", v)} />
                                            <SortableTh sortKey="asin"      label="ASIN"    sort={sort} onSort={onSort} align="left"  stickyLeft={340} minWidth={120} lastFrozen
                                                filterValues={allAsins}   filterSelected={selAsins}  onFilterChange={(v) => setMulti("asins", v)} />
                                            <SortableTh sortKey="brand"     label="Brand"   sort={sort} onSort={onSort} align="left" minWidth={120}
                                                filterValues={allBrands}  filterSelected={selBrands} onFilterChange={(v) => setMulti("brands", v)} />
                                            <SortableTh sortKey="ad_spend"  label="Ad Spend" sort={sort} onSort={onSort} className="col-sales col-divide-l" minWidth={120}
                                                numericRange={rSpend}   onNumericFilter={(r) => setRange("spend", r)}  numericPresets={[100, 1000, 10000]} />
                                            <SortableTh sortKey="attributed_sales" label="Attributed Sales" sort={sort} onSort={onSort} className="col-sales" minWidth={150}
                                                numericRange={rAttr}    onNumericFilter={(r) => setRange("attr", r)}   numericPresets={[100, 1000, 10000]} />
                                            <SortableTh sortKey="ams_orders"       label="AMS Orders" sort={sort} onSort={onSort} className="col-units col-divide-l" minWidth={120}
                                                numericRange={rOrders}  onNumericFilter={(r) => setRange("orders", r)} numericPresets={[1, 5, 10]} />
                                            <SortableTh sortKey="acos"      label="ACOS"    sort={sort} onSort={onSort} className="col-conv col-divide-l" minWidth={110}
                                                numericRange={rAcos}    onNumericFilter={(r) => setRange("acos", r)}   numericSuffix="%" numericPresets={[10, 30, 50]} />
                                            <SortableTh sortKey="roas"      label="ROAS"    sort={sort} onSort={onSort} className="col-conv" minWidth={100}
                                                numericRange={rRoas}    onNumericFilter={(r) => setRange("roas", r)}   numericPresets={[1, 2, 5]} />
                                            <SortableTh sortKey="tacos"     label="TACOS"   sort={sort} onSort={onSort} className="col-conv" minWidth={110}
                                                numericRange={rTacos}   onNumericFilter={(r) => setRange("tacos", r)}  numericSuffix="%" numericPresets={[10, 30, 50]} />
                                            <SortableTh sortKey="conversion_pct" label="Conversion %" sort={sort} onSort={onSort} className="col-conv" minWidth={130}
                                                numericRange={rConv}    onNumericFilter={(r) => setRange("conv", r)}   numericSuffix="%" numericPresets={[1, 5, 10]} />
                                            <SortableTh sortKey="clicks"      label="Clicks"      sort={sort} onSort={onSort} className="col-units col-divide-l" minWidth={100}
                                                numericRange={rClicks}  onNumericFilter={(r) => setRange("clicks", r)} numericPresets={[10, 100, 1000]} />
                                            <SortableTh sortKey="impressions" label="Impressions" sort={sort} onSort={onSort} className="col-units" minWidth={130}
                                                numericRange={rImpr}    onNumericFilter={(r) => setRange("impr", r)}   numericPresets={[100, 1000, 10000]} />
                                            <SortableTh sortKey="sessions"    label="Sessions"    sort={sort} onSort={onSort} className="col-summary col-divide-l" minWidth={110}
                                                numericRange={rSessions} onNumericFilter={(r) => setRange("sess", r)}  numericPresets={[10, 100, 1000]} />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sorted.map((r, i) => {
                                            const acos = r.acos != null ? Number(r.acos) : null;
                                            const roas = r.roas != null ? Number(r.roas) : null;
                                            const acosBad = acos != null && acos > acosMin;
                                            const roasBad = roas != null && roas < roasMax;
                                            return (
                                                <tr key={i} className="group">
                                                    <td style={{ position: "sticky", left: 0 }}   className="border-b z-10 min-w-[80px]">{safe(r.week)}</td>
                                                    <td style={{ position: "sticky", left: 80 }}  className="border-b z-10 min-w-[120px]">{safe(r.SKU)}</td>
                                                    <td style={{ position: "sticky", left: 200 }} className="font-medium border-b z-10 min-w-[140px]">{safe(r.Model)}</td>
                                                    <td style={{ position: "sticky", left: 340 }} className="border-b z-10 min-w-[120px] border-r-2 border-r-border">{safe(r.asin)}</td>
                                                    <td className="border-b">{safe(r.brand)}</td>
                                                    <td className="text-right tabular border-b col-sales col-divide-l font-medium">{fmtINR(r.ad_spend)}</td>
                                                    <td className="text-right tabular border-b col-sales">{fmtINR(r.attributed_sales)}</td>
                                                    <td className="text-right tabular border-b col-units col-divide-l">{int(r.ams_orders)}</td>
                                                    <td
                                                        className="text-right tabular border-b col-conv col-divide-l font-semibold"
                                                        style={acosBad ? { color: "#b91c1c", background: "#fef2f2" } : undefined}
                                                    >
                                                        {pct(r.acos)}
                                                    </td>
                                                    <td
                                                        className="text-right tabular border-b col-conv font-semibold"
                                                        style={roasBad ? { color: "#b91c1c", background: "#fef2f2" } : undefined}
                                                    >
                                                        {roasFmt(r.roas)}
                                                    </td>
                                                    <td className="text-right tabular border-b col-conv">{pct(r.tacos ?? r.TACOS)}</td>
                                                    <td className="text-right tabular border-b col-conv">{pct(r.conversion_pct)}</td>
                                                    <td className="text-right tabular border-b col-units col-divide-l">{int(r.clicks)}</td>
                                                    <td className="text-right tabular border-b col-units">{int(r.impressions)}</td>
                                                    <td className="text-right tabular border-b col-summary col-divide-l">{int(r.sessions)}</td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        )}
                    </Card>
                </>
            )}
        </AppLayout>
    );
}
