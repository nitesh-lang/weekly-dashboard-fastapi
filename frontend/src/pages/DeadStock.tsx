import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, exportToXlsx, copyTableToClipboard } from "@/lib/utils";
import { useSortedRows } from "@/lib/useSortedRows";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { AsinLink } from "@/components/AsinLink";
import { SortableTh } from "@/components/SortableTh";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { ExportButtons } from "@/components/ExportButtons";
import { Download, Archive, Sliders, Copy, Check } from "lucide-react";

interface TrendRow {
    model: string;
    brand?: string;
    asin?: string;
    category_l0?: string;
    category_l1?: string;
    category_l2?: string;
    last_4w_units: number;
    avg_4w: number;
    trend: "UP" | "DOWN" | "FLAT" | "N/A";
    inventory_units: number;
    [key: string]: any;
}
interface SalesTrendData { rows: TrendRow[]; weeks: string[]; brands: string[]; }

interface InventoryRow {
    brand?: string;
    model?: string;
    sku?: string;
    channel?: string;
    type?: string;
    inventory_units?: number;
    nlc?: number;
    inventory_value?: number;
}
interface InventoryData { rows: InventoryRow[]; latest_week: string; }

// Dead-stock thresholds — operator-tunable from the UI.
const DEFAULT_MIN_INV  = 20;     // ≥ 20 units on hand before flagging
const DEFAULT_MAX_4W   = 5;      // last 4w units sold ≤ 5
const DEFAULT_MIN_COVER = 12;    // OR weeks-of-cover ≥ 12 weeks

function numParam(p: URLSearchParams, name: string, fallback: number): number {
    const raw = p.get(name);
    if (raw == null) return fallback;
    const n = parseFloat(raw);
    return isFinite(n) ? n : fallback;
}

const AMPM_CHANNELS = new Set(["ampm", "b2b-ampm", "b2b - ampm"]);
function isAmpm(channel: string | undefined): boolean {
    if (!channel) return false;
    return AMPM_CHANNELS.has(channel.trim().toLowerCase());
}

export default function DeadStock() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const [filter, setFilter] = useDebouncedUrlParam("q");

    // URL-backed state — reload / share preserves operator config.
    const minInv    = numParam(params, "min_inv",   DEFAULT_MIN_INV);
    const max4w     = numParam(params, "max_4w",    DEFAULT_MAX_4W);
    const minCover  = numParam(params, "min_cover", DEFAULT_MIN_COVER);
    const selBrands = useMemo(() => params.getAll("brands").filter(Boolean), [qsKey]);

    function setNum(name: string, value: number, def: number) {
        const next = new URLSearchParams(params);
        if (value === def) next.delete(name);
        else               next.set(name, String(value));
        setParams(next, { replace: true });   // replace so slider-drags don't pollute history
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
    const setMinInv    = (v: number)   => setNum("min_inv",   v, DEFAULT_MIN_INV);
    const setMax4w     = (v: number)   => setNum("max_4w",    v, DEFAULT_MAX_4W);
    const setMinCover  = (v: number)   => setNum("min_cover", v, DEFAULT_MIN_COVER);
    const setSelBrands = (v: string[]) => setMulti("brands",  v);

    // Column-header filter state
    const selModels = useMemo(() => params.getAll("models").filter(Boolean), [qsKey]);
    const selAsins  = useMemo(() => params.getAll("asins").filter(Boolean),  [qsKey]);
    const selL0     = useMemo(() => params.getAll("cat_l0").filter(Boolean), [qsKey]);
    const rLast4w   = useMemo(() => getRange("last_4w"), [qsKey]);
    const rAvg4w    = useMemo(() => getRange("avg_4w"),  [qsKey]);
    const rCover    = useMemo(() => getRange("cover"),   [qsKey]);
    const rAmpm     = useMemo(() => getRange("ampm"),    [qsKey]);
    const rTotalInv = useMemo(() => getRange("tot_inv"), [qsKey]);
    const rStuck    = useMemo(() => getRange("stuck"),   [qsKey]);

    // Shares queryKey with /sales-trend + /inventory-dashboard so cache is
    // reused across the Signals pages and the main trend pages.
    const { data: salesData, isLoading: loadingSales, error: salesError } = useQuery<SalesTrendData>({
        queryKey: ["sales-trend", ""],
        queryFn: () => api.get("/api/sales-trend"),
    });
    const { data: invData,  isLoading: loadingInv } = useQuery<InventoryData>({
        queryKey: ["inventory-dashboard", ""],
        queryFn: () => api.get("/api/inventory-dashboard"),
    });
    const isLoading = loadingSales || loadingInv;

    // Build AMPM-only inventory and total inventory maps from the inventory snapshot.
    const { ampmByModel, totalInvByModel, valueByModel } = useMemo(() => {
        const ampm = new Map<string, number>();
        const total = new Map<string, number>();
        const value = new Map<string, number>();
        for (const r of invData?.rows || []) {
            const brand = (r.brand || "").trim().toLowerCase();
            const model = (r.model || "").trim().toLowerCase();
            if (!brand || !model) continue;
            const key = `${brand}|${model}`;
            const units = r.inventory_units || 0;
            const val   = r.inventory_value || 0;
            total.set(key, (total.get(key) || 0) + units);
            value.set(key, (value.get(key) || 0) + val);
            if (isAmpm(r.channel)) ampm.set(key, (ampm.get(key) || 0) + units);
        }
        return { ampmByModel: ampm, totalInvByModel: total, valueByModel: value };
    }, [invData]);

    function lookup(map: Map<string, number>, brand?: string, model?: string): number {
        if (!brand || !model) return 0;
        return map.get(`${brand.trim().toLowerCase()}|${model.trim().toLowerCase()}`) || 0;
    }

    const allBrands = useMemo(() => {
        const s = new Set<string>();
        for (const r of salesData?.rows || []) if (r.brand) s.add(r.brand);
        return Array.from(s).sort();
    }, [salesData]);
    const allModels = useMemo(() => Array.from(new Set((salesData?.rows || []).map((r) => r.model).filter(Boolean))).sort() as string[], [salesData]);
    const allAsins  = useMemo(() => Array.from(new Set((salesData?.rows || []).map((r) => r.asin).filter(Boolean))).sort()  as string[], [salesData]);
    const allL0     = useMemo(() => Array.from(new Set((salesData?.rows || []).map((r) => r.category_l0).filter(Boolean))).sort() as string[], [salesData]);

    // Apply dead-stock rule per row: high inventory + low velocity = dead stock.
    const enriched = useMemo(() => {
        const rows = salesData?.rows || [];
        return rows.map((r) => {
            const ampm  = lookup(ampmByModel,    r.brand, r.model);
            const total = lookup(totalInvByModel, r.brand, r.model) || r.inventory_units || 0;
            const value = lookup(valueByModel,    r.brand, r.model);
            const weeklyAvg = r.avg_4w || 0;
            const weeksOfCover = weeklyAvg > 0 ? total / weeklyAvg : Infinity;
            return {
                ...r,
                ampm_inventory:    ampm,
                total_inventory:   total,
                stuck_value:       value,
                weeks_of_cover:    weeksOfCover,
            };
        });
    }, [salesData, ampmByModel, totalInvByModel, valueByModel]);

    const deadRows = useMemo(() => {
        const brandSet = new Set(selBrands);
        const ms  = new Set(selModels);
        const as_ = new Set(selAsins);
        const l0s = new Set(selL0);
        function inRange(v: any, rng: { min: number | null; max: number | null }): boolean {
            if (rng.min == null && rng.max == null) return true;
            if (v == null || v === "" || isNaN(Number(v))) return false;
            const num = Number(v);
            if (!Number.isFinite(num)) return false;
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        return enriched.filter((r) => {
            if (brandSet.size && !brandSet.has(r.brand || "")) return false;
            if (ms.size  && !ms.has(r.model || ""))            return false;
            if (as_.size && !as_.has(r.asin || ""))            return false;
            if (l0s.size && !l0s.has(r.category_l0 || ""))     return false;
            const inv     = r.total_inventory || 0;
            const recent  = r.last_4w_units   || 0;
            const cover   = r.weeks_of_cover;
            if (inv < minInv) return false;            // not enough stock to bother
            const lowVelocity = recent <= max4w;
            const overCover   = cover >= minCover;
            if (!(lowVelocity || overCover)) return false;
            if (!inRange(r.last_4w_units,   rLast4w))   return false;
            if (!inRange(r.avg_4w,          rAvg4w))    return false;
            if (!inRange(r.weeks_of_cover === Infinity ? 9999 : r.weeks_of_cover, rCover)) return false;
            if (!inRange(r.ampm_inventory,  rAmpm))     return false;
            if (!inRange(r.total_inventory, rTotalInv)) return false;
            if (!inRange(r.stuck_value,     rStuck))    return false;
            return true;
        });
    }, [enriched, minInv, max4w, minCover, selBrands, selModels, selAsins, selL0,
        rLast4w, rAvg4w, rCover, rAmpm, rTotalInv, rStuck]);

    const searchFiltered = useMemo(() => {
        if (!filter.trim()) return deadRows;
        const q = filter.toLowerCase();
        return deadRows.filter((r) =>
            ((r.model || "") + " " + (r.asin || "") + " " + (r.brand || "") + " " + (r.category_l0 || ""))
                .toLowerCase()
                .includes(q),
        );
    }, [deadRows, filter]);

    const { sorted, sort, onSort } = useSortedRows(searchFiltered, { key: "stuck_value", dir: "desc" });

    const totals = useMemo(() => {
        let units = 0, value = 0;
        for (const r of searchFiltered) {
            units += r.total_inventory || 0;
            value += r.stuck_value || 0;
        }
        const brands = new Set<string>(searchFiltered.map((r) => r.brand || "").filter(Boolean));
        return { models: searchFiltered.length, units, value, brands: brands.size };
    }, [searchFiltered]);

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2" style={{ color: "#b91c1c" }}>Signals · Idle Capital</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <Archive className="h-5 w-5" style={{ color: "#b91c1c" }} />
                        Dead Stock
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        High inventory holding · low recent velocity · capital sitting on the shelf. Sort by
                        <strong style={{ color: "#0a0a0a" }}> Value Stuck </strong>to see where the most money is parked.
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Brands" options={allBrands} selected={selBrands} onApply={setSelBrands} />
            </div>

            {/* Threshold tuner */}
            <Card className="overflow-hidden">
                <SectionHeader icon={Sliders} iconColor="#6b7280" title="Thresholds" subtitle="Tune what counts as dead stock" />
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 p-4">
                    <label className="flex flex-col gap-1 text-[13px]">
                        <span className="font-medium">Min inventory (units)</span>
                        <Input type="number" step="1" min="0" value={minInv} onChange={(e) => setMinInv(parseFloat(e.target.value) || 0)} />
                        <span style={{ color: "#6b7280" }}>only models with ≥ {minInv} units on hand</span>
                    </label>
                    <label className="flex flex-col gap-1 text-[13px]">
                        <span className="font-medium">Max units sold (last 4W)</span>
                        <Input type="number" step="1" min="0" value={max4w} onChange={(e) => setMax4w(parseFloat(e.target.value) || 0)} />
                        <span style={{ color: "#6b7280" }}>flagged if ≤ {max4w} sold in 4 weeks</span>
                    </label>
                    <label className="flex flex-col gap-1 text-[13px]">
                        <span className="font-medium">Or: weeks of cover ≥</span>
                        <Input type="number" step="1" min="0" value={minCover} onChange={(e) => setMinCover(parseFloat(e.target.value) || 0)} />
                        <span style={{ color: "#6b7280" }}>flagged if ≥ {minCover} weeks of stock</span>
                    </label>
                </div>
            </Card>

            {isLoading && <LoadingSkeleton rows={8} />}
            {salesError && <div className="text-sm" style={{ color: "#b91c1c" }}>Failed to load: {(salesError as Error).message}</div>}

            {salesData && (
                <>
                    {/* Summary KPI strip */}
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                        {[
                            { label: "Dead Models",   value: fmtInt(totals.models), accent: "#b91c1c", color: "#0a0a0a" },
                            { label: "Brands Affected", value: fmtInt(totals.brands), accent: "#d97706", color: "#0a0a0a" },
                            { label: "Units Stuck",   value: fmtInt(totals.units),  accent: "#2563eb", color: "#0a0a0a" },
                            { label: "Value Stuck",   value: fmtINR(totals.value),  accent: "#b91c1c", color: "#b91c1c" },
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
                            icon={Archive}
                            iconColor="#b91c1c"
                            title="Dead Stock Models"
                            subtitle={
                                searchFiltered.length !== deadRows.length
                                    ? `${deadRows.length.toLocaleString()} flagged · ${searchFiltered.length} shown`
                                    : `${deadRows.length.toLocaleString()} flagged`
                            }
                            action={
                                <>
                                    <Input
                                        placeholder="Filter Model / ASIN / Brand / Category…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    <ExportButtons
                                            rows={sorted as any as any}
                                            columns={["model", "brand", "asin", "category_l0", "category_l1",
                                                "last_4w_units", "avg_4w", "weeks_of_cover",
                                                "ampm_inventory", "total_inventory", "stuck_value", "trend"]}
                                            filename="dead-stock.xlsx"
                                        />
                                </>
                            }
                        />

                        {sorted.length === 0 ? (
                            <div className="p-10 text-center text-sm" style={{ color: "#6b7280" }}>
                                {deadRows.length === 0
                                    ? "No dead stock at the current thresholds — adjust above to widen the net."
                                    : "No rows match the current text filter."}
                            </div>
                        ) : (
                            <div className="overflow-auto max-h-[72vh]">
                                <table className="w-full text-[14px] border-separate border-spacing-0">
                                    <thead className="sticky top-0 z-30">
                                        <tr>
                                            <SortableTh sortKey="model"           label="Model"        sort={sort} onSort={onSort} align="left" stickyLeft={0}   minWidth={140}
                                                filterValues={allModels} filterSelected={selModels} onFilterChange={(v) => setMulti("models", v)} />
                                            <SortableTh sortKey="brand"           label="Brand"        sort={sort} onSort={onSort} align="left" stickyLeft={140} minWidth={120}
                                                filterValues={allBrands} filterSelected={selBrands} onFilterChange={(v) => setMulti("brands", v)} />
                                            <SortableTh sortKey="asin"            label="ASIN"         sort={sort} onSort={onSort} align="left" stickyLeft={260} minWidth={120} lastFrozen
                                                filterValues={allAsins}  filterSelected={selAsins}  onFilterChange={(v) => setMulti("asins", v)} />
                                            <SortableTh sortKey="category_l0"     label="Category L0"  sort={sort} onSort={onSort} align="left" minWidth={140}
                                                filterValues={allL0}     filterSelected={selL0}     onFilterChange={(v) => setMulti("cat_l0", v)} />
                                            <SortableTh sortKey="last_4w_units"   label="Last 4W"      sort={sort} onSort={onSort} className="col-summary col-divide-l" minWidth={110}
                                                numericRange={rLast4w}   onNumericFilter={(r) => setRange("last_4w", r)} numericPresets={[0, 5, 10]} />
                                            <SortableTh sortKey="avg_4w"          label="4W Avg/wk"    sort={sort} onSort={onSort} className="col-summary" minWidth={120}
                                                numericRange={rAvg4w}    onNumericFilter={(r) => setRange("avg_4w", r)}  numericPresets={[0, 2, 5]} />
                                            <SortableTh sortKey="weeks_of_cover"  label="Weeks of Cover" sort={sort} onSort={onSort} className="col-conv col-divide-l" minWidth={140}
                                                numericRange={rCover}    onNumericFilter={(r) => setRange("cover", r)}   numericPresets={[10, 26, 52]} />
                                            <SortableTh sortKey="ampm_inventory"  label="AMPM Inv"     sort={sort} onSort={onSort} className="col-units col-divide-l" minWidth={120}
                                                numericRange={rAmpm}     onNumericFilter={(r) => setRange("ampm", r)}    numericPresets={[1, 50, 500]} />
                                            <SortableTh sortKey="total_inventory" label="Total Inv"    sort={sort} onSort={onSort} className="col-inv" minWidth={120}
                                                numericRange={rTotalInv} onNumericFilter={(r) => setRange("tot_inv", r)} numericPresets={[1, 100, 1000]} />
                                            <SortableTh sortKey="stuck_value"     label="Value Stuck"  sort={sort} onSort={onSort} className="col-sales col-divide-l" minWidth={130}
                                                numericRange={rStuck}    onNumericFilter={(r) => setRange("stuck", r)}   numericPresets={[1000, 10000, 100000]} />
                                            <SortableTh sortKey="trend"           label="Trend"        sort={sort} onSort={onSort} align="center" className="col-trend col-divide-l" />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sorted.map((r, i) => {
                                            const coverHigh = isFinite(r.weeks_of_cover) && r.weeks_of_cover >= minCover;
                                            const noVelocity = (r.last_4w_units || 0) <= max4w;
                                            return (
                                                <tr key={i} className="group">
                                                    <td style={{ position: "sticky", left: 0 }}   className="font-medium border-b z-10 min-w-[140px]">{r.model}</td>
                                                    <td style={{ position: "sticky", left: 140 }} className="border-b z-10 min-w-[120px]">{r.brand}</td>
                                                    <td style={{ position: "sticky", left: 260 }} className="border-b z-10 min-w-[110px] border-r-2 border-r-border"><AsinLink asin={r.asin} /></td>
                                                    <td className="border-b">{r.category_l0 || ""}</td>
                                                    <td
                                                        className="text-right tabular border-b col-summary col-divide-l font-medium"
                                                        style={noVelocity ? { color: "#b91c1c" } : undefined}
                                                    >
                                                        {fmtInt(r.last_4w_units)}
                                                    </td>
                                                    <td className="text-right tabular border-b col-summary">
                                                        {r.avg_4w != null ? r.avg_4w.toFixed(2) : "—"}
                                                    </td>
                                                    <td
                                                        className="text-right tabular border-b col-conv col-divide-l font-semibold"
                                                        style={coverHigh ? { color: "#b91c1c", background: "#fef2f2" } : undefined}
                                                    >
                                                        {isFinite(r.weeks_of_cover)
                                                            ? r.weeks_of_cover.toFixed(1) + "w"
                                                            : "∞"}
                                                    </td>
                                                    <td className="text-right tabular border-b col-units col-divide-l">
                                                        {r.ampm_inventory > 0 ? fmtInt(r.ampm_inventory) : "—"}
                                                    </td>
                                                    <td className="text-right tabular border-b col-inv">
                                                        {fmtInt(r.total_inventory)}
                                                    </td>
                                                    <td
                                                        className="text-right tabular border-b col-sales col-divide-l font-semibold"
                                                        style={{ color: "#b91c1c" }}
                                                    >
                                                        {fmtINR(r.stuck_value)}
                                                    </td>
                                                    <td className="text-center border-b col-trend col-divide-l">{r.trend}</td>
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
