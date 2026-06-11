import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, sortWeeks, exportToXlsx, copyTableToClipboard } from "@/lib/utils";
import { useCanExport } from "@/lib/auth";
import { useSortedRows } from "@/lib/useSortedRows";
import { makeTextFilter } from "@/lib/useTextFilter";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { SortableTh } from "@/components/SortableTh";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronRight, Package, Clock, Radio, Download, Copy, Check } from "lucide-react";

interface InventoryRow {
    week?: string;
    brand?: string;
    model?: string;
    sku?: string;
    category_l0?: string;
    category_l1?: string;
    category_l2?: string;
    channel?: string;
    type?: string;
    inventory_units?: number;
    nlc?: number;
    inventory_value?: number;
    [k: string]: any;
}
interface InvKpis {
    total_units?: number;
    total_value?: number;
    in_transit_pct?: number;
    unsellable_pct?: number;
}
interface AgingRow { bucket: string; units: number; }
interface ChannelRow { channel: string; location?: string; units: number; value: number; }
interface InvData {
    rows: InventoryRow[]; inv_total: number; latest_week: string;
    kpis: InvKpis; aging: AgingRow[]; channel_summary: ChannelRow[];
    available_weeks: string[]; available_brands: string[];
}

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

const MissingTag = () => <span className="text-destructive italic text-[11px]">⚠ missing</span>;

/** Extract distinct non-empty values for a key from row data, sorted alphabetically. */
function uniqSorted(rows: InventoryRow[] | undefined, key: keyof InventoryRow): string[] {
    if (!rows) return [];
    const s = new Set<string>();
    for (const r of rows) {
        const v = r[key];
        if (typeof v === "string" && v.trim()) s.add(v.trim());
    }
    return Array.from(s).sort();
}

export default function InventoryDashboard() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const selectedWeek  = params.get("week") || "";
    const selectedBrand = params.get("brand") || "";
    // New multi-pickers (URL-persisted, client-side filters).
    const selCatL0   = useMemo(() => params.getAll("cat_l0").filter(Boolean),   [qsKey]);
    const selCatL1   = useMemo(() => params.getAll("cat_l1").filter(Boolean),   [qsKey]);
    const selCatL2   = useMemo(() => params.getAll("cat_l2").filter(Boolean),   [qsKey]);
    const selChannel = useMemo(() => params.getAll("channel").filter(Boolean),  [qsKey]);
    const selType    = useMemo(() => params.getAll("type").filter(Boolean),     [qsKey]);
    const [filter, setFilter] = useDebouncedUrlParam("q");
    const [showBreakdowns, setShowBreakdowns] = useState(false);

    const qs = new URLSearchParams();
    if (selectedWeek)  qs.set("week", selectedWeek);
    if (selectedBrand) qs.set("brand", selectedBrand);

    const { data, isLoading, error } = useQuery<InvData>({
        queryKey: ["inventory-dashboard", qs.toString()],
        queryFn: () => api.get("/api/inventory-dashboard?" + qs.toString()),
    });

    function setSingle(name: string, values: string[]) {
        const next = new URLSearchParams(params);
        next.delete(name);
        if (values.length) next.set(name, values[0]);
        setParams(next, { replace: false });
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

    // Extra state for column-header filters
    const selModels  = useMemo(() => params.getAll("models").filter(Boolean), [qsKey]);
    const selSkus    = useMemo(() => params.getAll("skus").filter(Boolean),   [qsKey]);
    const selAsins   = useMemo(() => params.getAll("asins").filter(Boolean),  [qsKey]);
    const selBrandsM = useMemo(() => params.getAll("brands").filter(Boolean), [qsKey]);
    const selWeeksM  = useMemo(() => params.getAll("weeks").filter(Boolean),  [qsKey]);
    const rUnits     = useMemo(() => getRange("units"), [qsKey]);
    const rNlc       = useMemo(() => getRange("nlc"),   [qsKey]);
    const rValue     = useMemo(() => getRange("value"), [qsKey]);

    const allWeeks  = useMemo(() => sortWeeks(data?.available_weeks || []), [data]);
    // Brand picker excludes Fossil entirely — operator request.
    const allBrands = useMemo(
        () => (data?.available_brands || []).filter(
            (b) => b.trim().toLowerCase() !== "fossil",
        ),
        [data],
    );
    // Distinct values for the new pickers — derived from the loaded rows so
    // they always reflect what's actually in scope (week/brand-filtered).
    const allCatL0   = useMemo(() => uniqSorted(data?.rows, "category_l0"), [data]);
    const allCatL1   = useMemo(() => uniqSorted(data?.rows, "category_l1"), [data]);
    const allCatL2   = useMemo(() => uniqSorted(data?.rows, "category_l2"), [data]);
    const allChannel = useMemo(() => uniqSorted(data?.rows, "channel"),     [data]);
    const allType    = useMemo(() => uniqSorted(data?.rows, "type"),        [data]);
    const allModelsM = useMemo(() => uniqSorted(data?.rows, "model"),       [data]);
    const allSkusM   = useMemo(() => uniqSorted(data?.rows, "sku"),         [data]);
    const allAsinsM  = useMemo(() => uniqSorted(data?.rows, "asin"),        [data]);
    const allBrandsM = useMemo(() => uniqSorted(data?.rows, "brand"),       [data]);
    const allWeeksM  = useMemo(() => uniqSorted(data?.rows, "week"),        [data]);

    // Default the week selector to the latest available week on first load.
    // Without this the page lands on an "all weeks" aggregate which is rarely
    // what the operator wants — they want "current state".
    useEffect(() => {
        if (!selectedWeek && data?.latest_week) {
            const next = new URLSearchParams(params);
            next.set("week", data.latest_week);
            setParams(next, { replace: true });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data?.latest_week]);

    const filtered = useMemo(() => {
        if (!data) return [];
        // Old code searched Object.values(r) — that matched numeric columns
        // too, so typing "100" hit every row with inventory_units=100.
        // Restrict to identifier + category fields.
        const matchText = makeTextFilter<any>(filter, [
            "brand", "model", "sku", "asin",
            "category_l0", "category_l1", "category_l2",
            "channel", "type", "week",
        ]);
        const l0s  = new Set(selCatL0);
        const l1s  = new Set(selCatL1);
        const l2s  = new Set(selCatL2);
        const chs  = new Set(selChannel);
        const tps  = new Set(selType);
        const ms   = new Set(selModels);
        const ss   = new Set(selSkus);
        const as   = new Set(selAsins);
        const bs   = new Set(selBrandsM);
        const ws   = new Set(selWeeksM);
        function inRange(v: any, rng: { min: number | null; max: number | null }): boolean {
            if (rng.min == null && rng.max == null) return true;
            if (v == null || v === "" || isNaN(Number(v))) return false;
            const num = Number(v);
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        // Hide Fossil rows unless explicitly selected via the URL brand param OR a column-level brand filter that includes Fossil.
        const explicitFossil = (selectedBrand || "").trim().toLowerCase() === "fossil"
            || selBrandsM.some((b) => b.toLowerCase() === "fossil");
        return data.rows.filter((r) => {
            if (!explicitFossil && (r.brand || "").trim().toLowerCase() === "fossil") return false;
            if (l0s.size && !l0s.has(r.category_l0 || "")) return false;
            if (l1s.size && !l1s.has(r.category_l1 || "")) return false;
            if (l2s.size && !l2s.has(r.category_l2 || "")) return false;
            if (chs.size && !chs.has(r.channel || ""))     return false;
            if (tps.size && !tps.has(r.type || ""))        return false;
            if (ms.size  && !ms.has(r.model || ""))        return false;
            if (ss.size  && !ss.has(r.sku || ""))          return false;
            if (as.size  && !as.has(r.asin || ""))         return false;
            if (bs.size  && !bs.has(r.brand || ""))        return false;
            if (ws.size  && !ws.has(r.week || ""))         return false;
            if (!inRange(r.inventory_units, rUnits)) return false;
            if (!inRange(r.nlc,             rNlc))   return false;
            if (!inRange(r.inventory_value, rValue)) return false;
            return matchText(r);
        });
    }, [data, filter, selCatL0, selCatL1, selCatL2, selChannel, selType, selectedBrand,
        selModels, selSkus, selAsins, selBrandsM, selWeeksM, rUnits, rNlc, rValue]);

    const { sorted, sort, onSort } = useSortedRows(filtered, { key: "inventory_value", dir: "desc" });

    // KPIs derived from the *client-filtered* rows so picking Cat L0/L1/L2,
    // Channel, Type, or the column-header filters collapses the headline
    // numbers in step with the table.  data.kpis was server-computed over
    // the loaded dataset (week + brand from the URL) and ignored the rest.
    // Mirror the backend math:
    //   total_units  = sum(inventory_units)
    //   total_value  = sum(inventory_value)
    //   in_transit   = sum(units) where type contains "transit"
    //   unsellable   = sum(units) where type contains "unsellable"
    const filteredKpis = useMemo(() => {
        let totalUnits = 0, totalValue = 0, inTransit = 0, unsellable = 0;
        for (const r of filtered) {
            const u = Number(r.inventory_units ?? 0) || 0;
            const v = Number(r.inventory_value ?? 0) || 0;
            totalUnits += u;
            totalValue += v;
            const t = String(r.type ?? "").toLowerCase();
            if (t.includes("transit"))    inTransit  += u;
            if (t.includes("unsellable")) unsellable += u;
        }
        return {
            total_units:    totalUnits,
            total_value:    totalValue,
            in_transit_pct: totalUnits ? +(inTransit  / totalUnits * 100).toFixed(2) : null,
            unsellable_pct: totalUnits ? +(unsellable / totalUnits * 100).toFixed(2) : null,
        };
    }, [filtered]);

    const agingRows = data?.aging || [];
    const { sorted: sortedAging,  sort: sortAging,   onSort: onSortAging  } = useSortedRows(agingRows);
    const channelRows = data?.channel_summary || [];
    const { sorted: sortedChan,   sort: sortChan,    onSort: onSortChan   } = useSortedRows(channelRows, { key: "value", dir: "desc" });

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Operations · Stock</div>
                    <h1 className="text-2xl font-semibold tracking-tight">Inventory Snapshot</h1>
                    <p className="text-[13.5px] tabular mt-1.5" style={{ color: "#4b5563" }}>
                        {data?.latest_week && <>Week: <strong style={{ color: "#0a0a0a" }}>{data.latest_week}</strong></>}
                        {data && <> · <strong style={{ color: "#0a0a0a" }}>{data.inv_total.toLocaleString()}</strong> SKU positions across all channels.</>}
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Week"      options={allWeeks}    selected={selectedWeek  ? [selectedWeek]  : []} onApply={(v) => setSingle("week", v.slice(0, 1))} maxLabelItems={1} />
                <MultiPicker label="Brand"     options={allBrands}   selected={selectedBrand ? [selectedBrand] : []} onApply={(v) => setSingle("brand", v.slice(0, 1))} maxLabelItems={1} />
                <MultiPicker label="Cat L0"    options={allCatL0}    selected={selCatL0}    onApply={(v) => setMulti("cat_l0", v)} />
                <MultiPicker label="Cat L1"    options={allCatL1}    selected={selCatL1}    onApply={(v) => setMulti("cat_l1", v)} />
                <MultiPicker label="Cat L2"    options={allCatL2}    selected={selCatL2}    onApply={(v) => setMulti("cat_l2", v)} />
                <MultiPicker label="Channel"   options={allChannel}  selected={selChannel}  onApply={(v) => setMulti("channel", v)} />
                <MultiPicker label="Type"      options={allType}     selected={selType}     onApply={(v) => setMulti("type", v)} />
            </div>

            {isLoading && <LoadingSkeleton rows={8} />}
            {error && <ErrorBlock error={error} />}

            {data && (
                <>
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        <MiniKpi label="Total Units"  value={fmtInt(filteredKpis.total_units)}                                                          accent="#2563eb" />
                        <MiniKpi label="Total Value"  value={fmtINR(filteredKpis.total_value)}                                                          accent="#059669" />
                        <MiniKpi label="In-Transit %" value={filteredKpis.in_transit_pct != null ? filteredKpis.in_transit_pct.toFixed(1) + "%" : "—"}   accent="#d97706" />
                        <MiniKpi label="Unsellable %" value={filteredKpis.unsellable_pct != null ? filteredKpis.unsellable_pct.toFixed(1) + "%" : "—"}   accent="#b91c1c" />
                    </div>

                    {(data.aging?.length > 0 || data.channel_summary?.length > 0) && (
                        <div>
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={() => setShowBreakdowns((s) => !s)}
                                className="mb-2"
                            >
                                {showBreakdowns
                                    ? <ChevronDown className="h-3.5 w-3.5" />
                                    : <ChevronRight className="h-3.5 w-3.5" />}
                                {showBreakdowns ? "Hide" : "Show"} aging + channel breakdown
                            </Button>
                        </div>
                    )}
                    {showBreakdowns && (data.aging?.length > 0 || data.channel_summary?.length > 0) && (
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                            {data.aging?.length > 0 && (
                                <Card className="overflow-hidden">
                                    <SectionHeader icon={Clock} iconColor="#d97706" title="Aging" subtitle="By bucket" />
                                    <table className="w-full text-[13px]">
                                        <thead className="bg-secondary">
                                            <tr>
                                                <SortableTh sortKey="bucket" label="Bucket" sort={sortAging} onSort={onSortAging} align="left" />
                                                <SortableTh sortKey="units"  label="Units"  sort={sortAging} onSort={onSortAging} />
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {sortedAging.map((r, i) => (
                                                <tr key={i} className="border-b hover:bg-accent/40">
                                                    <td className="px-3 py-2">{r.bucket}</td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtInt(r.units)}</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </Card>
                            )}
                            {data.channel_summary?.length > 0 && (
                                <Card className="overflow-hidden">
                                    <SectionHeader icon={Radio} iconColor="#0891b2" title="Channel Summary" subtitle="By location" />
                                    <table className="w-full text-[13px]">
                                        <thead className="bg-secondary">
                                            <tr>
                                                <SortableTh sortKey="channel"  label="Channel"   sort={sortChan} onSort={onSortChan} align="left" />
                                                <SortableTh sortKey="location" label="Location"  sort={sortChan} onSort={onSortChan} align="left" />
                                                <SortableTh sortKey="units"    label="Units"     sort={sortChan} onSort={onSortChan} />
                                                <SortableTh sortKey="value"    label="Value (₹)" sort={sortChan} onSort={onSortChan} />
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {sortedChan.map((r, i) => (
                                                <tr key={i} className="border-b hover:bg-accent/40">
                                                    <td className="px-3 py-2">{r.channel}</td>
                                                    <td className="px-3 py-2 text-foreground">{r.location || ""}</td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtInt(r.units)}</td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtINR(r.value)}</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </Card>
                            )}
                        </div>
                    )}

                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={Package}
                            iconColor="#2563eb"
                            title="Inventory Detail"
                            subtitle="SKU × Channel positions"
                            action={
                                <>
                                    <Input
                                        placeholder="Filter…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    {useCanExport() && (
                                        <>
                                            <Button
                                                variant="outline"
                                                size="sm"
                                                onClick={() => exportToXlsx(
                                                    sorted as any,
                                                    // Keep all dims in the CSV export even though the table view
                                                    // hides them — operators may want them downstream.
                                                    ["week", "brand", "model", "sku",
                                                     "category_l0", "category_l1", "category_l2",
                                                     "channel", "type",
                                                     "inventory_units", "nlc", "inventory_value"],
                                                    "inventory-detail.xlsx",
                                                )}
                                            >
                                                <Download className="h-3.5 w-3.5" />
                                                Export
                                            </Button>
                                            <Button
                                                variant="outline"
                                                size="sm"
                                                onClick={() => copyTableToClipboard(sorted as any,
                                                    ["week", "brand", "model", "sku",
                                                     "category_l0", "category_l1", "category_l2",
                                                     "channel", "type",
                                                     "inventory_units", "nlc", "inventory_value"])}
                                            >
                                                <Copy className="h-3.5 w-3.5" />
                                                Copy
                                            </Button>
                                        </>
                                    )}
                                </>
                            }
                        />
                        <div className="overflow-auto max-h-[72vh]">
                            {/* No `w-full`: the table sizes to its content so columns don't stretch
                                to fill the viewport.  Frozen offsets are tight: Week 70 / Brand 100 /
                                Model 130 / SKU 100 — total 400px frozen area. */}
                            <table className="text-[13px] border-separate border-spacing-0">
                                <thead className="sticky top-0 z-30">
                                    <tr>
                                        {/* Hidden columns (Cat L0/L1/L2, Channel, Type) are now filter
                                            pickers in the page header — keeps the table compact + scannable. */}
                                        <SortableTh sortKey="week"        label="Week"  sort={sort} onSort={onSort} align="left" stickyLeft={0}   minWidth={80}
                                            filterValues={allWeeksM}  filterSelected={selWeeksM}  onFilterChange={(v) => setMulti("weeks", v)} />
                                        <SortableTh sortKey="brand"       label="Brand" sort={sort} onSort={onSort} align="left" stickyLeft={80}  minWidth={110}
                                            filterValues={allBrandsM} filterSelected={selBrandsM} onFilterChange={(v) => setMulti("brands", v)} />
                                        <SortableTh sortKey="model"       label="Model" sort={sort} onSort={onSort} align="left" stickyLeft={190} minWidth={140}
                                            filterValues={allModelsM} filterSelected={selModels}  onFilterChange={(v) => setMulti("models", v)} />
                                        <SortableTh sortKey="sku"         label="SKU"   sort={sort} onSort={onSort} align="left" stickyLeft={330} minWidth={110}
                                            filterValues={allSkusM}   filterSelected={selSkus}    onFilterChange={(v) => setMulti("skus", v)} />
                                        <SortableTh sortKey="asin"        label="ASIN"  sort={sort} onSort={onSort} align="left" stickyLeft={440} minWidth={120}
                                            filterValues={allAsinsM}  filterSelected={selAsins}   onFilterChange={(v) => setMulti("asins", v)} />
                                        {/* Channel sits next to ASIN so the operator can see at a glance
                                            where a model is parked (AMPM / YNT / Amazon …) without
                                            scrolling.  Reuses the existing top-bar Channel picker via
                                            the shared selChannel state. */}
                                        <SortableTh sortKey="channel"     label="Channel" sort={sort} onSort={onSort} align="left" stickyLeft={560} minWidth={100} lastFrozen
                                            filterValues={allChannel} filterSelected={selChannel} onFilterChange={(v) => setMulti("channel", v)} />
                                        <SortableTh sortKey="inventory_units" label="Units"     sort={sort} onSort={onSort} className="col-units col-divide-l" minWidth={110}
                                            numericRange={rUnits} onNumericFilter={(r) => setRange("units", r)} numericPresets={[1, 50, 500]} />
                                        <SortableTh sortKey="nlc"             label="NLC (₹)"   sort={sort} onSort={onSort} className="col-pct" minWidth={130}
                                            numericRange={rNlc}   onNumericFilter={(r) => setRange("nlc", r)}   numericPresets={[100, 500, 1000]} />
                                        <SortableTh sortKey="inventory_value" label="Value (₹)" sort={sort} onSort={onSort} className="col-sales" minWidth={130}
                                            numericRange={rValue} onNumericFilter={(r) => setRange("value", r)} numericPresets={[1000, 10000, 100000]} />
                                    </tr>
                                </thead>
                                <tbody>
                                    {sorted.map((r, i) => (
                                        <tr key={i} className="group">
                                            <td style={{ position: "sticky", left: 0 }}   className="px-3 py-2 border-b bg-background group-hover:bg-accent/40 z-10 min-w-[80px]">{r.week}</td>
                                            <td style={{ position: "sticky", left: 80 }}  className="px-3 py-2 border-b bg-background group-hover:bg-accent/40 z-10 min-w-[110px]">{r.brand}</td>
                                            <td style={{ position: "sticky", left: 190 }} className="px-3 py-2 font-medium border-b bg-background group-hover:bg-accent/40 z-10 min-w-[140px]">{r.model}</td>
                                            <td style={{ position: "sticky", left: 330 }} className="px-3 py-2 border-b bg-background group-hover:bg-accent/40 z-10 min-w-[110px]">{r.sku || "—"}</td>
                                            <td style={{ position: "sticky", left: 440 }} className="px-3 py-2 border-b bg-background group-hover:bg-accent/40 z-10 min-w-[120px] font-mono text-[12.5px]">{r.asin || "—"}</td>
                                            <td style={{ position: "sticky", left: 560 }} className="px-3 py-2 border-b bg-background group-hover:bg-accent/40 z-10 min-w-[100px] border-r-2 border-r-border text-[12.5px] uppercase tracking-wide text-foreground/80">{r.channel || "—"}</td>
                                            <td className="px-3 py-2 text-right tabular border-b col-units col-divide-l">{fmtInt(r.inventory_units)}</td>
                                            <td className="px-3 py-2 text-right tabular border-b col-pct">{fmtINR(r.nlc)}</td>
                                            <td className="px-3 py-2 text-right tabular border-b col-sales">{fmtINR(r.inventory_value)}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </Card>
                </>
            )}
        </AppLayout>
    );
}
