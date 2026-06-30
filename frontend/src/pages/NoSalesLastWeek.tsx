import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, sortWeeks, exportToXlsx, copyTableToClipboard } from "@/lib/utils";
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
import { Download, AlertCircle, Copy, Check } from "lucide-react";

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
interface SalesTrendData {
    rows: TrendRow[];
    weeks: string[]; all_weeks: string[]; brands: string[];
}

interface InventoryRow {
    brand?: string;
    model?: string;
    sku?: string;
    channel?: string;
    type?: string;
    inventory_units?: number;
}
interface InventoryData {
    rows: InventoryRow[];
    latest_week: string;
}

const EXCLUDE_BRAND = "Fossil";

// AMPM channel synonyms — matches the canonical inventory channel mapping
// in weekly_app/routes/inventory_dashboard.py (CHANNEL_TO_TYPE).
const AMPM_CHANNELS = new Set(["ampm", "b2b-ampm", "b2b - ampm"]);
function isAmpm(channel: string | undefined): boolean {
    if (!channel) return false;
    return AMPM_CHANNELS.has(channel.trim().toLowerCase());
}

export default function NoSalesLastWeek() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const selectedBrands = params.getAll("brands").filter(Boolean);
    const selectedWeeks  = params.getAll("sel_weeks").filter(Boolean);
    const [filter, setFilter] = useDebouncedUrlParam("q");

    const qs = new URLSearchParams();
    selectedBrands.forEach((b) => qs.append("brands", b));
    selectedWeeks.forEach((w)  => qs.append("sel_weeks", w));

    // Shares queryKey with /sales-trend + /inventory-dashboard so prior visits
    // to those pages (or hover prefetch) populate this page's cache.
    const { data, isLoading, error } = useQuery<SalesTrendData>({
        queryKey: ["sales-trend", qs.toString()],
        queryFn: () => api.get("/api/sales-trend?" + qs.toString()),
    });
    const { data: invData } = useQuery<InventoryData>({
        queryKey: ["inventory-dashboard", ""],
        queryFn: () => api.get("/api/inventory-dashboard"),
    });

    // Build {brand|model → AMPM units} from the inventory snapshot.
    // Sum across all SKUs/locations that map to an AMPM channel value.
    const ampmByModel = useMemo(() => {
        const m = new Map<string, number>();
        if (!invData?.rows) return m;
        for (const r of invData.rows) {
            if (!isAmpm(r.channel)) continue;
            const brand = (r.brand || "").trim().toLowerCase();
            const model = (r.model || "").trim().toLowerCase();
            if (!brand || !model) continue;
            const key = `${brand}|${model}`;
            m.set(key, (m.get(key) || 0) + (r.inventory_units || 0));
        }
        return m;
    }, [invData]);

    function ampmFor(brand?: string, model?: string): number | null {
        if (!brand || !model) return null;
        const key = `${brand.trim().toLowerCase()}|${model.trim().toLowerCase()}`;
        return ampmByModel.has(key) ? (ampmByModel.get(key) as number) : null;
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
    // Column-header filter state
    const selModels = useMemo(() => params.getAll("models").filter(Boolean), [qsKey]);
    const selAsins  = useMemo(() => params.getAll("asins").filter(Boolean),  [qsKey]);
    const selL0     = useMemo(() => params.getAll("cat_l0").filter(Boolean), [qsKey]);
    const selL1     = useMemo(() => params.getAll("cat_l1").filter(Boolean), [qsKey]);
    const rLast4w   = useMemo(() => getRange("last_4w"), [qsKey]);
    const rAvg4w    = useMemo(() => getRange("avg_4w"),  [qsKey]);
    const rAmpm     = useMemo(() => getRange("ampm"),    [qsKey]);
    const rInv      = useMemo(() => getRange("inv"),     [qsKey]);

    const weeks = data?.weeks || [];
    // Latest week is the last entry in the chronologically-sorted weeks array.
    // sales-trend API returns weeks in display order already.
    const latestWeek = weeks.length > 0 ? weeks[weeks.length - 1] : "";

    const allWeeks  = useMemo(() => sortWeeks(data?.all_weeks || []), [data]);
    const allBrands = useMemo(
        () => (data?.brands || []).filter((b) => b.toLowerCase() !== EXCLUDE_BRAND.toLowerCase()),
        [data],
    );
    const allModels = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.model).filter(Boolean))).sort() as string[], [data]);
    const allAsins  = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.asin).filter(Boolean))).sort()  as string[], [data]);
    const allL0     = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.category_l0).filter(Boolean))).sort() as string[], [data]);
    const allL1     = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.category_l1).filter(Boolean))).sort() as string[], [data]);

    // Core filter: brand != Fossil AND latest-week units == 0 (or null/undefined).
    const noSaleRows = useMemo(() => {
        if (!data || !latestWeek) return [];
        const unitsKey = `${latestWeek}_units`;
        return data.rows.filter((r) => {
            const brand = (r.brand || "").trim();
            if (!brand) return false;
            if (brand.toLowerCase() === EXCLUDE_BRAND.toLowerCase()) return false;
            const u = r[unitsKey];
            return u == null || u === 0;
        });
    }, [data, latestWeek]);

    // Inject the AMPM inventory onto each row so it's sortable like a native field.
    const rowsWithAmpm = useMemo(
        () => noSaleRows.map((r) => ({ ...r, ampm_inventory: ampmFor(r.brand, r.model) })),
        [noSaleRows, ampmByModel],
    );

    const searchFiltered = useMemo(() => {
        const q = filter.trim().toLowerCase();
        const ms  = new Set(selModels);
        const as_ = new Set(selAsins);
        const l0s = new Set(selL0);
        const l1s = new Set(selL1);
        function inRange(v: any, rng: { min: number | null; max: number | null }): boolean {
            if (rng.min == null && rng.max == null) return true;
            if (v == null || v === "" || isNaN(Number(v))) return false;
            const num = Number(v);
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        return rowsWithAmpm.filter((r) => {
            if (ms.size  && !ms.has(r.model || ""))         return false;
            if (as_.size && !as_.has(r.asin || ""))         return false;
            if (l0s.size && !l0s.has(r.category_l0 || "")) return false;
            if (l1s.size && !l1s.has(r.category_l1 || "")) return false;
            if (!inRange(r.last_4w_units,   rLast4w)) return false;
            if (!inRange(r.avg_4w,          rAvg4w))  return false;
            if (!inRange(r.ampm_inventory,  rAmpm))   return false;
            if (!inRange(r.inventory_units, rInv))    return false;
            if (!q) return true;
            return ((r.model || "") + " " + (r.asin || "") + " " + (r.brand || "") + " " + (r.category_l0 || ""))
                .toLowerCase()
                .includes(q);
        });
    }, [rowsWithAmpm, filter, selModels, selAsins, selL0, selL1, rLast4w, rAvg4w, rAmpm, rInv]);

    const { sorted, sort, onSort } = useSortedRows(searchFiltered, { key: "ampm_inventory", dir: "desc" });

    // Brand-level counts for the summary chip strip.
    const brandCounts = useMemo(() => {
        const m = new Map<string, number>();
        noSaleRows.forEach((r) => {
            const b = (r.brand || "—").trim() || "—";
            m.set(b, (m.get(b) || 0) + 1);
        });
        return Array.from(m.entries()).sort((a, b) => b[1] - a[1]);
    }, [noSaleRows]);

    // Aggregate KPIs: total flagged, brands affected, AMPM units sitting idle, 4W avg dollars at risk
    const topSummary = useMemo(() => {
        let ampmTotal = 0;
        let total4wUnits = 0;
        noSaleRows.forEach((r) => {
            if (r.ampm_inventory != null) ampmTotal += r.ampm_inventory;
            if (r.last_4w_units != null)  total4wUnits += r.last_4w_units;
        });
        return {
            flagged: noSaleRows.length,
            brands: brandCounts.length,
            ampmIdle: ampmTotal,
            momentum: total4wUnits,
        };
    }, [noSaleRows, brandCounts]);

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2" style={{ color: "#b91c1c" }}>Signals · Zero-Sale Watch</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <AlertCircle className="h-5 w-5" style={{ color: "#b91c1c" }} />
                        No Sales Last Week
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Models with zero units in <strong style={{ color: "#0a0a0a" }}>{latestWeek || "—"}</strong>
                        {" "}across all brands except <strong style={{ color: "#0a0a0a" }}>Fossil</strong>. Sort by AMPM
                        Inventory to see which stock is sitting idle.
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker
                    label="Weeks"
                    options={allWeeks}
                    selected={selectedWeeks}
                    onApply={(v) => setMulti("sel_weeks", v)}
                    placeholder={
                        weeks.length
                            ? `${weeks[0]}–${weeks[weeks.length - 1]} · ${weeks.length}w shown`
                            : "All"
                    }
                />
                <MultiPicker label="Brands" options={allBrands} selected={selectedBrands} onApply={(v) => setMulti("brands", v)} />
            </div>

            {isLoading && <LoadingSkeleton rows={8} />}
            {error    && <div className="text-sm" style={{ color: "#b91c1c" }}>Failed to load: {(error as Error).message}</div>}

            {data && (
                <>
                    {/* Summary KPI strip */}
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                        {[
                            { label: "Flagged Models",   value: fmtInt(topSummary.flagged),  accent: "#b91c1c", color: "#0a0a0a" },
                            { label: "Brands Affected",  value: fmtInt(topSummary.brands),   accent: "#d97706", color: "#0a0a0a" },
                            { label: "AMPM Units Idle",  value: fmtInt(topSummary.ampmIdle), accent: "#2563eb", color: "#0a0a0a" },
                            { label: "Recent 4W Units",  value: fmtInt(topSummary.momentum), accent: "#059669", color: "#0a0a0a" },
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

                    {/* Brand-level no-sale counts */}
                    {brandCounts.length > 0 && (
                        <div className="flex flex-wrap gap-2">
                            {brandCounts.map(([b, n]) => (
                                <span
                                    key={b}
                                    className="inline-flex items-center gap-1.5 rounded-md border px-3 py-1 text-[13px]"
                                    style={{ background: "#fef2f2", borderColor: "#fecaca", color: "#991b1b" }}
                                >
                                    <span className="font-semibold">{b}</span>
                                    <span>{n} model{n === 1 ? "" : "s"}</span>
                                </span>
                            ))}
                        </div>
                    )}

                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={AlertCircle}
                            iconColor="#b91c1c"
                            title="Models with No Sales"
                            subtitle={
                                searchFiltered.length !== noSaleRows.length
                                    ? `${latestWeek || "—"} · ${noSaleRows.length.toLocaleString()} models · ${searchFiltered.length} shown`
                                    : `${latestWeek || "—"} · ${noSaleRows.length.toLocaleString()} models`
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
                                            columns={["model", "asin", "brand", "category_l0", "category_l1", "category_l2",
                                                ...weeks.flatMap((w) => [`${w}_sales`, `${w}_units`]),
                                                "last_4w_units", "avg_4w", "inventory_units", "ampm_inventory", "trend"]}
                                            filename="no-sales-last-week.xlsx"
                                        />
                                </>
                            }
                        />

                        {sorted.length === 0 ? (
                            <div className="p-10 text-center text-sm">
                                {noSaleRows.length === 0
                                    ? `Nothing flagged — every non-Fossil model had sales in ${latestWeek || "the latest week"}.`
                                    : "No rows match the current filter."}
                            </div>
                        ) : (
                            <div className="overflow-auto max-h-[72vh]">
                                <table className="w-full text-[14px] border-separate border-spacing-0">
                                    <thead className="sticky top-0 z-30">
                                        <tr>
                                            <SortableTh sortKey="model"           label="Model"          sort={sort} onSort={onSort} align="left"  stickyLeft={0}   minWidth={140}
                                                filterValues={allModels} filterSelected={selModels} onFilterChange={(v) => setMulti("models", v)} />
                                            <SortableTh sortKey="brand"           label="Brand"          sort={sort} onSort={onSort} align="left"  stickyLeft={140} minWidth={120}
                                                filterValues={allBrands} filterSelected={selectedBrands} onFilterChange={(v) => setMulti("brands", v)} />
                                            <SortableTh sortKey="asin"            label="ASIN"           sort={sort} onSort={onSort} align="left"  stickyLeft={260} minWidth={120} lastFrozen
                                                filterValues={allAsins}  filterSelected={selAsins}  onFilterChange={(v) => setMulti("asins", v)} />
                                            <SortableTh sortKey="category_l0"     label="Category L0"    sort={sort} onSort={onSort} align="left" minWidth={140}
                                                filterValues={allL0}     filterSelected={selL0}     onFilterChange={(v) => setMulti("cat_l0", v)} />
                                            <SortableTh sortKey="category_l1"     label="Category L1"    sort={sort} onSort={onSort} align="left" minWidth={140}
                                                filterValues={allL1}     filterSelected={selL1}     onFilterChange={(v) => setMulti("cat_l1", v)} />
                                            <SortableTh sortKey="last_4w_units"   label="Last 4W Units"  sort={sort} onSort={onSort} className="col-summary col-divide-l" minWidth={130}
                                                numericRange={rLast4w} onNumericFilter={(r) => setRange("last_4w", r)} numericPresets={[1, 10, 50]} />
                                            <SortableTh sortKey="avg_4w"          label="4W Avg"         sort={sort} onSort={onSort} className="col-summary" minWidth={110}
                                                numericRange={rAvg4w}  onNumericFilter={(r) => setRange("avg_4w", r)}  numericPresets={[0.5, 2, 5]} />
                                            <SortableTh sortKey="ampm_inventory"  label="AMPM Inv"       sort={sort} onSort={onSort} className="col-units col-divide-l" minWidth={120}
                                                numericRange={rAmpm}   onNumericFilter={(r) => setRange("ampm", r)}    numericPresets={[1, 50, 500]} />
                                            <SortableTh sortKey="inventory_units" label="Total Inv"      sort={sort} onSort={onSort} className="col-inv" minWidth={120}
                                                numericRange={rInv}    onNumericFilter={(r) => setRange("inv", r)}     numericPresets={[1, 100, 1000]} />
                                            {weeks.slice(-4, -1).map((w, i) => (
                                                <SortableTh key={w + "_u"} sortKey={w + "_units"} label={w + " Units"} sort={sort} onSort={onSort} className={"col-pct" + (i === 0 ? " col-divide-l" : "")} />
                                            ))}
                                            <SortableTh sortKey={`${latestWeek}_units`} label={`${latestWeek} Units`} sort={sort} onSort={onSort} className="col-conv col-divide-l" />
                                            <SortableTh sortKey="trend" label="Trend" sort={sort} onSort={onSort} align="center" className="col-trend col-divide-l" />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sorted.map((r, i) => (
                                            <tr key={i} className="group">
                                                <td style={{ position: "sticky", left: 0 }}   className="font-medium border-b z-10 min-w-[140px]">{r.model}</td>
                                                <td style={{ position: "sticky", left: 140 }} className="border-b z-10 min-w-[120px]">{r.brand}</td>
                                                <td style={{ position: "sticky", left: 260 }} className="border-b z-10 min-w-[110px] border-r-2 border-r-border"><AsinLink asin={r.asin} /></td>
                                                <td className="border-b">{r.category_l0 || ""}</td>
                                                <td className="border-b">{r.category_l1 || ""}</td>
                                                <td className="text-right tabular border-b font-medium col-summary col-divide-l">{fmtInt(r.last_4w_units)}</td>
                                                <td className="text-right tabular border-b col-summary">{r.avg_4w != null ? r.avg_4w.toFixed(2) : "—"}</td>
                                                <td
                                                    className="text-right tabular border-b font-medium col-units col-divide-l"
                                                    style={r.ampm_inventory != null && r.ampm_inventory > 0
                                                        ? { color: "#1e3a8a", background: "#dbe9ff" }
                                                        : undefined}
                                                >
                                                    {r.ampm_inventory != null ? fmtInt(r.ampm_inventory) : "—"}
                                                </td>
                                                <td className="text-right tabular border-b col-inv">{fmtInt(r.inventory_units)}</td>
                                                {weeks.slice(-4, -1).map((w, i) => (
                                                    <td key={w + "_u"} className={"text-right tabular border-b col-pct" + (i === 0 ? " col-divide-l" : "")}>{fmtInt((r as any)[w + "_units"])}</td>
                                                ))}
                                                <td
                                                    className="text-right tabular border-b font-semibold col-divide-l"
                                                    style={{ color: "#b91c1c", background: "#fef2f2" }}
                                                >
                                                    0
                                                </td>
                                                <td className="text-center border-b col-trend col-divide-l">{r.trend}</td>
                                            </tr>
                                        ))}
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
