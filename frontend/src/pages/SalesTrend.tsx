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
import { TrendingUp, TrendingDown, Minus, Download, LineChart, Copy, Check, ChevronDown, ChevronUp } from "lucide-react";
import { TrendChart } from "@/components/TrendChart";

interface TrendRow {
    model: string;
    brand?: string;
    asin?: string;
    sku?: string;
    category_l0?: string;
    category_l1?: string;
    category_l2?: string;
    last_4w_units: number;
    avg_4w: number;
    trend: "UP" | "DOWN" | "FLAT" | "N/A";
    inventory_units: number;
    [key: string]: any;  // dynamic Week N_units / _sales / _sales_pct
}
interface SalesTrendData {
    rows: TrendRow[]; trend_total: number;
    weeks: string[]; all_weeks: string[]; brands: string[];
    selected_brands: string[]; selected_weeks: string[];
}

function TrendBadge({ t }: { t: TrendRow["trend"] }) {
    if (t === "UP")   return <span className="inline-flex items-center gap-1 text-success text-xs font-semibold"><TrendingUp className="h-3 w-3" />UP</span>;
    if (t === "DOWN") return <span className="inline-flex items-center gap-1 text-destructive text-xs font-semibold"><TrendingDown className="h-3 w-3" />DOWN</span>;
    if (t === "FLAT") return <span className="inline-flex items-center gap-1 text-muted-foreground text-xs"><Minus className="h-3 w-3" />FLAT</span>;
    return <span className="text-muted-foreground text-xs">—</span>;
}

export default function SalesTrend() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    // Memoize URL-derived arrays on the stable qsKey so React Query / useMemo
    // hooks downstream don't see fresh references every render.
    const selectedBrands = useMemo(() => params.getAll("brands").filter(Boolean),    [qsKey]);
    const selectedWeeks  = useMemo(() => params.getAll("sel_weeks").filter(Boolean), [qsKey]);
    const selModels      = useMemo(() => params.getAll("models").filter(Boolean),    [qsKey]);
    const selAsins       = useMemo(() => params.getAll("asins").filter(Boolean),     [qsKey]);
    const [filter, setFilter] = useDebouncedUrlParam("q");
    const setSelModels = (values: string[]) => setMulti("models", values);
    const setSelAsins  = (values: string[]) => setMulti("asins",  values);

    // Only weeks + brands are sent to the backend; models + asins filter
    // the loaded rows client-side, so they stay out of the queryKey to
    // avoid unnecessary refetches when the operator narrows the view.
    const qs = new URLSearchParams();
    selectedBrands.forEach((b) => qs.append("brands", b));
    selectedWeeks.forEach((w) => qs.append("sel_weeks", w));

    const { data, isLoading, error } = useQuery<SalesTrendData>({
        queryKey: ["sales-trend", qs.toString()],
        queryFn: () => api.get("/api/sales-trend?" + qs.toString()),
    });

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

    const weeks     = data?.weeks || [];
    const allWeeks  = useMemo(() => sortWeeks(data?.all_weeks || []), [data]);
    const allBrands = data?.brands || [];

    const allModels = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.model).filter(Boolean))).sort() as string[], [data]);
    const allAsins  = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.asin).filter(Boolean))).sort() as string[], [data]);
    const allL0     = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.category_l0).filter(Boolean))).sort() as string[], [data]);
    const allL1     = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.category_l1).filter(Boolean))).sort() as string[], [data]);
    const allL2     = useMemo(() => Array.from(new Set((data?.rows || []).map((r) => r.category_l2).filter(Boolean))).sort() as string[], [data]);

    const selL0       = useMemo(() => params.getAll("cat_l0").filter(Boolean), [qsKey]);
    const selL1       = useMemo(() => params.getAll("cat_l1").filter(Boolean), [qsKey]);
    const selL2       = useMemo(() => params.getAll("cat_l2").filter(Boolean), [qsKey]);
    const rLast4w     = useMemo(() => getRange("last_4w"),  [qsKey]);
    const rAvg4w      = useMemo(() => getRange("avg_4w"),   [qsKey]);
    const rInv        = useMemo(() => getRange("inv"),      [qsKey]);

    const filtered = useMemo(() => {
        if (!data) return [];
        const q = filter.trim().toLowerCase();
        const modelSet = new Set(selModels);
        const asinSet  = new Set(selAsins);
        const l0s = new Set(selL0);
        const l1s = new Set(selL1);
        const l2s = new Set(selL2);
        function inRange(v: any, rng: { min: number | null; max: number | null }): boolean {
            if (rng.min == null && rng.max == null) return true;
            if (v == null || v === "" || isNaN(Number(v))) return false;
            const num = Number(v);
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        return data.rows.filter((r) => {
            if (modelSet.size && !modelSet.has(r.model || "")) return false;
            if (asinSet.size  && !asinSet.has(r.asin   || "")) return false;
            if (l0s.size && !l0s.has(r.category_l0 || "")) return false;
            if (l1s.size && !l1s.has(r.category_l1 || "")) return false;
            if (l2s.size && !l2s.has(r.category_l2 || "")) return false;
            if (!inRange(r.last_4w_units,   rLast4w)) return false;
            if (!inRange(r.avg_4w,          rAvg4w))  return false;
            if (!inRange(r.inventory_units, rInv))    return false;
            if (!q) return true;
            return ((r.model || "") + " " + (r.asin || "") + " " + (r.brand || "") + " " + (r.category_l0 || ""))
                .toLowerCase()
                .includes(q);
        });
    }, [data, filter, selModels, selAsins, selL0, selL1, selL2, rLast4w, rAvg4w, rInv]);

    const { sorted, sort, onSort } = useSortedRows(filtered, { key: "last_4w_units", dir: "desc" });

    // Overview chart — per-week GMV + units rolled up across the currently
    // visible (filtered) rows.  Uses allWeeks (full history) so the chart's
    // own 4w/8w/12w/All toggle works independently of the page Weeks picker.
    const [showOverview, setShowOverview] = useState(true);
    const chartTrend = useMemo(() => {
        return allWeeks.map((w) => {
            let gmv = 0, units = 0;
            for (const r of filtered) {
                const s = r[w + "_sales"];
                const u = r[w + "_units"];
                if (typeof s === "number") gmv   += s;
                if (typeof u === "number") units += u;
            }
            return { week: w, gmv, units };
        });
    }, [filtered, allWeeks]);

    // KPI strip — current-week totals matching the chart's rightmost point
    // and the Dashboard's GMV tile.  Operator confusion was reading the
    // W23 chart peak (~Rs 1.45 Cr) as "current" when latest is W24.
    const kpis = useMemo(() => {
        if (!chartTrend.length) return null;
        const latest = chartTrend[chartTrend.length - 1];
        const prev   = chartTrend.length > 1 ? chartTrend[chartTrend.length - 2] : null;
        const wow   = (prev && prev.gmv > 0) ? (latest.gmv - prev.gmv) / prev.gmv : null;
        const wowU  = (prev && prev.units > 0) ? (latest.units - prev.units) / prev.units : null;
        const modelsActive = filtered.filter((r) => {
            const u = r[latest.week + "_units"];
            return typeof u === "number" && u > 0;
        }).length;
        return { latest, wow, wowU, modelsActive };
    }, [chartTrend, filtered]);

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Performance · Weekly</div>
                    <h1 className="text-2xl font-semibold tracking-tight">Sales Trend</h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Model-level units &amp; sales across all weeks · per-channel breakouts available via filter.
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Weeks"  options={allWeeks}  selected={selectedWeeks}  onApply={(v) => setMulti("sel_weeks", v)} />
                <MultiPicker label="Brands" options={allBrands} selected={selectedBrands} onApply={(v) => setMulti("brands", v)} />
                <MultiPicker label="Models" options={allModels} selected={selModels}      onApply={setSelModels} />
                <MultiPicker label="ASINs"  options={allAsins}  selected={selAsins}       onApply={setSelAsins} />
            </div>

            {isLoading && <LoadingSkeleton rows={8} />}
            {error && <ErrorBlock error={error} />}

            {data && (
                <>
                    {kpis && (
                        <Card className="px-5 py-4">
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                                <div>
                                    <div className="text-[11px] uppercase tracking-wider text-muted-foreground font-medium">
                                        {kpis.latest.week} GMV
                                    </div>
                                    <div className="flex items-baseline gap-2 mt-1">
                                        <div className="text-2xl font-semibold tabular tracking-tight">
                                            {fmtINR(kpis.latest.gmv)}
                                        </div>
                                        {kpis.wow != null && (
                                            <span className={`text-xs font-medium tabular ${kpis.wow >= 0 ? "text-emerald-600" : "text-red-600"}`}>
                                                {kpis.wow >= 0 ? "▲" : "▼"} {(Math.abs(kpis.wow) * 100).toFixed(1)}%
                                            </span>
                                        )}
                                    </div>
                                    <div className="text-[11px] text-muted-foreground mt-0.5">vs prior week</div>
                                </div>
                                <div>
                                    <div className="text-[11px] uppercase tracking-wider text-muted-foreground font-medium">
                                        {kpis.latest.week} Units
                                    </div>
                                    <div className="flex items-baseline gap-2 mt-1">
                                        <div className="text-2xl font-semibold tabular tracking-tight">
                                            {fmtInt(kpis.latest.units)}
                                        </div>
                                        {kpis.wowU != null && (
                                            <span className={`text-xs font-medium tabular ${kpis.wowU >= 0 ? "text-emerald-600" : "text-red-600"}`}>
                                                {kpis.wowU >= 0 ? "▲" : "▼"} {(Math.abs(kpis.wowU) * 100).toFixed(1)}%
                                            </span>
                                        )}
                                    </div>
                                    <div className="text-[11px] text-muted-foreground mt-0.5">vs prior week</div>
                                </div>
                                <div>
                                    <div className="text-[11px] uppercase tracking-wider text-muted-foreground font-medium">
                                        Models active
                                    </div>
                                    <div className="text-2xl font-semibold tabular tracking-tight mt-1">
                                        {fmtInt(kpis.modelsActive)}
                                    </div>
                                    <div className="text-[11px] text-muted-foreground mt-0.5">of {fmtInt(filtered.length)} shown</div>
                                </div>
                            </div>
                        </Card>
                    )}
                    <div className="flex items-center justify-end">
                        <Button variant="ghost" size="sm" onClick={() => setShowOverview((s) => !s)}>
                            {showOverview
                                ? <><ChevronUp className="h-3.5 w-3.5" />Hide overview</>
                                : <><ChevronDown className="h-3.5 w-3.5" />Show overview</>}
                        </Button>
                    </div>
                    {showOverview && (
                        <TrendChart
                            trend={chartTrend}
                            selectedBrandLabel={
                                selectedBrands.length === 1 ? selectedBrands[0]
                                : selectedBrands.length > 1 ? `${selectedBrands.length} brands`
                                : "All Brands"
                            }
                        />
                    )}
                <Card className="overflow-hidden">
                    <SectionHeader
                        icon={LineChart}
                        iconColor="#059669"
                        title="Model-Level Sales Trend"
                        subtitle={`${data.trend_total.toLocaleString()} models · ${filtered.length} shown`}
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
                                            ...weeks.flatMap((w) => [`${w}_sales`, `${w}_units`, `${w}_sales_pct`]),
                                            "last_4w_units", "avg_4w", "inventory_units", "trend"]}
                                            filename="sales-trend.xlsx"
                                        />
                            </>
                        }
                    />
                    <div className="overflow-auto max-h-[72vh]">
                        <table className="w-full text-[13px] border-separate border-spacing-0">
                            <thead className="sticky top-0 z-30">
                                <tr>
                                    <SortableTh sortKey="model"       label="Model"       sort={sort} onSort={onSort} align="left" stickyLeft={0}   minWidth={130}
                                        filterValues={allModels} filterSelected={selModels} onFilterChange={(v) => setMulti("models", v)} />
                                    <SortableTh sortKey="asin"        label="ASIN"        sort={sort} onSort={onSort} align="left" stickyLeft={130} minWidth={120}
                                        filterValues={allAsins}  filterSelected={selAsins}  onFilterChange={(v) => setMulti("asins", v)} />
                                    <SortableTh sortKey="sku"         label="SKU"         sort={sort} onSort={onSort} align="left" stickyLeft={250} minWidth={120} />
                                    <SortableTh sortKey="category_l0" label="Category L0" sort={sort} onSort={onSort} align="left" stickyLeft={370} minWidth={150} lastFrozen
                                        filterValues={allL0}     filterSelected={selL0}     onFilterChange={(v) => setMulti("cat_l0", v)} />
                                    <SortableTh sortKey="category_l1" label="Category L1" sort={sort} onSort={onSort} align="left" minWidth={140}
                                        filterValues={allL1}     filterSelected={selL1}     onFilterChange={(v) => setMulti("cat_l1", v)} />
                                    <SortableTh sortKey="category_l2" label="Category L2" sort={sort} onSort={onSort} align="left" minWidth={140}
                                        filterValues={allL2}     filterSelected={selL2}     onFilterChange={(v) => setMulti("cat_l2", v)} />
                                    {weeks.map((w, i) => (
                                        <SortableTh key={w + "_s"} sortKey={w + "_sales"}     label={w + " Sales ₹"} sort={sort} onSort={onSort} className={"col-sales" + (i === 0 ? " col-divide-l" : "")} />
                                    ))}
                                    {weeks.map((w, i) => (
                                        <SortableTh key={w + "_u"} sortKey={w + "_units"}     label={w + " Units"}   sort={sort} onSort={onSort} className={"col-units" + (i === 0 ? " col-divide-l" : "")} />
                                    ))}
                                    {weeks.map((w, i) => (
                                        <SortableTh key={w + "_p"} sortKey={w + "_sales_pct"} label={w + " Sales %"} sort={sort} onSort={onSort} className={"col-pct"   + (i === 0 ? " col-divide-l" : "")} />
                                    ))}
                                    <SortableTh sortKey="last_4w_units"   label="Last 4W Units" sort={sort} onSort={onSort} className="col-summary col-divide-l" minWidth={130}
                                        numericRange={rLast4w} onNumericFilter={(r) => setRange("last_4w", r)} numericPresets={[1, 10, 50]} />
                                    <SortableTh sortKey="avg_4w"          label="4W Avg"        sort={sort} onSort={onSort} className="col-summary" minWidth={110}
                                        numericRange={rAvg4w}  onNumericFilter={(r) => setRange("avg_4w", r)}  numericPresets={[0.5, 2, 5]} />
                                    <SortableTh sortKey="inventory_units" label="Inventory"     sort={sort} onSort={onSort} className="col-inv col-divide-l" minWidth={120}
                                        numericRange={rInv}    onNumericFilter={(r) => setRange("inv", r)}     numericPresets={[1, 50, 500]} />
                                    <SortableTh sortKey="trend"           label="Trend"         sort={sort} onSort={onSort} align="center" className="col-trend col-divide-l" />
                                </tr>
                            </thead>
                            <tbody>
                                {sorted.map((r, i) => (
                                    <tr key={i} className="group">
                                        <td style={{ position: "sticky", left: 0 }}   className="px-3 py-2 font-medium border-b bg-background group-hover:bg-accent/40 z-10 min-w-[130px]">{r.model}</td>
                                        <td style={{ position: "sticky", left: 130 }} className="px-3 py-2 border-b bg-background group-hover:bg-accent/40 z-10 min-w-[120px]"><AsinLink asin={r.asin} /></td>
                                        <td style={{ position: "sticky", left: 250 }} className="px-3 py-2 text-foreground border-b bg-background group-hover:bg-accent/40 z-10 min-w-[120px]">{r.sku || ""}</td>
                                        <td style={{ position: "sticky", left: 370 }} className="px-3 py-2 text-foreground border-b bg-background group-hover:bg-accent/40 z-10 min-w-[150px] border-r-2 border-r-border">{r.category_l0 || ""}</td>
                                        <td className="px-3 py-2 text-foreground border-b">{r.category_l1 || ""}</td>
                                        <td className="px-3 py-2 text-foreground border-b">{r.category_l2 || ""}</td>
                                        {weeks.map((w, i) => (
                                            <td key={w + "_s"} className={"px-3 py-2 text-right tabular col-sales" + (i === 0 ? " col-divide-l" : "")}>{fmtINR(r[w + "_sales"])}</td>
                                        ))}
                                        {weeks.map((w, i) => (
                                            <td key={w + "_u"} className={"px-3 py-2 text-right tabular col-units" + (i === 0 ? " col-divide-l" : "")}>{fmtInt(r[w + "_units"])}</td>
                                        ))}
                                        {weeks.map((w, i) => (
                                            <td key={w + "_p"} className={"px-3 py-2 text-right tabular col-pct" + (i === 0 ? " col-divide-l" : "")}>
                                                {r[w + "_sales_pct"] != null ? (r[w + "_sales_pct"] as number).toFixed(2) + "%" : "—"}
                                            </td>
                                        ))}
                                        <td className="px-3 py-2 text-right tabular font-medium col-summary col-divide-l">{fmtInt(r.last_4w_units)}</td>
                                        <td className="px-3 py-2 text-right tabular col-summary">{r.avg_4w != null ? r.avg_4w.toFixed(2) : "—"}</td>
                                        <td className="px-3 py-2 text-right tabular col-inv col-divide-l">{fmtInt(r.inventory_units)}</td>
                                        <td className="px-3 py-2 text-center col-trend col-divide-l"><TrendBadge t={r.trend} /></td>
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
