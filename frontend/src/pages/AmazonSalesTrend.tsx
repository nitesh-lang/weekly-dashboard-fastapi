import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, sortWeeks, exportToCsv } from "@/lib/utils";
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
import { TrendingUp, TrendingDown, Minus, Download, ShoppingCart } from "lucide-react";

interface AmazonRow {
    model: string;
    brand?: string;
    asin?: string;
    category_l0?: string;
    category_l1?: string;
    category_l2?: string;
    last_4w_units: number;
    avg_4w_units: number;
    last_4w_sessions: number;
    avg_4w_sessions: number;
    avg_4w_conversion: number;
    last_4w_conversion: number;
    inventory_units: number;
    trend: "UP" | "DOWN" | "FLAT" | "N/A";
    [key: string]: any;
}

interface AmazonData {
    rows: AmazonRow[];
    weeks: string[]; all_weeks: string[]; brands: string[];
    selected_brands: string[]; selected_weeks: string[];
}

function TrendBadge({ t }: { t: AmazonRow["trend"] }) {
    if (t === "UP")   return <span className="inline-flex items-center gap-1 text-success text-xs font-semibold"><TrendingUp className="h-3 w-3" />UP</span>;
    if (t === "DOWN") return <span className="inline-flex items-center gap-1 text-destructive text-xs font-semibold"><TrendingDown className="h-3 w-3" />DOWN</span>;
    if (t === "FLAT") return <span className="inline-flex items-center gap-1 text-muted-foreground text-xs"><Minus className="h-3 w-3" />FLAT</span>;
    return <span className="text-muted-foreground text-xs">—</span>;
}

export default function AmazonSalesTrend() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const selectedBrands = useMemo(() => params.getAll("brands").filter(Boolean),    [qsKey]);
    const selectedWeeks  = useMemo(() => params.getAll("sel_weeks").filter(Boolean), [qsKey]);
    const selModels      = useMemo(() => params.getAll("models").filter(Boolean),    [qsKey]);
    const selAsins       = useMemo(() => params.getAll("asins").filter(Boolean),     [qsKey]);
    const [filter, setFilter] = useDebouncedUrlParam("q");
    const setSelModels = (values: string[]) => setMulti("models", values);
    const setSelAsins  = (values: string[]) => setMulti("asins",  values);

    const qs = new URLSearchParams();
    selectedBrands.forEach((b) => qs.append("brands", b));
    selectedWeeks.forEach((w) => qs.append("sel_weeks", w));

    const { data, isLoading, error } = useQuery<AmazonData>({
        queryKey: ["amazon-sales-trend", qs.toString()],
        queryFn: () => api.get("/api/amazon-sales-trend?" + qs.toString()),
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

    const selL0  = useMemo(() => params.getAll("cat_l0").filter(Boolean), [qsKey]);
    const selL1  = useMemo(() => params.getAll("cat_l1").filter(Boolean), [qsKey]);
    const selL2  = useMemo(() => params.getAll("cat_l2").filter(Boolean), [qsKey]);
    const rL4u   = useMemo(() => getRange("last_4w_u"),    [qsKey]);
    const rA4u   = useMemo(() => getRange("avg_4w_u"),     [qsKey]);
    const rL4s   = useMemo(() => getRange("last_4w_s"),    [qsKey]);
    const rA4s   = useMemo(() => getRange("avg_4w_s"),     [qsKey]);
    const rA4c   = useMemo(() => getRange("avg_4w_conv"),  [qsKey]);
    const rInv   = useMemo(() => getRange("inv"),          [qsKey]);

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
            if (!inRange(r.last_4w_units,      rL4u)) return false;
            if (!inRange(r.avg_4w_units,       rA4u)) return false;
            if (!inRange(r.last_4w_sessions,   rL4s)) return false;
            if (!inRange(r.avg_4w_sessions,    rA4s)) return false;
            if (!inRange(r.avg_4w_conversion,  rA4c)) return false;
            if (!inRange(r.inventory_units,    rInv)) return false;
            if (!q) return true;
            return ((r.model || "") + " " + (r.asin || "") + " " + (r.brand || "") + " " + (r.category_l0 || ""))
                .toLowerCase()
                .includes(q);
        });
    }, [data, filter, selModels, selAsins, selL0, selL1, selL2, rL4u, rA4u, rL4s, rA4s, rA4c, rInv]);

    const { sorted, sort, onSort } = useSortedRows(filtered, { key: "last_4w_units", dir: "desc" });

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Marketplace · Amazon</div>
                    <h1 className="text-2xl font-semibold tracking-tight">Amazon + 1P Sales Trend</h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Per-week sales, units, sessions, conversion — broken out by AM, 1P, and combined Amazon.
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
                <Card className="overflow-hidden">
                    <SectionHeader
                        icon={ShoppingCart}
                        iconColor="#d97706"
                        title="Amazon + 1P Trend"
                        subtitle={`${data.rows.length.toLocaleString()} models · ${filtered.length} shown`}
                        action={
                            <>
                                <Input
                                    placeholder="Filter Model / ASIN / Brand / Category…"
                                    value={filter}
                                    onChange={(e) => setFilter(e.target.value)}
                                    className="max-w-xs h-8 text-sm"
                                />
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                        const cols = ["model", "asin", "brand", "category_l0", "category_l1", "category_l2",
                                            ...weeks.flatMap((w) => [`${w}_sales`, `${w}_units`, `${w}_sales_pct`, `${w}_sessions`, `${w}_conversion`]),
                                            "last_4w_units", "avg_4w_units", "last_4w_sessions", "avg_4w_sessions", "avg_4w_conversion",
                                            "inventory_units", "trend"];
                                        exportToCsv(sorted as any, cols, "amazon-sales-trend.csv");
                                    }}
                                >
                                    <Download className="h-3.5 w-3.5" />
                                    Export CSV
                                </Button>
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
                                    <SortableTh sortKey="category_l0" label="Category L0" sort={sort} onSort={onSort} align="left" stickyLeft={250} minWidth={150} lastFrozen
                                        filterValues={allL0}     filterSelected={selL0}     onFilterChange={(v) => setMulti("cat_l0", v)} />
                                    <SortableTh sortKey="category_l1" label="Category L1" sort={sort} onSort={onSort} align="left" minWidth={140}
                                        filterValues={allL1}     filterSelected={selL1}     onFilterChange={(v) => setMulti("cat_l1", v)} />
                                    <SortableTh sortKey="category_l2" label="Category L2" sort={sort} onSort={onSort} align="left" minWidth={140}
                                        filterValues={allL2}     filterSelected={selL2}     onFilterChange={(v) => setMulti("cat_l2", v)} />
                                    {weeks.map((w, i) => (
                                        <SortableTh key={w + "_s"}  sortKey={w + "_sales"}      label={w + " Sales ₹"} sort={sort} onSort={onSort} className={"col-sales" + (i === 0 ? " col-divide-l" : "")} />
                                    ))}
                                    {weeks.map((w, i) => (
                                        <SortableTh key={w + "_u"}  sortKey={w + "_units"}      label={w + " Units"}   sort={sort} onSort={onSort} className={"col-units" + (i === 0 ? " col-divide-l" : "")} />
                                    ))}
                                    {weeks.map((w, i) => (
                                        <SortableTh key={w + "_p"}  sortKey={w + "_sales_pct"}  label={w + " Sales %"} sort={sort} onSort={onSort} className={"col-pct" + (i === 0 ? " col-divide-l" : "")} />
                                    ))}
                                    {weeks.map((w, i) => (
                                        <SortableTh key={w + "_ss"} sortKey={w + "_sessions"}   label={w + " Sessions"} sort={sort} onSort={onSort} className={"col-sessions" + (i === 0 ? " col-divide-l" : "")} />
                                    ))}
                                    {weeks.map((w, i) => (
                                        <SortableTh key={w + "_c"}  sortKey={w + "_conversion"} label={w + " Conv %"}  sort={sort} onSort={onSort} className={"col-conv" + (i === 0 ? " col-divide-l" : "")} />
                                    ))}
                                    <SortableTh sortKey="last_4w_units"     label="Last 4W Units"    sort={sort} onSort={onSort} className="col-summary col-divide-l" minWidth={130}
                                        numericRange={rL4u} onNumericFilter={(r) => setRange("last_4w_u", r)}   numericPresets={[1, 10, 50]} />
                                    <SortableTh sortKey="avg_4w_units"      label="4W Avg Units"     sort={sort} onSort={onSort} className="col-summary" minWidth={130}
                                        numericRange={rA4u} onNumericFilter={(r) => setRange("avg_4w_u", r)}    numericPresets={[0.5, 2, 5]} />
                                    <SortableTh sortKey="last_4w_sessions"  label="Last 4W Sessions" sort={sort} onSort={onSort} className="col-summary" minWidth={150}
                                        numericRange={rL4s} onNumericFilter={(r) => setRange("last_4w_s", r)}   numericPresets={[10, 100, 500]} />
                                    <SortableTh sortKey="avg_4w_sessions"   label="4W Avg Sessions"  sort={sort} onSort={onSort} className="col-summary" minWidth={140}
                                        numericRange={rA4s} onNumericFilter={(r) => setRange("avg_4w_s", r)}    numericPresets={[10, 100, 500]} />
                                    <SortableTh sortKey="avg_4w_conversion" label="4W Avg Conv"      sort={sort} onSort={onSort} className="col-summary" minWidth={130}
                                        numericRange={rA4c} onNumericFilter={(r) => setRange("avg_4w_conv", r)} numericSuffix="%" numericPresets={[1, 5, 10]} />
                                    <SortableTh sortKey="inventory_units"   label="Inventory"        sort={sort} onSort={onSort} className="col-inv col-divide-l" minWidth={120}
                                        numericRange={rInv} onNumericFilter={(r) => setRange("inv", r)}         numericPresets={[1, 50, 500]} />
                                    <SortableTh sortKey="trend"             label="Trend"            sort={sort} onSort={onSort} align="center" className="col-trend col-divide-l" />
                                </tr>
                            </thead>
                            <tbody>
                                {sorted.map((r, i) => (
                                    <tr key={i} className="group">
                                        <td style={{ position: "sticky", left: 0 }}   className="px-3 py-2 font-medium border-b bg-background group-hover:bg-accent/40 z-10 min-w-[130px]">{r.model}</td>
                                        <td style={{ position: "sticky", left: 130 }} className="px-3 py-2  border-b bg-background group-hover:bg-accent/40 z-10 min-w-[120px]"><AsinLink asin={r.asin} /></td>
                                        <td style={{ position: "sticky", left: 250 }} className="px-3 py-2 text-foreground border-b bg-background group-hover:bg-accent/40 z-10 min-w-[150px] border-r-2 border-r-border">{r.category_l0 || ""}</td>
                                        <td className="px-3 py-2 text-foreground border-b">{r.category_l1 || ""}</td>
                                        <td className="px-3 py-2 text-foreground border-b">{r.category_l2 || ""}</td>
                                        {weeks.map((w, i) => (
                                            <td key={w + "_s"} className={"px-3 py-2 text-right tabular col-sales" + (i === 0 ? " col-divide-l" : "")}>{fmtINR(r[w + "_sales"])}</td>
                                        ))}
                                        {weeks.map((w, i) => (
                                            <td key={w + "_u"} className={"px-3 py-2 text-right tabular col-units" + (i === 0 ? " col-divide-l" : "")}>{fmtInt(r[w + "_units"])}</td>
                                        ))}
                                        {weeks.map((w, i) => (
                                            <td key={w + "_p"} className={"px-3 py-2 text-right tabular col-pct" + (i === 0 ? " col-divide-l" : "")}>{r[w + "_sales_pct"] != null ? (r[w + "_sales_pct"] as number).toFixed(2) + "%" : "—"}</td>
                                        ))}
                                        {weeks.map((w, i) => (
                                            <td key={w + "_ss"} className={"px-3 py-2 text-right tabular col-sessions" + (i === 0 ? " col-divide-l" : "")}>{fmtInt(r[w + "_sessions"])}</td>
                                        ))}
                                        {weeks.map((w, i) => (
                                            <td key={w + "_c"} className={"px-3 py-2 text-right tabular col-conv" + (i === 0 ? " col-divide-l" : "")}>{r[w + "_conversion"] != null ? (r[w + "_conversion"] as number).toFixed(2) + "%" : "—"}</td>
                                        ))}
                                        <td className="px-3 py-2 text-right tabular font-medium col-summary col-divide-l">{fmtInt(r.last_4w_units)}</td>
                                        <td className="px-3 py-2 text-right tabular col-summary">{r.avg_4w_units != null ? r.avg_4w_units.toFixed(2) : "—"}</td>
                                        <td className="px-3 py-2 text-right tabular col-summary">{fmtInt(r.last_4w_sessions)}</td>
                                        <td className="px-3 py-2 text-right tabular col-summary">{r.avg_4w_sessions != null ? r.avg_4w_sessions.toFixed(2) : "—"}</td>
                                        <td className="px-3 py-2 text-right tabular col-summary">{r.avg_4w_conversion != null ? r.avg_4w_conversion.toFixed(2) + "%" : "—"}</td>
                                        <td className="px-3 py-2 text-right tabular col-inv col-divide-l">{fmtInt(r.inventory_units)}</td>
                                        <td className="px-3 py-2 text-center col-trend col-divide-l"><TrendBadge t={r.trend} /></td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </Card>
            )}
        </AppLayout>
    );
}
