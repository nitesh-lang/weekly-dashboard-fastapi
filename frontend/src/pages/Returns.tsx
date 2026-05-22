import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { api } from "@/lib/api";
import { fmtInt, exportToCsv } from "@/lib/utils";
import { useSortedRows } from "@/lib/useSortedRows";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { SortableTh } from "@/components/SortableTh";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock, EmptyBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Download, RotateCcw, Inbox } from "lucide-react";

interface ReturnsOverviewRow {
    brand:             string;
    model:             string;
    sku:               string;
    asin:              string;
    category_l0:       string;
    category_l1:       string;
    units_sold_30d:    number;
    return_units:      number;
    returns_3p:        number;
    returns_1p:        number;
    return_pct:        number | null;
    sellable_units:    number;
    unsellable_units:  number;
    top_reason:        string;
    last_return_at:    string;
    source_files:      string;
}

interface ReturnsOverviewPayload {
    rows:          ReturnsOverviewRow[];
    brands:        string[];
    row_count:     number;
    available:     boolean;
    window_weeks:  number;
}

function pctColor(pct: number | null): { color: string; weight: number } | null {
    if (pct == null) return null;
    if (pct >= 5) return { color: "#b91c1c", weight: 600 };
    if (pct >= 2) return { color: "#d97706", weight: 600 };
    return null;
}

export default function Returns() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const [filter, setFilter] = useDebouncedUrlParam("q");

    const selBrands     = useMemo(() => params.getAll("brands").filter(Boolean),    [qsKey]);
    const selCategories = useMemo(() => params.getAll("category").filter(Boolean),  [qsKey]);
    const selReasons    = useMemo(() => params.getAll("reason").filter(Boolean),    [qsKey]);
    const onlyWithReturns = params.get("only") === "returns";

    function setMulti(name: string, values: string[]) {
        const next = new URLSearchParams(params);
        next.delete(name);
        values.forEach((v) => next.append(name, v));
        setParams(next, { replace: false });
    }
    function toggleOnly(flag: boolean) {
        const next = new URLSearchParams(params);
        if (flag) next.set("only", "returns"); else next.delete("only");
        setParams(next, { replace: false });
    }
    // Numeric range filter helpers — each column gets a {key}_min / {key}_max pair in the URL.
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

    const q = useQuery<ReturnsOverviewPayload>({
        queryKey: ["returns-overview"],
        queryFn:  () => api.get("/api/returns-overview"),
        staleTime: 5 * 60_000,
    });

    const rows      = q.data?.rows || [];
    const brands    = q.data?.brands || [];
    const available = q.data?.available ?? false;
    const windowWeeks = q.data?.window_weeks ?? 12;

    const categories = useMemo(() => {
        const s = new Set<string>();
        for (const r of rows) if (r.category_l0) s.add(r.category_l0);
        return Array.from(s).sort();
    }, [rows]);

    const categoriesL1 = useMemo(() => {
        const s = new Set<string>();
        for (const r of rows) if (r.category_l1) s.add(r.category_l1);
        return Array.from(s).sort();
    }, [rows]);

    const reasons = useMemo(() => {
        const s = new Set<string>();
        for (const r of rows) if (r.top_reason) s.add(r.top_reason);
        return Array.from(s).sort();
    }, [rows]);

    const models = useMemo(() => {
        const s = new Set<string>();
        for (const r of rows) if (r.model) s.add(r.model);
        return Array.from(s).sort();
    }, [rows]);

    const asins = useMemo(() => {
        const s = new Set<string>();
        for (const r of rows) if (r.asin) s.add(r.asin);
        return Array.from(s).sort();
    }, [rows]);

    const selCategoriesL1 = useMemo(() => params.getAll("cat_l1").filter(Boolean), [qsKey]);
    const selModels       = useMemo(() => params.getAll("model").filter(Boolean),  [qsKey]);
    const selAsins        = useMemo(() => params.getAll("asin").filter(Boolean),   [qsKey]);

    // Numeric range filters
    const rSold   = useMemo(() => getRange("sold"),   [qsKey]);
    const rRet    = useMemo(() => getRange("ret"),    [qsKey]);
    const rPct    = useMemo(() => getRange("pct"),    [qsKey]);
    const r3p     = useMemo(() => getRange("fba"),    [qsKey]);
    const r1p     = useMemo(() => getRange("onep"),   [qsKey]);
    const rSell   = useMemo(() => getRange("sell"),   [qsKey]);
    const rUnsell = useMemo(() => getRange("unsell"), [qsKey]);

    const filtered = useMemo(() => {
        const f = filter.trim().toLowerCase();
        const bs  = new Set(selBrands);
        const cs  = new Set(selCategories);
        const c1s = new Set(selCategoriesL1);
        const rs  = new Set(selReasons);
        const ms  = new Set(selModels);
        const as_ = new Set(selAsins);
        // For Return %, treat null as -Infinity for min/max comparisons so unfiltered nulls are kept,
        // but if pct min is set, null rows are excluded (no sales → can't measure rate).
        function inRange(v: number | null, rng: { min: number | null; max: number | null }, treatNullAsZero = true): boolean {
            const num = v == null ? (treatNullAsZero ? 0 : Number.NaN) : v;
            if (rng.min == null && rng.max == null) return true;
            if (Number.isNaN(num)) return false;
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        return rows.filter((r) => {
            if (bs.size  && !bs.has(r.brand))         return false;
            if (cs.size  && !cs.has(r.category_l0))   return false;
            if (c1s.size && !c1s.has(r.category_l1)) return false;
            if (rs.size  && !rs.has(r.top_reason))   return false;
            if (ms.size  && !ms.has(r.model))         return false;
            if (as_.size && !as_.has(r.asin))         return false;
            if (onlyWithReturns && r.return_units <= 0) return false;
            if (!inRange(r.units_sold_30d,   rSold))   return false;
            if (!inRange(r.return_units,     rRet))    return false;
            if (!inRange(r.return_pct,       rPct, false)) return false;
            if (!inRange(r.returns_3p,       r3p))     return false;
            if (!inRange(r.returns_1p,       r1p))     return false;
            if (!inRange(r.sellable_units,   rSell))   return false;
            if (!inRange(r.unsellable_units, rUnsell)) return false;
            if (f) {
                const hay = `${r.model} ${r.sku} ${r.asin} ${r.top_reason}`.toLowerCase();
                if (!hay.includes(f)) return false;
            }
            return true;
        });
    }, [rows, selBrands, selCategories, selCategoriesL1, selReasons, selModels, selAsins,
        onlyWithReturns, filter, rSold, rRet, rPct, r3p, r1p, rSell, rUnsell]);

    const { sorted, sort, onSort } = useSortedRows(filtered, {
        key: "return_units",
        dir: "desc",
    });

    const stats = useMemo(() => {
        let totalReturns = 0, total3p = 0, total1p = 0, sellable = 0, unsellable = 0;
        let withReturns = 0;
        let weightedSoldForPct = 0, weightedReturnsForPct = 0;
        for (const r of filtered) {
            totalReturns += r.return_units;
            total3p      += r.returns_3p;
            total1p      += r.returns_1p;
            sellable     += r.sellable_units;
            unsellable   += r.unsellable_units;
            if (r.return_units > 0) withReturns++;
            if (r.return_pct != null) {
                weightedSoldForPct    += r.units_sold_30d;
                weightedReturnsForPct += r.return_units;
            }
        }
        const portfolioPct = weightedSoldForPct > 0
            ? +(weightedReturnsForPct / weightedSoldForPct * 100).toFixed(1)
            : 0;
        return { totalReturns, total3p, total1p, sellable, unsellable, withReturns, portfolioPct, totalAsins: filtered.length };
    }, [filtered]);

    const { isLoading, error, refetch } = q;

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Operations · Quality Signal</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <RotateCcw className="h-5 w-5 text-primary" />
                        Returns
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Per-ASIN return rates · FBA (sellable / unsellable / reason) + 1P (Vendor Central Customer Returns) ·
                        denominator is units sold over the last <strong style={{ color: "#0a0a0a" }}>{windowWeeks} weeks</strong>.
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Brands"     options={brands}     selected={selBrands}     onApply={(v) => setMulti("brands", v)} />
                <MultiPicker label="Category"   options={categories} selected={selCategories} onApply={(v) => setMulti("category", v)} />
                <MultiPicker label="Reason"     options={reasons}    selected={selReasons}    onApply={(v) => setMulti("reason", v)} />
                <label className="inline-flex items-center gap-2 text-[12.5px] cursor-pointer select-none px-3 h-9 border border-input rounded-md hover:border-foreground/30"
                       style={{ color: "#1a1a1a" }}>
                    <input type="checkbox" checked={onlyWithReturns} onChange={(e) => toggleOnly(e.target.checked)}
                           className="accent-primary" />
                    Only with returns
                </label>
            </div>

            {isLoading && <LoadingSkeleton rows={10} />}
            {error && <ErrorBlock error={error} onRetry={() => refetch()} title="Couldn't load returns data" />}

            {!isLoading && !error && !available && (
                <EmptyBlock
                    icon={<Inbox className="h-5 w-5" />}
                    title="Master file missing"
                    hint="data/master/sku_master.xlsx wasn't found. Drop it in place and refresh."
                />
            )}

            {available && (
                <>
                    {/* KPI strip */}
                    <div className="grid grid-cols-2 sm:grid-cols-6 gap-4">
                        {[
                            { label: "Total ASINs",     value: fmtInt(stats.totalAsins),    accent: "#0a0a0a" },
                            { label: "With Returns",    value: fmtInt(stats.withReturns),   accent: "#1e40af" },
                            { label: "Total Returns",   value: fmtInt(stats.totalReturns),  accent: "#b91c1c" },
                            { label: "3P (FBA)",        value: fmtInt(stats.total3p),       accent: "#0a0a0a" },
                            { label: "1P (Vendor)",     value: fmtInt(stats.total1p),       accent: "#0a0a0a" },
                            { label: "Portfolio %",     value: `${stats.portfolioPct}%`,    accent: stats.portfolioPct >= 5 ? "#b91c1c" : stats.portfolioPct >= 2 ? "#d97706" : "#0a0a0a" },
                        ].map((k) => (
                            <Card key={k.label} className="p-4">
                                <div className="text-[10.5px] font-semibold uppercase mb-2"
                                     style={{ letterSpacing: "0.14em", color: k.accent }}>
                                    {k.label}
                                </div>
                                <div className="tabular" style={{
                                    fontSize: 22, fontWeight: 600, letterSpacing: "-0.014em",
                                    lineHeight: 1.1, color: "#0a0a0a",
                                }}>
                                    {k.value}
                                </div>
                            </Card>
                        ))}
                    </div>

                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={RotateCcw}
                            iconColor="#1e40af"
                            title="Per-ASIN Returns"
                            subtitle={
                                filtered.length !== rows.length
                                    ? `${rows.length.toLocaleString()} ASINs · ${filtered.length} shown`
                                    : `${rows.length.toLocaleString()} ASINs`
                            }
                            action={
                                <>
                                    <Input
                                        placeholder="Filter Model / SKU / ASIN / Reason…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => exportToCsv(
                                            sorted as any,
                                            ["brand", "model", "sku", "asin",
                                             "category_l0", "category_l1",
                                             "units_sold_30d", "return_units",
                                             "returns_3p", "returns_1p", "return_pct",
                                             "sellable_units", "unsellable_units",
                                             "top_reason", "last_return_at"],
                                            "returns-overview.csv",
                                        )}
                                    >
                                        <Download className="h-3.5 w-3.5" />
                                        Export CSV
                                    </Button>
                                </>
                            }
                        />

                        <div className="overflow-x-auto">
                            <table className="w-full text-[13px]">
                                <thead>
                                    <tr className="bg-muted/40 border-b text-left">
                                        <SortableTh sortKey="brand"             label="Brand"        sort={sort} onSort={onSort} align="left"  stickyLeft={0}   minWidth={120}
                                            filterValues={brands}        filterSelected={selBrands}        onFilterChange={(v) => setMulti("brands", v)} />
                                        <SortableTh sortKey="model"             label="Model"        sort={sort} onSort={onSort} align="left"  stickyLeft={120} minWidth={140}
                                            filterValues={models}        filterSelected={selModels}        onFilterChange={(v) => setMulti("model", v)} />
                                        <SortableTh sortKey="asin"              label="ASIN"         sort={sort} onSort={onSort} align="left"  stickyLeft={260} minWidth={120} lastFrozen
                                            filterValues={asins}         filterSelected={selAsins}         onFilterChange={(v) => setMulti("asin", v)} />
                                        <SortableTh sortKey="category_l0"       label="Category L0"  sort={sort} onSort={onSort} align="left"  minWidth={150}
                                            filterValues={categories}    filterSelected={selCategories}    onFilterChange={(v) => setMulti("category", v)} />
                                        <SortableTh sortKey="category_l1"       label="Category L1"  sort={sort} onSort={onSort} align="left"  minWidth={160}
                                            filterValues={categoriesL1}  filterSelected={selCategoriesL1}  onFilterChange={(v) => setMulti("cat_l1", v)} />
                                        <SortableTh sortKey="units_sold_30d"    label="Sold Qty"     sort={sort} onSort={onSort} className="col-units col-divide-l" minWidth={120}
                                            numericRange={rSold}   onNumericFilter={(r) => setRange("sold",   r)} numericPresets={[1, 10, 50]} />
                                        <SortableTh sortKey="return_units"      label="Returns"      sort={sort} onSort={onSort} className="col-conv col-divide-l"  minWidth={120}
                                            numericRange={rRet}    onNumericFilter={(r) => setRange("ret",    r)} numericPresets={[1, 5, 10]} />
                                        <SortableTh sortKey="return_pct"        label="Return %"     sort={sort} onSort={onSort} className="col-conv"                minWidth={130}
                                            numericRange={rPct}    onNumericFilter={(r) => setRange("pct",    r)} numericSuffix="%" numericPresets={[2, 5, 10]} />
                                        <SortableTh sortKey="returns_3p"        label="3P (FBA)"     sort={sort} onSort={onSort} className="col-sales col-divide-l" minWidth={110}
                                            numericRange={r3p}     onNumericFilter={(r) => setRange("fba",    r)} numericPresets={[1, 5, 10]} />
                                        <SortableTh sortKey="returns_1p"        label="1P"           sort={sort} onSort={onSort} className="col-sales"               minWidth={100}
                                            numericRange={r1p}     onNumericFilter={(r) => setRange("onep",   r)} numericPresets={[1, 5, 10]} />
                                        <SortableTh sortKey="sellable_units"    label="Sellable"     sort={sort} onSort={onSort} className="col-pct col-divide-l"   minWidth={120}
                                            numericRange={rSell}   onNumericFilter={(r) => setRange("sell",   r)} numericPresets={[1, 5, 10]} />
                                        <SortableTh sortKey="unsellable_units"  label="Unsellable"   sort={sort} onSort={onSort} className="col-pct"                 minWidth={130}
                                            numericRange={rUnsell} onNumericFilter={(r) => setRange("unsell", r)} numericPresets={[1, 5, 10]} />
                                        <SortableTh sortKey="top_reason"        label="Top Reason"   sort={sort} onSort={onSort} align="left" className="col-summary col-divide-l" minWidth={200}
                                            filterValues={reasons}       filterSelected={selReasons}       onFilterChange={(v) => setMulti("reason", v)} />
                                    </tr>
                                </thead>
                                <tbody>
                                    {sorted.length === 0 ? (
                                        <tr><td colSpan={13} className="px-4 py-10 text-center text-[13px]" style={{ color: "#6b7280" }}>
                                            No rows match the current filters.
                                        </td></tr>
                                    ) : sorted.map((r) => {
                                        const pcl = pctColor(r.return_pct);
                                        return (
                                            <tr key={`${r.asin}::${r.sku}`} className="border-b hover:bg-muted/20 group">
                                                <td style={{ position: "sticky", left: 0 }}   className="px-3 py-2 border-b z-10 bg-white">{r.brand || "—"}</td>
                                                <td style={{ position: "sticky", left: 120 }} className="px-3 py-2 font-medium border-b z-10 bg-white">{r.model || "—"}</td>
                                                <td style={{ position: "sticky", left: 260 }} className="px-3 py-2 border-b z-10 bg-white border-r-2 border-r-border" title={r.sku || undefined}>{r.asin}</td>
                                                <td className="px-3 py-2 border-b">{r.category_l0 || "—"}</td>
                                                <td className="px-3 py-2 border-b">{r.category_l1 || "—"}</td>
                                                <td className="px-3 py-2 text-right tabular border-b col-units col-divide-l">
                                                    {r.units_sold_30d > 0 ? fmtInt(r.units_sold_30d) : "—"}
                                                </td>
                                                <td
                                                    className="px-3 py-2 text-right tabular border-b col-conv col-divide-l"
                                                    style={r.return_units > 0 ? { color: "#b91c1c", fontWeight: 600 } : undefined}
                                                >
                                                    {r.return_units > 0 ? fmtInt(r.return_units) : "—"}
                                                </td>
                                                <td
                                                    className="px-3 py-2 text-right tabular border-b col-conv"
                                                    style={pcl ? { color: pcl.color, fontWeight: pcl.weight } : undefined}
                                                    title={r.return_pct != null
                                                        ? `${r.return_units} returned of ${r.units_sold_30d} sold (last ${windowWeeks} weeks)`
                                                        : (r.return_units > 0 ? `No sales in last ${windowWeeks} weeks` : undefined)}
                                                >
                                                    {r.return_pct != null
                                                        ? `${r.return_pct.toFixed(1)}%`
                                                        : (r.return_units > 0 ? "n/a" : "—")}
                                                </td>
                                                <td className="px-3 py-2 text-right tabular border-b col-sales col-divide-l">
                                                    {r.returns_3p > 0 ? fmtInt(r.returns_3p) : "—"}
                                                </td>
                                                <td className="px-3 py-2 text-right tabular border-b col-sales">
                                                    {r.returns_1p > 0 ? fmtInt(r.returns_1p) : "—"}
                                                </td>
                                                <td className="px-3 py-2 text-right tabular border-b col-pct col-divide-l" style={r.sellable_units > 0 ? { color: "#047857" } : undefined}>
                                                    {r.sellable_units > 0 ? fmtInt(r.sellable_units) : "—"}
                                                </td>
                                                <td className="px-3 py-2 text-right tabular border-b col-pct" style={r.unsellable_units > 0 ? { color: "#b91c1c", fontWeight: 500 } : undefined}>
                                                    {r.unsellable_units > 0 ? fmtInt(r.unsellable_units) : "—"}
                                                </td>
                                                <td className="px-3 py-2 border-b col-summary col-divide-l text-[12.5px]" style={{ color: "#4b5563" }}>
                                                    {r.top_reason || "—"}
                                                </td>
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
