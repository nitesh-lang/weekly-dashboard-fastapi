import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { fmtInt, fmtINR, exportToXlsx, copyTableToClipboard } from "@/lib/utils";
import { useSortedRows } from "@/lib/useSortedRows";
import { makeTextFilter } from "@/lib/useTextFilter";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import { useAmsPlanning, type PlanningRow } from "@/lib/useAmsPlanning";
import { useReturns } from "@/lib/useReturns";
import { useMargins } from "@/lib/useMargins";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { FilterChipStrip, type FilterChipGroup } from "@/components/FilterChipStrip";
import { SortableTh } from "@/components/SortableTh";
import { AsinLink } from "@/components/AsinLink";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock, EmptyBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Download, ClipboardList, Inbox, Copy, Check } from "lucide-react";

const STATUS_STYLES: Record<string, { color: string; bg: string }> = {
    "Out of Stock":  { color: "#b91c1c", bg: "#fef2f2" },
    "Low":           { color: "#9a3412", bg: "#fff7ed" },
    "Healthy":       { color: "#065f46", bg: "#ecfdf5" },
    "Overstocked":   { color: "#1e3a8a", bg: "#eff5ff" },
};

/** Inline-editable cell that saves on blur (or Enter) via the saveNote callback. */
function EditableCell({
    value,
    onSave,
    placeholder,
}: {
    value: string;
    onSave: (next: string) => void;
    placeholder?: string;
}) {
    const [local, setLocal] = useState(value);
    const [savedFlash, setSavedFlash] = useState(false);
    const initial = useRef(value);

    // Sync down if cache update brings a new value (e.g. another tab edited).
    useEffect(() => {
        setLocal(value);
        initial.current = value;
    }, [value]);

    function commit() {
        const trimmed = local.trim();
        if (trimmed === (initial.current || "").trim()) return;   // no-op
        onSave(trimmed);
        initial.current = trimmed;
        setSavedFlash(true);
        setTimeout(() => setSavedFlash(false), 600);
    }

    return (
        <div className="relative">
            <input
                value={local}
                onChange={(e) => setLocal(e.target.value)}
                onBlur={commit}
                onKeyDown={(e) => {
                    if (e.key === "Enter") (e.target as HTMLInputElement).blur();
                    if (e.key === "Escape") {
                        setLocal(initial.current);
                        (e.target as HTMLInputElement).blur();
                    }
                }}
                placeholder={placeholder || "—"}
                className="w-full text-[13px] px-2 py-1 bg-transparent border border-transparent hover:border-border focus:border-primary focus:bg-white outline-none rounded-sm"
                style={{ minWidth: 120 }}
            />
            {savedFlash && (
                <span
                    className="absolute right-1 top-1/2 -translate-y-1/2 pointer-events-none text-[10px] font-medium"
                    style={{ color: "#059669" }}
                >
                    ✓
                </span>
            )}
        </div>
    );
}

export default function AmsPlanning() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const [filter, setFilter] = useDebouncedUrlParam("q");
    const selBrands     = useMemo(() => params.getAll("brands").filter(Boolean),     [qsKey]);
    const selAsinTypes  = useMemo(() => params.getAll("asin_type").filter(Boolean),  [qsKey]);
    const selStatuses   = useMemo(() => params.getAll("status").filter(Boolean),     [qsKey]);

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

    const { rows, brands, available, isLoading, error, refetch, saveNote } = useAmsPlanning();
    const { lookupByAsin: returnsByAsin, lookupBySku: returnsBySku } = useReturns();
    const { lookupByModel: marginByModel } = useMargins();

    // Extra URL state for the in-header column filters
    const selModels       = useMemo(() => params.getAll("model").filter(Boolean),       [qsKey]);
    const selAsins        = useMemo(() => params.getAll("asin").filter(Boolean),        [qsKey]);
    const selCategories   = useMemo(() => params.getAll("category").filter(Boolean),    [qsKey]);
    const selCategoriesL1 = useMemo(() => params.getAll("cat_l1").filter(Boolean),      [qsKey]);
    const selAmsReq       = useMemo(() => params.getAll("ams_required").filter(Boolean),[qsKey]);
    const selVariations   = useMemo(() => params.getAll("variation").filter(Boolean),   [qsKey]);
    const rBau         = useMemo(() => getRange("bau"),          [qsKey]);
    const rSoh         = useMemo(() => getRange("soh"),          [qsKey]);
    const rAmSoh       = useMemo(() => getRange("am_soh"),       [qsKey]);
    const rAmIntransit = useMemo(() => getRange("am_intransit"), [qsKey]);
    const rTotalStock  = useMemo(() => getRange("total_stock"),  [qsKey]);
    const rRet         = useMemo(() => getRange("ret"),          [qsKey]);
    const rAvgRating   = useMemo(() => getRange("avg_rating"),   [qsKey]);
    const rRatingCount = useMemo(() => getRange("rating_count"), [qsKey]);
    const rNetMargin   = useMemo(() => getRange("net_margin"),   [qsKey]);
    const rNetMarginP  = useMemo(() => getRange("net_pct"),      [qsKey]);
    const rPct   = useMemo(() => getRange("pct"),     [qsKey]);

    // Distinct values for pickers — derived from loaded rows, post-brand filter for context.
    const asinTypes = useMemo(() => {
        const s = new Set<string>();
        for (const r of rows) if (r.asin_type) s.add(r.asin_type);
        return Array.from(s).sort();
    }, [rows]);

    const statuses = useMemo(() => {
        const s = new Set<string>();
        for (const r of rows) if (r.inventory_status) s.add(r.inventory_status);
        return Array.from(s).sort();
    }, [rows]);

    // Distinct values for in-header column filters
    const models       = useMemo(() => Array.from(new Set(rows.map((r) => r.model).filter(Boolean))).sort(), [rows]);
    const asinsList    = useMemo(() => Array.from(new Set(rows.map((r) => r.asin).filter(Boolean))).sort(), [rows]);
    const categoriesL0 = useMemo(() => Array.from(new Set(rows.map((r) => r.category_l0).filter(Boolean))).sort(), [rows]);
    const categoriesL1 = useMemo(() => Array.from(new Set(rows.map((r) => r.category_l1).filter(Boolean))).sort(), [rows]);
    const amsReqValues = useMemo(() => Array.from(new Set(rows.map((r) => r.ams_required).filter(Boolean))).sort(), [rows]);
    const variations   = useMemo(() => Array.from(new Set(rows.map((r) => r.variation).filter(Boolean))).sort(), [rows]);

    // Inject returns data per row (looked up by ASIN, SKU fallback) so sort + filter work natively.
    // Also pre-compute total_stock = SOH + AM Intransit + AM SOH so the column
    // can be sorted / filtered without re-deriving on every render.
    const rowsWithReturns = useMemo(
        () => rows.map((r) => {
            const ret = returnsByAsin(r.asin) || returnsBySku(r.sku);
            const mgn = marginByModel(r.model);
            return {
                ...r,
                return_units:       ret ? ret.return_units : 0,
                return_pct:         ret && ret.return_pct != null ? ret.return_pct : null,
                units_sold_30d:     ret ? ret.units_sold_30d : 0,
                top_return_reason:  ret ? ret.top_reason : "",
                total_stock:        (r.soh || 0) + (r.am_intransit || 0) + (r.am_soh || 0),
                net_margin:         mgn?.net_margin     ?? null,
                net_margin_pct:     mgn?.net_margin_pct ?? null,
            };
        }),
        [rows, returnsByAsin, returnsBySku, marginByModel],
    );

    const filtered = useMemo(() => {
        const matchText = makeTextFilter<typeof rowsWithReturns[number]>(filter, [
            "brand", "model", "sku", "asin",
            "category_l0", "category_l1",
            "asin_type", "ams_required", "remarks",
        ]);
        const b   = new Set(selBrands);
        const t   = new Set(selAsinTypes);
        const st  = new Set(selStatuses);
        const ms  = new Set(selModels);
        const as_ = new Set(selAsins);
        const cs  = new Set(selCategories);
        const c1s = new Set(selCategoriesL1);
        const ams = new Set(selAmsReq);
        const vs  = new Set(selVariations);
        function inRange(v: number | null | undefined, rng: { min: number | null; max: number | null }): boolean {
            const num = v == null ? 0 : Number(v);
            if (rng.min == null && rng.max == null) return true;
            if (!Number.isFinite(num)) return false;
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        return rowsWithReturns.filter((r) => {
            if (b.size   && !b.has(r.brand))             return false;
            if (t.size   && !t.has(r.asin_type))         return false;
            if (st.size  && !st.has(r.inventory_status)) return false;
            if (ms.size  && !ms.has(r.model))            return false;
            if (as_.size && !as_.has(r.asin))            return false;
            if (cs.size  && !cs.has(r.category_l0))      return false;
            if (c1s.size && !c1s.has(r.category_l1))     return false;
            if (ams.size && !ams.has(r.ams_required))    return false;
            if (vs.size  && !vs.has(r.variation))        return false;
            if (!inRange(r.bau == null || r.bau === "" ? null : Number(r.bau), rBau)) return false;
            if (!inRange(r.soh, rSoh))                   return false;
            if (!inRange(r.am_intransit, rAmIntransit)) return false;
            if (!inRange(r.am_soh, rAmSoh))              return false;
            if (!inRange((r as any).total_stock, rTotalStock)) return false;
            if (!inRange((r as any).return_units, rRet)) return false;
            if (!inRange((r as any).return_pct, rPct))   return false;
            if (!inRange(r.avg_rating,    rAvgRating))   return false;
            if (!inRange(r.rating_count,  rRatingCount)) return false;
            if (!inRange((r as any).net_margin,     rNetMargin))  return false;
            if (!inRange((r as any).net_margin_pct, rNetMarginP)) return false;
            return matchText(r);
        });
    }, [rowsWithReturns, filter, selBrands, selAsinTypes, selStatuses,
        selModels, selAsins, selCategories, selCategoriesL1, selAmsReq, selVariations,
        rBau, rSoh, rAmSoh, rAmIntransit, rTotalStock, rRet, rPct, rAvgRating, rRatingCount,
        rNetMargin, rNetMarginP]);

    // Default sort: Out of Stock + Low at the top so the operator sees the
    // urgent decisions first.  Tie-break by SOH descending.
    const STATUS_PRIORITY: Record<string, number> = {
        "Out of Stock": 0,
        "Low":          1,
        "Overstocked":  2,
        "Healthy":      3,
    };
    const initialSorted = useMemo(
        () => [...filtered].sort((a, b) => {
            const pa = STATUS_PRIORITY[a.inventory_status] ?? 99;
            const pb = STATUS_PRIORITY[b.inventory_status] ?? 99;
            if (pa !== pb) return pa - pb;
            return (b.soh || 0) - (a.soh || 0);
        }),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [filtered],
    );
    const { sorted, sort, onSort } = useSortedRows<PlanningRow>(initialSorted);

    // KPI strip — operator at-a-glance.
    const stats = useMemo(() => {
        let oos = 0, low = 0, over = 0, healthy = 0;
        for (const r of filtered) {
            if (r.inventory_status === "Out of Stock") oos++;
            else if (r.inventory_status === "Low") low++;
            else if (r.inventory_status === "Overstocked") over++;
            else if (r.inventory_status === "Healthy") healthy++;
        }
        return { total: filtered.length, oos, low, over, healthy };
    }, [filtered]);

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Advertising · Planning</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <ClipboardList className="h-5 w-5 text-primary" />
                        AMS Planning
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Per-ASIN planning grid · stock-on-hand from AMPM · edit
                        <strong style={{ color: "#0a0a0a" }}> AMS Required </strong>and
                        <strong style={{ color: "#0a0a0a" }}> Remarks </strong>inline (saves on blur).
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Brands"     options={brands}    selected={selBrands}    onApply={(v) => setMulti("brands", v)} />
                <MultiPicker label="ASIN Type"  options={asinTypes} selected={selAsinTypes} onApply={(v) => setMulti("asin_type", v)} />
                <MultiPicker label="Status"     options={statuses}  selected={selStatuses}  onApply={(v) => setMulti("status", v)} />
            </div>

            <FilterChipStrip
                filters={[
                    { label: "Brand",     values: selBrands,    onRemove: (v) => setMulti("brands",    selBrands.filter((x) => x !== v)),    onClear: () => setMulti("brands", []) },
                    { label: "ASIN Type", values: selAsinTypes, onRemove: (v) => setMulti("asin_type", selAsinTypes.filter((x) => x !== v)), onClear: () => setMulti("asin_type", []) },
                    { label: "Status",    values: selStatuses,  onRemove: (v) => setMulti("status",    selStatuses.filter((x) => x !== v)),  onClear: () => setMulti("status", []) },
                ] as FilterChipGroup[]}
            />

            {isLoading && <LoadingSkeleton rows={10} />}
            {error && <ErrorBlock error={error} onRetry={() => refetch()} title="Couldn't load planning data" />}

            {!isLoading && !error && !available && (
                <EmptyBlock
                    icon={<Inbox className="h-5 w-5" />}
                    title="Master file missing"
                    hint="data/master/sku_master.xlsx wasn't found. Drop it in place and refresh."
                />
            )}

            {available && (
                <>
                    {/* KPI strip — counts by inventory status */}
                    <div className="grid grid-cols-2 sm:grid-cols-5 gap-4">
                        {[
                            { label: "Total ASINs",  value: fmtInt(stats.total),   accent: "#0a0a0a", color: "#0a0a0a" },
                            { label: "Out of Stock", value: fmtInt(stats.oos),     accent: "#b91c1c", color: stats.oos > 0 ? "#b91c1c" : "#0a0a0a" },
                            { label: "Low",          value: fmtInt(stats.low),     accent: "#9a3412", color: stats.low > 0 ? "#9a3412" : "#0a0a0a" },
                            { label: "Healthy",      value: fmtInt(stats.healthy), accent: "#065f46", color: "#0a0a0a" },
                            { label: "Overstocked",  value: fmtInt(stats.over),    accent: "#1e3a8a", color: "#0a0a0a" },
                        ].map((k) => (
                            <Card key={k.label} className="p-4">
                                <div className="text-[10.5px] font-semibold uppercase mb-2"
                                     style={{ letterSpacing: "0.14em", color: k.accent }}>
                                    {k.label}
                                </div>
                                <div className="tabular" style={{
                                    fontSize: 22, fontWeight: 600, letterSpacing: "-0.014em",
                                    lineHeight: 1.1, color: k.color,
                                }}>
                                    {k.value}
                                </div>
                            </Card>
                        ))}
                    </div>

                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={ClipboardList}
                            iconColor="#1e40af"
                            title="Per-ASIN Planning"
                            subtitle={
                                filtered.length !== rows.length
                                    ? `${rows.length.toLocaleString()} ASINs · ${filtered.length} shown`
                                    : `${rows.length.toLocaleString()} ASINs`
                            }
                            action={
                                <>
                                    <Input
                                        placeholder="Filter Model / SKU / ASIN / Remarks…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => exportToXlsx(
                                            sorted as any,
                                            ["brand", "model", "bau", "asin",
                                             "category_l0", "category_l1",
                                             "asin_type", "soh", "am_intransit", "am_soh", "total_stock", "inventory_status",
                                             "return_units", "units_sold_30d", "return_pct", "top_return_reason",
                                             "avg_rating", "rating_count",
                                             "net_margin", "net_margin_pct",
                                             "ams_required", "remarks",
                                             "variation", "variation_asins"],
                                            "ams-planning.xlsx",
                                        )}
                                    >
                                        <Download className="h-3.5 w-3.5" />
                                        Export
                                    </Button>
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => copyTableToClipboard(sorted as any, ["brand", "model", "bau", "asin",
                                             "category_l0", "category_l1",
                                             "asin_type", "soh", "am_intransit", "am_soh", "total_stock", "inventory_status",
                                             "return_units", "units_sold_30d", "return_pct", "top_return_reason",
                                             "avg_rating", "rating_count",
                                             "net_margin", "net_margin_pct",
                                             "ams_required", "remarks",
                                             "variation", "variation_asins"])}
                                    >
                                        <Copy className="h-3.5 w-3.5" />
                                        Copy
                                    </Button>
                                </>
                            }
                        />

                        {sorted.length === 0 ? (
                            <EmptyBlock
                                title="No ASINs match the current filter"
                                hint="Adjust the filter inputs above to widen the view."
                            />
                        ) : (
                            <div className="overflow-auto max-h-[78vh]">
                                <table className="text-[13px] border-separate border-spacing-0">
                                    <thead className="sticky top-0 z-30">
                                        <tr>
                                            <SortableTh sortKey="brand" label="Brand" sort={sort} onSort={onSort} align="left" stickyLeft={0}   minWidth={110}
                                                filterValues={brands}     filterSelected={selBrands}    onFilterChange={(v) => setMulti("brands", v)} />
                                            <SortableTh sortKey="model" label="Model" sort={sort} onSort={onSort} align="left" stickyLeft={110} minWidth={140}
                                                filterValues={models}     filterSelected={selModels}    onFilterChange={(v) => setMulti("model", v)} />
                                            <SortableTh sortKey="asin"  label="ASIN"  sort={sort} onSort={onSort} align="left" stickyLeft={250} minWidth={120} lastFrozen
                                                filterValues={asinsList}  filterSelected={selAsins}     onFilterChange={(v) => setMulti("asin", v)} />
                                            <SortableTh sortKey="bau"          label="BAU"          sort={sort} onSort={onSort} className="col-sales col-divide-l" minWidth={110}
                                                numericRange={rBau}  onNumericFilter={(r) => setRange("bau", r)} numericPresets={[100, 500, 1000]} />
                                            <SortableTh sortKey="category_l0"  label="Category L0"  sort={sort} onSort={onSort} align="left" minWidth={140}
                                                filterValues={categoriesL0} filterSelected={selCategories}   onFilterChange={(v) => setMulti("category", v)} />
                                            <SortableTh sortKey="category_l1"  label="Category L1"  sort={sort} onSort={onSort} align="left" minWidth={150}
                                                filterValues={categoriesL1} filterSelected={selCategoriesL1} onFilterChange={(v) => setMulti("cat_l1", v)} />
                                            <SortableTh sortKey="asin_type"    label="ASIN Type"    sort={sort} onSort={onSort} align="left" minWidth={100}
                                                filterValues={asinTypes}    filterSelected={selAsinTypes}    onFilterChange={(v) => setMulti("asin_type", v)} />
                                            <SortableTh sortKey="soh"          label="SOH"          sort={sort} onSort={onSort} className="col-units col-divide-l" minWidth={100}
                                                numericRange={rSoh}  onNumericFilter={(r) => setRange("soh", r)} numericPresets={[1, 50, 500]} />
                                            <SortableTh sortKey="am_intransit" label="AM Intransit" sort={sort} onSort={onSort} className="col-units"            minWidth={120}
                                                numericRange={rAmIntransit} onNumericFilter={(r) => setRange("am_intransit", r)} numericPresets={[1, 50, 500]} />
                                            <SortableTh sortKey="am_soh"       label="AM SOH"       sort={sort} onSort={onSort} className="col-units"            minWidth={110}
                                                numericRange={rAmSoh}        onNumericFilter={(r) => setRange("am_soh", r)}      numericPresets={[1, 50, 500]} />
                                            <SortableTh sortKey="total_stock"  label="Total Stock"  sort={sort} onSort={onSort} className="col-summary col-divide-l" minWidth={120}
                                                numericRange={rTotalStock}   onNumericFilter={(r) => setRange("total_stock", r)} numericPresets={[1, 100, 1000]} />
                                            <SortableTh sortKey="inventory_status" label="Status" sort={sort} onSort={onSort} align="left" minWidth={130}
                                                filterValues={statuses}     filterSelected={selStatuses}     onFilterChange={(v) => setMulti("status", v)} />
                                            <SortableTh sortKey="return_units" label="Returns"      sort={sort} onSort={onSort} className="col-conv col-divide-l" minWidth={100}
                                                numericRange={rRet}  onNumericFilter={(r) => setRange("ret", r)} numericPresets={[1, 5, 10]} />
                                            <SortableTh sortKey="return_pct"   label="Return %"     sort={sort} onSort={onSort} className="col-conv" minWidth={110}
                                                numericRange={rPct}  onNumericFilter={(r) => setRange("pct", r)} numericSuffix="%" numericPresets={[2, 5, 10]} />
                                            <SortableTh sortKey="avg_rating"   label="Avg Rating"   sort={sort} onSort={onSort} className="col-summary col-divide-l" minWidth={110}
                                                numericRange={rAvgRating}   onNumericFilter={(r) => setRange("avg_rating", r)}   numericPresets={[3, 4, 4.5]} />
                                            <SortableTh sortKey="rating_count" label="# Reviews"    sort={sort} onSort={onSort} className="col-summary" minWidth={110}
                                                numericRange={rRatingCount} onNumericFilter={(r) => setRange("rating_count", r)} numericPresets={[10, 100, 1000]} />
                                            <SortableTh sortKey="net_margin"     label="Net ₹"  sort={sort} onSort={onSort} className="col-sales col-divide-l" minWidth={110}
                                                numericRange={rNetMargin}  onNumericFilter={(r) => setRange("net_margin", r)} numericPresets={[0, 100, 500]} />
                                            <SortableTh sortKey="net_margin_pct" label="Net %"  sort={sort} onSort={onSort} className="col-sales" minWidth={100}
                                                numericRange={rNetMarginP} onNumericFilter={(r) => setRange("net_pct", r)}    numericSuffix="%" numericPresets={[0, 10, 25]} />
                                            <SortableTh sortKey="ams_required" label="AMS Required" sort={sort} onSort={onSort} align="left" className="col-pct col-divide-l" minWidth={150}
                                                filterValues={amsReqValues} filterSelected={selAmsReq}       onFilterChange={(v) => setMulti("ams_required", v)} />
                                            <SortableTh sortKey="remarks"      label="Remarks"      sort={sort} onSort={onSort} align="left" minWidth={200} />
                                            <SortableTh sortKey="variation"    label="Variation"       sort={sort} onSort={onSort} align="left" className="col-summary col-divide-l" minWidth={100}
                                                filterValues={variations}   filterSelected={selVariations}   onFilterChange={(v) => setMulti("variation", v)} />
                                            <SortableTh sortKey="variation_asins" label="Variation ASINs" sort={sort} onSort={onSort} align="left" className="col-summary" minWidth={180} />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sorted.map((r) => {
                                            const statusStyle = STATUS_STYLES[r.inventory_status] || { color: "#374151", bg: "#f3f4f6" };
                                            return (
                                                <tr key={r.sku} className="group">
                                                    <td style={{ position: "sticky", left: 0 }}   className="px-3 py-2 border-b z-10">{r.brand}</td>
                                                    <td style={{ position: "sticky", left: 110 }} className="px-3 py-2 font-medium border-b z-10">{r.model}</td>
                                                    <td style={{ position: "sticky", left: 250 }} className="px-3 py-2 border-b z-10 border-r-2 border-r-border" title={r.sku}><AsinLink asin={r.asin} /></td>
                                                    <td className="px-3 py-2 text-right tabular border-b col-sales col-divide-l">
                                                        {r.bau != null && r.bau !== "" ? fmtINR(Number(r.bau)) : "—"}
                                                    </td>
                                                    <td className="px-3 py-2 border-b">{r.category_l0 || "—"}</td>
                                                    <td className="px-3 py-2 border-b">{r.category_l1 || "—"}</td>
                                                    <td className="px-3 py-2 border-b">{r.asin_type || "—"}</td>
                                                    <td className="px-3 py-2 text-right tabular border-b col-units col-divide-l font-medium">{fmtInt(r.soh)}</td>
                                                    <td className="px-3 py-2 text-right tabular border-b col-units"
                                                        style={r.am_intransit > 0 ? { color: "#1e40af", fontWeight: 500 } : undefined}>
                                                        {r.am_intransit > 0 ? fmtInt(r.am_intransit) : "—"}
                                                    </td>
                                                    <td className="px-3 py-2 text-right tabular border-b col-units"
                                                        style={r.am_soh > 0 ? { color: "#047857", fontWeight: 500 } : undefined}>
                                                        {r.am_soh > 0 ? fmtInt(r.am_soh) : "—"}
                                                    </td>
                                                    <td className="px-3 py-2 text-right tabular border-b col-summary col-divide-l font-semibold"
                                                        title="SOH (AMPM) + AM Intransit + AM SOH">
                                                        {fmtInt((r as any).total_stock || 0)}
                                                    </td>
                                                    <td className="px-3 py-2 border-b">
                                                        <span
                                                            className="inline-flex items-center px-2 py-0.5 rounded-md text-[12px] font-semibold"
                                                            style={{ color: statusStyle.color, background: statusStyle.bg }}
                                                        >
                                                            {r.inventory_status}
                                                        </span>
                                                    </td>
                                                    <td
                                                        className="px-3 py-2 text-right tabular border-b col-conv col-divide-l"
                                                        style={(r as any).return_units > 0 ? { color: "#b91c1c", fontWeight: 600 } : undefined}
                                                        title={(r as any).top_return_reason ? `Top reason: ${(r as any).top_return_reason}` : undefined}
                                                    >
                                                        {(r as any).return_units > 0 ? fmtInt((r as any).return_units) : "—"}
                                                    </td>
                                                    {(() => {
                                                        const pct = (r as any).return_pct as number | null;
                                                        const sold = (r as any).units_sold_30d as number;
                                                        let color: string | undefined;
                                                        let weight: number | undefined;
                                                        if (pct != null) {
                                                            if (pct >= 5)      { color = "#b91c1c"; weight = 600; }
                                                            else if (pct >= 2) { color = "#d97706"; weight = 600; }
                                                        }
                                                        const display = pct == null
                                                            ? ((r as any).return_units > 0 ? "n/a" : "—")
                                                            : `${pct.toFixed(1)}%`;
                                                        const titleText = pct != null
                                                            ? `${(r as any).return_units} returned of ${sold} sold (last 12 weeks)`
                                                            : ((r as any).return_units > 0 ? "No sales in last 12 weeks" : undefined);
                                                        return (
                                                            <td
                                                                className="px-3 py-2 text-right tabular border-b col-conv"
                                                                style={color ? { color, fontWeight: weight } : undefined}
                                                                title={titleText}
                                                            >
                                                                {display}
                                                            </td>
                                                        );
                                                    })()}
                                                    {/* Avg Rating — colour-codes by health band (≥4.0 healthy, 3.5-4.0 amber, <3.5 red) */}
                                                    <td
                                                        className="px-3 py-2 text-right tabular border-b col-summary col-divide-l"
                                                        style={(() => {
                                                            const v = r.avg_rating;
                                                            if (v == null) return undefined;
                                                            if (v < 3.5) return { color: "#b91c1c", fontWeight: 600 };
                                                            if (v < 4.0) return { color: "#d97706", fontWeight: 600 };
                                                            return { color: "#047857", fontWeight: 500 };
                                                        })()}
                                                    >
                                                        {r.avg_rating != null ? r.avg_rating.toFixed(1) : "—"}
                                                    </td>
                                                    {/* Review count — locale-formatted, mute when missing */}
                                                    <td className="px-3 py-2 text-right tabular border-b col-summary">
                                                        {r.rating_count != null ? fmtInt(r.rating_count) : "—"}
                                                    </td>
                                                    {/* Net margin (₹) — joined by model via /api/margins.  Coloured by the
                                                        % band: loss red, low-margin amber, healthy green.  Missing = "—". */}
                                                    {(() => {
                                                        const nm   = (r as any).net_margin     as number | null;
                                                        const npct = (r as any).net_margin_pct as number | null;
                                                        const style = npct == null
                                                            ? undefined
                                                            : npct < 0  ? { color: "#b91c1c", fontWeight: 600 }
                                                            : npct < 10 ? { color: "#9a3412", fontWeight: 600 }
                                                            :             { color: "#047857", fontWeight: 600 };
                                                        return (
                                                            <>
                                                                <td className="px-3 py-2 text-right tabular border-b col-sales col-divide-l" style={style}>
                                                                    {nm != null ? fmtINR(nm) : "—"}
                                                                </td>
                                                                <td className="px-3 py-2 text-right tabular border-b col-sales" style={style}>
                                                                    {npct != null ? `${npct.toFixed(1)}%` : "—"}
                                                                </td>
                                                            </>
                                                        );
                                                    })()}
                                                    <td className="px-1 py-1 border-b col-pct col-divide-l">
                                                        <EditableCell
                                                            value={r.ams_required}
                                                            placeholder="Yes / No / Pause / …"
                                                            onSave={(v) => saveNote({ sku: r.sku, ams_required: v })}
                                                        />
                                                    </td>
                                                    <td className="px-1 py-1 border-b">
                                                        <EditableCell
                                                            value={r.remarks}
                                                            placeholder="Free-text notes"
                                                            onSave={(v) => saveNote({ sku: r.sku, remarks: v })}
                                                        />
                                                    </td>
                                                    <td className="px-3 py-2 border-b col-summary col-divide-l">{r.variation || "—"}</td>
                                                    <td className="px-3 py-2 border-b col-summary" style={{ fontSize: 12 }}>{r.variation_asins || "—"}</td>
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
