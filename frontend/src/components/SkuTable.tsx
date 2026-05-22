import { useDeferredValue, useMemo, useRef, useState } from "react";
import {
    flexRender, getCoreRowModel, getSortedRowModel, useReactTable,
    type ColumnDef, type SortingState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowUp, ArrowDown, ArrowUpDown, Download, Package } from "lucide-react";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { SectionHeader } from "@/components/SectionHeader";
import { ColumnFilter } from "@/components/ColumnFilter";
import { NumberRangeFilter } from "@/components/NumberRangeFilter";
import { fmtINR, fmtInt, exportToCsv } from "@/lib/utils";
import { useMargins } from "@/lib/useMargins";
import { cn } from "@/lib/utils";

interface SkuRow {
    sku: string;
    net_margin_pct?: number | null;
    model_no?: string;
    asin?: string;
    category_l0?: string;
    total_units: number;
    total_sales: number;
    sales_contribution_pct: number;
    amazon_am_units: number;  amazon_am_sales: number;
    amazon_1p_units: number;  amazon_1p_sales: number;
    amazon_total_units: number; amazon_total_sales: number;
}

const FROZEN_COUNT = 4;  // SKU, Model, ASIN, Category
// Left offsets so each frozen header/cell stacks correctly when scrolling right.
const FROZEN_LEFT_PX = [0, 120, 240, 360];
const FROZEN_WIDTH   = ["min-w-[120px]", "min-w-[120px]", "min-w-[120px]", "min-w-[170px]"];

// Filter config keyed by column id (matches accessorKey).
// kind: "cat" → categorical multi-select; "num" → numeric range with presets.
type FilterCfg =
    | { kind: "cat" }
    | { kind: "num"; suffix?: string; presets?: number[] };

const COLUMN_FILTERS: Record<string, FilterCfg> = {
    sku:                     { kind: "cat" },
    model_no:                { kind: "cat" },
    asin:                    { kind: "cat" },
    category_l0:             { kind: "cat" },
    total_units:             { kind: "num", presets: [1, 50, 500] },
    total_sales:             { kind: "num", presets: [1000, 10000, 100000] },
    sales_contribution_pct:  { kind: "num", suffix: "%", presets: [1, 5, 10] },
    net_margin_pct:          { kind: "num", suffix: "%", presets: [0, 10, 25] },
    amazon_am_units:         { kind: "num", presets: [1, 50, 500] },
    amazon_am_sales:         { kind: "num", presets: [1000, 10000, 100000] },
    amazon_1p_units:         { kind: "num", presets: [1, 50, 500] },
    amazon_1p_sales:         { kind: "num", presets: [1000, 10000, 100000] },
    amazon_total_units:      { kind: "num", presets: [1, 50, 500] },
    amazon_total_sales:      { kind: "num", presets: [1000, 10000, 100000] },
};

type NumRange = { min: number | null; max: number | null };

const num   = (k: keyof SkuRow): ColumnDef<SkuRow> => ({
    accessorKey: k,
    cell: (info) => <span className="tabular text-right block">{fmtInt(info.getValue() as number)}</span>,
});
const money = (k: keyof SkuRow): ColumnDef<SkuRow> => ({
    accessorKey: k,
    cell: (info) => <span className="tabular text-right block">{fmtINR(info.getValue() as number)}</span>,
});

// Approximate row height for the virtualizer's overscan calculation.  The
// actual height is set by row content, so this is just a hint — the
// virtualizer measures real heights on mount and adjusts.
const ROW_HEIGHT_PX = 36;

export function SkuTable({ rows }: { rows: SkuRow[] }) {
    const [sorting, setSorting]   = useState<SortingState>([{ id: "total_sales", desc: true }]);
    const [filter, setFilter]     = useState("");
    // Deferred value — the search input stays instantly responsive while
    // React schedules the expensive 600-row filter recomputation as a
    // low-priority render that can be interrupted by more typing.
    const deferredFilter          = useDeferredValue(filter);
    // Scroll container for the virtualizer — must be the actual scrollable element.
    const scrollRef = useRef<HTMLDivElement>(null);
    // Component-local filter state — URL persistence on this 600+ row table
    // caused noticeable Dashboard lag because useSearchParams updates re-render
    // the entire Dashboard tree (4 charts + spark cards + AMS pivot). Keeping
    // state local here is the right perf trade-off; other operator pages where
    // the table is the dominant component retain URL-persisted filters.
    const [catFilters, setCatFilters] = useState<Record<string, string[]>>({});
    const [numFilters, setNumFilters] = useState<Record<string, NumRange>>({});

    function setCat(colId: string, vals: string[]) {
        setCatFilters((p) => ({ ...p, [colId]: vals }));
    }
    function setNum(colId: string, rng: NumRange) {
        setNumFilters((p) => ({ ...p, [colId]: rng }));
    }

    // Join margin data by model — Fossil rows return null and render "—".
    const { lookupByModel } = useMargins();
    const enrichedRows = useMemo<SkuRow[]>(
        () => {
            const t0 = performance.now();
            const out = rows.map((r) => ({
                ...r,
                net_margin_pct: lookupByModel(r.model_no)?.net_margin_pct ?? null,
            }));
            // eslint-disable-next-line no-console
            console.log(`[SkuTable] enrichedRows: ${rows.length} rows in ${(performance.now()-t0).toFixed(1)}ms`);
            return out;
        },
        [rows, lookupByModel],
    );

    // Build unique-value lists for the four categorical columns from the
    // pre-filtered row set so the dropdowns always reflect what's actually
    // available regardless of other active filters.
    const uniqueValues = useMemo(() => {
        const out: Record<string, string[]> = { sku: [], model_no: [], asin: [], category_l0: [] };
        const sets: Record<string, Set<string>> = { sku: new Set(), model_no: new Set(), asin: new Set(), category_l0: new Set() };
        for (const r of enrichedRows) {
            if (r.sku) sets.sku.add(r.sku);
            if (r.model_no)    sets.model_no.add(r.model_no);
            if (r.asin)        sets.asin.add(r.asin);
            if (r.category_l0) sets.category_l0.add(r.category_l0);
        }
        for (const k of Object.keys(sets)) out[k] = Array.from(sets[k]).sort();
        return out;
    }, [enrichedRows]);

    const columns = useMemo<ColumnDef<SkuRow>[]>(() => [
        {
            accessorKey: "sku",
            header: "SKU",
            cell: (info) => <span>{info.getValue() as string}</span>,
        },
        {
            accessorKey: "model_no",
            header: "Model",
            cell: (info) => <span className="font-medium">{info.getValue() as string}</span>,
        },
        {
            accessorKey: "asin",
            header: "ASIN",
            cell: (info) => <span>{(info.getValue() as string) || ""}</span>,
        },
        {
            accessorKey: "category_l0",
            header: "Category",
            cell: (info) => <span className="text-foreground">{info.getValue() as string}</span>,
        },
        { ...num("total_units"),               header: "All Ch. (Units)" },
        { ...money("total_sales"),             header: "All Ch. (Sales)" },
        {
            accessorKey: "sales_contribution_pct",
            header: "% Contrib",
            cell: (info) => <span className="tabular text-right block">{(info.getValue() as number)?.toFixed(2)}%</span>,
        },
        {
            accessorKey: "net_margin_pct",
            header: "Net Margin %",
            // Loss-makers in red, low-margin (<10%) in amber, healthy in default text.
            // Fossil and any unmapped model show "—".
            cell: (info) => {
                const v = info.getValue() as number | null;
                if (v == null) return <span className="tabular text-right block" style={{ color: "#9ca3af" }}>—</span>;
                const isLoss = v < 0;
                const isLow  = v >= 0 && v < 10;
                return (
                    <span
                        className="tabular text-right block font-semibold"
                        style={isLoss ? { color: "#b91c1c" } : isLow ? { color: "#9a3412" } : { color: "#047857" }}
                    >
                        {v.toFixed(2)}%
                    </span>
                );
            },
            sortingFn: (a, b, colId) => {
                // Push null to bottom regardless of sort direction.
                const av = a.getValue<number | null>(colId);
                const bv = b.getValue<number | null>(colId);
                if (av == null && bv == null) return 0;
                if (av == null) return 1;
                if (bv == null) return -1;
                return av - bv;
            },
        },
        { ...num("amazon_am_units"),           header: "AM (Units)" },
        { ...money("amazon_am_sales"),         header: "AM (Sales)" },
        { ...num("amazon_1p_units"),           header: "1P (Units)" },
        { ...money("amazon_1p_sales"),         header: "1P (Sales)" },
        { ...num("amazon_total_units"),        header: "Amz (Units)" },
        { ...money("amazon_total_sales"),      header: "Amz (Sales)" },
    ], []);

    const filtered = useMemo(() => {
        const q = deferredFilter.trim().toLowerCase();
        function inRange(v: any, rng: NumRange | undefined): boolean {
            if (!rng || (rng.min == null && rng.max == null)) return true;
            if (v == null || v === "" || isNaN(Number(v))) return false;
            const n = Number(v);
            if (rng.min != null && n < rng.min) return false;
            if (rng.max != null && n > rng.max) return false;
            return true;
        }
        const catEntries = Object.entries(catFilters).filter(([, vals]) => vals.length > 0);
        return enrichedRows.filter((r) => {
            // Free-text search
            if (q) {
                const hay = (r.sku + " " + (r.model_no || "") + " " + (r.asin || "") + " " + (r.category_l0 || "")).toLowerCase();
                if (!hay.includes(q)) return false;
            }
            // Categorical filters
            for (const [k, vals] of catEntries) {
                const s = new Set(vals);
                if (!s.has(String((r as any)[k] ?? ""))) return false;
            }
            // Numeric range filters
            for (const [k, rng] of Object.entries(numFilters)) {
                if (!inRange((r as any)[k], rng)) return false;
            }
            return true;
        });
    }, [enrichedRows, deferredFilter, catFilters, numFilters]);

    const table = useReactTable({
        data: filtered,
        columns,
        state: { sorting },
        onSortingChange: setSorting,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    const sortedRows = table.getRowModel().rows;

    // Row virtualizer — only renders rows currently in (or near) the viewport,
    // so a 600-row table is reconciled like a 30-row table on every sort.
    const rowVirtualizer = useVirtualizer({
        count: sortedRows.length,
        getScrollElement: () => scrollRef.current,
        estimateSize: () => ROW_HEIGHT_PX,
        overscan: 10,
    });
    const virtualRows = rowVirtualizer.getVirtualItems();
    const totalSize   = rowVirtualizer.getTotalSize();
    const paddingTop    = virtualRows.length > 0 ? virtualRows[0].start                : 0;
    const paddingBottom = virtualRows.length > 0 ? totalSize - virtualRows[virtualRows.length - 1].end : 0;

    return (
        <Card className="overflow-hidden">
            <SectionHeader
                icon={Package}
                iconColor="#1e40af"
                title="Sales by SKU"
                subtitle={`All channels · ${filtered.length.toLocaleString()} rows`}
                action={
                    <>
                        <Input
                            placeholder="Filter SKU / Model / ASIN / Category…"
                            value={filter}
                            onChange={(e) => setFilter(e.target.value)}
                            className="max-w-xs h-8 text-sm"
                        />
                        <Button
                            variant="outline"
                            size="sm"
                            onClick={() => exportToCsv(
                                table.getSortedRowModel().rows.map((r) => r.original as any),
                                columns.map((c) => (c as any).accessorKey as string),
                                "dashboard-sku.csv"
                            )}
                        >
                            <Download className="h-3.5 w-3.5" />
                            Export CSV
                        </Button>
                    </>
                }
            />
            <div ref={scrollRef} className="overflow-auto max-h-[78vh] relative">
                <table className="sku-grouped w-full text-[13px] border-separate border-spacing-0">
                    <thead className="sticky top-0 z-30">
                        {table.getHeaderGroups().map((hg) => (
                            <tr key={hg.id}>
                                {hg.headers.map((h, idx) => {
                                    const sorted = h.column.getIsSorted();
                                    const isFrozen = idx < FROZEN_COUNT;
                                    const style = isFrozen
                                        ? { left: FROZEN_LEFT_PX[idx], position: "sticky" as const }
                                        : undefined;
                                    const colId = h.column.id;
                                    const cfg   = COLUMN_FILTERS[colId];
                                    const catActive = !!(catFilters[colId]?.length);
                                    const numActive = !!(numFilters[colId] && (numFilters[colId].min != null || numFilters[colId].max != null));
                                    const filterActive = catActive || numActive;
                                    return (
                                        <th
                                            key={h.id}
                                            onClick={h.column.getToggleSortingHandler()}
                                            style={style}
                                            className={cn(
                                                "px-3 py-2 text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b cursor-pointer select-none hover:text-foreground bg-secondary",
                                                isFrozen ? "text-left z-40" : "text-right",
                                                idx === FROZEN_COUNT - 1 && "border-r-2 border-r-border",
                                                isFrozen && FROZEN_WIDTH[idx],
                                                filterActive && "bg-primary/5",
                                            )}
                                        >
                                            <span className="inline-flex items-center gap-1 whitespace-nowrap">
                                                {flexRender(h.column.columnDef.header, h.getContext())}
                                                {sorted === "asc"  && <ArrowUp className="h-3 w-3" />}
                                                {sorted === "desc" && <ArrowDown className="h-3 w-3" />}
                                                {sorted === false  && <ArrowUpDown className="h-3 w-3 opacity-30" />}
                                                {cfg?.kind === "cat" && (
                                                    <ColumnFilter
                                                        values={uniqueValues[colId] || []}
                                                        selected={catFilters[colId] || []}
                                                        onChange={(v) => setCat(colId, v)}
                                                    />
                                                )}
                                                {cfg?.kind === "num" && (
                                                    <NumberRangeFilter
                                                        value={numFilters[colId] || { min: null, max: null }}
                                                        onChange={(r) => setNum(colId, r)}
                                                        suffix={cfg.suffix}
                                                        presets={cfg.presets}
                                                    />
                                                )}
                                            </span>
                                        </th>
                                    );
                                })}
                            </tr>
                        ))}
                    </thead>
                    <tbody>
                        {/* Top spacer — height = px of virtually-hidden rows above */}
                        {paddingTop > 0 && (
                            <tr style={{ height: `${paddingTop}px` }}><td colSpan={columns.length} /></tr>
                        )}
                        {virtualRows.map((virtualRow) => {
                            const row = sortedRows[virtualRow.index];
                            return (
                            <tr key={row.id} className="group">
                                {row.getVisibleCells().map((cell, idx) => {
                                    const isFrozen = idx < FROZEN_COUNT;
                                    const style = isFrozen
                                        ? { left: FROZEN_LEFT_PX[idx], position: "sticky" as const }
                                        : undefined;
                                    return (
                                        <td
                                            key={cell.id}
                                            style={style}
                                            className={cn(
                                                "px-3 py-2 align-middle border-b bg-background group-hover:bg-accent/40",
                                                isFrozen ? "z-10" : "text-right",
                                                idx === FROZEN_COUNT - 1 && "border-r-2 border-r-border"
                                            )}
                                        >
                                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                        </td>
                                    );
                                })}
                            </tr>
                            );
                        })}
                        {/* Bottom spacer — height = px of virtually-hidden rows below */}
                        {paddingBottom > 0 && (
                            <tr style={{ height: `${paddingBottom}px` }}><td colSpan={columns.length} /></tr>
                        )}
                    </tbody>
                </table>
            </div>
        </Card>
    );
}
