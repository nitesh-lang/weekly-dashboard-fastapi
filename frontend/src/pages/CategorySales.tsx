import { useState, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, sortWeeks, exportToXlsx, copyTableToClipboard } from "@/lib/utils";
import { useCanExport } from "@/lib/auth";
import { useSortedRows } from "@/lib/useSortedRows";
import AppLayout from "@/components/AppLayout";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { MultiPicker } from "@/components/MultiPicker";
import { FilterChipStrip, type FilterChipGroup } from "@/components/FilterChipStrip";
import { SortableTh } from "@/components/SortableTh";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Download, Copy, Check } from "lucide-react";
import { cn } from "@/lib/utils";

interface CategoryRow {
    [key: string]: any;
    units_sold: number;
    gross_sales: number;
    gmv_pct: number;
}

interface CategoryData {
    rows:            CategoryRow[];
    weeks:           string[];
    brands:          string[];
    level:           "l0" | "l1" | "l2";
    value:           string | null;
    week:            string | null;
    selected_brands: string[];
    sel_weeks:       string[];
}

const LEVELS = [
    { value: "l0", label: "L0 — Top" },
    { value: "l1", label: "L1 — Sub" },
    { value: "l2", label: "L2 — Leaf" },
];

export default function CategorySales() {
    const [params, setParams] = useSearchParams();
    const level = (params.get("level") || "l0") as "l0" | "l1" | "l2";
    const value = params.get("value") || "";
    const selectedBrands = params.getAll("brands").filter(Boolean);
    const selectedWeeks  = params.getAll("sel_weeks").filter(Boolean);

    const qs = new URLSearchParams();
    qs.set("level", level);
    if (value) qs.set("value", value);
    selectedBrands.forEach((b) => qs.append("brands", b));
    selectedWeeks.forEach((w) => qs.append("sel_weeks", w));

    const { data, isLoading, error } = useQuery<CategoryData>({
        queryKey: ["category-sales", qs.toString()],
        queryFn: () => api.get("/api/category-sales?" + qs.toString()),
    });

    function setLevel(l: "l0" | "l1" | "l2") {
        const next = new URLSearchParams(params);
        next.set("level", l);
        next.delete("value");
        setParams(next);
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
    const selCats = params.getAll("cats").filter(Boolean);
    const rUnits  = getRange("units");
    const rGmv    = getRange("gmv");
    const rPct    = getRange("pct");
    function drillTo(catName: string) {
        if (level === "l2") return; // already at leaf
        const next = new URLSearchParams(params);
        next.set("level", level === "l0" ? "l1" : "l2");
        next.set("value", catName);
        setParams(next);
    }

    const allWeeks  = useMemo(() => sortWeeks(data?.weeks || []), [data]);
    const allBrands = data?.brands || [];
    const groupCol  = `category_${level}`;

    // Separate grand-total row so it stays pinned to the bottom regardless of sort.
    const { dataRows, totalRow } = useMemo(() => {
        const rows = data?.rows || [];
        const total = rows.find((r) => String(r[groupCol] ?? "").toLowerCase().includes("grand total")) || null;
        const body  = rows.filter((r) => r !== total);
        return { dataRows: body, totalRow: total };
    }, [data, groupCol]);

    const allCats = useMemo(() => Array.from(new Set(dataRows.map((r) => String(r[groupCol] ?? "")).filter(Boolean))).sort(), [dataRows, groupCol]);

    const filteredRows = useMemo(() => {
        const cs = new Set(selCats);
        function inRange(v: any, rng: { min: number | null; max: number | null }): boolean {
            if (rng.min == null && rng.max == null) return true;
            if (v == null || v === "" || isNaN(Number(v))) return false;
            const num = Number(v);
            if (rng.min != null && num < rng.min) return false;
            if (rng.max != null && num > rng.max) return false;
            return true;
        }
        return dataRows.filter((r) => {
            if (cs.size && !cs.has(String(r[groupCol] ?? ""))) return false;
            if (!inRange(r.units_sold,  rUnits)) return false;
            if (!inRange(r.gross_sales, rGmv))   return false;
            if (!inRange(r.gmv_pct,     rPct))   return false;
            return true;
        });
    }, [dataRows, groupCol, selCats, rUnits, rGmv, rPct]);

    const { sorted, sort, onSort } = useSortedRows(filteredRows, { key: "gross_sales", dir: "desc" });
    const displayRows = totalRow ? [...sorted, totalRow] : sorted;

    return (
        <AppLayout>
            <div>
                <div className="eyebrow mb-2">Portfolio · Categories</div>
                <h1 className="text-2xl font-semibold tracking-tight">Category Sales</h1>
                <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                    {value
                        ? <>Drilling into <strong style={{ color: "#0a0a0a" }}>{value}</strong> · click a category name to dive deeper.</>
                        : <>Top-level category breakdown · L0 / L1 / L2 hierarchy with drill-down.</>}
                </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
                <div className="inline-flex rounded-md border bg-secondary/60 p-0.5 gap-0.5">
                    {LEVELS.map((l) => (
                        <button
                            key={l.value}
                            onClick={() => setLevel(l.value as any)}
                            className={cn(
                                "px-3 py-1.5 text-[11px] font-medium rounded transition-colors",
                                level === l.value
                                    ? "bg-background text-foreground shadow-sm"
                                    : "text-muted-foreground hover:text-foreground"
                            )}
                        >
                            {l.label}
                        </button>
                    ))}
                </div>
                <MultiPicker label="Weeks"  options={allWeeks}  selected={selectedWeeks}  onApply={(v) => setMulti("sel_weeks", v)} />
                <MultiPicker label="Brands" options={allBrands} selected={selectedBrands} onApply={(v) => setMulti("brands", v)} />
            </div>

            <FilterChipStrip
                filters={[
                    { label: "Weeks", values: selectedWeeks,  onRemove: (v) => setMulti("sel_weeks", selectedWeeks.filter((x) => x !== v)),  onClear: () => setMulti("sel_weeks", []) },
                    { label: "Brand", values: selectedBrands, onRemove: (v) => setMulti("brands",    selectedBrands.filter((x) => x !== v)), onClear: () => setMulti("brands", []) },
                ] as FilterChipGroup[]}
            />

            {isLoading && <LoadingSkeleton rows={8} />}
            {error && <ErrorBlock error={error} />}

            {data && (
                <Card className="overflow-hidden">
                    {useCanExport() && (
                        <div className="flex items-center justify-end border-b px-4 py-2 gap-2">
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={() => exportToXlsx(
                                    sorted as any,
                                    [groupCol, "units_sold", "gross_sales", "gmv_pct"],
                                    `category-sales-${level}.csv`,
                                )}
                            >
                                <Download className="h-3.5 w-3.5" />
                                Export
                            </Button>
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={() => copyTableToClipboard(sorted as any, [groupCol, "units_sold", "gross_sales", "gmv_pct"])}
                            >
                                <Copy className="h-3.5 w-3.5" />
                                Copy
                            </Button>
                        </div>
                    )}
                    <div className="overflow-auto max-h-[72vh]">
                        <table className="text-[13px] w-full" style={{ tableLayout: "auto" }}>
                            <thead className="bg-secondary sticky top-0 z-10">
                                <tr>
                                    <SortableTh sortKey={groupCol}    label="Category" sort={sort} onSort={onSort} align="left" minWidth={260}
                                        filterValues={allCats} filterSelected={selCats} onFilterChange={(v) => setMulti("cats", v)} />
                                    <SortableTh sortKey="units_sold"  label="Units"    sort={sort} onSort={onSort} minWidth={120}
                                        numericRange={rUnits} onNumericFilter={(r) => setRange("units", r)} numericPresets={[1, 10, 100]} />
                                    <SortableTh sortKey="gross_sales" label="GMV"      sort={sort} onSort={onSort} minWidth={150}
                                        numericRange={rGmv}   onNumericFilter={(r) => setRange("gmv", r)}   numericPresets={[1000, 10000, 100000]} />
                                    <SortableTh sortKey="gmv_pct"     label="% of GMV" sort={sort} onSort={onSort} minWidth={140}
                                        numericRange={rPct}   onNumericFilter={(r) => setRange("pct", r)}   numericSuffix="%" numericPresets={[1, 5, 10]} />
                                </tr>
                            </thead>
                            <tbody>
                                {displayRows.map((r, i) => {
                                    const catName = String(r[groupCol] ?? "");
                                    const isTotal = catName.toLowerCase().includes("grand total");
                                    return (
                                        <tr key={i} className={cn("border-b", !isTotal && "hover:bg-accent/40", isTotal && "font-semibold bg-secondary/50")}>
                                            <td className="px-3 py-2">
                                                {isTotal || level === "l2" ? (
                                                    catName
                                                ) : (
                                                    <Button variant="link" className="p-0 h-auto" onClick={() => drillTo(catName)}>
                                                        {catName}
                                                    </Button>
                                                )}
                                            </td>
                                            <td className="px-3 py-2 text-right tabular">{fmtInt(r.units_sold)}</td>
                                            <td className="px-3 py-2 text-right tabular">{fmtINR(r.gross_sales)}</td>
                                            <td className="px-3 py-2 text-right tabular">{r.gmv_pct?.toFixed(1)}%</td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                </Card>
            )}
        </AppLayout>
    );
}
