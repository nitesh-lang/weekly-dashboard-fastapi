import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, sortWeeks, exportToXlsx, copyTableToClipboard } from "@/lib/utils";
import { Download, Copy, Check } from "lucide-react";
import { useSortedRows } from "@/lib/useSortedRows";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { FilterChipStrip, type FilterChipGroup } from "@/components/FilterChipStrip";
import { SortableTh } from "@/components/SortableTh";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { ChevronRight, BarChart3 } from "lucide-react";

interface ChannelRow {
    channel: string;
    units_sold: number;
    gmv: number;
    sales_nlc: number;
}
interface SkuRow {
    sku?: string;
    model_no?: string;
    category_l0?: string;
    units_sold?: number;
    gmv?: number;
    sales_nlc?: number;
    channel_contribution_pct?: number;
    [k: string]: any;
}
interface DrilldownData {
    week:              string | null;
    channel:           string | null;
    brand:             string | null;
    type:              string;
    level?:            string | null;
    value?:            string | null;
    channel_summary:   ChannelRow[];
    sku_channel_rows:  SkuRow[];
    available_brands?: string[];
    available_weeks?:  string[];
    active_weeks?:     string[];
    active_brands?:    string[];
}

export default function Drilldown() {
    const [params, setParams] = useSearchParams();
    const type    = params.get("type") || "sales";
    const channel = params.get("channel") || "";
    const selectedWeeks  = params.getAll("weeks").filter(Boolean);
    const selectedBrands = params.getAll("brands").filter(Boolean);
    const [filter, setFilter] = useDebouncedUrlParam("q");

    const qs = new URLSearchParams();
    qs.set("type", type);
    if (channel) qs.set("channel", channel);
    selectedWeeks.forEach((w) => qs.append("weeks", w));
    selectedBrands.forEach((b) => qs.append("brands", b));

    const { data, isLoading, error } = useQuery<DrilldownData>({
        queryKey: ["drilldown", qs.toString()],
        queryFn: () => api.get("/api/drilldown?" + qs.toString()),
    });

    function setMulti(name: string, values: string[]) {
        const next = new URLSearchParams(params);
        next.delete(name);
        values.forEach((v) => next.append(name, v));
        setParams(next, { replace: false });
    }

    function setChannel(ch: string | null) {
        const next = new URLSearchParams(params);
        if (ch) next.set("channel", ch); else next.delete("channel");
        setParams(next);
    }

    const allWeeks  = useMemo(() => sortWeeks(data?.available_weeks || []), [data]);
    const allBrands = data?.available_brands || [];

    const filteredSkus = useMemo(() => {
        if (!data) return [];
        if (!filter.trim()) return data.sku_channel_rows;
        const q = filter.toLowerCase();
        return data.sku_channel_rows.filter((r) =>
            ((r.sku || "") + " " + (r.model_no || "") + " " + (r.category_l0 || ""))
                .toLowerCase()
                .includes(q)
        );
    }, [data, filter]);

    // ── Build SKU table columns dynamically: All-Channels view has per-channel
    //    breakouts (amazon_am_units, etc.); single-channel view has units_sold/gmv.
    //    NLC columns intentionally suppressed per operator request — they're
    //    cost-side internal metrics, not needed in the drilldown.
    const skuCols = useMemo(() => {
        if (!filteredSkus.length) return { textKeys: [], numKeys: [] };
        const first = filteredSkus[0];
        const keys = Object.keys(first).filter((k) => !/nlc/i.test(k));
        // Identifier columns shown first as sticky text columns.  Order is
        // important — SKU / Model / ASIN / Category L0 matches the layout
        // operator uses on Sales Trend + AM+1P Trend pages.
        const textKeyOrder = ["sku", "model_no", "asin", "category_l0"];
        const textKeys = textKeyOrder.filter((k) => keys.includes(k));
        const numKeys  = keys.filter((k) => !textKeys.includes(k));
        return { textKeys, numKeys };
    }, [filteredSkus]);

    // Default-sort: pick the first numeric col that looks like a sales/GMV metric;
    // falls back to whatever numeric key exists first.
    const defaultSortKey = useMemo(() => {
        const preferred = skuCols.numKeys.find((k) => /sales|gmv|units/i.test(k));
        return preferred || skuCols.numKeys[0] || null;
    }, [skuCols]);
    const { sorted: sortedSkus, sort, onSort } = useSortedRows(
        filteredSkus,
        defaultSortKey ? { key: defaultSortKey, dir: "desc" } : null,
    );

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Drilldown · SKU-Level Sales</div>
                    <h1 className="text-2xl font-semibold tracking-tight">Sales Drilldown</h1>
                    <p className="text-[13.5px] mt-1.5 flex items-center gap-1.5" style={{ color: "#4b5563" }}>
                        <span>Sales</span>
                        {channel && (
                            <>
                                <ChevronRight className="h-3.5 w-3.5" />
                                <strong style={{ color: "#0a0a0a" }}>{channel}</strong>
                            </>
                        )}
                        {data?.brand && data.brand !== "None" && (
                            <>
                                <ChevronRight className="h-3.5 w-3.5" />
                                <strong style={{ color: "#0a0a0a" }}>{data.brand}</strong>
                            </>
                        )}
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Weeks"  options={allWeeks}  selected={selectedWeeks}  onApply={(v) => setMulti("weeks", v)} />
                <MultiPicker label="Brands" options={allBrands} selected={selectedBrands} onApply={(v) => setMulti("brands", v)} />
            </div>

            <FilterChipStrip
                filters={[
                    { label: "Weeks", values: selectedWeeks,  onRemove: (v) => setMulti("weeks",  selectedWeeks.filter((x) => x !== v)),  onClear: () => setMulti("weeks", []) },
                    { label: "Brand", values: selectedBrands, onRemove: (v) => setMulti("brands", selectedBrands.filter((x) => x !== v)), onClear: () => setMulti("brands", []) },
                ] as FilterChipGroup[]}
            />

            {isLoading && <LoadingSkeleton rows={8} />}
            {error && <ErrorBlock error={error} />}

            {data && (
                <>
                    {/* SKU table — always shown so the section header is visible
                        even when filter / source produces zero rows. */}
                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={BarChart3}
                            iconColor="#2563eb"
                            title="SKU-Level Sales"
                            subtitle={`${channel || "All channels"} · ${(data.sku_channel_rows?.length || 0).toLocaleString()} rows`}
                            action={
                                <>
                                    <Input
                                        placeholder="Filter SKU / Model / Category…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={sortedSkus.length === 0}
                                        onClick={() => {
                                            // CSV columns mirror the table — sticky identifier columns
                                            // first, then every numeric metric.  Dynamic per drilldown
                                            // type (All vs single channel vs category).
                                            const cols = [...skuCols.textKeys, ...skuCols.numKeys];
                                            const fname = `drilldown-${(channel || "all").toLowerCase().replace(/\s+/g, "-")}.xlsx`;
                                            exportToXlsx(sortedSkus as any, cols, fname);
                                        }}
                                    >
                                        <Download className="h-3.5 w-3.5" />
                                        Export
                                    </Button>
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={sortedSkus.length === 0}
                                        onClick={() => {
                                            const cols = [...skuCols.textKeys, ...skuCols.numKeys];
                                            copyTableToClipboard(sortedSkus as any, cols);
                                        }}
                                    >
                                        <Copy className="h-3.5 w-3.5" />
                                        Copy
                                    </Button>
                                </>
                            }
                        />
                        {filteredSkus.length === 0 ? (
                            <div className="p-8 text-center text-sm text-muted-foreground">
                                No SKU rows for this drilldown.
                            </div>
                        ) : (
                            <div className="overflow-auto max-h-[72vh]">
                                <table className="w-full text-[13px] border-separate border-spacing-0">
                                    <thead className="sticky top-0 z-30">
                                        <tr>
                                            {skuCols.textKeys.map((k, ti) => {
                                                const left = [0, 110, 220, 340][ti] ?? 0;
                                                const lastFrozen = ti === skuCols.textKeys.length - 1;
                                                return (
                                                    <SortableTh
                                                        key={k}
                                                        sortKey={k}
                                                        label={k.replace(/_/g, " ")}
                                                        sort={sort}
                                                        onSort={onSort}
                                                        align="left"
                                                        stickyLeft={left}
                                                        minWidth={110}
                                                        lastFrozen={lastFrozen}
                                                    />
                                                );
                                            })}
                                            {skuCols.numKeys.map((k) => (
                                                <SortableTh
                                                    key={k}
                                                    sortKey={k}
                                                    label={k.replace(/_/g, " ")}
                                                    sort={sort}
                                                    onSort={onSort}
                                                />
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sortedSkus.map((r, i) => (
                                            <tr key={i} className="group">
                                                {skuCols.textKeys.map((k, ti) => {
                                                    const left = [0, 110, 220, 340][ti] ?? 0;
                                                    const lastFrozen = ti === skuCols.textKeys.length - 1;
                                                    return (
                                                        <td
                                                            key={k}
                                                            style={{ position: "sticky", left }}
                                                            className={"px-3 py-2 border-b bg-background group-hover:bg-accent/40 z-10 min-w-[110px] " + (k === "sku" ? "" : k === "model_no" ? "font-medium" : "text-foreground") + (lastFrozen ? " border-r-2 border-r-border" : "")}
                                                        >
                                                            {r[k] ?? ""}
                                                        </td>
                                                    );
                                                })}
                                                {skuCols.numKeys.map((k) => {
                                                    const v = r[k];
                                                    const isPct = /pct|percent|share/i.test(k);
                                                    const isMoney = /sales|gmv|nlc|value|spend|cost/i.test(k);
                                                    let txt: string;
                                                    if (v == null) txt = "—";
                                                    else if (isPct) txt = (typeof v === "number" ? v.toFixed(2) : String(v)) + "%";
                                                    else if (isMoney) txt = fmtINR(v as number);
                                                    else txt = fmtInt(v as number);
                                                    return <td key={k} className="px-3 py-2 text-right tabular border-b">{txt}</td>;
                                                })}
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
