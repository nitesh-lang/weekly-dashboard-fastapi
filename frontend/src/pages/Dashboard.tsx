import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt, sortWeeks, exportToXlsx, copyTableToClipboard } from "@/lib/utils";
import { useCanExport } from "@/lib/auth";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { FilterChipStrip, type FilterChipGroup } from "@/components/FilterChipStrip";
import { KpiCard } from "@/components/KpiCard";
import { TrendChart } from "@/components/TrendChart";
import { BrandMix } from "@/components/BrandMix";
import { SkuTable } from "@/components/SkuTable";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ArrowRight, ChevronUp, ChevronDown, Radio, Layers, Download, Copy, Check } from "lucide-react";
import { SectionHeader } from "@/components/SectionHeader";

interface DashboardData {
    kpis:      { units: number; gmv: number; inventory_value?: number; sell_through?: number };
    kpi_sparks:{ weeks: string[]; units: number[]; gmv: number[]; nlc: number[] };
    kpi_deltas:{ units: number | null; gmv: number | null; nlc: number | null };
    weekly_trend:      Array<{ week: string; gmv: number; units: number; nlc?: number }>;
    weekly_all_brands: Array<{ week: string; gmv: number; units: number }>;
    brand_mix:         Array<{ brand: string; gmv: number; pct: number }>;
    channel_summary:   Array<{ channel: string; units_sold: number; gmv: number; sales_nlc: number; [k: string]: any }>;
    category_summary:  Array<{ category_l0?: string; category?: string; units_sold?: number; units?: number; gross_sales?: number; gmv?: number; sales_contribution_pct?: number; gmv_share_pct?: number; [k: string]: any }>;
    ams_pivot:         Array<{ week: string; ad_spend: number; attributed_sales: number; sessions: number; acos: number }>;
    sku_rows:          any[];
    sku_total:         number;
    weeks:             string[];
    brands:            string[];
    asin_types?:       string[];
    selected:          {
        week: string | null;
        brand: string | null;
        weeks: string[] | null;
        brands: string[] | null;
        asin_types?: string[] | null;
        view: string;
        weeks_display: string | null;
    };
    latest_week_label: string;
    generated_at:      string;
}

type InsightTab = "trend" | "brand" | "ams";

export default function Dashboard() {
    const [params, setParams] = useSearchParams();
    const [insightTab, setInsightTab] = useState<InsightTab>("trend");
    const [showOverview, setShowOverview] = useState(false);
    const [showSummaries, setShowSummaries] = useState(false);

    const selectedWeeks     = params.getAll("weeks").filter(Boolean);
    const selectedBrands    = params.getAll("brands").filter(Boolean);
    const selectedAsinTypes = params.getAll("asin_types").filter(Boolean);
    const view              = params.get("view") || "mapped";

    const qs = new URLSearchParams();
    selectedWeeks.forEach((w)     => qs.append("weeks", w));
    selectedBrands.forEach((b)    => qs.append("brands", b));
    selectedAsinTypes.forEach((t) => qs.append("asin_types", t));
    qs.set("view", view);

    const { data, isLoading, error } = useQuery<DashboardData>({
        queryKey: ["dashboard", qs.toString()],
        queryFn: () => api.get("/api/dashboard?" + qs.toString()),
    });

    function setMulti(name: string, values: string[]) {
        const next = new URLSearchParams(params);
        next.delete(name);
        // Strip legacy singular form to avoid drift
        if (name === "weeks")  next.delete("week");
        if (name === "brands") next.delete("brand");
        values.forEach((v) => next.append(name, v));
        setParams(next, { replace: false });
    }

    const allWeeks     = useMemo(() => sortWeeks(data?.weeks || []), [data]);
    const allBrands    = useMemo(() => data?.brands || [], [data]);
    const allAsinTypes = useMemo(() => data?.asin_types || [], [data]);

    // Sync backend-picked latest week to the URL EXACTLY ONCE on first
    // load.  Without this the picker shows "All Weeks" placeholder while
    // the numbers on screen are actually filtered to the latest week —
    // operator then picks W27 expecting to ADD it to W28 but ends up
    // REPLACING (because URL was empty).  Reported UX 2026-07-15.
    //
    // Uses useRef so we never overwrite a subsequent user selection.
    // data.selected.weeks is a new array reference on every refetch, so
    // without this ref-guard the effect would fire on every W27/W28
    // toggle and race the picker — potential fix bug landmine.
    const didSyncInitialWeeks = useRef(false);
    useEffect(() => {
        if (didSyncInitialWeeks.current) return;
        if (selectedWeeks.length > 0) {
            didSyncInitialWeeks.current = true;   // user already has an explicit selection
            return;
        }
        if (data?.selected?.weeks && data.selected.weeks.length > 0) {
            didSyncInitialWeeks.current = true;
            const next = new URLSearchParams(params);
            next.delete("weeks");
            next.delete("week"); // legacy singular
            data.selected.weeks.forEach((w) => next.append("weeks", w));
            setParams(next, { replace: true });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data?.selected?.weeks, selectedWeeks.length]);

    const selectedBrandLabel = useMemo(() => {
        if (!selectedBrands.length) return "All Brands";
        if (selectedBrands.length === 1) return selectedBrands[0];
        return `${selectedBrands.length} brands`;
    }, [selectedBrands]);

    return (
        <AppLayout>
            {/* Filter bar */}
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Operator Console</div>
                    <h1 className="text-2xl font-semibold tracking-tight">Weekly Brief</h1>
                    <p className="text-[13.5px] tabular mt-1.5" style={{ color: "#4b5563" }}>
                        {data?.latest_week_label && <>Latest: <strong style={{ color: "#0a0a0a" }}>{data.latest_week_label}</strong></>}
                        {data?.generated_at && <> · Generated {data.generated_at}</>}
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker
                    label="Weeks"
                    options={allWeeks}
                    selected={selectedWeeks}
                    onApply={(v) => setMulti("weeks", v)}
                    placeholder={
                        data?.selected?.weeks_display
                            ? data.selected.weeks_display
                            : data?.latest_week_label
                                ? `${data.latest_week_label} (latest)`
                                : "All Weeks"
                    }
                />
                <MultiPicker
                    label="Brands"
                    options={allBrands}
                    selected={selectedBrands}
                    onApply={(v) => setMulti("brands", v)}
                    placeholder="All Brands"
                />
                {allAsinTypes.length > 0 && (
                    <MultiPicker
                        label="ASIN Type"
                        options={allAsinTypes}
                        selected={selectedAsinTypes}
                        onApply={(v) => setMulti("asin_types", v)}
                        placeholder="All ASIN Types"
                    />
                )}
            </div>

            {/* Active-filter summary — auto-hides when no filter is applied. */}
            <FilterChipStrip
                filters={[
                    {
                        label: "Weeks",
                        values: selectedWeeks,
                        onRemove: (v) => setMulti("weeks",  selectedWeeks.filter((x) => x !== v)),
                        onClear:  () => setMulti("weeks",  []),
                    },
                    {
                        label: "Brand",
                        values: selectedBrands,
                        onRemove: (v) => setMulti("brands", selectedBrands.filter((x) => x !== v)),
                        onClear:  () => setMulti("brands", []),
                    },
                    {
                        label: "ASIN Type",
                        values: selectedAsinTypes,
                        onRemove: (v) => setMulti("asin_types", selectedAsinTypes.filter((x) => x !== v)),
                        onClear:  () => setMulti("asin_types", []),
                    },
                ] as FilterChipGroup[]}
            />

            {isLoading && (
                <div className="text-sm text-muted-foreground">Loading dashboard data…</div>
            )}
            {error && (
                <div className="text-sm text-destructive">
                    Failed to load: {(error as Error).message}
                </div>
            )}

            {data && (
                <>
                    {/* Compact: KPI strip + tabbed insights row.
                        Collapsible via the toggle so the SKU table can occupy the
                        full screen when operator just wants the SKU data. */}
                    {showOverview && (
                    <div className="grid grid-cols-1 lg:grid-cols-[260px_1fr] gap-4">
                        <div className="grid grid-cols-1 gap-3">
                            <KpiCard
                                hero
                                label="GMV — this week"
                                value={fmtINR(data.kpis.gmv)}
                                deltaPct={data.kpi_deltas.gmv}
                                deltaLabel=" vs prior week"
                                spark={data.kpi_sparks.gmv}
                                color="#059669"
                            />
                            <KpiCard
                                label="Units Sold"
                                value={fmtInt(data.kpis.units)}
                                deltaPct={data.kpi_deltas.units}
                                spark={data.kpi_sparks.units}
                                color="#2563eb"
                            />
                        </div>

                        {/* Tabbed insight panel — Trend / Brand / AMS share one card */}
                        <Card className="overflow-hidden">
                            <div className="border-b px-3 py-2 flex items-center gap-1">
                                {([
                                    { id: "trend", label: "Weekly Trend" },
                                    ...(data.brand_mix?.length > 1 ? [{ id: "brand", label: "Sales by Brand" }] : []),
                                    ...(data.ams_pivot?.length > 0 ? [{ id: "ams",   label: "AMS Trend" }] : []),
                                ] as { id: InsightTab; label: string }[]).map((t) => (
                                    <button
                                        key={t.id}
                                        onClick={() => setInsightTab(t.id)}
                                        className={
                                            "px-3 py-1.5 text-[12px] font-medium rounded transition-colors " +
                                            (insightTab === t.id
                                                ? "bg-secondary text-foreground"
                                                : "text-muted-foreground hover:text-foreground hover:bg-secondary/50")
                                        }
                                    >
                                        {t.label}
                                    </button>
                                ))}
                            </div>

                            {insightTab === "trend" && (
                                <div className="p-0">
                                    <TrendChart
                                        trend={data.weekly_trend}
                                        allBrands={data.weekly_all_brands?.length ? data.weekly_all_brands : undefined}
                                        selectedBrandLabel={selectedBrandLabel}
                                    />
                                </div>
                            )}
                            {insightTab === "brand" && data.brand_mix?.length > 1 && (
                                <div className="p-0">
                                    <BrandMix rows={data.brand_mix} />
                                </div>
                            )}
                            {insightTab === "ams" && data.ams_pivot?.length > 0 && (
                                <div className="overflow-auto">
                                    <table className="w-full text-[13px]">
                                        <thead className="bg-secondary">
                                            <tr>
                                                <th className="px-3 py-2 text-left text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">Week</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">Ad Spend (₹)</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">Attributed Sales (₹)</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">Sessions</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">ACOS %</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {data.ams_pivot.map((r, i) => (
                                                <tr key={i} className="border-b hover:bg-accent/40">
                                                    <td className="px-3 py-2">{r.week}</td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtINR(r.ad_spend)}</td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtINR(r.attributed_sales)}</td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtInt(r.sessions)}</td>
                                                    <td className="px-3 py-2 text-right tabular">{(r.acos * 100).toFixed(2)}%</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </Card>
                    </div>

                    )}

                    {/* SKU table header — with overview, summaries, drilldown toggles */}
                    <div className="flex items-center justify-between">
                        <h2 className="text-lg font-semibold tracking-tight">📦 Sales by SKU — All Channels</h2>
                        <div className="flex items-center gap-2">
                            <Button variant="ghost" size="sm" onClick={() => setShowOverview((s) => !s)}>
                                {showOverview
                                    ? <><ChevronUp className="h-3.5 w-3.5" />Hide overview</>
                                    : <><ChevronDown className="h-3.5 w-3.5" />Show overview</>}
                            </Button>
                            <Button variant="ghost" size="sm" onClick={() => setShowSummaries((s) => !s)}>
                                {showSummaries
                                    ? <><ChevronUp className="h-3.5 w-3.5" />Hide summaries</>
                                    : <><ChevronDown className="h-3.5 w-3.5" />Show channel + category</>}
                            </Button>
                            <Button asChild variant="outline" size="sm">
                                <Link to={`/drilldown?${(() => {
                                    const p = new URLSearchParams();
                                    p.set("type", "sales");
                                    selectedWeeks.forEach((w) => p.append("weeks", w));
                                    selectedBrands.forEach((b) => p.append("brands", b));
                                    selectedAsinTypes.forEach((t) => p.append("asin_types", t));
                                    return p.toString();
                                })()}`}>
                                    Channel Drilldown <ArrowRight className="h-3.5 w-3.5" />
                                </Link>
                            </Button>
                        </div>
                    </div>
                    <SkuTable rows={data.sku_rows as any} />

                    {/* Channel + Category summaries side-by-side — hidden by default */}
                    {showSummaries && (
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                        {data.channel_summary?.length > 0 && (
                            <Card className="overflow-hidden">
                                <SectionHeader
                                    icon={Radio}
                                    iconColor="#0891b2"
                                    title="Channel Summary"
                                    subtitle="By channel"
                                    action={
                                        useCanExport() ? (
                                            <>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    onClick={() => exportToXlsx(
                                                        data.channel_summary as any,
                                                        ["channel", "units_sold", "gmv", "sales_nlc", "sales_contribution_pct"],
                                                        "channel-summary.xlsx",
                                                    )}
                                                >
                                                    <Download className="h-3.5 w-3.5" />
                                                    Export
                                                </Button>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    onClick={() => copyTableToClipboard(
                                                        data.channel_summary as any,
                                                        ["channel", "units_sold", "gmv", "sales_nlc", "sales_contribution_pct"],
                                                    )}
                                                >
                                                    <Copy className="h-3.5 w-3.5" />
                                                    Copy
                                                </Button>
                                            </>
                                        ) : null
                                    }
                                />
                                <div className="overflow-auto">
                                    <table className="w-full text-[13px]">
                                        <thead className="bg-secondary">
                                            <tr>
                                                <th className="px-3 py-2 text-left text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">Channel</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">Units</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">GMV</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">% Contrib</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {data.channel_summary.map((r: any, i) => {
                                                const drillParams = new URLSearchParams();
                                                drillParams.set("type", "sales");
                                                drillParams.set("channel", r.channel);
                                                selectedWeeks.forEach((w) => drillParams.append("weeks", w));
                                                selectedBrands.forEach((b) => drillParams.append("brands", b));
                                                selectedAsinTypes.forEach((t) => drillParams.append("asin_types", t));
                                                return (
                                                <tr key={i} className="border-b hover:bg-accent/40">
                                                    <td className="px-3 py-2">
                                                        <Link to={`/drilldown?${drillParams.toString()}`} className="text-primary hover:underline">
                                                            {r.channel}
                                                        </Link>
                                                    </td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtInt(r.units_sold ?? r.units)}</td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtINR(r.gmv ?? r.gross_sales)}</td>
                                                    <td className="px-3 py-2 text-right tabular">{(r.gmv_share_pct ?? r.sales_contribution_pct ?? 0).toFixed(2)}%</td>
                                                </tr>
                                                );
                                            })}
                                        </tbody>
                                    </table>
                                </div>
                            </Card>
                        )}
                        {data.category_summary?.length > 0 && (
                            <Card className="overflow-hidden">
                                <SectionHeader
                                    icon={Layers}
                                    iconColor="#7c3aed"
                                    title="Category Summary"
                                    subtitle="By L0"
                                    action={
                                        useCanExport() ? (
                                            <>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    onClick={() => exportToXlsx(
                                                        data.category_summary as any,
                                                        ["category_l0", "units_sold", "gross_sales", "sales_contribution_pct"],
                                                        "category-summary.xlsx",
                                                    )}
                                                >
                                                    <Download className="h-3.5 w-3.5" />
                                                    Export
                                                </Button>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    onClick={() => copyTableToClipboard(
                                                        data.category_summary as any,
                                                        ["category_l0", "units_sold", "gross_sales", "sales_contribution_pct"],
                                                    )}
                                                >
                                                    <Copy className="h-3.5 w-3.5" />
                                                    Copy
                                                </Button>
                                            </>
                                        ) : null
                                    }
                                />
                                <div className="overflow-auto">
                                    <table className="w-full text-[13px]">
                                        <thead className="bg-secondary">
                                            <tr>
                                                <th className="px-3 py-2 text-left text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">Category (L0)</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">Units</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">GMV</th>
                                                <th className="px-3 py-2 text-right text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b">% Contrib</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {data.category_summary.map((r: any, i: number) => {
                                                const catName = r.category_l0 ?? r.category ?? "";
                                                const catParams = new URLSearchParams();
                                                catParams.set("level", "l1");
                                                catParams.set("value", catName);
                                                selectedWeeks.forEach((w) => catParams.append("sel_weeks", w));
                                                selectedBrands.forEach((b) => catParams.append("brands", b));
                                                return (
                                                <tr key={i} className="border-b hover:bg-accent/40">
                                                    <td className="px-3 py-2">
                                                        <Link to={`/category-sales?${catParams.toString()}`} className="text-primary hover:underline">
                                                            {catName}
                                                        </Link>
                                                    </td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtInt(r.units ?? r.units_sold)}</td>
                                                    <td className="px-3 py-2 text-right tabular">{fmtINR(r.gmv ?? r.gross_sales)}</td>
                                                    <td className="px-3 py-2 text-right tabular">{(r.gmv_share_pct ?? r.sales_contribution_pct ?? 0).toFixed(2)}%</td>
                                                </tr>
                                                );
                                            })}
                                        </tbody>
                                    </table>
                                </div>
                            </Card>
                        )}
                    </div>
                    )}
                </>
            )}
        </AppLayout>
    );
}
