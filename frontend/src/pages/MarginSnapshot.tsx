import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { fmtINR, fmtInt, exportToXlsx, copyTableToClipboard } from "@/lib/utils";
import { useSortedRows } from "@/lib/useSortedRows";
import { makeTextFilter } from "@/lib/useTextFilter";
import { useMargins, type MarginRow } from "@/lib/useMargins";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { AsinLink } from "@/components/AsinLink";
import { SortableTh } from "@/components/SortableTh";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock, EmptyBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Download, Percent, AlertTriangle, Inbox, Copy, Check } from "lucide-react";

const EXCLUDED_BRANDS = new Set(["fossil"]);

// Data-quality thresholds for the DP/MRP sanity check.
//   - DP < 5% of MRP → suspicious (e.g., AM-C13 DP=₹1 / MRP=₹2.3K → ratio 0.04%)
//   - DP > 50% of MRP → also suspicious (margin calc likely has bad cost data)
const DQ_DP_MRP_MIN = 0.05;
const DQ_DP_MRP_MAX = 0.50;

function pct(v: number | null | undefined): string {
    if (v == null || isNaN(v as number)) return "—";
    return `${(v as number).toFixed(2)}%`;
}

/** Is the DP/MRP ratio outside a sane window?  Used to flag rows where the
 *  margin calculator likely has bad data. */
function isSuspect(r: MarginRow): boolean {
    const dp = r.dp;
    const mrp = r.mrp;
    if (dp == null || mrp == null) return false;
    if (typeof dp !== "number" || typeof mrp !== "number") return false;
    if (mrp <= 0) return false;
    const ratio = dp / mrp;
    return ratio < DQ_DP_MRP_MIN || ratio > DQ_DP_MRP_MAX;
}

export default function MarginSnapshot() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const [filter, setFilter] = useDebouncedUrlParam("q");
    const selBrands = useMemo(
        () => params.getAll("brands").filter(Boolean),
        [qsKey],
    );
    const onlySuspect = params.get("dq") === "1";

    const { rows, brands, available, isLoading, error, refetch } = useMargins();

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
    const selSkus   = useMemo(() => params.getAll("skus").filter(Boolean),   [qsKey]);
    const selAsins  = useMemo(() => params.getAll("asins").filter(Boolean),  [qsKey]);
    const selL1     = useMemo(() => params.getAll("cat_l1").filter(Boolean), [qsKey]);
    const selL2     = useMemo(() => params.getAll("cat_l2").filter(Boolean), [qsKey]);
    const rDp       = useMemo(() => getRange("dp"),       [qsKey]);
    const rMrp      = useMemo(() => getRange("mrp"),      [qsKey]);
    const rBauSp    = useMemo(() => getRange("bau_sp"),   [qsKey]);
    const rGrossR   = useMemo(() => getRange("gross_r"),  [qsKey]);
    const rGrossP   = useMemo(() => getRange("gross_p"),  [qsKey]);
    const rNetR     = useMemo(() => getRange("net_r"),    [qsKey]);
    const rNetP     = useMemo(() => getRange("net_p"),    [qsKey]);

    // Drop Fossil (no margin file) + any explicit exclusions, then apply filters.
    const portfolioRows: MarginRow[] = useMemo(
        () => rows.filter((r) => r.brand && !EXCLUDED_BRANDS.has(r.brand.trim().toLowerCase())),
        [rows],
    );
    const portfolioBrands = useMemo(
        () => brands.filter((b) => !EXCLUDED_BRANDS.has(b.trim().toLowerCase())),
        [brands],
    );

    // Build unique-value sets for column-header filters
    const allModels = useMemo(() => Array.from(new Set(portfolioRows.map((r) => r.model).filter(Boolean))).sort() as string[], [portfolioRows]);
    const allSkus   = useMemo(() => Array.from(new Set(portfolioRows.map((r) => r.sku).filter(Boolean))).sort()   as string[], [portfolioRows]);
    const allAsins  = useMemo(() => Array.from(new Set(portfolioRows.map((r) => r.asin).filter(Boolean))).sort()  as string[], [portfolioRows]);
    const allL1     = useMemo(() => Array.from(new Set(portfolioRows.map((r) => r.category_l1).filter(Boolean))).sort() as string[], [portfolioRows]);
    const allL2     = useMemo(() => Array.from(new Set(portfolioRows.map((r) => r.category_l2).filter(Boolean))).sort() as string[], [portfolioRows]);

    const filtered = useMemo(() => {
        const matchText = makeTextFilter<typeof portfolioRows[number]>(filter, [
            "brand", "model", "sku", "asin",
            "product_name", "category_l1", "category_l2",
        ]);
        const brandSet = new Set(selBrands);
        const ms  = new Set(selModels);
        const ss  = new Set(selSkus);
        const as_ = new Set(selAsins);
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
        return portfolioRows.filter((r) => {
            if (brandSet.size && !brandSet.has(r.brand)) return false;
            if (ms.size   && !ms.has(r.model || ""))   return false;
            if (ss.size   && !ss.has(r.sku || ""))     return false;
            if (as_.size  && !as_.has(r.asin || ""))   return false;
            if (l1s.size  && !l1s.has(r.category_l1 || "")) return false;
            if (l2s.size  && !l2s.has(r.category_l2 || "")) return false;
            if (!inRange(r.dp,               rDp))     return false;
            if (!inRange(r.mrp,              rMrp))    return false;
            if (!inRange(r.bau_deal_sp,      rBauSp))  return false;
            if (!inRange(r.gross_margin,     rGrossR)) return false;
            if (!inRange(r.gross_margin_pct, rGrossP)) return false;
            if (!inRange(r.net_margin,       rNetR))   return false;
            if (!inRange(r.net_margin_pct,   rNetP))   return false;
            if (onlySuspect && !isSuspect(r)) return false;
            return matchText(r);
        });
    }, [portfolioRows, selBrands, filter, onlySuspect, selModels, selSkus, selAsins, selL1, selL2,
        rDp, rMrp, rBauSp, rGrossR, rGrossP, rNetR, rNetP]);

    // Count rows with suspect DP/MRP ratio across the full portfolio (pre-filter)
    // so the "X data issues" chip number is stable regardless of filter state.
    const suspectCount = useMemo(
        () => portfolioRows.reduce((acc, r) => acc + (isSuspect(r) ? 1 : 0), 0),
        [portfolioRows],
    );

    function toggleOnlySuspect() {
        const next = new URLSearchParams(params);
        if (onlySuspect) next.delete("dq"); else next.set("dq", "1");
        setParams(next, { replace: false });
    }

    // Default sort: lowest net margin first — operator wants to see losers up top.
    const { sorted, sort, onSort } = useSortedRows<MarginRow>(filtered, { key: "net_margin_pct", dir: "asc" });

    // KPI strip aggregates.  Average margin is DP-weighted so big-ticket
    // products dominate the headline number.
    const stats = useMemo(() => {
        let sumWeight = 0;
        let sumWeighted = 0;
        let lossCount = 0;
        const brandSet = new Set<string>();
        for (const r of filtered) {
            if (r.brand) brandSet.add(r.brand);
            const m = r.net_margin_pct;
            if (m != null && !isNaN(m as number)) {
                const w = r.dp != null && !isNaN(r.dp as number) ? Math.abs(r.dp as number) : 1;
                sumWeight   += w;
                sumWeighted += (m as number) * w;
                if ((m as number) < 0) lossCount += 1;
            }
        }
        return {
            models:  filtered.length,
            brands:  brandSet.size,
            avgPct:  sumWeight > 0 ? sumWeighted / sumWeight : null,
            losses:  lossCount,
        };
    }, [filtered]);

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2" style={{ color: "#059669" }}>Profitability · Per Model</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <Percent className="h-5 w-5" style={{ color: "#059669" }} />
                        Margin Snapshot
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Net &amp; gross margin per model across portfolio (Fossil excluded).
                        Default sort puts <strong style={{ color: "#0a0a0a" }}>lowest margin first</strong> so loss-makers
                        surface at the top.
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Brands" options={portfolioBrands} selected={selBrands} onApply={(v) => setMulti("brands", v)} />
            </div>

            {isLoading && <LoadingSkeleton rows={8} />}
            {error && <ErrorBlock error={error} onRetry={() => refetch()} title="Couldn't load margin data" />}
            {!isLoading && !error && !available && (
                <EmptyBlock
                    icon={<Inbox className="h-5 w-5" />}
                    title="Margin snapshot not generated yet"
                    hint={
                        <>
                            Run <code style={{ fontFamily: "monospace", background: "#f3f4f6", padding: "1px 4px", borderRadius: 3 }}>
                                python scripts/run_weekly_etl.py margin
                            </code> locally and commit{" "}
                            <code style={{ fontFamily: "monospace", background: "#f3f4f6", padding: "1px 4px", borderRadius: 3 }}>
                                data/processed/margin_snapshot.csv
                            </code>.
                        </>
                    }
                />
            )}

            {available && (
                <>
                    {/* KPI strip */}
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                        {[
                            { label: "Models",              value: fmtInt(stats.models),                                                          accent: "#059669", color: "#0a0a0a" },
                            { label: "Brands",              value: fmtInt(stats.brands),                                                          accent: "#2563eb", color: "#0a0a0a" },
                            { label: "Avg Net Margin %",    value: stats.avgPct != null ? `${stats.avgPct.toFixed(2)}%` : "—",                    accent: "#7c3aed", color: (stats.avgPct ?? 0) < 0 ? "#b91c1c" : "#0a0a0a" },
                            { label: "Loss-Making Models",  value: fmtInt(stats.losses),                                                          accent: "#b91c1c", color: stats.losses > 0 ? "#b91c1c" : "#0a0a0a" },
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

                    {/* Data-quality warning chip — surfaces when DP/MRP ratio is
                        outside a sane window (≤ 5% or ≥ 50%).  Clicking toggles
                        a "show only suspect rows" filter via ?dq=1. */}
                    {suspectCount > 0 && (
                        <button
                            onClick={toggleOnlySuspect}
                            className="inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-[13px] font-medium transition-colors"
                            style={{
                                background: onlySuspect
                                    ? "linear-gradient(135deg, #b91c1c 0%, #7f1d1d 100%)"
                                    : "linear-gradient(180deg, rgba(217,119,6,0.10) 0%, rgba(217,119,6,0.04) 100%)",
                                color: onlySuspect ? "#fff" : "#9a3412",
                                border: onlySuspect ? "1px solid #b91c1c" : "1px solid rgba(217,119,6,0.32)",
                                boxShadow: onlySuspect ? "0 4px 12px -2px rgba(185, 28, 28, 0.32)" : undefined,
                            }}
                        >
                            <AlertTriangle className="h-3.5 w-3.5" />
                            <span>
                                {onlySuspect ? "Showing " : ""}
                                <strong>{suspectCount}</strong> rows with suspect DP/MRP ratio
                                {onlySuspect ? " — click to show all" : " (click to view only these)"}
                            </span>
                        </button>
                    )}

                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={Percent}
                            iconColor="#059669"
                            title="Margin by Model"
                            subtitle={
                                filtered.length !== portfolioRows.length
                                    ? `${portfolioRows.length.toLocaleString()} models · ${filtered.length} shown`
                                    : `${portfolioRows.length.toLocaleString()} models`
                            }
                            action={
                                <>
                                    <Input
                                        placeholder="Filter Model / SKU / ASIN / Category…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => exportToXlsx(
                                            sorted as any,
                                            ["brand", "model", "sku", "asin",
                                             "category_l1", "category_l2", "product_name",
                                             "latest_fob", "dp", "mrp", "bau_deal_sp",
                                             "gross_margin", "gross_margin_pct",
                                             "net_margin", "net_margin_pct"],
                                            "margin-snapshot.xlsx",
                                        )}
                                    >
                                        <Download className="h-3.5 w-3.5" />
                                        Export
                                    </Button>
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => copyTableToClipboard(sorted as any, ["brand", "model", "sku", "asin",
                                             "category_l1", "category_l2", "product_name",
                                             "latest_fob", "dp", "mrp", "bau_deal_sp",
                                             "gross_margin", "gross_margin_pct",
                                             "net_margin", "net_margin_pct"])}
                                    >
                                        <Copy className="h-3.5 w-3.5" />
                                        Copy
                                    </Button>
                                </>
                            }
                        />

                        {sorted.length === 0 ? (
                            <div className="p-10 text-center text-sm" style={{ color: "#6b7280" }}>
                                No models match the current filter.
                            </div>
                        ) : (
                            <div className="overflow-auto max-h-[72vh]">
                                <table className="w-full text-[14px] border-separate border-spacing-0">
                                    <thead className="sticky top-0 z-30">
                                        <tr>
                                            <SortableTh sortKey="brand" label="Brand"  sort={sort} onSort={onSort} align="left" stickyLeft={0}   minWidth={120}
                                                filterValues={portfolioBrands} filterSelected={selBrands} onFilterChange={(v) => setMulti("brands", v)} />
                                            <SortableTh sortKey="model" label="Model"  sort={sort} onSort={onSort} align="left" stickyLeft={120} minWidth={140}
                                                filterValues={allModels} filterSelected={selModels} onFilterChange={(v) => setMulti("models", v)} />
                                            <SortableTh sortKey="sku"   label="SKU"    sort={sort} onSort={onSort} align="left" stickyLeft={260} minWidth={120}
                                                filterValues={allSkus}   filterSelected={selSkus}   onFilterChange={(v) => setMulti("skus", v)} />
                                            <SortableTh sortKey="asin"  label="ASIN"   sort={sort} onSort={onSort} align="left" stickyLeft={380} minWidth={120} lastFrozen
                                                filterValues={allAsins}  filterSelected={selAsins}  onFilterChange={(v) => setMulti("asins", v)} />
                                            <SortableTh sortKey="category_l1"  label="Category L1" sort={sort} onSort={onSort} align="left" minWidth={140}
                                                filterValues={allL1}     filterSelected={selL1}     onFilterChange={(v) => setMulti("cat_l1", v)} />
                                            <SortableTh sortKey="category_l2"  label="Category L2" sort={sort} onSort={onSort} align="left" minWidth={140}
                                                filterValues={allL2}     filterSelected={selL2}     onFilterChange={(v) => setMulti("cat_l2", v)} />
                                            <SortableTh sortKey="dp"           label="DP"          sort={sort} onSort={onSort} className="col-summary col-divide-l" minWidth={100}
                                                numericRange={rDp}     onNumericFilter={(r) => setRange("dp", r)}     numericPresets={[100, 500, 1000]} />
                                            <SortableTh sortKey="bau_deal_sp"  label="BAU SP"      sort={sort} onSort={onSort} className="col-summary" minWidth={110}
                                                numericRange={rBauSp}  onNumericFilter={(r) => setRange("bau_sp", r)} numericPresets={[100, 1000, 10000]} />
                                            <SortableTh sortKey="gross_margin" label="Gross ₹"     sort={sort} onSort={onSort} className="col-sales col-divide-l" minWidth={110}
                                                numericRange={rGrossR} onNumericFilter={(r) => setRange("gross_r", r)} numericPresets={[0, 100, 500]} />
                                            <SortableTh sortKey="gross_margin_pct" label="Gross %" sort={sort} onSort={onSort} className="col-sales" minWidth={110}
                                                numericRange={rGrossP} onNumericFilter={(r) => setRange("gross_p", r)} numericSuffix="%" numericPresets={[0, 10, 30]} />
                                            <SortableTh sortKey="net_margin"   label="Net ₹"       sort={sort} onSort={onSort} className="col-pct col-divide-l" minWidth={110}
                                                numericRange={rNetR}   onNumericFilter={(r) => setRange("net_r", r)}   numericPresets={[0, 100, 500]} />
                                            <SortableTh sortKey="net_margin_pct" label="Net %"     sort={sort} onSort={onSort} className="col-pct" minWidth={110}
                                                numericRange={rNetP}   onNumericFilter={(r) => setRange("net_p", r)}   numericSuffix="%" numericPresets={[0, 10, 25]} />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sorted.map((r, i) => {
                                            const npct = r.net_margin_pct;
                                            const isLoss = npct != null && (npct as number) < 0;
                                            const isLow  = npct != null && (npct as number) >= 0 && (npct as number) < 10;
                                            return (
                                                <tr key={i} className="group">
                                                    <td style={{ position: "sticky", left: 0 }}   className="border-b z-10 min-w-[120px]">{r.brand}</td>
                                                    <td style={{ position: "sticky", left: 120 }} className="font-medium border-b z-10 min-w-[140px]">
                                                        <span className="inline-flex items-center gap-1">
                                                            {r.model}
                                                            {isSuspect(r) && (
                                                                <AlertTriangle
                                                                    className="h-3 w-3 shrink-0"
                                                                    style={{ color: "#d97706" }}
                                                                    aria-label="Suspect DP/MRP ratio"
                                                                />
                                                            )}
                                                        </span>
                                                    </td>
                                                    <td style={{ position: "sticky", left: 260 }} className="border-b z-10 min-w-[120px]">{r.sku || "—"}</td>
                                                    <td style={{ position: "sticky", left: 380 }} className="border-b z-10 min-w-[120px] border-r-2 border-r-border"><AsinLink asin={r.asin} /></td>
                                                    <td className="border-b">{r.category_l1 || "—"}</td>
                                                    <td className="border-b">{r.category_l2 || "—"}</td>
                                                    <td className="text-right tabular border-b col-summary col-divide-l">{r.dp  != null ? fmtINR(r.dp as number)  : "—"}</td>
                                                    <td className="text-right tabular border-b col-summary">{r.bau_deal_sp != null ? fmtINR(r.bau_deal_sp as number) : "—"}</td>
                                                    <td className="text-right tabular border-b col-sales col-divide-l">{r.gross_margin != null ? fmtINR(r.gross_margin as number) : "—"}</td>
                                                    <td className="text-right tabular border-b col-sales">{pct(r.gross_margin_pct)}</td>
                                                    <td className="text-right tabular border-b col-pct col-divide-l font-medium">{r.net_margin != null ? fmtINR(r.net_margin as number) : "—"}</td>
                                                    <td
                                                        className="text-right tabular border-b col-pct font-semibold"
                                                        style={isLoss
                                                            ? { color: "#b91c1c", background: "#fef2f2" }
                                                            : isLow
                                                                ? { color: "#9a3412", background: "#fff7ed" }
                                                                : undefined}
                                                    >
                                                        {pct(r.net_margin_pct)}
                                                    </td>
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
