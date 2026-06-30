import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR } from "@/lib/utils";
import { useSortedRows } from "@/lib/useSortedRows";
import { useDebouncedUrlParam } from "@/lib/useDebouncedUrlParam";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { FilterChipStrip, type FilterChipGroup } from "@/components/FilterChipStrip";
import { SortableTh } from "@/components/SortableTh";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { ExportButtons } from "@/components/ExportButtons";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Percent } from "lucide-react";

interface PriceRow {
    asin: string;
    sku: string;
    brand: string;
    model: string;
    amazon_1p_price: number | null;
    buybox_price: number | null;
    buybox_seller_id: string | null;
    buybox_belongs_to_us: boolean;
    currency: string;
    [accountLabel: string]: any;   // dynamic per-account columns
}

interface PriceResponse {
    rows: PriceRow[];
    accounts: string[];          // ordered list of per-account column labels
    fetched_at: string;
    empty_reason?: string;
    error?: string;
}

function fmtMoney(v: number | null, currency: string): string {
    if (v == null || isNaN(v)) return "—";
    if (currency === "INR") return fmtINR(v);
    return `${currency} ${Number(v).toFixed(2)}`;
}

function fmtRelative(iso: string): string {
    if (!iso) return "never";
    const t = Date.parse(iso);
    if (isNaN(t)) return iso;
    const mins = Math.round((Date.now() - t) / 60_000);
    if (mins < 60) return `${mins} min ago`;
    const hrs = Math.round(mins / 60);
    if (hrs < 48) return `${hrs} h ago`;
    const days = Math.round(hrs / 24);
    return `${days} d ago`;
}

export default function Price() {
    const [params, setParams] = useSearchParams();
    const qsKey = params.toString();
    const selBrands = useMemo(() => params.getAll("brands").filter(Boolean), [qsKey]);
    const selModels = useMemo(() => params.getAll("models").filter(Boolean), [qsKey]);
    const selAsins  = useMemo(() => params.getAll("asins").filter(Boolean),  [qsKey]);
    const [filter, setFilter] = useDebouncedUrlParam("q");

    function setMulti(name: string, values: string[]) {
        const next = new URLSearchParams(params);
        next.delete(name);
        values.forEach((v) => next.append(name, v));
        setParams(next, { replace: false });
    }

    const { data, isLoading, error } = useQuery<PriceResponse>({
        queryKey: ["pricing"],
        queryFn: () => api.get("/api/pricing"),
        staleTime: 5 * 60_000,
    });

    const allRows = data?.rows ?? [];
    const accounts = data?.accounts ?? [];

    const allBrands = useMemo(
        () => Array.from(new Set(allRows.map((r) => r.brand).filter(Boolean))).sort(),
        [allRows]
    );
    const allModels = useMemo(
        () => Array.from(new Set(allRows.map((r) => r.model).filter(Boolean))).sort(),
        [allRows]
    );
    const allAsins = useMemo(
        () => Array.from(new Set(allRows.map((r) => r.asin).filter(Boolean))).sort(),
        [allRows]
    );

    const filtered = useMemo(() => {
        const q = filter.trim().toLowerCase();
        const brandSet = new Set(selBrands);
        const modelSet = new Set(selModels);
        const asinSet  = new Set(selAsins);
        return allRows.filter((r) => {
            if (brandSet.size && !brandSet.has(r.brand)) return false;
            if (modelSet.size && !modelSet.has(r.model)) return false;
            if (asinSet.size  && !asinSet.has(r.asin))  return false;
            if (!q) return true;
            return ((r.asin || "") + " " + (r.sku || "") + " " + (r.model || "") + " " + (r.brand || ""))
                .toLowerCase()
                .includes(q);
        });
    }, [allRows, filter, selBrands, selModels, selAsins]);

    const { sorted, sort, onSort } = useSortedRows<PriceRow>(filtered, {
        key: "brand", dir: "asc",
    });

    // KPI: count of ASINs with at least one listed price per account
    const accountCoverage = useMemo(() => {
        return accounts.map((label) => ({
            label,
            listed: allRows.filter((r) => typeof r[label] === "number").length,
            total: allRows.length,
        }));
    }, [accounts, allRows]);

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2">Pricing · Live SP</div>
                    <h1 className="text-2xl font-semibold tracking-tight">Price</h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Each of our seller accounts' currently listed Consumer price (SP-API
                        Product Pricing) for every non-Fossil ASIN in <code>sku_master</code>.
                        <span className="ml-2 text-muted-foreground">
                            Snapshot: <strong>{fmtRelative(data?.fetched_at || "")}</strong>
                        </span>
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker label="Brands" options={allBrands} selected={selBrands} onApply={(v) => setMulti("brands", v)} />
                <MultiPicker label="Models" options={allModels} selected={selModels} onApply={(v) => setMulti("models", v)} />
                <MultiPicker label="ASINs"  options={allAsins}  selected={selAsins}  onApply={(v) => setMulti("asins", v)} />
            </div>

            <FilterChipStrip
                filters={[
                    { label: "Brand", values: selBrands, onRemove: (v) => setMulti("brands", selBrands.filter((x) => x !== v)), onClear: () => setMulti("brands", []) },
                    { label: "Model", values: selModels, onRemove: (v) => setMulti("models", selModels.filter((x) => x !== v)), onClear: () => setMulti("models", []) },
                    { label: "ASIN",  values: selAsins,  onRemove: (v) => setMulti("asins",  selAsins.filter((x) => x !== v)),  onClear: () => setMulti("asins", []) },
                ] as FilterChipGroup[]}
            />

            {isLoading && <LoadingSkeleton rows={8} />}
            {error && <ErrorBlock error={error} />}

            {data && data.empty_reason && allRows.length === 0 && (
                <Card className="p-6 text-sm text-muted-foreground">
                    {data.empty_reason}
                </Card>
            )}

            {data && allRows.length > 0 && (
                <>
                    <Card className="px-5 py-4">
                        <div className="grid grid-cols-2 md:grid-cols-5 gap-6">
                            {accountCoverage.map((a) => (
                                <div key={a.label}>
                                    <div className="text-[11px] uppercase tracking-wider text-muted-foreground font-medium">
                                        {a.label}
                                    </div>
                                    <div className="text-2xl font-semibold tabular tracking-tight mt-1">
                                        {a.listed.toLocaleString()}
                                    </div>
                                    <div className="text-[11px] text-muted-foreground mt-0.5">
                                        listed of {a.total.toLocaleString()}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </Card>

                    <Card className="overflow-hidden">
                        <SectionHeader
                            icon={Percent}
                            iconColor="#0891b2"
                            title="Per-account Selling Price"
                            subtitle={`${allRows.length.toLocaleString()} ASINs · ${filtered.length.toLocaleString()} shown`}
                            action={
                                <>
                                    <Input
                                        placeholder="Filter ASIN / SKU / Model / Brand…"
                                        value={filter}
                                        onChange={(e) => setFilter(e.target.value)}
                                        className="max-w-xs h-8 text-sm"
                                    />
                                    <ExportButtons
                                        rows={sorted as any}
                                        columns={["asin", "sku", "brand", "model",
                                            ...accounts, "amazon_1p_price",
                                            "buybox_price", "buybox_seller_id", "buybox_belongs_to_us", "currency"]}
                                        filename="price-snapshot.xlsx"
                                    />
                                </>
                            }
                        />
                        <div className="overflow-auto max-h-[72vh]">
                            <table className="w-full text-[13px] border-separate border-spacing-0">
                                <thead className="sticky top-0 z-30">
                                    <tr>
                                        <SortableTh sortKey="asin"  label="ASIN"  sort={sort} onSort={onSort} align="left" stickyLeft={0}   minWidth={120} />
                                        <SortableTh sortKey="sku"   label="SKU"   sort={sort} onSort={onSort} align="left" stickyLeft={120} minWidth={120} />
                                        <SortableTh sortKey="model" label="Model" sort={sort} onSort={onSort} align="left" stickyLeft={240} minWidth={130} />
                                        <SortableTh sortKey="brand" label="Brand" sort={sort} onSort={onSort} align="left" stickyLeft={370} minWidth={140} lastFrozen />
                                        {accounts.map((label) => (
                                            <SortableTh
                                                key={label} sortKey={label} label={label}
                                                sort={sort} onSort={onSort} align="right"
                                                className="col-sales"
                                            />
                                        ))}
                                        <SortableTh sortKey="amazon_1p_price"  label="Amazon 1P ₹"  sort={sort} onSort={onSort} align="right" className="col-summary col-divide-l" />
                                        <SortableTh sortKey="buybox_price"     label="Buy Box ₹"    sort={sort} onSort={onSort} align="right" className="col-summary" />
                                        <SortableTh sortKey="buybox_seller_id" label="Buy Box Seller" sort={sort} onSort={onSort} align="left" />
                                    </tr>
                                </thead>
                                <tbody>
                                    {sorted.map((r, i) => (
                                        <tr key={r.asin + "_" + i} className="group">
                                            <td className="px-3 py-2 whitespace-nowrap border-b bg-background group-hover:bg-accent/40" style={{ position: "sticky", left: 0 }}>{r.asin || "—"}</td>
                                            <td className="px-3 py-2 whitespace-nowrap border-b bg-background group-hover:bg-accent/40" style={{ position: "sticky", left: 120 }}>{r.sku || "—"}</td>
                                            <td className="px-3 py-2 whitespace-nowrap border-b bg-background group-hover:bg-accent/40" style={{ position: "sticky", left: 240 }}>{r.model || "—"}</td>
                                            <td className="px-3 py-2 whitespace-nowrap border-b bg-background group-hover:bg-accent/40 border-r-2 border-r-border" style={{ position: "sticky", left: 370 }}>{r.brand || "—"}</td>
                                            {accounts.map((label) => (
                                                <td key={label} className="px-3 py-2 text-right tabular whitespace-nowrap border-b col-sales group-hover:bg-accent/40">
                                                    {fmtMoney(r[label] as number | null, r.currency)}
                                                </td>
                                            ))}
                                            <td className="px-3 py-2 text-right tabular whitespace-nowrap border-b col-summary col-divide-l group-hover:bg-accent/40">
                                                {fmtMoney(r.amazon_1p_price, r.currency)}
                                            </td>
                                            <td className="px-3 py-2 text-right tabular whitespace-nowrap border-b col-summary group-hover:bg-accent/40">
                                                {fmtMoney(r.buybox_price, r.currency)}
                                            </td>
                                            <td className="px-3 py-2 whitespace-nowrap border-b group-hover:bg-accent/40">
                                                {r.buybox_seller_id
                                                    ? (
                                                        <span className={r.buybox_belongs_to_us ? "text-emerald-700 font-medium" : "text-muted-foreground"}>
                                                            {r.buybox_seller_id}
                                                            {r.buybox_belongs_to_us ? "  · ours" : ""}
                                                        </span>
                                                    )
                                                    : "—"}
                                            </td>
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
