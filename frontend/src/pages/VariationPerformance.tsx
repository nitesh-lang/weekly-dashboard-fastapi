import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt } from "@/lib/utils";
import AppLayout from "@/components/AppLayout";
import { MultiPicker } from "@/components/MultiPicker";
import { FilterChipStrip, type FilterChipGroup } from "@/components/FilterChipStrip";
import { AsinLink } from "@/components/AsinLink";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Layers, ChevronRight, ChevronDown, Star } from "lucide-react";

interface MemberRow {
    asin: string;
    is_parent: boolean;
    model: string;
    gmv: number; units: number;
    spend: number; ams_sales: number;
    acos: number | null; tacos: number | null;
}
interface Family {
    parent_asin: string; title: string; brand: string;
    rank?: number | null; rating?: number | null; rating_count?: number | null;
    member_count: number; active_members: number;
    gmv: number; units: number; spend: number; ams_sales: number;
    acos: number | null; tacos: number | null; ams_share: number | null;
    members: MemberRow[];
}
interface VpData {
    weeks: number[]; selected_weeks: number[];
    brands: string[]; selected_brands: string[];
    families: Family[]; family_count: number; inactive_hidden: number;
    error?: string;
}

function pct(v: number | null | undefined): string {
    return v === null || v === undefined ? "—" : `${v.toFixed(1)}%`;
}

/** ACOS/TACOS get a quiet severity color — green ≤15, amber ≤30, red above.
 *  Thresholds mirror the AMS pages' informal bands. */
function pctClass(v: number | null | undefined): string {
    if (v === null || v === undefined) return "text-muted-foreground";
    if (v <= 15) return "text-emerald-700";
    if (v <= 30) return "text-amber-600";
    return "text-red-600";
}

export default function VariationPerformance() {
    const [weeks, setWeeks] = useState<string[]>([]);
    const [brands, setBrands] = useState<string[]>([]);
    const [search, setSearch] = useState("");
    const [open, setOpen] = useState<Set<string>>(new Set());

    const qs = new URLSearchParams();
    weeks.forEach((w) => qs.append("weeks", w));
    brands.forEach((b) => qs.append("brands", b));

    const q = useQuery<VpData>({
        queryKey: ["variation-performance", weeks, brands],
        queryFn: () => api.get(`/api/variation-performance?${qs.toString()}`),
        staleTime: 10 * 60_000,
    });

    const families = useMemo(() => {
        const all = q.data?.families ?? [];
        const needle = search.trim().toLowerCase();
        if (!needle) return all;
        return all.filter((f) =>
            f.title.toLowerCase().includes(needle) ||
            f.parent_asin.toLowerCase().includes(needle) ||
            f.members.some((m) => m.asin.toLowerCase().includes(needle) ||
                                  (m.model || "").toLowerCase().includes(needle)));
    }, [q.data, search]);

    function toggle(asin: string) {
        setOpen((prev) => {
            const next = new Set(prev);
            if (next.has(asin)) next.delete(asin); else next.add(asin);
            return next;
        });
    }

    const chips: FilterChipGroup[] = [
        { label: "Weeks", values: weeks.map((w) => `W${w}`),
          onRemove: (v) => setWeeks(weeks.filter((w) => `W${w}` !== v)),
          onClear: () => setWeeks([]) },
        { label: "Brand", values: brands,
          onRemove: (v) => setBrands(brands.filter((b) => b !== v)),
          onClear: () => setBrands([]) },
    ];

    const totals = useMemo(() => {
        const t = { gmv: 0, spend: 0, ams: 0 };
        for (const f of families) { t.gmv += f.gmv; t.spend += f.spend; t.ams += f.ams_sales; }
        return t;
    }, [families]);

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2" style={{ color: "#0e7490" }}>Weekly · Advertising</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <Layers className="h-5 w-5" style={{ color: "#0e7490" }} />
                        Variation Performance
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Sales, ad spend and efficiency rolled up by Amazon variation family
                        (Keepa parent → child ASINs). Expand a family to see which variation
                        actually pulls the weight.
                    </p>
                </div>
                <div className="flex-1" />
                <MultiPicker
                    label="Weeks"
                    options={(q.data?.weeks ?? []).map(String)}
                    selected={weeks}
                    onApply={setWeeks}
                    placeholder="Latest week"
                />
                <MultiPicker
                    label="Brand"
                    options={q.data?.brands ?? []}
                    selected={brands}
                    onApply={setBrands}
                    placeholder="All brands"
                />
                <div className="flex flex-col gap-1">
                    <span className="text-[11px] font-medium text-muted-foreground uppercase tracking-wider">Search</span>
                    <Input
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        placeholder="Title / ASIN / model…"
                        className="h-8 w-[200px] text-sm"
                    />
                </div>
            </div>

            <FilterChipStrip filters={chips} />

            {q.isLoading && <Card className="p-6"><LoadingSkeleton rows={8} /></Card>}
            {q.error && <ErrorBlock error={q.error} onRetry={() => q.refetch()} />}

            {q.data && (
                <>
                    <div className="flex items-center gap-4 text-[12.5px] text-muted-foreground tabular">
                        <span><b className="text-foreground">{families.length}</b> families with activity
                            {q.data.inactive_hidden > 0 && <> · {q.data.inactive_hidden} inactive hidden</>}</span>
                        <span>·</span>
                        <span>GMV <b className="text-foreground">{fmtINR(totals.gmv)}</b></span>
                        <span>Spend <b className="text-foreground">{fmtINR(totals.spend)}</b></span>
                        <span>AMS sales <b className="text-foreground">{fmtINR(totals.ams)}</b></span>
                        <span>·</span>
                        <span>Weeks: {q.data.selected_weeks.map((w) => `W${w}`).join(", ")}</span>
                    </div>

                    <Card className="p-0 overflow-x-auto">
                        <table className="w-full text-[13px]">
                            <thead>
                                <tr className="text-left text-[11px] uppercase tracking-wider text-muted-foreground border-b">
                                    <th className="px-3 py-2.5 w-[38%]">Family</th>
                                    <th className="px-2 py-2.5 text-right">GMV</th>
                                    <th className="px-2 py-2.5 text-right">Units</th>
                                    <th className="px-2 py-2.5 text-right">AMS Sales</th>
                                    <th className="px-2 py-2.5 text-right">Spend</th>
                                    <th className="px-2 py-2.5 text-right">ACOS</th>
                                    <th className="px-2 py-2.5 text-right">TACOS</th>
                                    <th className="px-2 py-2.5 text-right">AMS %</th>
                                    <th className="px-3 py-2.5 text-right">Variations</th>
                                </tr>
                            </thead>
                            <tbody className="tabular">
                                {families.map((f) => {
                                    const isOpen = open.has(f.parent_asin);
                                    return [
                                        <tr
                                            key={f.parent_asin}
                                            onClick={() => toggle(f.parent_asin)}
                                            className="border-b cursor-pointer hover:bg-accent/50"
                                        >
                                            <td className="px-3 py-2">
                                                <div className="flex items-center gap-1.5">
                                                    {isOpen
                                                        ? <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                                                        : <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />}
                                                    <div className="min-w-0">
                                                        <div className="truncate font-medium">{f.title || f.parent_asin}</div>
                                                        <div className="text-[11.5px] text-muted-foreground flex items-center gap-2">
                                                            <span>{f.brand}</span>
                                                            <AsinLink asin={f.parent_asin} />
                                                            {f.rating != null && (
                                                                <span className="inline-flex items-center gap-0.5">
                                                                    <Star className="h-3 w-3 fill-amber-400 text-amber-400" />
                                                                    {f.rating} ({fmtInt(f.rating_count ?? 0)})
                                                                </span>
                                                            )}
                                                        </div>
                                                    </div>
                                                </div>
                                            </td>
                                            <td className="px-2 py-2 text-right font-medium">{fmtINR(f.gmv)}</td>
                                            <td className="px-2 py-2 text-right">{fmtInt(f.units)}</td>
                                            <td className="px-2 py-2 text-right">{fmtINR(f.ams_sales)}</td>
                                            <td className="px-2 py-2 text-right">{fmtINR(f.spend)}</td>
                                            <td className={`px-2 py-2 text-right font-medium ${pctClass(f.acos)}`}>{pct(f.acos)}</td>
                                            <td className={`px-2 py-2 text-right font-medium ${pctClass(f.tacos)}`}>{pct(f.tacos)}</td>
                                            <td className="px-2 py-2 text-right text-muted-foreground">{pct(f.ams_share)}</td>
                                            <td className="px-3 py-2 text-right text-muted-foreground">
                                                {f.active_members}/{f.member_count} active
                                            </td>
                                        </tr>,
                                        isOpen && f.members.map((m) => (
                                            <tr key={`${f.parent_asin}:${m.asin}`} className="border-b bg-muted/30 text-[12.5px]">
                                                <td className="px-3 py-1.5">
                                                    <div className="flex items-center gap-2 pl-6">
                                                        <AsinLink asin={m.asin} />
                                                        {m.model && <span className="text-muted-foreground">{m.model}</span>}
                                                        {m.is_parent && (
                                                            <span className="text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded bg-primary/10 text-primary">parent</span>
                                                        )}
                                                        {m.gmv === 0 && m.spend === 0 && (
                                                            <span className="text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded bg-muted text-muted-foreground">no activity</span>
                                                        )}
                                                        {m.gmv === 0 && m.spend > 0 && (
                                                            <span className="text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded bg-red-100 text-red-700">spend, no sales</span>
                                                        )}
                                                    </div>
                                                </td>
                                                <td className="px-2 py-1.5 text-right">{fmtINR(m.gmv)}</td>
                                                <td className="px-2 py-1.5 text-right">{fmtInt(m.units)}</td>
                                                <td className="px-2 py-1.5 text-right">{fmtINR(m.ams_sales)}</td>
                                                <td className="px-2 py-1.5 text-right">{fmtINR(m.spend)}</td>
                                                <td className={`px-2 py-1.5 text-right ${pctClass(m.acos)}`}>{pct(m.acos)}</td>
                                                <td className={`px-2 py-1.5 text-right ${pctClass(m.tacos)}`}>{pct(m.tacos)}</td>
                                                <td className="px-2 py-1.5" colSpan={2}></td>
                                            </tr>
                                        )),
                                    ];
                                })}
                                {families.length === 0 && (
                                    <tr><td colSpan={9} className="px-3 py-8 text-center text-muted-foreground">
                                        No variation families match this slice.
                                    </td></tr>
                                )}
                            </tbody>
                        </table>
                    </Card>
                </>
            )}
        </AppLayout>
    );
}
