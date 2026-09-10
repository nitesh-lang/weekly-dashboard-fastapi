import { Fragment, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtINR, fmtInt } from "@/lib/utils";
import AppLayout from "@/components/AppLayout";
import { SectionHeader } from "@/components/SectionHeader";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { Card } from "@/components/ui/card";
import { IndianRupee } from "lucide-react";

interface Row { Section: string; Item: string; Amount: number; Note: string }
interface Kpis {
    sales: number; units_shipped: number; return_rate: number;
    amazon_fees: number; amazon_fees_pct: number; ads: number;
    settle: number; net_gst: number;
    contribution: number; contribution_pct: number;
    after_ads: number; after_ads_pct: number;
}
interface Data {
    accounts: string[]; months: string[]; account: string; month: string;
    pulled_at?: string; kpis: Kpis; rows: Row[]; error?: string;
}

/** Sections whose rows are counts, not money. */
const UNIT_ITEMS = new Set([
    "Units ordered (weekly report, order-date basis)",
    "Units shipped (what Amazon paid on)",
    "Ordered but not shipped",
    "Units refunded",
    "Return rate % (in-month, mixed cohorts)",
    "Event lists Amazon returned with data",
    "Event lists returned EMPTY",
]);

function Kpi({ label, value, sub, tone = "ink" }:
    { label: string; value: string; sub?: string; tone?: "ink" | "good" | "warn" }) {
    const colour = tone === "good" ? "#1e7a46" : tone === "warn" ? "#b45309" : "#0a0a0a";
    return (
        <Card className="p-4">
            <div className="text-[10.5px] font-semibold uppercase mb-2"
                style={{ letterSpacing: "0.14em", color: "#64748b" }}>{label}</div>
            <div className="tabular" style={{ fontSize: 22, fontWeight: 600, letterSpacing: "-0.014em", color: colour }}>
                {value}
            </div>
            {sub && <div className="text-[11px] text-muted-foreground mt-1">{sub}</div>}
        </Card>
    );
}

export default function Reconciliation() {
    const [params, setParams] = useSearchParams();
    const account = params.get("account") || "";
    const month = params.get("month") || "";
    const qs = useMemo(() => {
        const p: string[] = [];
        if (account) p.push(`account=${encodeURIComponent(account)}`);
        if (month) p.push(`month=${encodeURIComponent(month)}`);
        return p.join("&");
    }, [account, month]);

    const { data, isLoading, error } = useQuery<Data>({
        queryKey: ["reconciliation", qs],
        queryFn: () => api.get(`/api/reconciliation${qs ? `?${qs}` : ""}`),
    });

    const set = (k: string, v: string) => {
        const u = new URLSearchParams(params);
        if (v) u.set(k, v); else u.delete(k);
        if (k === "account") u.delete("month");
        setParams(u, { replace: false });
    };

    if (isLoading) return <AppLayout><LoadingSkeleton /></AppLayout>;
    if (error) return <AppLayout><ErrorBlock error={error} /></AppLayout>;
    if (!data || data.error) {
        return (
            <AppLayout>
                <SectionHeader icon={IndianRupee} title="Account Reconciliation" subtitle="" />
                <Card className="p-6 text-sm text-muted-foreground">
                    {data?.error || "No reconciliation available yet."}
                </Card>
            </AppLayout>
        );
    }

    const k = data.kpis;
    const sections = Array.from(new Set(data.rows.map((r) => r.Section)));

    return (
        <AppLayout>
            <SectionHeader
                icon={IndianRupee}
                title="Account Reconciliation"
                subtitle={`Every rupee Amazon reported — ${data.account} · ${data.month}${data.pulled_at ? ` · pulled ${data.pulled_at}` : ""}`}
            />

            <div className="flex flex-wrap gap-2 mb-4">
                <select className="h-9 rounded-md border px-3 text-sm bg-background"
                    value={data.account} onChange={(e) => set("account", e.target.value)}>
                    {data.accounts.map((a) => <option key={a} value={a}>{a}</option>)}
                </select>
                <select className="h-9 rounded-md border px-3 text-sm bg-background"
                    value={data.month} onChange={(e) => set("month", e.target.value)}>
                    {data.months.map((m) => <option key={m} value={m}>{m}</option>)}
                </select>
            </div>

            <div className="grid gap-3 mb-6" style={{ gridTemplateColumns: "repeat(auto-fit,minmax(190px,1fr))" }}>
                <Kpi label="Sales (GST not incl.)" value={fmtINR(k.sales)}
                    sub={`${fmtInt(k.units_shipped)} units shipped`} />
                <Kpi label="Amazon fees" value={fmtINR(k.amazon_fees)}
                    sub={`${k.amazon_fees_pct.toFixed(1)}% of sales`} tone="warn" />
                <Kpi label="Advertising" value={fmtINR(k.ads)}
                    sub="billed outside settlement" tone="warn" />
                <Kpi label="Amazon should pay" value={fmtINR(k.settle)}
                    sub={`less ${fmtINR(Math.abs(k.net_gst))} GST to remit`} />
                <Kpi label="Contribution before COGS" value={fmtINR(k.contribution)}
                    sub={`${k.contribution_pct.toFixed(1)}% of sales`} tone="good" />
                <Kpi label="After advertising" value={fmtINR(k.after_ads)}
                    sub={`${k.after_ads_pct.toFixed(1)}% — not profit, COGS excluded`} tone="good" />
            </div>

            <Card className="overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full text-[12.5px] tabular">
                        <thead>
                            <tr className="border-b bg-muted/40 text-[10px] uppercase tracking-wider text-muted-foreground">
                                <th className="px-3 py-2 text-left font-bold">Line</th>
                                <th className="px-3 py-2 text-right font-bold">Amount</th>
                                <th className="px-3 py-2 text-left font-bold">What it means</th>
                            </tr>
                        </thead>
                        <tbody>
                            {sections.map((sec) => (
                                <Fragment key={sec}>
                                    <tr className="bg-background">
                                        <td colSpan={3}
                                            className="px-3 pt-4 pb-1 text-[10px] font-bold uppercase tracking-[0.09em] text-muted-foreground">
                                            {sec.replace(/^\d+\.\s*/, "")}
                                        </td>
                                    </tr>
                                    {data.rows.filter((r) => r.Section === sec).map((r, i) => {
                                        const isTotal = /BOTTOM LINE/i.test(sec);
                                        const isUnits = UNIT_ITEMS.has(r.Item);
                                        const neg = r.Amount < 0;
                                        return (
                                            <tr key={`${sec}-${i}`}
                                                className={"border-b border-[hsl(var(--hairline))] " + (isTotal ? "font-semibold" : "")}>
                                                <td className="px-3 py-1.5 font-sans">{r.Item}</td>
                                                <td className={"px-3 py-1.5 text-right whitespace-nowrap " + (neg ? "text-[hsl(var(--rose))]" : "")}>
                                                    {isUnits ? fmtInt(r.Amount) : fmtINR(r.Amount)}
                                                </td>
                                                <td className="px-3 py-1.5 font-sans text-[11.5px] text-muted-foreground">{r.Note}</td>
                                            </tr>
                                        );
                                    })}
                                </Fragment>
                            ))}
                        </tbody>
                    </table>
                </div>
            </Card>
        </AppLayout>
    );
}
