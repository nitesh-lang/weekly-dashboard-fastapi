import { Fragment, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtInt } from "@/lib/utils";
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
    channel_contribution: number; channel_contribution_pct: number;
    pat: number; pat_pct: number;
}
interface Data {
    accounts: string[]; months: string[]; account: string; month: string;
    pulled_at?: string; kpis: Kpis; rows: Row[]; error?: string;
}

/** Rows that are counts, not money. */
const UNIT_ITEMS = new Set([
    "Units ordered in the month", "  less cancelled", "  less pending (payment not authorised)",
    "  less unfulfillable", "  less not shipped by month end",
    "= Shipped from this month's orders",
    "Units ordered (weekly report, order-date basis)",
    "Units shipped (what Amazon paid on)", "Units shipped for other channels (MCF)",
    "Ordered but not shipped", "Units refunded",
    "Event lists Amazon returned with data", "Event lists returned EMPTY",
    "Groups re-derived from their own events",
    "Units this month's shipments will return",
]);
/** Rows that carry a % rather than a rupee value. */
const PCT_ITEMS = new Set([
    "Return rate % (in-month, mixed cohorts)",
    "Return rate used above (in-month)",
    "Returns already arrived from this month's orders",
    "EXPECTED LIFETIME return rate for this month's orders",
]);

/** Accounting presentation: negatives in parentheses, never a minus sign. */
function acct(v: number): string {
    const n = Math.round(Math.abs(v)).toLocaleString("en-IN");
    return v < 0 ? `(${n})` : n;
}

/** A total is a line the reader should stop on — Amazon writes them in caps. */
const isTotal = (item: string) => item === item.toUpperCase() && /[A-Z]{4}/.test(item);
const isGrand = (item: string) =>
    /PROFIT AFTER TAX|AMAZON SHOULD PAY US|AMAZON CHANNEL CONTRIBUTION/.test(item);
const isIndent = (item: string) => item.startsWith("  ") || item.startsWith("= ");

function Kpi({ label, value, sub, tone = "ink" }:
    { label: string; value: string; sub?: string; tone?: "ink" | "good" | "warn" }) {
    const colour = tone === "good" ? "#1e7a46" : tone === "warn" ? "#9a3412" : "#0f172a";
    return (
        <div className="px-4 py-3 border-r last:border-r-0 border-[hsl(var(--hairline))]">
            <div className="text-[9.5px] font-semibold uppercase mb-1.5"
                style={{ letterSpacing: "0.15em", color: "#64748b" }}>{label}</div>
            <div className="tabular" style={{ fontSize: 19, fontWeight: 600, letterSpacing: "-0.02em", color: colour }}>
                {value}
            </div>
            {sub && <div className="text-[10.5px] text-muted-foreground mt-0.5">{sub}</div>}
        </div>
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
    const monthLabel = (() => {
        const [y, m] = data.month.split("-").map(Number);
        return isNaN(y) ? data.month
            : new Date(y, m - 1, 1).toLocaleDateString("en-IN", { month: "long", year: "numeric" });
    })();

    return (
        <AppLayout>
            <SectionHeader
                icon={IndianRupee}
                title="Account Reconciliation"
                subtitle={`Every rupee Amazon reported${data.pulled_at ? ` · pulled ${data.pulled_at}` : ""}`}
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

            <Card className="mb-5 overflow-hidden">
                <div className="grid" style={{ gridTemplateColumns: "repeat(auto-fit,minmax(165px,1fr))" }}>
                    <Kpi label="Net sales" value={acct(k.sales)}
                        sub={`${fmtInt(k.units_shipped)} units · GST not included`} />
                    <Kpi label="Amazon fees" value={acct(k.amazon_fees)}
                        sub={`${k.amazon_fees_pct.toFixed(1)}% of sales`} tone="warn" />
                    <Kpi label="Advertising" value={acct(-Math.abs(k.ads))}
                        sub="GST-inclusive, billed apart" tone="warn" />
                    <Kpi label="Amazon should pay" value={acct(k.settle)}
                        sub={`incl. ${acct(Math.abs(k.net_gst))} GST to remit`} />
                    <Kpi label="Channel contribution" value={acct(k.channel_contribution)}
                        sub={`${k.channel_contribution_pct.toFixed(1)}% — before overhead`} tone="good" />
                    <Kpi label="Profit after tax" value={acct(k.pat)}
                        sub={`${k.pat_pct.toFixed(1)}% of sales`} tone={k.pat >= 0 ? "good" : "warn"} />
                </div>
            </Card>

            <Card className="overflow-hidden">
                <div className="px-5 py-4 border-b border-[hsl(var(--hairline))]">
                    <div className="text-[15px] font-semibold tracking-tight">{data.account}</div>
                    <div className="text-[11.5px] text-muted-foreground">
                        Statement of Amazon 3P marketplace activity for {monthLabel} · amounts in ₹ ·
                        figures in brackets are deductions
                    </div>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-[12.5px]" style={{ borderCollapse: "collapse" }}>
                        <colgroup>
                            <col style={{ width: "38%" }} />
                            <col style={{ width: "15%" }} />
                            <col style={{ width: "8%" }} />
                            <col />
                        </colgroup>
                        <thead>
                            <tr className="text-[9.5px] uppercase tracking-[0.12em] text-muted-foreground">
                                <th className="px-5 py-2 text-left font-semibold border-b border-foreground/25">Particulars</th>
                                <th className="px-3 py-2 text-right font-semibold border-b border-foreground/25">Amount</th>
                                <th className="px-3 py-2 text-right font-semibold border-b border-foreground/25">% Sales</th>
                                <th className="px-4 py-2 text-left font-semibold border-b border-foreground/25">Basis</th>
                            </tr>
                        </thead>
                        <tbody>
                            {sections.map((sec) => (
                                <Fragment key={sec}>
                                    <tr>
                                        <td colSpan={4}
                                            className="px-5 pt-5 pb-1 text-[10px] font-bold uppercase tracking-[0.14em]"
                                            style={{ color: "#475569" }}>
                                            {sec.replace(/^\d+\.\s*/, "")}
                                        </td>
                                    </tr>
                                    {data.rows.filter((r) => r.Section === sec).map((r, i) => {
                                        const units = UNIT_ITEMS.has(r.Item);
                                        const pct = PCT_ITEMS.has(r.Item);
                                        const total = isTotal(r.Item);
                                        const grand = isGrand(r.Item);
                                        const showPct = !units && !pct && k.sales > 0 &&
                                            Math.abs(r.Amount) > 0 && !r.Item.startsWith("Event lists");
                                        return (
                                            <tr key={`${sec}-${i}`}>
                                                <td className={"px-5 py-[5px] " + (total ? "font-semibold" : "") +
                                                    (isIndent(r.Item) ? " pl-9" : "")}
                                                    style={total ? { letterSpacing: "0.01em" } : undefined}>
                                                    {r.Item.trim()}
                                                </td>
                                                <td className={"px-3 py-[5px] text-right tabular whitespace-nowrap " +
                                                    (total ? "font-semibold " : "")}
                                                    style={{
                                                        borderTop: total ? "1px solid hsl(var(--foreground)/0.3)" : undefined,
                                                        borderBottom: grand ? "3px double hsl(var(--foreground)/0.55)" : undefined,
                                                    }}>
                                                    {units ? fmtInt(r.Amount)
                                                        : pct ? `${r.Amount.toFixed(1)}%`
                                                            : acct(r.Amount)}
                                                </td>
                                                <td className="px-3 py-[5px] text-right tabular text-[11px] text-muted-foreground whitespace-nowrap">
                                                    {showPct ? `${(Math.abs(r.Amount) / k.sales * 100).toFixed(1)}%` : ""}
                                                </td>
                                                <td className="px-4 py-[5px] text-[11px] leading-snug text-muted-foreground">
                                                    {r.Note}
                                                </td>
                                            </tr>
                                        );
                                    })}
                                </Fragment>
                            ))}
                        </tbody>
                    </table>
                </div>
                <div className="px-5 py-3 border-t border-[hsl(var(--hairline))] text-[11px] text-muted-foreground">
                    Source: Amazon SP-API Finances v0 financial events for the calendar month, tied to
                    Amazon's own settlement-cycle totals. Landed cost from the margin master; advertising
                    from our AMS data. GST on fees and on advertising is claimed as input credit, so it is
                    not a cost.
                </div>
            </Card>
        </AppLayout>
    );
}
