import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Brain, ChevronDown, ChevronUp, TrendingUp, AlertTriangle, CheckCircle2, Activity, Boxes, BarChart3 } from "lucide-react";

/* Sales Trend insight panel — facts-only weekly read backed by
   /api/sales-trend/insights. Same payload shape as AmsInsightsPanel
   so the visual logic stays identical; differs only in endpoint,
   queryKey, and the "Inventory" section label (no "× Ads" here). */

interface Insight {
    kind: "positive" | "negative" | "warning" | "neutral" | "action";
    text: string;
    metric: string | null;
    delta_pct: number | null;
    weight: number;
}

interface InsightsPayload {
    window: { weeks: number[]; weeks_count: number; label: string };
    headline:   Insight[];
    efficiency: Insight[];
    movers:     Insight[];
    anomalies:  Insight[];
    inventory:  Insight[];
    trajectory: Insight[];
    actions:    Insight[];
    empty_reason?: string;
}

const KIND_STYLE: Record<Insight["kind"], { dot: string; rail: string; bg: string; text: string }> = {
    positive: { dot: "bg-emerald-500", rail: "border-emerald-500", bg: "bg-emerald-50/60",   text: "text-emerald-900" },
    negative: { dot: "bg-rose-500",    rail: "border-rose-500",    bg: "bg-rose-50/60",      text: "text-rose-900" },
    warning:  { dot: "bg-amber-500",   rail: "border-amber-500",   bg: "bg-amber-50/60",     text: "text-amber-900" },
    neutral:  { dot: "bg-slate-400",   rail: "border-slate-300",   bg: "bg-slate-50/60",     text: "text-slate-800" },
    action:   { dot: "bg-indigo-500",  rail: "border-indigo-500",  bg: "bg-indigo-50/60",    text: "text-indigo-900" },
};

const SECTION_META: Record<string, { label: string; icon: React.ComponentType<{ className?: string }> }> = {
    headline:   { label: "Headline",   icon: BarChart3 },
    movers:     { label: "Movers",     icon: TrendingUp },
    anomalies:  { label: "Anomalies",  icon: AlertTriangle },
    inventory:  { label: "Inventory",  icon: Boxes },
    trajectory: { label: "Trajectory", icon: Activity },
    actions:    { label: "Next actions", icon: CheckCircle2 },
};

function renderInline(text: string): React.ReactNode {
    const parts: React.ReactNode[] = [];
    let i = 0;
    let key = 0;
    while (i < text.length) {
        const open = text.indexOf("**", i);
        if (open === -1) {
            parts.push(<span key={key++}>{text.slice(i)}</span>);
            break;
        }
        if (open > i) {
            parts.push(<span key={key++}>{text.slice(i, open)}</span>);
        }
        const close = text.indexOf("**", open + 2);
        if (close === -1) {
            parts.push(<span key={key++}>{text.slice(open)}</span>);
            break;
        }
        parts.push(
            <strong key={key++} className="font-semibold tracking-tight">
                {text.slice(open + 2, close)}
            </strong>
        );
        i = close + 2;
    }
    return parts;
}

function InsightRow({ item }: { item: Insight }) {
    const s = KIND_STYLE[item.kind];
    return (
        <div className={`flex items-start gap-3 px-3 py-2.5 border-l-2 ${s.rail} ${s.bg}`}>
            <span className={`mt-1.5 inline-block h-2 w-2 rounded-full ${s.dot} flex-shrink-0`} />
            <p className={`text-[13px] leading-snug ${s.text}`}>{renderInline(item.text)}</p>
        </div>
    );
}

function Section({ name, items }: { name: keyof InsightsPayload; items: Insight[] }) {
    if (!items?.length) return null;
    const meta = SECTION_META[name as string];
    if (!meta) return null;
    const Icon = meta.icon;
    return (
        <div className="space-y-1">
            <div className="flex items-center gap-2 px-1 mb-1">
                <Icon className="h-3.5 w-3.5 text-slate-500" />
                <span
                    className="text-[10px] font-semibold uppercase text-slate-500"
                    style={{ letterSpacing: "0.16em" }}
                >
                    {meta.label}
                </span>
                <span className="text-[10px] text-slate-400 ml-auto tabular-nums">
                    {items.length} {items.length === 1 ? "note" : "notes"}
                </span>
            </div>
            <div className="space-y-1.5">
                {items.map((i, idx) => (
                    <InsightRow key={idx} item={i} />
                ))}
            </div>
        </div>
    );
}

export function SalesInsightsPanel({ qs }: { qs: string }) {
    const [open, setOpen] = useState(true);

    const { data, isLoading, error } = useQuery<InsightsPayload>({
        queryKey: ["sales-trend-insights", qs],
        queryFn: () => api.get(`/api/sales-trend/insights${qs ? `?${qs}` : ""}`),
        staleTime: 30_000,
    });

    const totalCount = useMemo(() => {
        if (!data) return 0;
        return (data.headline?.length || 0)
             + (data.movers?.length || 0)
             + (data.anomalies?.length || 0)
             + (data.inventory?.length || 0)
             + (data.trajectory?.length || 0)
             + (data.actions?.length || 0);
    }, [data]);

    return (
        <Card className="overflow-hidden">
            <div
                className="flex items-center gap-3 px-4 py-3 border-b border-slate-200/80"
                style={{ background: "linear-gradient(135deg, #ecfdf5 0%, #f5fffa 60%, #ffffff 100%)" }}
            >
                <div className="flex items-center justify-center h-8 w-8 rounded-md bg-emerald-600 text-white">
                    <Brain className="h-4 w-4" />
                </div>
                <div className="flex flex-col">
                    <div className="text-[14px] font-semibold text-slate-900 tracking-tight">
                        Performance read
                    </div>
                    <div
                        className="text-[10px] font-medium uppercase text-emerald-700 tabular-nums"
                        style={{ letterSpacing: "0.12em" }}
                    >
                        {data?.window?.label
                            ? `${data.window.label} · ${data.window.weeks_count}wk window`
                            : isLoading
                                ? "Reading…"
                                : "Pick a week to read"}
                        {!isLoading && totalCount > 0 && (
                            <span className="ml-2 text-slate-400 normal-case tracking-normal">
                                · {totalCount} observation{totalCount !== 1 ? "s" : ""}
                            </span>
                        )}
                    </div>
                </div>
                <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setOpen((s) => !s)}
                    className="ml-auto"
                >
                    {open
                        ? <><ChevronUp className="h-3.5 w-3.5" />Collapse</>
                        : <><ChevronDown className="h-3.5 w-3.5" />Expand</>}
                </Button>
            </div>

            {open && (
                <div className="p-4">
                    {error && (
                        <div className="text-[12px] text-rose-600 px-3 py-2 rounded bg-rose-50 border border-rose-200">
                            Couldn't load the read — endpoint returned an error.
                        </div>
                    )}
                    {!error && isLoading && !data && (
                        <div className="text-[12px] text-slate-500 px-3 py-4">
                            Reading the numbers…
                        </div>
                    )}
                    {!error && data && totalCount === 0 && (
                        <div className="text-[12px] text-slate-500 px-3 py-4">
                            {data.empty_reason || "No facts surfaced for the current selection — widen the filters."}
                        </div>
                    )}
                    {!error && data && totalCount > 0 && (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-4">
                            <div className="space-y-4">
                                <Section name="headline"   items={data.headline} />
                                <Section name="trajectory" items={data.trajectory} />
                            </div>
                            <div className="space-y-4">
                                <Section name="movers"     items={data.movers} />
                                <Section name="anomalies"  items={data.anomalies} />
                                <Section name="inventory"  items={data.inventory} />
                                <Section name="actions"    items={data.actions} />
                            </div>
                        </div>
                    )}
                </div>
            )}
        </Card>
    );
}
