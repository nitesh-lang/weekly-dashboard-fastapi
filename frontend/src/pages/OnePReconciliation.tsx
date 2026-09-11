/**
 * 1P Reconciliation — "the journey" (direction A).
 *
 * Every label on this page is the plain-English one. Amazon's own vocabulary
 * describes Amazon's paperwork, not our reality: a "purchase order" is a
 * shopping list, "accepted" is what we promised, "received" is what they say
 * turned up. The glossary lives in the page, not in someone's head.
 *
 * The one thing this page exists to make impossible to misread: "not arrived"
 * is TWO different things. Rs1.53Cr still travelling is normal; Rs18.0L
 * promised a month ago and still not counted is worth chasing. Showing them as
 * one number is what made this data useless before.
 */
import { useEffect, useState } from "react";
import { Truck } from "lucide-react";

import { SectionHeader } from "@/components/SectionHeader";

interface Cell { units: number; value: number }
interface Brand {
    brand: string;
    promised_units: number; promised_value: number;
    arrived_units: number; arrived_value: number;
    missing_units: number; missing_value: number;
    fill_pct: number;
}
interface Data {
    labels: string[]; months: string[]; label: string; month: string;
    pulled_at?: string; age_days: number;
    steps: Record<string, Cell>; brands: Brand[]; error?: string;
}

/** Indian grouping, no decimals — these are lakhs and crores, not paise. */
const inr = (v: number) => "₹" + Math.round(Math.abs(v)).toLocaleString("en-IN");
const units = (v: number) => Math.round(v).toLocaleString("en-IN");
/** Rs1,71,04,120 is unreadable at a glance; "Rs1.71 Cr" is not. */
const short = (v: number) => {
    const a = Math.abs(v);
    if (a >= 1e7) return "₹" + (a / 1e7).toFixed(2) + " Cr";
    if (a >= 1e5) return "₹" + (a / 1e5).toFixed(2) + " L";
    return inr(a);
};

function Step({ label, hint, value, unitCount, pct, tone }: {
    label: string; hint: string; value: number; unitCount: number;
    pct: number; tone: "accent" | "good" | "warn" | "bad";
}) {
    const bar = { accent: "#0f6e6e", good: "#2e7d52", warn: "#b8791c", bad: "#b23a3a" }[tone];
    return (
        <div className="grid items-center gap-4"
             style={{ gridTemplateColumns: "minmax(150px,210px) 1fr auto" }}>
            <div>
                <div className="text-[15px] leading-tight">{label}</div>
                <div className="text-[11.5px] text-muted-foreground">{hint}</div>
            </div>
            <div className="h-8 rounded-md border bg-muted/40 relative overflow-hidden">
                <div className="absolute inset-y-0 left-0 rounded-l-md"
                     style={{ width: `${Math.max(0, Math.min(100, pct))}%`, background: bar }} />
            </div>
            <div className="text-right tabular whitespace-nowrap">
                <div className="text-[14px] font-medium">{inr(value)}</div>
                <div className="text-[11px] text-muted-foreground">{units(unitCount)} items</div>
            </div>
        </div>
    );
}

export default function OnePReconciliation() {
    const [data, setData] = useState<Data | null>(null);
    const [label, setLabel] = useState<string>("");
    const [month, setMonth] = useState<string>("");
    const [err, setErr] = useState<string>("");

    useEffect(() => {
        const q = new URLSearchParams();
        if (label) q.set("label", label);
        if (month) q.set("month", month);
        fetch(`/api/1p-reconciliation?${q}`, { credentials: "include" })
            .then(r => r.ok ? r.json() : r.json().then(j => Promise.reject(j.detail || "Failed")))
            .then(setData).catch(e => setErr(String(e)));
    }, [label, month]);

    if (err) return <div className="p-8 text-sm text-destructive">{err}</div>;
    if (!data) return <div className="p-8 text-sm text-muted-foreground">Loading…</div>;
    if (data.error) return <div className="p-8 text-sm text-muted-foreground">{data.error}</div>;

    const s = data.steps;
    const asked = s.asked?.value || 0;
    const pct = (v: number) => (asked ? (v / asked) * 100 : 0);
    // The two halves of "not arrived", as a share of what has NOT arrived —
    // so the reader sees how little of it is actually a problem.
    const na = s.not_arrived?.value || 1;

    return (
        /* Capped width on purpose. Left unbounded, the 1fr bar column stretches
           to ~900px on a wide monitor and strands each number far from the
           label it belongs to - the exact failure this page exists to avoid. */
        <div className="mx-auto max-w-[1120px] space-y-6 p-6">
            <SectionHeader icon={Truck} title="1P Reconciliation"
                subtitle={`What Amazon ordered from us, and how much of it actually reached them · ${data.month}`} />

            <div className="flex flex-wrap gap-2">
                {/* Native selects on purpose: this page sits next to
                    /reconciliation, which uses exactly these. A third picker
                    pattern would be worse than the rule it satisfies. */}
                <select className="h-9 rounded-md border px-3 text-sm bg-background"
                        value={data.label} onChange={e => setLabel(e.target.value)}>
                    {data.labels.map(l => <option key={l} value={l}>{l.replace(/_/g, " ")}</option>)}
                </select>
                <select className="h-9 rounded-md border px-3 text-sm bg-background"
                        value={data.month} onChange={e => setMonth(e.target.value)}>
                    {data.months.map(m => <option key={m} value={m}>{m}</option>)}
                </select>
                <span className="self-center text-xs text-muted-foreground">
                    {units(s.pos?.units || 0)} shopping lists · {units(s.pos?.value || 0)} lines
                    {data.pulled_at ? ` · pulled ${data.pulled_at}` : ""}
                </span>
            </div>

            <div className="rounded-xl border bg-card p-6 space-y-6">
                <p className="text-[19px] leading-relaxed max-w-[56ch]">
                    In {data.month}, Amazon asked us for <b>{short(asked)}</b> of goods.
                    So far <b>{short(s.arrived?.value || 0)}</b> of it has actually
                    arrived in their warehouse.
                </p>

                <div className="space-y-3">
                    <Step label="Amazon asked for" hint="their shopping lists for the month"
                          value={asked} unitCount={s.asked?.units || 0} pct={100} tone="accent" />
                    <Step label="We couldn't send" hint="usually because it wasn't in stock"
                          value={s.could_not_send?.value || 0} unitCount={s.could_not_send?.units || 0}
                          pct={pct(s.could_not_send?.value || 0)} tone="warn" />
                    <Step label="So we promised" hint="this is what we owe them"
                          value={s.promised?.value || 0} unitCount={s.promised?.units || 0}
                          pct={pct(s.promised?.value || 0)} tone="accent" />
                    <Step label="Amazon says it got" hint="counted in at their door"
                          value={s.arrived?.value || 0} unitCount={s.arrived?.units || 0}
                          pct={pct(s.arrived?.value || 0)} tone="good" />
                </div>

                <div className="ml-6 border-l-2 border-dotted pl-5 space-y-3">
                    <p className="text-sm">
                        The rest — <b>{inr(s.not_arrived?.value || 0)}</b> — hasn't arrived.
                        Two very different reasons:
                    </p>
                    <Step label="Still on its way" hint="normal — nothing to chase here"
                          value={s.travelling?.value || 0} unitCount={s.travelling?.units || 0}
                          pct={((s.travelling?.value || 0) / na) * 100} tone="good" />
                    <Step label={`Promised over ${data.age_days} days ago, still not there`}
                          hint="worth chasing"
                          value={s.missing?.value || 0} unitCount={s.missing?.units || 0}
                          pct={((s.missing?.value || 0) / na) * 100} tone="bad" />
                </div>

                {!!(s.extra?.units) && (
                    <p className="text-xs text-muted-foreground">
                        Amazon also counted <b>{units(s.extra.units)} items more</b> than we
                        promised ({inr(s.extra.value)}), which cancels part of the gap.
                    </p>
                )}
            </div>

            <div className="rounded-xl border bg-card p-6">
                <h3 className="mb-1 text-[17px]">Which brand is behind</h3>
                <p className="mb-4 text-xs text-muted-foreground">
                    Only counts things promised more than {data.age_days} days ago, so goods
                    still in transit never look like a problem.
                </p>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                        <thead>
                            <tr className="border-b text-[11px] uppercase tracking-wider text-muted-foreground">
                                <th className="px-2 pb-2 text-left font-semibold">Brand</th>
                                <th className="px-2 pb-2 text-right font-semibold">We promised</th>
                                <th className="px-2 pb-2 text-right font-semibold">Amazon got</th>
                                <th className="px-2 pb-2 text-right font-semibold">Missing</th>
                                <th className="px-2 pb-2 text-left font-semibold">How much of our promise landed</th>
                            </tr>
                        </thead>
                        <tbody>
                            {data.brands.map(b => {
                                const ok = b.fill_pct >= 90;
                                // "75.7%" means roughly 1 in 4 did not land.
                                // N = 100 / (100 - fill), floored at 2 so a
                                // near-perfect brand never reads "1 in 1".
                                const missRate = Math.max(0.01, 100 - b.fill_pct);
                                const oneIn = Math.max(2, Math.round(100 / missRate));
                                return (
                                    <tr key={b.brand} className="border-b last:border-0">
                                        <td className="px-2 py-2.5">{b.brand}</td>
                                        <td className="px-2 py-2.5 text-right tabular">{units(b.promised_units)}</td>
                                        <td className="px-2 py-2.5 text-right tabular">{units(b.arrived_units)}</td>
                                        <td className="px-2 py-2.5 text-right tabular">{inr(b.missing_value)}</td>
                                        <td className="px-2 py-2.5">
                                            <span className="rounded-full px-2.5 py-0.5 text-[11.5px] font-medium"
                                                  style={ok
                                                      ? { background: "#ddede3", color: "#2e7d52" }
                                                      : { background: "#f5dfdf", color: "#b23a3a" }}>
                                                {b.fill_pct.toFixed(1)}% — {ok
                                                    ? "nearly all landed"
                                                    : `about 1 in ${oneIn} didn't`}
                                            </span>
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* The limit is part of the page, not a caveat someone has to remember. */}
            <div className="rounded-xl border-l-4 bg-muted/30 p-5"
                 style={{ borderLeftColor: "#b8791c" }}>
                <h3 className="text-[15px] font-semibold">What this page cannot tell you yet</h3>
                <p className="mt-1 max-w-[72ch] text-sm text-muted-foreground">
                    Everything above stops at <b>what Amazon says it received</b>. It does not show
                    what actually reached the bank, because Amazon publishes no vendor-payment API —
                    that lives in Vendor Central → Payments as a spreadsheet. And "promised but not
                    there" does not automatically mean Amazon owes us: it could be that we never
                    shipped it. Telling those apart needs the shipment confirmations we don't have here.
                </p>
            </div>
        </div>
    );
}
