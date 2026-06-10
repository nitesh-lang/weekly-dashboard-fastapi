import { useState } from "react";
import {
    ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn, fmtINR } from "@/lib/utils";

interface AmsTrendRow {
    week: string;
    ad_spend: number;
    acos:  number | null;   // ratios 0..1+ (not %)
    tacos: number | null;
}

interface Props {
    trend: AmsTrendRow[];
    selectedBrandLabel?: string;
}

const RANGES = [
    { n: 4,  label: "4w" },
    { n: 8,  label: "8w" },
    { n: 12, label: "12w" },
    { n: 0,  label: "All" },
];

export function AmsOverviewChart({ trend, selectedBrandLabel }: Props) {
    const [n, setN] = useState(12);
    const sliced = n > 0 ? trend.slice(-n) : trend;

    // Recharts wants finite numbers; chart % axis as 0–100 scale for readability
    const data = sliced.map((r) => ({
        week: r.week,
        ad_spend: r.ad_spend,
        acos:  r.acos  != null && isFinite(r.acos)  ? +(r.acos  * 100).toFixed(2) : null,
        tacos: r.tacos != null && isFinite(r.tacos) ? +(r.tacos * 100).toFixed(2) : null,
    }));

    return (
        <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
                <div>
                    <CardTitle>Spend · ACOS · TACOS</CardTitle>
                    <p className="text-xs text-muted-foreground mt-1 tabular">
                        {selectedBrandLabel || "All Brands"} · {n > 0 ? `last ${n} weeks` : "all weeks"}
                    </p>
                </div>
                <div className="inline-flex rounded-md border bg-secondary/60 p-0.5 gap-0.5">
                    {RANGES.map((r) => (
                        <button
                            key={r.label}
                            onClick={() => setN(r.n)}
                            className={cn(
                                "px-2.5 py-1 text-[11px] font-medium rounded transition-colors",
                                n === r.n
                                    ? "bg-background text-foreground shadow-sm"
                                    : "text-muted-foreground hover:text-foreground"
                            )}
                        >
                            {r.label}
                        </button>
                    ))}
                </div>
            </CardHeader>
            <CardContent>
                <div className="h-[220px] -ml-2">
                    <ResponsiveContainer width="100%" height="100%">
                        <ComposedChart data={data} margin={{ top: 8, right: 12, bottom: 4, left: 8 }}>
                            <CartesianGrid strokeDasharray="2 4" stroke="hsl(var(--border))" vertical={false} />
                            <XAxis
                                dataKey="week"
                                tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
                                tickLine={false}
                                axisLine={{ stroke: "hsl(var(--border))" }}
                            />
                            <YAxis
                                yAxisId="left"
                                tickFormatter={(v) => fmtINR(v, "")}
                                tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
                                tickLine={false}
                                axisLine={false}
                                width={70}
                            />
                            <YAxis
                                yAxisId="right"
                                orientation="right"
                                tickFormatter={(v) => `${v}%`}
                                tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
                                tickLine={false}
                                axisLine={false}
                                width={50}
                            />
                            <Tooltip
                                contentStyle={{
                                    borderRadius: 8,
                                    border: "1px solid hsl(var(--border))",
                                    fontSize: 12,
                                }}
                                formatter={(v: any, k: any) => {
                                    if (v == null) return ["—", String(k)];
                                    const num = Number(v);
                                    if (k === "ad_spend") return [fmtINR(num), "Ad Spend"];
                                    if (k === "acos")     return [`${num.toFixed(2)}%`, "ACOS"];
                                    if (k === "tacos")    return [`${num.toFixed(2)}%`, "TACOS"];
                                    return [String(v), String(k)];
                                }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11 }} />
                            <Bar
                                yAxisId="left"
                                dataKey="ad_spend"
                                name="Ad Spend"
                                fill="hsl(var(--primary) / 0.85)"
                                radius={[3, 3, 0, 0]}
                                maxBarSize={28}
                            />
                            <Line
                                yAxisId="right"
                                type="monotone"
                                dataKey="acos"
                                name="ACOS %"
                                stroke="hsl(var(--destructive))"
                                strokeWidth={2}
                                dot={{ r: 2.5 }}
                                activeDot={{ r: 5 }}
                                connectNulls
                            />
                            <Line
                                yAxisId="right"
                                type="monotone"
                                dataKey="tacos"
                                name="TACOS %"
                                stroke="hsl(var(--warning, 38 92% 50%))"
                                strokeWidth={2}
                                strokeDasharray="4 3"
                                dot={{ r: 2.5 }}
                                connectNulls
                            />
                        </ComposedChart>
                    </ResponsiveContainer>
                </div>
            </CardContent>
        </Card>
    );
}
