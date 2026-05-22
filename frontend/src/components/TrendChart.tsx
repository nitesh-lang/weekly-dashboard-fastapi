import { useState } from "react";
import {
    ComposedChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { cn, fmtINR, fmtInt } from "@/lib/utils";

interface TrendRow {
    week: string;
    gmv?: number;
    units?: number;
    nlc?: number;
}

interface Props {
    trend: TrendRow[];
    allBrands?: TrendRow[];
    selectedBrandLabel?: string;
}

const RANGES = [
    { n: 4, label: "4w" },
    { n: 8, label: "8w" },
    { n: 12, label: "12w" },
    { n: 0, label: "All" },
];

export function TrendChart({ trend, allBrands, selectedBrandLabel }: Props) {
    const [n, setN] = useState(12);

    const sliced = n > 0 ? trend.slice(-n) : trend;
    const allSliced = allBrands && n > 0 ? allBrands.slice(-n) : allBrands;

    const merged = sliced.map((r, i) => ({
        ...r,
        all_gmv: allSliced?.[i]?.gmv,
    }));

    return (
        <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
                <div>
                    <CardTitle>Weekly Trend</CardTitle>
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
                <div className="h-[200px] -ml-2">
                    <ResponsiveContainer width="100%" height="100%">
                        <ComposedChart data={merged} margin={{ top: 8, right: 12, bottom: 4, left: 8 }}>
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
                                tickFormatter={(v) => fmtInt(v)}
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
                                formatter={(v: number, k: string) =>
                                    k === "units" ? fmtInt(v) + " units" : fmtINR(v)
                                }
                            />
                            <Legend wrapperStyle={{ fontSize: 11 }} />
                            <Line
                                yAxisId="left"
                                type="monotone"
                                dataKey="gmv"
                                name="GMV"
                                stroke="hsl(var(--primary))"
                                strokeWidth={2.2}
                                dot={{ r: 2.5 }}
                                activeDot={{ r: 5 }}
                            />
                            <Line
                                yAxisId="right"
                                type="monotone"
                                dataKey="units"
                                name="Units"
                                stroke="hsl(var(--success))"
                                strokeWidth={1.6}
                                strokeDasharray="3 3"
                                dot={false}
                            />
                            {allSliced && (
                                <Line
                                    yAxisId="left"
                                    type="monotone"
                                    dataKey="all_gmv"
                                    name="All Brands GMV"
                                    stroke="hsl(var(--muted-foreground))"
                                    strokeWidth={1.2}
                                    strokeDasharray="2 4"
                                    dot={false}
                                />
                            )}
                        </ComposedChart>
                    </ResponsiveContainer>
                </div>
            </CardContent>
        </Card>
    );
}
