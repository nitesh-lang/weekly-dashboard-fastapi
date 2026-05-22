import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { fmtINR } from "@/lib/utils";

interface BrandRow { brand: string; gmv: number; pct: number; }

const PALETTE = [
    "#1e40af", "#0d9266", "#92400e", "#3451c7", "#6040a8",
    "#be185d", "#0e7490", "#a16207", "#52525b",
];

export function BrandMix({ rows }: { rows: BrandRow[] }) {
    if (!rows.length) return null;
    return (
        <Card>
            <CardHeader className="pb-3">
                <CardTitle>Sales by Brand</CardTitle>
            </CardHeader>
            <CardContent>
                <div className="grid grid-cols-1 gap-3">
                    <div className="h-[140px]">
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Pie
                                    data={rows}
                                    dataKey="gmv"
                                    nameKey="brand"
                                    innerRadius="60%"
                                    outerRadius="92%"
                                    paddingAngle={1}
                                    strokeWidth={2}
                                    stroke="#fff"
                                >
                                    {rows.map((_, i) => (
                                        <Cell key={i} fill={PALETTE[i % PALETTE.length]} />
                                    ))}
                                </Pie>
                                <Tooltip
                                    contentStyle={{
                                        borderRadius: 8,
                                        border: "1px solid hsl(var(--border))",
                                        fontSize: 12,
                                    }}
                                    formatter={(v: number) => fmtINR(v)}
                                />
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                    <div className="text-[12px] divide-y max-h-[180px] overflow-auto">
                        {rows.map((r, i) => (
                            <div key={r.brand} className="flex items-center justify-between py-1">
                                <span className="flex items-center gap-2">
                                    <span
                                        className="h-2.5 w-2.5 rounded-sm"
                                        style={{ background: PALETTE[i % PALETTE.length] }}
                                    />
                                    {r.brand}
                                </span>
                                <span className="tabular text-muted-foreground">
                                    {fmtINR(r.gmv)} · {r.pct.toFixed(1)}%
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            </CardContent>
        </Card>
    );
}
