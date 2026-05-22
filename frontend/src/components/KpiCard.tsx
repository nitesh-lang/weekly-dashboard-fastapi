import { LineChart, Line, ResponsiveContainer } from "recharts";
import { Card } from "@/components/ui/card";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import { cn } from "@/lib/utils";

interface KpiCardProps {
    label: string;
    value: string;
    deltaPct: number | null;
    deltaLabel?: string;
    spark?: number[];
    sparkLabels?: string[];
    color?: string;
    hero?: boolean;
}

export function KpiCard({
    label,
    value,
    deltaPct,
    deltaLabel,
    spark,
    color = "#1e40af",
    hero,
}: KpiCardProps) {
    const data = spark?.map((v, i) => ({ i, v }));
    const dir = deltaPct == null ? "flat" : deltaPct > 0 ? "up" : deltaPct < 0 ? "down" : "flat";
    const DeltaIcon = dir === "up" ? TrendingUp : dir === "down" ? TrendingDown : Minus;

    return (
        <Card className={cn("relative", hero && "lg:col-span-2")}>
            {/* Override the global oxblood top stripe with this card's metric color */}
            <span
                className="absolute inset-x-0 top-0 h-[3px] rounded-t-lg"
                style={{
                    background: `linear-gradient(90deg, ${color} 0%, ${shade(color, -12)} 50%, ${color} 100%)`,
                    boxShadow: `0 0 12px ${color}55`,
                    zIndex: 1,
                }}
            />
            <div className={cn("p-6 relative", hero && "p-7")}>
                {/* Eyebrow label */}
                <div
                    className="text-[10.5px] font-semibold uppercase mb-3"
                    style={{ letterSpacing: "0.14em", color: shade(color, -8) }}
                >
                    {label}
                </div>

                {/* Hero numeral — display-weight Geist */}
                <div
                    className={cn("tabular tracking-tight", hero ? "text-[56px]" : "text-[36px]")}
                    style={{
                        fontWeight: 700,
                        letterSpacing: "-0.035em",
                        lineHeight: 1.02,
                        color: "#0a0a0a",
                    }}
                >
                    {value}
                </div>

                {/* Delta chip — vibrant per direction */}
                {deltaPct != null && (
                    <div
                        className="inline-flex items-center gap-1.5 rounded-md px-2.5 py-1 text-[12px] font-semibold mt-3 tabular"
                        style={{
                            background:
                                dir === "up"
                                    ? "linear-gradient(180deg, rgba(5,150,105,0.10) 0%, rgba(5,150,105,0.04) 100%)"
                                    : dir === "down"
                                      ? "linear-gradient(180deg, rgba(185,28,28,0.10) 0%, rgba(185,28,28,0.04) 100%)"
                                      : "linear-gradient(180deg, rgba(107,114,128,0.10) 0%, rgba(107,114,128,0.04) 100%)",
                            border: `1px solid ${
                                dir === "up"
                                    ? "rgba(5,150,105,0.22)"
                                    : dir === "down"
                                      ? "rgba(185,28,28,0.22)"
                                      : "rgba(107,114,128,0.22)"
                            }`,
                            color:
                                dir === "up"
                                    ? "#047857"
                                    : dir === "down"
                                      ? "#b91c1c"
                                      : "#4b5563",
                        }}
                    >
                        <DeltaIcon className="h-3 w-3" />
                        {deltaPct > 0 ? "+" : ""}
                        {deltaPct.toFixed(1)}%
                        {deltaLabel && (
                            <span style={{ opacity: 0.7, fontWeight: 400 }}>{deltaLabel}</span>
                        )}
                    </div>
                )}

                {/* Sparkline with soft gradient fill underneath the line */}
                {data && data.length > 0 && (
                    <div className={cn("mt-5", hero ? "h-20" : "h-12")}>
                        <ResponsiveContainer width="100%" height="100%">
                            <LineChart data={data} margin={{ top: 4, right: 0, bottom: 0, left: 0 }}>
                                <defs>
                                    <linearGradient id={`spark-${color.replace("#", "")}`} x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="0%"  stopColor={color} stopOpacity={0.22} />
                                        <stop offset="100%" stopColor={color} stopOpacity={0} />
                                    </linearGradient>
                                </defs>
                                <Line
                                    type="monotone"
                                    dataKey="v"
                                    stroke={color}
                                    strokeWidth={hero ? 2 : 1.6}
                                    dot={false}
                                    isAnimationActive
                                    animationDuration={680}
                                    fill={`url(#spark-${color.replace("#", "")})`}
                                />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </div>
        </Card>
    );
}

/** Adjust an HSL-equivalent hex by % lightness (positive lightens, negative darkens). */
function shade(hex: string, percent: number): string {
    const c = hex.replace("#", "");
    if (c.length !== 6) return hex;
    const r = parseInt(c.slice(0, 2), 16);
    const g = parseInt(c.slice(2, 4), 16);
    const b = parseInt(c.slice(4, 6), 16);
    const adj = (v: number) =>
        Math.max(0, Math.min(255, Math.round(v + (percent / 100) * 255)));
    const out = (n: number) => adj(n).toString(16).padStart(2, "0");
    return `#${out(r)}${out(g)}${out(b)}`;
}
