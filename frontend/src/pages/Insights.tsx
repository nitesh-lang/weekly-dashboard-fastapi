import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type ReactNode } from "react";
import AppLayout from "@/components/AppLayout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { api } from "@/lib/api";
import { MultiPicker } from "@/components/MultiPicker";
import { Sparkles, RefreshCw, Clock } from "lucide-react";

interface BriefResponse {
    markdown:       string;
    cached:         boolean;
    generated_at:   number;
    context_mtime:  number;
    brand:          string;
}

interface BrandsResponse { brands: string[]; }

export default function Insights() {
    const qc = useQueryClient();
    const [brand, setBrand] = useState<string>("all");
    const [busy,  setBusy]  = useState(false);

    const brands = useQuery<BrandsResponse>({
        queryKey: ["insights-brands"],
        queryFn:  () => api.get("/api/insights/brands"),
        staleTime: 60 * 60_000,
    });

    const brief = useQuery<BriefResponse>({
        queryKey: ["insights-brief", brand],
        queryFn:  () => api.get(`/api/insights/brief${brand && brand !== "all" ? `?brand=${encodeURIComponent(brand)}` : ""}`),
        staleTime: 30 * 60_000,
        retry: false,
    });

    async function regenerate() {
        setBusy(true);
        try {
            await api.get(`/api/insights/brief?force=true${brand && brand !== "all" ? `&brand=${encodeURIComponent(brand)}` : ""}`);
            await qc.invalidateQueries({ queryKey: ["insights-brief", brand] });
        } finally {
            setBusy(false);
        }
    }

    const brandOptions = ["all", ...(brands.data?.brands ?? [])];

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2" style={{ color: "#7c3aed" }}>Weekly · Briefing</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <Sparkles className="h-5 w-5" style={{ color: "#7c3aed" }} />
                        Insights
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        A written brief of the week — what moved, what to watch, what to do.
                        Drafted by Claude from the same context the chatbot uses; cached per data refresh.
                    </p>
                </div>
                <div className="flex-1" />
                <div>
                    <label className="text-[10px] uppercase tracking-wider text-muted-foreground block mb-1">Brand</label>
                    <select
                        value={brand}
                        onChange={(e) => setBrand(e.target.value)}
                        className="h-8 text-sm rounded-md border bg-background px-2"
                    >
                        {brandOptions.map((b) => (
                            <option key={b} value={b}>{b === "all" ? "All brands" : b}</option>
                        ))}
                    </select>
                </div>
                <Button onClick={regenerate} disabled={busy || brief.isFetching} variant="outline" size="sm">
                    <RefreshCw className={"h-3.5 w-3.5 " + ((busy || brief.isFetching) ? "animate-spin" : "")} />
                    Regenerate
                </Button>
            </div>

            {brief.isLoading && (
                <Card className="p-6">
                    <div className="flex items-center gap-2 mb-3 text-sm text-muted-foreground">
                        <Sparkles className="h-4 w-4" />
                        Drafting your brief…
                    </div>
                    <LoadingSkeleton rows={8} />
                </Card>
            )}
            {brief.error && (
                <ErrorBlock
                    error={brief.error as Error}
                    onRetry={() => brief.refetch()}
                    title="Couldn't generate the brief"
                />
            )}

            {brief.data && (
                <>
                    <div className="flex items-center gap-3 text-[12px] text-muted-foreground tabular">
                        <Clock className="h-3.5 w-3.5" />
                        <span>
                            {brief.data.cached ? "Cached" : "Just generated"}
                            {" · "}
                            {new Date(brief.data.generated_at * 1000).toLocaleString("en-IN", {
                                day: "numeric", month: "short", hour: "numeric", minute: "2-digit",
                            })}
                        </span>
                        <span>·</span>
                        <span>
                            Source data refreshed{" "}
                            {new Date(brief.data.context_mtime * 1000).toLocaleString("en-IN", {
                                day: "numeric", month: "short", hour: "numeric", minute: "2-digit",
                            })}
                        </span>
                    </div>

                    <Card className="p-7 max-w-[860px]">
                        <Brief markdown={brief.data.markdown} />
                    </Card>
                </>
            )}
        </AppLayout>
    );
}

/** Minimal Markdown renderer — handles `## Heading`, `- bullet`, `**bold**`,
 *  and paragraphs.  Avoids pulling in react-markdown for ~30KB savings since
 *  Claude's brief output is constrained by the system prompt to exactly
 *  these constructs. */
function Brief({ markdown }: { markdown: string }) {
    const blocks = splitBlocks(markdown);
    return (
        <article className="prose-brief">
            {blocks.map((b, i) => renderBlock(b, i))}
        </article>
    );
}

type Block =
    | { kind: "h2";   text: string }
    | { kind: "h3";   text: string }
    | { kind: "ul";   items: string[] }
    | { kind: "p";    text: string };

function splitBlocks(md: string): Block[] {
    const lines = md.replace(/\r\n/g, "\n").split("\n");
    const blocks: Block[] = [];
    let pIdx = -1;
    let ulIdx = -1;
    for (const raw of lines) {
        const line = raw.trim();
        if (!line) { pIdx = -1; ulIdx = -1; continue; }
        if (line.startsWith("## ")) {
            blocks.push({ kind: "h2", text: line.slice(3) });
            pIdx = -1; ulIdx = -1;
        } else if (line.startsWith("### ")) {
            blocks.push({ kind: "h3", text: line.slice(4) });
            pIdx = -1; ulIdx = -1;
        } else if (line.startsWith("- ") || line.startsWith("* ")) {
            const item = line.slice(2);
            if (ulIdx >= 0) (blocks[ulIdx] as any).items.push(item);
            else { blocks.push({ kind: "ul", items: [item] }); ulIdx = blocks.length - 1; }
            pIdx = -1;
        } else {
            if (pIdx >= 0) (blocks[pIdx] as any).text += " " + line;
            else { blocks.push({ kind: "p", text: line }); pIdx = blocks.length - 1; }
            ulIdx = -1;
        }
    }
    return blocks;
}

function renderBlock(b: Block, i: number): ReactNode {
    if (b.kind === "h2") {
        return (
            <h2
                key={i}
                className="text-[17px] font-semibold tracking-tight mt-7 mb-2.5 first:mt-0"
                style={{ color: "#0a0a0a", borderBottom: "1px solid hsl(var(--border))", paddingBottom: 6 }}
            >
                {inlineMd(b.text)}
            </h2>
        );
    }
    if (b.kind === "h3") {
        return (
            <h3 key={i} className="text-[14px] font-semibold mt-5 mb-2" style={{ color: "#0a0a0a" }}>
                {inlineMd(b.text)}
            </h3>
        );
    }
    if (b.kind === "ul") {
        return (
            <ul key={i} className="my-2 space-y-1.5 pl-1">
                {b.items.map((it, j) => (
                    <li
                        key={j}
                        className="flex gap-2.5 text-[14px] leading-[1.55]"
                        style={{ color: "#27272a" }}
                    >
                        <span className="mt-[6px] h-1.5 w-1.5 rounded-full shrink-0" style={{ background: "#7c3aed" }} />
                        <span className="flex-1">{inlineMd(it)}</span>
                    </li>
                ))}
            </ul>
        );
    }
    return (
        <p key={i} className="my-2 text-[14px] leading-[1.65]" style={{ color: "#27272a" }}>
            {inlineMd(b.text)}
        </p>
    );
}

/** Renders `**bold**` segments — preserves the rest as-is. */
function inlineMd(text: string): ReactNode {
    const parts: ReactNode[] = [];
    const re = /\*\*([^*]+)\*\*/g;
    let last = 0;
    let m: RegExpExecArray | null;
    let k = 0;
    while ((m = re.exec(text)) !== null) {
        if (m.index > last) parts.push(text.slice(last, m.index));
        parts.push(<strong key={`b${k++}`} className="font-semibold" style={{ color: "#0a0a0a" }}>{m[1]}</strong>);
        last = m.index + m[0].length;
    }
    if (last < text.length) parts.push(text.slice(last));
    return parts.length ? parts : text;
}
