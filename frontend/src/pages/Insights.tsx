import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState, type ReactNode } from "react";
import AppLayout from "@/components/AppLayout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LoadingSkeleton, ErrorBlock } from "@/components/StateBlocks";
import { api } from "@/lib/api";
import { MultiPicker } from "@/components/MultiPicker";
import { Sparkles, RefreshCw, Clock, AlertTriangle } from "lucide-react";
import { ApiError } from "@/lib/api";

interface BriefResponse {
    markdown:       string;
    cached:         boolean;
    generated_at:   number;
    context_mtime:  number;
    brand:          string;
}

interface MetaResponse {
    weeks: number[]; brands: string[]; asin_types: string[];
    /** unique [brand, category_l0, category_l1, model] rows — drives the
     *  dependent Category / Sub-category / Model pickers (category fields
     *  may be "" for unclassified SKUs). */
    model_index: [string, string, string, string][];
}

export default function Insights() {
    const qc = useQueryClient();
    const [brand,    setBrand]    = useState<string>("all");
    const [week,     setWeek]     = useState<string>("latest");
    const [asinType, setAsinType] = useState<string>("all");
    const [category, setCategory] = useState<string>("all");
    const [subCat,   setSubCat]   = useState<string>("all");
    const [model,    setModel]    = useState<string>("all");
    const [busy,     setBusy]     = useState(false);

    const meta = useQuery<MetaResponse>({
        queryKey: ["insights-meta"],
        queryFn:  () => api.get("/api/insights/meta"),
        staleTime: 60 * 60_000,
    });

    // Any non-default filter recomputes the WHOLE brief server-side for
    // that slice — week-wise / brand-wise / ASIN-type briefs are different
    // documents, not the global one with sections hidden.
    function briefUrl(extra = "") {
        const p = new URLSearchParams();
        if (brand && brand !== "all") p.set("brand", brand);
        // "lastN" week values are window mode, not a specific week.
        if (week.startsWith("last")) p.set("last_n", week.slice(4));
        else if (week && week !== "latest") p.set("week", week);
        if (asinType && asinType !== "all") p.set("asin_type", asinType);
        if (category && category !== "all") p.set("category", category);
        if (subCat && subCat !== "all") p.set("subcategory", subCat);
        if (model && model !== "all") p.set("model", model);
        const qs = p.toString();
        return `/api/insights/brief${qs || extra ? "?" : ""}${qs}${qs && extra ? "&" : ""}${extra}`;
    }

    const brief = useQuery<BriefResponse>({
        queryKey: ["insights-brief", brand, week, asinType, category, subCat, model],
        queryFn:  () => api.get(briefUrl()),
        staleTime: 30 * 60_000,
        retry: false,
    });

    async function regenerate() {
        setBusy(true);
        try {
            await api.get(briefUrl("force=true"));
            await qc.invalidateQueries({ queryKey: ["insights-brief", brand, week, asinType, category, subCat, model] });
        } finally {
            setBusy(false);
        }
    }

    const brandOptions = ["all", ...(meta.data?.brands ?? [])];
    const weekOptions  = ["latest", ...(meta.data?.weeks ?? []).map(String)];
    const typeOptions  = ["all", ...(meta.data?.asin_types ?? [])];

    // Category → Sub-category → Model narrow through the (brand, l0, l1,
    // model) index — never an 800-entry flat list.
    const idx = meta.data?.model_index ?? [];
    const inBrand = brand === "all" ? idx : idx.filter(([b]) => b === brand);
    const catOptions = ["all", ...Array.from(new Set(inBrand.map(([, c]) => c).filter(Boolean))).sort()];
    const inCat = category === "all" ? inBrand : inBrand.filter(([, c]) => c === category);
    const subOptions = ["all", ...Array.from(new Set(inCat.map(([, , sc]) => sc).filter(Boolean))).sort()];
    const inSub = subCat === "all" ? inCat : inCat.filter(([, , sc]) => sc === subCat);
    const modelOptions = ["all", ...Array.from(new Set(inSub.map(([, , , m]) => m))).sort()];

    // A stale narrower pick (brand switched, category gone) resets to "all"
    // rather than silently filtering to an empty slice.
    if (category !== "all" && !catOptions.includes(category)) setCategory("all");
    if (subCat !== "all" && !subOptions.includes(subCat)) setSubCat("all");
    if (model !== "all" && !modelOptions.includes(model)) setModel("all");

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
                    <label className="text-[10px] uppercase tracking-wider text-muted-foreground block mb-1">Week</label>
                    <select
                        value={week}
                        onChange={(e) => setWeek(e.target.value)}
                        className="h-8 text-sm rounded-md border bg-background px-2"
                    >
                        <option value="latest">Latest week</option>
                        <optgroup label="Quick ranges">
                            {[2, 4, 6, 8, 10, 12].map((n) => (
                                <option key={n} value={`last${n}`}>Last {n} weeks</option>
                            ))}
                        </optgroup>
                        <optgroup label="Specific week">
                            {weekOptions.filter((w) => w !== "latest").map((w) => (
                                <option key={w} value={w}>Week {w}</option>
                            ))}
                        </optgroup>
                    </select>
                </div>
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
                <div>
                    <label className="text-[10px] uppercase tracking-wider text-muted-foreground block mb-1">ASIN Type</label>
                    <select
                        value={asinType}
                        onChange={(e) => setAsinType(e.target.value)}
                        className="h-8 text-sm rounded-md border bg-background px-2"
                    >
                        {typeOptions.map((t) => (
                            <option key={t} value={t}>{t === "all" ? "All types" : t}</option>
                        ))}
                    </select>
                </div>
                <SearchSelect
                    label="Category" placeholder="All categories"
                    value={category} options={catOptions.filter((c) => c !== "all")}
                    onPick={(v) => { setCategory(v); setSubCat("all"); setModel("all"); }}
                />
                <SearchSelect
                    label="Sub-category" placeholder="All sub-categories"
                    value={subCat} options={subOptions.filter((c) => c !== "all")}
                    onPick={(v) => { setSubCat(v); setModel("all"); }}
                />
                <SearchSelect
                    label="Model" placeholder="All models"
                    value={model} options={modelOptions.filter((m) => m !== "all")}
                    onPick={setModel}
                />
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
                <BriefError error={brief.error} onRetry={() => brief.refetch()} />
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

/** Type-to-search single picker over a native <datalist> — no dropdown lib.
 *  Commits when the text exactly matches an option (case-insensitive) or on
 *  clear; junk text just never commits, so the active filter stays valid.
 *  Datalist UX: click shows all options, typing narrows them. */
function SearchSelect({ label, placeholder, value, options, onPick }: {
    label: string; placeholder: string; value: string;
    options: string[]; onPick: (v: string) => void;
}) {
    const [text, setText] = useState(value === "all" ? "" : value);
    // External resets (brand switch wiping the pick) must clear the box too.
    useEffect(() => { setText(value === "all" ? "" : value); }, [value]);
    const listId = `ss-${label.toLowerCase().replace(/[^a-z]/g, "")}`;
    function commit(raw: string) {
        const v = raw.trim();
        if (!v) { onPick("all"); return; }
        const hit = options.find((o) => o.toLowerCase() === v.toLowerCase());
        if (hit) onPick(hit);
    }
    return (
        <div>
            <label className="text-[10px] uppercase tracking-wider text-muted-foreground block mb-1">{label}</label>
            <input
                list={listId}
                value={text}
                placeholder={placeholder}
                onChange={(e) => { setText(e.target.value); commit(e.target.value); }}
                onBlur={(e) => { if (!e.target.value.trim()) onPick("all"); }}
                className="h-8 text-sm rounded-md border bg-background px-2 w-[170px]"
            />
            <datalist id={listId}>
                {options.map((o) => <option key={o} value={o} />)}
            </datalist>
        </div>
    );
}

/** Surfaces the backend's specific 503/500 reason so the operator knows
 *  whether it's missing API key vs missing data vs Anthropic rate limit. */
function BriefError({ error, onRetry }: { error: unknown; onRetry: () => void }) {
    const apiErr = error instanceof ApiError ? error : null;
    const status = apiErr?.status;
    const body   = (apiErr?.body as { error?: string; detail?: string }) || null;
    const reason = body?.error || body?.detail || (error as Error)?.message || "Unknown failure";

    const isKeyMissing =
        status === 503 && /ANTHROPIC_API_KEY/i.test(reason);
    const isContextMissing =
        status === 503 && /ai_context\.json/i.test(reason);

    return (
        <Card className="p-6 max-w-[720px]" style={{ borderColor: "#fecaca" }}>
            <div className="flex items-start gap-3">
                <AlertTriangle className="h-5 w-5 mt-0.5 shrink-0" style={{ color: "#b91c1c" }} />
                <div className="flex-1">
                    <div className="text-[15px] font-semibold mb-1">Couldn't generate the brief</div>
                    <div className="text-[13px] mb-3" style={{ color: "#4b5563" }}>{reason}</div>

                    {isKeyMissing && (
                        <div className="text-[13px] rounded-md border p-3 mb-3" style={{ background: "#fffbeb", borderColor: "#fde68a", color: "#78350f" }}>
                            <strong>Fix:</strong> add <code className="font-mono">ANTHROPIC_API_KEY</code> to the Render service Environment tab, then redeploy.  The chatbot uses the same key.
                        </div>
                    )}
                    {isContextMissing && (
                        <div className="text-[13px] rounded-md border p-3 mb-3" style={{ background: "#fffbeb", borderColor: "#fde68a", color: "#78350f" }}>
                            <strong>Fix:</strong> run the weekly ETL — <code className="font-mono">POST /api/ai/ai-chat/rebuild-context</code> — to build the context the brief reads from.
                        </div>
                    )}

                    <Button size="sm" variant="outline" onClick={onRetry}>Retry</Button>
                </div>
            </div>
        </Card>
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
