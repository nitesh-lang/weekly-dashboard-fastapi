import { useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppLayout from "@/components/AppLayout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { api, ApiError } from "@/lib/api";
import { UploadCloud, CheckCircle2, AlertTriangle, FileUp, Layers, X, FilePlus2 } from "lucide-react";

interface StatusData {
    bsr_latest: Record<string, string | null>;
    variations: { rows: number } | null;
    planning_latest: Record<string, string | null>;
    github_configured: boolean;
    today: string;
}
interface UploadResult {
    ok: boolean; commit: string | null; note: string;
    brands?: string[]; date?: string; warnings?: string[];
}

async function postFiles(url: string, files: File[], fieldName: string): Promise<UploadResult> {
    const fd = new FormData();
    for (const f of files) fd.append(fieldName, f);
    const res = await fetch(url, { method: "POST", body: fd, credentials: "include" });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) throw new ApiError(res.status, `Upload failed (${res.status})`, body);
    return body as UploadResult;
}

/** Staged upload: files accumulate across picker sessions (so all four
 *  brands can be gathered even one at a time), then ONE explicit upload
 *  sends everything in a single request → a single commit → a single
 *  rebuild. */
function UploadCard({
    title, subtitle, accept, multiple, endpoint, fieldName, onDone,
}: {
    title: string; subtitle: string; accept: string; multiple: boolean;
    endpoint: string; fieldName: string; onDone: () => void;
}) {
    const inputRef = useRef<HTMLInputElement>(null);
    const [staged, setStaged] = useState<File[]>([]);
    const [busy, setBusy] = useState(false);
    const [result, setResult] = useState<UploadResult | null>(null);
    const [error, setError] = useState<string | null>(null);

    function addFiles(list: FileList | null) {
        if (!list) return;
        setResult(null); setError(null);
        setStaged((prev) => {
            const next = multiple ? [...prev] : [];
            for (const f of Array.from(list)) {
                if (!next.some((x) => x.name === f.name)) next.push(f);
            }
            return multiple ? next : next.slice(-1);
        });
        if (inputRef.current) inputRef.current.value = "";
    }

    async function uploadAll() {
        if (staged.length === 0) return;
        setBusy(true); setError(null); setResult(null);
        try {
            const r = await postFiles(endpoint, staged, fieldName);
            setResult(r);
            setStaged([]);
            onDone();
        } catch (e) {
            const body = (e instanceof ApiError ? e.body : null) as { detail?: string } | null;
            setError(body?.detail || (e as Error).message || "Upload failed");
        } finally {
            setBusy(false);
        }
    }

    return (
        <Card className="p-5 flex-1 min-w-[320px]">
            <div className="text-[15px] font-semibold mb-1">{title}</div>
            <p className="text-[12.5px] text-muted-foreground mb-4">{subtitle}</p>
            <input
                ref={inputRef} type="file" accept={accept} multiple={multiple}
                className="hidden" onChange={(e) => addFiles(e.target.files)}
            />
            <div className="flex items-center gap-2">
                <Button onClick={() => inputRef.current?.click()} disabled={busy} size="sm" variant="outline">
                    <FilePlus2 className="h-4 w-4" />
                    Add file{multiple ? "s" : ""}
                </Button>
                <Button onClick={uploadAll} disabled={busy || staged.length === 0} size="sm">
                    <UploadCloud className={"h-4 w-4 " + (busy ? "animate-pulse" : "")} />
                    {busy ? "Uploading & committing…"
                          : staged.length === 0 ? "Upload"
                          : `Upload ${staged.length} file${staged.length > 1 ? "s" : ""} — one commit`}
                </Button>
            </div>
            {staged.length > 0 && (
                <ul className="mt-3 space-y-1">
                    {staged.map((f) => (
                        <li key={f.name} className="flex items-center gap-2 text-[12.5px] rounded border px-2 py-1 bg-muted/40">
                            <span className="flex-1 truncate">{f.name}</span>
                            <span className="text-muted-foreground">{(f.size / 1024).toFixed(0)} KB</span>
                            <button type="button" onClick={() => setStaged((p) => p.filter((x) => x.name !== f.name))}
                                    className="opacity-60 hover:opacity-100" aria-label={`Remove ${f.name}`}>
                                <X className="h-3.5 w-3.5" />
                            </button>
                        </li>
                    ))}
                </ul>
            )}
            {result && (
                <div className="mt-4 flex items-start gap-2 text-[13px] rounded-md border p-3"
                     style={{ background: "#f0fdf4", borderColor: "#bbf7d0", color: "#166534" }}>
                    <CheckCircle2 className="h-4 w-4 mt-0.5 shrink-0" />
                    <div>
                        {result.note}
                        {result.brands && <div className="mt-1 text-[12px]">Brands: {result.brands.join(", ")} · dated {result.date}</div>}
                        {result.commit && <div className="mt-1 font-mono text-[11px] opacity-70">{result.commit.slice(0, 10)}</div>}
                    </div>
                </div>
            )}
            {error && (
                <div className="mt-4 flex items-start gap-2 text-[13px] rounded-md border p-3"
                     style={{ background: "#fef2f2", borderColor: "#fecaca", color: "#991b1b" }}>
                    <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                    <div>{error}</div>
                </div>
            )}
        </Card>
    );
}

const PLANNING_BRANDS = [
    { key: "nexlev", label: "Nexlev" },
    { key: "audio_array", label: "Audio Array" },
] as const;

/** Monthly ASIN planning workbook for the Sales Dashboard.  One brand at a
 *  time (the two brands are separate files by design), brand chosen with
 *  house-style toggle buttons — never a native select. */
function PlanningCard({ latest, onDone }: {
    latest: Record<string, string | null> | undefined; onDone: () => void;
}) {
    const inputRef = useRef<HTMLInputElement>(null);
    const [brand, setBrand] = useState<string>("nexlev");
    const [staged, setStaged] = useState<File | null>(null);
    const [busy, setBusy] = useState(false);
    const [result, setResult] = useState<UploadResult | null>(null);
    const [error, setError] = useState<string | null>(null);

    async function upload() {
        if (!staged) return;
        setBusy(true); setError(null); setResult(null);
        try {
            const r = await postFiles(`/api/keepa-upload/planning?brand=${brand}`, [staged], "file");
            setResult(r);
            setStaged(null);
            onDone();
        } catch (e) {
            const st = e instanceof ApiError ? e.status : 0;
            if (st === 502 || st === 504) {
                // The commit very likely landed and only the response was lost
                // (seen live on 01/09/2026). Refresh status so the brand chip
                // tells the truth instead of leaving a scary dead-end error.
                onDone();
                setError("Server hiccup (502) — the upload may still have gone through. " +
                         "Check the brand chip above: if it shows your month, it's committed. " +
                         "Re-uploading the same file is always safe (it just says 'already up to date').");
            } else {
                const body = (e instanceof ApiError ? e.body : null) as { detail?: string } | null;
                setError(body?.detail || (e as Error).message || "Upload failed");
            }
        } finally {
            setBusy(false);
        }
    }

    return (
        <Card className="p-5 flex-1 min-w-[320px]">
            <div className="text-[15px] font-semibold mb-1">Sales Dashboard — monthly ASIN plan</div>
            <p className="text-[12.5px] text-muted-foreground mb-3">
                The workbook named exactly <span className="font-mono">ASIN Planning file - &lt;Mon&gt; &lt;YYYY&gt;.xlsx</span>.
                A month's sales only load if its plan is in <b>before</b> that month's pull — upload the
                new month's file early.
            </p>
            <div className="flex items-center gap-1.5 mb-3" role="radiogroup" aria-label="Brand">
                {PLANNING_BRANDS.map((b) => (
                    <button
                        key={b.key} type="button" role="radio" aria-checked={brand === b.key}
                        onClick={() => { setBrand(b.key); setResult(null); setError(null); }}
                        className={"px-3 py-1.5 rounded-full text-[12.5px] font-medium border transition-colors " +
                            (brand === b.key
                                ? "bg-foreground text-background border-foreground"
                                : "bg-background text-muted-foreground hover:text-foreground")}>
                        {b.label}
                        {latest && (
                            <span className={"ml-1.5 text-[11px] " + (brand === b.key ? "opacity-70" : "opacity-60")}>
                                {latest[b.key] ?? "no plan"}
                            </span>
                        )}
                    </button>
                ))}
            </div>
            <input
                ref={inputRef} type="file" accept=".xlsx" className="hidden"
                onChange={(e) => {
                    const f = e.target.files?.[0] ?? null;
                    setStaged(f); setResult(null); setError(null);
                    if (inputRef.current) inputRef.current.value = "";
                }}
            />
            <div className="flex items-center gap-2">
                <Button onClick={() => inputRef.current?.click()} disabled={busy} size="sm" variant="outline">
                    <FilePlus2 className="h-4 w-4" />
                    Choose plan
                </Button>
                <Button onClick={upload} disabled={busy || !staged} size="sm">
                    <UploadCloud className={"h-4 w-4 " + (busy ? "animate-pulse" : "")} />
                    {busy ? "Validating & committing…" : "Upload plan"}
                </Button>
            </div>
            {staged && (
                <ul className="mt-3 space-y-1">
                    <li className="flex items-center gap-2 text-[12.5px] rounded border px-2 py-1 bg-muted/40">
                        <span className="flex-1 truncate">{staged.name}</span>
                        <span className="text-muted-foreground">{(staged.size / 1024).toFixed(0)} KB</span>
                        <button type="button" onClick={() => setStaged(null)}
                                className="opacity-60 hover:opacity-100" aria-label={`Remove ${staged.name}`}>
                            <X className="h-3.5 w-3.5" />
                        </button>
                    </li>
                </ul>
            )}
            {result && (
                <div className="mt-4 flex items-start gap-2 text-[13px] rounded-md border p-3"
                     style={{ background: "#f0fdf4", borderColor: "#bbf7d0", color: "#166534" }}>
                    <CheckCircle2 className="h-4 w-4 mt-0.5 shrink-0" />
                    <div>
                        {result.note}
                        {result.warnings && result.warnings.length > 0 && (
                            <div className="mt-1 text-[12px]" style={{ color: "#92400e" }}>
                                {result.warnings.join(" ")}
                            </div>
                        )}
                        {result.commit && <div className="mt-1 font-mono text-[11px] opacity-70">{result.commit.slice(0, 10)}</div>}
                    </div>
                </div>
            )}
            {error && (
                <div className="mt-4 flex items-start gap-2 text-[13px] rounded-md border p-3"
                     style={{ background: "#fef2f2", borderColor: "#fecaca", color: "#991b1b" }}>
                    <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                    <div>{error}</div>
                </div>
            )}
        </Card>
    );
}

export default function KeepaUpload() {
    const qc = useQueryClient();
    const status = useQuery<StatusData>({
        queryKey: ["keepa-upload-status"],
        queryFn: () => api.get("/api/keepa-upload/status"),
        staleTime: 60_000,
    });
    const refresh = () => qc.invalidateQueries({ queryKey: ["keepa-upload-status"] });

    return (
        <AppLayout>
            <div>
                <div className="eyebrow mb-2" style={{ color: "#0e7490" }}>Weekly · Data</div>
                <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                    <FileUp className="h-5 w-5" style={{ color: "#0e7490" }} />
                    Keepa Upload
                </h1>
                <p className="text-[13.5px] mt-1.5 max-w-[640px]" style={{ color: "#4b5563" }}>
                    Upload your Keepa exports here — they're committed to the repo automatically
                    and the site rebuilds itself with the fresh data (~5 minutes). No terminal needed.
                </p>
            </div>

            {status.data && !status.data.github_configured && (
                <Card className="p-4 text-[13px]" style={{ borderColor: "#fde68a", background: "#fffbeb", color: "#78350f" }}>
                    Uploads are disabled: <code className="font-mono">GITHUB_TOKEN</code> is not set on the
                    server. Add it in Render → Environment, then redeploy.
                </Card>
            )}

            <div className="flex flex-wrap gap-4">
                <UploadCard
                    title="Buybox — BSR export"
                    subtitle={"Add all four brand CSVs (Nexlev / Tonor / Audio Array / White Mulberry) or one ZIP, then upload once — one commit covers all brands. Filed under today's date" + (status.data ? ` (${status.data.today})` : "") + "; /buybox rebuilds with the new BSR data."}
                    accept=".csv,.zip"
                    multiple
                    endpoint="/api/keepa-upload/bsr"
                    fieldName="files"
                    onDone={refresh}
                />
                <UploadCard
                    title="Variation Performance — parent/child export"
                    subtitle="The 'Merge All brand Keepa' file (ASIN + Variation ASINs + Brand columns). Replaces the variation map used by the Variation Performance page."
                    accept=".csv"
                    multiple={false}
                    endpoint="/api/keepa-upload/variations"
                    fieldName="file"
                    onDone={refresh}
                />
                <PlanningCard latest={status.data?.planning_latest} onDone={refresh} />
            </div>

            {status.data && (
                <Card className="p-5 max-w-[560px]">
                    <div className="text-[11px] uppercase tracking-wider text-muted-foreground font-semibold mb-3 flex items-center gap-1.5">
                        <Layers className="h-3.5 w-3.5" /> Current data on record
                    </div>
                    <table className="w-full text-[13px] tabular">
                        <tbody>
                            {Object.entries(status.data.bsr_latest).map(([brand, d]) => (
                                <tr key={brand} className="border-b last:border-0">
                                    <td className="py-1.5">{brand} — latest BSR</td>
                                    <td className="py-1.5 text-right font-medium">{d ?? "—"}</td>
                                </tr>
                            ))}
                            <tr className="border-b">
                                <td className="py-1.5">Variation map rows</td>
                                <td className="py-1.5 text-right font-medium">{status.data.variations?.rows ?? "—"}</td>
                            </tr>
                            {PLANNING_BRANDS.map((b) => (
                                <tr key={b.key} className="border-b last:border-0">
                                    <td className="py-1.5">{b.label} — latest ASIN plan</td>
                                    <td className="py-1.5 text-right font-medium">{status.data.planning_latest?.[b.key] ?? "—"}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </Card>
            )}
        </AppLayout>
    );
}
