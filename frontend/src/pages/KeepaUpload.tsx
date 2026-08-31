import { useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppLayout from "@/components/AppLayout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { api, ApiError } from "@/lib/api";
import { UploadCloud, CheckCircle2, AlertTriangle, FileUp, Layers } from "lucide-react";

interface StatusData {
    bsr_latest: Record<string, string | null>;
    variations: { rows: number } | null;
    github_configured: boolean;
    today: string;
}
interface UploadResult { ok: boolean; commit: string | null; note: string; brands?: string[]; date?: string; }

async function postFiles(url: string, files: FileList, fieldName: string): Promise<UploadResult> {
    const fd = new FormData();
    for (const f of Array.from(files)) fd.append(fieldName, f);
    const res = await fetch(url, { method: "POST", body: fd, credentials: "include" });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) throw new ApiError(res.status, `Upload failed (${res.status})`, body);
    return body as UploadResult;
}

function UploadCard({
    title, subtitle, accept, multiple, endpoint, fieldName, onDone,
}: {
    title: string; subtitle: string; accept: string; multiple: boolean;
    endpoint: string; fieldName: string; onDone: () => void;
}) {
    const inputRef = useRef<HTMLInputElement>(null);
    const [busy, setBusy] = useState(false);
    const [result, setResult] = useState<UploadResult | null>(null);
    const [error, setError] = useState<string | null>(null);

    async function handle(files: FileList | null) {
        if (!files || files.length === 0) return;
        setBusy(true); setError(null); setResult(null);
        try {
            const r = await postFiles(endpoint, files, fieldName);
            setResult(r);
            onDone();
        } catch (e) {
            const body = (e instanceof ApiError ? e.body : null) as { detail?: string } | null;
            setError(body?.detail || (e as Error).message || "Upload failed");
        } finally {
            setBusy(false);
            if (inputRef.current) inputRef.current.value = "";
        }
    }

    return (
        <Card className="p-5 flex-1 min-w-[320px]">
            <div className="text-[15px] font-semibold mb-1">{title}</div>
            <p className="text-[12.5px] text-muted-foreground mb-4">{subtitle}</p>
            <input
                ref={inputRef} type="file" accept={accept} multiple={multiple}
                className="hidden" onChange={(e) => handle(e.target.files)}
            />
            <Button onClick={() => inputRef.current?.click()} disabled={busy} size="sm">
                <UploadCloud className={"h-4 w-4 " + (busy ? "animate-pulse" : "")} />
                {busy ? "Uploading & committing…" : "Choose file" + (multiple ? "(s)" : "")}
            </Button>
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
                    subtitle={"Per-brand Keepa CSVs (Nexlev / Audio array / Tonor / White Mulberry) or one ZIP of them. Filed under today's date" + (status.data ? ` (${status.data.today})` : "") + " and /buybox rebuilds with the new BSR data."}
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
                            <tr>
                                <td className="py-1.5">Variation map rows</td>
                                <td className="py-1.5 text-right font-medium">{status.data.variations?.rows ?? "—"}</td>
                            </tr>
                        </tbody>
                    </table>
                </Card>
            )}
        </AppLayout>
    );
}
