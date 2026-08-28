import { useState } from "react";
import { Upload as UploadIcon, CheckCircle2, AlertCircle } from "lucide-react";
import { apiUrl } from "@/lib/api";
import { useBrand } from "@/store/useBrand";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { todayIso } from "@/lib/format";

interface AccountRow {
  label: string;
  file: File | null;
  isVendor: boolean;
}

export function UploadPage() {
  const brand = useBrand((s) => s.brand);
  const [uploadKey, setUploadKey] = useState("");
  const [date, setDate] = useState(new Date().toISOString().slice(0, 10));
  const [replaceDay, setReplaceDay] = useState(true);
  const [rows, setRows] = useState<AccountRow[]>([
    { label: "", file: null, isVendor: false },
  ]);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; message?: string; error?: string; rows?: number } | null>(null);

  function updateRow(i: number, patch: Partial<AccountRow>) {
    setRows((r) => r.map((row, idx) => (idx === i ? { ...row, ...patch } : row)));
  }

  async function submit() {
    setBusy(true);
    setResult(null);
    try {
      const fd = new FormData();
      fd.append("upload_key", uploadKey);
      fd.append("sales_date", date);
      fd.append("replace_day", replaceDay ? "1" : "0");
      const accounts: string[] = [];
      const flags: string[] = [];
      for (const r of rows) {
        if (!r.file || !r.label.trim()) continue;
        fd.append("files", r.file);
        accounts.push(r.label.trim());
        flags.push(r.isVendor ? "1" : "0");
      }
      fd.append("accounts", accounts.join("|"));
      fd.append("is_vendor_flags", flags.join("|"));

      const resp = await fetch(apiUrl(`/api/${brand}/upload`), {
        method: "POST",
        credentials: "include",
        body: fd,
      });
      const json = await resp.json();
      setResult(json);
    } catch (e) {
      setResult({ ok: false, error: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto max-w-3xl px-8 pt-6 pb-10 space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Excel Upload</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Fallback for pushing sales without SP-API. Multi-account: add one row per Amazon file
          (parent-ASIN Seller Central reports, or Ordered-Revenue Vendor Central reports).
        </p>
      </div>

      <div className="rounded-xl border border-[hsl(var(--hairline))] bg-white p-5 shadow-[0_1px_2px_rgba(15,27,45,0.03)]">
        <div className="grid gap-4 md:grid-cols-3">
          <div>
            <label className="mb-1 block text-xs font-medium uppercase tracking-widest text-muted-foreground">
              Admin key
            </label>
            <Input type="password" value={uploadKey} onChange={(e) => setUploadKey(e.target.value)} />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium uppercase tracking-widest text-muted-foreground">
              Sales date
            </label>
            <Input type="date" value={date} max={todayIso()} onChange={(e) => setDate(e.target.value)} />
          </div>
          <label className="mt-6 flex items-center gap-2 text-sm">
            <input type="checkbox" checked={replaceDay} onChange={(e) => setReplaceDay(e.target.checked)} />
            Replace day (idempotent)
          </label>
        </div>

        <div className="mt-6 space-y-2">
          {rows.map((r, i) => (
            <div key={i} className="grid gap-2 rounded-md border border-[hsl(var(--hairline))] p-3 md:grid-cols-[1fr_2fr_auto]">
              <Input
                placeholder="Account label (e.g. Nexlev / Vendor Central)"
                value={r.label}
                onChange={(e) => updateRow(i, { label: e.target.value })}
              />
              <Input type="file" accept=".xlsx,.xls,.csv" onChange={(e) => updateRow(i, { file: e.target.files?.[0] ?? null })} />
              <label className="flex items-center gap-2 text-sm">
                <input type="checkbox" checked={r.isVendor} onChange={(e) => updateRow(i, { isVendor: e.target.checked })} />
                Vendor Central (1P)
              </label>
            </div>
          ))}
          <button
            type="button"
            className="text-xs font-medium text-[hsl(var(--primary))] hover:underline"
            onClick={() => setRows((rs) => [...rs, { label: "", file: null, isVendor: false }])}
          >
            + Add another account file
          </button>
        </div>

        <div className="mt-6 flex items-center justify-end">
          <Button onClick={submit} disabled={busy}>
            <UploadIcon className="mr-1.5 h-4 w-4" />
            {busy ? "Uploading…" : "Upload"}
          </Button>
        </div>
      </div>

      {result && (
        <div
          className={
            result.ok
              ? "rounded-md border border-[hsl(var(--emerald))] bg-[hsl(var(--emerald-soft))]/60 p-4 text-sm text-[hsl(var(--emerald))]"
              : "rounded-md border border-[hsl(var(--coral))] bg-[hsl(var(--coral-soft))] p-4 text-sm text-[hsl(var(--coral))]"
          }
        >
          <div className="flex items-center gap-2 font-semibold">
            {result.ok ? <CheckCircle2 className="h-4 w-4" /> : <AlertCircle className="h-4 w-4" />}
            {result.ok ? result.message ?? "Uploaded" : result.error ?? "Upload failed"}
          </div>
          {result.rows !== undefined && <div className="mt-1 text-xs opacity-80">{result.rows} rows ingested</div>}
        </div>
      )}
    </div>
  );
}
