import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { RefreshCw, CheckCircle2, AlertCircle, User, XCircle } from "lucide-react";
import { api } from "@/lib/api";
import { useBrand } from "@/store/useBrand";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { todayIso, formatDate } from "@/lib/format";
import { cn } from "@/lib/utils";

interface PullResult {
  ok: boolean;
  brand: string;
  sales_date: string;
  vendor_basis?: string;
  fetched_by_account: Record<string, number>;
  rows_by_account: Record<string, number>;
  total_rows: number;
  out_of_plan_count: number;
  warnings: string[];
}

interface RunRow {
  id: number;
  brand: string;
  sales_date: string | null;
  started_at: string;
  ended_at: string | null;
  ok: boolean | null;
  total_rows: number;
  out_of_plan_count: number;
  rows_by_account: Record<string, number> | null;
  warnings: string[] | null;
  error: string | null;
  triggered_by: string | null;
}

export function SyncPage() {
  const brand = useBrand((s) => s.brand);
  const yesterday = new Date(Date.now() - 24 * 3600_000).toISOString().slice(0, 10);
  const [date, setDate] = useState(yesterday);
  // 1P revenue basis. Ordered is the historical default and stays selected so
  // a routine daily pull is unchanged. Shipped is picked deliberately, for the
  // days where Amazon booked a PO cancellation as negative Ordered Revenue.
  const [basis, setBasis] = useState<"ordered" | "shipped">("ordered");

  const pull = useMutation<PullResult, Error, void>({
    mutationFn: () => api.post<PullResult>(`/api/${brand}/pull-sales`, undefined, { date, basis }),
    onSuccess: () => runs.refetch(),
  });

  const runs = useQuery<{ runs: RunRow[] }>({
    queryKey: ["sync-runs-page", brand],
    queryFn: () => api.get(`/api/${brand}/sync-runs`, { limit: 30 }),
    refetchInterval: 30_000,
    enabled: !!brand,
  });

  return (
    <div className="mx-auto max-w-3xl px-8 pt-6 pb-10 space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">SP-API Sync</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Pulls daily orders from every SP-API account attached to the selected brand — seller (3P) and
          vendor (1P). Numbers land in the ledger identically to a manual Excel upload.
        </p>
      </div>

      <div className="rounded-xl border border-[hsl(var(--hairline))] bg-white p-5 shadow-[0_1px_2px_rgba(15,27,45,0.03)]">
        <div className="grid gap-4 sm:grid-cols-[1fr_auto_auto_auto] sm:items-end">
          <div>
            <label className="mb-1 block text-xs font-medium uppercase tracking-widest text-muted-foreground">
              Sales date
            </label>
            <Input type="date" value={date} onChange={(e) => setDate(e.target.value)} max={todayIso()} />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium uppercase tracking-widest text-muted-foreground">
              1P revenue basis
            </label>
            <select
              value={basis}
              onChange={(e) => setBasis(e.target.value as "ordered" | "shipped")}
              className="h-9 rounded-md border border-[hsl(var(--hairline))] bg-white px-2 text-sm"
            >
              <option value="ordered">Ordered (default)</option>
              <option value="shipped">Shipped</option>
            </select>
          </div>
          <div className="text-xs text-muted-foreground">Brand · {brand}</div>
          <Button onClick={() => pull.mutate()} disabled={pull.isPending}>
            <RefreshCw className={pull.isPending ? "mr-1.5 h-4 w-4 animate-spin" : "mr-1.5 h-4 w-4"} />
            {pull.isPending ? "Pulling…" : "Pull now"}
          </Button>
        </div>
        {basis === "shipped" && (
          <p className="mt-3 text-xs text-muted-foreground">
            Shipped Revenue / Shipped Units, from Amazon's Sourcing view. Use this only for days a
            cancelled PO turned negative — mixing bases across dates makes the day-wise card
            unreadable. Seller (3P) accounts are unaffected either way.
          </p>
        )}
      </div>

      {pull.isError && (
        <div className="rounded-md border border-[hsl(var(--coral))] bg-[hsl(var(--coral-soft))] p-4 text-sm text-[hsl(var(--coral))]">
          <div className="flex items-center gap-2 font-semibold">
            <AlertCircle className="h-4 w-4" /> Pull failed
          </div>
          <div className="mt-1 text-xs">{pull.error.message}</div>
        </div>
      )}

      {pull.data && (
        <div className="rounded-xl border border-[hsl(var(--hairline))] bg-white p-5 shadow-[0_1px_2px_rgba(15,27,45,0.03)]">
          <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-[hsl(var(--emerald))]">
            <CheckCircle2 className="h-4 w-4" />
            Pull complete — {formatDate(pull.data.sales_date)}
            {pull.data.vendor_basis === "shipped" && (
              <span className="font-normal text-muted-foreground">· 1P on Shipped Revenue</span>
            )}
          </div>
          <div className="grid gap-3 text-sm sm:grid-cols-2">
            <Stat label="Total ledger rows" value={pull.data.total_rows} />
            <Stat label="Out-of-plan ASINs" value={pull.data.out_of_plan_count} tone={pull.data.out_of_plan_count ? "warn" : undefined} />
          </div>
          <div className="mt-4">
            <div className="mb-1 text-xs font-medium uppercase tracking-widest text-muted-foreground">Per account</div>
            <table className="w-full text-sm">
              <thead className="text-xs uppercase tracking-widest text-muted-foreground">
                <tr>
                  <th className="px-2 py-1.5 text-left font-medium">Account</th>
                  <th className="px-2 py-1.5 text-right font-medium">Fetched</th>
                  <th className="px-2 py-1.5 text-right font-medium">Ingested</th>
                </tr>
              </thead>
              <tbody className="font-mono tabular-nums">
                {Object.keys(pull.data.fetched_by_account).map((acc) => (
                  <tr key={acc} className="border-t border-[hsl(var(--hairline))]">
                    <td className="px-2 py-1.5 font-sans">{acc}</td>
                    <td className="px-2 py-1.5 text-right">{pull.data!.fetched_by_account[acc] ?? 0}</td>
                    <td className="px-2 py-1.5 text-right">{pull.data!.rows_by_account[acc] ?? 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {pull.data.warnings.length > 0 && (
            <div className="mt-4 rounded-md bg-[hsl(var(--amber-soft))] p-3 text-xs text-[hsl(var(--amber-deep))]">
              <div className="mb-1 font-semibold">Warnings</div>
              <ul className="list-disc pl-5">
                {pull.data.warnings.map((w, i) => (
                  <li key={i}>{w}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}

      <RunsHistory rows={runs.data?.runs ?? []} loading={runs.isLoading} />
    </div>
  );
}

function RunsHistory({ rows, loading }: { rows: RunRow[]; loading: boolean }) {
  return (
    <div className="rounded-xl border border-[hsl(var(--hairline))] bg-white p-5">
      <div className="mb-3 flex items-center justify-between">
        <h2 className="text-[11px] font-bold uppercase tracking-widest text-[hsl(var(--ink-2))]">
          Recent syncs · every account, every attempt
        </h2>
        <span className="pill pill-muted">{rows.length} recent</span>
      </div>
      {loading && rows.length === 0 && (
        <div className="py-6 text-center text-sm text-[hsl(var(--ink-2))]">Loading…</div>
      )}
      {!loading && rows.length === 0 && (
        <div className="py-6 text-center text-sm text-[hsl(var(--ink-2))]">No syncs yet — hit Pull now above.</div>
      )}
      {rows.length > 0 && (
        <div className="overflow-auto">
          <table className="w-full text-sm">
            <thead className="text-[11px] uppercase tracking-widest text-[hsl(var(--ink))]">
              <tr className="border-b hairline">
                <th className="px-2 py-2 text-left font-bold">Status</th>
                <th className="px-2 py-2 text-left font-bold">Sales date</th>
                <th className="px-2 py-2 text-left font-bold">When</th>
                <th className="px-2 py-2 text-left font-bold">By</th>
                <th className="px-2 py-2 text-left font-bold">Per account (ingested)</th>
                <th className="px-2 py-2 text-right font-bold">Total</th>
              </tr>
            </thead>
            <tbody className="font-mono tabular-nums">
              {rows.map((r) => {
                const state = r.ok === true ? "ok" : r.ok === false ? "fail" : "pending";
                const dur =
                  r.started_at && r.ended_at
                    ? Math.round((new Date(r.ended_at).getTime() - new Date(r.started_at).getTime()) / 1000)
                    : null;
                return (
                  <tr key={r.id} className="border-b border-[hsl(var(--hairline))] align-top">
                    <td className="px-2 py-2">
                      <span
                        className={cn(
                          "pill text-[10px]",
                          state === "ok" ? "pill-emerald-soft" : state === "fail" ? "pill-coral-soft" : "pill-amber-soft"
                        )}
                      >
                        {state === "ok" ? <CheckCircle2 className="mr-1 h-3 w-3" /> : state === "fail" ? <XCircle className="mr-1 h-3 w-3" /> : <RefreshCw className="mr-1 h-3 w-3 animate-spin" />}
                        {state}
                      </span>
                    </td>
                    <td className="px-2 py-2 font-sans">{r.sales_date ? formatDate(r.sales_date) : "—"}</td>
                    <td className="px-2 py-2 font-sans text-[hsl(var(--ink-2))]">
                      {new Date(r.started_at).toLocaleString("en-IN", {
                        day: "2-digit",
                        month: "short",
                        hour: "2-digit",
                        minute: "2-digit",
                      })}
                      {dur !== null && <span className="ml-1 text-[10px]">· {dur}s</span>}
                    </td>
                    <td className="px-2 py-2 font-sans text-[12px]">
                      {r.triggered_by ? (
                        <span className="inline-flex items-center gap-1">
                          <User className="h-3 w-3 text-[hsl(var(--ink-2))]" />
                          {r.triggered_by.split("@")[0]}
                        </span>
                      ) : (
                        <span className="text-[hsl(var(--ink-2))]">—</span>
                      )}
                    </td>
                    <td className="px-2 py-2 font-sans">
                      <div className="flex flex-wrap gap-1">
                        {Object.entries(r.rows_by_account ?? {}).map(([acc, n]) => (
                          <span
                            key={acc}
                            className={cn(
                              "pill text-[10px]",
                              n > 0 ? "pill-emerald-soft" : "pill-muted"
                            )}
                            title={acc}
                          >
                            {acc.split(" ")[0]}: {n}
                          </span>
                        ))}
                      </div>
                      {r.error && (
                        <div className="mt-1 text-[11px] text-[hsl(var(--coral))]">{r.error.slice(0, 200)}</div>
                      )}
                    </td>
                    <td className="px-2 py-2 text-right font-semibold">{r.total_rows.toLocaleString("en-IN")}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function Stat({ label, value, tone }: { label: string; value: number; tone?: "warn" }) {
  return (
    <div className="rounded-md border border-[hsl(var(--hairline))] bg-[hsl(var(--hairline-soft))]/40 px-3 py-2">
      <div className="text-xs uppercase tracking-widest text-muted-foreground">{label}</div>
      <div className={`mt-1 font-mono text-xl font-semibold tabular-nums ${tone === "warn" ? "text-[hsl(var(--amber-deep))]" : ""}`}>
        {value.toLocaleString("en-IN")}
      </div>
    </div>
  );
}
