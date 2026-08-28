import { useEffect, useMemo, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import {
  Area,
  AreaChart,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  PieChart,
  Pie,
  Cell,
  Legend,
  ReferenceLine,
  Sector,
} from "recharts";
import { motion } from "framer-motion";
import { TrendingUp, Target, Activity, Package, Zap, AlertCircle, ChevronDown, ChevronRight, Search, ArrowUpDown, ArrowUp, ArrowDown, Download } from "lucide-react";
import { api, apiUrl } from "@/lib/api";
import { useBrand } from "@/store/useBrand";
import { track } from "@/lib/activity";
import { formatINRCompact, formatNumber, formatPct, formatDate } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { AsinRow, CategoryRow, DashboardData, DonutBundle, ModelTrendBundle } from "@/types/dashboard";

const DONUT_COLORS = [
  "hsl(212, 51%, 25%)",
  "hsl(40, 51%, 65%)",
  "hsl(158, 94%, 24%)",
  "hsl(0, 74%, 42%)",
  "hsl(262, 80%, 50%)",
  "hsl(36, 40%, 50%)",
  "hsl(220, 9%, 46%)",
  "hsl(212, 51%, 45%)",
];

interface Filters {
  month?: string;
  from_date?: string;
  to_date?: string;
  trend_month?: string;
  plan_scope?: boolean;
}

function currentMonthLabel(): string {
  return new Date().toLocaleDateString("en-US", { month: "short", year: "numeric" });
}

export function Dashboard() {
  const brand = useBrand((s) => s.brand);
  // Start with the current calendar month so the FIRST fetch is already
  // scoped — no unfiltered flash before the auto-select useEffect kicks in.
  const [filters, setFilters] = useState<Filters>(() => ({ month: currentMonthLabel(), plan_scope: true }));

  const { data, isLoading, isError, error } = useQuery<DashboardData>({
    queryKey: ["dashboard", brand, filters],
    queryFn: () =>
      api.get<DashboardData>(`/api/${brand}/dashboard`, {
        month: filters.month,
        from_date: filters.from_date,
        to_date: filters.to_date,
        trend_month: filters.trend_month,
        plan_scope: filters.plan_scope === false ? "false" : "true",
      }),
    enabled: !!brand,
    placeholderData: keepPreviousData,
  });

  // Auto-select the latest available month on first load. Keeps every section
  // (ASIN target/actual, category rollup, donut) populated without requiring
  // the operator to touch the filter bar first.
  useEffect(() => {
    if (!filters.month && !filters.from_date && data?.months?.length) {
      setFilters({ month: data.months[data.months.length - 1] });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [brand, data?.months?.length]);

  // On brand switch, reset to current calendar month so the fetch is scoped
  // immediately (avoids a one-frame unfiltered flash).
  useEffect(() => {
    setFilters((prev) => ({ month: currentMonthLabel(), plan_scope: prev.plan_scope ?? true }));
  }, [brand]);

  if (isLoading && !data) {
    return <div className="animate-pulse text-sm text-muted-foreground">Loading dashboard…</div>;
  }
  if (isError || !data) {
    return (
      <div className="rounded-md border border-[hsl(var(--coral))] bg-[hsl(var(--coral-soft))] p-4 text-sm text-[hsl(var(--coral))]">
        <div className="flex items-center gap-2 font-semibold">
          <AlertCircle className="h-4 w-4" /> Failed to load dashboard
        </div>
        <div className="mt-1 text-xs">{error instanceof Error ? error.message : String(error)}</div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-[1440px] px-8 pt-6 pb-10 space-y-6">
      {/* 1. Header — brand + status pills + download */}
      <Header brandLabel={data.brand.label} spEnabled={data.sp_api_enabled} brandKey={data.brand.key} filters={filters} />

      {/* 2. Filter bar — month/date/scope in one row */}
      <FilterBar data={data} filters={filters} setFilters={setFilters} />

      {/* 3. Headline KPI band */}
      <KpiRow data={data} />

      {/* 4. Hero visual — MTD cumulative (full-width, biggest thing on the page) */}
      <MtdCumulativeCard data={data} />

      {/* 5. Insights + day-wise perf side-by-side (both live-updating, need to be together) */}
      <div className="grid gap-6 xl:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <InsightsCard data={data} />
        <DayWiseCard data={data} />
      </div>

      {/* 6. Category rollup — donut + table side-by-side (single narrative) */}
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <CategoryDonutCard donut={data.category_donut} catRows={data.cat_rows} />
        <CategoryTable rows={data.cat_rows} />
      </div>

      {/* 7. Week-wise — compact, moved down since it's summary-of-summary */}
      <WeekWiseCard data={data} />

      {/* 8. Deep-dives — collapsed by default */}
      <AsinTable rows={data.asin_rows} />
      {data.monthwise_chart?.labels?.length ? (
        <MonthwiseCard chart={data.monthwise_chart} />
      ) : (
        <ComingSoonCard
          title="Monthwise — by ASIN"
          hint="Fills as multiple months of history accumulate."
        />
      )}
      {data.model_trend && data.model_trend.rows?.length ? (
        <ModelTrendCard bundle={data.model_trend} />
      ) : (
        <ComingSoonCard
          title="3-Month Model Trend"
          hint="Fills automatically once two full past months of data are available."
        />
      )}
    </div>
  );
}

function ComingSoonCard({ title, hint }: { title: string; hint: string }) {
  return (
    <Card title={title}>
      <div className="grid h-32 place-items-center text-center">
        <div>
          <div className="text-sm font-medium text-[hsl(var(--ink-2))]">Building up history…</div>
          <div className="mt-1 max-w-md text-xs text-muted-foreground">{hint}</div>
        </div>
      </div>
    </Card>
  );
}

function Header({
  brandLabel,
  spEnabled,
  brandKey,
  filters,
}: {
  brandLabel: string;
  spEnabled: boolean;
  brandKey: string;
  filters: Filters;
}) {
  const downloadLedger = () => {
    const q = new URLSearchParams();
    if (filters.month) q.set("month", filters.month);
    if (filters.from_date) q.set("from_date", filters.from_date);
    if (filters.to_date) q.set("to_date", filters.to_date);
    const url = apiUrl(`/api/${brandKey}/download-ledger.csv${q.toString() ? "?" + q : ""}`);
    // Same-origin fetch (credentials) → blob → save. window.open on a different
    // subdomain wouldn't carry the auth cookie.
    fetch(url, { credentials: "include" })
      .then((r) => r.blob())
      .then((blob) => {
        const dl = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = dl;
        a.download = `${brandKey}-ledger-${filters.month ?? "all"}.csv`;
        a.click();
        URL.revokeObjectURL(dl);
      });
  };
  return (
    <div className="flex items-start justify-between gap-4">
      <div>
        <h1 className="text-[32px] leading-none">Sales performance</h1>
        <div className="mt-2 text-[13px] font-semibold flex items-center gap-2 flex-wrap">
          <span className="pill pill-navy-soft">{brandLabel}</span>
          <span className="pill pill-muted">
            {new Date().toLocaleDateString("en-IN", { weekday: "short", day: "2-digit", month: "short", year: "numeric" })}
          </span>
          <span className={cn("pill", spEnabled ? "pill-emerald-soft" : "pill-muted")}>
            SP-API {spEnabled ? "connected" : "not configured"}
          </span>
          <LastSyncPill brandKey={brandKey} />
        </div>
      </div>
      <button onClick={downloadLedger} className="btn-outline flex h-9 items-center gap-1.5 rounded-md px-3 text-[12px] font-semibold">
        <Download className="h-3.5 w-3.5" />
        Download ledger
      </button>
    </div>
  );
}

function LastSyncPill({ brandKey }: { brandKey: string }) {
  const { data } = useQuery<{ last_ok?: { started_at: string; total_rows: number; sales_date: string } | null; runs?: Array<{ ok: boolean | null; started_at: string }> }>({
    queryKey: ["sync-runs", brandKey],
    queryFn: () => api.get(`/api/${brandKey}/sync-runs`, { limit: 5 }),
    refetchInterval: 60_000,
    staleTime: 30_000,
    enabled: !!brandKey,
  });
  if (!data) return null;
  const last = data.last_ok;
  const lastAny = data.runs?.[0];
  const lastFailed = lastAny && lastAny.ok === false;
  if (!last && !lastAny) {
    return <span className="pill pill-muted">Never synced</span>;
  }
  const when = last?.started_at ? new Date(last.started_at) : lastAny?.started_at ? new Date(lastAny.started_at) : null;
  const rel = when ? relTime(when) : "—";
  return (
    <span
      className={cn(
        "pill",
        lastFailed ? "pill-coral-soft" : "pill-emerald-soft"
      )}
      title={when ? when.toLocaleString("en-IN") : ""}
    >
      {lastFailed ? "Last sync failed" : `Last sync ${rel}`}
      {last?.total_rows ? ` · ${last.total_rows.toLocaleString("en-IN")} rows` : ""}
    </span>
  );
}

function relTime(d: Date): string {
  const ms = Date.now() - d.getTime();
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  const days = Math.round(h / 24);
  return `${days}d ago`;
}

function FilterBar({ data, filters, setFilters }: { data: DashboardData; filters: Filters; setFilters: (f: Filters) => void }) {
  const planOn = filters.plan_scope !== false;
  return (
    <div className="surface-hi rounded-xl p-4">
      <div className="grid gap-3 md:grid-cols-4">
      <div>
        <label className="mb-1 block text-xs font-medium uppercase tracking-widest text-muted-foreground">
          Month
        </label>
        <select
          className="h-9 w-full rounded-md border hairline bg-[hsl(var(--paper))] px-2 text-[13px] font-semibold"
          value={filters.month ?? ""}
          onChange={(e) => setFilters({ ...filters, month: e.target.value || undefined })}
        >
          <option value="">All months</option>
          {data.months.map((m) => (
            <option key={m} value={m}>
              {m}
            </option>
          ))}
        </select>
      </div>
      <div>
        <label className="mb-1 block text-xs font-medium uppercase tracking-widest text-muted-foreground">
          From
        </label>
        <input
          type="date"
          className="h-9 w-full rounded-md border hairline bg-[hsl(var(--paper))] px-2 text-[13px] font-semibold"
          value={filters.from_date ?? ""}
          onChange={(e) => setFilters({ ...filters, from_date: e.target.value || undefined, month: undefined })}
        />
      </div>
      <div>
        <label className="mb-1 block text-xs font-medium uppercase tracking-widest text-muted-foreground">
          To
        </label>
        <input
          type="date"
          className="h-9 w-full rounded-md border hairline bg-[hsl(var(--paper))] px-2 text-[13px] font-semibold"
          value={filters.to_date ?? ""}
          onChange={(e) => setFilters({ ...filters, to_date: e.target.value || undefined, month: undefined })}
        />
      </div>
      <div>
        <label className="mb-1 block text-xs font-medium uppercase tracking-widest text-muted-foreground">
          Trend month
        </label>
        <select
          className="h-9 w-full rounded-md border hairline bg-[hsl(var(--paper))] px-2 text-[13px] font-semibold"
          value={filters.trend_month ?? ""}
          onChange={(e) => setFilters({ ...filters, trend_month: e.target.value || undefined })}
        >
          <option value="">Latest complete</option>
          {data.trend_months.map((m) => (
            <option key={m} value={m}>
              {m}
            </option>
          ))}
        </select>
      </div>
      </div>

      <div className="mt-3 pt-3 border-t hairline flex items-center justify-between gap-3">
        <label className="flex items-center gap-2 text-[13px] font-semibold text-[hsl(var(--ink))] cursor-pointer">
          <input
            type="checkbox"
            checked={planOn}
            onChange={(e) => setFilters({ ...filters, plan_scope: e.target.checked })}
            className="h-4 w-4"
          />
          Plan-scope
          <span className="pill pill-muted text-[10px]">
            {planOn
              ? `KPIs use ${data.plan_asin_count} planned ASINs only`
              : "KPIs include every row (cross-brand inflated)"}
          </span>
        </label>
        {data.plan_scope_active === false && planOn && (
          <span className="pill pill-amber-soft">Plan file missing for period — showing all rows</span>
        )}
      </div>
    </div>
  );
}

function KpiCard({
  label,
  value,
  tint,
  icon: Icon,
  spark,
  sparkColor,
  index = 0,
  sub,
  progressPct,
  progressColor,
}: {
  label: string;
  value: React.ReactNode;
  tint: string;
  icon: React.ComponentType<{ className?: string }>;
  spark?: number[];
  sparkColor?: string;
  index?: number;
  sub?: React.ReactNode;
  progressPct?: number;
  progressColor?: string;
}) {
  const rows = useMemo(() => (spark ?? []).map((v, i) => ({ i, v })), [spark]);
  const clamped = progressPct !== undefined ? Math.max(0, Math.min(150, progressPct)) : undefined;
  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, delay: index * 0.04, ease: "easeOut" }}
      className="group surface-hi relative overflow-hidden rounded-xl p-5 transition-shadow hover:shadow-[0_2px_8px_rgba(15,27,45,0.06)]"
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <div className="text-[10px] font-bold uppercase tracking-widest text-[hsl(var(--ink-2))]">{label}</div>
          <div className="num mt-2 text-[28px] leading-none">{value}</div>
          {sub && <div className="mt-1.5 text-[11px] font-semibold text-[hsl(var(--ink-2))]">{sub}</div>}
        </div>
        <div className={cn("grid h-9 w-9 shrink-0 place-items-center rounded-md", tint)}>
          <Icon className="h-4 w-4" />
        </div>
      </div>
      {clamped !== undefined && (
        <div className="mt-3 space-y-1">
          <div className="h-1.5 w-full overflow-hidden rounded-full bg-[hsl(var(--hairline))]">
            <div
              className="h-full rounded-full transition-all duration-700"
              style={{
                width: `${Math.min(100, clamped)}%`,
                background: progressColor ?? "hsl(var(--primary))",
                boxShadow: clamped > 100 ? `inset 0 0 0 999px ${progressColor ?? "hsl(var(--primary))"}` : undefined,
              }}
            />
          </div>
          <div className="flex items-center justify-between font-mono text-[10px] font-semibold tabular-nums text-[hsl(var(--ink-2))]">
            <span>0%</span>
            <span>{clamped >= 100 ? "on / above pace" : "behind pace"}</span>
            <span>100%</span>
          </div>
        </div>
      )}
      {rows.length > 1 && clamped === undefined && (
        <div className="mt-3 h-10">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={rows} margin={{ top: 2, right: 0, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id={`spark-${label.replace(/\s/g, "")}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={sparkColor ?? "hsl(var(--primary))"} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={sparkColor ?? "hsl(var(--primary))"} stopOpacity={0} />
                </linearGradient>
              </defs>
              <Area
                type="monotone"
                dataKey="v"
                stroke={sparkColor ?? "hsl(var(--primary))"}
                strokeWidth={1.75}
                fill={`url(#spark-${label.replace(/\s/g, "")})`}
                isAnimationActive
                animationDuration={500}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </motion.div>
  );
}

// Values/data text should be solid ink (no greyish muted foreground).
function KpiRow({ data }: { data: DashboardData }) {
  const actualSeries = useMemo(() => data.daywise.map((r) => r.actual ?? r.net_sales ?? 0), [data.daywise]);
  const targetSeries = useMemo(() => data.daywise.map((r) => r.target ?? 0), [data.daywise]);
  const cumActual = useMemo(() => {
    let s = 0;
    return actualSeries.map((v) => (s += v));
  }, [actualSeries]);
  const cumTarget = useMemo(() => {
    let s = 0;
    return targetSeries.map((v) => (s += v));
  }, [targetSeries]);
  const unitsSeries = useMemo(
    () => data.daywise.map((r) => (r as unknown as { units?: number }).units ?? 0),
    [data.daywise]
  );
  const achievementPct = (data.achievement ?? 0) * 100;
  const pacePct = (data.pace ?? 0) * 100;
  const achColor =
    achievementPct >= 100
      ? "hsl(var(--emerald))"
      : achievementPct >= 70
      ? "hsl(var(--amber-deep))"
      : "hsl(var(--coral))";
  const paceDelta = data.actual - data.target_till;
  const avgPerDay =
    data.daywise.length ? Math.round(data.total_units_ordered / data.daywise.length) : 0;
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
      <KpiCard
        index={0}
        label="Monthly target"
        tint="bg-[hsl(var(--primary-soft))] text-[hsl(var(--primary))]"
        icon={Target}
        value={formatINRCompact(data.monthly_target)}
        spark={cumTarget}
        sparkColor="hsl(var(--primary))"
        sub={data.plan_asin_count ? `${data.plan_asin_count} planned ASINs` : undefined}
      />
      <KpiCard
        index={1}
        label="Target till today"
        tint="bg-[hsl(var(--amber-soft))] text-[hsl(var(--amber-deep))]"
        icon={Activity}
        value={formatINRCompact(data.target_till)}
        spark={targetSeries}
        sparkColor="hsl(var(--amber-deep))"
        sub={data.daywise.length ? `${data.daywise.length} days in view` : undefined}
      />
      <KpiCard
        index={2}
        label="Actual"
        tint="bg-[hsl(var(--emerald-soft))] text-[hsl(var(--emerald))]"
        icon={TrendingUp}
        value={formatINRCompact(data.actual)}
        spark={cumActual}
        sparkColor="hsl(var(--emerald))"
        sub={
          data.target_till > 0 ? (
            <span
              className={
                paceDelta >= 0 ? "text-[hsl(var(--emerald))]" : "text-[hsl(var(--coral))]"
              }
            >
              {paceDelta >= 0 ? "▲" : "▼"} {formatINRCompact(Math.abs(paceDelta))} vs pace
            </span>
          ) : undefined
        }
      />
      <KpiCard
        index={3}
        label="Achievement"
        tint="bg-[hsl(var(--plum-soft))] text-[hsl(var(--plum))]"
        icon={Zap}
        value={formatPct(achievementPct)}
        progressPct={achievementPct}
        progressColor={achColor}
        sub={pacePct ? `Pace ${formatPct(pacePct)}` : undefined}
      />
      <KpiCard
        index={4}
        label="Units ordered"
        tint="bg-[hsl(var(--gold-soft))] text-[hsl(var(--gold))]"
        icon={Package}
        value={formatNumber(data.total_units_ordered)}
        spark={unitsSeries}
        sparkColor="hsl(var(--gold))"
        sub={avgPerDay ? `${formatNumber(avgPerDay)} avg/day` : undefined}
      />
    </div>
  );
}

function InsightsCard({ data }: { data: DashboardData }) {
  const inPlan = data.asin_rows.filter((r) => !r.out_of_plan);

  const withTarget = inPlan.filter((r) => (r.target || 0) > 0);
  const topWinners = [...withTarget]
    .filter((r) => (r.actual || 0) > 0)
    .sort((a, b) => (b.achievement || 0) - (a.achievement || 0))
    .slice(0, 5);
  const topMisses = [...withTarget]
    .sort((a, b) => (a.achievement || 0) - (b.achievement || 0))
    .slice(0, 5);
  const topRevenue = [...inPlan].sort((a, b) => (b.actual || 0) - (a.actual || 0)).slice(0, 5);

  if (inPlan.length === 0) return null;

  return (
    <Card title="Insights">
      <div className="grid gap-4 md:grid-cols-3">
        <InsightList
          title="Top achievers"
          tone="emerald"
          rows={topWinners.map((r) => ({
            primary: r.model_no || r.asin,
            secondary: r.asin,
            value: formatPct(r.achievement),
            valueClass: "text-[hsl(var(--emerald))]",
          }))}
        />
        <InsightList
          title="Biggest misses"
          tone="coral"
          rows={topMisses.map((r) => ({
            primary: r.model_no || r.asin,
            secondary: r.asin,
            value: formatPct(r.achievement),
            valueClass: "text-[hsl(var(--coral))]",
          }))}
        />
        <InsightList
          title="Highest revenue"
          tone="primary"
          rows={topRevenue.map((r) => ({
            primary: r.model_no || r.asin,
            secondary: r.category || r.asin,
            value: formatINRCompact(r.actual),
            valueClass: "text-[hsl(var(--ink))]",
          }))}
        />
      </div>
    </Card>
  );
}

function InsightList({
  title,
  tone,
  rows,
}: {
  title: string;
  tone: "primary" | "amber" | "coral" | "emerald";
  rows: Array<{ primary: string; secondary: string; value: string; valueClass?: string }>;
}) {
  const pillTone = {
    primary: "pill-navy-soft",
    amber: "pill-amber-soft",
    coral: "pill-coral-soft",
    emerald: "pill-emerald-soft",
  }[tone];
  return (
    <div className="rounded-lg border hairline p-3">
      <div className="mb-2 flex items-center justify-between">
        <span className={cn("pill text-[10px]", pillTone)}>{title}</span>
      </div>
      {rows.length === 0 ? (
        <div className="py-4 text-center text-[12px] text-[hsl(var(--ink-2))]">No data</div>
      ) : (
        <ul className="space-y-1.5">
          {rows.map((r, i) => (
            <li key={i} className="flex items-center justify-between gap-2 text-[13px]">
              <span className="min-w-0 flex-1 truncate">
                <span className="font-semibold">{r.primary}</span>
                {r.secondary && r.secondary !== r.primary && (
                  <span className="ml-1.5 text-[11px] text-[hsl(var(--ink-2))]">{r.secondary}</span>
                )}
              </span>
              <span className={cn("font-mono tabular-nums font-semibold", r.valueClass)}>{r.value}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function MtdCumulativeCard({ data }: { data: DashboardData }) {
  const rows = useMemo(
    () =>
      data.chart.labels.map((l, i) => ({
        label: l,
        actual: data.chart.actual[i] ?? 0,
        target: data.chart.target[i] ?? 0,
        gap: (data.chart.actual[i] ?? 0) - (data.chart.target[i] ?? 0),
      })),
    [data.chart]
  );
  const finalActual = rows.at(-1)?.actual ?? 0;
  const finalTarget = rows.at(-1)?.target ?? 0;
  const pace = finalTarget ? (finalActual / finalTarget) * 100 : 0;
  return (
    <Card
      title="Cumulative — Actual vs Target"
      badge={
        rows.length > 0 ? (
          <span
            className={cn(
              "rounded-full px-2 py-0.5 font-mono text-[11px] font-semibold tabular-nums",
              pace >= 100
                ? "bg-[hsl(var(--emerald-soft))] text-[hsl(var(--emerald))]"
                : pace >= 70
                ? "bg-[hsl(var(--amber-soft))] text-[hsl(var(--amber-deep))]"
                : "bg-[hsl(var(--coral-soft))] text-[hsl(var(--coral))]"
            )}
          >
            {formatPct(pace)} pace
          </span>
        ) : null
      }
    >
      {rows.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={rows} margin={{ top: 8, right: 16, left: 8, bottom: 4 }}>
              <defs>
                <linearGradient id="mtdActual" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="hsl(var(--primary))" stopOpacity={0.28} />
                  <stop offset="100%" stopColor="hsl(var(--primary))" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="mtdTarget" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="hsl(var(--amber-deep))" stopOpacity={0.14} />
                  <stop offset="100%" stopColor="hsl(var(--amber-deep))" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="2 4" stroke="hsl(var(--hairline))" vertical={false} />
              <XAxis dataKey="label" tick={{ fontSize: 11, fill: "hsl(var(--ink-2))" }} stroke="hsl(var(--hairline))" tickLine={false} axisLine={false} />
              <YAxis tick={{ fontSize: 11, fill: "hsl(var(--ink-2))" }} tickLine={false} axisLine={false} tickFormatter={(v) => formatINRCompact(v)} />
              <Tooltip content={<SmartTooltip />} cursor={{ stroke: "hsl(var(--hairline))", strokeDasharray: "3 3" }} />
              <Area type="monotone" dataKey="target" stroke="hsl(var(--amber-deep))" strokeDasharray="4 4" strokeWidth={2} fill="url(#mtdTarget)" name="Target" isAnimationActive animationDuration={600} />
              <Area type="monotone" dataKey="actual" stroke="hsl(var(--primary))" strokeWidth={2.5} fill="url(#mtdActual)" name="Actual" isAnimationActive animationDuration={800} />
              {finalTarget > 0 && (
                <ReferenceLine y={finalTarget} stroke="hsl(var(--amber-deep))" strokeOpacity={0.3} strokeDasharray="2 4" />
              )}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </Card>
  );
}

function SmartTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ name: string; value: number; color: string; dataKey: string }>; label?: string }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border border-[hsl(var(--hairline))] bg-white px-3 py-2 shadow-md">
      <div className="mb-1 text-[11px] font-medium uppercase tracking-widest text-muted-foreground">{label}</div>
      <div className="space-y-1">
        {payload.map((p) => (
          <div key={p.dataKey} className="flex items-center justify-between gap-4 text-xs">
            <span className="flex items-center gap-1.5">
              <span className="h-2 w-2 rounded-full" style={{ background: p.color }} />
              <span className="font-medium">{p.name}</span>
            </span>
            <span className="font-mono tabular-nums">{formatINRCompact(p.value)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function DayWiseCard({ data }: { data: DashboardData }) {
  const { sortKey, sortDir, flip, sort } = useSort<Record<string, unknown>>("date", "desc");
  const sorted = useMemo(() => sort(data.daywise as unknown as Record<string, unknown>[]), [data.daywise, sortKey, sortDir]);
  const exportDay = () => {
    downloadCsv(
      `daywise_${new Date().toISOString().slice(0, 10)}.csv`,
      ["Date", "Actual", "Target", "Achievement %"],
      (sorted as unknown as DashboardData["daywise"]).map((r) => [
        String(r.date).slice(0, 10),
        r.actual ?? r.net_sales ?? 0,
        r.target ?? 0,
        ((r.achieved ?? 0) * 100).toFixed(1),
      ])
    );
  };
  return (
    <Card title="Day-wise performance" badge={<ExportBtn onClick={exportDay} />}>
      {data.daywise.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="max-h-72 overflow-auto text-sm">
          <table className="w-full">
            <thead className="sticky top-0 bg-white text-[11px] uppercase tracking-widest text-[hsl(var(--ink))]">
              <tr className="border-b hairline">
                <th className="px-2 py-2 text-left font-bold cursor-pointer" onClick={() => flip("date", "asc")}>
                  Date<SortArrow active={sortKey === "date"} dir={sortDir} />
                </th>
                <th className="px-2 py-2 text-right font-bold cursor-pointer" onClick={() => flip("net_sales")}>
                  Actual<SortArrow active={sortKey === "net_sales"} dir={sortDir} />
                </th>
                <th className="px-2 py-2 text-right font-bold cursor-pointer" onClick={() => flip("target")}>
                  Target<SortArrow active={sortKey === "target"} dir={sortDir} />
                </th>
                <th className="px-2 py-2 text-right font-bold cursor-pointer" onClick={() => flip("achieved")}>
                  Ach.<SortArrow active={sortKey === "achieved"} dir={sortDir} />
                </th>
              </tr>
            </thead>
            <tbody className="font-mono tabular-nums">
              {(sorted as unknown as DashboardData["daywise"]).map((r, i) => (
                <tr key={i} className="border-b border-[hsl(var(--hairline))]">
                  <td className="px-2 py-1.5 font-sans">{formatDate(r.date)}</td>
                  <td className="px-2 py-1.5 text-right">{formatINRCompact(r.actual ?? r.net_sales)}</td>
                  <td className="px-2 py-1.5 text-right">{formatINRCompact(r.target)}</td>
                  <td
                    className={cn(
                      "px-2 py-1.5 text-right font-semibold",
                      (r.achieved ?? 0) >= 1
                        ? "text-[hsl(var(--emerald))]"
                        : (r.achieved ?? 0) >= 0.7
                        ? "text-[hsl(var(--amber-deep))]"
                        : "text-[hsl(var(--coral))]"
                    )}
                  >
                    {formatPct((r.achieved ?? 0) * 100)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

function WeekWiseCard({ data }: { data: DashboardData }) {
  const { sortKey, sortDir, flip, sort } = useSort<Record<string, unknown>>("week", "asc");
  const sorted = useMemo(() => sort(data.weekwise as unknown as Record<string, unknown>[]), [data.weekwise, sortKey, sortDir]);
  const exportWeek = () => {
    downloadCsv(
      `weekwise_${new Date().toISOString().slice(0, 10)}.csv`,
      ["Week", "Net sales"],
      (sorted as unknown as DashboardData["weekwise"]).map((r) => [r.week, r.net_sales])
    );
  };
  return (
    <Card title="Week-wise" badge={<ExportBtn onClick={exportWeek} />}>
      {data.weekwise.length === 0 ? (
        <EmptyState />
      ) : (
        <table className="w-full text-sm">
          <thead className="text-[11px] uppercase tracking-widest text-[hsl(var(--ink))]">
            <tr className="border-b hairline">
              <th className="px-2 py-2 text-left font-bold cursor-pointer" onClick={() => flip("week", "asc")}>
                Week<SortArrow active={sortKey === "week"} dir={sortDir} />
              </th>
              <th className="px-2 py-2 text-right font-bold cursor-pointer" onClick={() => flip("net_sales")}>
                Net sales<SortArrow active={sortKey === "net_sales"} dir={sortDir} />
              </th>
            </tr>
          </thead>
          <tbody className="font-mono tabular-nums">
            {(sorted as unknown as DashboardData["weekwise"]).map((r, i) => (
              <tr key={i} className="border-b border-[hsl(var(--hairline))]">
                <td className="px-2 py-1.5 font-sans">{r.week}</td>
                <td className="px-2 py-1.5 text-right">{formatINRCompact(r.net_sales)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

function CategoryTable({ rows: allRows }: { rows: CategoryRow[] }) {
  const { sortKey, sortDir, flip, sort } = useSort<Record<string, unknown>>("actual", "desc");
  // Drop any "— No Plan —" bucket the ported services emit.
  const rows = useMemo(
    () => allRows.filter((r) => !/no\s*plan/i.test(String(r.category))),
    [allRows]
  );
  const sorted = useMemo(() => sort(rows as unknown as Record<string, unknown>[]), [rows, sortKey, sortDir]);
  const exportCat = () => {
    downloadCsv(
      `category_target_vs_actual_${new Date().toISOString().slice(0, 10)}.csv`,
      ["Category", "Target", "Actual", "Achievement %"],
      (sorted as unknown as CategoryRow[]).map((r) => [r.category, r.target, r.actual, r.achievement ?? 0])
    );
  };
  return (
    <Card title="Target vs Actual — by Category" badge={<ExportBtn onClick={exportCat} />}>
      {rows.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="overflow-auto">
          <table className="w-full text-sm">
            <thead className="text-[11px] uppercase tracking-widest text-[hsl(var(--ink))]">
              <tr className="border-b hairline">
                <th className="px-3 py-2 text-left font-bold cursor-pointer" onClick={() => flip("category", "asc")}>
                  Category<SortArrow active={sortKey === "category"} dir={sortDir} />
                </th>
                <th className="px-3 py-2 text-right font-bold cursor-pointer" onClick={() => flip("target")}>
                  Target<SortArrow active={sortKey === "target"} dir={sortDir} />
                </th>
                <th className="px-3 py-2 text-right font-bold cursor-pointer" onClick={() => flip("actual")}>
                  Actual<SortArrow active={sortKey === "actual"} dir={sortDir} />
                </th>
                <th className="px-3 py-2 text-right font-bold cursor-pointer" onClick={() => flip("achievement")}>
                  Ach.<SortArrow active={sortKey === "achievement"} dir={sortDir} />
                </th>
              </tr>
            </thead>
            <tbody className="font-mono tabular-nums">
              {(sorted as unknown as CategoryRow[]).map((r, i) => (
                <tr key={i} className="border-b border-[hsl(var(--hairline))]">
                  <td className="px-3 py-1.5 font-sans">{r.category}</td>
                  <td className="px-3 py-1.5 text-right">{formatINRCompact(r.target)}</td>
                  <td className="px-3 py-1.5 text-right">{formatINRCompact(r.actual)}</td>
                  <td
                    className={cn(
                      "px-3 py-1.5 text-right font-semibold",
                      (r.achievement ?? 0) >= 100
                        ? "text-[hsl(var(--emerald))]"
                        : (r.achievement ?? 0) >= 70
                        ? "text-[hsl(var(--amber-deep))]"
                        : "text-[hsl(var(--coral))]"
                    )}
                  >
                    {formatPct(r.achievement)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

function CategoryDonutCard({ donut, catRows }: { donut: DonutBundle | null; catRows: CategoryRow[] }) {
  const [active, setActive] = useState<number | undefined>(undefined);
  const rows = useMemo(() => {
    const raw = (() => {
      if (donut) {
        if (Array.isArray(donut.data)) return donut.data as Array<{ label: string; value: number }>;
        if (donut.labels && donut.values)
          return donut.labels.map((l, i) => ({ label: l, value: donut.values![i] ?? 0 }));
      }
      // Fallback (Nexlev, whose services module has no dedicated donut): derive
      // from cat_rows' net-sales split.
      return catRows
        .filter((r) => (r.actual ?? 0) > 0)
        .map((r) => ({ label: r.category, value: Number(r.actual) || 0 }));
    })();
    // Never show a "No Plan" wedge.
    return raw.filter((r) => !/no\s*plan/i.test(r.label));
  }, [donut, catRows]);
  const total = useMemo(() => rows.reduce((s, r) => s + r.value, 0), [rows]);

  if (rows.length === 0) return null;

  return (
    <Card
      title="Category split"
      badge={<span className="rounded-full bg-[hsl(var(--hairline-soft))] px-2 py-0.5 font-mono text-[11px] font-semibold tabular-nums text-[hsl(var(--ink-2))]">{rows.length} categories</span>}
    >
      <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_240px]">
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={rows}
                dataKey="value"
                nameKey="label"
                cx="50%"
                cy="50%"
                outerRadius={120}
                innerRadius={72}
                paddingAngle={2}
                activeIndex={active}
                activeShape={(props: any) => (
                  <g>
                    <Sector {...props} outerRadius={props.outerRadius + 6} />
                    <text x={props.cx} y={props.cy - 6} textAnchor="middle" className="fill-[hsl(var(--ink-2))]" style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 1.5 }}>
                      {props.payload.label}
                    </text>
                    <text x={props.cx} y={props.cy + 14} textAnchor="middle" className="fill-[hsl(var(--ink))]" style={{ fontSize: 16, fontWeight: 600 }}>
                      {formatINRCompact(props.payload.value)}
                    </text>
                  </g>
                )}
                onMouseEnter={(_, i) => setActive(i)}
                onMouseLeave={() => setActive(undefined)}
              >
                {rows.map((_, i) => (
                  <Cell key={i} fill={DONUT_COLORS[i % DONUT_COLORS.length]} stroke="white" strokeWidth={2} />
                ))}
              </Pie>
              {active === undefined && (
                <>
                  <text x="50%" y="47%" textAnchor="middle" className="fill-[hsl(var(--ink-2))]" style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 1.5 }}>
                    Total
                  </text>
                  <text x="50%" y="55%" textAnchor="middle" className="fill-[hsl(var(--ink))]" style={{ fontSize: 18, fontWeight: 600 }}>
                    {formatINRCompact(total)}
                  </text>
                </>
              )}
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div className="flex flex-col gap-1.5 self-center text-xs">
          {rows
            .slice()
            .sort((a, b) => b.value - a.value)
            .map((r, i) => {
              const share = total ? (r.value / total) * 100 : 0;
              return (
                <div
                  key={r.label}
                  className="flex items-center justify-between gap-2 rounded-md px-2 py-1 hover:bg-[hsl(var(--hairline-soft))]"
                  onMouseEnter={() => setActive(rows.findIndex((x) => x.label === r.label))}
                  onMouseLeave={() => setActive(undefined)}
                >
                  <span className="flex min-w-0 items-center gap-1.5">
                    <span className="h-2.5 w-2.5 shrink-0 rounded-full" style={{ background: DONUT_COLORS[i % DONUT_COLORS.length] }} />
                    <span className="truncate">{r.label}</span>
                  </span>
                  <span className="shrink-0 font-mono tabular-nums text-[hsl(var(--ink-2))]">{formatPct(share, 1)}</span>
                </div>
              );
            })}
        </div>
      </div>
    </Card>
  );
}

type AsinSortKey = "asin" | "model_no" | "category" | "target" | "actual" | "units_ordered" | "achievement";
type SortDir = "asc" | "desc";

function useSort<T extends Record<string, unknown>>(
  initialKey: keyof T,
  initialDir: SortDir = "desc",
) {
  const [sortKey, setSortKey] = useState<keyof T>(initialKey);
  const [sortDir, setSortDir] = useState<SortDir>(initialDir);
  const flip = (k: keyof T, defaultDir: SortDir = "desc") => {
    if (sortKey === k) setSortDir(sortDir === "asc" ? "desc" : "asc");
    else {
      setSortKey(k);
      setSortDir(defaultDir);
    }
  };
  const sort = (rows: T[]): T[] => {
    const dir = sortDir === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
      return String(av ?? "").localeCompare(String(bv ?? "")) * dir;
    });
  };
  return { sortKey, sortDir, flip, sort };
}

function downloadCsv(filename: string, headers: string[], rows: (string | number)[][]) {
  // Every on-screen export routes through here, so this one call records the
  // lot. The report is identified by the filename stem (daywise, weekwise,
  // category_target_vs_actual, asin_target_vs_actual, model_trend).
  track("export", {
    page: filename.replace(/_\d{4}-\d{2}-\d{2}\.csv$/, "").replace(/\.csv$/, ""),
    brand: useBrand.getState().brand,
    detail: { filename, rows: rows.length },
  });
  const escape = (v: string | number) => {
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const csv = [headers.map(escape).join(","), ...rows.map((r) => r.map(escape).join(","))].join("\n");
  const blob = new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function ExportBtn({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      title="Download CSV"
      className="inline-flex h-7 items-center gap-1 rounded-md border hairline bg-[hsl(var(--paper))] px-2 text-[11px] font-semibold text-[hsl(var(--ink))] hover:bg-[hsl(var(--canvas))]"
    >
      <Download className="h-3 w-3" />
      Export CSV
    </button>
  );
}

function SortArrow({ active, dir }: { active: boolean; dir: SortDir }) {
  if (!active) return <ArrowUpDown className="ml-1 inline h-3 w-3 opacity-30" />;
  return dir === "asc" ? <ArrowUp className="ml-1 inline h-3 w-3" /> : <ArrowDown className="ml-1 inline h-3 w-3" />;
}

function AsinTable({ rows: allRows }: { rows: AsinRow[] }) {
  const [q, setQ] = useState("");
  const [sortKey, setSortKey] = useState<AsinSortKey>("actual");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [pageSize, setPageSize] = useState(50);
  // Never display out-of-plan ASIN sales.
  const rows = useMemo(() => allRows.filter((r) => !r.out_of_plan), [allRows]);

  const filtered = useMemo(() => {
    let out = rows;
    const s = q.trim().toLowerCase();
    if (s) {
      out = out.filter(
        (r) =>
          r.asin.toLowerCase().includes(s) ||
          (r.model_no || "").toLowerCase().includes(s) ||
          (r.category || "").toLowerCase().includes(s) ||
          (r.product_name || "").toLowerCase().includes(s)
      );
    }
    const dir = sortDir === "asc" ? 1 : -1;
    return [...out].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
      return String(av ?? "").localeCompare(String(bv ?? "")) * dir;
    });
  }, [rows, q, sortKey, sortDir]);

  const shown = filtered.slice(0, pageSize);

  const flip = (k: AsinSortKey) => {
    if (sortKey === k) setSortDir(sortDir === "asc" ? "desc" : "asc");
    else {
      setSortKey(k);
      setSortDir(k === "asin" || k === "model_no" || k === "category" ? "asc" : "desc");
    }
  };

  const SortIcon = ({ k }: { k: AsinSortKey }) => {
    if (sortKey !== k) return <ArrowUpDown className="ml-1 inline h-3 w-3 opacity-30" />;
    return sortDir === "asc" ? <ArrowUp className="ml-1 inline h-3 w-3" /> : <ArrowDown className="ml-1 inline h-3 w-3" />;
  };

  const exportAsin = () => {
    downloadCsv(
      `asin_target_vs_actual_${new Date().toISOString().slice(0, 10)}.csv`,
      ["ASIN", "Model", "Category", "Target", "Actual", "Units", "Achievement %"],
      filtered.map((r) => [r.asin, r.model_no || "", r.category || "", r.target, r.actual, r.units_ordered, r.achievement])
    );
  };
  return (
    <Card
      title="Target vs Actual — by ASIN"
      collapsible
      defaultOpen={false}
      badge={
        <div className="flex items-center gap-2">
          <ExportBtn onClick={exportAsin} />
          <span className="pill pill-muted">
            {filtered.length.toLocaleString("en-IN")} of {rows.length.toLocaleString("en-IN")}
          </span>
        </div>
      }
    >
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <div className="relative flex-1 min-w-[200px]">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-[hsl(var(--ink-2))]" />
          <input
            className="h-9 w-full rounded-md border hairline pl-8 pr-2 text-[13px] font-semibold"
            placeholder="Search ASIN, model, category…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
        <select
          value={pageSize}
          onChange={(e) => setPageSize(Number(e.target.value))}
          className="h-9 rounded-md border hairline px-2 text-[12px] font-semibold"
        >
          <option value={25}>Show 25</option>
          <option value={50}>Show 50</option>
          <option value={100}>Show 100</option>
          <option value={5000}>Show all</option>
        </select>
      </div>
      {shown.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="overflow-auto">
          <table className="w-full text-sm">
            <thead className="text-[11px] uppercase tracking-widest text-[hsl(var(--ink-2))]">
              <tr className="border-b hairline">
                <th className="px-3 py-2 text-left font-bold cursor-pointer hover:text-[hsl(var(--ink))]" onClick={() => flip("asin")}>ASIN<SortIcon k="asin" /></th>
                <th className="px-3 py-2 text-left font-bold cursor-pointer hover:text-[hsl(var(--ink))]" onClick={() => flip("model_no")}>Model<SortIcon k="model_no" /></th>
                <th className="px-3 py-2 text-left font-bold cursor-pointer hover:text-[hsl(var(--ink))]" onClick={() => flip("category")}>Category<SortIcon k="category" /></th>
                <th className="px-3 py-2 text-right font-bold cursor-pointer hover:text-[hsl(var(--ink))]" onClick={() => flip("target")}>Target<SortIcon k="target" /></th>
                <th className="px-3 py-2 text-right font-bold cursor-pointer hover:text-[hsl(var(--ink))]" onClick={() => flip("actual")}>Actual<SortIcon k="actual" /></th>
                <th className="px-3 py-2 text-right font-bold cursor-pointer hover:text-[hsl(var(--ink))]" onClick={() => flip("units_ordered")}>Units<SortIcon k="units_ordered" /></th>
                <th className="px-3 py-2 text-right font-bold cursor-pointer hover:text-[hsl(var(--ink))]" onClick={() => flip("achievement")}>Ach.<SortIcon k="achievement" /></th>
                <th className="px-3 py-2 text-left font-bold">Trend</th>
              </tr>
            </thead>
            <tbody className="font-mono tabular-nums">
              {shown.map((r, i) => (
                <tr key={i} className="border-b border-[hsl(var(--hairline))]">
                  <td className="px-3 py-1.5">{r.asin}</td>
                  <td className="px-3 py-1.5 font-sans">{r.model_no || "—"}</td>
                  <td className="px-3 py-1.5 font-sans">{r.category}</td>
                  <td className="px-3 py-1.5 text-right">{formatINRCompact(r.target)}</td>
                  <td className="px-3 py-1.5 text-right">{formatINRCompact(r.actual)}</td>
                  <td className="px-3 py-1.5 text-right">{formatNumber(r.units_ordered)}</td>
                  <td
                    className={cn(
                      "px-3 py-1.5 text-right font-semibold",
                      r.achievement >= 100
                        ? "text-[hsl(var(--emerald))]"
                        : r.achievement >= 70
                        ? "text-[hsl(var(--amber-deep))]"
                        : r.achievement > 0
                        ? "text-[hsl(var(--coral))]"
                        : "text-[hsl(var(--ink-2))]"
                    )}
                  >
                    {formatPct(r.achievement)}
                  </td>
                  <td className="px-3 py-1.5" dangerouslySetInnerHTML={{ __html: r.sparkline || "" }} />
                </tr>
              ))}
            </tbody>
          </table>
          {filtered.length > shown.length && (
            <div className="mt-3 text-center text-[12px] text-[hsl(var(--ink-2))]">
              Showing {shown.length} of {filtered.length}. Increase page size to see more.
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

function ModelTrendCard({ bundle }: { bundle: ModelTrendBundle }) {
  const [q, setQ] = useState("");
  const filtered = useMemo(() => {
    const s = q.trim().toLowerCase();
    if (!s) return bundle.rows;
    return bundle.rows.filter(
      (r) => r.model.toLowerCase().includes(s) || (r.category || "").toLowerCase().includes(s)
    );
  }, [q, bundle.rows]);
  const [m1, m2, m3] = bundle.months;
  const [d1, d2, d3] = bundle.days ?? [];
  const exportTrend = () => {
    downloadCsv(
      `model_trend_${new Date().toISOString().slice(0, 10)}.csv`,
      ["Model", "Category", m1 ?? "M-2", m2 ?? "M-1", m3 ?? "Current", "Trend %", "Units (current)"],
      filtered.map((r) => [r.model, r.category || "", r.s1, r.s2, r.s3, r.sales_trend_pct, r.u3])
    );
  };
  const label = (m: string | undefined, d: number | undefined) =>
    m ? (d !== undefined ? `${m} · ${d}d` : m) : "—";
  return (
    <Card
      title="3-Month Model Trend"
      collapsible
      defaultOpen={false}
      badge={
        <div className="flex items-center gap-2">
          <ExportBtn onClick={exportTrend} />
          <span className="pill pill-muted">{filtered.length.toLocaleString("en-IN")} models</span>
        </div>
      }
    >
      <div className="mb-3 flex items-center gap-2">
        <div className="relative flex-1 min-w-[200px]">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-[hsl(var(--ink))]" />
          <input
            className="h-9 w-full rounded-md border hairline pl-8 pr-2 text-[13px] font-semibold"
            placeholder="Search model / category…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
      </div>
      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead className="text-[11px] uppercase tracking-widest text-[hsl(var(--ink))]">
            <tr className="border-b hairline">
              <th className="px-3 py-2 text-left font-bold">Model</th>
              <th className="px-3 py-2 text-left font-bold">Category</th>
              <th className="px-3 py-2 text-right font-bold">{label(m1, d1)}</th>
              <th className="px-3 py-2 text-right font-bold">{label(m2, d2)}</th>
              <th className="px-3 py-2 text-right font-bold">{label(m3, d3)}</th>
              <th className="px-3 py-2 text-right font-bold">Trend</th>
              <th className="px-3 py-2 text-right font-bold">Units</th>
            </tr>
          </thead>
          <tbody className="font-mono tabular-nums">
            {filtered.map((r, i) => (
              <tr key={i} className="border-b border-[hsl(var(--hairline))]">
                <td className="px-3 py-1.5 font-sans font-semibold">{r.model}</td>
                <td className="px-3 py-1.5 font-sans">{r.category}</td>
                <td className="px-3 py-1.5 text-right">{formatINRCompact(r.s1)}</td>
                <td className="px-3 py-1.5 text-right">{formatINRCompact(r.s2)}</td>
                <td className="px-3 py-1.5 text-right font-semibold">{formatINRCompact(r.s3)}</td>
                <td
                  className={cn(
                    "px-3 py-1.5 text-right font-semibold",
                    r.trend_class === "good"
                      ? "text-[hsl(var(--emerald))]"
                      : r.trend_class === "bad"
                      ? "text-[hsl(var(--coral))]"
                      : "text-[hsl(var(--ink))]"
                  )}
                >
                  {r.sales_trend_pct > 0 ? "▲" : r.sales_trend_pct < 0 ? "▼" : "•"} {formatPct(Math.abs(r.sales_trend_pct))}
                </td>
                <td className="px-3 py-1.5 text-right">{formatNumber(r.u3)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function MonthwiseCard({ chart }: { chart: DashboardData["monthwise_chart"] }) {
  const rows = useMemo(
    () =>
      chart.labels.map((l, i) => {
        const row: Record<string, string | number> = { label: l };
        chart.asins.forEach((a, j) => {
          row[a] = chart.data[j]?.[i] ?? 0;
        });
        return row;
      }),
    [chart]
  );
  const top = chart.asins.slice(0, 8);
  return (
    <Card title="Monthwise — by ASIN (top 8)" collapsible defaultOpen={false}>
      <div className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={rows} margin={{ top: 8, right: 16, left: 8, bottom: 4 }}>
            <defs>
              {top.map((a, i) => (
                <linearGradient key={a} id={`mw-${a}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={DONUT_COLORS[i % DONUT_COLORS.length]} stopOpacity={0.3} />
                  <stop offset="100%" stopColor={DONUT_COLORS[i % DONUT_COLORS.length]} stopOpacity={0} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="2 4" stroke="hsl(var(--hairline))" vertical={false} />
            <XAxis dataKey="label" tick={{ fontSize: 11, fill: "hsl(var(--ink-2))" }} tickLine={false} axisLine={false} />
            <YAxis tick={{ fontSize: 11, fill: "hsl(var(--ink-2))" }} tickLine={false} axisLine={false} tickFormatter={(v) => formatINRCompact(v)} />
            <Tooltip content={<SmartTooltip />} cursor={{ stroke: "hsl(var(--hairline))", strokeDasharray: "3 3" }} />
            <Legend wrapperStyle={{ fontSize: 11 }} iconType="circle" />
            {top.map((a, i) => (
              <Area
                key={a}
                type="monotone"
                dataKey={a}
                stroke={DONUT_COLORS[i % DONUT_COLORS.length]}
                strokeWidth={1.75}
                fill={`url(#mw-${a})`}
                dot={false}
                isAnimationActive
                animationDuration={600}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

// ── Reusable primitives ──
function Card({
  title,
  children,
  badge,
  collapsible,
  defaultOpen = true,
}: {
  title: string;
  children: React.ReactNode;
  badge?: React.ReactNode;
  collapsible?: boolean;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <section className="surface-hi rounded-xl p-5">
      <div
        className={cn(
          "flex items-center justify-between",
          collapsible && "cursor-pointer select-none",
          open ? "mb-3" : ""
        )}
        onClick={collapsible ? () => setOpen((v) => !v) : undefined}
      >
        <h2 className="flex items-center gap-2 text-[11px] font-bold uppercase tracking-widest text-[hsl(var(--ink-2))]">
          {collapsible && (
            <span className="text-[hsl(var(--ink-2))]">
              {open ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            </span>
          )}
          {title}
        </h2>
        {badge}
      </div>
      {(!collapsible || open) && children}
    </section>
  );
}

function EmptyState() {
  return <div className="grid h-32 place-items-center text-sm text-muted-foreground">No data yet</div>;
}

