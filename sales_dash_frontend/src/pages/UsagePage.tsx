/**
 * Usage — who is actually using the tool.
 *
 * Reads the activity log written by `backend/app/activity.py` (see
 * ACTIVITY_TRACKING.md). Admin only: the nav entry is hidden for viewers and
 * the page refuses to render for them, on top of the server-side
 * `require_admin` on every endpoint it calls.
 *
 * The headline cards are always lifetime figures — they answer "how big is
 * this tool's audience", which a window would only obscure. The timeline and
 * the per-user table follow the period switch.
 */
import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Activity, Users as UsersIcon, Zap, UserCheck, Download, ArrowUpDown, RefreshCw } from "lucide-react";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis } from "recharts";
import { api } from "@/lib/api";
import { useAuth } from "@/lib/auth";
import { track } from "@/lib/activity";
import { cn } from "@/lib/utils";

interface PerUser {
  user_email: string;
  logins: number;
  logouts: number;
  page_views: number;
  exports: number;
  data_fetches: number;
  syncs: number;
  active_days: number;
  first_seen: string | null;
  last_seen: string | null;
  sessions: number;
  minutes_total: number;
  minutes_avg_session: number;
}

interface PerDay {
  day: string;
  users: number;
  logins: number;
  page_views: number;
  exports: number;
}

interface UsageRes {
  since: string | null;
  until: string | null;
  per_user: PerUser[];
  per_day: PerDay[];
  exports: { user_email: string; page: string | null; brand: string | null; n: number; last_time: string | null }[];
}

interface AccountRow {
  email: string;
  full_name: string | null;
  role: "admin" | "viewer";
  is_active: boolean;
}

interface EventRow {
  id: number;
  user_email: string;
  session_id: string | null;
  event: string;
  page: string | null;
  brand: string | null;
  detail: Record<string, unknown> | null;
  occurred_at: string | null;
}

/** How each event type reads in the feed, and the pill it wears. */
const EVENT_STYLE: Record<string, { label: string; pill: string }> = {
  login: { label: "Signed in", pill: "pill-emerald-soft" },
  logout: { label: "Signed out", pill: "pill-muted" },
  page_view: { label: "Opened page", pill: "pill-navy-soft" },
  export: { label: "Exported", pill: "pill-gold-soft" },
  data_fetch: { label: "Loaded data", pill: "pill-muted" },
  sync: { label: "Ran sync", pill: "pill-plum-soft" },
  heartbeat: { label: "Still open", pill: "pill-muted" },
};

type Period = "7" | "30" | "all";

const PERIODS: { key: Period; label: string }[] = [
  { key: "7", label: "7 days" },
  { key: "30", label: "30 days" },
  { key: "all", label: "All time" },
];

/** Total *deliberate* actions — heartbeats are excluded, they are not a use. */
function eventTotal(u: PerUser): number {
  return u.logins + u.logouts + u.page_views + u.exports + u.data_fetches + u.syncs;
}

function isoDaysAgo(days: number): string {
  return new Date(Date.now() - days * 86_400_000).toISOString().slice(0, 10);
}

function isoHoursAgo(hours: number): string {
  return new Date(Date.now() - hours * 3_600_000).toISOString();
}

function formatMinutes(m: number | null | undefined): string {
  if (!m || m <= 0) return "—";
  if (m < 60) return `${Math.round(m)}m`;
  const h = Math.floor(m / 60);
  const rem = Math.round(m % 60);
  return `${h}h ${String(rem).padStart(2, "0")}m`;
}

function formatWhen(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  const day = d.toLocaleDateString("en-GB", { day: "2-digit", month: "short" });
  const time = d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: true }).toLowerCase();
  return `${day}, ${time}`;
}

function useUsage(since: string | null) {
  return useQuery<UsageRes>({
    queryKey: ["admin-usage", since ?? "all"],
    queryFn: () => api.get<UsageRes>("/api/admin/usage", since ? { since } : undefined),
    staleTime: 60_000,
  });
}

export function UsagePage() {
  const { me } = useAuth();
  const [period, setPeriod] = useState<Period>("30");
  const [sortKey, setSortKey] = useState<keyof PerUser | "total">("total");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const lifetime = useUsage(null);
  const week = useUsage(isoDaysAgo(7));
  const day = useUsage(isoHoursAgo(24));
  const month = useUsage(isoDaysAgo(30));

  const { data: accounts } = useQuery<{ users: AccountRow[] }>({
    queryKey: ["admin-users"],
    queryFn: () => api.get("/api/admin/users"),
    staleTime: 5 * 60_000,
  });

  const [feedUser, setFeedUser] = useState("");
  const [feedEvent, setFeedEvent] = useState("");
  const [showBeats, setShowBeats] = useState(false);

  const feed = useQuery<{ events: EventRow[] }>({
    queryKey: ["admin-usage-events"],
    queryFn: () => api.get("/api/admin/usage/events", { limit: 500 }),
    staleTime: 30_000,
  });

  const nameFor = useMemo(() => {
    const m = new Map<string, string>();
    for (const a of accounts?.users ?? []) {
      if (a.full_name) m.set(a.email.toLowerCase(), a.full_name);
    }
    return m;
  }, [accounts]);

  const active = period === "7" ? week : period === "30" ? month : lifetime;

  // ── headline figures (always lifetime, like the audience they describe) ──
  const totalEvents = (lifetime.data?.per_user ?? []).reduce((s, u) => s + eventTotal(u), 0);
  const eventsWeek = (week.data?.per_user ?? []).reduce((s, u) => s + eventTotal(u), 0);
  const events24h = (day.data?.per_user ?? []).reduce((s, u) => s + eventTotal(u), 0);
  const active7 = week.data?.per_user.length ?? 0;
  const active30 = month.data?.per_user.length ?? 0;
  const uniqueEver = lifetime.data?.per_user.length ?? 0;
  const neverUsed = Math.max(
    0,
    (accounts?.users?.length ?? 0) -
      (accounts?.users ?? []).filter((a) =>
        lifetime.data?.per_user.some((u) => u.user_email === a.email.toLowerCase())
      ).length
  );

  // ── 30-day timeline: every calendar day, so gaps read as gaps ────────────
  const timeline = useMemo(() => {
    const byDay = new Map<string, PerDay>();
    for (const r of lifetime.data?.per_day ?? []) byDay.set(String(r.day).slice(0, 10), r);
    const out: { key: string; label: string; events: number; users: number }[] = [];
    for (let i = 29; i >= 0; i--) {
      const key = isoDaysAgo(i);
      const r = byDay.get(key);
      out.push({
        key,
        label: key.slice(8, 10) + "/" + key.slice(5, 7),
        events: r ? r.logins + r.page_views + r.exports : 0,
        users: r?.users ?? 0,
      });
    }
    return out;
  }, [lifetime.data]);

  const daysWithActivity = timeline.filter((d) => d.events > 0).length;

  // ── per-user table ───────────────────────────────────────────────────────
  const rows = useMemo(() => {
    const list = (active.data?.per_user ?? []).map((u) => ({ ...u, total: eventTotal(u) }));
    const dir = sortDir === "asc" ? 1 : -1;
    return list.sort((a, b) => {
      const av = a[sortKey as keyof typeof a];
      const bv = b[sortKey as keyof typeof b];
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
      return String(av ?? "").localeCompare(String(bv ?? "")) * dir;
    });
  }, [active.data, sortKey, sortDir]);

  // ── raw event feed ───────────────────────────────────────────────────────
  // Heartbeats are hidden by default: they are the majority of rows and say
  // nothing except "the tab was open".
  const feedUsers = useMemo(
    () => Array.from(new Set((feed.data?.events ?? []).map((e) => e.user_email))).sort(),
    [feed.data]
  );

  const feedRows = useMemo(() => {
    let list = feed.data?.events ?? [];
    if (!showBeats) list = list.filter((e) => e.event !== "heartbeat");
    if (feedUser) list = list.filter((e) => e.user_email === feedUser);
    if (feedEvent) list = list.filter((e) => e.event === feedEvent);
    return list;
  }, [feed.data, feedUser, feedEvent, showBeats]);

  function flip(key: keyof PerUser | "total") {
    if (key === sortKey) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else {
      setSortKey(key);
      setSortDir(key === "user_email" ? "asc" : "desc");
    }
  }

  function exportCsv() {
    const label = PERIODS.find((p) => p.key === period)?.label ?? period;
    track("export", { page: "usage", detail: { period: label, rows: rows.length } });
    const headers = [
      "User",
      "Email",
      "Logins",
      "Page views",
      "Syncs",
      "Exports",
      "Data requests",
      "Sessions",
      "Minutes in tool",
      "Active days",
      "Total events",
      "Last activity",
    ];
    const escape = (v: string | number) => {
      const s = String(v ?? "");
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const body = rows.map((r) => [
      nameFor.get(r.user_email) ?? "",
      r.user_email,
      r.logins,
      r.page_views,
      r.syncs,
      r.exports,
      r.data_fetches,
      r.sessions,
      r.minutes_total,
      r.active_days,
      r.total,
      r.last_seen ?? "",
    ]);
    const csv = [headers.map(escape).join(","), ...body.map((r) => r.map(escape).join(","))].join("\n");
    const url = URL.createObjectURL(new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `usage_${period === "all" ? "all-time" : period + "d"}_${new Date()
      .toISOString()
      .slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  if (me?.user?.role !== "admin") {
    return (
      <div className="mx-auto max-w-3xl px-8 pt-6">
        <div className="rounded-md border border-[hsl(var(--coral))] bg-[hsl(var(--coral-soft))] p-4 text-[hsl(var(--coral))]">
          Admin access required.
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-[1440px] space-y-6 px-8 pb-10 pt-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h1 className="text-[32px] leading-none">Usage</h1>
          <div className="mt-2 flex items-center gap-2 text-[13px] font-semibold">
            <span className="pill pill-navy-soft">{uniqueEver} users have used the tool</span>
            {neverUsed > 0 && <span className="pill pill-muted">{neverUsed} never signed in</span>}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex overflow-hidden rounded-md border hairline">
            {PERIODS.map((p) => (
              <button
                key={p.key}
                onClick={() => setPeriod(p.key)}
                className={cn(
                  "h-8 px-3 text-[12px] font-semibold transition-colors",
                  period === p.key
                    ? "bg-primary text-white"
                    : "bg-[hsl(var(--paper))] text-[hsl(var(--ink-2))] hover:bg-[hsl(var(--canvas))]"
                )}
              >
                {p.label}
              </button>
            ))}
          </div>
          <button
            onClick={exportCsv}
            className="flex h-8 items-center gap-1.5 rounded-md border hairline px-3 text-[12px] font-semibold text-[hsl(var(--ink-2))] hover:text-[hsl(var(--ink))]"
          >
            <Download className="h-3.5 w-3.5" />
            Export
          </button>
        </div>
      </div>

      {/* Headline band */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi
          label="Total events"
          value={totalEvents.toLocaleString("en-IN")}
          sub="Lifetime"
          icon={Activity}
          tint="bg-[hsl(var(--primary-soft))] text-[hsl(var(--primary))]"
        />
        <Kpi
          label="Active (7 days)"
          value={active7}
          sub={`${active30} in last 30 days`}
          icon={UserCheck}
          tint="bg-[hsl(var(--emerald-soft))] text-[hsl(var(--emerald))]"
        />
        <Kpi
          label="Events (24h)"
          value={events24h.toLocaleString("en-IN")}
          sub={`${eventsWeek.toLocaleString("en-IN")} in last 7 days`}
          icon={Zap}
          tint="bg-[hsl(var(--gold-soft))] text-[hsl(var(--gold))]"
        />
        <Kpi
          label="Unique users ever"
          value={uniqueEver}
          sub="across all logins"
          icon={UsersIcon}
          tint="bg-[hsl(var(--plum-soft))] text-[hsl(var(--plum))]"
        />
      </div>

      {/* Timeline */}
      <div className="surface-hi rounded-xl p-5">
        <div className="flex items-start justify-between">
          <div>
            <div className="text-[10px] font-bold uppercase tracking-widest text-[hsl(var(--ink-2))]">
              Last 30 days
            </div>
            <div className="mt-1 text-[15px] font-bold">Activity timeline</div>
          </div>
          <div className="text-[12px] font-semibold text-[hsl(var(--ink-2))]">
            {daysWithActivity} days with activity
          </div>
        </div>
        <div className="mt-4 h-[180px]">
          {lifetime.isLoading ? (
            <div className="grid h-full place-items-center text-[12px] text-[hsl(var(--ink-2))]">Loading…</div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={timeline} margin={{ top: 4, right: 0, bottom: 0, left: 0 }}>
                <XAxis
                  dataKey="label"
                  tick={{ fontSize: 10, fill: "hsl(var(--ink-2))" }}
                  axisLine={false}
                  tickLine={false}
                  interval={3}
                />
                <Tooltip
                  cursor={{ fill: "hsl(var(--hairline-soft))" }}
                  contentStyle={{
                    borderRadius: 8,
                    border: "1px solid hsl(var(--hairline))",
                    fontSize: 12,
                    fontWeight: 600,
                  }}
                  formatter={(v: number) => [v, "Events"]}
                  labelFormatter={(l: string) => `Day ${l}`}
                />
                <Bar dataKey="events" fill="hsl(var(--primary))" radius={[3, 3, 0, 0]} maxBarSize={22} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Per user */}
      <div className="surface-hi overflow-hidden rounded-xl">
        <div className="border-b hairline px-5 py-4">
          <div className="text-[10px] font-bold uppercase tracking-widest text-[hsl(var(--ink-2))]">Per user</div>
          <div className="mt-1 text-[15px] font-bold">
            {rows.length} {rows.length === 1 ? "user" : "users"} in this period
          </div>
        </div>
        {active.isLoading ? (
          <div className="px-5 py-10 text-center text-[13px] text-[hsl(var(--ink-2))]">Loading…</div>
        ) : rows.length === 0 ? (
          <div className="px-5 py-10 text-center text-[13px] text-[hsl(var(--ink-2))]">
            No recorded activity in this period.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-[10px] uppercase tracking-widest text-[hsl(var(--ink-2))]">
                <tr className="border-b hairline">
                  <Th onClick={() => flip("user_email")} active={sortKey === "user_email"} dir={sortDir} align="left">
                    User
                  </Th>
                  <Th onClick={() => flip("logins")} active={sortKey === "logins"} dir={sortDir}>
                    Logins
                  </Th>
                  <Th onClick={() => flip("page_views")} active={sortKey === "page_views"} dir={sortDir}>
                    Page views
                  </Th>
                  <Th onClick={() => flip("syncs")} active={sortKey === "syncs"} dir={sortDir}>
                    Syncs
                  </Th>
                  <Th onClick={() => flip("exports")} active={sortKey === "exports"} dir={sortDir}>
                    Exports
                  </Th>
                  <Th onClick={() => flip("minutes_total")} active={sortKey === "minutes_total"} dir={sortDir}>
                    Time in tool
                  </Th>
                  <Th onClick={() => flip("total")} active={sortKey === "total"} dir={sortDir}>
                    Total
                  </Th>
                  <Th onClick={() => flip("last_seen")} active={sortKey === "last_seen"} dir={sortDir}>
                    Last activity
                  </Th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const name = nameFor.get(r.user_email);
                  return (
                    <tr
                      key={r.user_email}
                      className="border-b hairline last:border-0 hover:bg-[hsl(var(--hairline-soft))]"
                    >
                      <td className="px-5 py-3">
                        <div className="flex items-center gap-2.5">
                          <div className="grid h-7 w-7 shrink-0 place-items-center rounded-full bg-[hsl(var(--primary-soft))] text-[10px] font-bold uppercase text-[hsl(var(--primary))]">
                            {(name ?? r.user_email).slice(0, 2)}
                          </div>
                          <div className="min-w-0">
                            <div className="truncate text-[13px] font-bold">{name ?? r.user_email}</div>
                            {name && (
                              <div className="truncate text-[11px] font-semibold text-[hsl(var(--ink-2))]">
                                {r.user_email}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>
                      <Td>{r.logins}</Td>
                      <Td>{r.page_views}</Td>
                      <Td muted={r.syncs === 0}>{r.syncs}</Td>
                      <Td muted={r.exports === 0}>{r.exports}</Td>
                      <Td>{formatMinutes(r.minutes_total)}</Td>
                      <Td bold>{r.total}</Td>
                      <td className="whitespace-nowrap px-4 py-3 text-right text-[12px] font-semibold text-[hsl(var(--ink-2))]">
                        {formatWhen(r.last_seen)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Raw event feed — the evidence behind every figure above */}
      <div className="surface-hi overflow-hidden rounded-xl">
        <div className="flex flex-wrap items-end justify-between gap-3 border-b hairline px-5 py-4">
          <div>
            <div className="text-[10px] font-bold uppercase tracking-widest text-[hsl(var(--ink-2))]">
              Latest 500 events
            </div>
            <div className="mt-1 text-[15px] font-bold">
              Recent activity
              <span className="ml-2 text-[12px] font-semibold text-[hsl(var(--ink-2))]">
                {feedRows.length} shown
              </span>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <select
              value={feedUser}
              onChange={(e) => setFeedUser(e.target.value)}
              className="h-8 rounded-md border hairline bg-[hsl(var(--paper))] px-2 text-[12px] font-semibold text-[hsl(var(--ink))]"
            >
              <option value="">All users</option>
              {feedUsers.map((u) => (
                <option key={u} value={u}>
                  {nameFor.get(u) ?? u}
                </option>
              ))}
            </select>
            <select
              value={feedEvent}
              onChange={(e) => setFeedEvent(e.target.value)}
              className="h-8 rounded-md border hairline bg-[hsl(var(--paper))] px-2 text-[12px] font-semibold text-[hsl(var(--ink))]"
            >
              <option value="">All events</option>
              {Object.entries(EVENT_STYLE).map(([k, v]) => (
                <option key={k} value={k}>
                  {v.label}
                </option>
              ))}
            </select>
            <label className="flex h-8 cursor-pointer items-center gap-1.5 rounded-md border hairline px-2.5 text-[12px] font-semibold text-[hsl(var(--ink-2))]">
              <input
                type="checkbox"
                checked={showBeats}
                onChange={(e) => setShowBeats(e.target.checked)}
                className="h-3 w-3"
              />
              Heartbeats
            </label>
            <button
              onClick={() => feed.refetch()}
              className="flex h-8 items-center gap-1.5 rounded-md border hairline px-3 text-[12px] font-semibold text-[hsl(var(--ink-2))] hover:text-[hsl(var(--ink))]"
            >
              <RefreshCw className={cn("h-3.5 w-3.5", feed.isFetching && "animate-spin")} />
              Refresh
            </button>
          </div>
        </div>
        {feed.isLoading ? (
          <div className="px-5 py-10 text-center text-[13px] text-[hsl(var(--ink-2))]">Loading…</div>
        ) : feedRows.length === 0 ? (
          <div className="px-5 py-10 text-center text-[13px] text-[hsl(var(--ink-2))]">
            Nothing recorded for this filter.
          </div>
        ) : (
          <div className="max-h-[420px] overflow-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 z-10 bg-[hsl(var(--paper))] text-[10px] uppercase tracking-widest text-[hsl(var(--ink-2))]">
                <tr className="border-b hairline">
                  <th className="px-5 py-3 text-left font-bold">When</th>
                  <th className="px-4 py-3 text-left font-bold">User</th>
                  <th className="px-4 py-3 text-left font-bold">Event</th>
                  <th className="px-4 py-3 text-left font-bold">Page</th>
                  <th className="px-4 py-3 text-left font-bold">Brand</th>
                </tr>
              </thead>
              <tbody>
                {feedRows.map((e) => {
                  const style = EVENT_STYLE[e.event] ?? { label: e.event, pill: "pill-muted" };
                  return (
                    <tr key={e.id} className="border-b hairline last:border-0 hover:bg-[hsl(var(--hairline-soft))]">
                      <td className="whitespace-nowrap px-5 py-2.5 font-mono text-[12px] tabular-nums text-[hsl(var(--ink-2))]">
                        {formatWhen(e.occurred_at)}
                      </td>
                      <td className="whitespace-nowrap px-4 py-2.5 text-[12px] font-bold">
                        {nameFor.get(e.user_email) ?? e.user_email}
                      </td>
                      <td className="whitespace-nowrap px-4 py-2.5">
                        <span className={cn("pill", style.pill)}>{style.label}</span>
                      </td>
                      <td className="max-w-[280px] truncate px-4 py-2.5 font-mono text-[12px] text-[hsl(var(--ink-2))]">
                        {e.page ?? "—"}
                      </td>
                      <td className="whitespace-nowrap px-4 py-2.5 text-[12px] font-semibold text-[hsl(var(--ink-2))]">
                        {e.brand ?? "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <p className="text-[11px] font-semibold text-[hsl(var(--ink-2))]">
        Recording started 25 Aug 2026, 1:13 pm — use before that moment was never written down anywhere and
        cannot be recovered. Time in tool counts only minutes the tab was actually visible.
      </p>
    </div>
  );
}

function Kpi({
  label,
  value,
  sub,
  icon: Icon,
  tint,
}: {
  label: string;
  value: React.ReactNode;
  sub?: string;
  icon: React.ComponentType<{ className?: string }>;
  tint: string;
}) {
  return (
    <div className="surface-hi rounded-xl p-5">
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
    </div>
  );
}

function Th({
  children,
  onClick,
  active,
  dir,
  align = "right",
}: {
  children: React.ReactNode;
  onClick: () => void;
  active: boolean;
  dir: "asc" | "desc";
  align?: "left" | "right";
}) {
  return (
    <th
      onClick={onClick}
      className={cn(
        "cursor-pointer select-none whitespace-nowrap py-3 font-bold",
        align === "left" ? "px-5 text-left" : "px-4 text-right"
      )}
    >
      <span className={cn("inline-flex items-center gap-1", active && "text-[hsl(var(--ink))]")}>
        {children}
        <ArrowUpDown className={cn("h-3 w-3", active ? "opacity-100" : "opacity-25")} />
        {active && <span className="sr-only">{dir}</span>}
      </span>
    </th>
  );
}

function Td({ children, bold, muted }: { children: React.ReactNode; bold?: boolean; muted?: boolean }) {
  return (
    <td
      className={cn(
        "whitespace-nowrap px-4 py-3 text-right font-mono text-[13px] tabular-nums",
        bold && "font-bold",
        muted && "text-[hsl(var(--ink-2))] opacity-50"
      )}
    >
      {children}
    </td>
  );
}
