import { type ReactNode } from "react";
import { Link, NavLink, useNavigate, useLocation } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/lib/auth";
import { api } from "@/lib/api";
import { useSyncStatus, formatRelative } from "@/lib/useSyncStatus";
import { Button } from "@/components/ui/button";
import {
    LayoutDashboard, TrendingUp, ShoppingCart, Tag, Package, Megaphone,
    AlertCircle, AlertTriangle, Archive, Percent, ClipboardList, RotateCcw,
    LogOut, ArrowLeft, ChevronRight,
    Target, Truck, Calculator, Award, ExternalLink,
} from "lucide-react";

// ── Hover prefetch ──────────────────────────────────────────────────────
// When the operator hovers a sidebar item, we fire two parallel prefetches:
//   1. The dynamic import() for that route's chunk (so the JS arrives
//      before they click — same import React.lazy() uses, so the result
//      goes straight into Vite's module cache).
//   2. The /api/* queries that page mounts on load (so React Query's
//      cache is warm by the time the page renders).
// Both are idempotent / deduplicated, so hovering rapidly across 9 items
// is safe — no thundering herd.
const CHUNK_LOADERS: Record<string, () => Promise<unknown>> = {
    "/dashboard":            () => import("@/pages/Dashboard"),
    "/sales-trend":          () => import("@/pages/SalesTrend"),
    "/amazon-sales-trend":   () => import("@/pages/AmazonSalesTrend"),
    "/category-sales":       () => import("@/pages/CategorySales"),
    "/inventory-dashboard":  () => import("@/pages/InventoryDashboard"),
    "/ams-trend":            () => import("@/pages/AmsTrend"),
    "/ams-poor-performers":  () => import("@/pages/AmsPoorPerformers"),
    "/ams-planning":         () => import("@/pages/AmsPlanning"),
    "/no-sales-last-week":   () => import("@/pages/NoSalesLastWeek"),
    "/dead-stock":           () => import("@/pages/DeadStock"),
    "/margin-snapshot":      () => import("@/pages/MarginSnapshot"),
    "/returns":              () => import("@/pages/Returns"),
};

type PrefetchSpec = Array<{ key: readonly unknown[]; fn: () => Promise<unknown> }>;
const PREFETCH_QUERIES: Record<string, PrefetchSpec> = {
    "/dashboard":           [{ key: ["dashboard", ""],              fn: () => api.get("/api/dashboard") }],
    "/sales-trend":         [{ key: ["sales-trend", ""],            fn: () => api.get("/api/sales-trend") }],
    "/amazon-sales-trend":  [{ key: ["amazon-sales-trend", ""],     fn: () => api.get("/api/amazon-sales-trend") }],
    "/category-sales":      [{ key: ["category-sales", "level=l0"], fn: () => api.get("/api/category-sales?level=l0") }],
    "/inventory-dashboard": [{ key: ["inventory-dashboard", ""],    fn: () => api.get("/api/inventory-dashboard") }],
    "/ams-trend":           [{ key: ["ams-trend"],                  fn: () => api.get("/api/ams/trend") }],
    "/ams-poor-performers": [{ key: ["ams-trend"],                  fn: () => api.get("/api/ams/trend") }],
    "/ams-planning":        [{ key: ["ams-planning"],               fn: () => api.get("/api/ams-planning") }],
    "/no-sales-last-week":  [
        { key: ["sales-trend", ""],         fn: () => api.get("/api/sales-trend") },
        { key: ["inventory-dashboard", ""], fn: () => api.get("/api/inventory-dashboard") },
    ],
    "/dead-stock":          [
        { key: ["sales-trend", ""],         fn: () => api.get("/api/sales-trend") },
        { key: ["inventory-dashboard", ""], fn: () => api.get("/api/inventory-dashboard") },
    ],
    "/margin-snapshot":     [{ key: ["margins"], fn: () => api.get("/api/margins") }],
    "/returns":             [{ key: ["returns-overview"], fn: () => api.get("/api/returns-overview") }],
};

const NAV_GROUPS = [
    {
        label: "Overview",
        items: [
            { to: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
        ],
    },
    {
        label: "Performance",
        items: [
            { to: "/sales-trend",         label: "Sales Trend",     icon: TrendingUp },
            { to: "/amazon-sales-trend",  label: "Amazon + 1P",     icon: ShoppingCart },
            { to: "/category-sales",      label: "Category",        icon: Tag },
            { to: "/margin-snapshot",     label: "Margin Snapshot", icon: Percent },
        ],
    },
    {
        label: "Operations",
        items: [
            { to: "/inventory-dashboard", label: "Inventory",    icon: Package },
        ],
    },
    {
        label: "Advertising",
        items: [
            { to: "/ams-trend",           label: "AMS Trend",       icon: Megaphone },
            { to: "/ams-planning",        label: "AMS Planning",    icon: ClipboardList },
            { to: "/ams-poor-performers", label: "Ad Underperformers", icon: AlertTriangle },
        ],
    },
    {
        label: "Signals",
        items: [
            { to: "/no-sales-last-week",  label: "No Sales Last Week", icon: AlertCircle },
            { to: "/dead-stock",          label: "Dead Stock",         icon: Archive },
            { to: "/returns",             label: "Returns",            icon: RotateCcw },
        ],
    },
];

// External operator tools — different Render services that complement
// the weekly brief.  Rendered at the bottom of the sidebar as <a target=_blank>
// so the current page stays open.
const EXTERNAL_LINKS = [
    { href: "https://audio-array-sales-dashboard.onrender.com/", label: "Audio Array — Target vs Actual", icon: Target },
    { href: "https://nexlev-sales-dashboard-v2.onrender.com/",   label: "Nexlev — Target vs Actual",      icon: Target },
    { href: "https://am-replenishment-1.onrender.com/",          label: "Replenishment",                  icon: Truck },
    { href: "https://nexlev-margin-calculator.onrender.com/",    label: "Nexlev Margin Calculator",       icon: Calculator },
    { href: "https://buybox-report.onrender.com/",               label: "Buy Box Report",                 icon: Award },
];

export default function AppLayout({ children }: { children: ReactNode }) {
    const { user, logout } = useAuth();
    const navigate = useNavigate();
    const location = useLocation();
    const showBack = location.pathname !== "/dashboard";
    const queryClient = useQueryClient();
    const syncStatus = useSyncStatus();

    function prefetchRoute(path: string) {
        // Fire the chunk download — React.lazy() shares this module cache.
        CHUNK_LOADERS[path]?.().catch(() => { /* swallow; not blocking */ });
        // Warm React Query's cache.  prefetchQuery is a no-op if data
        // already exists within staleTime, so spamming hover is safe.
        const queries = PREFETCH_QUERIES[path];
        if (!queries) return;
        for (const q of queries) {
            queryClient.prefetchQuery({
                queryKey: q.key as any,
                queryFn: q.fn,
                staleTime: 5 * 60_000,
            }).catch(() => { /* unauth or net error — page will retry */ });
        }
    }

    return (
        <div className="min-h-screen grid" style={{ gridTemplateColumns: "260px 1fr" }}>
            {/* ── SIDEBAR ───────────────────────────────────────── */}
            <aside
                className="sticky top-0 h-screen flex flex-col border-r"
                style={{
                    background: "linear-gradient(180deg, rgba(255,255,255,0.92) 0%, rgba(250,251,252,0.92) 100%)",
                    backdropFilter: "saturate(180%) blur(16px)",
                    WebkitBackdropFilter: "saturate(180%) blur(16px)",
                    borderRightColor: "#e4e4e7",
                }}
            >
                {/* Brand mark */}
                <div className="px-5 pt-6 pb-5">
                    <Link to="/dashboard" className="flex items-center gap-3">
                        <span
                            className="h-9 w-9 flex items-center justify-center text-[15px] font-bold text-white"
                            style={{
                                background: "linear-gradient(135deg, #1e40af 0%, #0c1e5b 100%)",
                                boxShadow:
                                    "0 6px 16px -4px rgba(30, 64, 175, 0.45), inset 0 1px 0 rgba(255,255,255,0.20)",
                                borderRadius: 9,
                            }}
                        >
                            W
                        </span>
                        <div className="flex flex-col leading-none">
                            <span className="text-[15px] font-bold tracking-tight text-foreground">Weekly Brief</span>
                            <span className="text-[10.5px] mt-1" style={{ color: "#6b7280", letterSpacing: "0.04em" }}>
                                operator console
                            </span>
                        </div>
                    </Link>
                </div>

                {/* Nav groups */}
                <nav className="flex-1 overflow-y-auto px-3 pb-4">
                    {NAV_GROUPS.map((grp) => (
                        <div key={grp.label} className="mb-5">
                            <div
                                className="text-[10px] font-semibold uppercase mb-2 px-2"
                                style={{ letterSpacing: "0.14em", color: "#9ca3af" }}
                            >
                                {grp.label}
                            </div>
                            <div className="flex flex-col gap-0.5">
                                {grp.items.map(({ to, label, icon: Icon }) => (
                                    <NavLink
                                        key={to}
                                        to={to}
                                        onMouseEnter={() => prefetchRoute(to)}
                                        onFocus={() => prefetchRoute(to)}
                                        onTouchStart={() => prefetchRoute(to)}
                                        className={({ isActive }) =>
                                            `group relative inline-flex items-center gap-2.5 rounded-md px-2.5 py-2 text-[13px] font-medium transition-all ${
                                                isActive
                                                    ? "text-white"
                                                    : "text-foreground hover:bg-accent"
                                            }`
                                        }
                                        style={({ isActive }: { isActive: boolean }) =>
                                            isActive
                                                ? {
                                                      background:
                                                          "linear-gradient(135deg, #1e40af 0%, #1e3a8a 100%)",
                                                      boxShadow:
                                                          "0 6px 16px -4px rgba(30, 64, 175, 0.35), inset 0 1px 0 rgba(255,255,255,0.16)",
                                                  }
                                                : undefined
                                        }
                                    >
                                        <Icon className="h-4 w-4 shrink-0" />
                                        <span className="flex-1">{label}</span>
                                        <ChevronRight
                                            className="h-3.5 w-3.5 opacity-0 group-hover:opacity-50 transition-opacity"
                                        />
                                    </NavLink>
                                ))}
                            </div>
                        </div>
                    ))}

                    {/* ── External tools ── separate Render apps that
                       complement the weekly brief.  Opens in a new tab. */}
                    <div className="mb-5">
                        <div
                            className="text-[10px] font-semibold uppercase mb-2 px-2"
                            style={{ letterSpacing: "0.14em", color: "#9ca3af" }}
                        >
                            External Tools
                        </div>
                        <div className="flex flex-col gap-0.5">
                            {EXTERNAL_LINKS.map(({ href, label, icon: Icon }) => (
                                <a
                                    key={href}
                                    href={href}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="group relative inline-flex items-center gap-2.5 rounded-md px-2.5 py-2 text-[13px] font-medium text-foreground hover:bg-accent transition-all"
                                >
                                    <Icon className="h-4 w-4 shrink-0" />
                                    <span className="flex-1 truncate">{label}</span>
                                    <ExternalLink
                                        className="h-3.5 w-3.5 opacity-30 group-hover:opacity-70 transition-opacity"
                                    />
                                </a>
                            ))}
                        </div>
                    </div>
                </nav>

                {/* Status pill + user */}
                <div className="px-3 pb-4 mt-auto">
                    <div
                        className="rounded-lg p-3 mb-2"
                        style={{
                            background: "linear-gradient(135deg, rgba(5,150,105,0.06) 0%, rgba(5,150,105,0.02) 100%)",
                            border: "1px solid rgba(5,150,105,0.18)",
                        }}
                    >
                        <div className="flex items-center gap-2">
                            <span
                                className="h-2 w-2 rounded-full inline-block"
                                style={{
                                    background: "#059669",
                                    boxShadow: "0 0 0 3px rgba(5,150,105,0.18)",
                                    animation: "pulse-dot 2.4s ease-in-out infinite",
                                }}
                            />
                            <span className="text-[11px] font-semibold" style={{ color: "#065f46" }}>
                                {syncStatus.data ? `Synced · ${formatRelative(syncStatus.data.newest_mtime)}` : "Synced"}
                            </span>
                        </div>
                        <div className="text-[10.5px] mt-1" style={{ color: "#6b7280", letterSpacing: "0.02em" }}>
                            {(() => {
                                const s = syncStatus.data?.snapshots;
                                if (!s) return "Latest snapshot is current";
                                const wk = s.sales?.latest_week || s.inventory?.latest_week;
                                return wk ? `Latest: ${wk}` : "Latest snapshot is current";
                            })()}
                        </div>
                    </div>

                    {/* User card */}
                    <div
                        className="flex items-center gap-2.5 rounded-lg p-2.5"
                        style={{ background: "#fafafa", border: "1px solid #e4e4e7" }}
                    >
                        <span
                            className="h-7 w-7 rounded-full flex items-center justify-center text-[11px] font-bold text-white shrink-0"
                            style={{ background: "linear-gradient(135deg, #1a1a1a 0%, #404040 100%)" }}
                        >
                            {user?.email?.[0]?.toUpperCase() || "U"}
                        </span>
                        <span
                            className="flex-1 truncate text-[11.5px] font-medium"
                            style={{ color: "#374151" }}
                            title={user?.email || ""}
                        >
                            {user?.email}
                        </span>
                        <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => logout().then(() => (window.location.href = "/login"))}
                            className="h-7 w-7 p-0 shrink-0"
                            title="Logout"
                        >
                            <LogOut className="h-3.5 w-3.5" />
                        </Button>
                    </div>
                </div>
            </aside>

            {/* ── MAIN ──────────────────────────────────────────── */}
            <main className="min-w-0 overflow-x-hidden">
                {/* Top action bar — only shows when not on Dashboard (back button) */}
                {showBack && (
                    <div
                        className="sticky top-0 z-30 px-8 py-3 flex items-center gap-3"
                        style={{
                            background: "rgba(255,255,255,0.78)",
                            backdropFilter: "saturate(180%) blur(14px)",
                            WebkitBackdropFilter: "saturate(180%) blur(14px)",
                            borderBottom: "1px solid #e4e4e7",
                        }}
                    >
                        <Button variant="ghost" size="sm" onClick={() => navigate(-1)} title="Back">
                            <ArrowLeft className="h-4 w-4" />
                            <span className="text-[12px]">Back</span>
                        </Button>
                    </div>
                )}
                <div className="px-8 py-8">
                    <div className="mx-auto max-w-[1640px] space-y-7">{children}</div>
                </div>
            </main>
        </div>
    );
}
