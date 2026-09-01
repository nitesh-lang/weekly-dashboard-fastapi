import { useEffect } from "react";
import { Link, NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { LayoutDashboard, RefreshCw, Upload, LogOut, Search, Circle, Users as UsersIcon, Activity } from "lucide-react";
import { useAuth } from "@/lib/auth";
import { useBrand, type BrandOption } from "@/store/useBrand";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";
import { startHeartbeat, track } from "@/lib/activity";

const NAV = [
  { to: "/", label: "Dashboard", icon: LayoutDashboard, end: true },
  { to: "/sync", label: "SP-API Sync", icon: RefreshCw },
  { to: "/upload", label: "Excel Upload", icon: Upload },
  { to: "/usage", label: "Usage", icon: Activity, adminOnly: true },
  { to: "/users", label: "Users", icon: UsersIcon, adminOnly: true },
];

export function AppLayout() {
  const { me, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const setBrands = useBrand((s) => s.setBrands);
  const brand = useBrand((s) => s.brand);
  const setBrand = useBrand((s) => s.setBrand);
  const brands = useBrand((s) => s.brands);

  const { data: brandsRes } = useQuery({
    queryKey: ["brands"],
    queryFn: () => api.get<{ brands: BrandOption[] }>("/api/brands"),
    staleTime: 5 * 60_000,
  });

  useEffect(() => {
    if (brandsRes?.brands?.length) {
      setBrands(brandsRes.brands);
      if (!brandsRes.brands.find((b) => b.key === brand)) {
        setBrand(brandsRes.brands[0].key);
      }
    }
  }, [brandsRes, brand, setBrand, setBrands]);

  // Record each page the user opens, and keep a heartbeat running so time
  // spent in the tool can be measured. Both are fire-and-forget.
  useEffect(() => {
    if (!me?.user) return;
    track("page_view", {
      page: location.pathname,
      brand: useBrand.getState().brand,
    });
  }, [location.pathname, me?.user]);

  useEffect(() => {
    if (!me?.user) return;
    return startHeartbeat(() => useBrand.getState().brand);
  }, [me?.user]);

  async function handleLogout() {
    await logout();
    navigate("/login", { replace: true });
  }

  return (
    <div className="flex h-full">
      <aside className="flex w-[256px] shrink-0 flex-col surface border-l-0 border-t-0 border-b-0">
        {/* Brand — click to go home */}
        <Link
          to="/"
          className="flex h-14 items-center gap-2.5 px-4 border-b hairline hover:bg-[hsl(var(--canvas))] transition-colors"
          title="Go to dashboard"
        >
          <div className="grid h-8 w-8 place-items-center rounded-md text-[11px] font-bold text-white bg-primary">
            SD
          </div>
          <div>
            <div className="text-[14px] font-bold">Sales Dashboard</div>
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[hsl(var(--ink-2))]">
              Cambium Retail
            </div>
          </div>
        </Link>

        {/* Search */}
        <div className="p-3">
          <button className="w-full flex items-center gap-2 h-8 px-2.5 rounded-md text-[12px] font-medium bg-[hsl(var(--canvas))] border hairline text-[hsl(var(--ink-2))] hover:text-[hsl(var(--ink))]">
            <Search className="h-3.5 w-3.5" />
            <span className="flex-1 text-left">Search</span>
            <span className="kbd">⌘K</span>
          </button>
        </div>

        {/* Brand switch */}
        <div className="px-3 pb-3">
          <div className="mb-1.5 flex items-center justify-between text-[10px] font-bold uppercase tracking-widest text-[hsl(var(--ink-2))]">
            <span>Brand</span>
            <span className="font-mono text-[10px] text-[hsl(var(--ink-2))]">{brands.length}</span>
          </div>
          <div className="rounded-md border hairline overflow-hidden">
            {brands.map((b) => (
              <button
                key={b.key}
                onClick={() => setBrand(b.key)}
                className={cn(
                  "w-full text-left px-2.5 h-9 text-[12px] font-semibold transition-colors flex items-center gap-2",
                  brand === b.key
                    ? "bg-primary text-white"
                    : "bg-[hsl(var(--paper))] text-[hsl(var(--ink))] hover:bg-[hsl(var(--canvas))]"
                )}
              >
                <Circle
                  className={cn(
                    "h-2 w-2 shrink-0",
                    brand === b.key ? "fill-white text-white" : "fill-[hsl(var(--ink-2))] text-[hsl(var(--ink-2))] opacity-50"
                  )}
                />
                <span className="flex-1">{b.label}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Nav */}
        <div className="px-3 pb-1 text-[10px] font-bold uppercase tracking-widest text-[hsl(var(--ink-2))]">
          Workspace
        </div>
        <nav className="flex-1 px-2 space-y-0.5 overflow-y-auto">
          {NAV.filter((n) => !("adminOnly" in n) || me?.user?.role === "admin").map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              end={n.end}
              className={({ isActive }) =>
                cn(
                  "group relative flex items-center gap-2.5 h-9 px-2.5 rounded-md text-[13px] font-semibold transition-colors",
                  isActive
                    ? "bg-primary text-white shadow-sm"
                    : "text-[hsl(var(--ink))] hover:bg-[hsl(var(--canvas))]"
                )
              }
            >
              {({ isActive }) => (
                <>
                  {isActive && (
                    <span className="absolute left-0 top-1/2 h-4 w-0.5 -translate-y-1/2 rounded-r-full bg-white/70" />
                  )}
                  <n.icon className="h-4 w-4" />
                  <span className="flex-1">{n.label}</span>
                </>
              )}
            </NavLink>
          ))}
        </nav>

        {/* Back to the Weekly dashboard — plain anchor: leaves the SPA */}
        <div className="px-2 pb-1">
          <a
            href="/"
            className="flex items-center gap-2.5 h-9 px-2.5 rounded-md text-[13px] font-semibold text-[hsl(var(--ink-2))] hover:bg-[hsl(var(--canvas))] hover:text-[hsl(var(--ink))] transition-colors"
            title="Back to the Weekly dashboard"
          >
            <span aria-hidden>←</span>
            <span className="flex-1">Weekly Dashboard</span>
          </a>
        </div>

        {/* Workspace / user */}
        <div className="p-3 border-t hairline">
          <div className="rounded-md p-2.5 flex items-center gap-2 bg-[hsl(var(--canvas))] border hairline">
            <div className="w-7 h-7 rounded-full grid place-items-center text-white text-[11px] font-bold bg-primary">
              {(me?.user?.full_name || me?.user?.email || "?").charAt(0).toUpperCase()}
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-[12px] font-bold truncate">
                {me?.user?.full_name || me?.user?.email}
              </div>
              <div className="text-[10px] font-bold uppercase tracking-wider text-[hsl(var(--ink-2))]">
                {me?.user?.role ?? "viewer"}
              </div>
            </div>
            <button
              onClick={handleLogout}
              className="p-1.5 rounded text-[hsl(var(--ink-2))] hover:text-[hsl(var(--ink))]"
              title="Sign out"
            >
              <LogOut className="h-3.5 w-3.5" />
            </button>
          </div>
        </div>
      </aside>

      <main className="flex-1 overflow-auto bg-[hsl(var(--canvas))]">
        <Outlet />
      </main>
    </div>
  );
}
