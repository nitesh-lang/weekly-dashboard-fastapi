import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { api, ApiError } from "./api";

export type Role = "admin" | "operator" | "viewer";

export interface User {
    email: string;
    role: Role;
    tabs: string[];        // allowlist; empty = no per-user restriction
    known_tabs: string[];  // catalog reported by backend (for admin UI)
}

interface AuthCtx {
    user: User | null;
    loading: boolean;
    refresh: () => Promise<void>;
    logout: () => Promise<void>;
}

const Ctx = createContext<AuthCtx>({
    user: null,
    loading: true,
    refresh: async () => {},
    logout: async () => {},
});

export function AuthProvider({ children }: { children: ReactNode }) {
    const [user, setUser] = useState<AuthCtx["user"]>(null);
    const [loading, setLoading] = useState(true);

    async function refresh() {
        try {
            const data = await api.get<User>("/api/me");
            // Defensive defaults — backend always sends these now, but
            // older sessions or future schema drift shouldn't break the UI.
            setUser({
                email:      data.email,
                role:       (data.role || "admin") as Role,
                tabs:       data.tabs || [],
                known_tabs: data.known_tabs || [],
            });
        } catch (e) {
            if (e instanceof ApiError && e.status === 401) setUser(null);
            else console.error("auth refresh failed", e);
        } finally {
            setLoading(false);
        }
    }

    async function logout() {
        try { await api.post("/logout"); } catch { /* ignore */ }
        setUser(null);
    }

    useEffect(() => { refresh(); }, []);

    return <Ctx.Provider value={{ user, loading, refresh, logout }}>{children}</Ctx.Provider>;
}

export function useAuth() { return useContext(Ctx); }

export function RequireAuth({ children }: { children: ReactNode }) {
    const { user, loading } = useAuth();
    const loc = useLocation();
    if (loading) {
        return (
            <div className="flex h-screen items-center justify-center text-muted-foreground">
                Loading…
            </div>
        );
    }
    if (!user) {
        return <Navigate to={`/login?next=${encodeURIComponent(loc.pathname)}`} replace />;
    }
    return <>{children}</>;
}

// ─── Permission helpers ──────────────────────────────────────────────
// Admins bypass every check.  Operators get full data access.  Viewers
// see what they're allowed to see and lose export / edit affordances.

export function canExport(user: User | null): boolean {
    if (!user) return false;
    return user.role === "admin" || user.role === "operator";
}

export function canEdit(user: User | null): boolean {
    if (!user) return false;
    return user.role === "admin" || user.role === "operator";
}

export function canAccessTab(user: User | null, tab: string): boolean {
    if (!user) return false;
    if (user.role === "admin") return true;
    // Empty tabs allowlist = no per-user restriction; show every tab.
    if (!user.tabs || user.tabs.length === 0) return true;
    return user.tabs.includes(tab);
}

export function useCanExport() { return canExport(useAuth().user); }
export function useCanEdit()   { return canEdit(useAuth().user);   }
export function useRole(): Role | null { return useAuth().user?.role ?? null; }

/** Wrap a route so only admins reach it; everyone else gets bounced to /dashboard. */
export function RequireAdmin({ children }: { children: ReactNode }) {
    const { user, loading } = useAuth();
    const loc = useLocation();
    if (loading) {
        return (
            <div className="flex h-screen items-center justify-center text-muted-foreground">
                Loading…
            </div>
        );
    }
    if (!user) {
        return <Navigate to={`/login?next=${encodeURIComponent(loc.pathname)}`} replace />;
    }
    if (user.role !== "admin") {
        return (
            <div className="flex h-screen flex-col items-center justify-center gap-2 text-muted-foreground">
                <div className="text-lg font-semibold text-foreground">403 — Admin only</div>
                <div>You don't have permission to view this page.</div>
            </div>
        );
    }
    return <>{children}</>;
}

/** Wrap a route so users without the tab in their allowlist are redirected. */
export function RequireTab({ tab, children }: { tab: string; children: ReactNode }) {
    const { user, loading } = useAuth();
    const loc = useLocation();
    if (loading) {
        return (
            <div className="flex h-screen items-center justify-center text-muted-foreground">
                Loading…
            </div>
        );
    }
    if (!user) {
        return <Navigate to={`/login?next=${encodeURIComponent(loc.pathname)}`} replace />;
    }
    if (!canAccessTab(user, tab)) {
        return (
            <div className="flex h-screen flex-col items-center justify-center gap-2 text-muted-foreground">
                <div className="text-lg font-semibold text-foreground">403 — Access denied</div>
                <div>This page isn't in your tab allowlist. Ask an admin to grant access.</div>
            </div>
        );
    }
    return <>{children}</>;
}
