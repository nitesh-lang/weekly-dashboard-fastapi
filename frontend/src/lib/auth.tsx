import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { api, ApiError } from "./api";

interface AuthCtx {
    user: { email: string } | null;
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
            const data = await api.get<{ email: string }>("/api/me");
            setUser(data);
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
