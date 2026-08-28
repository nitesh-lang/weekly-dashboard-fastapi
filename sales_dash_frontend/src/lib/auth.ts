import { create } from "zustand";
import { api } from "./api";
import { newSession, sessionId } from "./activity";

export interface Me {
  user:
    | {
        email: string;
        full_name: string | null;
        role: "admin" | "viewer";
        must_reset_password: boolean;
      }
    | null;
}

interface AuthState {
  me: Me | null;
  loading: boolean;
  fetchMe: () => Promise<void>;
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

export const useAuth = create<AuthState>((set) => ({
  me: null,
  loading: true,
  fetchMe: async () => {
    set({ loading: true });
    try {
      const me = await api.get<Me>("/api/auth/me");
      set({ me, loading: false });
    } catch {
      set({ me: null, loading: false });
    }
  },
  login: async (email, password) => {
    // A fresh session id per sign-in, so time-in-tool is measured per visit.
    const session_id = newSession();
    const res = await api.post<{ ok: boolean; user?: Me["user"]; error?: string }>(
      "/api/auth/login",
      { email, password, session_id }
    );
    if (!res.ok) throw new Error(res.error || "Login failed");
    set({ me: { user: res.user ?? null } });
  },
  logout: async () => {
    await api.post("/api/auth/logout", undefined, { session_id: sessionId() });
    set({ me: null });
  },
}));
