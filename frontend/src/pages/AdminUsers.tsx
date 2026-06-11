import { useEffect, useMemo, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import AppLayout from "@/components/AppLayout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api";
import { ShieldCheck, Trash2, Plus, Save } from "lucide-react";

type Role = "admin" | "operator" | "viewer";

interface UserOut {
    email: string;
    role: Role;
    tabs: string[];
    created_at?: string | null;
    password_updated_at?: string | null;
}

interface TabsCatalog {
    tabs: string[];
    roles: Role[];
}

export default function AdminUsers() {
    const qc = useQueryClient();
    const users = useQuery<UserOut[]>({
        queryKey: ["admin-users"],
        queryFn:  () => api.get("/api/admin/users"),
    });
    const catalog = useQuery<TabsCatalog>({
        queryKey: ["admin-tabs-catalog"],
        queryFn:  () => api.get("/api/admin/tabs"),
        staleTime: Infinity,
    });

    const knownTabs = catalog.data?.tabs ?? [];
    const roles: Role[] = catalog.data?.roles ?? ["admin", "operator", "viewer"];

    // ── New-user form state ────────────────────────────────────────
    const [newEmail, setNewEmail] = useState("");
    const [newPwd,   setNewPwd]   = useState("");
    const [newRole,  setNewRole]  = useState<Role>("operator");
    const [newTabs,  setNewTabs]  = useState<string[]>([]);
    const [createError, setCreateError] = useState<string | null>(null);

    const createMut = useMutation({
        mutationFn: () => api.post("/api/admin/users", {
            email: newEmail, password: newPwd, role: newRole, tabs: newTabs,
        }),
        onSuccess: () => {
            setNewEmail(""); setNewPwd(""); setNewRole("operator"); setNewTabs([]);
            setCreateError(null);
            qc.invalidateQueries({ queryKey: ["admin-users"] });
        },
        onError: (e: any) => setCreateError(e?.body?.detail || e?.message || "Failed to create"),
    });

    return (
        <AppLayout>
            <div className="flex flex-wrap items-end gap-4">
                <div>
                    <div className="eyebrow mb-2" style={{ color: "#1e40af" }}>Settings · Access Control</div>
                    <h1 className="text-2xl font-semibold tracking-tight flex items-center gap-2">
                        <ShieldCheck className="h-5 w-5" style={{ color: "#1e40af" }} />
                        Users &amp; Roles
                    </h1>
                    <p className="text-[13.5px] mt-1.5" style={{ color: "#4b5563" }}>
                        Manage who can sign in, what role they hold, and which tabs they see.
                        Roles: <strong>admin</strong> (everything, including this page),
                        {" "}<strong>operator</strong> (view + export + edit), {" "}
                        <strong>viewer</strong> (view-only, no exports).
                    </p>
                </div>
            </div>

            {/* ─── Create user ─── */}
            <Card className="p-4">
                <h2 className="text-sm font-semibold tracking-tight mb-3 flex items-center gap-2">
                    <Plus className="h-4 w-4" /> Add user
                </h2>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-3 mb-3">
                    <div>
                        <label className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">Email</label>
                        <Input
                            type="email" value={newEmail} onChange={(e) => setNewEmail(e.target.value)}
                            placeholder="name@brand.com" className="h-9 text-sm mt-1"
                        />
                    </div>
                    <div>
                        <label className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">Password</label>
                        <Input
                            type="password" value={newPwd} onChange={(e) => setNewPwd(e.target.value)}
                            placeholder="≥ 8 characters" className="h-9 text-sm mt-1"
                        />
                    </div>
                    <div>
                        <label className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">Role</label>
                        <select
                            value={newRole}
                            onChange={(e) => setNewRole(e.target.value as Role)}
                            className="w-full h-9 text-sm mt-1 rounded-md border bg-background px-2"
                        >
                            {roles.map((r) => <option key={r} value={r}>{r}</option>)}
                        </select>
                    </div>
                    <div className="flex items-end">
                        <Button
                            onClick={() => createMut.mutate()}
                            disabled={!newEmail || newPwd.length < 8 || createMut.isPending}
                            className="w-full"
                        >
                            {createMut.isPending ? "Adding…" : "Add user"}
                        </Button>
                    </div>
                </div>
                <div>
                    <label className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">Tabs (empty = all)</label>
                    <TabPicker
                        tabs={knownTabs}
                        selected={newTabs}
                        onChange={setNewTabs}
                    />
                </div>
                {createError && (
                    <div className="text-xs mt-3" style={{ color: "#b91c1c" }}>{createError}</div>
                )}
            </Card>

            {/* ─── Existing users ─── */}
            <Card className="overflow-hidden">
                <div className="border-b px-4 py-2.5 flex items-center justify-between bg-secondary/30">
                    <h2 className="text-sm font-semibold tracking-tight">Existing users</h2>
                    <div className="text-xs text-muted-foreground">{users.data?.length ?? 0} total</div>
                </div>
                {users.isLoading && <div className="p-6 text-sm text-muted-foreground">Loading…</div>}
                {users.error && <div className="p-6 text-sm" style={{ color: "#b91c1c" }}>Failed to load users.</div>}
                {users.data && (
                    <div className="divide-y">
                        {users.data.map((u) => (
                            <UserRow key={u.email} user={u} roles={roles} knownTabs={knownTabs} />
                        ))}
                    </div>
                )}
            </Card>
        </AppLayout>
    );
}

function UserRow({ user, roles, knownTabs }: { user: UserOut; roles: Role[]; knownTabs: string[] }) {
    const qc = useQueryClient();
    const [role, setRole]   = useState<Role>(user.role);
    const [tabs, setTabs]   = useState<string[]>(user.tabs);
    const [pwd,  setPwd]    = useState("");
    const [err,  setErr]    = useState<string | null>(null);
    useEffect(() => { setRole(user.role); setTabs(user.tabs); }, [user]);

    const dirty = role !== user.role
        || tabs.length !== user.tabs.length
        || tabs.some((t) => !user.tabs.includes(t))
        || pwd.length > 0;

    const saveMut = useMutation({
        mutationFn: () => api.patch(`/api/admin/users/${encodeURIComponent(user.email)}`, {
            role, tabs, ...(pwd ? { password: pwd } : {}),
        }),
        onSuccess: () => {
            setPwd(""); setErr(null);
            qc.invalidateQueries({ queryKey: ["admin-users"] });
        },
        onError: (e: any) => setErr(e?.body?.detail || e?.message || "Save failed"),
    });
    const deleteMut = useMutation({
        mutationFn: () => api.delete(`/api/admin/users/${encodeURIComponent(user.email)}`),
        onSuccess:  () => qc.invalidateQueries({ queryKey: ["admin-users"] }),
        onError:    (e: any) => setErr(e?.body?.detail || e?.message || "Delete failed"),
    });

    return (
        <div className="p-4">
            <div className="flex flex-wrap items-start gap-3 mb-3">
                <div className="flex-1 min-w-[200px]">
                    <div className="text-sm font-semibold tracking-tight">{user.email}</div>
                    <div className="text-[11px] text-muted-foreground tabular mt-0.5">
                        joined {user.created_at?.slice(0, 10) ?? "—"}
                        {user.password_updated_at ? ` · pwd updated ${user.password_updated_at.slice(0, 10)}` : ""}
                    </div>
                </div>

                <div>
                    <label className="text-[10px] uppercase tracking-wider text-muted-foreground block">Role</label>
                    <select
                        value={role} onChange={(e) => setRole(e.target.value as Role)}
                        className="h-8 text-sm rounded-md border bg-background px-2 mt-1"
                    >
                        {roles.map((r) => <option key={r} value={r}>{r}</option>)}
                    </select>
                </div>

                <div className="min-w-[200px]">
                    <label className="text-[10px] uppercase tracking-wider text-muted-foreground block">Reset password (optional)</label>
                    <Input
                        type="password" value={pwd} onChange={(e) => setPwd(e.target.value)}
                        placeholder="≥ 8 chars to update" className="h-8 text-sm mt-1"
                    />
                </div>

                <div className="flex items-end gap-1">
                    <Button
                        size="sm" onClick={() => saveMut.mutate()}
                        disabled={!dirty || saveMut.isPending || (pwd.length > 0 && pwd.length < 8)}
                    >
                        <Save className="h-3.5 w-3.5" />
                        {saveMut.isPending ? "Saving…" : "Save"}
                    </Button>
                    <Button
                        size="sm" variant="outline"
                        onClick={() => {
                            if (confirm(`Delete user ${user.email}?`)) deleteMut.mutate();
                        }}
                        disabled={deleteMut.isPending}
                    >
                        <Trash2 className="h-3.5 w-3.5" style={{ color: "#b91c1c" }} />
                    </Button>
                </div>
            </div>

            <div>
                <label className="text-[10px] uppercase tracking-wider text-muted-foreground block mb-1">
                    Tabs (empty = role default)
                </label>
                <TabPicker tabs={knownTabs} selected={tabs} onChange={setTabs} />
            </div>

            {err && <div className="text-xs mt-2" style={{ color: "#b91c1c" }}>{err}</div>}
        </div>
    );
}

function TabPicker({ tabs, selected, onChange }: { tabs: string[]; selected: string[]; onChange: (v: string[]) => void }) {
    const toggle = (t: string) => {
        onChange(selected.includes(t) ? selected.filter((x) => x !== t) : [...selected, t]);
    };
    const sortedTabs = useMemo(() => [...tabs].sort(), [tabs]);
    return (
        <div className="flex flex-wrap gap-1.5 mt-1">
            {sortedTabs.map((t) => {
                const on = selected.includes(t);
                return (
                    <button
                        key={t}
                        type="button"
                        onClick={() => toggle(t)}
                        className={
                            "inline-flex items-center gap-1 rounded-md px-2 py-1 text-[11px] font-medium border transition-colors " +
                            (on
                                ? "bg-primary/10 text-primary border-primary/30"
                                : "bg-background text-muted-foreground border-border hover:bg-secondary")
                        }
                    >
                        {t}
                    </button>
                );
            })}
            {sortedTabs.length === 0 && <span className="text-xs text-muted-foreground">No tabs catalog loaded.</span>}
        </div>
    );
}
