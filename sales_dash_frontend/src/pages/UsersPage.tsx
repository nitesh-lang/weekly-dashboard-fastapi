import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Users, UserPlus, KeyRound, Trash2, ShieldCheck, Ban, CheckCircle2, X } from "lucide-react";
import { api } from "@/lib/api";
import { useAuth } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

interface Row {
  id: number;
  email: string;
  full_name: string | null;
  role: "admin" | "viewer";
  is_active: boolean;
  must_reset_password: boolean;
  created_at: string;
}

export function UsersPage() {
  const { me } = useAuth();
  const qc = useQueryClient();
  const [showAdd, setShowAdd] = useState(false);
  const [flash, setFlash] = useState<{ ok: boolean; text: string } | null>(null);

  const { data, isLoading } = useQuery<{ users: Row[] }>({
    queryKey: ["admin-users"],
    queryFn: () => api.get("/api/admin/users"),
  });

  const invalidate = () => qc.invalidateQueries({ queryKey: ["admin-users"] });
  const notify = (ok: boolean, text: string) => {
    setFlash({ ok, text });
    setTimeout(() => setFlash(null), 3500);
  };

  const patch = useMutation({
    mutationFn: (v: { id: number; body: Partial<Row> }) => api.patch(`/api/admin/users/${v.id}`, v.body),
    onSuccess: () => {
      invalidate();
      notify(true, "Updated");
    },
    onError: (e: Error) => notify(false, e.message),
  });
  const resetPw = useMutation({
    mutationFn: (id: number) => api.post<{ temporary_password: string }>(`/api/admin/users/${id}/reset-password`),
    onSuccess: (r) => {
      invalidate();
      notify(true, `Reset — temp password: ${r.temporary_password}`);
    },
    onError: (e: Error) => notify(false, e.message),
  });
  const remove = useMutation({
    mutationFn: (id: number) => api.delete(`/api/admin/users/${id}`),
    onSuccess: () => {
      invalidate();
      notify(true, "Deleted");
    },
    onError: (e: Error) => notify(false, e.message),
  });

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
    <div className="mx-auto max-w-[1440px] px-8 pt-6 pb-10 space-y-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h1 className="text-[32px] leading-none">Users</h1>
          <div className="mt-2 text-[13px] font-semibold flex items-center gap-2">
            <span className="pill pill-navy-soft">{data?.users.length ?? 0} accounts</span>
          </div>
        </div>
        <Button onClick={() => setShowAdd(true)}>
          <UserPlus className="mr-1.5 h-4 w-4" /> Add user
        </Button>
      </div>

      {flash && (
        <div className={cn(
          "rounded-md border p-3 text-sm",
          flash.ok
            ? "border-[hsl(var(--emerald))] bg-[hsl(var(--emerald-soft))]/60 text-[hsl(var(--emerald))]"
            : "border-[hsl(var(--coral))] bg-[hsl(var(--coral-soft))] text-[hsl(var(--coral))]"
        )}>
          {flash.text}
        </div>
      )}

      <div className="surface-hi rounded-xl p-0 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="text-[11px] uppercase tracking-widest text-[hsl(var(--ink))]">
            <tr className="border-b hairline">
              <th className="px-4 py-3 text-left font-bold">Name / Email</th>
              <th className="px-4 py-3 text-left font-bold">Role</th>
              <th className="px-4 py-3 text-left font-bold">Status</th>
              <th className="px-4 py-3 text-right font-bold">Actions</th>
            </tr>
          </thead>
          <tbody>
            {isLoading && (
              <tr><td colSpan={4} className="px-4 py-6 text-center text-[hsl(var(--ink-2))]">Loading…</td></tr>
            )}
            {data?.users.map((u) => (
              <tr key={u.id} className="border-b border-[hsl(var(--hairline))] last:border-0">
                <td className="px-4 py-3">
                  <div className="font-semibold">{u.full_name || "—"}</div>
                  <div className="text-[12px] text-[hsl(var(--ink-2))]">{u.email}</div>
                </td>
                <td className="px-4 py-3">
                  <select
                    value={u.role}
                    onChange={(e) => patch.mutate({ id: u.id, body: { role: e.target.value as "admin" | "viewer" } })}
                    className="h-8 rounded-md border hairline bg-[hsl(var(--paper))] px-2 text-[12px] font-semibold"
                  >
                    <option value="viewer">viewer</option>
                    <option value="admin">admin</option>
                  </select>
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <span className={cn("pill text-[10px]", u.is_active ? "pill-emerald-soft" : "pill-muted")}>
                      {u.is_active ? "active" : "inactive"}
                    </span>
                    {u.must_reset_password && (
                      <span className="pill pill-amber-soft text-[10px]">must reset</span>
                    )}
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center justify-end gap-1.5">
                    <button
                      onClick={() => patch.mutate({ id: u.id, body: { is_active: !u.is_active } })}
                      title={u.is_active ? "Deactivate" : "Activate"}
                      className="rounded-md border hairline p-1.5 hover:bg-[hsl(var(--hairline-soft))]"
                    >
                      {u.is_active ? <Ban className="h-3.5 w-3.5" /> : <CheckCircle2 className="h-3.5 w-3.5" />}
                    </button>
                    <button
                      onClick={() => {
                        if (confirm(`Reset ${u.email} to the default password?`)) resetPw.mutate(u.id);
                      }}
                      title="Reset password"
                      className="rounded-md border hairline p-1.5 hover:bg-[hsl(var(--hairline-soft))]"
                    >
                      <KeyRound className="h-3.5 w-3.5" />
                    </button>
                    <button
                      onClick={() => {
                        if (confirm(`Delete ${u.email}? This cannot be undone.`)) remove.mutate(u.id);
                      }}
                      title="Delete user"
                      className="rounded-md border border-[hsl(var(--coral))]/40 bg-[hsl(var(--coral-soft))] p-1.5 text-[hsl(var(--coral))] hover:bg-[hsl(var(--coral-soft))]/70"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {showAdd && <AddUserDialog onClose={() => setShowAdd(false)} onSaved={() => { invalidate(); notify(true, "User added"); }} />}
    </div>
  );
}

function AddUserDialog({ onClose, onSaved }: { onClose: () => void; onSaved: () => void }) {
  const [email, setEmail] = useState("");
  const [name, setName] = useState("");
  const [role, setRole] = useState<"admin" | "viewer">("viewer");
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function save() {
    setErr(null);
    setBusy(true);
    try {
      await api.post("/api/admin/users", { email: email.trim(), full_name: name.trim() || null, role });
      onSaved();
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 grid place-items-center bg-black/40 p-4" onClick={onClose}>
      <div className="w-full max-w-md rounded-xl bg-white p-6 shadow-lg" onClick={(e) => e.stopPropagation()}>
        <div className="mb-4 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <ShieldCheck className="h-5 w-5 text-[hsl(var(--primary))]" />
            <h2 className="text-lg font-semibold">Add user</h2>
          </div>
          <button onClick={onClose} className="rounded-md p-1 hover:bg-[hsl(var(--hairline-soft))]">
            <X className="h-4 w-4" />
          </button>
        </div>
        <label className="mb-1.5 block text-sm font-medium">Email</label>
        <Input type="email" value={email} onChange={(e) => setEmail(e.target.value)} className="mb-3" placeholder="name@cambiumretail.com" autoFocus />
        <label className="mb-1.5 block text-sm font-medium">Full name</label>
        <Input value={name} onChange={(e) => setName(e.target.value)} className="mb-3" placeholder="Optional" />
        <label className="mb-1.5 block text-sm font-medium">Role</label>
        <select
          value={role}
          onChange={(e) => setRole(e.target.value as "admin" | "viewer")}
          className="mb-4 h-9 w-full rounded-md border hairline bg-white px-2 text-[13px] font-semibold"
        >
          <option value="viewer">viewer</option>
          <option value="admin">admin</option>
        </select>
        <div className="mb-4 text-[12px] text-[hsl(var(--ink-2))]">
          User will start with password <code className="mono text-[11px]">Cambium@109</code> and be forced to change on first login.
        </div>
        {err && (
          <div className="mb-3 rounded-md border border-[hsl(var(--coral))] bg-[hsl(var(--coral-soft))] px-3 py-2 text-sm text-[hsl(var(--coral))]">
            {err}
          </div>
        )}
        <div className="flex items-center justify-end gap-2">
          <button onClick={onClose} className="btn-outline h-9 rounded-md px-3 text-[12px]">Cancel</button>
          <Button onClick={save} disabled={busy || !email.trim()}>
            <Users className="mr-1.5 h-4 w-4" />
            {busy ? "Adding…" : "Add"}
          </Button>
        </div>
      </div>
    </div>
  );
}
