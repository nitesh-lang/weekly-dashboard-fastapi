import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { KeyRound, CheckCircle2 } from "lucide-react";
import { api } from "@/lib/api";
import { useAuth } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export function ResetPassword() {
  const { me, fetchMe } = useAuth();
  const navigate = useNavigate();
  const [cur, setCur] = useState("");
  const [next, setNext] = useState("");
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [ok, setOk] = useState(false);
  const [busy, setBusy] = useState(false);

  const forced = me?.user?.must_reset_password;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (next.length < 8) return setError("New password must be at least 8 characters.");
    if (next !== confirm) return setError("New passwords don't match.");
    setBusy(true);
    try {
      const r = await api.post<{ ok: boolean; error?: string }>("/api/auth/reset-password", {
        current_password: cur,
        new_password: next,
      });
      if (!r.ok) throw new Error(r.error || "Reset failed");
      setOk(true);
      await fetchMe();
      setTimeout(() => navigate("/", { replace: true }), 800);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reset failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid min-h-screen place-items-center bg-[hsl(var(--hairline-soft))] p-4">
      <div className="w-full max-w-md">
        <div className="mb-8 flex items-center gap-2">
          <div className="grid h-9 w-9 place-items-center rounded-md bg-[hsl(var(--primary))] text-white shadow-sm">
            <KeyRound className="h-5 w-5" />
          </div>
          <div>
            <div className="text-lg font-semibold leading-tight">Set your password</div>
            <div className="text-xs text-muted-foreground">
              {forced ? "First-login requirement" : "Change password"}
            </div>
          </div>
        </div>
        <form
          onSubmit={submit}
          className="rounded-xl border border-[hsl(var(--hairline))] bg-white p-6 shadow-[0_1px_2px_rgba(15,27,45,0.04)]"
        >
          <div className="mb-3 text-[13px] font-semibold">{me?.user?.email}</div>
          <label className="mb-1.5 block text-sm font-medium">Current password</label>
          <Input type="password" value={cur} onChange={(e) => setCur(e.target.value)} required autoFocus className="mb-3" />
          <label className="mb-1.5 block text-sm font-medium">New password</label>
          <Input type="password" value={next} onChange={(e) => setNext(e.target.value)} required className="mb-3" />
          <label className="mb-1.5 block text-sm font-medium">Confirm new password</label>
          <Input type="password" value={confirm} onChange={(e) => setConfirm(e.target.value)} required className="mb-4" />

          {error && (
            <div className="mb-4 rounded-md border border-[hsl(var(--coral))] bg-[hsl(var(--coral-soft))] px-3 py-2 text-sm text-[hsl(var(--coral))]">
              {error}
            </div>
          )}
          {ok && (
            <div className="mb-4 flex items-center gap-2 rounded-md border border-[hsl(var(--emerald))] bg-[hsl(var(--emerald-soft))]/60 px-3 py-2 text-sm text-[hsl(var(--emerald))]">
              <CheckCircle2 className="h-4 w-4" /> Password updated — redirecting…
            </div>
          )}
          <Button type="submit" className="w-full" disabled={busy || ok}>
            {busy ? "Updating…" : "Update password"}
          </Button>
        </form>
      </div>
    </div>
  );
}
