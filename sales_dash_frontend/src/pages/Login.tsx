import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { LineChart, TrendingUp } from "lucide-react";

export function Login() {
  const { login } = useAuth();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await login(email.trim(), password);
      navigate("/", { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid min-h-screen place-items-center bg-[hsl(var(--hairline-soft))] p-4">
      <div className="w-full max-w-md">
        <div className="mb-8 flex items-center gap-2">
          <div className="grid h-9 w-9 place-items-center rounded-md bg-[hsl(var(--primary))] text-white shadow-sm">
            <LineChart className="h-5 w-5" />
          </div>
          <div>
            <div className="text-lg font-semibold leading-tight tracking-tight">Sales Dashboard</div>
            <div className="text-xs text-muted-foreground">Multi-brand · Nexlev + Audio Array</div>
          </div>
        </div>
        <form
          onSubmit={onSubmit}
          className="rounded-xl border border-[hsl(var(--hairline))] bg-white p-6 shadow-[0_1px_2px_rgba(15,27,45,0.04)]"
        >
          <div className="mb-4">
            <label className="mb-1.5 block text-sm font-medium">Email</label>
            <Input
              type="email"
              autoComplete="username"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="info@cambiumretail.com"
              required
            />
          </div>
          <div className="mb-4">
            <label className="mb-1.5 block text-sm font-medium">Password</label>
            <Input
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>
          {error && (
            <div className="mb-4 rounded-md border border-[hsl(var(--coral))] bg-[hsl(var(--coral-soft))] px-3 py-2 text-sm text-[hsl(var(--coral))]">
              {error}
            </div>
          )}
          <Button type="submit" className="w-full" disabled={busy}>
            {busy ? "Signing in…" : "Sign in"}
            <TrendingUp className="ml-1.5 h-4 w-4" />
          </Button>
        </form>
      </div>
    </div>
  );
}
