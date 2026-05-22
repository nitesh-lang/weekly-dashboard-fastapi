import { useState, type FormEvent } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { api, ApiError } from "@/lib/api";
import { useAuth } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function Login() {
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [error, setError] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const nav = useNavigate();
    const [params] = useSearchParams();
    const { refresh } = useAuth();
    const next = params.get("next") || "/dashboard";

    async function onSubmit(e: FormEvent<HTMLFormElement>) {
        e.preventDefault();
        setBusy(true);
        setError(null);
        try {
            await api.post("/api/login", { email: email.trim(), password });
            await refresh();
            nav(next, { replace: true });
        } catch (err) {
            if (err instanceof ApiError) {
                const body = err.body as { error?: string } | null;
                setError(body?.error || "Invalid email or password.");
            } else {
                setError("Login failed. Try again.");
            }
        } finally {
            setBusy(false);
        }
    }

    return (
        <div className="min-h-screen flex items-center justify-center bg-secondary/30 px-4">
            <Card className="w-full max-w-sm shadow-lg">
                <CardHeader className="space-y-1">
                    <div className="mx-auto h-11 w-11 rounded-lg bg-primary flex items-center justify-center text-primary-foreground font-bold text-lg">
                        W
                    </div>
                    <CardTitle className="text-center text-xl pt-2">Weekly Dashboard</CardTitle>
                    <p className="text-center text-sm text-muted-foreground">
                        Sign in to continue
                    </p>
                </CardHeader>
                <CardContent>
                    <form onSubmit={onSubmit} className="space-y-4">
                        <div className="space-y-1.5">
                            <Label htmlFor="email">Email</Label>
                            <Input
                                id="email"
                                type="email"
                                autoComplete="email"
                                required
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                            />
                        </div>
                        <div className="space-y-1.5">
                            <Label htmlFor="password">Password</Label>
                            <Input
                                id="password"
                                type="password"
                                autoComplete="current-password"
                                required
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                            />
                        </div>
                        {error && (
                            <p className="text-sm text-destructive">{error}</p>
                        )}
                        <Button type="submit" className="w-full" disabled={busy}>
                            {busy ? "Signing in…" : "Sign in"}
                        </Button>
                        <p className="text-xs text-center text-muted-foreground pt-2">
                            <Link to="/forgot-password" className="hover:underline">
                                Forgot password?
                            </Link>
                        </p>
                    </form>
                </CardContent>
            </Card>
        </div>
    );
}
