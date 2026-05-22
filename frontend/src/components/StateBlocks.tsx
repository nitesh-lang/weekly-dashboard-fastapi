import type { ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { AlertOctagon, RefreshCw } from "lucide-react";

/* ─────────────────────────────────────────────────────────
   Reusable loading + error + empty state primitives.
   Drop into any page that uses React Query.
   ───────────────────────────────────────────────────────── */

interface LoadingProps {
    rows?: number;       // how many skeleton rows to render
    showKpis?: boolean;  // also render a 4-card KPI skeleton row above
}

/** Animated shimmering placeholder — replaces `<div>Loading…</div>`. */
export function LoadingSkeleton({ rows = 6, showKpis = true }: LoadingProps) {
    return (
        <div className="space-y-4" aria-label="Loading content">
            {showKpis && (
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                    {[0, 1, 2, 3].map((i) => (
                        <div key={i} className="rounded-lg border border-border p-4" style={{ background: "#fff" }}>
                            <div className="skeleton skeleton-line mb-3" style={{ width: "55%", height: 11 }} />
                            <div className="skeleton" style={{ height: 28, width: "70%", borderRadius: 4 }} />
                        </div>
                    ))}
                </div>
            )}
            <div className="rounded-lg border border-border" style={{ background: "#fff" }}>
                <div className="px-4 py-3 border-b flex items-center gap-3">
                    <div className="skeleton" style={{ height: 24, width: 24, borderRadius: 6 }} />
                    <div className="skeleton skeleton-line" style={{ width: 160 }} />
                </div>
                <div className="p-4 space-y-2">
                    {Array.from({ length: rows }).map((_, i) => (
                        <div key={i} className="skeleton skeleton-row" style={{ opacity: 1 - i * 0.05 }} />
                    ))}
                </div>
            </div>
        </div>
    );
}

interface ErrorProps {
    error: unknown;
    onRetry?: () => void;
    title?: string;
}

/** Designed error card with retry button.  `error` may be the React Query
 *  Error object or any thrown value. */
export function ErrorBlock({ error, onRetry, title = "Couldn't load this view" }: ErrorProps) {
    const message = error instanceof Error
        ? error.message
        : (typeof error === "string" ? error : "Unknown error");
    const isUnauth = /unauthorized|401/i.test(message);
    return (
        <div
            className="rounded-lg p-6 flex items-start gap-4"
            style={{
                background: "linear-gradient(180deg, rgba(185,28,28,0.04) 0%, rgba(185,28,28,0.01) 100%)",
                border: "1px solid rgba(185,28,28,0.22)",
            }}
        >
            <div
                className="shrink-0 inline-flex items-center justify-center h-10 w-10 rounded-lg"
                style={{ background: "rgba(185,28,28,0.10)" }}
            >
                <AlertOctagon className="h-5 w-5" style={{ color: "#b91c1c" }} strokeWidth={2} />
            </div>
            <div className="min-w-0 flex-1">
                <div className="text-[15px] font-semibold tracking-tight" style={{ color: "#7f1d1d" }}>{title}</div>
                <div className="text-[13px] mt-1" style={{ color: "#9b2c2c" }}>
                    {isUnauth
                        ? "Your session expired. Sign in again to continue."
                        : message}
                </div>
                {onRetry && !isUnauth && (
                    <Button
                        variant="outline"
                        size="sm"
                        className="mt-3"
                        onClick={onRetry}
                    >
                        <RefreshCw className="h-3.5 w-3.5" />
                        Try again
                    </Button>
                )}
                {isUnauth && (
                    <Button
                        variant="outline"
                        size="sm"
                        className="mt-3"
                        onClick={() => { window.location.href = "/login"; }}
                    >
                        Sign in
                    </Button>
                )}
            </div>
        </div>
    );
}

interface EmptyProps {
    icon?: ReactNode;
    title?: string;
    hint?: ReactNode;
    action?: ReactNode;
}

/** Designed empty state — used when filters narrow to zero rows.  Each
 *  page can supply its own copy via the props. */
export function EmptyBlock({ icon, title = "No results", hint, action }: EmptyProps) {
    return (
        <div className="p-12 text-center flex flex-col items-center gap-3">
            {icon && (
                <div
                    className="inline-flex items-center justify-center h-12 w-12 rounded-lg mb-1"
                    style={{ background: "#f4f4f5", color: "#6b7280" }}
                >
                    {icon}
                </div>
            )}
            <div className="text-[15px] font-semibold tracking-tight" style={{ color: "#0a0a0a" }}>{title}</div>
            {hint && <div className="text-[13px]" style={{ color: "#6b7280", maxWidth: 480 }}>{hint}</div>}
            {action && <div className="mt-2">{action}</div>}
        </div>
    );
}
