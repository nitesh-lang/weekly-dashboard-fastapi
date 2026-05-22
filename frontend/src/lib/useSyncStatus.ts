import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";

interface SnapshotInfo {
    present: boolean;
    mtime: string | null;
    size_bytes?: number;
    latest_week: string | null;
}

interface SyncStatus {
    newest_mtime: string | null;
    snapshots: {
        sales:     SnapshotInfo;
        inventory: SnapshotInfo;
        ams:       SnapshotInfo;
        margin:    SnapshotInfo;
    };
}

/** Format an ISO timestamp as a short relative-time string ("3h ago"). */
export function formatRelative(iso: string | null | undefined): string {
    if (!iso) return "—";
    const t = Date.parse(iso);
    if (isNaN(t)) return "—";
    const seconds = (Date.now() - t) / 1000;
    if (seconds < 60)        return "just now";
    if (seconds < 3600)      return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400)     return `${Math.floor(seconds / 3600)}h ago`;
    if (seconds < 86400 * 7) return `${Math.floor(seconds / 86400)}d ago`;
    return new Date(t).toISOString().slice(0, 10);     // older than a week → date
}

export function useSyncStatus() {
    return useQuery<SyncStatus>({
        queryKey: ["sync-status"],
        queryFn:  () => api.get("/api/sync-status"),
        // Snapshot mtimes change at most once a week, but operator may want
        // to see "just now" refresh after triggering ETL → 5-min stale is
        // a good balance.
        staleTime:        5 * 60_000,
        gcTime:           30 * 60_000,
        refetchInterval:  5 * 60_000,
    });
}
