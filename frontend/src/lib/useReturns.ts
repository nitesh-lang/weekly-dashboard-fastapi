import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";

export interface ReturnsRow {
    brand: string;
    model: string;
    sku: string;
    asin: string;
    category_l0: string;
    category_l1: string;
    return_units: number;
    return_value: number | null;
    sellable_units: number;
    unsellable_units: number;
    sellable_pct: number;
    units_sold_30d: number;
    return_pct: number | null;   // null when no sales in window
    top_reason: string;
    last_return_at: string;
    source_files: string;
}

interface ReturnsPayload {
    rows: ReturnsRow[];
    brands: string[];
    row_count: number;
    available: boolean;
}

export function useReturns() {
    const q = useQuery<ReturnsPayload>({
        queryKey: ["returns"],
        queryFn:  () => api.get("/api/returns"),
        staleTime: 30 * 60_000,
    });

    // Indices for fast lookup.  ASIN is the canonical key.  SKU is a
    // fallback for tables that only carry SKU (Dashboard SKU table).
    const { byAsin, bySku } = useMemo(() => {
        const ba = new Map<string, ReturnsRow>();
        const bs = new Map<string, ReturnsRow>();
        for (const r of q.data?.rows || []) {
            if (r.asin) ba.set(r.asin.trim().toLowerCase(), r);
            if (r.sku)  bs.set(r.sku.trim().toLowerCase(), r);
        }
        return { byAsin: ba, bySku: bs };
    }, [q.data]);

    function lookupByAsin(asin: string | undefined | null): ReturnsRow | null {
        if (!asin) return null;
        return byAsin.get(asin.trim().toLowerCase()) || null;
    }
    function lookupBySku(sku: string | undefined | null): ReturnsRow | null {
        if (!sku) return null;
        return bySku.get(sku.trim().toLowerCase()) || null;
    }

    return {
        rows:      q.data?.rows || [],
        brands:    q.data?.brands || [],
        available: q.data?.available ?? false,
        isLoading: q.isLoading,
        error:     q.error,
        refetch:   q.refetch,
        lookupByAsin,
        lookupBySku,
    };
}
