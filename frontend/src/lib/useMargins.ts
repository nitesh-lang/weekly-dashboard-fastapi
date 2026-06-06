/**
 * useMargins() — one fetch of /api/margins, returns a hook that exposes
 * the full row list + a `lookup(brand, model)` helper.
 *
 * Model is the canonical join key (operator confirmed: "model is master").
 * SKU/ASIN are not used for merging — only displayed alongside.
 *
 * Brands without margin files (e.g., Fossil) return `null` from lookup
 * — callers should treat that as "no data" not "zero margin".
 */
import { useCallback, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";

export interface MarginRow {
    brand: string;
    sku: string | null;
    asin: string | null;
    model: string | null;
    category_l1: string | null;
    category_l2: string | null;
    product_name: string | null;
    latest_fob: number | null;
    nlc: number | null;
    dp: number | null;
    mrp: number | null;
    bau_deal_sp: number | null;
    bau_sp_mop: number | null;
    gross_margin: number | null;
    gross_margin_pct: number | null;
    net_margin: number | null;
    net_margin_pct: number | null;
    // Inventory bifurcation — joined by ASIN in margin_snapshot ETL.
    //   total_stock  = every channel summed (latest week inv snapshot)
    //   soh          = AMPM + B2B-AMPM warehouse (matches AMS Planning)
    //   am_soh       = sellable stock already at Amazon FBA (inbound)
    //   am_intransit = warehouse → Amazon shipments in flight (inbound)
    total_stock: number | null;
    soh: number | null;
    am_soh: number | null;
    am_intransit: number | null;
}

interface MarginPayload {
    rows: MarginRow[];
    brands: string[];
    row_count: number;
    available: boolean;
}

function key(brand: string | null | undefined, model: string | null | undefined): string {
    return `${(brand || "").trim().toLowerCase()}|${(model || "").trim().toLowerCase()}`;
}

export function useMargins() {
    const q = useQuery<MarginPayload>({
        queryKey: ["margins"],
        queryFn: () => api.get("/api/margins"),
        staleTime: 30 * 60_000,    // margins refresh weekly; 30-min stale window is generous
    });

    // Pre-build the lookup map once per response.  Keyed by `brand|model`.
    const byModel = useMemo(() => {
        const m = new Map<string, MarginRow>();
        for (const r of q.data?.rows || []) {
            m.set(key(r.brand, r.model), r);
        }
        return m;
    }, [q.data]);

    // Secondary index keyed by model alone (no brand).  Dashboard SKU rows
    // may not carry brand explicitly — operator confirmed "model is master".
    // First-match wins if a model name appears under multiple brands.
    const byModelOnly = useMemo(() => {
        const m = new Map<string, MarginRow>();
        for (const r of q.data?.rows || []) {
            const k = (r.model || "").trim().toLowerCase();
            if (!k) continue;
            if (!m.has(k)) m.set(k, r);
        }
        return m;
    }, [q.data]);

    // useCallback so consumers can use these as useMemo / useEffect deps
    // without re-running on every parent render.  Without this, the SKU
    // table's enrichedRows useMemo was re-computing 600 rows × hash lookup
    // on every Dashboard render — which cascades into uniqueValues, filtered,
    // and TanStack's table state.  Made click→sort feel 15-20s.
    const lookup = useCallback(
        (brand: string | undefined | null, model: string | undefined | null): MarginRow | null => {
            if (!brand || !model) return null;
            return byModel.get(key(brand, model)) || null;
        },
        [byModel],
    );

    const lookupByModel = useCallback(
        (model: string | undefined | null): MarginRow | null => {
            if (!model) return null;
            return byModelOnly.get(model.trim().toLowerCase()) || null;
        },
        [byModelOnly],
    );

    return {
        rows: q.data?.rows || [],
        brands: q.data?.brands || [],
        available: q.data?.available ?? false,
        isLoading: q.isLoading,
        error: q.error,
        refetch: q.refetch,
        lookup,
        lookupByModel,
    };
}
