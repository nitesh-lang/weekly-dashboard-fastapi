import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";

export interface PlanningRow {
    sku: string;
    brand: string;
    model: string;
    bau: number | string | null;
    asin: string;
    category_l0: string;
    category_l1: string;
    asin_type: string;
    variation: string;
    variation_asins: string;
    soh: number;
    inventory_status: "Out of Stock" | "Low" | "Healthy" | "Overstocked" | string;
    ams_required: string;
    remarks: string;
}

interface PlanningPayload {
    rows: PlanningRow[];
    brands: string[];
    row_count: number;
    available: boolean;
}

export function useAmsPlanning() {
    const qc = useQuery<PlanningPayload>({
        queryKey: ["ams-planning"],
        queryFn:  () => api.get("/api/ams-planning"),
        staleTime: 60_000,
    });

    const queryClient = useQueryClient();

    // Note upsert.  Optimistically patches the cache so the cell stays
    // populated with the new value while the request is in flight.
    const saveNote = useMutation<unknown, Error, { sku: string; ams_required?: string; remarks?: string }>({
        mutationFn: (body) => api.post("/api/ams-planning/notes", body),
        onMutate: async (body) => {
            await queryClient.cancelQueries({ queryKey: ["ams-planning"] });
            const previous = queryClient.getQueryData<PlanningPayload>(["ams-planning"]);
            if (previous) {
                queryClient.setQueryData<PlanningPayload>(["ams-planning"], {
                    ...previous,
                    rows: previous.rows.map((r) =>
                        r.sku === body.sku
                            ? {
                                  ...r,
                                  ams_required: body.ams_required ?? r.ams_required,
                                  remarks:      body.remarks      ?? r.remarks,
                              }
                            : r,
                    ),
                });
            }
            return { previous };
        },
        onError: (_err, _body, ctx: any) => {
            if (ctx?.previous) queryClient.setQueryData(["ams-planning"], ctx.previous);
        },
        // Don't refetch on success — server response is identical to the optimistic patch.
    });

    return {
        rows:      qc.data?.rows || [],
        brands:    qc.data?.brands || [],
        available: qc.data?.available ?? false,
        isLoading: qc.isLoading,
        error:     qc.error,
        refetch:   qc.refetch,
        saveNote:  saveNote.mutate,
        saving:    saveNote.isPending,
    };
}
