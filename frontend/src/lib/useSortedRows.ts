import { useMemo, useState } from "react";

export type SortDir = "asc" | "desc";
export type SortState = { key: string; dir: SortDir } | null;

/**
 * Tri-state column sorter for plain row arrays.
 * Click cycle: desc → asc → cleared. Nulls and empty strings always sink
 * to the bottom regardless of direction so blank rows don't pollute the
 * top of either sort order.
 */
export function useSortedRows<T extends Record<string, any>>(
    rows: T[],
    initial: SortState = null,
) {
    const [sort, setSort] = useState<SortState>(initial);

    const sorted = useMemo(() => {
        if (!sort) return rows;
        const { key, dir } = sort;
        const m = dir === "asc" ? 1 : -1;
        return [...rows].sort((a, b) => {
            const av = a[key];
            const bv = b[key];
            const aNil = av == null || av === "";
            const bNil = bv == null || bv === "";
            if (aNil && bNil) return 0;
            if (aNil) return 1;
            if (bNil) return -1;
            if (typeof av === "number" && typeof bv === "number") {
                return (av - bv) * m;
            }
            return String(av).localeCompare(String(bv), undefined, { numeric: true }) * m;
        });
    }, [rows, sort]);

    function onSort(key: string) {
        setSort((cur) => {
            if (!cur || cur.key !== key) return { key, dir: "desc" };
            if (cur.dir === "desc") return { key, dir: "asc" };
            return null;
        });
    }

    return { sorted, sort, onSort, setSort };
}
