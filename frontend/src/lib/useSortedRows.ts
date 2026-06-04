import { useMemo, useState } from "react";

export type SortDir = "asc" | "desc";
export type SortState = { key: string; dir: SortDir } | null;

/**
 * Two-state column sorter for plain row arrays.
 * Click cycle: desc ↔ asc.  The previous tri-state cycle included a
 * "cleared" third click — operators kept hitting it by accident,
 * sending rows back to the raw API order (which looked like sort
 * was broken).  Two-state toggling matches Excel / shadcn expectations.
 *
 * Nulls and empty strings always sink to the bottom regardless of
 * direction so blank rows don't pollute the top of either sort order.
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
            // NaN counts as nil too — pandas-derived rows occasionally
            // sneak NaN through when a percentage column has no sales
            // denominator (return_pct, conversion_pct, etc.).
            const aNil = av == null || av === "" || (typeof av === "number" && Number.isNaN(av));
            const bNil = bv == null || bv === "" || (typeof bv === "number" && Number.isNaN(bv));
            if (aNil && bNil) return 0;
            if (aNil) return 1;
            if (bNil) return -1;
            // Coerce numeric-looking strings ("5.2", "5.2%") to numbers so
            // they sort with real numbers and not lexicographically.
            const an = typeof av === "number"
                ? av
                : (typeof av === "string" ? parseFloat(av.replace(/[%,\s]/g, "")) : Number.NaN);
            const bn = typeof bv === "number"
                ? bv
                : (typeof bv === "string" ? parseFloat(bv.replace(/[%,\s]/g, "")) : Number.NaN);
            if (!Number.isNaN(an) && !Number.isNaN(bn)) {
                return (an - bn) * m;
            }
            return String(av).localeCompare(String(bv), undefined, { numeric: true }) * m;
        });
    }, [rows, sort]);

    function onSort(key: string) {
        setSort((cur) => {
            if (!cur || cur.key !== key) return { key, dir: "desc" };
            // Two-state toggle — never clears on the third click.
            return { key, dir: cur.dir === "desc" ? "asc" : "desc" };
        });
    }

    return { sorted, sort, onSort, setSort };
}
