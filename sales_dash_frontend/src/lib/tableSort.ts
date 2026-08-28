import { useMemo, useState } from "react";

export type SortOrder = "asc" | "desc";

export interface SortState<K extends string> {
  key: K | null;
  order: SortOrder;
}

/**
 * Local table sort state with a 3-state toggle: asc → desc → cleared.
 * Extractor is optional; defaults to `row[key]` (works for flat rows).
 */
export function useTableSort<T, K extends string>(
  data: T[] | undefined,
  opts: {
    initialKey?: K;
    initialOrder?: SortOrder;
    extractors?: Partial<Record<K, (row: T) => unknown>>;
  } = {}
) {
  const { initialKey, initialOrder = "asc", extractors } = opts;
  const [sort, setSort] = useState<SortState<K>>({
    key: initialKey ?? null,
    order: initialOrder,
  });

  const toggle = (key: K) => {
    setSort((prev) => {
      if (prev.key !== key) return { key, order: "asc" };
      if (prev.order === "asc") return { key, order: "desc" };
      return { key: null, order: "asc" };
    });
  };

  const sorted = useMemo(() => {
    if (!data || !sort.key) return data;
    const k = sort.key;
    const extract = extractors?.[k];
    const arr = [...data];
    arr.sort((a, b) => {
      const av = extract ? extract(a) : (a as Record<string, unknown>)[k];
      const bv = extract ? extract(b) : (b as Record<string, unknown>)[k];
      // nulls last
      const aN = av === null || av === undefined || av === "";
      const bN = bv === null || bv === undefined || bv === "";
      if (aN && bN) return 0;
      if (aN) return 1;
      if (bN) return -1;
      const an = typeof av === "number" ? av : Number(av);
      const bn = typeof bv === "number" ? bv : Number(bv);
      if (!Number.isNaN(an) && !Number.isNaN(bn) && typeof av !== "string" && typeof bv !== "string") {
        return sort.order === "asc" ? an - bn : bn - an;
      }
      const sa = String(av).toLowerCase();
      const sb = String(bv).toLowerCase();
      return sort.order === "asc" ? sa.localeCompare(sb) : sb.localeCompare(sa);
    });
    return arr;
  }, [data, sort, extractors]);

  return { sorted, sort, toggle };
}

/**
 * Token-based fuzzy text filter. Every whitespace-separated token in the
 * query must appear (in any order, case-insensitive) in the concatenated
 * `fields`. Hyphens are stripped from both haystack and query so
 * "FBA-756" matches "FBA75683".
 */
export function makeTextFilter<T>(query: string, fields: (keyof T)[]) {
  const q = query.trim().toLowerCase().replace(/-/g, "");
  if (!q) return () => true;
  const tokens = q.split(/\s+/).filter(Boolean);
  return (row: T) => {
    const hay = fields
      .map((f) => String(row[f] ?? ""))
      .join(" ")
      .toLowerCase()
      .replace(/-/g, "");
    return tokens.every((t) => hay.includes(t));
  };
}
