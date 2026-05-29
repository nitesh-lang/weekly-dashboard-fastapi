/**
 * Shared text-filter for any row table.
 *
 * Semantics:
 *  - Query is tokenised on whitespace.  Every token must match somewhere
 *    in the row, in SOME of the searchable fields.  Multiple tokens are
 *    AND-combined ("Tonor G11" requires both words to appear).
 *  - Match is case-insensitive substring AFTER normalising both sides:
 *    hyphens and spaces are stripped so "ET01BK" matches "ET-01-BK" and
 *    "ET 01 BK" matches "ET-01-BK".  Product codes routinely vary across
 *    sources, so this is the convention.
 *  - Only the fields you pass in are searched.  This avoids the
 *    InventoryDashboard-style bug where searching "100" matched every row
 *    with a 100 in any numeric column.
 *
 * Returns the filter function so callers can use it inside .filter() inside
 * a useMemo with their own deps.
 */

/** Strips chars that legitimately vary across exports of the same SKU/ASIN. */
function normalise(s: unknown): string {
    if (s == null) return "";
    return String(s).toLowerCase().replace(/[-\s_/]/g, "");
}

/**
 * Build a row predicate from a search query.  Empty/whitespace query passes
 * everything.  Use inside row.filter() / useMemo.
 *
 * @example
 *   const match = makeTextFilter(filter, ["brand","model","sku","asin"]);
 *   const filtered = rows.filter(match);
 */
export function makeTextFilter<T extends Record<string, any>>(
    query: string | null | undefined,
    fields: (keyof T)[],
): (row: T) => boolean {
    const q = (query ?? "").trim();
    if (!q) return () => true;
    const tokens = q.split(/\s+/).map(normalise).filter(Boolean);
    if (tokens.length === 0) return () => true;

    return (row: T) => {
        // Build the normalised blob once per row from only the selected fields.
        let blob = "";
        for (const f of fields) {
            const v = row[f];
            if (v != null && v !== "") blob += normalise(v);
        }
        // Every token must appear in the blob.
        for (const t of tokens) {
            if (!blob.includes(t)) return false;
        }
        return true;
    };
}
