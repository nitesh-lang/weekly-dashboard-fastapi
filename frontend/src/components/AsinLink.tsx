/**
 * AsinLink — renders an ASIN string as a clickable link to its Amazon.in
 * product page (https://www.amazon.in/dp/{ASIN}).
 *
 * - Opens in a new tab so the operator never loses their dashboard view.
 * - `rel="noopener noreferrer"` for security (prevents the opened tab from
 *   reaching back into our window via `window.opener`).
 * - `stopPropagation` on click so it doesn't trigger row-level handlers
 *   (sort, etc.) that may wrap the cell.
 * - Falls back to "—" when ASIN is missing/empty.
 */
interface Props {
    asin: string | null | undefined;
    /** Optional className passed through, e.g. for tabular-nums or color overrides. */
    className?: string;
}

export function AsinLink({ asin, className }: Props) {
    const a = (asin || "").trim();
    if (!a) return <span style={{ color: "#9ca3af" }}>—</span>;
    return (
        <a
            href={`https://www.amazon.in/dp/${encodeURIComponent(a)}`}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => e.stopPropagation()}
            className={className}
            style={{
                color: "inherit",
                textDecoration: "none",
                borderBottom: "1px dotted transparent",
                transition: "border-color 0.12s ease, color 0.12s ease",
            }}
            onMouseEnter={(e) => {
                (e.currentTarget as HTMLAnchorElement).style.color = "#1e40af";
                (e.currentTarget as HTMLAnchorElement).style.borderBottomColor = "#1e40af";
            }}
            onMouseLeave={(e) => {
                (e.currentTarget as HTMLAnchorElement).style.color = "inherit";
                (e.currentTarget as HTMLAnchorElement).style.borderBottomColor = "transparent";
            }}
            title={`Open ${a} on Amazon.in`}
        >
            {a}
        </a>
    );
}
