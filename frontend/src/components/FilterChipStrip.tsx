import { X } from "lucide-react";

/**
 * Compact summary of which top-of-page filters are currently active.
 *
 * Shows one chip per selected value (or a "N selected" pill once a
 * filter has more than `expandThreshold` items so the strip stays
 * compact).  Each chip has an X to drop that single value; a
 * "Clear all" link wipes every active filter group.
 *
 * The strip auto-hides when no filter is active so it doesn't take
 * up visual real-estate by default.
 */
export interface FilterChipGroup {
    /** Operator-facing label e.g. "Weeks", "Brand", "Category L0". */
    label: string;
    /** Currently selected values for this filter. */
    values: string[];
    /** Remove a single value from the selection. */
    onRemove: (value: string) => void;
    /** Clear every value in this filter group. */
    onClear: () => void;
}

interface FilterChipStripProps {
    filters: FilterChipGroup[];
    /** When a single filter has more than this many values, collapse
     *  to a single "N selected" chip instead of listing each value. */
    expandThreshold?: number;
}

function Chip({
    label,
    onRemove,
}: {
    label: string;
    onRemove: () => void;
}) {
    return (
        <span
            className="inline-flex items-center gap-1 pl-2 pr-1 py-0.5 rounded-md border border-primary/25 bg-primary/5 text-[12px] text-foreground/90"
        >
            <span className="tabular-nums">{label}</span>
            <button
                type="button"
                onClick={onRemove}
                aria-label={`Remove ${label}`}
                className="inline-flex h-4 w-4 items-center justify-center rounded hover:bg-primary/15 opacity-60 hover:opacity-100 transition-opacity"
            >
                <X className="h-3 w-3" />
            </button>
        </span>
    );
}

export function FilterChipStrip({
    filters,
    expandThreshold = 4,
}: FilterChipStripProps) {
    const active = filters.filter((f) => f.values.length > 0);
    if (active.length === 0) return null;

    return (
        <div className="flex flex-wrap items-center gap-1.5 px-3 py-2 rounded-md border border-border bg-muted/30">
            <span
                className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground mr-1"
                style={{ letterSpacing: "0.14em" }}
            >
                Filters
            </span>
            {active.map((f) => {
                // Collapse long lists to a single "N selected" chip that
                // clears the whole group on remove — keeps the strip
                // scannable when the operator picks 8 weeks at once.
                if (f.values.length > expandThreshold) {
                    return (
                        <Chip
                            key={f.label}
                            label={`${f.label}: ${f.values.length} selected`}
                            onRemove={f.onClear}
                        />
                    );
                }
                return f.values.map((v) => (
                    <Chip
                        key={`${f.label}:${v}`}
                        label={`${f.label}: ${v}`}
                        onRemove={() => f.onRemove(v)}
                    />
                ));
            })}
            <button
                type="button"
                onClick={() => active.forEach((f) => f.onClear())}
                className="ml-auto text-[11px] font-medium text-muted-foreground hover:text-foreground hover:underline"
            >
                Clear all
            </button>
        </div>
    );
}
