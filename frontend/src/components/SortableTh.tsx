import type { CSSProperties, ReactNode } from "react";
import { ArrowUp, ArrowDown, ArrowUpDown } from "lucide-react";
import { cn } from "@/lib/utils";
import type { SortState } from "@/lib/useSortedRows";
import { ColumnFilter } from "@/components/ColumnFilter";
import { NumberRangeFilter } from "@/components/NumberRangeFilter";

interface Range { min: number | null; max: number | null }

interface Props {
    sortKey: string;
    label: ReactNode;
    sort: SortState;
    onSort: (key: string) => void;
    align?: "left" | "right" | "center";
    stickyLeft?: number;
    lastFrozen?: boolean;
    minWidth?: number;
    className?: string;
    style?: CSSProperties;
    /** Categorical (checkbox-list) filter — pass all three to enable. */
    filterValues?:   string[];
    filterSelected?: string[];
    onFilterChange?: (next: string[]) => void;
    /** Numeric range filter — pass both to enable; mutually exclusive with categorical. */
    numericRange?:    Range;
    onNumericFilter?: (next: Range) => void;
    numericSuffix?:   string;
    numericPresets?:  number[];
}

export function SortableTh({
    sortKey,
    label,
    sort,
    onSort,
    align = "right",
    stickyLeft,
    lastFrozen,
    minWidth,
    className,
    style,
    filterValues,
    filterSelected,
    onFilterChange,
    numericRange,
    onNumericFilter,
    numericSuffix,
    numericPresets,
}: Props) {
    const active = sort?.key === sortKey;
    const dir = active ? sort!.dir : null;
    const filterable    = filterValues != null && onFilterChange != null;
    const numericMode   = numericRange != null && onNumericFilter != null;
    const filterActive  = (filterable    && (filterSelected?.length ?? 0) > 0)
                        || (numericMode  && (numericRange!.min != null || numericRange!.max != null));

    const mergedStyle: CSSProperties = {
        ...(stickyLeft != null ? { position: "sticky", left: stickyLeft } : null),
        ...(minWidth != null ? { minWidth } : null),
        ...style,
    };

    return (
        <th
            onClick={() => onSort(sortKey)}
            style={mergedStyle}
            className={cn(
                "px-3 py-2 text-[10.5px] font-semibold uppercase tracking-wider text-foreground border-b bg-secondary cursor-pointer select-none whitespace-nowrap hover:text-foreground",
                align === "right"  && "text-right",
                align === "left"   && "text-left",
                align === "center" && "text-center",
                stickyLeft != null && "z-40",
                lastFrozen && "border-r-2 border-r-border",
                filterActive && "bg-primary/5",
                className,
            )}
        >
            <span
                className={cn(
                    "inline-flex items-center gap-1",
                    align === "right"  && "justify-end w-full",
                    align === "center" && "justify-center w-full",
                )}
            >
                {label}
                {dir === "desc" && <ArrowDown   className="h-3 w-3 text-primary" />}
                {dir === "asc"  && <ArrowUp     className="h-3 w-3 text-primary" />}
                {!dir            && <ArrowUpDown className="h-3 w-3 opacity-30" />}
                {filterable && (
                    <ColumnFilter
                        values={filterValues!}
                        selected={filterSelected || []}
                        onChange={onFilterChange!}
                    />
                )}
                {numericMode && (
                    <NumberRangeFilter
                        value={numericRange!}
                        onChange={onNumericFilter!}
                        suffix={numericSuffix}
                        presets={numericPresets}
                    />
                )}
            </span>
        </th>
    );
}
