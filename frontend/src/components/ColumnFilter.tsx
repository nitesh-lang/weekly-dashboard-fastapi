import { useMemo, useState } from "react";
import { Filter, X } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

interface Props {
    /** Unique values available for this column. */
    values:   string[];
    /** Currently selected values; empty array = "no filter, show all". */
    selected: string[];
    /** Called on every toggle — commits instantly. Empty array = clear. */
    onChange: (next: string[]) => void;
}

/**
 * Excel-style per-column filter dropdown with FLUENT instant-commit UX.
 *
 * - Funnel icon: faint when inactive, oxblood + filled when a filter is set.
 * - Click funnel → popover opens.
 * - Every checkbox toggle commits to the URL immediately; the table updates
 *   live without an Apply button.  Popover stays open so the operator can
 *   refine selections in one fluent motion.
 * - Search box filters the visible value list (does not commit).
 * - "All" toggles every value at once; "None" clears within this column.
 * - When a filter is active, a small × badge appears beside the funnel —
 *   one click clears it without ever opening the popover.
 * - Click outside or press Esc to close.
 */
export function ColumnFilter({ values, selected, onChange }: Props) {
    const [open, setOpen]     = useState(false);
    const [search, setSearch] = useState("");

    const active = selected.length > 0 && selected.length < values.length;

    const filtered = useMemo(() => {
        const q = search.trim().toLowerCase();
        if (!q) return values;
        return values.filter((v) => v.toLowerCase().includes(q));
    }, [values, search]);

    function toggle(v: string) {
        // Direct commit — no draft state.
        onChange(selected.includes(v) ? selected.filter((x) => x !== v) : [...selected, v]);
    }
    function selectAll()  { onChange([]); }       // empty = "all", semantically
    function clearAll()   { onChange([]); }       // user-facing: same outcome
    function selectVisible() { onChange([...new Set([...selected, ...filtered])]); }
    function clearVisible() {
        const fs = new Set(filtered);
        onChange(selected.filter((v) => !fs.has(v)));
    }

    function onOpenChange(o: boolean) {
        if (o) setSearch("");
        setOpen(o);
    }

    return (
        <span className="inline-flex items-center">
            <Popover open={open} onOpenChange={onOpenChange}>
                <PopoverTrigger asChild>
                    <button
                        type="button"
                        aria-label="Filter column"
                        onClick={(e) => e.stopPropagation()}
                        className={cn(
                            "inline-flex items-center justify-center w-5 h-5 ml-1 rounded transition-colors",
                            active
                                ? "text-primary"
                                : "text-muted-foreground/50 hover:text-foreground",
                        )}
                    >
                        <Filter className="h-3.5 w-3.5" fill={active ? "currentColor" : "none"} />
                    </button>
                </PopoverTrigger>
                <PopoverContent
                    className="w-64 p-0"
                    align="start"
                    onClick={(e) => e.stopPropagation()}
                >
                    <div className="px-2 pt-2 pb-1.5 border-b">
                        <Input
                            placeholder="Search values…"
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            className="h-7 text-xs"
                            autoFocus
                        />
                    </div>
                    <div className="px-3 py-1.5 border-b flex gap-3 text-[11px] font-medium" style={{ color: "#4b5563" }}>
                        <button type="button" onClick={search ? selectVisible : selectAll}
                                className="hover:text-foreground hover:underline">
                            {search ? "Add visible" : "All"}
                        </button>
                        <button type="button" onClick={search ? clearVisible : clearAll}
                                className="hover:text-foreground hover:underline">
                            {search ? "Remove visible" : "None"}
                        </button>
                        <span className="ml-auto tabular" style={{ color: "#9ca3af" }}>
                            {selected.length === 0 ? `all (${values.length})` : `${selected.length}/${values.length}`}
                        </span>
                    </div>
                    <div className="max-h-60 overflow-y-auto py-1">
                        {filtered.length === 0 ? (
                            <div className="px-3 py-4 text-xs text-center" style={{ color: "#9ca3af" }}>
                                No matches
                            </div>
                        ) : filtered.map((v) => (
                            <label
                                key={v}
                                className="flex items-center gap-2 px-3 py-1.5 hover:bg-muted text-[12.5px] cursor-pointer select-none"
                            >
                                <Checkbox
                                    checked={selected.includes(v)}
                                    onCheckedChange={() => toggle(v)}
                                />
                                <span className="truncate">{v || "(blank)"}</span>
                            </label>
                        ))}
                    </div>
                </PopoverContent>
            </Popover>
            {active && (
                <button
                    type="button"
                    aria-label="Clear column filter"
                    onClick={(e) => { e.stopPropagation(); onChange([]); }}
                    title="Clear filter"
                    className="inline-flex items-center justify-center w-4 h-4 ml-0.5 rounded text-primary/70 hover:text-primary hover:bg-primary/10"
                >
                    <X className="h-3 w-3" />
                </button>
            )}
        </span>
    );
}
