import { useMemo, useState, type ReactNode } from "react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { ChevronDown, X } from "lucide-react";
import { cn } from "@/lib/utils";

interface MultiPickerProps {
    label: string;
    options: string[];
    selected: string[];
    /** Fires on every checkbox toggle — table updates live. */
    onApply: (selected: string[]) => void;
    placeholder?: string;
    maxLabelItems?: number;
    icon?: ReactNode;
}

/**
 * Top-of-page multi-select with fluent instant-commit UX.
 *
 * - Every checkbox toggle commits immediately (auto-refresh).
 * - Popover stays open while refining — operator can chain selections.
 * - Apply button is now "Done" — closes the popover.  Kept as a fallback
 *   so users who prefer explicit confirm-and-close still have a target.
 * - X badge on the trigger button clears the entire selection in one click.
 * - Search box for long option lists (>10 items).
 */
export function MultiPicker({
    label,
    options,
    selected,
    onApply,
    placeholder = "All",
    maxLabelItems = 3,
}: MultiPickerProps) {
    const [open, setOpen]     = useState(false);
    const [search, setSearch] = useState("");

    const filtered = useMemo(() => {
        const q = search.trim().toLowerCase();
        if (!q) return options;
        return options.filter((o) => o.toLowerCase().includes(q));
    }, [options, search]);

    function display() {
        if (selected.length === 0) return placeholder;
        // Treat "every option selected" the same as the empty placeholder
        // visually — both mean "no filter applied" downstream.
        if (selected.length === options.length && options.length > 0) {
            return `All ${label.toLowerCase()} (${options.length})`;
        }
        if (selected.length === 1) return selected[0];
        if (selected.length <= maxLabelItems) return selected.join(", ");
        return `${selected.length} of ${options.length} ${label.toLowerCase()}`;
    }

    function toggle(v: string) {
        onApply(selected.includes(v) ? selected.filter((x) => x !== v) : [...selected, v]);
    }
    // "All" explicitly checks every option so the operator sees the
    // selection state.  Filter logic treats N selected the same as 0
    // selected when N === options.length, so behaviour is identical.
    function selectAll() { onApply([...options]); }
    function selectVisible() { onApply(Array.from(new Set([...selected, ...filtered]))); }
    function clearVisible()  {
        const fs = new Set(filtered);
        onApply(selected.filter((v) => !fs.has(v)));
    }
    function clearAll() { onApply([]); }

    function onOpenChange(o: boolean) {
        if (o) setSearch("");
        setOpen(o);
    }

    const showSearch = options.length > 10;
    const active = selected.length > 0;

    return (
        <div className="flex flex-col gap-1">
            <span className="text-[11px] font-medium text-muted-foreground uppercase tracking-wider">
                {label}
            </span>
            <div className="inline-flex items-stretch">
                <Popover open={open} onOpenChange={onOpenChange}>
                    <PopoverTrigger asChild>
                        <Button
                            variant="outline"
                            size="sm"
                            className={cn(
                                "justify-between min-w-[160px]",
                                active && "border-primary/40 text-foreground",
                            )}
                        >
                            <span className="truncate">{display()}</span>
                            <ChevronDown className="ml-2 h-3.5 w-3.5 opacity-60" />
                        </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-60 p-0">
                        {showSearch && (
                            <div className="px-2 pt-2 pb-1.5 border-b">
                                <Input
                                    placeholder="Search…"
                                    value={search}
                                    onChange={(e) => setSearch(e.target.value)}
                                    className="h-7 text-xs"
                                    autoFocus
                                />
                            </div>
                        )}
                        <div className="px-3 py-1.5 border-b flex gap-3 text-[11px] font-medium" style={{ color: "#4b5563" }}>
                            <button
                                type="button"
                                onClick={search ? selectVisible : selectAll}
                                className="hover:text-foreground hover:underline"
                            >
                                {search ? "Add visible" : "All"}
                            </button>
                            <button
                                type="button"
                                onClick={search ? clearVisible : clearAll}
                                className="hover:text-foreground hover:underline"
                            >
                                {search ? "Remove visible" : "None"}
                            </button>
                            <span className="ml-auto tabular" style={{ color: "#9ca3af" }}>
                                {selected.length === 0 ? `all (${options.length})` : `${selected.length}/${options.length}`}
                            </span>
                        </div>
                        <div className="max-h-64 overflow-y-auto p-1">
                            {filtered.length === 0 ? (
                                <div className="px-3 py-4 text-xs text-center" style={{ color: "#9ca3af" }}>
                                    No matches
                                </div>
                            ) : filtered.map((opt) => {
                                const isSel = selected.includes(opt);
                                return (
                                    <label
                                        key={opt}
                                        className={cn(
                                            "flex items-center gap-2 cursor-pointer rounded px-2 py-1 hover:bg-accent text-sm",
                                            isSel && "bg-primary/10 font-medium text-foreground"
                                        )}
                                    >
                                        <Checkbox
                                            checked={isSel}
                                            onCheckedChange={() => toggle(opt)}
                                        />
                                        <span className="truncate">{opt}</span>
                                    </label>
                                );
                            })}
                        </div>
                        <div className="border-t p-2">
                            <Button
                                size="sm"
                                variant="outline"
                                className="w-full"
                                onClick={() => setOpen(false)}
                            >
                                Done
                            </Button>
                        </div>
                    </PopoverContent>
                </Popover>
                {active && (
                    <button
                        type="button"
                        aria-label={`Clear ${label} filter`}
                        onClick={(e) => { e.stopPropagation(); onApply([]); }}
                        title="Clear filter"
                        className="inline-flex items-center justify-center w-7 ml-0.5 text-muted-foreground hover:text-primary hover:bg-primary/10 rounded border border-input"
                    >
                        <X className="h-3.5 w-3.5" />
                    </button>
                )}
            </div>
        </div>
    );
}
