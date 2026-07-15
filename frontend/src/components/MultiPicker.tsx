import { useEffect, useMemo, useState, type ReactNode } from "react";
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
    /** Fires ONCE when the popover closes — one API refetch per session, not per click. */
    onApply: (selected: string[]) => void;
    placeholder?: string;
    maxLabelItems?: number;
    icon?: ReactNode;
}

/**
 * Top-of-page multi-select with STAGED-COMMIT UX.
 *
 * - Checkbox toggles update local `staged` state only — no refetch per click.
 * - Popover close (Done, click outside, or trigger click) commits the
 *   staged selection via onApply, if different from the committed value.
 * - X badge on the trigger button clears the committed selection in one
 *   click (fast path, no popover open).
 * - Search box for long option lists (>10 items).
 *
 * Rationale: prior "commit-on-every-toggle" mode caused a burst of
 * sequential API refetches (weekly_dashboard-fastapi backend) whenever
 * the operator picked N weeks — each toggle triggered a URL param
 * change → React Query refetch.  Reported UX: "week selector is very
 * slow, have to do one by one".  Staged commit means picking N weeks
 * is one API call after the popover closes.
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
    // Staged (draft) selections — mutated by toggle/All/None; committed
    // to the parent via onApply when the popover closes.
    const [staged, setStaged] = useState<string[]>(selected);

    // Keep `staged` in sync with `selected` prop changes coming FROM
    // the parent (URL param update, external reset, another picker
    // triggering a re-render).  Only sync while the popover is closed
    // so we don't clobber the operator's in-progress selection.
    useEffect(() => {
        if (!open) setStaged(selected);
    }, [selected, open]);

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
        setStaged((prev) => prev.includes(v) ? prev.filter((x) => x !== v) : [...prev, v]);
    }
    function selectAll()      { setStaged([...options]); }
    function selectVisible()  { setStaged((prev) => Array.from(new Set([...prev, ...filtered]))); }
    function clearVisibleStg(){ const fs = new Set(filtered); setStaged((prev) => prev.filter((v) => !fs.has(v))); }
    function clearAllStaged() { setStaged([]); }

    // Commit staged -> onApply only if it actually differs from `selected`.
    // Sameness check ignores order so re-open+re-close of an unchanged
    // popover doesn't trigger a spurious refetch.
    function commitIfChanged() {
        if (staged.length !== selected.length) { onApply(staged); return; }
        const sSel = new Set(selected);
        for (const s of staged) if (!sSel.has(s)) { onApply(staged); return; }
        // No change — do nothing.
    }

    function onOpenChange(o: boolean) {
        if (o) {
            // Popover opening — reset search + reseed staged from current
            // committed selection (in case parent updated between opens).
            setSearch("");
            setStaged(selected);
        } else {
            // Popover closing — commit staged.
            commitIfChanged();
        }
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
                                onClick={search ? clearVisibleStg : clearAllStaged}
                                className="hover:text-foreground hover:underline"
                            >
                                {search ? "Remove visible" : "None"}
                            </button>
                            <span className="ml-auto tabular" style={{ color: "#9ca3af" }}>
                                {staged.length === 0 ? `all (${options.length})` : `${staged.length}/${options.length}`}
                            </span>
                        </div>
                        <div className="max-h-64 overflow-y-auto p-1">
                            {filtered.length === 0 ? (
                                <div className="px-3 py-4 text-xs text-center" style={{ color: "#9ca3af" }}>
                                    No matches
                                </div>
                            ) : filtered.map((opt) => {
                                const isSel = staged.includes(opt);
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
                        <div className="border-t p-2 flex gap-2">
                            <Button
                                size="sm"
                                variant="ghost"
                                className="flex-1"
                                onClick={() => { setStaged(selected); setOpen(false); }}
                                title="Discard staged changes"
                            >
                                Cancel
                            </Button>
                            <Button
                                size="sm"
                                variant="default"
                                className="flex-1"
                                onClick={() => setOpen(false)}
                            >
                                Apply
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
