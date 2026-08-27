import { useMemo, useState } from "react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Check, ChevronDown, X } from "lucide-react";
import { cn } from "@/lib/utils";

export interface PickerOption {
    value: string;
    label: string;
}

export interface PickerGroup {
    /** Section header inside the popover, e.g. "Quick ranges". Omit for flat lists. */
    label?: string;
    options: PickerOption[];
}

interface SinglePickerProps {
    label: string;
    /** Option sections. Pass one unnamed group for a flat list. */
    groups: PickerGroup[];
    value: string;
    onChange: (v: string) => void;
    /** The sentinel meaning "no filter". Selecting it (or the clear X) resets. */
    allValue?: string;
    /** Trigger text when value === allValue, e.g. "All brands". */
    allLabel?: string;
    className?: string;
}

/**
 * Single-select sibling of MultiPicker — same trigger and popover anatomy
 * (outline button + chevron, search box on long lists, X fast-clear) so the
 * two read as one control family on a filter bar.  Single-select commits on
 * click and closes immediately: no staged Apply/Cancel needed, because one
 * click is one choice is one refetch.
 */
export function SinglePicker({
    label,
    groups,
    value,
    onChange,
    allValue = "all",
    allLabel = "All",
    className,
}: SinglePickerProps) {
    const [open, setOpen] = useState(false);
    const [search, setSearch] = useState("");

    const total = useMemo(
        () => groups.reduce((n, g) => n + g.options.length, 0),
        [groups],
    );

    const visible = useMemo(() => {
        const q = search.trim().toLowerCase();
        if (!q) return groups;
        return groups
            .map((g) => ({ ...g, options: g.options.filter((o) => o.label.toLowerCase().includes(q)) }))
            .filter((g) => g.options.length > 0);
    }, [groups, search]);

    const active = value !== allValue;
    const current = useMemo(() => {
        for (const g of groups) {
            const hit = g.options.find((o) => o.value === value);
            if (hit) return hit.label;
        }
        return allLabel;
    }, [groups, value, allLabel]);

    function pick(v: string) {
        if (v !== value) onChange(v);
        setOpen(false);
    }

    function onOpenChange(o: boolean) {
        if (o) setSearch("");
        setOpen(o);
    }

    const showSearch = total > 10;

    // Plain render helper, NOT a nested component: a component type defined
    // inside the render body is new every render, so React would unmount and
    // remount every option row per keystroke in the search box — destroying
    // keyboard focus and redoing full mount work on long model lists.
    const row = (opt: PickerOption) => {
        const isSel = opt.value === value;
        return (
            <button
                key={opt.value}
                type="button"
                onClick={() => pick(opt.value)}
                className={cn(
                    "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm hover:bg-accent",
                    isSel && "bg-primary/10 font-medium text-foreground",
                )}
            >
                <Check className={cn("h-3.5 w-3.5 shrink-0", isSel ? "opacity-100 text-primary" : "opacity-0")} />
                <span className="truncate">{opt.label}</span>
            </button>
        );
    };

    return (
        <div className={cn("flex flex-col gap-1", className)}>
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
                                "justify-between min-w-[150px] max-w-[220px]",
                                active && "border-primary/40 text-foreground",
                            )}
                        >
                            <span className="truncate">{active ? current : allLabel}</span>
                            <ChevronDown className="ml-2 h-3.5 w-3.5 shrink-0 opacity-60" />
                        </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-64 p-0" align="start">
                        {showSearch && (
                            <div className="px-2 pt-2 pb-1.5 border-b">
                                <Input
                                    placeholder={`Search ${label.toLowerCase()}…`}
                                    value={search}
                                    onChange={(e) => setSearch(e.target.value)}
                                    className="h-7 text-xs"
                                    autoFocus
                                />
                            </div>
                        )}
                        <div className="max-h-72 overflow-y-auto p-1">
                            {!search && row({ value: allValue, label: allLabel })}
                            {visible.length === 0 ? (
                                <div className="px-3 py-4 text-xs text-center" style={{ color: "#9ca3af" }}>
                                    No matches
                                </div>
                            ) : visible.map((g, gi) => (
                                <div key={g.label ?? gi}>
                                    {g.label && (
                                        <div className="px-2 pt-2 pb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                                            {g.label}
                                        </div>
                                    )}
                                    {g.options.map(row)}
                                </div>
                            ))}
                        </div>
                    </PopoverContent>
                </Popover>
                {active && (
                    <button
                        type="button"
                        aria-label={`Clear ${label} filter`}
                        onClick={() => onChange(allValue)}
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
