import { memo, useEffect, useState } from "react";
import { Filter, X } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

interface Range {
    min: number | null;
    max: number | null;
}

interface Props {
    value:    Range;
    onChange: (next: Range) => void;
    /** Trailing label, e.g. "%" — purely decorative in the popover header. */
    suffix?: string;
    /** Quick-pick thresholds shown as one-click buttons (interpreted as ≥). */
    presets?: number[];
}

/**
 * Excel-style "Number Filters" with fluent instant-commit UX.
 *
 * - Funnel icon (filled + primary when active).
 * - Click → popover with From / To inputs and preset chips.
 * - Inputs commit on blur OR Enter — no Apply button required.
 * - Preset chips commit instantly and keep the popover open for refinement.
 * - When a filter is active, a small × badge appears beside the funnel —
 *   one click clears it without opening the popover.
 */
function NumberRangeFilterImpl({ value, onChange, suffix, presets = [1, 5, 10] }: Props) {
    const [open, setOpen]         = useState(false);
    const [draftMin, setDraftMin] = useState<string>(value.min != null ? String(value.min) : "");
    const [draftMax, setDraftMax] = useState<string>(value.max != null ? String(value.max) : "");

    // Re-sync drafts when external value changes (e.g., URL-driven reset).
    useEffect(() => {
        setDraftMin(value.min != null ? String(value.min) : "");
        setDraftMax(value.max != null ? String(value.max) : "");
    }, [value.min, value.max]);

    const active = value.min != null || value.max != null;

    function parse(s: string): number | null {
        const t = s.trim();
        if (t === "") return null;
        const n = Number(t);
        return Number.isFinite(n) ? n : null;
    }
    function commitMin() {
        const n = parse(draftMin);
        if (n !== value.min) onChange({ ...value, min: n });
    }
    function commitMax() {
        const n = parse(draftMax);
        if (n !== value.max) onChange({ ...value, max: n });
    }
    function clear() {
        setDraftMin(""); setDraftMax("");
        onChange({ min: null, max: null });
    }
    function preset(min: number) {
        setDraftMin(String(min));
        setDraftMax("");
        onChange({ min, max: null });
    }
    function onOpenChange(o: boolean) {
        if (!o) {
            // Force a commit on close in case user typed but didn't blur.
            const m = parse(draftMin);
            const M = parse(draftMax);
            if (m !== value.min || M !== value.max) onChange({ min: m, max: M });
        }
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
                            active ? "text-primary" : "text-muted-foreground/50 hover:text-foreground",
                        )}
                    >
                        <Filter className="h-3.5 w-3.5" fill={active ? "currentColor" : "none"} />
                    </button>
                </PopoverTrigger>
                <PopoverContent
                    className="w-60 p-3"
                    align="start"
                    onClick={(e) => e.stopPropagation()}
                >
                    <div className="text-[10.5px] font-semibold uppercase mb-2"
                         style={{ letterSpacing: "0.12em", color: "#6b7280" }}>
                        Number filter{suffix ? ` (${suffix})` : ""}
                    </div>
                    <div className="grid grid-cols-2 gap-2 mb-2">
                        <div>
                            <label className="text-[10px] font-medium uppercase block mb-1" style={{ letterSpacing: "0.08em", color: "#6b7280" }}>From</label>
                            <Input
                                type="number"
                                value={draftMin}
                                onChange={(e) => setDraftMin(e.target.value)}
                                onBlur={commitMin}
                                onKeyDown={(e) => {
                                    if (e.key === "Enter") { commitMin(); (e.target as HTMLInputElement).blur(); }
                                    if (e.key === "Escape") { setOpen(false); }
                                }}
                                placeholder="min"
                                className="h-7 text-xs"
                                autoFocus
                            />
                        </div>
                        <div>
                            <label className="text-[10px] font-medium uppercase block mb-1" style={{ letterSpacing: "0.08em", color: "#6b7280" }}>To</label>
                            <Input
                                type="number"
                                value={draftMax}
                                onChange={(e) => setDraftMax(e.target.value)}
                                onBlur={commitMax}
                                onKeyDown={(e) => {
                                    if (e.key === "Enter") { commitMax(); (e.target as HTMLInputElement).blur(); }
                                    if (e.key === "Escape") { setOpen(false); }
                                }}
                                placeholder="max"
                                className="h-7 text-xs"
                            />
                        </div>
                    </div>
                    {presets.length > 0 && (
                        <div className="flex gap-1 mb-2 flex-wrap">
                            {presets.map((p) => (
                                <button
                                    key={p}
                                    type="button"
                                    onClick={() => preset(p)}
                                    className="px-2 py-0.5 border border-input rounded text-[10.5px] font-medium hover:bg-muted hover:border-foreground/30 tabular"
                                >
                                    ≥ {p}{suffix || ""}
                                </button>
                            ))}
                        </div>
                    )}
                    <div className="pt-2 border-t flex items-center justify-between text-[11px]" style={{ color: "#6b7280" }}>
                        <span>Tap outside or press Esc to close</span>
                        <button type="button" onClick={clear} className="hover:text-foreground hover:underline font-medium">
                            Clear
                        </button>
                    </div>
                </PopoverContent>
            </Popover>
            {active && (
                <button
                    type="button"
                    aria-label="Clear column filter"
                    onClick={(e) => { e.stopPropagation(); clear(); }}
                    title="Clear filter"
                    className="inline-flex items-center justify-center w-4 h-4 ml-0.5 rounded text-primary/70 hover:text-primary hover:bg-primary/10"
                >
                    <X className="h-3 w-3" />
                </button>
            )}
        </span>
    );
}

/**
 * Memoized export — see ColumnFilter for the same rationale.  Skips re-render
 * unless the numeric range actually changed; ignores the always-fresh onChange.
 */
export const NumberRangeFilter = memo(NumberRangeFilterImpl, (prev, next) => {
    if (prev.value.min !== next.value.min) return false;
    if (prev.value.max !== next.value.max) return false;
    if (prev.suffix !== next.suffix) return false;
    if (prev.presets !== next.presets) return false;
    return true;
});
