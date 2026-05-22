import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

/**
 * Two-way binding between a free-text input and a URL search param, with
 * debounce so we don't push a new history entry on every keystroke.
 *
 * Usage:
 *   const [filter, setFilter] = useDebouncedUrlParam("q");
 *   <Input value={filter} onChange={(e) => setFilter(e.target.value)} />
 *
 * - Reads the initial value from `?q=...` on mount.
 * - Local state updates instantly (input stays responsive).
 * - URL is updated 350ms after the user stops typing, with `replace: true`
 *   so the back button doesn't navigate through every keystroke.
 * - Clearing the input removes the param from the URL entirely.
 */
export function useDebouncedUrlParam(
    name: string,
    delayMs: number = 350,
): [string, (next: string) => void] {
    const [params, setParams] = useSearchParams();
    // Local state mirrors the input so typing is never blocked on a re-render.
    const [value, setValue] = useState<string>(() => params.get(name) || "");
    const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

    // External changes (e.g. nav from a link with ?q=...) should sync into state.
    useEffect(() => {
        const urlVal = params.get(name) || "";
        if (urlVal !== value) setValue(urlVal);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [params.toString()]);

    // When local state changes, schedule a URL write.
    useEffect(() => {
        if (timer.current) clearTimeout(timer.current);
        timer.current = setTimeout(() => {
            const cur = params.get(name) || "";
            if (cur === value) return;
            const next = new URLSearchParams(params);
            if (value) next.set(name, value);
            else next.delete(name);
            setParams(next, { replace: true });
        }, delayMs);
        return () => { if (timer.current) clearTimeout(timer.current); };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [value]);

    return [value, setValue];
}
