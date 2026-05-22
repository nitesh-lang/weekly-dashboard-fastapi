import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
    return twMerge(clsx(inputs));
}

/** Indian-format currency: ₹ 12.34 L / 4.21 Cr / 1,234 */
export function fmtINR(n: number | null | undefined, prefix = "₹ "): string {
    if (n == null || isNaN(n as number)) return prefix + "0";
    const v = Number(n);
    const abs = Math.abs(v);
    if (abs >= 1e7) return prefix + (v / 1e7).toFixed(2) + " Cr";
    if (abs >= 1e5) return prefix + (v / 1e5).toFixed(2) + " L";
    if (abs >= 1e3) return prefix + (v / 1e3).toFixed(1) + " K";
    return prefix + v.toLocaleString("en-IN");
}

/** Indian thousand-separator for plain integers (units, etc.) */
export function fmtInt(n: number | null | undefined): string {
    if (n == null || isNaN(n as number)) return "0";
    return Math.round(Number(n)).toLocaleString("en-IN");
}

/** Parse "Week 19" → 19, "19" → 19, anything else → 0 */
export function weekNum(w: string | number | null | undefined): number {
    if (w == null) return 0;
    const s = String(w);
    const m = s.match(/(\d+)/);
    return m ? parseInt(m[1], 10) : 0;
}

export function sortWeeks(weeks: string[]): string[] {
    return [...weeks].sort((a, b) => weekNum(a) - weekNum(b));
}

/** Round numbers and stringify for CSV. Integers stay int, floats round to 0dp. */
function csvCell(v: unknown): string {
    if (v == null) return "";
    if (typeof v === "number") return String(Math.round(v));
    const s = String(v);
    // Escape RFC4180: wrap in quotes if contains , " or newline; double internal quotes.
    if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
}

/** Download an array of row objects as a CSV file.
 *  - columns: explicit ordered keys. Cells are rounded (numbers → integers)
 *  - filename: download name (e.g. "dashboard-sku.csv") */
export function exportToCsv(rows: Record<string, unknown>[], columns: string[], filename: string) {
    const header = columns.map(csvCell).join(",");
    const lines  = rows.map((r) => columns.map((c) => csvCell(r[c])).join(","));
    const csv    = [header, ...lines].join("\n");
    const blob   = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url    = URL.createObjectURL(blob);
    const a      = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}
