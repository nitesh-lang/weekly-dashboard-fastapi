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

/** Round numeric values for export — integers stay int, floats clamp to 2dp.
 *  Strings pass through.  null/undefined become "". */
function exportCell(v: unknown): string | number | "" {
    if (v == null || v === "") return "";
    if (typeof v === "number") {
        if (!Number.isFinite(v)) return "";
        if (Number.isInteger(v)) return v;
        return Math.round(v * 100) / 100;     // 2 decimal places, no garbage
    }
    // Try to detect numeric strings that came from JSON as text
    if (typeof v === "string") {
        const trimmed = v.trim();
        if (trimmed && !isNaN(Number(trimmed)) && /^-?\d+(\.\d+)?$/.test(trimmed)) {
            const n = Number(trimmed);
            return Number.isInteger(n) ? n : Math.round(n * 100) / 100;
        }
        return trimmed;
    }
    return String(v);
}

/** Round numbers and stringify for CSV.  Kept for any legacy caller.
 *  New code should prefer exportToXlsx + copyTableToClipboard. */
function csvCell(v: unknown): string {
    const r = exportCell(v);
    if (r === "") return "";
    if (typeof r === "number") return String(r);
    if (/[",\r\n]/.test(r)) return `"${r.replace(/"/g, '""')}"`;
    return r;
}

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

/** Export rows to a formatted .xlsx — Calibri 11, center + middle align,
 *  wrap text, thin all-borders, header row bolded with light grey fill.
 *  Numbers rounded to 2dp; integers stay int.  Loaded lazily so the
 *  exceljs bundle only ships when the user actually clicks Export. */
export async function exportToXlsx(
    rows: Record<string, unknown>[],
    columns: string[],
    filename: string,
    sheetName: string = "Sheet1",
) {
    const ExcelJS = (await import("exceljs")).default;
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet(sheetName);

    const CALIBRI: any = { name: "Calibri", size: 11 };
    const ALIGN: any   = { vertical: "middle", horizontal: "center", wrapText: true };
    const THIN: any    = { style: "thin", color: { argb: "FF000000" } };
    const BORDER: any  = { top: THIN, left: THIN, bottom: THIN, right: THIN };

    // Header
    const headerRow = ws.addRow(columns.map((c) => c.replace(/_/g, " ")));
    headerRow.height = 28;
    headerRow.eachCell({ includeEmpty: true }, (cell: any) => {
        cell.font      = { ...CALIBRI, bold: true };
        cell.alignment = ALIGN;
        cell.border    = BORDER;
        cell.fill      = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF1F1F0" } };
    });

    // Data
    for (const r of rows) {
        const row = ws.addRow(columns.map((k) => exportCell(r[k])));
        row.eachCell({ includeEmpty: true }, (cell: any) => {
            cell.font      = CALIBRI;
            cell.alignment = ALIGN;
            cell.border    = BORDER;
        });
    }

    // Auto-width columns based on max content length (capped 8–40)
    ws.columns.forEach((col: any, i: number) => {
        let max = String(columns[i] ?? "").length;
        for (const r of rows) {
            const v = exportCell(r[columns[i]]);
            const len = String(v).length;
            if (len > max) max = len;
        }
        col.width = Math.min(Math.max(max + 2, 10), 40);
    });

    const buf  = await wb.xlsx.writeBuffer();
    const blob = new Blob([buf], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href = url;
    a.download = filename.endsWith(".xlsx") ? filename : filename.replace(/\.csv$/i, "") + ".xlsx";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

/** Copy the table as tab-separated values to the clipboard.  Pasting
 *  into Excel turns this into proper cells without needing a file
 *  download.  Numbers are rounded (no garbage decimals). */
export async function copyTableToClipboard(
    rows: Record<string, unknown>[],
    columns: string[],
): Promise<{ ok: boolean; count: number }> {
    const cell = (v: unknown): string => {
        const r = exportCell(v);
        if (r === "") return "";
        const s = String(r);
        // Tabs and newlines would break the TSV grid — collapse to spaces
        return s.replace(/[\t\r\n]+/g, " ");
    };
    const header = columns.map((c) => c.replace(/_/g, " ")).join("\t");
    const lines  = rows.map((r) => columns.map((c) => cell(r[c])).join("\t"));
    const tsv    = [header, ...lines].join("\n");
    try {
        await navigator.clipboard.writeText(tsv);
        return { ok: true, count: rows.length };
    } catch {
        return { ok: false, count: 0 };
    }
}
