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

/** Copy the table to the clipboard with formatting that matches the
 *  Excel export — Calibri 11, centre + middle align, wrap text, thin
 *  all-borders, header row bold with light grey fill.
 *
 *  Writes two MIME types simultaneously:
 *    • text/html      — an inline-styled <table>; Excel paste respects
 *                       font / align / border / fill / wrap settings.
 *    • text/plain     — tab-separated fallback for editors / Slack /
 *                       anything that can't read HTML clipboard.
 *
 *  Numbers are rounded with the same exportCell() helper the xlsx
 *  export uses so the values match across both paths. */
export async function copyTableToClipboard(
    rows: Record<string, unknown>[],
    columns: string[],
): Promise<{ ok: boolean; count: number }> {
    const escapeHtml = (s: string) => s
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");

    const cellText = (v: unknown): string => {
        const r = exportCell(v);
        if (r === "") return "";
        return String(r);
    };

    // ── text/plain (TSV) ────────────────────────────────────────────────
    const headerTsv = columns.map((c) => c.replace(/_/g, " ")).join("\t");
    const lineTsv = rows.map((r) =>
        columns.map((c) => cellText(r[c]).replace(/[\t\r\n]+/g, " ")).join("\t"),
    );
    const tsv = [headerTsv, ...lineTsv].join("\n");

    // ── text/html (styled <table>) ──────────────────────────────────────
    const TABLE_STYLE = "border-collapse:collapse; font-family:Calibri; font-size:11pt;";
    const CELL_STYLE  = "border:1px solid #000; padding:4px 8px; "
                      + "text-align:center; vertical-align:middle; "
                      + "white-space:normal;";
    const HEAD_STYLE  = CELL_STYLE + " font-weight:bold; background:#F1F1F0;";

    const headerHtml = "<tr>" + columns
        .map((c) => `<th style="${HEAD_STYLE}">${escapeHtml(c.replace(/_/g, " "))}</th>`)
        .join("") + "</tr>";

    const bodyHtml = rows.map((r) => "<tr>" + columns
        .map((c) => `<td style="${CELL_STYLE}">${escapeHtml(cellText(r[c]))}</td>`)
        .join("") + "</tr>").join("");

    const html = `<table style="${TABLE_STYLE}">`
               + `<thead>${headerHtml}</thead>`
               + `<tbody>${bodyHtml}</tbody>`
               + `</table>`;

    // ── Write both MIME types via ClipboardItem if supported ────────────
    try {
        if (typeof ClipboardItem !== "undefined" && navigator.clipboard?.write) {
            const item = new ClipboardItem({
                "text/html":  new Blob([html], { type: "text/html" }),
                "text/plain": new Blob([tsv],  { type: "text/plain" }),
            });
            await navigator.clipboard.write([item]);
            return { ok: true, count: rows.length };
        }
        // Fallback for browsers that don't support ClipboardItem: TSV only
        await navigator.clipboard.writeText(tsv);
        return { ok: true, count: rows.length };
    } catch {
        return { ok: false, count: 0 };
    }
}
