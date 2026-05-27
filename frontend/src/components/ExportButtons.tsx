import { useState } from "react";
import { Download, Copy, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { exportToXlsx, copyTableToClipboard } from "@/lib/utils";

/**
 * Export + Copy buttons used on every data-table page.
 *
 * - Export → downloads a formatted .xlsx file (Calibri 11, centre +
 *   middle align, wrap text, thin all-borders, header row bolded).
 *   Numbers rounded to 2 decimal places so no garbage decimals leak in.
 *
 * - Copy   → puts the table on the clipboard as tab-separated text.
 *   Pasting into Excel turns it into proper cells without the file
 *   download step.  Shows a "Copied!" check mark for 1.5s on success.
 *
 * Disabled when the rows array is empty.
 */
export function ExportButtons({
    rows,
    columns,
    filename,
    disabled,
}: {
    rows: Record<string, unknown>[];
    columns: string[];
    filename: string;
    disabled?: boolean;
}) {
    const [copied, setCopied] = useState(false);
    const isDisabled = !!disabled || !rows.length;

    async function handleExport() {
        await exportToXlsx(rows, columns, filename);
    }

    async function handleCopy() {
        const res = await copyTableToClipboard(rows, columns);
        if (res.ok) {
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
        }
    }

    return (
        <>
            <Button
                variant="outline"
                size="sm"
                disabled={isDisabled}
                onClick={handleExport}
                title="Download a formatted Excel file"
            >
                <Download className="h-3.5 w-3.5" />
                Export
            </Button>
            <Button
                variant="outline"
                size="sm"
                disabled={isDisabled}
                onClick={handleCopy}
                title="Copy the table to clipboard — paste into Excel"
            >
                {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
                {copied ? "Copied!" : "Copy"}
            </Button>
        </>
    );
}
