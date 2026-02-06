// =====================================================
// TRUE Excel-like Table Sort (Universal)
// - Works with or without <thead>
// - Stable sort (Excel tie behaviour)
// - Numeric-first, blanks-last
// - ₹, %, commas handled
// - Entire row always stays in sync
// =====================================================

document.addEventListener("DOMContentLoaded", () => {

    document.querySelectorAll("table").forEach(table => {

        let headerCells = [];
        let tbody = table.tBodies[0];
        if (!tbody) return;

        // ---------------------------------------------
        // Detect header row (thead OR first tbody row)
        // ---------------------------------------------
        if (table.tHead && table.tHead.rows.length > 0) {
            headerCells = Array.from(table.tHead.rows[0].cells);
        } else if (tbody.rows.length > 0) {
            headerCells = Array.from(tbody.rows[0].cells);
        }

        if (headerCells.length === 0) return;

        // ---------------------------------------------
        // Cache original row index for stable sorting
        // (exclude header row if it's inside tbody)
        // ---------------------------------------------
        Array.from(tbody.rows).forEach((row, index) => {
            row.__rowIndex = index;
        });

        const sortState = {};

        headerCells.forEach((th, colIndex) => {

            th.style.cursor = "pointer";
            th.title = "Click to sort";

            // First click = DESC (Excel style)
            sortState[colIndex] = "desc";

            th.addEventListener("click", () => {

                let rows = Array.from(tbody.rows);

                // If header is inside tbody, exclude it from sorting
                if (!table.tHead) {
                    rows = rows.slice(1);
                }

                const direction = sortState[colIndex];

                rows.sort((rowA, rowB) => {

                    const A = parseExcelValue(rowA.cells[colIndex]?.innerText);
                    const B = parseExcelValue(rowB.cells[colIndex]?.innerText);

                    // ----- numeric comparison -----
                    if (A.type === "number" && B.type === "number") {
                        if (A.value !== B.value) {
                            return direction === "desc"
                                ? B.value - A.value
                                : A.value - B.value;
                        }
                        return rowA.__rowIndex - rowB.__rowIndex;
                    }

                    // ----- blanks always last -----
                    if (A.type !== B.type) {
                        return A.type === "blank" ? 1 : -1;
                    }

                    // ----- text comparison -----
                    if (A.value !== B.value) {
                        return direction === "desc"
                            ? B.value.localeCompare(A.value)
                            : A.value.localeCompare(B.value);
                    }

                    return rowA.__rowIndex - rowB.__rowIndex;
                });

                // ---------------------------------------------
                // Re-attach rows (keep header untouched)
                // ---------------------------------------------
                rows.forEach(row => tbody.appendChild(row));

                // Toggle direction
                sortState[colIndex] = direction === "desc" ? "asc" : "desc";
            });
        });
    });
});

// =====================================================
// Excel-style value parser
// =====================================================
function parseExcelValue(text = "") {

    const raw = text.trim();

    // Blank cell
    if (raw === "" || raw === "-" || raw === "—") {
        return { type: "blank", value: null };
    }

    // Clean numeric formatting
    const cleaned = raw.replace(/₹|,|%/g, "");
    const num = Number(cleaned);

    if (!isNaN(num)) {
        return { type: "number", value: num };
    }

    // Text fallback
    return {
        type: "text",
        value: raw.toLowerCase()
    };
}
