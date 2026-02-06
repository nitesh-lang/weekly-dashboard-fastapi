// =====================================================
// Excel-like Table Sort for Category Sales
// - Stable sort (keeps row order on ties)
// - Numeric-first, blanks-last
// - ₹, %, commas, decimals handled
// - Entire row always stays in sync
// =====================================================

document.addEventListener("DOMContentLoaded", () => {

    const table = document.querySelector("table");
    if (!table) return;

    const tbody = table.tBodies[0];
    if (!tbody) return;

    // Detect header cells (Category Sales HAS thead)
    const headers = Array.from(table.tHead.rows[0].cells);

    // Cache original row order (Excel stability)
    Array.from(tbody.rows).forEach((row, index) => {
        row.__rowIndex = index;
    });

    const sortState = {};

    headers.forEach((th, colIndex) => {

        th.style.cursor = "pointer";
        th.title = "Click to sort";

        // First click = DESC (Highest → Lowest)
        sortState[colIndex] = "desc";

        th.addEventListener("click", () => {

            const direction = sortState[colIndex];
            const rows = Array.from(tbody.rows);

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
                    // tie → preserve original order
                    return rowA.__rowIndex - rowB.__rowIndex;
                }

                // ----- blanks last -----
                if (A.type !== B.type) {
                    return A.type === "blank" ? 1 : -1;
                }

                // ----- text comparison -----
                if (A.value !== B.value) {
                    return direction === "desc"
                        ? B.value.localeCompare(A.value)
                        : A.value.localeCompare(B.value);
                }

                // final fallback
                return rowA.__rowIndex - rowB.__rowIndex;
            });

            // Re-attach rows
            rows.forEach(row => tbody.appendChild(row));

            // Toggle direction
            sortState[colIndex] = direction === "desc" ? "asc" : "desc";
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

    // Remove formatting
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
