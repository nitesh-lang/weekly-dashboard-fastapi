/* =============================================================
   page_chrome.js
   Cross-page filter persistence.

   Reads weeks/sel_weeks/brand/view from the current URL and rewrites
   every nav-strip / nav-links anchor on the page to carry those
   params along. Lets the user pick "Audio Array" once and have the
   selection stick when they hop between Dashboard / Sales Trend /
   Analytics / etc.

   Param translation (page-to-page differences):
     - Dashboard / Analytics / Inventory          → uses `weeks`
     - Sales Trend / Amazon Trend / Category      → uses `sel_weeks`
     We forward BOTH names with the same values; each route picks
     whichever it accepts and ignores the other.
   ============================================================= */
(function () {
    const params = new URLSearchParams(window.location.search);
    const carry = new URLSearchParams();

    // Forward weeks under both names
    const weeks = new Set([
        ...params.getAll('weeks'),
        ...params.getAll('sel_weeks'),
    ]);
    weeks.forEach(w => {
        if (!w) return;
        carry.append('weeks', w);
        carry.append('sel_weeks', w);
    });

    // Single-value params
    ['brand', 'view'].forEach(k => {
        const v = params.get(k);
        if (v) carry.set(k, v);
    });

    if (![...carry.keys()].length) return;

    document.querySelectorAll('.nav-strip a, .nav-links a').forEach(a => {
        let url;
        try { url = new URL(a.href, window.location.href); }
        catch { return; }
        if (url.origin !== window.location.origin) return;

        // Don't double-append if the link already has these params
        for (const [k, v] of carry.entries()) {
            const existing = url.searchParams.getAll(k);
            if (!existing.includes(v)) {
                url.searchParams.append(k, v);
            }
        }
        a.href = url.toString();
    });
})();
