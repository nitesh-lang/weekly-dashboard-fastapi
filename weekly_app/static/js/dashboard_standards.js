/**
 * DASHBOARD STANDARDS  v1.0
 * ─────────────────────────────────────────────────────────────
 * Drop-in JS that fixes all inconsistencies across modules.
 * No backend changes. No template rewrites required.
 *
 * What it does:
 *  1. Removes auto-submit on onchange (prevents surprise reloads)
 *  2. Renders active-filter chips below the filter bar
 *  3. Universal sticky-column shadow on scroll
 *  4. Universal table sort with consistent ▲/▼ indicators
 *  5. Column-level inline filter inputs
 *  6. Auto-calculates totals row (tfoot) for numeric columns
 *  7. Heatmap: colorizes cells with data-heat attribute
 *  8. Keyboard shortcut: Ctrl+F → focus the search/filter box
 *  9. Toast helper (window.dsToast)
 * 10. Row click to highlight / select
 * ─────────────────────────────────────────────────────────────
 */

(function () {
  "use strict";

  /* ─────────────────────────────────────────────────────────
     UTIL
  ───────────────────────────────────────────────────────── */
  function qs(sel, ctx) { return (ctx || document).querySelector(sel); }
  function qsa(sel, ctx) { return Array.from((ctx || document).querySelectorAll(sel)); }

  function parseNum(text) {
    if (!text) return NaN;
    const cleaned = text.trim().replace(/[₹,%\s,]/g, "");
    const n = parseFloat(cleaned);
    return isNaN(n) ? NaN : n;
  }

  function fmtNum(n) {
    if (isNaN(n)) return "";
    if (Math.abs(n) >= 1e7) return "₹" + (n / 1e7).toFixed(2) + " Cr";
    if (Math.abs(n) >= 1e5) return "₹" + (n / 1e5).toFixed(2) + " L";
    return n.toLocaleString("en-IN");
  }

  /* ─────────────────────────────────────────────────────────
     1. REMOVE AUTO-SUBMIT on brand/view dropdowns
        (onchange="autoRefresh()" wires are the culprit)
  ───────────────────────────────────────────────────────── */
  function patchAutoSubmit() {
    // Override autoRefresh globally — make it a no-op.
    // Users must click Apply. This matches every other page.
    if (typeof window.autoRefresh === "function") {
      window._autoRefresh_original = window.autoRefresh;
      window.autoRefresh = function () {
        // Show a subtle indicator that filter changed but not yet applied
        dsMarkFilterDirty(true);
      };
    }
  }

  function dsMarkFilterDirty(dirty) {
    const applyBtns = qsa(
      ".ds-btn-apply, .btn-apply, button[type='submit']"
    ).filter(b => {
      const txt = b.textContent.trim().toLowerCase();
      return txt.includes("apply") || b.type === "submit";
    });
    applyBtns.forEach(btn => {
      if (dirty) {
        btn.style.boxShadow = "0 0 0 3px rgba(79,110,247,0.3)";
        btn.title = "Filters changed — click Apply to update";
      } else {
        btn.style.boxShadow = "";
        btn.title = "";
      }
    });
  }

  // Clear dirty state when Apply is clicked
  document.addEventListener("click", function (e) {
    const btn = e.target.closest("button[type='submit'], .ds-btn-apply, .btn-apply");
    if (btn) dsMarkFilterDirty(false);
  });


  /* ─────────────────────────────────────────────────────────
     2. ACTIVE FILTER CHIPS
        Reads current URL params and renders chips below the
        filter bar so users always know what's applied.
  ───────────────────────────────────────────────────────── */
  function renderFilterChips() {
    // Try to find or create the chip row
    let chipRow = qs("#dsChipRow, .ds-chip-row");
    if (!chipRow) {
      chipRow = document.createElement("div");
      chipRow.className = "ds-chip-row";
      chipRow.id = "dsChipRow";
      const filterBar = qs(
        ".ds-filter-bar, .filters-row, .filter-bar, .filter-panel, #amsFilterForm"
      );
      if (filterBar && filterBar.parentNode) {
        filterBar.parentNode.insertBefore(chipRow, filterBar.nextSibling);
      }
    }

    const params = new URLSearchParams(window.location.search);
    const skipKeys = new Set(["view", "export", "level"]);
    const chips = [];

    // Named param chips
    const labelMap = {
      brand: "Brand",
      week: "Week",
      weeks: "Week",
      channel: "Channel",
      category: "Category",
      value: "Value",
    };

    params.forEach((val, key) => {
      if (skipKeys.has(key) || !val || val === "None" || val === "all") return;
      const label = labelMap[key] || key;
      // Deduplicate multi-week
      const existing = chips.find(c => c.label === label);
      if (existing) {
        existing.values.push(val);
      } else {
        chips.push({ label, key, values: [val] });
      }
    });

    if (chips.length === 0) {
      chipRow.innerHTML = "";
      return;
    }

    chipRow.innerHTML =
      '<span class="ds-chip-label">Active filters:</span>' +
      chips
        .map(c => {
          const display = c.values.join(", ");
          const removeUrl = buildRemoveUrl(c.key, c.values);
          return (
            `<span class="ds-chip">` +
            `<span class="ds-chip-name">${c.label}:</span> ${display}` +
            (removeUrl
              ? `<button onclick="location.href='${removeUrl}'" title="Remove filter">×</button>`
              : "") +
            `</span>`
          );
        })
        .join("");
  }

  function buildRemoveUrl(key, vals) {
    const params = new URLSearchParams(window.location.search);
    // Remove all occurrences of this key
    params.delete(key);
    const base = window.location.pathname;
    const str = params.toString();
    return base + (str ? "?" + str : "");
  }


  /* ─────────────────────────────────────────────────────────
     3. STICKY COLUMN SHADOW on horizontal scroll
  ───────────────────────────────────────────────────────── */
  function initStickyColumnShadow() {
    qsa(".ds-table-wrap, .table-wrapper, .table-wrapper-sm").forEach(wrap => {
      wrap.addEventListener("scroll", function () {
        if (this.scrollLeft > 4) {
          this.classList.add("scrolled-x");
        } else {
          this.classList.remove("scrolled-x");
        }
      }, { passive: true });
    });
  }


  /* ─────────────────────────────────────────────────────────
     4. UNIVERSAL TABLE SORT
        Works on any table. Unifies sorted-asc/sort-asc naming.
        If a table already has a sort handler, we don't double-attach.
  ───────────────────────────────────────────────────────── */
  function initUniversalSort() {
    qsa("table").forEach(table => {
      // Skip if already wired up with onclick
      const thead = table.tHead;
      if (!thead) return;
      const headers = qsa("th", thead);
      if (!headers.length) return;

      // Skip if first header already has onclick defined inline
      const firstTh = headers[0];
      if (firstTh.getAttribute("onclick")) return; // already has handler

      const sortState = {};
      const tbody = table.tBodies[0];
      if (!tbody) return;

      // Cache original index for stable sort
      Array.from(tbody.rows).forEach((row, i) => { row._dsIdx = i; });

      headers.forEach((th, colIdx) => {
        // Skip checkbox columns
        if (th.querySelector("input[type='checkbox']")) return;
        // Skip if no-sort
        if (th.classList.contains("ds-no-sort")) return;

        th.style.cursor = "pointer";
        th.title = "Click to sort";
        sortState[colIdx] = "desc";

        // Add ⇅ hint if not already styled
        if (!th.classList.contains("ds-sort-asc") && !th.classList.contains("ds-sort-desc")) {
          th.classList.add("ds-sort-hint");
        }

        th.addEventListener("click", function () {
          const dir = sortState[colIdx];
          const rows = Array.from(tbody.rows);

          rows.sort((a, b) => {
            const cellA = a.cells[colIdx];
            const cellB = b.cells[colIdx];
            const rawA = (cellA?.dataset?.sort ?? cellA?.innerText ?? "").trim();
            const rawB = (cellB?.dataset?.sort ?? cellB?.innerText ?? "").trim();

            const nA = parseNum(rawA);
            const nB = parseNum(rawB);

            if (!isNaN(nA) && !isNaN(nB)) {
              if (nA !== nB) return dir === "desc" ? nB - nA : nA - nB;
              return a._dsIdx - b._dsIdx;
            }

            // Blanks last
            if (rawA === "" || rawA === "—") return 1;
            if (rawB === "" || rawB === "—") return -1;

            const cmp = rawA.toLowerCase().localeCompare(rawB.toLowerCase());
            return dir === "desc" ? cmp * -1 : cmp;
          });

          rows.forEach(r => tbody.appendChild(r));
          sortState[colIdx] = dir === "desc" ? "asc" : "desc";

          // Update indicators — unified class names
          headers.forEach(h => {
            h.classList.remove(
              "sorted-asc", "sorted-desc",
              "sort-asc",   "sort-desc",
              "ds-sort-asc","ds-sort-desc"
            );
          });
          th.classList.add(dir === "desc" ? "ds-sort-desc" : "ds-sort-asc");
          // Also add old names so existing CSS still works
          th.classList.add(dir === "desc" ? "sorted-desc" : "sorted-asc");
        });
      });
    });
  }


  /* ─────────────────────────────────────────────────────────
     5. COLUMN INLINE FILTER INPUTS
        Adds a second header row for columns that don't already
        have one. Activated by adding data-col-filter="true"
        to the <table>, or class ds-col-filterable.
  ───────────────────────────────────────────────────────── */
  function initColumnFilters() {
    qsa("table[data-col-filter='true'], table.ds-col-filterable").forEach(table => {
      const thead = table.tHead;
      const tbody = table.tBodies[0];
      if (!thead || !tbody) return;

      // Only add if no filter row already exists
      if (thead.querySelector(".ds-col-filter-row")) return;

      const headerRow = thead.rows[0];
      const filterRow = thead.insertRow();
      filterRow.className = "ds-col-filter-row";

      Array.from(headerRow.cells).forEach((th, colIdx) => {
        const td = filterRow.insertCell();
        if (th.querySelector("input[type='checkbox']")) return; // skip checkbox col

        const inp = document.createElement("input");
        inp.type = "text";
        inp.placeholder = th.textContent.trim().split(" ")[0].toLowerCase();
        inp.setAttribute("aria-label", "Filter " + th.textContent.trim());

        inp.addEventListener("input", function () {
          const val = this.value.toLowerCase().trim();
          Array.from(tbody.rows).forEach(row => {
            const cell = row.cells[colIdx];
            const text = (cell?.innerText || "").toLowerCase();
            if (val === "" || text.includes(val)) {
              row.style.display = "";
            } else {
              row.style.display = "none";
            }
          });
        });

        td.appendChild(inp);
      });
    });
  }


  /* ─────────────────────────────────────────────────────────
     6. AUTO TOTALS ROW
        For tables with data-totals="true" or class ds-totals.
        Sums all numeric columns and appends a <tfoot>.
  ───────────────────────────────────────────────────────── */
  function initTotalsRow() {
    qsa("table[data-totals='true'], table.ds-totals").forEach(table => {
      const tbody = table.tBodies[0];
      if (!tbody) return;
      if (table.tFoot && table.tFoot.rows.length) return; // already has tfoot

      const rows = Array.from(tbody.rows);
      if (!rows.length) return;

      const colCount = rows[0].cells.length;
      const sums = new Array(colCount).fill(null);
      const hasNum = new Array(colCount).fill(false);

      rows.forEach(row => {
        Array.from(row.cells).forEach((cell, i) => {
          const n = parseNum(cell.innerText);
          if (!isNaN(n)) {
            sums[i] = (sums[i] || 0) + n;
            hasNum[i] = true;
          }
        });
      });

      const tfoot = table.createTFoot();
      const tr = tfoot.insertRow();

      sums.forEach((sum, i) => {
        const td = tr.insertCell();
        if (i === 0) {
          td.innerHTML = '<span class="ds-total-label">TOTAL</span>';
          td.className = "ds-num";
        } else if (hasNum[i]) {
          td.textContent = fmtNum(sum);
          td.className = "ds-num";
        }
      });
    });
  }


  /* ─────────────────────────────────────────────────────────
     7. HEATMAP / CONDITIONAL FORMATTING
        Cells with data-heat="0..100" get color classes.
        Per-column: add data-heat-col="N" to the table,
        and the JS will rank-colorize that column.
  ───────────────────────────────────────────────────────── */
  function initHeatmap() {
    // data-heat attribute on individual cells
    qsa("td[data-heat]").forEach(td => {
      const v = parseFloat(td.dataset.heat);
      td.classList.remove("ds-heat-best","ds-heat-good","ds-heat-mid","ds-heat-bad","ds-heat-worst");
      if      (v >= 80) td.classList.add("ds-heat-best");
      else if (v >= 60) td.classList.add("ds-heat-good");
      else if (v >= 40) td.classList.add("ds-heat-mid");
      else if (v >= 20) td.classList.add("ds-heat-bad");
      else              td.classList.add("ds-heat-worst");
    });

    // data-heat-col="N" on table → colorize column N relative to its values
    qsa("table[data-heat-col]").forEach(table => {
      const colIdx = parseInt(table.dataset.heatCol, 10);
      if (isNaN(colIdx)) return;
      const tbody = table.tBodies[0];
      if (!tbody) return;

      const rows = Array.from(tbody.rows);
      const vals = rows.map(r => parseNum(r.cells[colIdx]?.innerText)).filter(v => !isNaN(v));
      if (!vals.length) return;

      const min = Math.min(...vals);
      const max = Math.max(...vals);
      const range = max - min || 1;

      rows.forEach(row => {
        const cell = row.cells[colIdx];
        if (!cell) return;
        const v = parseNum(cell.innerText);
        if (isNaN(v)) return;
        const pct = ((v - min) / range) * 100;
        cell.dataset.heat = pct.toFixed(0);
        // Re-use single-cell logic
        cell.classList.remove("ds-heat-best","ds-heat-good","ds-heat-mid","ds-heat-bad","ds-heat-worst");
        if      (pct >= 80) cell.classList.add("ds-heat-best");
        else if (pct >= 60) cell.classList.add("ds-heat-good");
        else if (pct >= 40) cell.classList.add("ds-heat-mid");
        else if (pct >= 20) cell.classList.add("ds-heat-bad");
        else                cell.classList.add("ds-heat-worst");
      });
    });

    // Stock levels: cells with data-stock="ok|low|out"
    qsa("td[data-stock]").forEach(td => {
      const s = td.dataset.stock;
      td.classList.remove("ds-stock-ok","ds-stock-low","ds-stock-out");
      if (s === "ok")  td.classList.add("ds-stock-ok");
      if (s === "low") td.classList.add("ds-stock-low");
      if (s === "out") td.classList.add("ds-stock-out");
    });
  }


  /* ─────────────────────────────────────────────────────────
     8. KEYBOARD SHORTCUT: Ctrl+F → focus filter search box
  ───────────────────────────────────────────────────────── */
  function initKeyboardShortcuts() {
    document.addEventListener("keydown", function (e) {
      if ((e.ctrlKey || e.metaKey) && e.key === "f") {
        const searchBox = qs(
          "#filterBox, input[placeholder*='Filter'], input[placeholder*='Search'], input[type='search']"
        );
        if (searchBox) {
          e.preventDefault();
          searchBox.focus();
          searchBox.select();
          dsToast("Search mode — type to filter rows");
        }
      }
    });
  }


  /* ─────────────────────────────────────────────────────────
     9. TOAST HELPER
        window.dsToast("message", "success|warn|")
  ───────────────────────────────────────────────────────── */
  let _toastWrap = null;
  window.dsToast = function (msg, type) {
    if (!_toastWrap) {
      _toastWrap = document.createElement("div");
      _toastWrap.id = "ds-toast";
      document.body.appendChild(_toastWrap);
    }
    const el = document.createElement("div");
    el.className = "ds-toast-msg" + (type ? " ds-toast-" + type : "");
    el.textContent = msg;
    _toastWrap.appendChild(el);
    setTimeout(() => {
      el.classList.add("ds-toast-out");
      setTimeout(() => el.remove(), 350);
    }, 2500);
  };


  /* ─────────────────────────────────────────────────────────
     10. ROW CLICK TO HIGHLIGHT / SELECT
         Tables with class ds-selectable get click-to-select.
  ───────────────────────────────────────────────────────── */
  function initRowSelection() {
    qsa("table.ds-selectable").forEach(table => {
      const tbody = table.tBodies[0];
      if (!tbody) return;
      tbody.addEventListener("click", function (e) {
        const tr = e.target.closest("tr");
        if (!tr || !tbody.contains(tr)) return;
        // Ctrl/Cmd = multi-select, else single
        if (!e.ctrlKey && !e.metaKey) {
          qsa("tr.ds-selected", tbody).forEach(r => r.classList.remove("ds-selected"));
        }
        tr.classList.toggle("ds-selected");
      });
    });
  }


  /* ─────────────────────────────────────────────────────────
     NAV CONSISTENCY FIX
     Ensures Dashboard always has a 🏠 Home link and all
     pages share the same nav order.
  ───────────────────────────────────────────────────────── */
  function fixNav() {
    const nav = qs(".nav-links");
    if (!nav) return;

    // Add Home if missing
    if (!nav.querySelector('a[href="/dashboard"]')) {
      const home = document.createElement("a");
      home.href = "/dashboard";
      home.textContent = "🏠 Home";
      home.style.cssText = "order:-1";
      nav.prepend(home);
    }

    // Mark active link
    const currentPath = window.location.pathname;
    qsa("a", nav).forEach(a => {
      if (a.getAttribute("href") === currentPath) {
        a.classList.add("active");
        a.style.background = "rgba(255,255,255,0.18)";
        a.style.color = "#fff";
        a.style.fontWeight = "700";
      }
    });
  }


  /* ─────────────────────────────────────────────────────────
     INIT — phased to avoid blocking first paint
  ───────────────────────────────────────────────────────── */
  function init() {
    // Phase 1: critical path — patch behavior before user can interact
    patchAutoSubmit();
    fixNav();
    initKeyboardShortcuts();

    // Phase 2: visual layer — next animation frame after paint
    requestAnimationFrame(function () {
      renderFilterChips();
      initStickyColumnShadow();

      // Phase 3: table work — idle time only
      var doTableWork = function () {
        initUniversalSort();
        initColumnFilters();
        initTotalsRow();
        initHeatmap();
        initRowSelection();
      };

      if ("requestIdleCallback" in window) {
        requestIdleCallback(doTableWork, { timeout: 3000 });
      } else {
        setTimeout(doTableWork, 200);
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

})();
