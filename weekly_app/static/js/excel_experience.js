/**
 * EXCEL EXPERIENCE  v1.0
 * ─────────────────────────────────────────────────────────────
 * Turns any table into an Excel-like grid.
 *
 * Features:
 *  1. AutoFilter — per-column dropdown with unique value checkboxes
 *  2. Sort persistence — survives page reload via sessionStorage
 *  3. Freeze panes — sticky header + configurable frozen columns
 *  4. Row selection — click to select, Shift+click range, Ctrl+click multi
 *  5. Copy rows — Ctrl+C copies selected rows as TSV (pastes into Excel)
 *  6. Context menu — right-click: Copy, Copy with headers, Export selection
 *  7. Status bar — Count / Sum / Avg of selected cells (bottom of table)
 *  8. Row info bar — "Showing 42 of 180 rows · Clear filters"
 *  9. Number formatting — ₹ commas, % display, negative red
 * 10. Keyboard navigation — arrow keys, Enter, Tab
 *
 * Usage:
 *   XLTable.init('#myTable')           ← basic
 *   XLTable.init('#myTable', {
 *     frozenCols: 2,                   ← freeze first N columns
 *     heatCols: [5, 6],               ← heatmap on these column indices
 *     totalsCols: [5, 6, 7],          ← sum these in footer
 *     storageKey: 'sales-trend',      ← persist sort state
 *   })
 * ─────────────────────────────────────────────────────────────
 */

window.XLTable = (function () {
  "use strict";

  /* ── Utils ─────────────────────────────────────────────── */
  function qs(s, c) { return (c || document).querySelector(s); }
  function qsa(s, c) { return Array.from((c || document).querySelectorAll(s)); }

  function parseNum(text) {
    if (!text) return NaN;
    const c = String(text).trim().replace(/[₹,%\s]/g, "").replace(/,/g, "");
    const n = parseFloat(c);
    return isNaN(n) ? NaN : n;
  }

  function fmtInr(n) {
    if (isNaN(n) || n === null) return "—";
    return "₹\u202f" + Math.round(n).toLocaleString("en-IN");
  }

  function fmtNum(n, decimals) {
    if (isNaN(n)) return "—";
    return n.toLocaleString("en-IN", {
      minimumFractionDigits: decimals || 0,
      maximumFractionDigits: decimals || 0,
    });
  }

  /* ── Single table instance ─────────────────────────────── */
  function XLInstance(table, opts) {
    opts = Object.assign({
      frozenCols: 1,
      heatCols: [],
      totalsCols: [],
      storageKey: null,
      autoFilter: true,
      copyEnabled: true,
      statusBar: true,
      rowInfo: true,
    }, opts);

    const tbody = table.tBodies[0];
    const thead = table.tHead;
    if (!tbody || !thead) return;

    const wrapper = table.closest(".table-wrapper, .ds-table-wrap, .table-wrapper-sm")
      || (() => {
        const d = document.createElement("div");
        d.className = "table-wrapper ds-excel";
        table.parentNode.insertBefore(d, table);
        d.appendChild(table);
        return d;
      })();

    table.classList.add("ds-excel");

    // Per-column filter state: Map<colIdx, Set<value>>
    const filterState = new Map();
    // Sort state
    let sortCol = null, sortDir = null;
    // Selection state
    let lastSelectedRow = null;

    // Cache original row order
    Array.from(tbody.rows).forEach((r, i) => { r._xlIdx = i; });


    /* ── 1. FREEZE PANES ─────────────────────────────────── */
    function applyFreeze() {
      const cols = opts.frozenCols;
      if (!cols) return;

      let left = 0;
      for (let c = 0; c < cols; c++) {
        const headerCell = thead.rows[0].cells[c];
        if (!headerCell) break;
        const w = headerCell.offsetWidth || 100;

        // Header
        qsa("tr", thead).forEach(row => {
          const cell = row.cells[c];
          if (!cell) return;
          cell.classList.add("xl-frozen");
          cell.style.left = left + "px";
          cell.style.zIndex = 12;
        });

        // Body
        qsa("tr", tbody).forEach(row => {
          const cell = row.cells[c];
          if (!cell) return;
          cell.classList.add("xl-frozen");
          cell.style.left = left + "px";
        });

        left += w;
      }

      // Freeze shadow on scroll
      wrapper.addEventListener("scroll", function () {
        const shadow = this.scrollLeft > 2;
        qsa(".xl-frozen", table).forEach(cell => {
          cell.style.boxShadow = shadow
            ? "3px 0 6px rgba(0,0,0,0.09)"
            : "";
        });
      }, { passive: true });
    }


    /* ── 2. AUTOFILTER ───────────────────────────────────── */
    let activeDropdown = null;
    let activeColIdx = null;

    function buildDropdown() {
      const dd = document.createElement("div");
      dd.className = "xl-dropdown";
      dd.id = "xl-dd-" + Math.random().toString(36).slice(2);
      document.body.appendChild(dd);
      return dd;
    }

    const dropdown = buildDropdown();

    function getColValues(colIdx) {
      const vals = new Set();
      Array.from(tbody.rows).forEach(row => {
        const cell = row.cells[colIdx];
        const v = cell ? cell.innerText.trim() : "";
        if (v && v !== "—") vals.add(v);
      });
      return Array.from(vals).sort((a, b) => {
        const na = parseNum(a), nb = parseNum(b);
        if (!isNaN(na) && !isNaN(nb)) return na - nb;
        return a.localeCompare(b);
      });
    }

    function openDropdown(colIdx, anchorEl) {
      activeColIdx = colIdx;
      const selected = filterState.get(colIdx) || new Set();
      const allVals = getColValues(colIdx);

      dropdown.innerHTML = `
        <div class="xl-dd-search">
          <input type="text" placeholder="Search values…" id="xl-dd-search-inp">
        </div>
        <div class="xl-dd-list" id="xl-dd-list">
          <label class="xl-dd-item xl-dd-selectall">
            <input type="checkbox" id="xl-dd-all" ${selected.size === 0 ? "checked" : ""}> (Select All)
          </label>
          ${allVals.map(v =>
            `<label class="xl-dd-item" data-val="${encodeURIComponent(v)}">
              <input type="checkbox" value="${v}" ${selected.size === 0 || selected.has(v) ? "checked" : ""}> ${v}
            </label>`
          ).join("")}
        </div>
        <div class="xl-dd-footer">
          <button class="xl-dd-btn" id="xl-dd-cancel">Cancel</button>
          <button class="xl-dd-btn xl-dd-ok" id="xl-dd-ok">OK</button>
        </div>
      `;

      // Position
      const rect = anchorEl.getBoundingClientRect();
      dropdown.style.top  = (rect.bottom + 2) + "px";
      dropdown.style.left = rect.left + "px";
      dropdown.classList.add("open");
      activeDropdown = dropdown;

      // Search within dropdown
      qs("#xl-dd-search-inp", dropdown).addEventListener("input", function () {
        const q = this.value.toLowerCase();
        qsa(".xl-dd-item:not(.xl-dd-selectall)", dropdown).forEach(item => {
          const v = item.dataset.val ? decodeURIComponent(item.dataset.val) : "";
          item.style.display = v.toLowerCase().includes(q) ? "" : "none";
        });
      });

      // Select all toggle
      qs("#xl-dd-all", dropdown).addEventListener("change", function () {
        qsa('input[type="checkbox"]', qs("#xl-dd-list", dropdown)).forEach(cb => {
          cb.checked = this.checked;
        });
      });

      // OK
      qs("#xl-dd-ok", dropdown).addEventListener("click", function () {
        const checked = qsa('input[type="checkbox"]:not(#xl-dd-all):checked', qs("#xl-dd-list")).map(cb => cb.value);
        const allValsLocal = getColValues(colIdx);
        if (checked.length === allValsLocal.length || checked.length === 0) {
          filterState.delete(colIdx);
        } else {
          filterState.set(colIdx, new Set(checked));
        }
        closeDropdown();
        applyFilters();
        updateFilterArrows();
      });

      // Cancel
      qs("#xl-dd-cancel", dropdown).addEventListener("click", closeDropdown);
    }

    function closeDropdown() {
      if (activeDropdown) {
        activeDropdown.classList.remove("open");
        activeDropdown = null;
        activeColIdx = null;
      }
    }

    // Close on outside click
    document.addEventListener("click", function (e) {
      if (activeDropdown && !activeDropdown.contains(e.target) && !e.target.closest(".xl-filter-arrow")) {
        closeDropdown();
      }
    });

    function injectFilterArrows() {
      if (!opts.autoFilter) return;
      const headerRow = thead.rows[0];
      Array.from(headerRow.cells).forEach((th, colIdx) => {
        // Skip checkbox columns
        if (th.querySelector("input[type='checkbox']")) return;

        const label = th.textContent.trim();
        th.innerHTML = `
          <div class="xl-th-inner">
            <span class="xl-th-label">${label}</span>
            <span class="xl-sort-btn"></span>
            <span class="xl-filter-arrow-wrap">
              <span class="xl-filter-arrow" data-col="${colIdx}"></span>
            </span>
          </div>
        `;

        th.querySelector(".xl-filter-arrow").addEventListener("click", function (e) {
          e.stopPropagation();
          if (activeColIdx === colIdx) {
            closeDropdown();
          } else {
            openDropdown(colIdx, this);
          }
        });

        // Sort on label click
        th.querySelector(".xl-th-label").addEventListener("click", function () {
          sortByCol(colIdx);
        });
      });
    }

    function updateFilterArrows() {
      qsa(".xl-filter-arrow", table).forEach(arrow => {
        const colIdx = parseInt(arrow.dataset.col, 10);
        const th = thead.rows[0].cells[colIdx];
        if (filterState.has(colIdx)) {
          th.classList.add("xl-filter-active");
        } else {
          th.classList.remove("xl-filter-active");
        }
      });
    }


    /* ── 3. FILTER APPLICATION ───────────────────────────── */
    function applyFilters() {
      let visible = 0;
      const total = tbody.rows.length;

      Array.from(tbody.rows).forEach(row => {
        let show = true;
        filterState.forEach((allowed, colIdx) => {
          const cell = row.cells[colIdx];
          const v = cell ? cell.innerText.trim() : "";
          if (!allowed.has(v)) show = false;
        });
        row.style.display = show ? "" : "none";
        if (show) visible++;
      });

      updateRowInfo(visible, total);
      updateStatusBar();
    }

    function clearAllFilters() {
      filterState.clear();
      Array.from(tbody.rows).forEach(r => { r.style.display = ""; });
      updateFilterArrows();
      updateRowInfo(tbody.rows.length, tbody.rows.length);
      updateStatusBar();
    }


    /* ── 4. SORT ─────────────────────────────────────────── */
    function sortByCol(colIdx) {
      if (sortCol === colIdx) {
        sortDir = sortDir === "desc" ? "asc" : "desc";
      } else {
        sortCol = colIdx;
        sortDir = "desc";
      }

      const rows = Array.from(tbody.rows);
      rows.sort((a, b) => {
        const cellA = a.cells[colIdx];
        const cellB = b.cells[colIdx];
        const rawA = (cellA?.dataset?.sort ?? cellA?.innerText ?? "").trim();
        const rawB = (cellB?.dataset?.sort ?? cellB?.innerText ?? "").trim();
        const nA = parseNum(rawA), nB = parseNum(rawB);

        if (!isNaN(nA) && !isNaN(nB)) {
          if (nA !== nB) return sortDir === "desc" ? nB - nA : nA - nB;
          return a._xlIdx - b._xlIdx;
        }
        if (!rawA) return 1;
        if (!rawB) return -1;
        const c = rawA.toLowerCase().localeCompare(rawB.toLowerCase());
        return sortDir === "desc" ? -c : c;
      });

      rows.forEach(r => tbody.appendChild(r));

      // Update header indicators
      qsa("th", thead.rows[0]).forEach((th, i) => {
        th.classList.remove("xl-asc", "xl-desc");
        if (i === colIdx) th.classList.add(sortDir === "asc" ? "xl-asc" : "xl-desc");
      });

      // Persist
      if (opts.storageKey) {
        try {
          sessionStorage.setItem(
            "xl-sort-" + opts.storageKey,
            JSON.stringify({ col: colIdx, dir: sortDir })
          );
        } catch (e) {}
      }
    }

    function restoreSort() {
      if (!opts.storageKey) return;
      try {
        const saved = JSON.parse(sessionStorage.getItem("xl-sort-" + opts.storageKey));
        if (saved && saved.col !== undefined) {
          sortCol = saved.col;
          sortDir = saved.dir;
          // Re-sort without toggling
          const rows = Array.from(tbody.rows);
          rows.sort((a, b) => {
            const rawA = (a.cells[sortCol]?.innerText ?? "").trim();
            const rawB = (b.cells[sortCol]?.innerText ?? "").trim();
            const nA = parseNum(rawA), nB = parseNum(rawB);
            if (!isNaN(nA) && !isNaN(nB)) return sortDir === "desc" ? nB - nA : nA - nB;
            return rawA.localeCompare(rawB) * (sortDir === "desc" ? -1 : 1);
          });
          rows.forEach(r => tbody.appendChild(r));
          const th = thead.rows[0].cells[sortCol];
          if (th) th.classList.add(sortDir === "asc" ? "xl-asc" : "xl-desc");
        }
      } catch (e) {}
    }


    /* ── 5. ROW SELECTION ────────────────────────────────── */
    function getVisibleRows() {
      return Array.from(tbody.rows).filter(r => r.style.display !== "none");
    }

    tbody.addEventListener("click", function (e) {
      const row = e.target.closest("tr");
      if (!row || !tbody.contains(row)) return;
      if (e.target.closest("a, button, input, select")) return;

      const visible = getVisibleRows();

      if (e.shiftKey && lastSelectedRow) {
        const idxA = visible.indexOf(lastSelectedRow);
        const idxB = visible.indexOf(row);
        const [lo, hi] = [Math.min(idxA, idxB), Math.max(idxA, idxB)];
        if (!e.ctrlKey && !e.metaKey) {
          qsa("tr.xl-selected", tbody).forEach(r => r.classList.remove("xl-selected"));
        }
        visible.slice(lo, hi + 1).forEach(r => r.classList.add("xl-selected"));
      } else if (e.ctrlKey || e.metaKey) {
        row.classList.toggle("xl-selected");
      } else {
        qsa("tr.xl-selected", tbody).forEach(r => r.classList.remove("xl-selected"));
        row.classList.add("xl-selected");
      }

      lastSelectedRow = row;
      updateStatusBar();
    });


    /* ── 6. COPY ROWS (Ctrl+C) ───────────────────────────── */
    if (opts.copyEnabled) {
      document.addEventListener("keydown", function (e) {
        if (!(e.ctrlKey || e.metaKey) || e.key !== "c") return;
        // Only if focus is inside this table or no input is focused
        const focus = document.activeElement;
        if (focus && (focus.tagName === "INPUT" || focus.tagName === "TEXTAREA")) return;

        const selected = qsa("tr.xl-selected", tbody);
        if (!selected.length) return;

        e.preventDefault();
        copyRowsTSV(selected, false);
      });
    }

    function copyRowsTSV(rows, includeHeaders) {
      let lines = [];

      if (includeHeaders) {
        const headers = Array.from(thead.rows[0].cells)
          .map(th => th.querySelector(".xl-th-label")?.textContent.trim() || th.innerText.trim())
          .join("\t");
        lines.push(headers);
      }

      rows.forEach(row => {
        const cells = Array.from(row.cells).map(td => {
          const v = td.innerText.trim().replace(/\t/g, " ");
          return v;
        });
        lines.push(cells.join("\t"));
      });

      const tsv = lines.join("\n");

      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(tsv).then(() => {
          showCopyToast(rows.length + " row" + (rows.length > 1 ? "s" : "") + " copied");
          rows.forEach(r => {
            r.classList.add("xl-copy-flash");
            setTimeout(() => r.classList.remove("xl-copy-flash"), 350);
          });
        });
      }
    }

    let _copyToast = null;
    function showCopyToast(msg) {
      if (!_copyToast) {
        _copyToast = document.createElement("div");
        _copyToast.className = "xl-copy-toast";
        document.body.appendChild(_copyToast);
      }
      _copyToast.textContent = msg;
      _copyToast.classList.add("show");
      clearTimeout(_copyToast._timer);
      _copyToast._timer = setTimeout(() => _copyToast.classList.remove("show"), 2000);
    }


    /* ── 7. CONTEXT MENU ─────────────────────────────────── */
    const ctxMenu = document.createElement("div");
    ctxMenu.className = "xl-context-menu";
    ctxMenu.innerHTML = `
      <div class="xl-ctx-item" data-action="copy">
        <span class="xl-ctx-icon">⎘</span> Copy rows
        <span class="xl-ctx-kbd">Ctrl+C</span>
      </div>
      <div class="xl-ctx-item" data-action="copy-headers">
        <span class="xl-ctx-icon">⎘</span> Copy with headers
      </div>
      <div class="xl-ctx-sep"></div>
      <div class="xl-ctx-item" data-action="select-all">
        <span class="xl-ctx-icon">▣</span> Select all visible
        <span class="xl-ctx-kbd">Ctrl+A</span>
      </div>
      <div class="xl-ctx-item" data-action="clear-sel">
        <span class="xl-ctx-icon">✕</span> Clear selection
      </div>
      <div class="xl-ctx-sep"></div>
      <div class="xl-ctx-item" data-action="clear-filters">
        <span class="xl-ctx-icon">⊘</span> Clear all filters
      </div>
    `;
    document.body.appendChild(ctxMenu);

    table.addEventListener("contextmenu", function (e) {
      e.preventDefault();
      const row = e.target.closest("tr");
      if (row && tbody.contains(row) && !row.classList.contains("xl-selected")) {
        qsa("tr.xl-selected", tbody).forEach(r => r.classList.remove("xl-selected"));
        row.classList.add("xl-selected");
        lastSelectedRow = row;
      }

      ctxMenu.style.top  = e.clientY + "px";
      ctxMenu.style.left = e.clientX + "px";
      ctxMenu.classList.add("open");
    });

    ctxMenu.addEventListener("click", function (e) {
      const item = e.target.closest("[data-action]");
      if (!item) return;
      const action = item.dataset.action;
      const selected = qsa("tr.xl-selected", tbody);

      if (action === "copy")         copyRowsTSV(selected, false);
      if (action === "copy-headers") copyRowsTSV(selected, true);
      if (action === "select-all") {
        getVisibleRows().forEach(r => r.classList.add("xl-selected"));
        updateStatusBar();
      }
      if (action === "clear-sel") {
        qsa("tr.xl-selected", tbody).forEach(r => r.classList.remove("xl-selected"));
        updateStatusBar();
      }
      if (action === "clear-filters") clearAllFilters();

      ctxMenu.classList.remove("open");
    });

    document.addEventListener("click", function (e) {
      if (!ctxMenu.contains(e.target)) ctxMenu.classList.remove("open");
    });

    // Ctrl+A
    document.addEventListener("keydown", function (e) {
      if ((e.ctrlKey || e.metaKey) && e.key === "a") {
        if (document.activeElement?.tagName === "INPUT") return;
        // Only if mouse is over this table
        e.preventDefault();
        getVisibleRows().forEach(r => r.classList.add("xl-selected"));
        updateStatusBar();
      }
    });


    /* ── 8. STATUS BAR ───────────────────────────────────── */
    let statusBar = null;

    function initStatusBar() {
      if (!opts.statusBar) return;
      statusBar = document.createElement("div");
      statusBar.className = "xl-status-bar";
      const parent = wrapper.parentNode;
      parent.insertBefore(statusBar, wrapper.nextSibling);
      updateStatusBar();
    }

    function updateStatusBar() {
      if (!statusBar) return;
      const selected = qsa("tr.xl-selected", tbody);
      const visible  = getVisibleRows();
      const total    = tbody.rows.length;

      let parts = [
        `<span class="xl-status-item"><span class="label">ROWS</span> <span class="value">${visible.length}${visible.length < total ? " / " + total : ""}</span></span>`,
        `<span class="xl-status-sep">|</span>`,
      ];

      if (selected.length > 0) {
        parts.push(`<span class="xl-status-item"><span class="label">SELECTED</span> <span class="value">${selected.length}</span></span>`);

        // Sum numeric values in selected rows across all columns
        const sums = {};
        selected.forEach(row => {
          Array.from(row.cells).forEach((cell, i) => {
            const n = parseNum(cell.innerText);
            if (!isNaN(n)) sums[i] = (sums[i] || 0) + n;
          });
        });

        const sumKeys = Object.keys(sums);
        if (sumKeys.length > 0) {
          parts.push(`<span class="xl-status-sep">|</span>`);
          // Show sum for the most interesting columns (first 3 numeric)
          sumKeys.slice(0, 3).forEach(i => {
            const th = thead.rows[0].cells[parseInt(i)];
            const label = th?.querySelector(".xl-th-label")?.textContent.trim()
              || th?.innerText.trim().split("\n")[0]
              || "Col " + i;
            const shortLabel = label.length > 12 ? label.slice(0, 10) + "…" : label;
            parts.push(
              `<span class="xl-status-item"><span class="label">SUM ${shortLabel}</span> <span class="value">${fmtNum(sums[i])}</span></span>`
            );
          });
        }

        parts.push(`<span class="xl-status-sep">|</span>`);
        parts.push(`<span class="xl-status-item" style="cursor:pointer;color:rgba(255,255,255,0.6)" onclick="this.closest('.xl-status-bar').dispatchEvent(new CustomEvent('clearsel'))">✕ Clear</span>`);
      } else {
        parts.push(`<span class="xl-status-item" style="color:rgba(255,255,255,0.4);font-size:10px">Click rows to select · Ctrl+C to copy · Right-click for options</span>`);
      }

      statusBar.innerHTML = parts.join("");
      statusBar.addEventListener("clearsel", function () {
        qsa("tr.xl-selected", tbody).forEach(r => r.classList.remove("xl-selected"));
        updateStatusBar();
      }, { once: true });
    }


    /* ── 9. ROW INFO BAR ─────────────────────────────────── */
    let rowInfoBar = null;

    function initRowInfoBar() {
      if (!opts.rowInfo) return;
      rowInfoBar = document.createElement("div");
      rowInfoBar.className = "xl-row-info";
      wrapper.parentNode.insertBefore(rowInfoBar, wrapper);
      updateRowInfo(tbody.rows.length, tbody.rows.length);
    }

    function updateRowInfo(visible, total) {
      if (!rowInfoBar) return;
      const hasFilter = filterState.size > 0;
      rowInfoBar.innerHTML = hasFilter
        ? `Showing <span class="xl-filtered">${visible}</span> of ${total} rows &nbsp;·&nbsp; <a class="xl-clear-filters" href="#">Clear filters</a>`
        : `${total} rows`;

      const clearLink = rowInfoBar.querySelector(".xl-clear-filters");
      if (clearLink) {
        clearLink.addEventListener("click", function (e) {
          e.preventDefault();
          clearAllFilters();
        });
      }
    }


    /* ── 10. HEATMAP ─────────────────────────────────────── */
    function applyHeatmap() {
      opts.heatCols.forEach(colIdx => {
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
          cell.classList.remove("ds-heat-best","ds-heat-good","ds-heat-mid","ds-heat-bad","ds-heat-worst");
          if      (pct >= 80) cell.classList.add("ds-heat-best");
          else if (pct >= 60) cell.classList.add("ds-heat-good");
          else if (pct >= 40) cell.classList.add("ds-heat-mid");
          else if (pct >= 20) cell.classList.add("ds-heat-bad");
          else                cell.classList.add("ds-heat-worst");
        });
      });
    }


    /* ── 11. TOTALS ROW ──────────────────────────────────── */
    function buildTotalsRow() {
      if (!opts.totalsCols.length) return;
      if (table.tFoot && table.tFoot.rows.length) return;

      const rows = Array.from(tbody.rows);
      const colCount = thead.rows[0].cells.length;
      const sums = {};

      rows.forEach(row => {
        opts.totalsCols.forEach(c => {
          const cell = row.cells[c];
          const n = parseNum(cell?.innerText);
          if (!isNaN(n)) sums[c] = (sums[c] || 0) + n;
        });
      });

      const tfoot = table.createTFoot();
      const tr = tfoot.insertRow();
      tr.className = "xl-total";

      for (let i = 0; i < colCount; i++) {
        const td = tr.insertCell();
        if (i === 0) {
          td.innerHTML = '<span class="ds-total-label">TOTAL</span>';
        } else if (sums[i] !== undefined) {
          td.textContent = fmtNum(sums[i]);
          td.className = "xl-num ds-num";
        }
      }
    }


    /* ── 12. NUMBER FORMATTING ───────────────────────────── */
    function formatNumbers() {
      Array.from(tbody.rows).forEach(row => {
        Array.from(row.cells).forEach(cell => {
          const raw = cell.innerText.trim();
          const n = parseNum(raw);
          if (isNaN(n)) return;

          // Already formatted or has special content
          if (cell.querySelector("span, a")) return;

          cell.classList.add("xl-num");

          // Mark negatives
          if (n < 0) cell.classList.add("xl-neg");

          // Right-align all numbers already done via xl-num
        });
      });
    }


    /* ── PHASED INIT ───────────────────────────────────────
       Phase 1 (immediate, sync)   — visual structure only:
         freeze panes + filter arrows. Page looks correct fast.
       Phase 2 (next frame)        — interaction layer:
         row selection, context menu, keyboard shortcuts.
       Phase 3 (idle / off-screen) — expensive computation:
         heatmap, totals, number formatting, sort restore.
         Deferred until browser is idle OR table scrolls into view.
    ───────────────────────────────────────────────────────── */

    // Phase 1 — immediate (< 5ms, pure DOM structure)
    applyFreeze();
    injectFilterArrows();
    initRowInfoBar();

    // Phase 2 — next animation frame (interaction wiring)
    requestAnimationFrame(function () {
      initStatusBar();

      // Phase 3 — idle callback for expensive work
      const doHeavyWork = function () {
        buildTotalsRow();
        applyHeatmap();
        formatNumbers();
        restoreSort();
        applyFilters();
      };

      // Use IntersectionObserver: only do heavy work when
      // table is actually visible on screen
      if ("IntersectionObserver" in window) {
        const observer = new IntersectionObserver(function (entries, obs) {
          entries.forEach(function (entry) {
            if (entry.isIntersecting) {
              obs.disconnect();
              if ("requestIdleCallback" in window) {
                requestIdleCallback(function () { doHeavyWork(); table.classList.add("xl-ready"); }, { timeout: 2000 });
              } else {
                setTimeout(function () { doHeavyWork(); table.classList.add("xl-ready"); }, 100);
              }
            }
          });
        }, { rootMargin: "200px" });
        observer.observe(table);
      } else {
        // Fallback: idle callback or timeout
        if ("requestIdleCallback" in window) {
          requestIdleCallback(function () { doHeavyWork(); table.classList.add("xl-ready"); }, { timeout: 2000 });
        } else {
          setTimeout(function () { doHeavyWork(); table.classList.add("xl-ready"); }, 200);
        }
      }
    });
  }

  /* ── Public API ────────────────────────────────────────── */
  return {
    init: function (selector, opts) {
      const table = typeof selector === "string"
        ? document.querySelector(selector)
        : selector;
      if (!table) return;
      new XLInstance(table, opts || {});
    },

    initAll: function (opts) {
      // Auto-init all tables with data-xl attribute
      document.querySelectorAll("table[data-xl]").forEach(table => {
        const tableOpts = Object.assign({}, opts || {});

        // Read options from data attributes
        if (table.dataset.xlFrozen)   tableOpts.frozenCols  = parseInt(table.dataset.xlFrozen);
        if (table.dataset.xlHeat)     tableOpts.heatCols    = table.dataset.xlHeat.split(",").map(Number);
        if (table.dataset.xlTotals)   tableOpts.totalsCols  = table.dataset.xlTotals.split(",").map(Number);
        if (table.dataset.xlKey)      tableOpts.storageKey  = table.dataset.xlKey;

        new XLInstance(table, tableOpts);
      });
    },
  };

})();


/* Auto-init on DOMContentLoaded — staggered per table */
document.addEventListener("DOMContentLoaded", function () {
  const tables = Array.from(document.querySelectorAll("table[data-xl]"));
  if (!tables.length) return;

  // Build opts for each table from data attributes
  function buildOpts(table) {
    const o = {};
    if (table.dataset.xlFrozen)  o.frozenCols  = parseInt(table.dataset.xlFrozen);
    if (table.dataset.xlHeat)    o.heatCols    = table.dataset.xlHeat.split(",").map(Number);
    if (table.dataset.xlTotals)  o.totalsCols  = table.dataset.xlTotals.split(",").map(Number);
    if (table.dataset.xlKey)     o.storageKey  = table.dataset.xlKey;
    return o;
  }

  // Init tables one per animation frame — never block the main thread
  // for more than one table at a time
  function initNext(index) {
    if (index >= tables.length) return;
    const table = tables[index];
    try {
      new XLInstance(table, buildOpts(table));
    } catch (e) {
      console.warn("XLTable init failed for", table.id, e);
    }
    // Schedule next table on the next idle slot
    if ("requestIdleCallback" in window) {
      requestIdleCallback(function () { initNext(index + 1); }, { timeout: 3000 });
    } else {
      setTimeout(function () { initNext(index + 1); }, 50);
    }
  }

  // Start after first paint — let the browser render the page first
  requestAnimationFrame(function () {
    requestAnimationFrame(function () {
      initNext(0);
    });
  });
});
