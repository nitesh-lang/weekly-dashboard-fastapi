/**
 * EXCEL EXPERIENCE  v2.0
 * Fixed: autofilter ID collisions, arrows always visible,
 * AMS dynamic reinit, scoped querySelector, phased init
 */

window.XLTable = (function () {
  "use strict";

  function qs(s, c)  { return (c || document).querySelector(s); }
  function qsa(s, c) { return Array.from((c || document).querySelectorAll(s)); }
  let _uid = 0;
  function uid() { return "xl" + (++_uid); }

  function parseNum(text) {
    if (!text) return NaN;
    let c = String(text).trim().replace(/[₹,%\s↑↓→]/g,"").replace(/,/g,"");
    if (!c) return NaN;
    // Indian-format suffixes: Cr = 1e7, L = 1e5, K = 1e3
    let mult = 1;
    const tail2 = c.slice(-2).toLowerCase();
    const tail1 = c.slice(-1).toLowerCase();
    if (tail2 === "cr") { mult = 1e7; c = c.slice(0, -2); }
    else if (tail1 === "l") { mult = 1e5; c = c.slice(0, -1); }
    else if (tail1 === "k") { mult = 1e3; c = c.slice(0, -1); }
    const n = parseFloat(c);
    return isNaN(n) ? NaN : n * mult;
  }
  function fmtNum(n) {
    if (isNaN(n) || n === null) return "—";
    return Math.round(n).toLocaleString("en-IN");
  }

  function XLInstance(table, opts) {
    opts = Object.assign({
      frozenCols:1, heatCols:[], totalsCols:[],
      storageKey:null, autoFilter:true,
      copyEnabled:true, statusBar:true, rowInfo:true,
    }, opts);

    const tbody = table.tBodies[0];
    const thead = table.tHead;
    if (!tbody || !thead) return;

    const iid = uid(); // unique per instance — no ID collisions

    let wrapper = table.closest(".table-wrapper,.ds-table-wrap,.table-wrapper-sm");
    if (!wrapper) {
      wrapper = document.createElement("div");
      wrapper.className = "table-wrapper";
      table.parentNode.insertBefore(wrapper, table);
      wrapper.appendChild(table);
    }

    table.classList.add("ds-excel");

    const filterState = new Map();
    let sortCol = null, sortDir = null, lastSelectedRow = null;

    function cacheIdx() {
      Array.from(tbody.rows).forEach((r,i) => { r._xlIdx = i; });
    }
    cacheIdx();

    /* ── FREEZE PANES ── */
    function applyFreeze() {
      const cols = opts.frozenCols;
      if (!cols) return;
      let left = 0;
      for (let c = 0; c < cols; c++) {
        const hc = thead.rows[0] && thead.rows[0].cells[c];
        if (!hc) break;
        const w = hc.offsetWidth || 120;
        qsa("tr", thead).forEach(row => {
          const cell = row.cells[c];
          if (cell) { cell.style.position="sticky"; cell.style.left=left+"px"; cell.style.zIndex="12"; cell.style.background="#f0f2f5"; }
        });
        qsa("tr", tbody).forEach(row => {
          const cell = row.cells[c];
          if (cell) { cell.style.position="sticky"; cell.style.left=left+"px"; cell.style.zIndex="3"; cell.style.background="inherit"; }
        });
        left += w;
      }
      wrapper.addEventListener("scroll", function() {
        const sh = this.scrollLeft > 2 ? "3px 0 6px rgba(0,0,0,0.09)" : "";
        qsa("[style*='position: sticky'],[style*='position:sticky']", table).forEach(c => c.style.boxShadow = sh);
      }, {passive:true});
    }

    /* ── AUTOFILTER ── */
    // One shared dropdown per instance, with instance-scoped IDs
    const dropdown = document.createElement("div");
    dropdown.className = "xl-dropdown";
    dropdown.id = "xl-dd-" + iid;
    document.body.appendChild(dropdown);

    let activeDropdown = null, activeColIdx = null;

    function getColValues(colIdx) {
      const vals = new Set();
      Array.from(tbody.rows).forEach(row => {
        const cell = row.cells[colIdx];
        const v = cell ? cell.innerText.trim() : "";
        if (v && v !== "—") vals.add(v);
      });
      return Array.from(vals).sort((a,b) => {
        const na=parseNum(a), nb=parseNum(b);
        if (!isNaN(na)&&!isNaN(nb)) return na-nb;
        return a.localeCompare(b);
      });
    }

    function openDropdown(colIdx, anchorEl) {
      activeColIdx = colIdx;
      const selected = filterState.get(colIdx) || new Set();
      const allVals  = getColValues(colIdx);

      // All IDs scoped to this instance
      const sId = "xls-"+iid, lId = "xll-"+iid, aId = "xla-"+iid, okId = "xlok-"+iid, cnId = "xlcn-"+iid;

      dropdown.innerHTML = `
        <div class="xl-dd-search"><input id="${sId}" type="text" placeholder="Search…" autocomplete="off"></div>
        <div class="xl-dd-list" id="${lId}">
          <label class="xl-dd-item xl-dd-selectall">
            <input type="checkbox" id="${aId}" ${selected.size===0?"checked":""}><span>(Select All)</span>
          </label>
          ${allVals.length === 0
            ? '<div style="padding:8px 14px;color:#9ca3af;font-size:11px">No data in this column</div>'
            : allVals.map(v => {
                const e = v.replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/</g,"&lt;");
                const chk = (selected.size===0||selected.has(v)) ? "checked" : "";
                return `<label class="xl-dd-item" data-val="${e}"><input type="checkbox" value="${e}" ${chk}><span>${e}</span></label>`;
              }).join("")
          }
        </div>
        <div class="xl-dd-footer">
          <button class="xl-dd-btn" id="${cnId}">Cancel</button>
          <button class="xl-dd-btn xl-dd-ok" id="${okId}">OK</button>
        </div>`;

      // Position
      const rect = anchorEl.getBoundingClientRect();
      let left = rect.left;
      if (left + 220 > window.innerWidth - 8) left = window.innerWidth - 228;
      dropdown.style.top  = (rect.bottom + window.scrollY + 2) + "px";
      dropdown.style.left = left + "px";
      dropdown.classList.add("open");
      activeDropdown = dropdown;

      // Scope all queries to THIS dropdown
      const dd = dropdown;
      const searchInp = qs("#"+sId, dd);
      const listEl    = qs("#"+lId, dd);
      const allCb     = qs("#"+aId, dd);

      searchInp.focus();

      searchInp.addEventListener("input", function() {
        const q = this.value.toLowerCase();
        qsa(".xl-dd-item:not(.xl-dd-selectall)", listEl).forEach(item => {
          item.style.display = (item.dataset.val||"").toLowerCase().includes(q) ? "" : "none";
        });
      });

      allCb.addEventListener("change", function() {
        qsa("input[type='checkbox']", listEl).forEach(cb => cb.checked = this.checked);
      });

      listEl.addEventListener("change", function(e) {
        if (e.target === allCb) return;
        allCb.checked = qsa("input[type='checkbox']:not(#"+aId+")", listEl).every(cb => cb.checked);
      });

      qs("#"+okId, dd).addEventListener("click", function() {
        const checked = qsa("input[type='checkbox']:not(#"+aId+")", listEl)
          .filter(cb => cb.checked).map(cb => cb.value);
        const all = getColValues(colIdx);
        if (!checked.length || checked.length === all.length) {
          filterState.delete(colIdx);
        } else {
          filterState.set(colIdx, new Set(checked));
        }
        closeDropdown();
        applyFilters();
        updateFilterArrows();
      });

      qs("#"+cnId, dd).addEventListener("click", closeDropdown);
    }

    function closeDropdown() {
      if (activeDropdown) { activeDropdown.classList.remove("open"); activeDropdown=null; activeColIdx=null; }
    }

    document.addEventListener("click", function(e) {
      if (!activeDropdown) return;
      if (activeDropdown.contains(e.target)) return;
      if (e.target.closest(".xl-filter-arrow")) return;
      closeDropdown();
    });

    function injectFilterArrows() {
      if (!opts.autoFilter) return;
      const hr = thead.rows[0];
      if (!hr) return;
      Array.from(hr.cells).forEach((th, colIdx) => {
        if (th.querySelector(".xl-th-inner")) return; // already done
        if (th.querySelector("input[type='checkbox']")) return;
        const label = th.textContent.trim();
        th.innerHTML = `<div class="xl-th-inner">
          <span class="xl-th-label" style="cursor:pointer;flex:1;overflow:hidden;text-overflow:ellipsis">${label}</span>
          <span class="xl-filter-arrow" data-col="${colIdx}" title="AutoFilter"></span>
        </div>`;
        th.querySelector(".xl-filter-arrow").addEventListener("click", function(e) {
          e.stopPropagation();
          (activeColIdx===colIdx && activeDropdown) ? closeDropdown() : openDropdown(colIdx, this);
        });
        th.querySelector(".xl-th-label").addEventListener("click", () => sortByCol(colIdx));
      });
    }

    function updateFilterArrows() {
      qsa(".xl-filter-arrow", thead).forEach(arrow => {
        const ci = parseInt(arrow.dataset.col, 10);
        const th = thead.rows[0] && thead.rows[0].cells[ci];
        if (!th) return;
        if (filterState.has(ci)) { th.classList.add("xl-filter-active"); arrow.title="Filter active"; }
        else                     { th.classList.remove("xl-filter-active"); arrow.title="AutoFilter"; }
      });
    }

    /* ── FILTER APPLICATION ── */
    function applyFilters() {
      let vis = 0, tot = tbody.rows.length;
      Array.from(tbody.rows).forEach(row => {
        let show = true;
        filterState.forEach((allowed, ci) => {
          const v = (row.cells[ci] ? row.cells[ci].innerText.trim() : "");
          if (!allowed.has(v)) show = false;
        });
        row.style.display = show ? "" : "none";
        if (show) vis++;
      });
      updateRowInfo(vis, tot);
      updateStatusBar();
    }

    function clearAllFilters() {
      filterState.clear();
      Array.from(tbody.rows).forEach(r => r.style.display = "");
      updateFilterArrows();
      updateRowInfo(tbody.rows.length, tbody.rows.length);
      updateStatusBar();
    }

    /* ── SORT ── */
    function sortByCol(colIdx) {
      sortDir = (sortCol===colIdx && sortDir==="desc") ? "asc" : "desc";
      sortCol = colIdx;
      const rows = Array.from(tbody.rows);
      rows.sort((a,b) => {
        const rA = (a.cells[colIdx]?.dataset?.sort ?? a.cells[colIdx]?.innerText ?? "").trim();
        const rB = (b.cells[colIdx]?.dataset?.sort ?? b.cells[colIdx]?.innerText ?? "").trim();
        const nA = parseNum(rA), nB = parseNum(rB);
        if (!isNaN(nA)&&!isNaN(nB)) { if (nA!==nB) return sortDir==="desc"?nB-nA:nA-nB; return a._xlIdx-b._xlIdx; }
        if (!rA) return 1; if (!rB) return -1;
        return rA.toLowerCase().localeCompare(rB.toLowerCase()) * (sortDir==="desc"?-1:1);
      });
      rows.forEach(r => tbody.appendChild(r));
      qsa("th", thead.rows[0]).forEach((th,i) => {
        th.classList.remove("xl-asc","xl-desc");
        if (i===colIdx) th.classList.add(sortDir==="asc"?"xl-asc":"xl-desc");
      });
      if (opts.storageKey) { try { sessionStorage.setItem("xl-sort-"+opts.storageKey, JSON.stringify({col:colIdx,dir:sortDir})); } catch(e){} }
    }

    function restoreSort() {
      if (!opts.storageKey) return;
      try {
        const s = JSON.parse(sessionStorage.getItem("xl-sort-"+opts.storageKey));
        if (!s||s.col===undefined) return;
        sortCol=s.col; sortDir=s.dir;
        const rows = Array.from(tbody.rows);
        rows.sort((a,b) => {
          const rA=(a.cells[sortCol]?.innerText??"").trim(), rB=(b.cells[sortCol]?.innerText??"").trim();
          const nA=parseNum(rA), nB=parseNum(rB);
          if (!isNaN(nA)&&!isNaN(nB)) return sortDir==="desc"?nB-nA:nA-nB;
          return rA.localeCompare(rB)*(sortDir==="desc"?-1:1);
        });
        rows.forEach(r => tbody.appendChild(r));
        const th = thead.rows[0]&&thead.rows[0].cells[sortCol];
        if (th) th.classList.add(sortDir==="asc"?"xl-asc":"xl-desc");
      } catch(e){}
    }

    /* ── ROW SELECTION ── */
    function getVisible() { return Array.from(tbody.rows).filter(r=>r.style.display!=="none"); }

    tbody.addEventListener("click", function(e) {
      const row = e.target.closest("tr");
      if (!row||!tbody.contains(row)) return;
      if (e.target.closest("a,button,input,select")) return;
      const vis = getVisible();
      if (e.shiftKey && lastSelectedRow) {
        const iA=vis.indexOf(lastSelectedRow), iB=vis.indexOf(row);
        const [lo,hi]=[Math.min(iA,iB),Math.max(iA,iB)];
        if (!e.ctrlKey&&!e.metaKey) qsa("tr.xl-selected",tbody).forEach(r=>r.classList.remove("xl-selected"));
        vis.slice(lo,hi+1).forEach(r=>r.classList.add("xl-selected"));
      } else if (e.ctrlKey||e.metaKey) {
        row.classList.toggle("xl-selected");
      } else {
        qsa("tr.xl-selected",tbody).forEach(r=>r.classList.remove("xl-selected"));
        row.classList.add("xl-selected");
      }
      lastSelectedRow=row; updateStatusBar();
    });

    /* ── COPY ── */
    if (opts.copyEnabled) {
      document.addEventListener("keydown", function(e) {
        if (!(e.ctrlKey||e.metaKey)||e.key!=="c") return;
        const f=document.activeElement;
        if (f&&(f.tagName==="INPUT"||f.tagName==="TEXTAREA")) return;
        const sel=qsa("tr.xl-selected",tbody);
        if (!sel.length) return;
        e.preventDefault(); copyTSV(sel,false);
      });
    }

    function copyTSV(rows, withHeaders) {
      const lines=[];
      if (withHeaders) lines.push(Array.from(thead.rows[0].cells).map(th=>(th.querySelector(".xl-th-label")||th).innerText.trim()).join("\t"));
      rows.forEach(row=>lines.push(Array.from(row.cells).map(td=>td.innerText.trim().replace(/\t/g," ")).join("\t")));
      if (navigator.clipboard) navigator.clipboard.writeText(lines.join("\n")).then(()=>{
        showToast(rows.length+" row"+(rows.length>1?"s":"")+" copied — paste into Excel");
        rows.forEach(r=>{r.classList.add("xl-copy-flash");setTimeout(()=>r.classList.remove("xl-copy-flash"),400);});
      });
    }

    let _toast=null;
    function showToast(msg) {
      if (!_toast) { _toast=document.createElement("div"); _toast.className="xl-copy-toast"; document.body.appendChild(_toast); }
      _toast.textContent=msg; _toast.classList.add("show");
      clearTimeout(_toast._t); _toast._t=setTimeout(()=>_toast.classList.remove("show"),2200);
    }

    /* ── CONTEXT MENU ── */
    const ctx=document.createElement("div");
    ctx.className="xl-context-menu";
    ctx.innerHTML=`
      <div class="xl-ctx-item" data-a="copy"><span class="xl-ctx-icon">⎘</span>Copy rows<span class="xl-ctx-kbd">Ctrl+C</span></div>
      <div class="xl-ctx-item" data-a="copyh"><span class="xl-ctx-icon">⎘</span>Copy with headers</div>
      <div class="xl-ctx-sep"></div>
      <div class="xl-ctx-item" data-a="sela"><span class="xl-ctx-icon">▣</span>Select all visible<span class="xl-ctx-kbd">Ctrl+A</span></div>
      <div class="xl-ctx-item" data-a="selc"><span class="xl-ctx-icon">✕</span>Clear selection</div>
      <div class="xl-ctx-sep"></div>
      <div class="xl-ctx-item" data-a="clf"><span class="xl-ctx-icon">⊘</span>Clear all filters</div>`;
    document.body.appendChild(ctx);

    table.addEventListener("contextmenu", function(e) {
      e.preventDefault();
      const row=e.target.closest("tr");
      if (row&&tbody.contains(row)&&!row.classList.contains("xl-selected")) {
        qsa("tr.xl-selected",tbody).forEach(r=>r.classList.remove("xl-selected"));
        row.classList.add("xl-selected"); lastSelectedRow=row;
      }
      ctx.style.cssText=`display:block;top:${e.clientY+window.scrollY}px;left:${Math.min(e.clientX,window.innerWidth-180)}px`;
      ctx.classList.add("open");
    });
    ctx.addEventListener("click", function(e) {
      const item=e.target.closest("[data-a]"); if(!item) return;
      const sel=qsa("tr.xl-selected",tbody);
      const a=item.dataset.a;
      if (a==="copy")  copyTSV(sel,false);
      if (a==="copyh") copyTSV(sel,true);
      if (a==="sela")  { getVisible().forEach(r=>r.classList.add("xl-selected")); updateStatusBar(); }
      if (a==="selc")  { qsa("tr.xl-selected",tbody).forEach(r=>r.classList.remove("xl-selected")); updateStatusBar(); }
      if (a==="clf")   clearAllFilters();
      ctx.classList.remove("open");
    });
    document.addEventListener("click", function(e) { if (!ctx.contains(e.target)) ctx.classList.remove("open"); });
    document.addEventListener("keydown", function(e) {
      if ((e.ctrlKey||e.metaKey)&&e.key==="a") {
        if (document.activeElement?.tagName==="INPUT") return;
        e.preventDefault(); getVisible().forEach(r=>r.classList.add("xl-selected")); updateStatusBar();
      }
    });

    /* ── STATUS BAR ── */
    let statusBar=null;
    function initStatusBar() {
      if (!opts.statusBar) return;
      statusBar=document.createElement("div"); statusBar.className="xl-status-bar";
      wrapper.parentNode.insertBefore(statusBar, wrapper.nextSibling);
      updateStatusBar();
    }
    function updateStatusBar() {
      if (!statusBar) return;
      const sel=qsa("tr.xl-selected",tbody);
      const vis=getVisible().length, tot=tbody.rows.length;
      let p=[`<span class="xl-status-item"><span class="label">ROWS</span><span class="value">${vis}${vis<tot?" / "+tot:""}</span></span>`,`<span class="xl-status-sep">|</span>`];
      if (sel.length>0) {
        p.push(`<span class="xl-status-item"><span class="label">SEL</span><span class="value">${sel.length}</span></span>`);
        const sums={};
        sel.forEach(row=>Array.from(row.cells).forEach((c,i)=>{const n=parseNum(c.innerText);if(!isNaN(n))sums[i]=(sums[i]||0)+n;}));
        const keys=Object.keys(sums).slice(0,3);
        if (keys.length) {
          p.push(`<span class="xl-status-sep">|</span>`);
          keys.forEach(i=>{
            const th=thead.rows[0]&&thead.rows[0].cells[parseInt(i)];
            const lbl=((th?.querySelector(".xl-th-label")||th)?.innerText||"").trim().slice(0,10);
            p.push(`<span class="xl-status-item"><span class="label">Σ ${lbl}</span><span class="value">${fmtNum(sums[i])}</span></span>`);
          });
        }
        p.push(`<span class="xl-status-sep">|</span><span class="xl-status-item" style="cursor:pointer;opacity:0.65" onclick="document.querySelectorAll('#${table.id||iid} tr.xl-selected').forEach(r=>r.classList.remove('xl-selected'))">✕ Clear</span>`);
      } else {
        p.push(`<span class="xl-status-item" style="opacity:0.45;font-size:10px">Click rows · Shift+click range · Ctrl+C copy · Right-click menu</span>`);
      }
      statusBar.innerHTML=p.join("");
    }

    /* ── ROW INFO BAR ── */
    let rowInfoEl=null;
    function initRowInfoBar() {
      if (!opts.rowInfo) return;
      rowInfoEl=document.createElement("div"); rowInfoEl.className="xl-row-info";
      wrapper.parentNode.insertBefore(rowInfoEl, wrapper);
      updateRowInfo(tbody.rows.length, tbody.rows.length);
    }
    function updateRowInfo(vis, tot) {
      if (!rowInfoEl) return;
      if (vis===undefined) vis=getVisible().length;
      if (tot===undefined) tot=tbody.rows.length;
      rowInfoEl.innerHTML = filterState.size>0
        ? `Showing <span class="xl-filtered">${vis}</span> of ${tot} rows &nbsp;·&nbsp; <a class="xl-clear-filters" href="#">Clear filters</a>`
        : `${tot} row${tot!==1?"s":""}`;
      const cl=rowInfoEl.querySelector(".xl-clear-filters");
      if (cl) cl.addEventListener("click", e=>{e.preventDefault();clearAllFilters();});
    }

    /* ── HEATMAP ── */
    function applyHeatmap() {
      opts.heatCols.forEach(ci=>{
        const rows=Array.from(tbody.rows);
        const vals=rows.map(r=>parseNum(r.cells[ci]?.innerText)).filter(v=>!isNaN(v));
        if (!vals.length) return;
        const min=Math.min(...vals),max=Math.max(...vals),range=max-min||1;
        rows.forEach(row=>{
          const cell=row.cells[ci]; if (!cell) return;
          const v=parseNum(cell.innerText); if (isNaN(v)) return;
          const pct=((v-min)/range)*100;
          cell.classList.remove("ds-heat-best","ds-heat-good","ds-heat-mid","ds-heat-bad","ds-heat-worst");
          if      (pct>=80) cell.classList.add("ds-heat-best");
          else if (pct>=60) cell.classList.add("ds-heat-good");
          else if (pct>=40) cell.classList.add("ds-heat-mid");
          else if (pct>=20) cell.classList.add("ds-heat-bad");
          else              cell.classList.add("ds-heat-worst");
        });
      });
    }

    /* ── TOTALS ROW ── */
    function buildTotalsRow(force) {
      if (!opts.totalsCols.length) return;
      const ex=table.tFoot; if (ex&&!force) return; if (ex) ex.remove();
      const rows=Array.from(tbody.rows), colCount=thead.rows[0]?thead.rows[0].cells.length:0;
      const sums={};
      rows.forEach(row=>opts.totalsCols.forEach(c=>{const n=parseNum(row.cells[c]?.innerText);if(!isNaN(n))sums[c]=(sums[c]||0)+n;}));
      const tfoot=table.createTFoot(), tr=tfoot.insertRow(); tr.className="xl-total";
      for (let i=0;i<colCount;i++) {
        const td=tr.insertCell();
        if (i===0) td.innerHTML='<span class="ds-total-label">TOTAL</span>';
        else if (sums[i]!==undefined) { td.textContent=fmtNum(sums[i]); td.className="xl-num"; }
      }
    }

    /* ── NUMBER FORMATTING ── */
    function formatNumbers() {
      Array.from(tbody.rows).forEach(row=>Array.from(row.cells).forEach(cell=>{
        if (cell.querySelector("span,a,input")) return;
        const n=parseNum(cell.innerText); if (isNaN(n)) return;
        cell.classList.add("xl-num"); if (n<0) cell.classList.add("xl-neg");
      }));
    }

    /* ── PUBLIC REINIT (for dynamic tables like AMS) ── */
    table._xlReinit = function() {
      cacheIdx(); injectFilterArrows();
      filterState.clear(); applyFilters(); updateFilterArrows();
      buildTotalsRow(true); applyHeatmap(); updateRowInfo();
    };

    /* ── PHASED INIT ── */
    // Phase 1 — immediate: visual structure
    applyFreeze();
    injectFilterArrows();   // arrows visible immediately, no hiding
    initRowInfoBar();

    // Phase 2 — after first paint
    requestAnimationFrame(function() {
      initStatusBar();

      // Phase 3 — idle, only when table is near viewport
      const heavy = function() {
        buildTotalsRow(); applyHeatmap(); formatNumbers();
        restoreSort(); applyFilters();
        table.classList.add("xl-ready");
      };

      if ("IntersectionObserver" in window) {
        const obs = new IntersectionObserver(function(entries, o) {
          if (entries[0].isIntersecting) {
            o.disconnect();
            "requestIdleCallback" in window
              ? requestIdleCallback(heavy, {timeout:2000})
              : setTimeout(heavy, 100);
          }
        }, {rootMargin:"300px"});
        obs.observe(table);
      } else {
        "requestIdleCallback" in window
          ? requestIdleCallback(heavy, {timeout:2000})
          : setTimeout(heavy, 200);
      }
    });
  }

  return {
    init: function(selector, opts) {
      const el = typeof selector==="string" ? document.querySelector(selector) : selector;
      if (el) new XLInstance(el, opts||{});
    },
    reinit: function(selector) {
      const el = typeof selector==="string" ? document.querySelector(selector) : selector;
      if (el && el._xlReinit) el._xlReinit();
    },
  };

})();


/* ── AUTO-INIT: staggered, never blocks paint ── */
document.addEventListener("DOMContentLoaded", function() {
  const tables = Array.from(document.querySelectorAll("table[data-xl]"));
  if (!tables.length) return;

  function buildOpts(t) {
    const o={};
    if (t.dataset.xlFrozen)  o.frozenCols = parseInt(t.dataset.xlFrozen);
    if (t.dataset.xlHeat)    o.heatCols   = t.dataset.xlHeat.split(",").map(Number);
    if (t.dataset.xlTotals)  o.totalsCols = t.dataset.xlTotals.split(",").map(Number);
    if (t.dataset.xlKey)     o.storageKey = t.dataset.xlKey;
    return o;
  }

  // Double rAF = after first paint, then stagger 60ms per table
  requestAnimationFrame(function() {
    requestAnimationFrame(function() {
      tables.forEach(function(t, i) {
        setTimeout(function() {
          try { XLTable.init(t, buildOpts(t)); }
          catch(e) { console.warn("XLTable init:", t.id, e); }
        }, i * 60);
      });
    });
  });
});
