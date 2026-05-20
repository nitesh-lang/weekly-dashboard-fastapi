/**
 * DASHBOARD CORE  v1.0
 * Single lean file replacing excel_experience.js + dashboard_standards.js
 * No double work. Paint-first. Scroll-loads remaining rows.
 */
(function(){
"use strict";

/* ─── UTILS ──────────────────────────────────────────────── */
var _uid = 0;
function uid(){ return "dc"+(++_uid); }
function qs(s,c){ return (c||document).querySelector(s); }
function qsa(s,c){ return Array.from((c||document).querySelectorAll(s)); }
function parseNum(t){
  if(!t) return NaN;
  // Strip currency, %, whitespace, commas, arrows
  var c=String(t).trim().replace(/[₹,%\s↑↓→]/g,"").replace(/,/g,"");
  if(!c) return NaN;
  // Handle Indian-format suffixes: Cr = 1e7, L/Lac/Lakh = 1e5, K = 1e3
  var mult=1;
  var tail2=c.slice(-2).toLowerCase();
  var tail1=c.slice(-1).toLowerCase();
  if(tail2==="cr"){ mult=1e7; c=c.slice(0,-2); }
  else if(tail1==="l"){ mult=1e5; c=c.slice(0,-1); }
  else if(tail1==="k"){ mult=1e3; c=c.slice(0,-1); }
  var n=parseFloat(c); return isNaN(n)?NaN:n*mult;
}
function fmtNum(n){
  if(isNaN(n)||n===null) return "—";
  return Math.round(n).toLocaleString("en-IN");
}
function idle(fn){ "requestIdleCallback"in window?requestIdleCallback(fn,{timeout:2000}):setTimeout(fn,100); }
function raf2(fn){ requestAnimationFrame(function(){ requestAnimationFrame(fn); }); }

/* ─── INFINITE SCROLL LOADER ─────────────────────────────── */
function initInfiniteScroll(table){
  var apiUrl   = table.dataset.apiUrl;
  var apiParams= table.dataset.apiParams || "";
  var total    = parseInt(table.dataset.totalRows||"0",10);
  var loaded   = parseInt(table.dataset.loadedRows||"0",10);
  if(!apiUrl || loaded >= total) return;

  var tbody   = table.tBodies[0];
  var page    = 2;
  var loading = false;

  // Find the scroll container — the table is inside .table-wrapper with overflow:auto
  var scrollEl = table.closest(".table-wrapper,.table-wrapper-sm") || document.documentElement;

  // Sentinel row appended at the bottom of tbody
  var sentinel = document.createElement("tr");
  sentinel.className = "dc-sentinel";
  sentinel.innerHTML = '<td colspan="99" style="text-align:center;padding:12px;color:#9ca3af;font-size:11px;font-style:italic">Loading more rows…</td>';
  tbody.appendChild(sentinel);

  function loadNext(){
    if(loading || loaded >= total) return;
    loading = true;
    var url = apiUrl+"?page="+page+"&page_size=100"+(apiParams?"&"+apiParams:"");
    fetch(url)
      .then(function(r){ return r.json(); })
      .then(function(data){
        if(!data.rows||!data.rows.length){ sentinel.remove(); loading=false; return; }
        appendRows(table, data.rows);
        loaded += data.rows.length;
        table.dataset.loadedRows = loaded;
        page++;
        loading = false;
        if(!data.has_more || loaded >= total){
          sentinel.remove();
        } else {
          tbody.appendChild(sentinel); // keep sentinel at bottom
        }
      })
      .catch(function(){ loading=false; });
  }

  // PRIMARY: IntersectionObserver with scroll container as root
  // This fires when sentinel enters the visible area of the scroll container
  var obsOpts = { root: scrollEl === document.documentElement ? null : scrollEl, rootMargin: "200px", threshold: 0 };
  var obs = new IntersectionObserver(function(entries){
    if(entries[0].isIntersecting) loadNext();
  }, obsOpts);
  obs.observe(sentinel);

  // FALLBACK: scroll listener on the container in case observer root doesn't fire
  function onScroll(){
    if(loading || loaded >= total) return;
    var rect = sentinel.getBoundingClientRect();
    var containerRect = scrollEl === document.documentElement
      ? { top:0, bottom: window.innerHeight }
      : scrollEl.getBoundingClientRect();
    if(rect.top <= containerRect.bottom + 300) loadNext();
  }
  scrollEl.addEventListener("scroll", onScroll, {passive:true});

  // SAFETY NET: auto-load all remaining rows even if scroll/observer never fires
  // Waits for each fetch to complete before triggering next one
  setTimeout(function autoLoad(){
    if(loaded >= total) return;
    if(!loading){ loadNext(); }
    setTimeout(autoLoad, 600);
  }, 400);
}

function appendRows(table, rows){
  var tbody  = table.tBodies[0];
  var thead  = table.tHead;
  var cols   = thead && thead.rows[0] ? Array.from(thead.rows[0].cells).map(function(th){
    return (th.querySelector(".dc-th-label")||th).innerText.trim();
  }) : [];

  // Get column order from existing rows for key mapping
  var keyMap = table.dataset.colKeys ? JSON.parse(table.dataset.colKeys) : null;
  var frag = document.createDocumentFragment();

  rows.forEach(function(r){
    var tr = document.createElement("tr");
    if(keyMap){
      keyMap.forEach(function(key){
        var td = document.createElement("td");
        var v = r[key];
        td.textContent = (v===null||v===undefined)?"—":v;
        var n = parseNum(String(v));
        if(!isNaN(n)){ td.className="dc-num"; if(n<0) td.classList.add("dc-neg"); }
        tr.appendChild(td);
      });
    } else {
      // Fallback: render values in object order
      Object.values(r).forEach(function(v){
        var td = document.createElement("td");
        td.textContent = (v===null||v===undefined)?"—":v;
        var n = parseNum(String(v));
        if(!isNaN(n)){ td.className="dc-num"; if(n<0) td.classList.add("dc-neg"); }
        tr.appendChild(td);
      });
    }
    frag.appendChild(tr);
  });
  tbody.appendChild(frag);
  // Re-apply any active filter so newly loaded rows respect current search terms
  if(typeof window._invFilterRefresh === "function") window._invFilterRefresh();
  // For dashboard_core autofilter tables, re-apply current filter without resetting state
  if(table._applyCurrentFilter) table._applyCurrentFilter();
}

/* ─── AUTOFILTER ─────────────────────────────────────────── */
function initAutoFilter(table){
  var thead = table.tHead;
  var tbody = table.tBodies[0];
  if(!thead||!tbody) return;

  var iid        = uid();
  var filterState= new Map();
  var dropdown   = document.createElement("div");
  dropdown.className = "dc-dropdown"; dropdown.id="dc-dd-"+iid;
  document.body.appendChild(dropdown);
  var activeDd=null, activeCol=null;

  // Inject header UI
  var hr = thead.rows[0];
  Array.from(hr.cells).forEach(function(th,ci){
    if(th.querySelector(".dc-th-inner")) return;
    if(th.querySelector("input[type='checkbox']")) return;
    var label = th.textContent.trim();
    th.innerHTML='<div class="dc-th-inner"><span class="dc-th-label">'+label+'</span><span class="dc-fa" data-ci="'+ci+'" title="Filter">▾</span></div>';
    th.querySelector(".dc-th-label").onclick=function(){ sortBy(table,ci); };
    th.querySelector(".dc-fa").onclick=function(e){
      e.stopPropagation();
      activeCol===ci&&activeDd?closeDd():openDd(ci,this);
    };
  });

  function getVals(ci){
    var s=new Set();
    Array.from(tbody.rows).forEach(function(row){
      var c=row.cells[ci]; var v=c?c.innerText.trim():"";
      if(v&&v!=="—") s.add(v);
    });
    return Array.from(s).sort(function(a,b){
      var na=parseNum(a),nb=parseNum(b);
      return(!isNaN(na)&&!isNaN(nb))?na-nb:a.localeCompare(b);
    });
  }

  function openDd(ci, anchor){
    activeCol=ci;
    var sel=filterState.get(ci)||new Set();
    var vals=getVals(ci);
    var sId="dcs"+iid,lId="dcl"+iid,aId="dca"+iid,okId="dcok"+iid,cnId="dccn"+iid;
    dropdown.innerHTML=
      '<div class="dc-dd-search"><input id="'+sId+'" type="text" placeholder="Search…" autocomplete="off"></div>'+
      '<div class="dc-dd-list" id="'+lId+'">'+
        '<label class="dc-dd-item dc-dd-all"><input type="checkbox" id="'+aId+'" '+(sel.size===0?"checked":"")+'>  (Select All)</label>'+
        (vals.length===0?'<div class="dc-dd-empty">No values</div>':
          vals.map(function(v){
            var e=v.replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/</g,"&lt;");
            return'<label class="dc-dd-item" data-v="'+e+'"><input type="checkbox" value="'+e+'" '+(sel.size===0||sel.has(v)?"checked":"")+'>'+e+'</label>';
          }).join(""))+
      '</div>'+
      '<div class="dc-dd-footer"><button class="dc-dd-btn" id="'+cnId+'">Cancel</button><button class="dc-dd-btn dc-dd-ok" id="'+okId+'">OK</button></div>';

    var rect=anchor.getBoundingClientRect();
    var left=Math.min(rect.left,window.innerWidth-230);
    // position:fixed — use viewport coords directly, no scrollY needed
    dropdown.style.cssText="display:flex;flex-direction:column;top:"+(rect.bottom+4)+"px;left:"+left+"px;position:fixed;z-index:9999;";
    activeDd=dropdown;

    var dd=dropdown,si=qs("#"+sId,dd),li=qs("#"+lId,dd),ai=qs("#"+aId,dd);
    si.focus();
    si.oninput=function(){
      var q=this.value.toLowerCase();
      qsa(".dc-dd-item:not(.dc-dd-all)",li).forEach(function(it){it.style.display=it.dataset.v.toLowerCase().includes(q)?"":"none";});
    };
    ai.onchange=function(){qsa("input[type='checkbox']",li).forEach(function(cb){cb.checked=ai.checked;});};
    li.onchange=function(e){if(e.target!==ai){ai.checked=qsa("input[type='checkbox']:not(#"+aId+")",li).every(function(cb){return cb.checked;});}};
    qs("#"+okId,dd).onclick=function(){
      var all=getVals(ci);
      var chk=qsa("input[type='checkbox']:not(#"+aId+")",li).filter(function(cb){return cb.checked;}).map(function(cb){return cb.value;});
      (!chk.length||chk.length===all.length)?filterState.delete(ci):filterState.set(ci,new Set(chk));
      closeDd(); applyFilter(table,filterState);
      // Update arrow indicator
      var fa=qs(".dc-fa[data-ci='"+ci+"']",thead);
      if(fa) fa.classList.toggle("dc-fa-active",filterState.has(ci));
    };
    qs("#"+cnId,dd).onclick=closeDd;
  }

  function closeDd(){ if(activeDd){activeDd.style.display="none";activeDd=null;activeCol=null;} }
  document.addEventListener("click",function(e){
    if(!activeDd) return;
    if(activeDd.contains(e.target)||e.target.closest(".dc-fa")) return;
    closeDd();
  });
  // Close on scroll - fixed dropdown doesn't follow the anchor
  window.addEventListener("scroll",closeDd,{passive:true});
  document.querySelectorAll(".table-wrapper,.table-wrapper-sm").forEach(function(el){
    el.addEventListener("scroll",closeDd,{passive:true});
  });

  // Public: rebuild after dynamic row injection
  table._dcReinit=function(){
    filterState.clear();
    initFreeze(table); // re-apply sticky to newly injected rows
    // Re-inject arrows for any new header cells
    Array.from(thead.rows[0].cells).forEach(function(th,ci){
      if(th.querySelector(".dc-th-inner")) return;
      if(th.querySelector("input[type='checkbox']")) return;
      var label=th.textContent.trim();
      th.innerHTML='<div class="dc-th-inner"><span class="dc-th-label">'+label+'</span><span class="dc-fa" data-ci="'+ci+'" title="Filter">▾</span></div>';
      th.querySelector(".dc-th-label").onclick=function(){ sortBy(table,ci); };
      th.querySelector(".dc-fa").onclick=function(e){e.stopPropagation();activeCol===ci&&activeDd?closeDd():openDd(ci,this);};
    });
    applyFilter(table,filterState);
  };
  // Re-apply current filter state to newly appended rows without clearing
  table._applyCurrentFilter=function(){
    if(filterState.size>0) applyFilter(table,filterState);
  };
}

function applyFilter(table, filterState){
  var tbody=table.tBodies[0]; if(!tbody) return;
  var vis=0,tot=0;
  Array.from(tbody.rows).forEach(function(row){
    if(row.classList.contains("dc-sentinel")||row.classList.contains("xl-total")){return;}
    tot++;
    var show=true;
    filterState.forEach(function(allowed,ci){
      var c=row.cells[ci]; var v=c?c.innerText.trim():"";
      if(!allowed.has(v)) show=false;
    });
    row.style.display=show?"":"none";
    if(show) vis++;
  });
  // Update row-info bar - use stored reference
  var info=table._rowInfo;
  if(info){
    info.innerHTML=filterState.size>0
      ?"Showing <b>"+vis+"</b> of "+tot+" rows &nbsp;·&nbsp; <a class='dc-clear-f' href='#'>Clear filters</a>"
      :tot+" rows";
    var cl=info.querySelector(".dc-clear-f");
    if(cl) cl.onclick=function(e){e.preventDefault();filterState.clear();applyFilter(table,filterState);qsa(".dc-fa-active",table).forEach(function(f){f.classList.remove("dc-fa-active");});};
  }
}

/* ─── SORT ───────────────────────────────────────────────── */
var _sortState={};
function sortBy(table,ci){
  var key=table.id||"t";
  var prev=_sortState[key]||{};
  var dir=(prev.ci===ci&&prev.dir==="desc")?"asc":"desc";
  _sortState[key]={ci:ci,dir:dir};

  var tbody=table.tBodies[0]; if(!tbody) return;
  var rows=Array.from(tbody.rows).filter(function(r){return!r.classList.contains("dc-sentinel")&&!r.classList.contains("xl-total");});
  rows.sort(function(a,b){
    var rA=(a.cells[ci]?a.cells[ci].innerText:"").trim();
    var rB=(b.cells[ci]?b.cells[ci].innerText:"").trim();
    var nA=parseNum(rA),nB=parseNum(rB);
    if(!isNaN(nA)&&!isNaN(nB)) return dir==="desc"?nB-nA:nA-nB;
    if(!rA) return 1; if(!rB) return -1;
    return dir==="desc"?-rA.localeCompare(rB):rA.localeCompare(rB);
  });
  var frag=document.createDocumentFragment();
  rows.forEach(function(r){frag.appendChild(r);});
  tbody.appendChild(frag);

  // Update sort arrows on header
  var hr=table.tHead&&table.tHead.rows[0];
  if(hr) Array.from(hr.cells).forEach(function(th,i){
    th.classList.remove("dc-asc","dc-desc");
    if(i===ci) th.classList.add(dir==="asc"?"dc-asc":"dc-desc");
  });

  // Persist to sessionStorage
  if(table.dataset.xlKey){
    try{sessionStorage.setItem("dc-sort-"+table.dataset.xlKey,JSON.stringify({ci:ci,dir:dir}));}catch(e){}
  }
}

function restoreSort(table){
  if(!table.dataset.xlKey) return;
  try{
    var s=JSON.parse(sessionStorage.getItem("dc-sort-"+table.dataset.xlKey));
    if(!s||s.ci===undefined) return;
    sortBy(table,s.ci);
  }catch(e){}
}

/* ─── ROW SELECTION + COPY ───────────────────────────────── */
function initSelection(table){
  var tbody=table.tBodies[0]; if(!tbody) return;
  var lastRow=null;

  tbody.addEventListener("click",function(e){
    var row=e.target.closest("tr");
    if(!row||!tbody.contains(row)) return;
    if(e.target.closest("a,button,input,select")) return;
    if(row.classList.contains("dc-sentinel")||row.classList.contains("xl-total")) return;
    var vis=Array.from(tbody.rows).filter(function(r){return r.style.display!=="none"&&!r.classList.contains("dc-sentinel");});
    if(e.shiftKey&&lastRow){
      var iA=vis.indexOf(lastRow),iB=vis.indexOf(row);
      var lo=Math.min(iA,iB),hi=Math.max(iA,iB);
      if(!e.ctrlKey&&!e.metaKey) qsa("tr.dc-sel",tbody).forEach(function(r){r.classList.remove("dc-sel");});
      vis.slice(lo,hi+1).forEach(function(r){r.classList.add("dc-sel");});
    } else if(e.ctrlKey||e.metaKey){
      row.classList.toggle("dc-sel");
    } else {
      qsa("tr.dc-sel",tbody).forEach(function(r){r.classList.remove("dc-sel");});
      row.classList.add("dc-sel");
    }
    lastRow=row;
    updateStatus(table);
  });

  // Ctrl+C copy
  document.addEventListener("keydown",function(e){
    if(!(e.ctrlKey||e.metaKey)||e.key!=="c") return;
    var f=document.activeElement;
    if(f&&(f.tagName==="INPUT"||f.tagName==="TEXTAREA")) return;
    var sel=qsa("tr.dc-sel",tbody);
    if(!sel.length) return;
    e.preventDefault();
    var thead=table.tHead;
    var lines=[];
    lines.push(Array.from(thead.rows[0].cells).map(function(th){return(th.querySelector(".dc-th-label")||th).innerText.trim();}).join("\t"));
    sel.forEach(function(row){lines.push(Array.from(row.cells).map(function(td){return td.innerText.trim().replace(/\t/g," ");}).join("\t"));});
    navigator.clipboard&&navigator.clipboard.writeText(lines.join("\n")).then(function(){
      showToast(sel.length+" row"+(sel.length>1?"s":"")+" copied — paste into Excel");
      sel.forEach(function(r){r.classList.add("dc-flash");setTimeout(function(){r.classList.remove("dc-flash");},350);});
    });
  });

  // Ctrl+A
  document.addEventListener("keydown",function(e){
    if(!(e.ctrlKey||e.metaKey)||e.key!=="a") return;
    if(document.activeElement&&document.activeElement.tagName==="INPUT") return;
    e.preventDefault();
    Array.from(tbody.rows).filter(function(r){return r.style.display!=="none"&&!r.classList.contains("dc-sentinel");}).forEach(function(r){r.classList.add("dc-sel");});
    updateStatus(table);
  });
}

/* ─── STATUS BAR ─────────────────────────────────────────── */
function initStatusBar(table){
  if(!table._statusBar){
    var bar=document.createElement("div");
    bar.className="dc-status";
    bar._dcTable=table;
    var wrap=table.closest(".table-wrapper,.table-wrapper-sm")||table.parentNode;
    wrap.parentNode.insertBefore(bar,wrap.nextSibling);
    table._statusBar=bar;
  }
  updateStatus(table);
}

function updateStatus(table){
  var bar=table._statusBar; if(!bar) return;
  var tbody=table.tBodies[0]; if(!tbody) return;
  var sel=qsa("tr.dc-sel",tbody);
  var vis=Array.from(tbody.rows).filter(function(r){return r.style.display!=="none"&&!r.classList.contains("dc-sentinel");}).length;
  var tot=parseInt(table.dataset.totalRows||tbody.rows.length,10);

  var html='<span class="dc-s-item"><span class="dc-s-lbl">ROWS</span><span class="dc-s-val">'+vis+(vis<tot?" / "+tot:"")+'</span></span><span class="dc-s-sep">|</span>';
  if(sel.length){
    html+='<span class="dc-s-item"><span class="dc-s-lbl">SEL</span><span class="dc-s-val">'+sel.length+'</span></span>';
    // Sum numeric columns of selection
    var sums={};
    sel.forEach(function(row){Array.from(row.cells).forEach(function(c,i){var n=parseNum(c.innerText);if(!isNaN(n))sums[i]=(sums[i]||0)+n;});});
    var keys=Object.keys(sums).slice(0,3);
    if(keys.length){
      html+='<span class="dc-s-sep">|</span>';
      keys.forEach(function(i){
        var th=table.tHead&&table.tHead.rows[0]&&table.tHead.rows[0].cells[parseInt(i)];
        var lbl=((th&&th.querySelector(".dc-th-label"))||th||{innerText:""}).innerText.trim().slice(0,10);
        html+='<span class="dc-s-item"><span class="dc-s-lbl">Σ '+lbl+'</span><span class="dc-s-val">'+fmtNum(sums[i])+'</span></span>';
      });
    }
    html+='<span class="dc-s-sep">|</span><span class="dc-s-clear" onclick="this.dispatchEvent(new CustomEvent(\'dcclear\',{bubbles:true}))">✕ Clear</span>';
  } else {
    html+='<span class="dc-s-hint">Click row · Shift+click range · Ctrl+C copy · Right-click menu</span>';
  }
  bar.innerHTML=html;
}

document.addEventListener("dcclear",function(e){
  var bar=e.target.closest(".dc-status"); if(!bar) return;
  var table=bar._dcTable;
  if(!table) return;
  qsa("tr.dc-sel",table).forEach(function(r){r.classList.remove("dc-sel");});
  updateStatus(table);
});

/* ─── RIGHT-CLICK MENU ───────────────────────────────────── */
var _ctx=null;
function getCtx(){
  if(_ctx) return _ctx;
  _ctx=document.createElement("div");
  _ctx.className="dc-ctx";
  _ctx.innerHTML=
    '<div class="dc-ctx-item" data-a="copy">⎘ Copy rows <span class="dc-ctx-kbd">Ctrl+C</span></div>'+
    '<div class="dc-ctx-item" data-a="copyh">⎘ Copy with headers</div>'+
    '<div class="dc-ctx-sep"></div>'+
    '<div class="dc-ctx-item" data-a="sela">▣ Select all visible <span class="dc-ctx-kbd">Ctrl+A</span></div>'+
    '<div class="dc-ctx-item" data-a="clr">✕ Clear selection</div>'+
    '<div class="dc-ctx-sep"></div>'+
    '<div class="dc-ctx-item" data-a="clf">⊘ Clear all filters</div>';
  document.body.appendChild(_ctx);
  document.addEventListener("click",function(e){if(!_ctx.contains(e.target))_ctx.classList.remove("open");});
  _ctx.addEventListener("click",function(e){
    var item=e.target.closest("[data-a]"); if(!item) return;
    var a=item.dataset.a, table=_ctx._table;
    if(!table) return;
    var tbody=table.tBodies[0], thead=table.tHead;
    var sel=qsa("tr.dc-sel",tbody);
    if(a==="copy"||a==="copyh"){
      var lines=[];
      if(a==="copyh") lines.push(Array.from(thead.rows[0].cells).map(function(th){return(th.querySelector(".dc-th-label")||th).innerText.trim();}).join("\t"));
      sel.forEach(function(row){lines.push(Array.from(row.cells).map(function(td){return td.innerText.trim().replace(/\t/g," ");}).join("\t"));});
      navigator.clipboard&&navigator.clipboard.writeText(lines.join("\n")).then(function(){showToast(sel.length+" rows copied");});
    }
    if(a==="sela"){Array.from(tbody.rows).filter(function(r){return r.style.display!=="none"&&!r.classList.contains("dc-sentinel");}).forEach(function(r){r.classList.add("dc-sel");});updateStatus(table);}
    if(a==="clr"){qsa("tr.dc-sel",tbody).forEach(function(r){r.classList.remove("dc-sel");});updateStatus(table);}
    if(a==="clf"){qsa(".dc-fa-active",thead).forEach(function(f){f.classList.remove("dc-fa-active");});applyFilter(table,new Map());}
    _ctx.classList.remove("open");
  });
  return _ctx;
}

/* ─── FREEZE PANES ───────────────────────────────────────── */
function initFreeze(table){
  var n=parseInt(table.dataset.xlFrozen||"1",10);
  if(!n) return;
  var thead=table.tHead, tbody=table.tBodies[0]; if(!thead||!tbody) return;
  var left=0;
  // Wait one frame so offsetWidth is real
  requestAnimationFrame(function(){
    for(var c=0;c<n;c++){
      var hc=thead.rows[0]&&thead.rows[0].cells[c]; if(!hc) break;
      var w=hc.offsetWidth||120;
      [].concat(qsa("tr",thead),qsa("tr",tbody)).forEach(function(row){
        var cell=row.cells[c]; if(!cell) return;
        cell.style.position="sticky";
        cell.style.left=left+"px";
        cell.style.zIndex=row.parentNode.tagName==="THEAD"?"12":"3";
        if(row.parentNode.tagName==="THEAD") cell.style.background="#f0f2f5";
      });
      left+=w;
    }
    var wrap=table.closest(".table-wrapper,.table-wrapper-sm")||table.parentNode;
    if(wrap) wrap.addEventListener("scroll",function(){
      var sh=this.scrollLeft>2?"3px 0 6px rgba(0,0,0,0.08)":"";
      qsa("td[style*='sticky'],th[style*='sticky']",table).forEach(function(c){c.style.boxShadow=sh;});
    },{passive:true});
  });
}

/* ─── TOTALS ROW ─────────────────────────────────────────── */
function buildTotals(table){
  var spec=table.dataset.xlTotals; if(!spec) return;
  if(table.tFoot) return;
  var cols=spec.split(",").map(Number);
  var tbody=table.tBodies[0], thead=table.tHead; if(!tbody||!thead) return;
  var colCount=thead.rows[0]?thead.rows[0].cells.length:0;
  var sums={};
  Array.from(tbody.rows).forEach(function(row){
    cols.forEach(function(ci){
      var n=parseNum(row.cells[ci]?row.cells[ci].innerText:"");
      if(!isNaN(n)) sums[ci]=(sums[ci]||0)+n;
    });
  });
  var tfoot=table.createTFoot(), tr=tfoot.insertRow(); tr.className="xl-total";
  for(var i=0;i<colCount;i++){
    var td=tr.insertCell();
    if(i===0) td.innerHTML='<span class="ds-total-label">TOTAL</span>';
    else if(sums[i]!==undefined){td.textContent=fmtNum(sums[i]);td.className="dc-num";}
  }
}

/* ─── ROW INFO BAR ───────────────────────────────────────── */
function initRowInfo(table){
  if(table._rowInfo) return;
  var bar=document.createElement("div"); bar.className="dc-row-info";
  var wrap=table.closest(".table-wrapper,.table-wrapper-sm")||table.parentNode;
  wrap.parentNode.insertBefore(bar,wrap);
  table._rowInfo=bar;
  var total=parseInt(table.dataset.totalRows||"0",10)||
            (table.tBodies[0]?table.tBodies[0].rows.length:0);
  bar.textContent=total+" rows";
}

/* ─── TOAST ──────────────────────────────────────────────── */
var _toast=null;
function showToast(msg){
  if(!_toast){_toast=document.createElement("div");_toast.className="xl-copy-toast";document.body.appendChild(_toast);}
  _toast.textContent=msg;_toast.classList.add("show");
  clearTimeout(_toast._t);_toast._t=setTimeout(function(){_toast.classList.remove("show");},2200);
}

/* ─── FILTER CHIPS FROM URL ──────────────────────────────── */
function renderChips(){
  var bar=qs(".ds-chip-row,#dsChipRow");
  if(!bar){
    bar=document.createElement("div"); bar.className="ds-chip-row"; bar.id="dsChipRow";
    var fb=qs(".ds-filter-bar,.filters-row,.filter-bar,.filter-panel");
    if(fb&&fb.parentNode) fb.parentNode.insertBefore(bar,fb.nextSibling);
  }
  var params=new URLSearchParams(window.location.search);
  var skip=new Set(["view","export","level"]);
  var map={brand:"Brand",week:"Week",weeks:"Week",channel:"Channel",category:"Category"};
  var chips={};
  params.forEach(function(v,k){
    if(skip.has(k)||!v||v==="None"||v==="all") return;
    var lbl=map[k]||k;
    if(!chips[lbl]) chips[lbl]={key:k,values:[]};
    chips[lbl].values.push(v);
  });
  var keys=Object.keys(chips);
  if(!keys.length){bar.innerHTML="";return;}
  bar.innerHTML='<span class="ds-chip-label">Active:</span>'+keys.map(function(lbl){
    var c=chips[lbl];
    var removeUrl=(function(){var p=new URLSearchParams(window.location.search);p.delete(c.key);var s=p.toString();return window.location.pathname+(s?"?"+s:"");})();
    return'<span class="ds-chip"><span class="ds-chip-name">'+lbl+':</span> '+c.values.join(", ")+'<button onclick="location.href=\''+removeUrl+'\'" title="Remove">×</button></span>';
  }).join("");
}

/* ─── NAV: ensure Home link exists ──────────────────────── */
function fixNav(){
  var nav=qs(".nav-links"); if(!nav) return;
  if(!nav.querySelector('a[href="/dashboard"]')){
    var a=document.createElement("a"); a.href="/dashboard"; a.textContent="🏠 Home";
    nav.prepend(a);
  }
  var path=window.location.pathname;
  qsa("a",nav).forEach(function(a){
    a.classList.remove("active");
    if(a.getAttribute("href")===path){ a.classList.add("active"); a.style.background="rgba(255,255,255,0.18)"; a.style.color="#fff"; a.style.fontWeight="700"; }
  });
}

/* ─── PATCH autoRefresh — leave template's version intact ── */
function patchAutoRefresh(){
  // The template defines: function autoRefresh(){ document.getElementById("filterForm").submit(); }
  // We do NOT override it — brand/view dropdowns need it to work normally
  // Only define a fallback if no template version exists
  if(typeof window.autoRefresh !== "function"){
    window.autoRefresh = function(){
      var form = document.getElementById("filterForm");
      if(form) form.submit();
    };
  }
}

/* ─── KEYBOARD: Ctrl+F → search box ─────────────────────── */
function initCtrlF(){
  document.addEventListener("keydown",function(e){
    if(!(e.ctrlKey||e.metaKey)||e.key!=="f") return;
    var box=qs("#filterBox,#globalFilter,input[placeholder*='Filter'],input[placeholder*='Search']");
    if(box){e.preventDefault();box.focus();box.select();}
  });
}

/* ─── MAIN INIT ──────────────────────────────────────────── */
function initTable(table){
  // Do NOT wrap the table - it's already in .table-wrapper
  // Attaching a new wrapper breaks existing sticky CSS and overflow

  // Phase 1 — immediate (visual)
  initFreeze(table);
  initAutoFilter(table);
  initRowInfo(table);
  initSelection(table);

  // Right-click
  table.addEventListener("contextmenu",function(e){
    e.preventDefault();
    var ctx=getCtx(); ctx._table=table;
    var row=e.target.closest("tr");
    if(row&&table.tBodies[0]&&table.tBodies[0].contains(row)&&!row.classList.contains("dc-sel")){
      qsa("tr.dc-sel",table.tBodies[0]).forEach(function(r){r.classList.remove("dc-sel");});
      row.classList.add("dc-sel"); updateStatus(table);
    }
    ctx.style.cssText="display:block;top:"+e.clientY+"px;left:"+Math.min(e.clientX,window.innerWidth-200)+"px";
    ctx.classList.add("open");
  });

  // Phase 2 — after first paint
  raf2(function(){
    initStatusBar(table);
    // Phase 3 — run immediately, don't gate behind intersection observer
    // (the outer observer was preventing initInfiniteScroll from ever firing on Render)
    idle(function(){
      buildTotals(table);
      restoreSort(table);
      initInfiniteScroll(table);
      table.classList.add("xl-ready");
    });
  });
}

// AMS reinit hook (called after renderTable/renderSBTable)
window.XLTable={
  reinit:function(sel){
    var el=typeof sel==="string"?qs(sel):sel;
    if(el&&el._dcReinit) el._dcReinit();
  }
};

/* ─── BOOT ───────────────────────────────────────────────── */
function boot(){
  patchAutoRefresh(); fixNav(); renderChips(); initCtrlF();
  var tables=qsa("table[data-xl]");
  // Stagger init: one table per 60ms after double-rAF
  raf2(function(){
    tables.forEach(function(t,i){
      setTimeout(function(){ try{initTable(t);}catch(e){console.warn("dc init:",t.id,e);} },i*60);
    });
  });
}

if(document.readyState==="loading"){
  document.addEventListener("DOMContentLoaded",boot);
} else { boot(); }

})();
