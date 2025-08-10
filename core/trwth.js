// /core/trwth.js – complete readable runtime
// -----------------------------------------------------------------------------
// 0. Namespace (Trwth)
// 1. Utilities  (helpers, applyFilters w/ list + comparisons, merge, formulas)
// -----------------------------------------------------------------------------

(function () {
  /* ───── 0. Namespace ───── */
  const Trwth = (window.Trwth = window.Trwth || {});
  Trwth.utils  = Trwth.utils  || {};
  Trwth.loader = Trwth.loader || {};
  Trwth.dom    = Trwth.dom    || {};
  Trwth.grid   = Trwth.grid   || {};
  Trwth.core   = Trwth.core   || {};

  /* ───── 1. Utilities ───── */
  const U = Trwth.utils;

  /* number + string helpers */
  U.toNum   = v => (Number.isFinite(+v) ? +v : 0);
  U.keyOf   = (row, id) => String(row?.[id] ?? "");
  U.formatValue = (v, t) => {
    if (v == null || Number.isNaN(v)) return "";
    switch (t) {
      case "integer":  return Number(v).toLocaleString();
      case "currency": return Number(v).toLocaleString(undefined,{style:"currency",currency:"USD"});
      case "percent":  return Math.round(Number(v) * 100) + "%";
      default:         return String(v);
    }
  };

  // Filter engine: lists (type-tolerant) + comparisons
  U.applyFilters = (rows, filters = []) => {
    if (!filters?.length) return rows;

    // ---- local helpers ----
    const cleanNum = (x) => String(x).trim().replace(/[$,%\s,]/g, "");
    const isNumericLike = (x) =>
      typeof x === "number" ? Number.isFinite(x)
      : typeof x === "string" ? (cleanNum(x) !== "" && !Number.isNaN(Number(cleanNum(x))))
      : false;
    const toNum = (x) => Number(cleanNum(x));

    // split on a top-level separator; ignore inside [...] or quotes
    const splitTop = (s, sep) => {
      const out = []; let buf = ""; let depth = 0; let q = null;
      for (let i = 0; i < s.length; i++) {
        const ch = s[i];
        if (q) { if (ch === q && s[i-1] !== "\\") q = null; buf += ch; continue; }
        if (ch === '"' || ch === "'") { q = ch; buf += ch; continue; }
        if (ch === "[") { depth++; buf += ch; continue; }
        if (ch === "]") { depth = Math.max(0, depth - 1); buf += ch; continue; }
        if (!depth && s.slice(i, i + sep.length) === sep) {
          out.push(buf.trim()); buf = ""; i += sep.length - 1; continue;
        }
        buf += ch;
      }
      if (buf.trim()) out.push(buf.trim());
      return out;
    };

    const buildTermPred = (id, term) => {
      term = term.trim();

      // [a,b,c] list — tolerant of numbers/strings
      if (/^\[.*\]$/.test(term)) {
        let arr;
        try { arr = JSON.parse(term.replace(/'(.*?)'/g, '"$1"')); }
        catch { return () => true; }
        const strSet = new Set(arr.map(v => String(v)));
        const numSet = new Set(arr
          .map(v => toNum(v))
          .filter(n => Number.isFinite(n)));
        return (r) => {
          const raw = r?.[id];
          if (raw == null) return false;
          if (strSet.has(String(raw))) return true;
          return isNumericLike(raw) && numSet.has(toNum(raw));
        };
      }

      // comparators: == != > < >= <= (spaces optional)
      const m = term.match(/^(==|!=|>=|<=|>|<)\s*(.+)$/);
      if (m) {
        const op = m[1];
        const rhsRaw = m[2].replace(/^"(.*)"$|^'(.*)'$/, "$1$2");
        const rhsNum = isNumericLike(rhsRaw) ? toNum(rhsRaw) : NaN;

        return (r) => {
          const lvRaw = r?.[id];
          if (lvRaw == null) return false;

          // numeric compare if both numeric-like
          if (isNumericLike(lvRaw) && Number.isFinite(rhsNum)) {
            const lv = toNum(lvRaw);
            switch (op) {
              case "==": return lv === rhsNum;
              case "!=": return lv !== rhsNum;
              case ">":  return lv >  rhsNum;
              case "<":  return lv <  rhsNum;
              case ">=": return lv >= rhsNum;
              case "<=": return lv <= rhsNum;
            }
          }
          // fallback: string compare
          const L = String(lvRaw), R = String(rhsRaw);
          switch (op) {
            case "==": return L === R;
            case "!=": return L !== R;
            case ">":  return L >  R;
            case "<":  return L <  R;
            case ">=": return L >= R;
            case "<=": return L <= R;
            default:   return true;
          }
        };
      }

      // unsupported term → allow
      return () => true;
    };

    // build predicate for one filter object (handles A && B || C over SAME id)
    const buildFilterPred = (f) => {
      // legacy {op:"in", values:[...]}
      if (f.op === "in" && Array.isArray(f.values)) {
        const strSet = new Set(f.values.map(String));
        const numSet = new Set(f.values.map(toNum).filter(Number.isFinite));
        return (r) => {
          const raw = r?.[f.id];
          if (raw == null) return false;
          if (strSet.has(String(raw))) return true;
          return isNumericLike(raw) && numSet.has(toNum(raw));
        };
      }

      const expr = typeof f.filter === "string" ? f.filter.trim() : "";
      if (!expr) return () => true;

      // OR groups
      const orParts = splitTop(expr, "||");
      const orPreds = orParts.map(part => {
        // AND within group
        const andParts = splitTop(part, "&&");
        const andPreds = andParts.map(t => buildTermPred(f.id, t));
        return (r) => andPreds.every(p => p(r));
      });
      return (r) => orPreds.some(p => p(r));
    };

    const preds = filters.map(buildFilterPred);
    return rows.filter(r => preds.every(p => p(r)));
  };

  /* merge helpers & formula evaluation */
  U.mergeByKey = (base, sources, keyId, cols, prim) => {
    const map = new Map(), key = r => U.keyOf(r, keyId);
    base.forEach(r => map.set(key(r), { ...r }));
    cols.forEach(c => {
      if (!c.source || c.source === prim) return;
      (sources[c.source] || []).forEach(sr => {
        const k = key(sr); if (map.has(k)) Object.assign(map.get(k), sr);
      });
    });
    return [...map.values()];
  };

  U.evaluateFunctionColumns = (rows, fnCols, reg) => rows.map(r => {
    const out = { ...r };
    fnCols.forEach(c => {
      const fn = reg?.[c.fn]?.implementation;
      if (typeof fn !== "function") { out[c.id] = null; return; }
      const args = (c.params || []).map(id => out[id]);
      try { out[c.id] = fn(...args); } catch { out[c.id] = null; }
    });
    return out;
  });

  // Aggregate rows by primary key for the PRIMARY SOURCE ONLY.
  // Rules per data_type:
  // - currency, float, number  -> SUM
  // - percent, rate            -> AVERAGE (arithmetic mean of decimals, e.g. 0.2 for 20%)
  // - integer                  -> MODE (most frequent; first-win on ties)
  // - date                     -> AVERAGE AGE (in days, rounded)
  U.aggregateByPrimary = function(rows, keyId, columns, primarySource, secondaryKeys = []) {
    const U = Trwth.utils;
    const now = Date.now();
    const MS_PER_DAY = 24 * 60 * 60 * 1000;

    // Only consider DATA columns that belong to the primary source,
    // skip the PK itself and any secondary keys.
    const colDefs = (columns || []).filter(c =>
      c &&
      c.column_type === "data" &&
      c.source === primarySource &&
      c.id !== keyId &&
      !secondaryKeys.includes(c.id)
    );

    // Group rows by key
    const groups = new Map();
    for (const r of rows || []) {
      const k = String(r?.[keyId] ?? "");
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(r);
    }

    // Helpers
    const num = v => (v == null || v === "" || Number.isNaN(+v) ? null : +v);
    const isNumberSum = t => ["currency","float","number"].includes((t || "").toLowerCase());
    const isNumberAvg = t => ["percent","rate"].includes((t || "").toLowerCase());
    const isInteger   = t => (t || "").toLowerCase() === "integer";
    const isDateType  = t => (t || "").toLowerCase() === "date";

    const mode = (vals) => {
      const freq = new Map();
      let best = null, bestCount = -1;
      for (const v of vals) {
        const n = Math.trunc(v); // treat as integer bucket
        const c = (freq.get(n) || 0) + 1;
        freq.set(n, c);
        if (c > bestCount) { best = n; bestCount = c; }
      }
      return bestCount >= 0 ? best : null;
    };

    // Aggregate each group
    const out = [];
    for (const [k, rowsInGroup] of groups.entries()) {
      const agg = { [keyId]: k };

      for (const c of colDefs) {
        const t = c.data_type;

        if (isNumberSum(t)) {
          let s = 0, seen = false;
          for (const r of rowsInGroup) {
            const v = num(r?.[c.id]); if (v != null) { s += v; seen = true; }
          }
          agg[c.id] = seen ? s : null;
        }
        else if (isNumberAvg(t)) {
          let s = 0, n = 0;
          for (const r of rowsInGroup) {
            const v = num(r?.[c.id]); if (v != null) { s += v; n++; }
          }
          agg[c.id] = n ? (s / n) : null;
        }
        else if (isInteger(t)) {
          const vals = [];
          for (const r of rowsInGroup) {
            const v = num(r?.[c.id]); if (v != null) vals.push(v);
          }
          agg[c.id] = vals.length ? mode(vals) : null;
        }
        else if (isDateType(t)) {
          // Average age in days from "today"
          let s = 0, n = 0;
          for (const r of rowsInGroup) {
            const raw = r?.[c.id];
            const d = raw ? new Date(raw) : null;
            if (d && !Number.isNaN(d.getTime())) {
              const ageDays = (now - d.getTime()) / MS_PER_DAY;
              s += ageDays; n++;
            }
          }
          agg[c.id] = n ? Math.round(s / n) : null; // integer days
        }
        else {
          // Fallback: carry first non-null value
          let picked = null;
          for (const r of rowsInGroup) {
            const v = r?.[c.id];
            if (v != null && v !== "") { picked = v; break; }
          }
          agg[c.id] = picked;
        }
      }

      out.push(agg);
    }

    console.log("[aggregate] groups:", groups.size, "→ rows:", out.length);
    return out;
  };

  /* ───── 2. Dynamic library loader ───── */
  const L = Trwth.loader;

  L.injectScript = (src) =>
    new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = src;
      s.async = false; // preserve order
      s.onload = () => resolve(src);
      s.onerror = () => reject(new Error(`Failed to load ${src}`));
      document.head.appendChild(s);
    });

  L.loadScriptsSequentially = async (urls) => {
    for (const u of urls) await L.injectScript(u);
  };

  const defaultLibPath = () => {
    const cur = document.currentScript?.src || "";
    return cur.replace(/\/core\/[^/]+$/, "/libraries/financial.js");
  };

  L.resolveLibraries = () => {
    const cfg = window.boardConfig || {};
    if (Array.isArray(cfg.libraries) && cfg.libraries.length) return cfg.libraries;
    const perBlock = (cfg.board || []).flatMap((b) => b.libraries || []);
    return perBlock.length ? [...new Set(perBlock)] : [defaultLibPath()];
  };

  /* ───── 3. DOM helpers ───── */
  const D = Trwth.dom;

  D.ensureContainer = () => {
    let el = document.getElementById("board-root");
    if (!el) {
      el = document.createElement("div");
      el.id = "board-root";
      el.style.padding = "1rem";
      document.body.appendChild(el);
    }
    return el;
  };

 
  D.renderTable = (parent, cfg, rows, opts = {}) => {
    const { primaryKeyId, secondaryKeys = [], enableHeaderMapping = false } = opts;

    console.log("[renderTable] start", {
      title: cfg?.title ?? "(no title)",
      columns: (cfg?.columns || []).map(c => ({ id: c.id, heading: c.heading, data_type: c.data_type })),
      rowCount: rows?.length ?? 0,
      opts: { primaryKeyId, secondaryKeys, enableHeaderMapping }
    });

    const sec = parent.appendChild(document.createElement("section"));
    sec.className = "board-section";

    if (cfg.title) {
      const h2 = document.createElement("h2");
      h2.textContent = cfg.title;
      sec.appendChild(h2);
    }

    const tbl = document.createElement("table");
    tbl.className = "board-table";
    sec.appendChild(tbl);

    const thead = tbl.createTHead();
    const hr = thead.insertRow();

    // Which columns are interactive? (PK + secondary), only if enabled
    const interactive = new Set();
    if (enableHeaderMapping && primaryKeyId) interactive.add(primaryKeyId);
    if (enableHeaderMapping && secondaryKeys?.length) secondaryKeys.forEach(k => interactive.add(k));
    console.log("[renderTable] interactive column ids", [...interactive]);

    // Tiny 2-col CSV -> Map(key->value)
    function parseToMap(text) {
      console.time("[map] parseToMap");
      const map = new Map();
      const lines = text.replace(/\r\n?/g, "\n").split("\n").filter(l => l.trim().length);
      const parseLine = (line) => {
        const out = []; let cur = "", q = false;
        for (let i = 0; i < line.length; i++) {
          const ch = line[i];
          if (ch === '"') {
            if (q && line[i+1] === '"') { cur += '"'; i++; }
            else { q = !q; }
          } else if (ch === ',' && !q) {
            out.push(cur); cur = "";
          } else {
            cur += ch;
          }
        }
        out.push(cur);
        return out.map(s => s.trim());
      };
      for (const line of lines) {
        const cols = parseLine(line);
        if (cols.length >= 2) map.set(cols[0], cols[1]);
      }
      console.timeEnd("[map] parseToMap");
      console.log("[map] keys loaded", map.size);
      return map;
    }

    // Header
    (cfg?.columns || []).forEach((c, colIdx) => {
      const th = document.createElement("th");

      const numericTypes = ["integer","currency","percent","float","number","rate"];
      const isNum = numericTypes.includes((c.data_type || "").toLowerCase());
      if (isNum) th.classList.add("num");

      const isInteractive = interactive.has(c.id);
      console.log(`[renderTable] th ${c.id} (${c.heading || c.id}) interactive=${isInteractive}`);

      if (isInteractive) {
        // Make the heading itself a button
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "th-map-btn";
        btn.textContent = c.heading || c.id;

        const file = document.createElement("input");
        file.type = "file";
        file.accept = ".csv,text/csv";
        file.style.display = "none";

        btn.addEventListener("click", () => file.click());
        file.addEventListener("change", async (e) => {
          const f = e.target.files?.[0];
          if (!f) return;
          try {
            console.log(`[map] applying to column ${c.id} from file: ${f.name}`);
            const text = await f.text();
            const map = parseToMap(text);

            const tbody = tbl.tBodies[0];
            if (!tbody) { console.warn("[map] no tbody found"); return; }

            let replaced = 0;
            const total = tbody.rows.length;
            for (const tr of Array.from(tbody.rows)) {
              const td = tr.cells[colIdx];
              if (!td) continue;
              const raw = td.textContent.trim();
              if (map.has(raw)) {
                if (!td.dataset.original) td.dataset.original = raw;
                td.textContent = map.get(raw);
                replaced++;
              }
            }
            console.log(`[map] column ${c.id}: replaced ${replaced}/${total}`);
            btn.textContent = (c.heading || c.id) + " ✓";
            btn.disabled = true;
          } catch (err) {
            console.error("[map] failed:", err);
            btn.textContent = (c.heading || c.id) + " (error)";
          } finally {
            e.target.value = ""; // allow re-pick later
          }
        });

        th.appendChild(btn);
      } else {
        th.textContent = c.heading || c.id;
      }

      hr.appendChild(th);
    });

    // Body
    const tbody = tbl.createTBody();
    (rows || []).forEach(r => {
      const tr = tbody.insertRow();
      (cfg?.columns || []).forEach(c => {
        const td = tr.insertCell();
        td.textContent = Trwth.utils.formatValue(r?.[c.id], c.data_type);
        const numericTypes = ["integer","currency","percent","float","number","rate"];
        if (numericTypes.includes((c.data_type || "").toLowerCase())) {
          td.classList.add("num");
        }
      });
    });

    console.log("[renderTable] done", { rows: rows?.length || 0, cols: cfg?.columns?.length || 0 }, tbl);
    return tbl;
  };

  Trwth.dom.createModal = function (titleText, opts = {}) {
    const {
      destroyOnClose = true,   // remove node from DOM on close (prevents buildup)
      clearBodyOnOpen = true,  // wipe previous content each open
      labelledBy = null        // custom aria-labelledby id (optional)
    } = opts;

    const wrap = document.createElement("div");
    wrap.className = "trwth-modal";
    wrap.innerHTML = `
      <div class="trwth-modal__backdrop" data-close></div>
      <div class="trwth-modal__dialog" role="dialog" aria-modal="true" aria-label="Underlying data">
        <div class="trwth-modal__header">
          <span class="trwth-modal__title"></span>
          <span class="trwth-modal__spacer"></span>
          <button type="button" class="trwth-modal__btn" data-close>Close</button>
        </div>
        <div class="trwth-modal__body"></div>
      </div>
    `;
    document.body.appendChild(wrap);

    // Title & aria
    const titleEl = wrap.querySelector(".trwth-modal__title");
    titleEl.textContent = titleText || "Details";
    const dialog = wrap.querySelector(".trwth-modal__dialog");
    if (labelledBy) {
      dialog.setAttribute("aria-labelledby", labelledBy);
      dialog.removeAttribute("aria-label");
    } else {
      // give the title an id and point aria-labelledby to it
      const tid = `trwth-modal-title-${Date.now()}`;
      titleEl.id = tid;
      dialog.setAttribute("aria-labelledby", tid);
      dialog.removeAttribute("aria-label");
    }

    const body = wrap.querySelector(".trwth-modal__body");

    // Focus mgmt
    let lastActive = null;
    const focusables = () =>
      wrap.querySelectorAll('button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])');

    function trapTab(e) {
      if (e.key !== "Tab") return;
      const f = Array.from(focusables()).filter(el => el.offsetParent !== null);
      if (f.length === 0) return;
      const first = f[0], last = f[f.length - 1];
      if (e.shiftKey && document.activeElement === first) { last.focus(); e.preventDefault(); }
      else if (!e.shiftKey && document.activeElement === last) { first.focus(); e.preventDefault(); }
    }

    function onKey(e) { if (e.key === "Escape") close(); else trapTab(e); }

    function open() {
      lastActive = document.activeElement;
      wrap.classList.add("is-open");
      document.addEventListener("keydown", onKey);
      // Focus first interactive control, fallback to dialog
      const f = Array.from(focusables());
      (f[0] || dialog).focus({ preventScroll: true });
    }

    function close() {
      wrap.classList.remove("is-open");
      document.removeEventListener("keydown", onKey);
      if (destroyOnClose) {
        // delay remove a tick to allow CSS transitions if any
        setTimeout(() => wrap.remove(), 120);
      }
      if (lastActive && typeof lastActive.focus === "function") {
        lastActive.focus({ preventScroll: true });
      }
    }

    // Click-to-close (backdrop + close buttons)
    wrap.querySelectorAll("[data-close]").forEach(el => el.addEventListener("click", close));

    return {
      el: wrap,
      body,
      open,
      close,
      setTitle(txt) { titleEl.textContent = txt || ""; }
    };
  };


  /* ───── 4. Grid / Dashboard ───── */
  (function () {
    const G = Trwth.grid;

    // Auto-placement for blocks missing x,y,w,h
    G.autoPosition = (blocks, cols, defW = 6, defH = 3) => {
      let cx = 1,
        cy = 1,
        rowH = defH;
      return blocks.map((b) => {
        if (b.x && b.y) return b;
        const w = b.w || defW,
          h = b.h || defH;
        if (cx + w - 1 > cols) {
          cx = 1;
          cy += rowH;
          rowH = defH;
        }
        const placed = { ...b, x: cx, y: cy, w, h };
        cx += w;
        rowH = Math.max(rowH, h);
        return placed;
      });
    };

    // Helpers
    const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
    const metrics = (el, lay) => {
      const gap = +lay.gap || 0;
      const cols = +lay.cols || 12;
      const rh = +lay.row_height || 100;
      return {
        cols,
        gap,
        rh,
        cw: (el.clientWidth - (cols - 1) * gap) / cols,
      };
    };
    const place = (card, { x, y, w, h }) => {
      card.style.gridColumn = `${x} / span ${w}`;
      card.style.gridRow = `${y} / span ${h}`;
      Object.assign(card.dataset, { x, y, w, h });
    };

    // Persistence
    const loadLayout = (key) => {
      try {
        return (key && JSON.parse(localStorage.getItem(key))) || null;
      } catch {
        return null;
      }
    };
    const saveLayout = (key, gridEl) => {
      if (!key) return;
      const obj = {};
      gridEl.querySelectorAll(".board-card").forEach((c) => {
        obj[c.dataset.blockId] = {
          x: +c.dataset.x,
          y: +c.dataset.y,
          w: +c.dataset.w,
          h: +c.dataset.h,
        };
      });
      localStorage.setItem(key, JSON.stringify(obj));
    };

    // Drag / Resize
    function enableDragResize(container, card, layout, onCommit) {
      const header = card.querySelector(".board-card__header");
      const resizer = card.querySelector(".board-card__resizer");
      let active = null;

      const snap = {
        drag(start, dx, dy, m) {
          const dCols = Math.round(dx / (m.cw + m.gap));
          const dRows = Math.round(dy / (m.rh + m.gap));
          return { x: start.x + dCols, y: start.y + dRows };
        },
        resize(start, dx, dy, m) {
          const dW = Math.round(dx / (m.cw + m.gap));
          const dH = Math.round(dy / (m.rh + m.gap));
          return { w: start.w + dW, h: start.h + dH };
        },
      };

      const pointerMove = (ev) => {
        if (!active) return;
        const m = metrics(container, layout);
        const dx = ev.clientX - active.px;
        const dy = ev.clientY - active.py;

        if (active.mode === "drag") {
          let { x, y } = snap.drag(active.start, dx, dy, m);
          x = clamp(x, 1, m.cols - active.start.w + 1);
          y = Math.max(1, y);
          place(card, { x, y, w: active.start.w, h: active.start.h });
        } else {
          let { w, h } = snap.resize(active.start, dx, dy, m);
          w = clamp(w, 1, m.cols - active.start.x + 1);
          h = Math.max(1, h);
          place(card, { x: active.start.x, y: active.start.y, w, h });
        }
      };

      const pointerUp = () => {
        if (!active) return;
        card.classList.remove("is-active");
        document.removeEventListener("pointermove", pointerMove);
        document.removeEventListener("pointerup", pointerUp);
        onCommit();
        active = null;
      };

      const begin = (mode) => (ev) => {
        ev.preventDefault();
        active = {
          mode,
          px: ev.clientX,
          py: ev.clientY,
          start: {
            x: +card.dataset.x,
            y: +card.dataset.y,
            w: +card.dataset.w,
            h: +card.dataset.h,
          },
        };
        card.classList.add("is-active");
        document.addEventListener("pointermove", pointerMove);
        document.addEventListener("pointerup", pointerUp);
      };

      header?.addEventListener("pointerdown", begin("drag"));
      resizer?.addEventListener("pointerdown", begin("resize"));

      // Keyboard accessibility
      card.tabIndex = 0;
      card.addEventListener("keydown", (e) => {
        const step = e.ctrlKey || e.metaKey ? 5 : 1;
        let { x, y, w, h } = {
          x: +card.dataset.x,
          y: +card.dataset.y,
          w: +card.dataset.w,
          h: +card.dataset.h,
        };
        const m = metrics(container, layout);

        if (e.shiftKey) {
          if (e.key === "ArrowRight") w = clamp(w + step, 1, m.cols - x + 1);
          else if (e.key === "ArrowLeft") w = clamp(w - step, 1, m.cols - x + 1);
          else if (e.key === "ArrowDown") h = Math.max(1, h + step);
          else if (e.key === "ArrowUp") h = Math.max(1, h - step);
          else return;
        } else {
          if (e.key === "ArrowRight") x = clamp(x + step, 1, m.cols - w + 1);
          else if (e.key === "ArrowLeft") x = clamp(x - step, 1, m.cols - w + 1);
          else if (e.key === "ArrowDown") y = Math.max(1, y + step);
          else if (e.key === "ArrowUp") y = Math.max(1, y - step);
          else return;
        }

        place(card, { x, y, w, h });
        onCommit();
        e.preventDefault();
      });
    }

    // Main: render grid & cards
    G.renderDashboard = (rootEl, boardConfig, sources, buildRowsForTable) => {
      const U = Trwth.utils;
      const layout = boardConfig.board_layout || {};
      const cols = layout.cols || 12;

      // Auto-position any blocks missing x/y
      boardConfig.board = G.autoPosition(boardConfig.board || [], cols);

      // Grid container
      const grid = document.createElement("div");
      grid.className = "board-grid";
      grid.style.setProperty("--cols", cols);
      grid.style.setProperty("--row-height", (layout.row_height || 110) + "px");
      grid.style.setProperty("--gap", (layout.gap || 8) + "px");
      rootEl.appendChild(grid);

      const saved = layout.storage_key ? loadLayout(layout.storage_key) : null;

      (boardConfig.board || []).forEach((block) => {
        const card = document.createElement("div");
        card.className = "board-card";
        card.dataset.blockId = block.id;

        // Header + actions
        const header = document.createElement("div");
        header.className = "board-card__header";
        header.textContent = block.title || block.id;

        const actions = document.createElement("div");
        actions.className = "board-card__actions";
        header.appendChild(actions);

        const drillBtn = document.createElement("button");
        drillBtn.type = "button";
        drillBtn.className = "board-card__iconbtn";
        drillBtn.title = "Drill down to underlying rows";
        drillBtn.textContent = "🔎";
        actions.appendChild(drillBtn);

        // Body + resizer
        const body = document.createElement("div");
        body.className = "board-card__body";

        const resizer = document.createElement("div");
        resizer.className = "board-card__resizer";

        card.append(header, body, resizer);
        grid.appendChild(card);

        // Place card (saved layout takes precedence)
        const pos = saved?.[block.id] || {
          x: block.x || 1, y: block.y || 1, w: block.w || 6, h: block.h || 3
        };
        place(card, pos);

        // Render inline table in the card (mapping OFF here)
        if (block.type === "table") {
          try {
            const rows = buildRowsForTable(block, sources);
            Trwth.dom.renderTable(
              body,
              { title: block.title, columns: block.columns },
              rows,
              {
                primaryKeyId: block.primary_key?.id,
                secondaryKeys: block.secondary_keys || [],
                enableHeaderMapping: true
              }
            );
          } catch (e) {
            console.error("[grid] render error:", e);
            body.textContent = "Render error: " + e.message;
          }
        }

        // Drilldown → modal with PK + secondary + primary-source cols (mapping ON)
        drillBtn.addEventListener("click", () => {
        console.log("[drill] click", { blockId: block.id, title: block.title });

        try {
          const pkId = block.primary_key?.id;
          const pkCol = (block.columns || []).find(c => c.id === pkId);
          const prim  = block.primary_key?.source || pkCol?.source;

          if (!pkId || !prim) {
            console.error("[drill] missing primary key id or source", { pkId, prim });
            const m = Trwth.dom.createModal(`${block.title || block.id} — error`);
            m.body.innerHTML = `<p style="color:#f88;margin:0">Missing primary_key id or source.</p>`;
            m.open();
            return;
          }

          // Base rows come from the primary source; apply only filters for that source.
          const base = [...(sources[prim] || [])];
          const scoped = (block.filters || []).filter(f => (f.source || prim) === prim);
          const filtered = Trwth.utils.applyFilters(base, scoped);

          // Column order for drilldown: PK → secondary keys → all block columns from the primary source.
          const colsById = new Map((block.columns || []).map(c => [c.id, c]));
          const order = [];
          const pushUnique = (id) => { if (id && !order.includes(id)) order.push(id); };

          pushUnique(pkId);
          (block.secondary_keys || []).forEach(pushUnique);
          (block.columns || []).filter(c => c.source === prim).forEach(c => pushUnique(c.id));

          const drillDefs = order.map(id => colsById.get(id) || { id, heading: id, data_type: undefined });

          console.log("[drill] computed", {
            pkId,
            primarySource: prim,
            secondaryKeys: block.secondary_keys || [],
            drillCols: drillDefs.map(c => c.id),
            filteredRows: filtered.length
          });

          // Build modal and render the table with header mapping ON for PK + secondary keys
          const modal = Trwth.dom.createModal(`${block.title || block.id} — underlying rows`);
          const container = modal.body;
          container.innerHTML = "";


          try {
            const tbl = Trwth.dom.renderTable(
              container,
              { title: `${block.title || block.id} — underlying rows`, columns: drillDefs },
              filtered,
              {
                primaryKeyId: pkId,
                secondaryKeys: block.secondary_keys || [],
                enableHeaderMapping: true
              }
            );
            if (tbl && !container.contains(tbl)) {
              console.warn("[drill] returned table not in modal body; appending manually.");
              container.appendChild(tbl);
            }
          } catch (e) {
            console.error("[drill] renderTable threw; falling back to manual table", e);

            // Fallback: minimal manual table so the user still sees data
            const table = document.createElement("table");
            table.className = "board-table";
            const thead = table.createTHead();
            const hr = thead.insertRow();
            drillDefs.forEach(def => {
              const th = document.createElement("th");
              th.textContent = def.heading || def.id;
              if (["integer","currency","percent","float","number","rate"]
                  .includes((def.data_type || "").toLowerCase())) th.classList.add("num");
              hr.appendChild(th);
            });
            const tbody = table.createTBody();
            filtered.forEach(row => {
              const tr = tbody.insertRow();
              drillDefs.forEach(def => {
                const td = tr.insertCell();
                td.textContent = Trwth.utils.formatValue(row?.[def.id], def.data_type);
                if (["integer","currency","percent","float","number","rate"]
                    .includes((def.data_type || "").toLowerCase())) td.classList.add("num");
              });
            });
            container.appendChild(table);
          }

          // Assert something got rendered
          const rowsRendered = container.querySelectorAll("tbody tr").length;
          const colsRendered = container.querySelectorAll("thead th").length;
          console.log("[drill] rendered", { rowsRendered, colsRendered });

          if (!container.querySelector("table")) {
            console.warn("[drill] no <table> found in modal body; injecting empty table");
            const t = document.createElement("table");
            t.className = "board-table";
            container.appendChild(t);
          }
          modal.open();

        } catch (err) {
          console.error("[drill] fatal error", err);
          const modal = Trwth.dom.createModal(`${block.title || block.id} — error`);
          modal.body.innerHTML = `<pre style="white-space:pre-wrap;color:#f88">Drilldown error:\n${err?.stack || err}</pre>`;
          modal.open();
        }
      });


        // Drag/resize + persist
        enableDragResize(grid, card, layout, () => saveLayout(layout.storage_key, grid));
      });

      if (layout.storage_key) saveLayout(layout.storage_key, grid);
    };


  })();
    /* ───── 5. CSV Source-loader panel ───── */
  (function () {
    const NEED = new Set();

    function discoverSources() {
      const cfg = window.boardConfig || {};
      (cfg.board || []).forEach((b) => {
        b.columns?.forEach((c) => c.source && NEED.add(c.source));
        b.filters?.forEach((f) => f.source && NEED.add(f.source));
        b.primary_key?.source && NEED.add(b.primary_key.source);
      });
    }

    function parseCSV(text) {
      const rows = [];
      let i = 0, field = "", row = [], quoted = false;

      const pushField = () => { row.push(field); field = ""; };
      const pushRow = () => { if (row.length) rows.push(row); row = []; };

      while (i < text.length) {
        const c = text[i++];
        if (quoted) {
          if (c === '"') {
            if (text[i] === '"') { field += '"'; i++; }
            else quoted = false;
          } else field += c;
        } else {
          if (c === '"') quoted = true;
          else if (c === ",") pushField();
          else if (c === "\n") { pushField(); pushRow(); }
          else if (c === "\r") { /* ignore */ }
          else field += c;
        }
      }
      pushField(); pushRow();

      if (!rows.length) return [];
      const hdr = rows[0];
      return rows
        .slice(1)
        .filter(r => r.some(v => v !== ""))
        .map(r => {
          const o = {};
          hdr.forEach((h, idx) => (o[h.trim()] = r[idx] ?? ""));
          return o;
        });
    }

    function buildPanel() {
      const pane = document.createElement("div");
      pane.className = "loader-pane brand-topline";
      pane.setAttribute("role", "region");
      pane.setAttribute("aria-label", "Trwth data sources");

      // Brand header + subtitle
      pane.innerHTML = `
        <div class="loader-pane__brand">
          <div class="logo" aria-label="Trwth">
            <span class="logo-text">Tr</span>
            <div class="logo-shield">
              <div class="middle"></div><div class="left"></div><div class="right"></div>
            </div>
            <span class="logo-text">th</span>
          </div>
          <div>
            <div class="loader-pane__subtitle">Load data sources (CSV)</div>
          </div>
        </div>
      `;
      document.body.appendChild(pane);

      const needed = [...NEED];        // expected source names
      const state  = {};               // parsed CSV objects keyed by source
      const labels = {};               // label refs for UI updates

      // Build a labeled file input for each required source
      needed.forEach((src) => {
        const row = document.createElement("div");
        row.style = "margin-top:6px";

        const label = document.createElement("label");
        label.className = "custom-file-upload";
        label.innerHTML = `
          <span class="label-text">Choose <strong>${src}</strong></span>
          <span class="subtle">(CSV)</span>
          <span class="filename"></span>
        `;

        const input = document.createElement("input");
        input.type = "file";
        input.accept = ".csv,text/csv";
        input.className = "hidden-file-input";

        label.appendChild(input);
        row.appendChild(label);
        pane.appendChild(row);

        labels[src] = label;

        input.onchange = async (e) => {
          const file = e.target.files && e.target.files[0];
          if (!file) return;

          try {
            const text  = await file.text();
            const parse = (Trwth.utils && Trwth.utils.parseCSV) || window.parseCSV;
            state[src]  = parse ? parse(text) : parseCSV(text); // fallback to global parseCSV

            label.classList.add("completed");
            label.querySelector(".filename").textContent = file.name;
          } catch (err) {
            console.error(`[loader] parse error for ${src}:`, err);
            delete state[src];
            label.classList.remove("completed");
            label.querySelector(".filename").textContent = "Parse error";
          } finally {
            check();
          }
        };
      });

      // Render button
      const btn = document.createElement("button");
      btn.textContent = "Render Board";
      btn.disabled = true;
      btn.className = "trwth-modal__btn";
      btn.style.marginTop = "10px";
      btn.onclick = () => {
        window.data = state;
        pane.remove();
        Trwth.core.renderBoard();
      };
      pane.appendChild(btn);

      // Update subtitle + button enabled state
      function check() {
        const loaded = Object.keys(state).length;
        const sub = pane.querySelector(".loader-pane__subtitle");
        if (sub) sub.textContent = `Load data sources (CSV) — ${loaded}/${needed.length} loaded`;
        btn.disabled = needed.some(s => !state[s]);
      }
    }

    Trwth.core.beforeRender = () => {
      discoverSources();
      if (!NEED.size) return false;
      buildPanel();
      return true; // block initial render until user loads files
    };
  })();

  /* ───── 6. Core: buildRows + render + bootstrap (with logs) ───── */
  (function () {
    function validate(tbl) {
      if (!tbl.primary_key?.id) throw new Error("primary_key.id is required");
      if (!tbl.columns.find((c) => c.id === tbl.primary_key.id)) {
        throw new Error("primary_key.id not found in columns");
      }
    }

    function buildRows(tbl, sources) {
      validate(tbl) // if you have it, keep it
      const keyId = tbl?.primary_key?.id;
      if (!keyId) throw new Error("primary_key.id is required");

      const keyCol = (tbl.columns || []).find(c => c.id === keyId);
      const prim   = tbl?.primary_key?.source || keyCol?.source;
      if (!prim) throw new Error("primary_key.source could not be resolved");

      // 1) Base = primary source only
      const base = [...(sources[prim] || [])];

      // 2) Apply ONLY filters scoped to the primary source
      const primFilters = (tbl.filters || []).filter(f => (f.source || prim) === prim);
      const filtered = Trwth.utils.applyFilters(base, primFilters);
      console.log("[buildRows] primary:", prim, "filtered:", filtered.length);

      // 3) Aggregate by primary key (exclude any secondary_keys)
      const aggregated = Trwth.utils.aggregateByPrimary(
        filtered,
        keyId,
        tbl.columns || [],
        prim,
        tbl.secondary_keys || []
      );

      // 4) Merge in non-primary sources (e.g., goals) by key
      const merged = Trwth.utils.mergeByKey(
        aggregated,
        sources,
        keyId,
        tbl.columns || [],
        prim
      );

      // 5) Compute function columns on top of aggregated+merged data
      const fnCols = (tbl.columns || []).filter(c => c.column_type === "function");
      const result = Trwth.utils.evaluateFunctionColumns(
        merged,
        fnCols,
        (window.financial && window.financial.functions) || {}
      );

      console.log("[buildRows] out rows:", result.length);
      return result;
    }

    Trwth.core.renderBoard = () => {
      const cfg = window.boardConfig;
      if (!cfg || !Array.isArray(cfg.board) || cfg.board.length === 0) {
        console.warn("No boardConfig.board found.");
        return;
      }

      const root = Trwth.dom.ensureContainer();
      root.innerHTML = "";

      const sources = (window.data && typeof window.data === "object") ? window.data : {};
      console.log("[renderBoard] sources", Object.keys(sources));

      if (cfg.board_layout) {
        console.log("[renderBoard] grid path");
        // Drilldown + header mapping is wired inside Trwth.grid.renderDashboard
        Trwth.grid.renderDashboard(root, cfg, sources, buildRows);
        return;
      }

      console.log("[renderBoard] simple path");
      cfg.board.forEach((block) => {
        if (block.type !== "table") return;

        const rows = buildRows(block, sources);
        Trwth.dom.renderTable(
          root,
          { title: block.title, columns: block.columns },
          rows,
          {
            primaryKeyId: block.primary_key?.id,
            secondaryKeys: block.secondary_keys || [],
            enableHeaderMapping: true // set to true if you also want header-map buttons in simple path
          }
        );
      });
    };


    Trwth.core.bootstrap = async () => {
      try {
        await L.loadScriptsSequentially(L.resolveLibraries());
      } catch (e) {
        console.error("[Trwth] library load failed:", e);
      }

      // Wait for CSVs if needed
      if (Trwth.core.beforeRender && Trwth.core.beforeRender()) return;

      // Otherwise render immediately
      Trwth.core.renderBoard();
    };
  })();

  /* ───── 7. Auto-start ───── */
  const start = () => Trwth.core.bootstrap();
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
  renderFavicon();
})();

function renderFavicon() {
  const link = document.createElement('link');
  link.rel = 'icon';
  link.type = 'image/svg+xml';
  link.href =
    'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA2NCA2NCI+PHBhdGggZmlsbD0iIzJFOEJDMCIgZD0iTTE2IDEyLjhMMzIgMEw0OCAxMi44TDQ4IDUxLjJMMzIgNjRMMTYgNTEuMloiLz48cGF0aCBmaWxsPSIjMEEyNTQwIiBkPSJNMCAwTDE2IDEyLjhMMzIgNjRMMTYgNTEuMloiLz48cGF0aCBmaWxsPSIjNThDNkIxIiBkPSJNNjQgMEw0OCAxMi44TDMyIDY0TDQ4IDUxLjJaIi8+PC9zdmc+';
  document.head.appendChild(link);
}
