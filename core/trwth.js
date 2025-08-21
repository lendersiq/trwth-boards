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
  Trwth.IO     = Trwth.IO     || {};

  /* ───── I/O — CSV + XLSX (no external libraries) ───── */
  const IO = (Trwth.io = Trwth.io || {});

  // ---------- CSV ----------
  IO.parsers = IO.parsers || {};
  IO.parsers.csv = function parseCSV(text) {
    const out = [];
    const lines = text.replace(/\r\n?/g, "\n").split("\n");
    if (!lines.length) return out;

    // Parse one CSV line with quotes
    const parseLine = (line) => {
      const cells = [];
      let cur = "", q = false;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') {
          if (q && line[i + 1] === '"') { cur += '"'; i++; }
          else { q = !q; }
        } else if (ch === "," && !q) {
          cells.push(cur); cur = "";
        } else {
          cur += ch;
        }
      }
      cells.push(cur);
      return cells.map(s => s.trim());
    };

    const headers = parseLine(lines[0] || "");
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i];
      if (!line.trim()) continue;
      const cols = parseLine(line);
      const row = {};
      headers.forEach((h, idx) => { row[h] = cols[idx] ?? ""; });
      out.push(row);
    }
    return out;
  };

  // ---------- File type + dispatcher ----------
  IO.detectFileType = function detectFileType(file) {
    const name = (file.name || "").toLowerCase();
    const mime = file.type || "";
    if (name.endsWith(".csv") || /csv/.test(mime)) return "csv";
    if (name.endsWith(".xlsx") || /spreadsheetml/.test(mime)) return "xlsx";
    return "unknown";
  };

  IO.fileToRows = async function fileToRows(file) {
    const kind = IO.detectFileType(file);
    if (kind === "csv") {
      const text = await file.text();
      return IO.parsers.csv(text);
    }
    if (kind === "xlsx") {
      return IO.parsers.xlsx(file);
    }
    throw new Error(`Unsupported file type: ${file.name || "(unnamed)"}`);
  };

  // ---------- Minimal ZIP reader (central directory) ----------
  // Uses built-in DecompressionStream('deflate-raw') for deflated entries.
  // Works in modern Chromium/Edge/Safari. If not supported, we throw.
  class ZipReader {
    constructor(bytes) {
      this.bytes = bytes; // Uint8Array
      this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      this.entries = this._parseCentralDirectory();
    }
    _readU16(off) { return this.view.getUint16(off, true); }
    _readU32(off) { return this.view.getUint32(off, true); }

    _findEOCD() {
      // End of Central Directory record signature: 0x06054b50
      const sig = 0x06054b50;
      const { bytes } = this;
      const maxBack = Math.min(bytes.length, 22 + 0xffff + 256); // EOCD + max comment
      for (let i = bytes.length - 22; i >= bytes.length - maxBack; i--) {
        if (i < 0) break;
        if (this._readU32(i) === sig) return i;
      }
      throw new Error("ZIP: EOCD not found");
    }

    _parseCentralDirectory() {
      const eocd = this._findEOCD();
      const total = this._readU16(eocd + 10); // total number of entries
      const cdSize = this._readU32(eocd + 12);
      const cdOff  = this._readU32(eocd + 16);

      const sigCDH = 0x02014b50;
      let off = cdOff;
      const list = [];

      for (let i = 0; i < total; i++) {
        if (this._readU32(off) !== sigCDH) break;
        const compMethod = this._readU16(off + 10);
        const compSize   = this._readU32(off + 20);
        const uncompSize = this._readU32(off + 24);
        const nameLen    = this._readU16(off + 28);
        const extraLen   = this._readU16(off + 30);
        const cmtLen     = this._readU16(off + 32);
        const localOff   = this._readU32(off + 42);
        const nameBytes  = this.bytes.subarray(off + 46, off + 46 + nameLen);
        const name = new TextDecoder().decode(nameBytes);
        list.push({ name, compMethod, compSize, uncompSize, localOff });
        off += 46 + nameLen + extraLen + cmtLen;
      }
      return list;
    }

    _localDataOffset(entry) {
      // Local file header signature: 0x04034b50
      const sigLFH = 0x04034b50;
      const off = entry.localOff;
      if (this._readU32(off) !== sigLFH) {
        throw new Error(`ZIP: bad local header for ${entry.name}`);
      }
      const nameLen  = this._readU16(off + 26);
      const extraLen = this._readU16(off + 28);
      return off + 30 + nameLen + extraLen;
    }

    async read(path) {
      const entry = this.entries.find(e => e.name === path);
      if (!entry) return null;
      const start = this._localDataOffset(entry);
      const end   = start + entry.compSize;
      const chunk = this.bytes.subarray(start, end);
      if (entry.compMethod === 0) {
        return chunk; // stored
      }
      if (entry.compMethod === 8) {
        // deflate (raw)
        if (!("DecompressionStream" in window)) {
          throw new Error("This browser lacks DecompressionStream('deflate-raw').");
        }
        const stream = new Blob([chunk]).stream()
          .pipeThrough(new DecompressionStream("deflate-raw"));
        const ab = await new Response(stream).arrayBuffer();
        return new Uint8Array(ab);
      }
      throw new Error(`ZIP: unsupported compression method ${entry.compMethod} for ${entry.name}`);
    }

    async readText(path) {
      const u8 = await this.read(path);
      if (!u8) return null;
      return new TextDecoder("utf-8").decode(u8);
    }

    // Convenience: list entries under a prefix
    list(prefix) {
      return this.entries.filter(e => e.name.startsWith(prefix)).map(e => e.name);
    }
  }

  IO.zip = {
    read(bytes) { return new ZipReader(bytes); }
  };

  // ---------- XLSX (very small subset) ----------
  IO.xlsx = IO.xlsx || {};

  // Built-in Excel date numFmtIds (plus common date/time ids)
  IO.xlsx.BUILTIN_DATE_IDS = new Set([14,15,16,17,22,27,30,36,45,46,47,50,57,58]);

  // Excel serial → ISO (1900 epoch with Excel's leap quirk adjustment baked in)
  IO.xlsx.excelSerialToISO = function excelSerialToISO(serial) {
    const n = Number(serial);
    if (!Number.isFinite(n)) return String(serial);
    const base = Date.UTC(1899, 11, 30); // 1899-12-30 (Excel base)
    const whole = Math.floor(n);
    const frac  = n - whole;
    const ms = whole * 86400000 + Math.round(frac * 86400000);
    return new Date(base + ms).toISOString().slice(0, 10);
  };

  // Parse styles.xml → set of style indices (cellXf) that are date/time formats
  IO.xlsx.readDateStyleSet = function readDateStyleSet(stylesXmlText) {
    if (!stylesXmlText) return new Set();
    const doc = new DOMParser().parseFromString(stylesXmlText, "application/xml");

    // Collect custom numFmtId → formatCode
    const fmtById = {};
    doc.querySelectorAll("numFmts > numFmt").forEach(n => {
      const id = Number(n.getAttribute("numFmtId"));
      const code = (n.getAttribute("formatCode") || "").toLowerCase();
      if (Number.isFinite(id)) fmtById[id] = code;
    });

    const isDateFmtId = (id) => {
      if (IO.xlsx.BUILTIN_DATE_IDS.has(id)) return true;
      const code = fmtById[id];
      if (!code) return false;
      // Strip bracket sections like [h], then look for date/time tokens
      const clean = code.replace(/\[[^\]]+\]/g, "");
      return /y|m|d|h|s/.test(clean);
    };

    const dateSet = new Set();
    doc.querySelectorAll("cellXfs > xf").forEach((xf, idx) => {
      const numFmtId = Number(xf.getAttribute("numFmtId"));
      if (Number.isFinite(numFmtId) && isDateFmtId(numFmtId)) dateSet.add(idx);
    });
    return dateSet;
  };

  IO.xlsx.parseSharedStrings = function parseSharedStrings(xml) {
    if (!xml) return [];
    const doc = new DOMParser().parseFromString(xml, "application/xml");
    const arr = [];
    doc.querySelectorAll("sst > si").forEach(si => {
      // sst/si may have multiple t's; concat them
      const parts = [];
      si.querySelectorAll("t").forEach(t => parts.push(t.textContent || ""));
      arr.push(parts.join(""));
    });
    return arr;
  };

  IO.xlsx.colLettersToIndex = function colLettersToIndex(ref) {
    // "A1" -> { col:0, row:1 } ; "AB12" -> { col:27, row:12 }
    const m = String(ref).match(/^([A-Z]+)(\d+)$/i);
    if (!m) return { col: 0, row: 0 };
    const letters = m[1].toUpperCase();
    const row = parseInt(m[2], 10);
    let col = 0;
    for (let i = 0; i < letters.length; i++) {
      col = col * 26 + (letters.charCodeAt(i) - 64);
    }
    return { col: col - 1, row };
  };

  IO.xlsx.firstSheetPath = async function firstSheetPath(zip) {
    // Try to resolve via workbook + rels. Fallback to sheet1.xml or any worksheets/*.xml
    const wbXml = await zip.readText("xl/workbook.xml");
    if (wbXml) {
      const doc = new DOMParser().parseFromString(wbXml, "application/xml");
      const firstSheet = doc.querySelector("workbook > sheets > sheet");
      const rId = firstSheet && firstSheet.getAttribute("r:id");
      if (rId) {
        const relsXml = await zip.readText("xl/_rels/workbook.xml.rels");
        if (relsXml) {
          const rdoc = new DOMParser().parseFromString(relsXml, "application/xml");
          const rel = Array.from(rdoc.querySelectorAll("Relationships > Relationship"))
            .find(n => n.getAttribute("Id") === rId);
          if (rel) {
            const target = rel.getAttribute("Target") || "";
            return target.startsWith("/") ? target.slice(1) : `xl/${target}`;
          }
        }
      }
    }
    // Fallbacks
    if (zip.entries.find(e => e.name === "xl/worksheets/sheet1.xml")) {
      return "xl/worksheets/sheet1.xml";
    }
    const any = zip.list("xl/worksheets/");
    if (any.length) return any[0];
    throw new Error("XLSX: no worksheets found");
  };

  // NOTE: now accepts style index + dateStyleSet to convert date-serials
  IO.xlsx.cellValue = function cellValue(c, sharedStrings, dateStyleSet) {
    const t = c.getAttribute("t") || "";              // cell type
    const sIdx = Number(c.getAttribute("s") || -1);   // style index
    const vNode = c.querySelector("v");
    const raw = vNode ? vNode.textContent : "";

    if (t === "s") { // shared string
      const idx = raw ? parseInt(raw, 10) : 0;
      return sharedStrings[idx] ?? "";
    }
    if (t === "b") return raw === "1";           // boolean
    if (t === "str") return raw || "";           // cached formula result (string)
    if (t === "d")   return String(raw).slice(0, 10); // ISO date already

    // default: numeric/general. If style indicates date, convert serial → ISO
    const num = Number(raw);
    if (Number.isFinite(num) && dateStyleSet && dateStyleSet.has(sIdx)) {
      return IO.xlsx.excelSerialToISO(num);
    }
    return raw;
  };

  IO.parsers.xlsx = async function parseXLSX(file) {
    const ab = await file.arrayBuffer();
    const zip = IO.zip.read(new Uint8Array(ab));

    // shared strings (optional)
    const sstXml = await zip.readText("xl/sharedStrings.xml");
    const sharedStrings = IO.xlsx.parseSharedStrings(sstXml);

    // styles → determine which cellXf indices are dates
    const stylesXml = await zip.readText("xl/styles.xml");
    const dateStyleSet = IO.xlsx.readDateStyleSet(stylesXml);

    // pick first sheet
    const sheetPath = await IO.xlsx.firstSheetPath(zip);
    const sheetXml = await zip.readText(sheetPath);
    if (!sheetXml) throw new Error("XLSX: worksheet XML not found");

    const doc = new DOMParser().parseFromString(sheetXml, "application/xml");

    // Build sparse grid: row index -> { colIndex: value }
    const rowsMap = new Map();
    doc.querySelectorAll("worksheet sheetData row").forEach(rowEl => {
      const rIndex = parseInt(rowEl.getAttribute("r") || "0", 10);
      const cells = {};
      rowEl.querySelectorAll("c").forEach(c => {
        const ref = c.getAttribute("r") || ""; // e.g., B3
        const { col } = IO.xlsx.colLettersToIndex(ref);
        cells[col] = IO.xlsx.cellValue(c, sharedStrings, dateStyleSet);
      });
      rowsMap.set(rIndex, cells);
    });

    // FIRST ROW = headers
    const allRows = [...rowsMap.keys()].sort((a, b) => a - b);
    if (!allRows.length) return [];

    const headerRow = rowsMap.get(allRows[0]) || {};
    const maxCol = Math.max(0, ...Object.keys(headerRow).map(n => +n));
    const headers = [];
    for (let c = 0; c <= maxCol; c++) {
      const h = headerRow[c];
      headers.push((h == null || h === "") ? `Column${c + 1}` : String(h));
    }

    const out = [];
    for (let i = 1; i < allRows.length; i++) {
      const rIdx = allRows[i];
      const cells = rowsMap.get(rIdx) || {};
      const obj = {};
      headers.forEach((h, col) => { obj[h] = (cells[col] ?? ""); });
      out.push(obj);
    }
    return out;
  };

  /* ───── Utilities ───── */
  const U = Trwth.utils;

  // Normalize keys so "2", "2.0", 2 all match.
  U.normalizeKey = (v) => {
    if (v == null) return "";
    const s = String(v).trim();
    // numeric-like → normalize to number (handles 2 / "2.0")
    const n = Number(s);
    if (!Number.isNaN(n)) return String(n);
    return s;
  };

  // Add fields to base rows before aggregation for any columns
  // with { source !== primary, join_on: "<fk-field>" }.
  // Example: for Annual_Goal (from loan_classes) joined on Class_Code.
  U.rowEnrichJoins = (rows, cols, sources, primarySource) => {
    const need = (cols || []).filter(c => c.source && c.source !== primarySource && c.join_on);
    if (!need.length) return rows;

    // Build per-source index by join key
    const indexes = new Map();
    const indexOf = (srcName) => {
      if (indexes.has(srcName)) return indexes.get(srcName);
      const idx = new Map();
      (sources[srcName] || []).forEach(r => {
        // key is whatever the column's join_on references (on the row later)
        // Here we index by the *foreign table's* join key column = the same name
        // as specified in join_on (e.g., "Class_Code").
        const k = U.normalizeKey(r?.[/* fk column name */ undefined]); // will handle below
      });
      return idx;
    };

    // Build specific indexes per (source, join_on) pair
    const key = (src, on) => `${src}::${on}`;
    const idxMap = new Map();
    const ensureIndex = (src, on) => {
      const k = key(src, on);
      if (idxMap.has(k)) return idxMap.get(k);
      const idx = new Map();
      (sources[src] || []).forEach(r => {
        const fk = U.normalizeKey(r?.[on]);
        if (!idx.has(fk)) idx.set(fk, r);
      });
      idxMap.set(k, idx);
      return idx;
    };

    rows.forEach(r => {
      need.forEach(col => {
        const idx = ensureIndex(col.source, col.join_on);
        const fk = U.normalizeKey(r?.[col.join_on]); // find by same field name in base row
        const match = idx.get(fk);
        if (match && col.id in match) {
          // write the joined value onto the base row under the target id
          r[col.id] = match[col.id];
        }
      });
    });
    return rows;
  };


  /* number + string helpers */
  U.toNum   = v => (Number.isFinite(+v) ? +v : 0);
  //U.keyOf   = (row, id) => String(row?.[id] ?? "");
  U.keyOf = (row, id) => U.normalizeKey(row?.[id]);
  U.formatValue = (v, t) => {
    if (v == null || Number.isNaN(v)) return "";
    switch (t) {
      case "integer":  return String(Number(v)); //Number(v).toLocaleString();
      case "currency": return Number(v).toLocaleString(undefined,{style:"currency",currency:"USD"});
      case "percent":  return typeof v === "string" ? v : Math.round(Number(v) * 100) + "%";
      default:         return String(v);
    }
  };

  // Filter engine: lists (type-tolerant) + comparisons
  U.applyFilters = (rows, filters = []) => {
    if (!filters?.length) return rows;

    // ─── helpers ──────────────────────────────────────────────────────────────
    const cleanNum = (x) => String(x).trim().replace(/[$,%\s,]/g, "");
    const isNumericLike = (x) =>
      typeof x === "number" ? Number.isFinite(x)
      : typeof x === "string" ? (cleanNum(x) !== "" && !Number.isNaN(Number(cleanNum(x))))
      : false;
    const toNum = (x) => Number(cleanNum(x));

    // Excel serial (# of days since 1899-12-30); allow fractional day (time)
    const excelSerialToMs = (n) => Date.UTC(1899, 11, 30) + Math.round(Number(n) * 86400000);
    const dayKey = (ms) => Math.floor(ms / 86400000); // compare by day

    const parseDateLoose = (val) => {
      if (val == null) return NaN;
      if (val instanceof Date) return +val;

      if (isNumericLike(val)) {
        const n = toNum(val);
        if (n >= 60 && n < 100000) return excelSerialToMs(n); // Excel serial
        if (n >= 1e10) return n;                               // epoch ms
        if (n >= 1e5 && n < 1e10) return n * 1000;             // epoch s → ms
      }

      const s = String(val).trim();
      if (!s) return NaN;

      // Native parser (ISO / RFC, many human formats)
      const t = Date.parse(s);
      if (!Number.isNaN(t)) return t;

      // YYYYMMDD
      let m = s.match(/^(\d{4})(\d{2})(\d{2})$/);
      if (m) return Date.UTC(+m[1], +m[2]-1, +m[3]);

      // YYYY[-/.]MM[-/.]DD [hh:mm[:ss]]
      m = s.match(/^(\d{4})[\/\-.](\d{1,2})[\/\-.](\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
      if (m) return Date.UTC(+m[1], +m[2]-1, +m[3], +(m[4]||0), +(m[5]||0), +(m[6]||0));

      // D/M/Y or M/D/Y (assume M/D/Y unless first part > 12)
      m = s.match(/^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})$/);
      if (m) {
        let d = +m[1], mo = +m[2], y = +m[3];
        if (y < 100) y += (y >= 70 ? 1900 : 2000);
        if (d <= 12) [d, mo] = [mo, d];
        return Date.UTC(y, mo-1, d);
      }

      return NaN;
    };

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

    // build a predicate for a single term against one field id
    const buildTermPred = (id, term) => {
      term = term.trim();

      // [a,b,c] — strings, numbers, dates (date match by day)
      if (/^\[.*\]$/.test(term)) {
        let arr;
        try { arr = JSON.parse(term.replace(/'(.*?)'/g, '"$1"')); }
        catch { return () => true; }

        const strSet = new Set(arr.map(v => String(v)));
        const numSet = new Set(arr.map(v => toNum(v)).filter(Number.isFinite));
        const dateDaySet = new Set(arr.map(parseDateLoose).filter(Number.isFinite).map(dayKey));

        return (r) => {
          const raw = r?.[id];
          if (raw == null) return true; // missing field → no-op
          if (dateDaySet.size) {
            const ms = parseDateLoose(raw);
            if (Number.isFinite(ms) && dateDaySet.has(dayKey(ms))) return true;
          }
          if (strSet.has(String(raw))) return true;
          return isNumericLike(raw) && numSet.has(toNum(raw));
        };
      }

      // comparators: == != > < >= <=
      const m = term.match(/^(==|!=|>=|<=|>|<)\s*(.+)$/);
      if (m) {
        const op = m[1];
        const rhsRaw = m[2].replace(/^"(.*)"$|^'(.*)'$/, "$1$2");

        const rhsDate = parseDateLoose(rhsRaw);
        const rhsNum  = isNumericLike(rhsRaw) ? toNum(rhsRaw) : NaN;

        return (r) => {
          const lvRaw = r?.[id];
          if (lvRaw == null) return true; // missing field → no-op

          // Date compare (if RHS is a date)
          if (Number.isFinite(rhsDate)) {
            const L = parseDateLoose(lvRaw), R = rhsDate;
            if (!Number.isFinite(L)) return false;
            const dL = dayKey(L), dR = dayKey(R);
            switch (op) {
              case "==": return dL === dR; // by day
              case "!=": return dL !== dR;
              case ">":  return L >  R;
              case "<":  return L <  R;
              case ">=": return L >= R;
              case "<=": return L <= R;
              default:   return true;
            }
          }

          // Numeric compare
          if (isNumericLike(lvRaw) && Number.isFinite(rhsNum)) {
            const lv = toNum(lvRaw);
            switch (op) {
              case "==": return lv === rhsNum;
              case "!=": return lv !== rhsNum;
              case ">":  return lv  > rhsNum;
              case "<":  return lv  < rhsNum;
              case ">=": return lv >= rhsNum;
              case "<=": return lv <= rhsNum;
            }
          }

          // String compare fallback
          const Ls = String(lvRaw), Rs = String(rhsRaw);
          switch (op) {
            case "==": return Ls === Rs;
            case "!=": return Ls !== Rs;
            case ">":  return Ls >  Rs;
            case "<":  return Ls <  Rs;
            case ">=": return Ls >= Rs;
            case "<=": return Ls <= Rs;
            default:   return true;
          }
        };
      }

      // unsupported → pass
      return () => true;
    };

    // compile a full expression (A && B) || C for one field id
    const compile = (id, exprSrc) => {
      const expr = String(exprSrc || "").trim();
      if (!expr || expr === "*") return () => true;
      const orParts = splitTop(expr, "||");
      const orPreds = orParts.map(part => {
        const andParts = splitTop(part, "&&");
        const andPreds = andParts.map(t => buildTermPred(id, t));
        return r => andPreds.every(p => p(r));
      });
      return r => orPreds.some(p => p(r));
    };

    // expose compiler so other features (e.g., group_by) can reuse it
    if (!U.compileWherePred) U.compileWherePred = (id, where) => compile(id, where);

    // SAFEGUARD #1: skip filters whose field is missing on ALL rows
    const usable = (filters || []).filter(f => {
      if (!f?.id) return false;
      const existsSomewhere = rows.some(r => r && Object.prototype.hasOwnProperty.call(r, f.id));
      if (!existsSomewhere) {
        console.warn(`[filters] skipping filter on missing field '${f.id}'`);
        return false;
      }
      return true;
    });

    const preds = usable.map(f => {
      // legacy: { op:"in", values:[...] }
      if (f.op === "in" && Array.isArray(f.values)) {
        const strSet = new Set(f.values.map(String));
        const numSet = new Set(f.values.map(toNum).filter(Number.isFinite));
        const dateSet = new Set(f.values.map(parseDateLoose).filter(Number.isFinite).map(dayKey));
        return (r) => {
          const raw = r?.[f.id];
          if (raw == null) return true; // missing field → no-op
          if (dateSet.size) {
            const ms = parseDateLoose(raw);
            if (Number.isFinite(ms) && dateSet.has(dayKey(ms))) return true;
          }
          if (strSet.has(String(raw))) return true;
          return isNumericLike(raw) && numSet.has(toNum(raw));
        };
      }

      // SAFEGUARD #2: prefer `where`, fall back to legacy `filter` (warn once)
      const expr = (typeof f.where === "string" ? f.where : f.filter) ?? "";
      if (typeof f.filter === "string" && !("where" in f)) {
        console.warn("[filters] `filter` is deprecated; use `where` instead for", f.id);
      }
      return compile(f.id, expr);
    });

    return rows.filter(r => preds.every(p => p(r)));
  };

  
  /* merge helpers & formula evaluation */
  U.mergeByKey = (base, sources, keyId, cols, prim) => {
    const map = new Map(), key = r => U.keyOf(r, keyId);
    base.forEach(r => map.set(key(r), { ...r }));
    cols.forEach(c => {
      if (!c.source || c.source === prim) return;
      (sources[c.source] || []).forEach(sr => {
        const k = key(sr); 
        if (map.has(k)) Object.assign(map.get(k), sr);
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

  // ── Aggregate helpers (totals row) ─────────────────────────────────────────
  U._isNumLike = (x) => {
    if (x == null) return false;
    if (typeof x === "number") return Number.isFinite(x);
    if (typeof x !== "string") return false;
    const s = x.trim().replace(/[$,%\s,]/g, "");
    if (!s) return false;
    return !Number.isNaN(Number(s));
  };
  U._toNum = (x) => Number(String(x).trim().replace(/[$,%\s,]/g, ""));

  U._mode = (arr) => {
    const cnt = new Map(); let best = undefined, bestN = -1;
    for (const v of arr) {
      const k = String(v);
      const n = (cnt.get(k) || 0) + 1;
      cnt.set(k, n);
      if (n > bestN) { best = v; bestN = n; }
    }
    return best;
  };

  U.computeAggregateRow = (rows, columns, { skipIds = [], fnRegistry = {} } = {}) => {
    // 1) Aggregate base (non-function) columns, excluding primary/secondary keys
    const skip = new Set(skipIds || []);
    const dataCols = (columns || []).filter(c => c.column_type !== "function" && !skip.has(c.id));
    const out = {};

    const today = Date.now();
    for (const c of dataCols) {
      const type = String(c.data_type || "").toLowerCase();
      const vals = rows.map(r => r?.[c.id]).filter(v => v != null && v !== "");

      if (!vals.length) { out[c.id] = null; continue; }

      switch (type) {
        case "currency":
        case "float":
        case "number": {
          let sum = 0; for (const v of vals) if (U._isNumLike(v)) sum += U._toNum(v);
          out[c.id] = sum;
          break;
        }
        case "percent":
        case "rate": {
          let sum = 0, n = 0;
          for (const v of vals) {
            if (U._isNumLike(v)) { sum += U._toNum(v); n++; }
          }
          out[c.id] = n ? (sum / n) : null; // simple mean
          break;
        }
        case "integer": {
          // Keep consistent with prior grouping rule: mode for integer columns
          out[c.id] = U._mode(vals.map(v => U._isNumLike(v) ? U._toNum(v) : v));
          break;
        }
        case "date": {
          // Average age (days); display as "Xd"
          const ages = vals
            .map(v => new Date(v))
            .filter(d => Number.isFinite(d.getTime()))
            .map(d => (today - d.getTime()) / 86400000);
          const avg = ages.length ? (ages.reduce((a,b)=>a+b,0) / ages.length) : null;
          out[c.id] = (avg == null) ? null : `${Math.round(avg)} days`;
          break;
        }
        default: {
          // Strings / enums → mode
          out[c.id] = U._mode(vals.map(v => String(v)));
        }
      }
    }

    // 2) Compute function columns using aggregated inputs
    const fnCols = (columns || []).filter(c => c.column_type === "function");
    for (const c of fnCols) {
      const impl = fnRegistry?.[c.fn]?.implementation;
      if (typeof impl === "function") {
        const args = (c.params || []).map(id => out[id]); // use aggregated values
        try { out[c.id] = impl(...args); } catch { out[c.id] = null; }
      } else {
        out[c.id] = null;
      }
    }

    return out;
  };

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

  // Derive a new key field from groups of where-clauses.
  // Example: from_id "Class_Code" → as "Type_Group" with labels.
  U.deriveGroupField = (rows, groupCfg) => {
    if (!groupCfg) return rows;

    const {
      source,          // not used here; rows already from primary
      from_id,         // e.g., "Class_Code"
      as,              // e.g., "Type_Group"
      unmatched = "drop", // "drop" | "keep" | "label"
      groups = []
    } = groupCfg;

    if (!from_id || !as || !groups.length) return rows;

    const compiled = groups.map(g => ({
      label: g.label,
      pred: U.compileWherePred(from_id, g.where || "*")
    }));

    const out = [];
    for (const r of rows) {
      let label = null;
      for (const g of compiled) {
        if (g.pred(r)) { label = g.label; break; }
      }
      if (!label) {
        if (unmatched === "drop") continue;
        if (unmatched === "label") label = (groupCfg.unmatched_label || "Other");
        // unmatched === "keep" → leave undefined
      }
      const nr = { ...r, [as]: label };
      out.push(nr);
    }
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

  D.renderTableBars = function renderTableBars(tableEl, opts = {}) {
    const settings = {
      mode: 'cell',              // 'cell' | 'row' | 'cell+row'
      defaultScale: 'max',       // 'max' | 'range'
      minWidthPct: 2,
      animate: true,
      skipRowSelector: '.board-table__totals',
      ...opts
    };

    const table = tableEl instanceof Element ? tableEl : document.querySelector(tableEl);
    if (!table || !table.tHead) return;

    const wantCell = settings.mode === 'cell' || settings.mode === 'cell+row';
    const wantRow  = settings.mode === 'row'  || settings.mode === 'cell+row';

    const heads = Array.from(table.tHead.querySelectorAll('th'));
    const barColIdx = heads.reduce((acc, th, i) => (th.classList.contains('bar') ? (acc.push(i), acc) : acc), []);
    if (!barColIdx.length) return;

    const bodies = Array.from(table.tBodies || []);
    const colStats = new Map();
    barColIdx.forEach(idx => {
      const vals = [];
      bodies.forEach(tbody => {
        Array.from(tbody.rows).forEach(row => {
          if (row.matches(settings.skipRowSelector)) return;
          const cell = row.cells[idx];
          if (!cell) return;
          const v = parseCellNumber(cell.textContent);
          if (Number.isFinite(v)) vals.push(v);
        });
      });
      colStats.set(idx, { min: vals.length ? Math.min(...vals) : 0, max: vals.length ? Math.max(...vals) : 0 });
    });

    bodies.forEach(tbody => {
      Array.from(tbody.rows).forEach(row => {
        if (row.matches(settings.skipRowSelector)) return;

        barColIdx.forEach(idx => {
          const th = heads[idx];
          const colScale = th?.dataset?.scale || settings.defaultScale;

          const cell = row.cells[idx];
          if (!cell) return;

          // === CELL BARS ===
          if (wantCell) {
            if (!cell.querySelector('.bar-wrap')) { // idempotent
              const raw = (cell.textContent || '').trim();
              const value = parseCellNumber(raw);
              const { min, max } = colStats.get(idx) || { min: 0, max: 0 };
              const widthPct = computeWidth(value, min, max, colScale, settings.minWidthPct);

              const wrap = document.createElement('div');
              wrap.className = 'bar-wrap';

              const fill = document.createElement('span');
              fill.className = 'bar-fill';
              fill.style.width = widthPct + '%';
              if (settings.animate) fill.style.transition = 'width .6s ease';
              fill.classList.add(value < 0 ? 'neg' : 'pos');
              fill.setAttribute('aria-hidden', 'true');

              const text = document.createElement('span');
              text.className = 'bar-text';
              text.textContent = raw;

              cell.textContent = '';
              wrap.append(fill, text);
              cell.appendChild(wrap);
            }
          }
        });

        // === ROW SHADING ===
        if (wantRow) {
          const refIdx = barColIdx[0];
          const th = heads[refIdx];
          const colScale = th?.dataset?.scale || settings.defaultScale;
          const refCell = row.cells[refIdx];
          const v = parseCellNumber(refCell ? refCell.textContent : '');
          const { min, max } = colStats.get(refIdx) || { min: 0, max: 0 };
          const w = computeWidth(v, min, max, colScale, settings.minWidthPct);
          row.style.setProperty('--row-bar-width', w + '%');
          row.classList.add('row-bar');
        }
      });
    });

    // helpers
    function parseCellNumber(str) {
      if (!str) return NaN;
      const isPercent = /%/.test(str);
      let s = str.replace(/[\s,]/g, '').replace(/\$/g, '').trim();
      const parenNeg = /^\(.*\)$/.test(s);
      if (parenNeg) s = s.replace(/[()]/g, '');
      const num = parseFloat(s.replace('%', ''));
      if (!Number.isFinite(num)) return NaN;
      const signed = parenNeg ? -num : num;
      return isPercent ? signed : signed; // treat "225%" as 225 for distribution
    }
    function computeWidth(value, min, max, scale, minWidthPct) {
      if (!Number.isFinite(value)) return 0;
      if (scale === 'range') {
        const span = (max - min) || 1;
        const pct = ((value - min) / span) * 100;
        return clamp(pct, value !== 0 ? minWidthPct : 0, 100);
      }
      if (min < 0) {
        const absMax = Math.max(Math.abs(min), Math.abs(max)) || 1;
        const pct = ((value + absMax) / (2 * absMax)) * 100;
        return clamp(pct, value !== 0 ? minWidthPct : 0, 100);
      }
      const denom = max || 1;
      const pct = (value / denom) * 100;
      return clamp(pct, value !== 0 ? minWidthPct : 0, 100);
    }
    function clamp(n, min, max) { return Math.max(min, Math.min(max, n)); }
  };

  // ---- Your existing renderTable, with minimal additions ----------------
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

    const tbl = document.createElement("table");
    tbl.className = "board-table";
    sec.appendChild(tbl);

    const thead = tbl.createTHead();
    const hr = thead.insertRow();

    const interactive = new Set();
    if (enableHeaderMapping && primaryKeyId) interactive.add(primaryKeyId);
    if (enableHeaderMapping && secondaryKeys?.length) secondaryKeys.forEach(k => interactive.add(k));
    console.log("[renderTable] interactive column ids", [...interactive]);

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
          } else if (ch === ',' && !q) { out.push(cur); cur = ""; }
          else { cur += ch; }
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

      // NEW: allow cfg.columns[i].bar === true to mark bar columns
      if (c?.bar === true) th.classList.add("bar");
      // NEW: optional per-column scale override: 'max' | 'range'
      if (c?.barScale) th.dataset.scale = c.barScale;

      const numericTypes = ["integer","currency","percent","float","number","rate"];
      const isNum = numericTypes.includes((c.data_type || "").toLowerCase());
      if (isNum) th.classList.add("num");

      const isInteractive = interactive.has(c.id);
      console.log(`[renderTable] th ${c.id} (${c.heading || c.id}) interactive=${isInteractive}, bar=${th.classList.contains('bar')}`);

      if (isInteractive) {
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
            e.target.value = "";
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
        td.innerHTML = Trwth.utils.formatValue(r?.[c.id], c.data_type);
        const numericTypes = ["integer","currency","percent","float","number","rate"];
        if (numericTypes.includes((c.data_type || "").toLowerCase())) {
          td.classList.add("num");
        }
      });
    });

    // NEW: apply bar rendering after the table is populated
    try {
      D.renderTableBars(tbl, {
        mode: (opts.bar && opts.bar.mode) || 'row',  // or cell
        defaultScale: (opts.bar && opts.bar.scale) || 'max',
        minWidthPct: (opts.bar && opts.bar.minWidthPct) || 2,
        animate: (opts.bar && opts.bar.animate) ?? true
      });
    } catch (e) {
      console.warn("[renderTable] bar rendering skipped:", e);
    }

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
      const D = Trwth.dom;

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

        // Render inline table in the card (header mapping OFF here), plus totals row
        if (block.type === "table") {
          try {
            const rows = buildRowsForTable(block, sources);

            const tbl = Trwth.dom.renderTable(
              body,
              { title: block.title, columns: block.columns },
              rows,
              {
                primaryKeyId: block.primary_key?.id,
                secondaryKeys: block.secondary_keys || [],
                enableHeaderMapping: true 
              }
            );

            // ── Totals row (skip primary + secondary keys; recompute function columns) ──
            try {
              const skipIds = [
                block.primary_key?.id,
                ...(block.secondary_keys || [])
              ].filter(Boolean);

              const agg = U.computeAggregateRow(
                rows,
                block.columns,
                { skipIds, fnRegistry: (window.financial?.functions) || {} }
              );

              const tfoot = tbl.createTFoot();
              const trTot = tfoot.insertRow();
              trTot.className = "board-table__totals";

              block.columns.forEach((c, idx) => {
                const td = trTot.insertCell();
                const isSkipped = skipIds.includes(c.id);

                if (isSkipped) {
                  // Put a label under the first skipped column (usually the PK)
                  if (idx === 0 || c.id === block.primary_key?.id) {
                    td.textContent = "Total";
                    td.style.fontWeight = "700";
                  } else {
                    td.textContent = "";
                  }
                  return;
                }

                const v = Object.prototype.hasOwnProperty.call(agg, c.id) ? agg[c.id] : null;
                const type = String(c.data_type || "").toLowerCase();
                td.innerHTML = U.formatValue(v, type);
                if (["integer","currency","percent","float","number","rate"].includes(type)) {
                  td.classList.add("num");
                  td.style.fontWeight = "600";
                }
              });
            } catch (tErr) {
              console.warn("[totals] failed to render aggregate row:", tErr);
            }
          } catch (e) {
            console.error("[grid] render error:", e);
            body.textContent = "Render error: " + e.message;
          }
        }

        // Drilldown → modal with PK + secondary + primary-source cols (header mapping ON)
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

          // Base rows: primary source + only filters for that source
          const base = [...(sources[prim] || [])];
          const scoped = (block.filters || []).filter(f => (f.source || prim) === prim);
          const filtered = Trwth.utils.applyFilters(base, scoped);

          // If group_by applies to this primary source, annotate rows with group label (so PK can render),
          // and remember the original field (from_id) to include in the drill columns.
          const gb = block.group_by;
          const gbSrc = gb?.source || prim;
          const gbFromId = gb?.from_id;
          const gbAs = gb?.as;

          if (gb && gbSrc === prim && gbFromId && gbAs) {
            const compile = Trwth.utils.compileWherePred || ((id, where) => (() => true));
            const rules = (gb.groups || [])
              .filter(g => typeof g?.label === "string" && typeof g?.where === "string" && g.where.trim() !== "*")
              .map(g => ({ label: g.label, pred: compile(gbFromId, g.where) }));
            const hasFallback = (gb.groups || []).some(g => String(g?.where || "").trim() === "*");
            const policy = (gb.unmatched || "keep"); // "drop" | "keep" | "label"

            for (const r of filtered) {
              let label = null;
              for (const g of rules) {
                if (g.pred(r)) { label = g.label; break; }
              }
              if (!label) {
                if (hasFallback) {
                  // use the first '*' fallback label
                  const fb = (gb.groups || []).find(g => String(g?.where || "").trim() === "*");
                  label = fb?.label ?? null;
                } else if (policy === "label" && typeof gb.label === "string") {
                  label = gb.label;
                } else if (policy === "keep") {
                  label = String(r?.[gbFromId] ?? "");
                } else if (policy === "drop") {
                  // Leave label null; PK cell may be blank, but we still keep row in drilldown
                }
              }
              if (label != null) r[gbAs] = label;
            }
          }

          // Column order for drilldown:
          // PK → secondary keys → (group_by.from_id if present) → all block columns from the primary source (excluding formulas)
          const colsById = new Map((block.columns || []).map(c => [c.id, c]));
          const order = [];
          const pushUnique = (id) => { if (id && !order.includes(id)) order.push(id); };

          pushUnique(pkId);
          (block.secondary_keys || []).forEach(pushUnique);
          if (gbFromId) pushUnique(gbFromId); // <— ensure original field is shown

          (block.columns || [])
            .filter(c => c.source === prim && c.column_type !== "formula")
            .forEach(c => pushUnique(c.id));

          // Build defs, fabricating one for from_id if it isn't in block.columns
          const drillDefs = order.map(id => {
            const def = colsById.get(id);
            if (def) return def;

            // create a lightweight def for ad-hoc columns (like group_by.from_id)
            let heading = id;
            if (gbFromId && id === gbFromId && typeof gb?.from_heading === "string") {
              heading = gb.from_heading;
            }

            // naive data_type sniff (optional)
            let data_type;
            for (let i = 0; i < filtered.length; i++) {
              const v = filtered[i]?.[id];
              if (v != null && v !== "") {
                const n = Number(String(v).replace(/[$,%\s,]/g, ""));
                if (!Number.isNaN(n)) { data_type = Number.isInteger(n) ? "integer" : "number"; }
                break;
              }
            }
            return { id, heading, data_type, source: prim };
          });

          console.log("[drill] computed", {
            pkId,
            primarySource: prim,
            secondaryKeys: block.secondary_keys || [],
            drillCols: drillDefs.map(c => c.id),
            filteredRows: filtered.length,
            group_from_id: gbFromId
          });

          // Modal + render, mapping enabled
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
            if (tbl && !container.contains(tbl)) container.appendChild(tbl);
          } catch (e) {
            console.error("[drill] renderTable threw; falling back to manual table", e);
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
          <span class="subtle">(CSV/XLSX)</span>
          <span class="filename"></span>
        `;

        const input = document.createElement("input");
        input.type = "file";
        input.accept = [
          ".csv",
          "text/csv",
          ".xlsx",
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ].join(",");
        input.className = "hidden-file-input";

        label.appendChild(input);
        row.appendChild(label);
        pane.appendChild(row);

        labels[src] = label;

        input.onchange = async (e) => {
          const file = e.target.files && e.target.files[0];
          if (!file) return;

          try {
            // Dispatch to CSV/XLSX transparently
            const rows = await Trwth.io.fileToRows(file);
            state[src] = rows;

            label.classList.add("completed");
            label.querySelector(".filename").textContent = file.name;
            console.log(`[loader] ${src}: loaded ${rows.length} rows from ${file.name}`);
          } catch (err) {
            console.error(`[loader] parse error for ${src}:`, err);
            delete state[src];
            label.classList.remove("completed");
            label.querySelector(".filename").textContent = "Parse error";
          } finally {
            check();
            // allow re-pick of the same file name later
            e.target.value = "";
          }
        };
      });
      console.log('state', state)

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

    // Supports formula columns like:
    // { heading: "Type Count", id: "type_count", column_type: "formula", data_type: "integer", formula: "count",   source: "loan", target_id: "Class_Code" },
    // { heading: "Average Principal", id: "averagePrincipal", column_type: "formula", data_type: "currency", formula: "average", source: "loan", target_id: "Principal" }
    function buildRows(tbl, sources) {
      const U = Trwth.utils;
      const log = (...a) => console.log("[buildRows]", ...a);

      // ── 0) Validate & resolve primary key + primary source ─────────────────────
      if (!tbl?.primary_key?.id) throw new Error("primary_key.id is required");
      const keyId = tbl.primary_key.id;

      const keyCol = (tbl.columns || []).find(c => c.id === keyId);
      const prim   = tbl.primary_key.source || keyCol?.source;
      if (!prim) throw new Error("primary_key.source could not be resolved");

      const allCols = tbl.columns || [];

      // Helpers
      const cleanNum = (x) => String(x).trim().replace(/[$,%\s,]/g, "");
      const isNumericLike = (x) =>
        typeof x === "number" ? Number.isFinite(x)
        : typeof x === "string" ? (cleanNum(x) !== "" && !Number.isNaN(Number(cleanNum(x))))
        : false;
      const toNum = (v) => Number.isFinite(+v) ? +v : (isNumericLike(v) ? +cleanNum(v) : 0);

      // ── 1) Start with primary source rows ──────────────────────────────────────
      const primRowsRaw = [...(sources[prim] || [])];

      // Apply ONLY filters scoped to the primary source
      const primFilters = (tbl.filters || []).filter(f => (f.source || prim) === prim);
      let primFiltered = U.applyFilters(primRowsRaw, primFilters);
      log("primary:", prim, "filtered rows:", primFiltered.length);

      // ── 2) Optional: GROUP BY (derive a label field) ───────────────────────────
      // Example:
      // group_by: { source:"loans", from_id:"Class_Code", as:"Type_Group",
      //             unmatched:"drop|keep|label", groups:[{label, where}, {label, where}, ...] }
      const gb = tbl.group_by;
      let groupIndex = null; // Map<groupKey, Set<join_on values>> for group-aware joins
      if (gb) {
        const gbSrc = gb.source || prim;
        if (gbSrc !== prim) {
          console.warn("[buildRows] group_by.source differs from primary; only primary supported for labeling in this pass.");
        }
        const fromId = gb.from_id;
        const asId   = gb.as || keyId;
        const unmatchedMode = (gb.unmatched || "drop").toLowerCase(); // drop|keep|label

        // compile predicates once
        const anyGroup = gb.groups?.some(g => g?.where === "*");
        const compiled = (gb.groups || []).map(g => ({
          label: g.label,
          pred: g.where === "*" ? (() => true) : (U.compileWherePred ? U.compileWherePred(fromId, g.where) : (() => true)),
          where: g.where
        }));

        const out = [];
        groupIndex = new Map(); // groupKey -> Set(join_on candidates) — filled later on demand

        for (const r of primFiltered) {
          let label = null;
          for (const g of compiled) {
            try {
              if (g.pred(r)) { label = g.label; break; }
            } catch {
              // ignore bad predicate
            }
          }

          if (!label) {
            if (unmatchedMode === "drop") continue;
            if (unmatchedMode === "keep") label = String(r?.[fromId] ?? "");
            if (unmatchedMode === "label") label = (gb.unmatched_label || "Other");
            // if still empty and a catch-all exists, give it to that
            if (!label && anyGroup) {
              const catchAll = compiled.find(g => g.where === "*");
              label = catchAll?.label || label;
            }
          }

          if (!label) continue; // still no label → drop
          const copy = { ...r, [asId]: label };
          out.push(copy);
        }

        primFiltered = out;
        // If the group writes to a different key id, ensure primary_key.id sees it
        if (asId !== keyId) {
          console.warn("[buildRows] group_by.as differs from primary_key.id; using primary_key.id =", keyId, "but labels were written to", asId);
        }
        log("group_by:", { from: gb.from_id, as: asId, rowsLabeled: primFiltered.length });
      }

      // ── 3) Aggregate by primary key (exclude any secondary_keys) ───────────────
      const aggregated = U.aggregateByPrimary
        ? U.aggregateByPrimary(
            primFiltered,
            keyId,
            allCols,
            prim,
            tbl.secondary_keys || []
          )
        : primFiltered; // fallback if aggregator not present
      log("aggregated rows:", aggregated.length);

      // ── 4) Merge in non-primary sources (simple key join) ──────────────────────
      // (For special group-aware join_on, we’ll add an extra step below.)
      let merged = U.mergeByKey
        ? U.mergeByKey(aggregated, sources, keyId, allCols, prim)
        : aggregated;

      // ── 4b) Group-aware joins for columns that specify `join_on` ───────────────
      // When the primary key is a derived group label (e.g., Type_Group),
      // and a non-primary column wants to join on e.g. Class_Code, we aggregate
      // that external source over the set of join keys that appear within each group.
      const joinOnCols = allCols.filter(c => c.source && c.source !== prim && c.join_on);
      if (joinOnCols.length) {
        // Build group -> Set(join_on values) from *primary filtered rows* (pre-aggregation)
        const groupKeyField = keyId; // primary key field in aggregated/merged rows
        const joinKeySets = new Map(); // pk -> Set(join_on values)
        for (const r of primFiltered) {
          const pk = String(r?.[groupKeyField] ?? "");
          if (!pk) continue;
          let set = joinKeySets.get(pk);
          if (!set) { set = new Set(); joinKeySets.set(pk, set); }
          for (const c of joinOnCols) {
            const val = r?.[c.join_on];
            if (val != null && val !== "") set.add(String(val));
          }
        }

        // For each join_on column, compute a value per pk
        for (const c of joinOnCols) {
          const srcRowsRaw = [...(sources[c.source] || [])];
          const scopedFilters = (tbl.filters || []).filter(f => (f.source || c.source) === c.source);
          const srcRows = U.applyFilters ? U.applyFilters(srcRowsRaw, scopedFilters) : srcRowsRaw;

          // Build map: pk -> aggregated value from srcRows whose join_on is in that pk's joinKey set
          const valueByPk = new Map();

          for (const [pk, set] of joinKeySets) {
            if (!set.size) continue;

            let aggNum = null; // sum for numeric; first non-empty for strings
            let firstStr = null;

            for (const sr of srcRows) {
              const keyVal = sr?.[c.join_on];
              if (keyVal == null) continue;
              if (!set.has(String(keyVal))) continue;

              const v = sr?.[c.id];

              if (isNumericLike(v)) {
                const n = toNum(v);
                aggNum = (aggNum == null) ? n : (aggNum + n);
              } else {
                if (firstStr == null && v != null && String(v).trim() !== "") {
                  firstStr = v;
                }
              }
            }

            const finalVal = (aggNum != null) ? aggNum : (firstStr ?? null);
            valueByPk.set(pk, finalVal);
          }

          // Write results back into merged rows
          merged.forEach(row => {
            const pk = String(row?.[groupKeyField] ?? "");
            if (!pk) return;
            if (valueByPk.has(pk)) row[c.id] = valueByPk.get(pk);
          });
        }
      }

      // ── 5) Per-key FORMULA columns (count, average, sum, min, max, distinct…) ──
      const formulaCols = allCols.filter(c => c.column_type === "formula" && typeof c.formula === "string");
      if (formulaCols.length) {
        // Precompute source-scoped rows; for primary source use the **group-labeled** / filtered rows
        const scopedCache = new Map(); // src -> rows (already filtered)
        const getScopedRows = (src) => {
          if (scopedCache.has(src)) return scopedCache.get(src);
          if (src === prim) {
            scopedCache.set(src, primFiltered.slice()); // use labeled/filtered prim rows
            return primFiltered;
          }
          const base = [...(sources[src] || [])];
          const f = (tbl.filters || []).filter(ff => (ff.source || src) === src);
          const out = U.applyFilters ? U.applyFilters(base, f) : base;
          scopedCache.set(src, out);
          return out;
        };

        const normalizeFormula = (s) => (s || "").toLowerCase().trim()
          .replace(/^avg$/, "average")
          .replace(/^distinct$/, "distinct_count");

        // Compute each formula per primary key
        for (const c of formulaCols) {
          const src = c.source || prim;
          const fName = normalizeFormula(c.formula);
          if (!["count","average","sum","min","max","distinct_count"].includes(fName)) {
            console.warn(`[buildRows] unsupported formula '${c.formula}' on column '${c.id}'`);
            continue;
          }

          const rows = getScopedRows(src);
          const perKey = new Map();

          for (const r of rows) {
            const pk = String(r?.[keyId] ?? "");
            if (!pk) continue;

            switch (fName) {
              case "count": {
                if (c.target_id) {
                  const v = r?.[c.target_id];
                  if (v == null || v === "") break;
                }
                perKey.set(pk, (perKey.get(pk) || 0) + 1);
                break;
              }
              case "sum": {
                if (!c.target_id) break;
                const n = toNum(r?.[c.target_id]);
                perKey.set(pk, (perKey.get(pk) || 0) + n);
                break;
              }
              case "average": {
                if (!c.target_id) break;
                const n = toNum(r?.[c.target_id]);
                if (!Number.isFinite(n)) break;
                const agg = perKey.get(pk) || { sum: 0, count: 0 };
                agg.sum += n; agg.count += 1;
                perKey.set(pk, agg);
                break;
              }
              case "min":
              case "max": {
                if (!c.target_id) break;
                const n = toNum(r?.[c.target_id]);
                if (!Number.isFinite(n)) break;
                if (!perKey.has(pk)) perKey.set(pk, n);
                else perKey.set(pk, fName === "min" ? Math.min(perKey.get(pk), n) : Math.max(perKey.get(pk), n));
                break;
              }
              case "distinct_count": {
                if (!c.target_id) break;
                const v = r?.[c.target_id];
                let set = perKey.get(pk);
                if (!set) set = new Set();
                if (v != null) set.add(String(v));
                perKey.set(pk, set);
                break;
              }
            }
          }

          // finalize structures
          if (fName === "average") {
            for (const [k, agg] of perKey) perKey.set(k, agg.count ? (agg.sum / agg.count) : 0);
          } else if (fName === "distinct_count") {
            for (const [k, set] of perKey) perKey.set(k, set.size);
          }

          // write onto merged rows
          merged.forEach(row => {
            const pk = String(row?.[keyId] ?? "");
            row[c.id] = perKey.has(pk)
              ? perKey.get(pk)
              : (fName === "count" || fName === "distinct_count" ? 0 : null);
          });
        }
      }

      // ── 6) Evaluate classic function columns on top of everything ──────────────
      const fnCols = allCols.filter(c => c.column_type === "function");
      const result = U.evaluateFunctionColumns
        ? U.evaluateFunctionColumns(merged, fnCols, (window.financial && window.financial.functions) || {})
        : merged;

      log("out rows:", result.length);
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

    // ── Export current dashboard → standalone HTML (no drilldown/UI) ─────────────
    Trwth.core.exportDashboard = async function exportDashboard(opts = {}) {
      const grid = document.querySelector(".board-grid");
      if (!grid) {
        console.warn("[export] no .board-grid found");
        alert("Nothing to export yet.");
        return;
      }

      // Grab layout tokens
      const cfgLayout = (window.boardConfig && window.boardConfig.board_layout) || {};
      const cs   = getComputedStyle(grid);
      const cols = parseInt(cs.getPropertyValue("--cols"))       || cfgLayout.cols       || 12;
      const rowH = parseInt(cs.getPropertyValue("--row-height")) || cfgLayout.row_height || 110;
      const gap  = parseInt(cs.getPropertyValue("--gap"))        || cfgLayout.gap        || 8;

      // Clone and sanitize cards (remove actions, resizers, unwrap header-map buttons, drop <h2>)
      const container = document.createElement("div");
      container.className = "board-grid";
      container.style.setProperty("--cols", cols);
      container.style.setProperty("--row-height", rowH + "px");
      container.style.setProperty("--gap", gap + "px");

      Array.from(grid.querySelectorAll(".board-card")).forEach((card) => {
        const x = +card.dataset.x || 1, y = +card.dataset.y || 1;
        const w = +card.dataset.w || 6, h = +card.dataset.h || 3;

        const clone = card.cloneNode(true);

        // strip runtime-only bits
        clone.querySelector(".board-card__actions")?.remove();
        clone.querySelector(".board-card__resizer")?.remove();

        // unwrap any header mapping buttons if they exist (modal-only in app, but safe)
        clone.querySelectorAll("button.th-map-btn").forEach(btn => {
          const th = btn.closest("th");
          if (th) th.textContent = btn.textContent || th.textContent;
          else btn.replaceWith(document.createTextNode(btn.textContent || ""));
        });

        // drop <h2> subtitles inside bodies (the card header already holds the title)
        clone.querySelectorAll(".board-section > h2, section.board-section > h2, h2").forEach(h => h.remove());

        // re-apply grid position as inline style
        clone.style.gridColumn = `${x}/span ${w}`;
        clone.style.gridRow    = `${y}/span ${h}`;
        Object.assign(clone.dataset, { x, y, w, h });

        container.appendChild(clone);
      });

      const STYLE = `
      :root{
        --bg:#0f1216;--panel:#1a1f26;--panel-2:#161b21;--border:#2a313a;--border-soft:#222a33;
        --fg:#e8ecf1;--fg-muted:#aeb7c2;--shadow:0 8px 22px rgba(0,0,0,.35);
        --radius:12px;--gap:8px;
        /* bar + stripe colors */
        --bar-pos:#1e6b9f; --bar-neg:#ff5b5b; --row-bar-color:#1e6b9f; --stripe:rgba(255,255,255,.02);
      }
      html,body{background:var(--bg);color:var(--fg);font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,"Noto Sans";margin:0}
      .board-grid{display:grid;grid-template-columns:repeat(var(--cols,12),1fr);grid-auto-rows:var(--row-height,110px);gap:var(--gap);padding:12px}
      .board-card{display:flex;flex-direction:column;min-width:0;background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);overflow:hidden}
      .board-card__header{padding:10px 12px;font-weight:600;font-size:1.25rem;color:var(--fg);background:linear-gradient(180deg,var(--panel-2),transparent);border-bottom:1px solid var(--border-soft)}
      .board-card__body{min-height:0;overflow:auto;padding:10px 12px}

      .board-table{width:100%;border-collapse:separate;border-spacing:0}
      .board-table thead th{position:sticky;top:0;z-index:1;text-align:left;font-weight:600;color:var(--fg);
        background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,0));border-bottom:1px solid var(--border);padding:8px 10px}
      .board-table th.num, .board-table td.num{text-align:right}

      /* rows: stripe only when NOT shaded by a row bar */
      .board-table tbody tr{background:transparent}
      .board-table tbody tr:nth-child(even):not(.row-bar){background-color:var(--stripe)}

      .board-table td{color:var(--fg);padding:8px 10px;border-bottom:1px solid var(--border-soft);white-space:nowrap;text-overflow:ellipsis;overflow:hidden}

      /* in-cell bars (rendered only if mode includes 'cell') */
      .board-table td .bar-wrap{position:relative;display:block;padding:2px 0}
      .board-table td .bar-fill{position:absolute;left:0;top:50%;transform:translateY(-50%);height:70%;border-radius:6px;opacity:.25;pointer-events:none}
      .board-table td .bar-fill.pos{background:var(--bar-pos)}
      .board-table td .bar-fill.neg{background:var(--bar-neg)}
      .board-table td .bar-text{position:relative;z-index:1;display:inline-block;padding:0 .25rem;white-space:nowrap}

      /* whole-row shading (when mode includes 'row') — comes AFTER stripe rule */
      .board-table tr.row-bar{
        background:
          linear-gradient(
            to right,
            var(--row-bar-color) 0%,
            var(--row-bar-color) var(--row-bar-width,0%),
            transparent var(--row-bar-width,0%)
          );
        background-size:100% 100%;
        background-repeat:no-repeat;
      }

      /* keep totals/footer unchanged */
      .board-table tfoot .bar-wrap,
      .board-table .board-table__totals .bar-wrap{display:contents}
      `;

      // favicon (same as live page)
      const faviconHref = (Trwth.core.renderFavicon && Trwth.core.renderFavicon()) || "";

      const title = document.title || "Trwth Dashboard";
      const html = `<!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width,initial-scale=1"/>
    <title>${title}</title>
    <link rel="icon" type="image/svg+xml" href="${faviconHref}"/>
    <style>${STYLE}</style>
    </head>
    <body>
    ${container.outerHTML}
    </body>
    </html>`;

      // Save As: File System Access API when available; fallback to download
      const saveName = (title || "trwth-dashboard").replace(/[\\/:*?"<>|]+/g, "_") + ".html";
      try {
        if ("showSaveFilePicker" in window) {
          const handle = await window.showSaveFilePicker({
            suggestedName: saveName,
            types: [{ description: "HTML", accept: { "text/html": [".html"] } }],
          });
          const writable = await handle.createWritable();
          await writable.write(new Blob([html], { type: "text/html" }));
          await writable.close();
          return;
        }
      } catch (e) {
        // user canceled or API blocked → fall through to download
        console.warn("[export] save picker failed; using download fallback", e);
      }

      // Fallback: regular download (lets browser decide location)
      const blob = new Blob([html], { type: "text/html" });
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement("a");
      a.href = url; a.download = saveName;
      document.body.appendChild(a); a.click();
      setTimeout(() => { URL.revokeObjectURL(url); a.remove(); }, 800);
    };

  })();

  // ── Trwth favicon (SVG data-URI) ─────────────────────────────
  Trwth.core.renderFavicon = function renderFavicon() {
    const href =
      'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA2NCA2NCI+PHBhdGggZmlsbD0iIzJFOEJDMCIgZD0iTTE2IDEyLjhMMzIgMEw0OCAxMi44TDQ4IDUxLjJMMzIgNjRMMTYgNTEuMloiLz48cGF0aCBmaWxsPSIjMEEyNTQwIiBkPSJNMCAwTDE2IDEyLjhMMzIgNjRMMTYgNTEuMloiLz48cGF0aCBmaWxsPSIjNThDNkIxIiBkPSJNNjQgMEw0OCAxMi44TDMyIDY0TDQ4IDUxLjJaIi8+PC9zdmc+';
    // remove old icons, add the new one
    document.querySelectorAll('link[rel="icon"]').forEach(n => n.remove());
    const link = document.createElement('link');
    link.rel = 'icon';
    link.type = 'image/svg+xml';
    link.href = href;
    document.head.appendChild(link);
    return href; // handy to reuse as a background-image
  };

  /* ───── 7. Auto-start ───── */
  const start = () => Trwth.core.bootstrap();
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }

  Trwth.core.renderFavicon();
})();

// Add a floating "Export HTML" button once per page
(() => {
  if (document.querySelector(".trwth-export-fab")) return;
  const href = (Trwth.core.renderFavicon && Trwth.core.renderFavicon()) || "";

  const btn = document.createElement("button");
  btn.className = "trwth-export-fab";
  btn.type = "button";
  btn.title = "Export dashboard as HTML";
  btn.setAttribute("aria-label", "Export dashboard as HTML");
  btn.style.cssText = `
    position:fixed; right:14px; bottom:14px; z-index:9999;
    width:56px; height:56px; border-radius:50%;
    border:1px solid #2a313a; background:#313e48; cursor:pointer;
    box-shadow:0 8px 22px rgba(0,0,0,.35), inset 0 0 0 999px rgba(0,0,0,.0);
    display:inline-flex; align-items:center; justify-content:center;
    padding:0; outline: none;
  `;
  const img = document.createElement("div");
  img.style.cssText = `
    width:28px; height:28px; border-radius:50%;
    background-image:url('${href}');
    background-size:contain; background-position:center; background-repeat:no-repeat;
  `;
  btn.appendChild(img);
  btn.addEventListener("click", () => Trwth.core.exportDashboard());
  document.body.appendChild(btn);
})();
