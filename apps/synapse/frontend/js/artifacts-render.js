/** Artifact rendering for the chat surfaces: charts, tables, KPI
 * tiles, dashboards, diagrams, and the strip under every number.
 *
 * This file is IDENTICAL on both surfaces (apps/synapse_admin and
 * apps/synapse); a test pins that. What differs per surface — the
 * words under a number, the markdown renderer, the escaper — arrives
 * through createArtifactRenderer(hooks). Nothing here calls the API
 * or touches the page outside the container it is handed.
 *
 * numberFormat / formatValue are the JS twins of number_format /
 * format_value in sahs/assistant/artifacts.py: same rules, same
 * answers on the shared cases (tests/fixtures/number_format_cases.json).
 * The selection heuristic (choose_visual) lives server-side; its
 * reason rides in the spec and shows here on hover.
 *
 * Charts are hand-drawn SVG: axes, gridlines, legends, <title>
 * tooltips, sorted categories, an optional reference line, a colour
 * scale for heatmaps; the CSS tokens carry light and dark, and the
 * reduced-motion rules in the stylesheet's artifacts block stop every
 * animation. Every kind in CHART_KINDS draws; the list is the server's.
 */

export const CHART_KINDS = ["line", "area", "bar", "hbar", "stacked_bar",
  "percent_bar", "scatter", "histogram", "heatmap", "small_multiples",
  "combo", "waterfall"];
// categorical hues in a fixed order, never cycled: past six, the
// validator refuses the series (small multiples share the first)
export const PALETTE = ["#2f6feb", "#e8710a", "#1a9850", "#9970ab",
                        "#d6604d", "#35978f"];
const UP = "#1a9850";
const DOWN = "#d6604d";

// ═══ number formatting, from the data (twin of artifacts.py) ═══

const MAX_DECIMALS = 4;
const CURRENCY_DECIMALS = 2;
const COMPACT_ABOVE = 100000;
const NULL_TEXT = "—";
const CURRENCY_SYMBOLS = { USD: "$", EUR: "€", GBP: "£", JPY: "¥",
  INR: "₹", CAD: "$", AUD: "$", $: "$", "€": "€", "£": "£", "¥": "¥",
  "₹": "₹" };
const CURRENCY_CODES = ["usd", "eur", "gbp", "jpy", "inr", "cad", "aud",
  "chf", "cny", "mxn", "brl", "sgd", "hkd"];
const DURATION_UNITS = { ms: "ms", millisecond: "ms", milliseconds: "ms",
  s: "s", sec: "s", secs: "s", second: "s", seconds: "s", min: "min",
  mins: "min", minute: "min", minutes: "min", h: "h", hr: "h", hrs: "h",
  hour: "h", hours: "h", d: "d", day: "d", days: "d" };
const FORMAT_KINDS = ["percent", "currency", "count", "ratio", "duration",
  "date", "number", "identifier", "text"];
const DATE_VALUE = /^\d{4}-\d{2}(-\d{2})?([T ].*)?$/;
const ID_NAME = /(^|_)(id|ids|uuid|guid|code|zip|postal|phone|sku|iban|key|account_no|acct|mid|pan)($|_)|_number$|_no$/;
const DATE_NAME = /(^|_)(date|day|month|week|quarter|year|period|dt|ts|time|timestamp)($|_)|_at$|_on$/;
const PERCENT_NAME = /(^|_)(pct|percent|percentage|share|rate|conversion|penetration|cvr|ctr|apr)($|_)|_pc$|%/;
const CURRENCY_NAME = /(^|_)(spend|revenue|sales|cost|costs|amount|amt|price|fee|fees|gmv|usd|dollars|value|margin|profit|income|budget|volume_usd|tpv|aov)($|_)/;
const COUNT_NAME = /(^|_)(count|cnt|n|num|number|qty|quantity|transactions|txns|users|rows|orders|visits|sessions|customers|merchants|accounts|events|clicks|impressions|total)($|_)|^number_|^num_/;
const RATIO_NAME = /(^|_)(ratio|multiple|index|factor|per)($|_)|_per_|_x$/;
const DURATION_NAME = /(^|_)(duration|latency|elapsed|age|tenure|ttl|wait)($|_)|_(ms|secs?|seconds|mins?|minutes|hours|hrs|days)$/;

const normName = (name) => String(name || "").toLowerCase()
  .replace(/[^a-z0-9%]+/g, "_").replace(/^_+|_+$/g, "");

export function asNumber(value) {
  if (value === null || value === undefined || typeof value === "boolean") {
    return null;
  }
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const text = value.trim().replace(/,/g, "");
    if (!text) return null;
    const out = Number(text);
    return Number.isFinite(out) ? out : null;
  }
  return null;
}

export function decimalsOf(value) {
  if (!Number.isFinite(value) || Number.isInteger(value)) return 0;
  let text = value.toPrecision(10);
  if (/e/i.test(text)) text = value.toFixed(MAX_DECIMALS);
  if (!text.includes(".")) return 0;
  return Math.min(MAX_DECIMALS, text.split(".")[1].replace(/0+$/, "").length);
}

function median(nums) {
  if (!nums.length) return 0;
  const ordered = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(ordered.length / 2);
  return ordered.length % 2 ? ordered[mid]
    : (ordered[mid - 1] + ordered[mid]) / 2;
}

function kindFromUnit(unit) {
  const u = String(unit || "").trim();
  const low = u.toLowerCase();
  if (!u) return ["", ""];
  if (["%", "percent", "pct", "percentage"].includes(low)) return ["percent", "%"];
  if (CURRENCY_SYMBOLS[u.toUpperCase()] || CURRENCY_CODES.includes(low)) {
    return ["currency", u.length === 3 ? u.toUpperCase() : u];
  }
  if (DURATION_UNITS[low]) return ["duration", DURATION_UNITS[low]];
  if (["x", "ratio", "multiple"].includes(low)) return ["ratio", low === "x" ? "x" : ""];
  if (["count", "n", "rows", "number", "#"].includes(low)) return ["count", ""];
  if (["date", "day", "month"].includes(low)) return ["date", ""];
  return ["", u];
}

function kindFromName(name) {
  if (!name) return ["", ""];
  if (ID_NAME.test(name)) return ["identifier", ""];
  if (PERCENT_NAME.test(name)) return ["percent", "%"];
  if (DURATION_NAME.test(name)) {
    const tail = name.split("_").pop();
    return ["duration", DURATION_UNITS[tail] || ""];
  }
  if (RATIO_NAME.test(name)) return ["ratio", ""];
  if (CURRENCY_NAME.test(name)) return ["currency", ""];
  if (COUNT_NAME.test(name)) return ["count", ""];
  if (DATE_NAME.test(name)) return ["date", ""];
  return ["", ""];
}

function cleanFormat(fmt) {
  if (typeof fmt === "string") {
    const word = fmt.trim().toLowerCase();
    if (FORMAT_KINDS.includes(word)) return { kind: word };
    const [kind, unit] = kindFromUnit(fmt);
    return kind ? { kind, unit } : {};
  }
  if (!fmt || typeof fmt !== "object") return {};
  const out = {};
  const kind = String(fmt.kind || "").trim().toLowerCase();
  if (FORMAT_KINDS.includes(kind)) out.kind = kind;
  if (typeof fmt.decimals === "number" && Number.isFinite(fmt.decimals)) {
    out.decimals = Math.max(0, Math.min(MAX_DECIMALS, Math.trunc(fmt.decimals)));
  }
  if (fmt.unit !== undefined && fmt.unit !== null) out.unit = String(fmt.unit).slice(0, 12);
  for (const flag of ["compact", "grouping"]) {
    if (typeof fmt[flag] === "boolean") out[flag] = fmt[flag];
  }
  if (fmt.sign === "auto" || fmt.sign === "always") out.sign = fmt.sign;
  if (fmt.scale === 1 || fmt.scale === 100) out.scale = fmt.scale;
  return out;
}

/** How a column of values reads — see number_format in artifacts.py
 * for the rules (kind, decimals from the data, grouping, compact
 * units past a 100k median, the percent 0..1 vs 0..100 rule). */
export function numberFormat(values, hint) {
  hint = hint || {};
  const name = normName(hint.name || hint.key || hint.label);
  const explicit = cleanFormat(hint.format);
  let [unitKind, unit] = kindFromUnit(hint.unit || "");
  const list = Array.isArray(values) ? values : [];
  const nums = list.map(asNumber).filter((n) => n !== null);
  const present = list.filter((v) => v !== null && v !== undefined
    && v !== "" && !(typeof v === "number" && Number.isNaN(v)));
  const dates = present.filter((v) => typeof v === "string"
    && DATE_VALUE.test(v.trim())).length;

  const [nameKind, nameUnit] = kindFromName(name);
  let kind = explicit.kind || unitKind || nameKind;
  if (!unit && nameUnit) unit = nameUnit;
  if (!kind) {
    if (present.length && dates / present.length >= 0.8) kind = "date";
    else if (!nums.length) kind = "text";
    else if (nums.length < present.length * 0.8) kind = "text";
    else kind = "number";
  }
  let plain = false;
  if (kind === "date" && !explicit.kind && !dates && present.length) {
    // the name says date but the values do not: "year" as 2024, 2025
    // is a number that never groups; "day" as Mon, Tue is a label
    kind = nums.length ? "number" : "text";
    plain = true;
  } else if (kind === "identifier" && !nums.length && present.length && dates) {
    kind = "date";
  }

  let scale = 1;
  if (kind === "percent") {
    const fraction = nums.length > 0 && nums.every((n) => Math.abs(n) <= 1.0);
    scale = fraction ? 100 : 1;
  }
  if ("scale" in explicit) scale = explicit.scale;

  let decimals;
  const maxOf = (arr) => arr.reduce((a, b) => (b > a ? b : a), 0);
  if ("decimals" in explicit) decimals = explicit.decimals;
  else if (["count", "identifier", "date", "text"].includes(kind)) decimals = 0;
  else if (kind === "percent") {
    decimals = maxOf(nums.map((n) => (scale === 100
      ? Math.max(0, decimalsOf(n) - 2) : decimalsOf(n))));
  } else {
    decimals = maxOf(nums.map(decimalsOf));
    if (kind === "currency") decimals = Math.min(decimals, CURRENCY_DECIMALS);
  }
  decimals = Math.min(MAX_DECIMALS, decimals);

  let grouping = !["identifier", "date", "text"].includes(kind) && !plain;
  if ("grouping" in explicit) grouping = explicit.grouping;
  let compact = ["number", "count", "currency"].includes(kind)
    && median(nums.map(Math.abs)) > COMPACT_ABOVE;
  if ("compact" in explicit) compact = explicit.compact;
  if ("unit" in explicit) unit = explicit.unit;
  else if (kind === "percent") unit = "%";
  return { kind, decimals, grouping, compact, unit, scale,
           sign: explicit.sign || "auto", null: NULL_TEXT };
}

function group(digits) {
  const out = [];
  while (digits.length > 3) {
    out.unshift(digits.slice(-3));
    digits = digits.slice(0, -3);
  }
  out.unshift(digits);
  return out.join(",");
}

function fixed(value, decimals, grouping) {
  // half away from zero on the same float arithmetic as the Python
  // twin, so both sides agree on every case
  const factor = 10 ** decimals;
  const n = Math.floor(Math.abs(value) * factor + 0.5 + 1e-9);
  const whole = Math.floor(n / factor);
  const frac = n - whole * factor;
  let text = grouping ? group(String(whole)) : String(whole);
  if (decimals) text += "." + String(frac).padStart(decimals, "0");
  return text;
}

function compactText(value) {
  const mag = Math.abs(value);
  for (const [unit, label] of [[1e12, "T"], [1e9, "B"], [1e6, "M"], [1e3, "K"]]) {
    if (mag >= unit) {
      const scaled = mag / unit;
      let text = fixed(scaled, scaled >= 100 ? 0 : 1, false);
      if (text.endsWith(".0")) text = text.slice(0, -2);
      return text + label;
    }
  }
  return fixed(mag, 0, true);
}

/** One value through a column's format (twin of format_value). */
export function formatValue(value, fmt) {
  fmt = fmt || numberFormat([value]);
  const nullText = String(fmt.null ?? NULL_TEXT);
  if (value === null || value === undefined || value === ""
      || (typeof value === "number" && Number.isNaN(value))) {
    return nullText;
  }
  const kind = fmt.kind || "number";
  if (kind === "date" || kind === "text") return String(value);
  let num = asNumber(value);
  if (num === null) return String(value);
  if (kind === "identifier") {
    return typeof value === "string" ? value
      : (Number.isInteger(num) ? String(num) : String(num));
  }
  num = num * (fmt.scale || 1);
  let negative = num < 0;
  let body;
  if (fmt.compact && Math.abs(num) >= 1000) body = compactText(num);
  else {
    body = fixed(num, Number(fmt.decimals || 0), fmt.grouping !== false);
    if (body.replace(/[0.,]/g, "") === "") negative = false;
  }
  const unit = String(fmt.unit || "");
  if (kind === "percent") body += "%";
  else if (kind === "currency") {
    const symbol = CURRENCY_SYMBOLS[unit.length === 3 ? unit.toUpperCase() : unit];
    if (symbol) body = symbol + body;
    else if (unit) body += " " + unit;
  } else if (unit && kind !== "count") {
    body += (unit.length > 1 || kind === "duration" ? " " : "") + unit;
  }
  const sign = negative ? "-" : (fmt.sign === "always" && num > 0 ? "+" : "");
  return sign + body;
}

// ═══ the renderer ═══════════════════════════════════════════════

const isDate = (v) => typeof v === "string" && DATE_VALUE.test(v);
const isNum = (v) => asNumber(v) !== null;
const textW = (s) => String(s).length * 6.2;     // 10.5px, roughly
const NUMERIC_KINDS = ["percent", "currency", "count", "ratio", "duration",
  "number"];

/** Round ticks over [lo, hi]: a step of 1, 2, 2.5 or 5 × 10^k. */
export function niceTicks(lo, hi, count = 5) {
  if (!Number.isFinite(lo) || !Number.isFinite(hi)) return [0, 1];
  if (hi === lo) hi = lo + (lo === 0 ? 1 : Math.abs(lo) * 0.1);
  const raw = (hi - lo) / (count - 1);
  const mag = 10 ** Math.floor(Math.log10(raw));
  const norm = raw / mag;
  const step = (norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 2.5 ? 2.5
    : norm <= 5 ? 5 : 10) * mag;
  const start = Math.floor(lo / step) * step;
  const end = Math.ceil(hi / step) * step;
  const ticks = [];
  for (let v = start; v <= end + step / 2; v += step) {
    ticks.push(Number(v.toFixed(10)));
  }
  return ticks;
}

export function createArtifactRenderer(hooks) {
  const { esc, prose, statusLabel, renderMarkdown } = hooks;
  // the strip under a number: the surface decides the words
  const meridian = hooks.meridian || ((prov) => (prov && prov.meridian_line
    ? `<span class="meridian" title="${esc(prov.meridian_line)}">${
        prose ? prose(prov.meridian_line) : esc(prov.meridian_line)}</span>`
    : ""));

  function statusChip(prov) {
    if (!prov || !prov.status) return "";
    return `<span class="status-chip s-${esc(prov.status)}">${
      esc(statusLabel(prov.status))}</span>`;
  }

  // ── charts ──────────────────────────────────────────────

  // the union of every series' x labels in order of first appearance
  // (chronological when they are dates), each series placed by label:
  // a forecast over Jul–Dec lands on Jul–Dec; a null, or a label a
  // series lacks, is a gap
  function prepSeries(spec) {
    let cats = [];
    for (const s of spec.series || []) {
      for (const p of s.points || []) {
        const label = String(p[0]);
        if (!cats.includes(label)) cats.push(label);
      }
    }
    const dated = cats.length > 1 && cats.every(isDate);
    if (dated) cats.sort();
    const lookup = (spec.series || []).map((s) => new Map(
      (s.points || []).map((p) => [String(p[0]),
        p[1] === null || p[1] === undefined ? NaN : Number(p[1])])));
    const sort = spec.sort || "";
    if (sort === "-y" || sort === "y") {
      const first = lookup[0] || new Map();
      const key = (c) => (first.has(c) && Number.isFinite(first.get(c))
        ? first.get(c) : -Infinity);
      cats = [...cats].sort((a, b) => (sort === "-y" ? key(b) - key(a)
                                                     : key(a) - key(b)));
    } else if (sort === "-x") {
      cats = [...cats].reverse();
    }
    const series = (spec.series || []).map((s, i) => ({
      name: s.name,
      role: s.role || "",
      // a forecast, a projection or a target reads dashed
      dashed: !!s.dashed || /forecast|projection|projected|estimate|target|\bplan\b/i
        .test(String(s.name || "")),
      values: cats.map((c) => (lookup[i].has(c) ? lookup[i].get(c) : NaN)),
      fmt: numberFormat((s.points || []).map((p) => p[1]),
        { name: (spec.series || []).length === 1 ? (spec.y_label || s.name)
            : s.name, unit: s.unit || spec.unit,
          format: s.format || spec.format }),
    }));
    return { cats, series, dated };
  }

  function yFormat(spec, series) {
    const all = series.flatMap((s) => s.values).filter(Number.isFinite);
    const one = series.length === 1 ? series[0] : null;
    return numberFormat(all, {
      name: spec.y_label || (one ? one.name : ""),
      unit: spec.unit, format: spec.format });
  }

  const tickFormat = (fmt, ticks) => {
    const step = ticks.length > 1 ? Math.abs(ticks[1] - ticks[0]) : 1;
    const big = Math.max(...ticks.map(Math.abs)) * (fmt.scale || 1);
    return { ...fmt, compact: fmt.compact || big >= 1e4,
             decimals: Math.min(fmt.decimals, decimalsOf(step * (fmt.scale || 1))) };
  };

  // the runs of consecutive values a line is drawn through: a gap
  // ends one run and starts the next
  const runs = (values) => {
    const out = [];
    let cur = [];
    values.forEach((v, i) => {
      if (Number.isFinite(v)) cur.push(i);
      else if (cur.length) { out.push(cur); cur = []; }
    });
    if (cur.length) out.push(cur);
    return out;
  };

  function legendSVG(items, x, y) {
    let lx = x;
    let body = "";
    for (const it of items) {
      body += `<text x="${lx}" y="${y}" class="tick legend"><tspan fill="${
        it.color}">${it.glyph || "■"}</tspan> ${esc(it.name)}</text>`;
      lx += textW(it.name) + 22;
    }
    return body;
  }

  function watermarkSVG(spec, width, height) {
    if (!spec.watermark) return "";
    return `<text x="${width / 2}" y="${height / 2}" class="watermark"
      text-anchor="middle" transform="rotate(-18 ${width / 2} ${
      height / 2})">${esc(spec.watermark)}</text>`;
  }

  function svgOpen(spec, width, height, extra = "") {
    const label = spec.reason || spec.y_label || "chart";
    return `<svg viewBox="0 0 ${width} ${height}" class="chartv2 chart-${
      esc(spec.kind || "line")}${extra}" role="img" aria-label="${
      esc(label)}" xmlns="http://www.w3.org/2000/svg">${
      spec.reason ? `<title>${esc(spec.reason)}</title>` : ""}`;
  }

  // a vertical frame: categories along x, values up y — the frame
  // fits its labels (the left margin from the widest tick, the bottom
  // from the category labels, rotated when they would collide, thinned
  // when even that would), the legend above the plot
  function verticalFrame(spec, width, height, cats, ticks, fmt, opts) {
    const { isBar, legend } = opts;
    const tfmt = tickFormat(fmt, ticks);
    const tickText = (v) => formatValue(v, tfmt);
    const padL = Math.ceil(Math.max(...ticks.map((t) => textW(tickText(t)))) + 14);
    const plotW = width - padL - 12;
    const n = cats.length;
    const widest = Math.max(8, ...cats.map(textW));
    const slot = plotW / Math.max(1, n);
    const rotate = isBar && widest > slot - 6 && n <= 30;
    const step = rotate ? Math.max(1, Math.ceil(14 / slot))
      : Math.max(1, Math.ceil((widest + 10) / slot));
    const pad = { l: padL, r: 12, t: legend ? 30 : 16,
                  b: rotate ? Math.min(96, 24 + Math.ceil(widest * 0.7)) : 34 };
    const yMin = ticks[0];
    const yMax = ticks[ticks.length - 1];
    const px = (i) => (isBar
      ? pad.l + ((i + 0.5) * plotW) / Math.max(1, n)
      : pad.l + (n < 2 ? 0 : (i * plotW) / (n - 1)));
    const py = (v) => pad.t + (height - pad.t - pad.b)
      * (1 - (v - yMin) / (yMax - yMin || 1));
    let body = "";
    for (const v of ticks) {
      const y = py(v).toFixed(1);
      body += `<line x1="${pad.l}" y1="${y}" x2="${width - pad.r}"
        y2="${y}" class="grid${v === 0 ? " zero" : ""}"/>
        <text x="${pad.l - 6}" y="${(py(v) + 4).toFixed(1)}" class="tick"
        text-anchor="end">${esc(tickText(v))}</text>`;
    }
    // the category labels: every one when they fit, every k-th when
    // not, the last always on a line chart so the range reads
    cats.forEach((label, i) => {
      const drawn = isBar ? i % step === 0
        : (i === n - 1 || (i % step === 0 && n - 1 - i >= step));
      if (!drawn) return;
      const x = px(i);
      if (rotate) {
        body += `<text x="${x}" y="${height - pad.b + 14}" class="tick"
          text-anchor="end" transform="rotate(-35 ${x} ${height - pad.b + 14})"
          >${esc(label)}</text>`;
      } else {
        const anchor = isBar ? "middle" : (i === 0 ? "start"
          : i === n - 1 ? "end" : "middle");
        body += `<text x="${x}" y="${height - 12}" class="tick"
          text-anchor="${anchor}">${esc(label)}</text>`;
      }
    });
    if (legend && legend.length) body += legendSVG(legend, pad.l, 12);
    if (spec.unit && !(fmt.kind === "percent" || fmt.kind === "currency")) {
      body += `<text x="${width - pad.r}" y="12" class="tick"
        text-anchor="end">${esc(spec.unit)}</text>`;
    }
    // the x axis named, bottom right, when the labels lie flat
    if (spec.x_label && !rotate) {
      body += `<text x="${width - pad.r}" y="${height - 1}" class="tick axis"
        text-anchor="end">${esc(spec.x_label)}</text>`;
    }
    return { pad, plotW, slot, px, py, body, yMin, yMax, n, tfmt };
  }

  function referenceSVG(spec, frame, width, fmt) {
    const ref = spec.reference;
    if (!ref || !Number.isFinite(Number(ref.value))) return "";
    const v = Number(ref.value);
    if (v < frame.yMin || v > frame.yMax) return "";
    const y = frame.py(v);
    const label = `${ref.label ? esc(ref.label) + " " : ""}${
      esc(formatValue(v, fmt))}`;
    return `<line x1="${frame.pad.l}" y1="${y}" x2="${width - frame.pad.r}"
      y2="${y}" class="reference"/><text x="${width - frame.pad.r}"
      y="${y - 4}" class="tick reference-label" text-anchor="end">${
      label}</text>`;
  }

  const tip = (name, cat, v, fmt, many) =>
    `<title>${many ? esc(name) + " · " : ""}${esc(cat)}: ${
      esc(formatValue(v, fmt))}</title>`;

  function categoryChart(spec, width, height) {
    const { cats, series } = prepSeries(spec);
    const kind = spec.kind;
    const all = series.flatMap((s) => s.values).filter(Number.isFinite);
    if (!all.length || !cats.length) return "<svg></svg>";
    const fmt = yFormat(spec, series);
    const stacked = kind === "stacked_bar" || kind === "percent_bar";
    const barKinds = ["bar", "stacked_bar", "percent_bar", "histogram",
      "waterfall", "combo"];
    const isBar = barKinds.includes(kind);
    const many = series.length > 1;
    // the y domain: bars start at zero; a stack sums; a waterfall runs
    let lo = Math.min(0, ...all);
    let hi = Math.max(...all);
    let stackTops = null;
    let running = null;
    if (stacked) {
      stackTops = cats.map((c, i) => {
        let up = 0;
        let down = 0;
        for (const s of series) {
          const v = s.values[i];
          if (!Number.isFinite(v)) continue;
          if (v >= 0) up += v; else down += v;
        }
        return [up, down];
      });
      if (kind === "percent_bar") { lo = 0; hi = 100; } else {
        lo = Math.min(0, ...stackTops.map((t) => t[1]));
        hi = Math.max(0, ...stackTops.map((t) => t[0]));
      }
    }
    if (kind === "waterfall") {
      const totals = new Set((spec.totals || []).map(String));
      running = [];
      let acc = 0;
      series[0].values.forEach((v, i) => {
        const val = Number.isFinite(v) ? v : 0;
        if (totals.has(cats[i])) {
          running.push({ from: 0, to: val, total: true });
          acc = val;
        } else {
          running.push({ from: acc, to: acc + val, total: false });
          acc += val;
        }
      });
      lo = Math.min(0, ...running.flatMap((r) => [r.from, r.to]));
      hi = Math.max(0, ...running.flatMap((r) => [r.from, r.to]));
    }
    const axisFmt = kind === "percent_bar"
      ? { kind: "percent", decimals: 0, grouping: true, compact: false,
          unit: "%", scale: 1, sign: "auto", null: NULL_TEXT }
      : fmt;
    const ticks = niceTicks(lo, hi);
    const legend = many ? series.map((s, si) => ({
      name: s.name, color: PALETTE[si % PALETTE.length],
      glyph: s.dashed || s.role === "line" ? "┄" : "■" })) : [];
    if (kind === "waterfall") {
      legend.push({ name: "up", color: UP }, { name: "down", color: DOWN });
    }
    const frame = verticalFrame(spec, width, height, cats, ticks, axisFmt,
                                { isBar, legend });
    const { px, py, slot } = frame;
    let body = frame.body;
    const zero = py(0);
    const rect = (x, y0, y1, w, color, title, i, extra = "") =>
      `<rect class="chart-bar" x="${x.toFixed(1)}" y="${
        Math.min(y0, y1).toFixed(1)}" width="${w.toFixed(1)}" height="${
        Math.max(0.5, Math.abs(y1 - y0)).toFixed(1)}" fill="${color}"${
        extra} style="animation-delay:${i * 12}ms">${title}</rect>`;

    if (kind === "histogram") {
      const s = series[0];
      const bw = Math.max(2, slot * 0.96);
      s.values.forEach((v, i) => {
        if (!Number.isFinite(v)) return;
        body += rect(px(i) - bw / 2, zero, py(v), bw, PALETTE[0],
                     tip(s.name, cats[i], v, s.fmt, false), i);
      });
    } else if (kind === "waterfall") {
      const bw = Math.max(3, slot * 0.6);
      running.forEach((r, i) => {
        const v = series[0].values[i];
        const color = r.total ? PALETTE[0] : (r.to >= r.from ? UP : DOWN);
        body += rect(px(i) - bw / 2, py(r.from), py(r.to), bw, color,
                     tip(series[0].name, cats[i], r.total ? r.to : v,
                         series[0].fmt, false), i);
        if (i < running.length - 1) {
          body += `<line class="connector" x1="${(px(i) + bw / 2).toFixed(1)}"
            y1="${py(r.to).toFixed(1)}" x2="${(px(i + 1) - bw / 2).toFixed(1)}"
            y2="${py(r.to).toFixed(1)}"/>`;
        }
      });
    } else if (stacked) {
      const bw = Math.max(3, slot * 0.64);
      cats.forEach((c, i) => {
        let up = 0;
        let down = 0;
        const [totUp, totDown] = stackTops[i];
        series.forEach((s, si) => {
          let v = s.values[i];
          if (!Number.isFinite(v) || v === 0) return;
          let shown = v;
          if (kind === "percent_bar") {
            const whole = v >= 0 ? totUp : Math.abs(totDown);
            v = whole ? (Math.abs(v) / whole) * 100 * Math.sign(v) : 0;
          }
          const from = v >= 0 ? up : down;
          const to = from + v;
          if (v >= 0) up = to; else down = to;
          const title = kind === "percent_bar"
            ? `<title>${esc(s.name)} · ${esc(c)}: ${esc(formatValue(v, axisFmt))} (${
                esc(formatValue(shown, s.fmt))})</title>`
            : tip(s.name, c, shown, s.fmt, true);
          // a 2px gap in the surface colour between the segments
          body += rect(px(i) - bw / 2, py(from), py(to), bw,
                       PALETTE[si % PALETTE.length], title, i,
                       ' stroke="var(--surface, #fff)" stroke-width="2"');
        });
      });
    } else if (isBar || kind === "combo") {
      const bars = kind === "combo" ? series.filter((s) => s.role !== "line")
        : series;
      const group = slot * 0.72;
      const bw = Math.max(3, Math.min(24, group / Math.max(1, bars.length) - 2));
      const groupW = bw * bars.length + 2 * (bars.length - 1);
      bars.forEach((s, bi) => {
        const si = series.indexOf(s);
        const color = PALETTE[si % PALETTE.length];
        s.values.forEach((v, i) => {
          if (!Number.isFinite(v)) return;
          const x = px(i) - groupW / 2 + bi * (bw + 2);
          body += rect(x, zero, py(v), bw, color,
                       tip(s.name, cats[i], v, s.fmt, many), i,
                       s.dashed ? ' opacity="0.55"' : "");
        });
      });
    }
    // lines: every series on a line/area chart, the line-role series
    // on a combo
    const lines = kind === "combo" ? series.filter((s) => s.role === "line")
      : (isBar ? [] : series);
    for (const s of lines) {
      const si = series.indexOf(s);
      const color = PALETTE[si % PALETTE.length];
      const dash = s.dashed ? ' stroke-dasharray="6 4"' : "";
      for (const run of runs(s.values)) {
        const path = run.map((i, k) =>
          `${k ? "L" : "M"}${px(i).toFixed(1)},${py(s.values[i]).toFixed(1)}`)
          .join(" ");
        if (kind === "area" && run.length > 1) {
          body += `<path class="fill" d="${path} L${
            px(run[run.length - 1]).toFixed(1)},${py(Math.max(0, frame.yMin)).toFixed(1)} L${
            px(run[0]).toFixed(1)},${py(Math.max(0, frame.yMin)).toFixed(1)} Z" fill="${
            color}"/>`;
        }
        if (kind !== "scatter" && run.length > 1) {
          body += `<path class="line" d="${path}" fill="none"
            stroke="${color}" stroke-width="2" stroke-linejoin="round"
            stroke-linecap="round"${dash}/>`;
        }
      }
      s.values.forEach((v, i) => {
        if (!Number.isFinite(v)) return;
        body += `<circle class="dot" cx="${px(i).toFixed(1)}" cy="${
          py(v).toFixed(1)}" r="3" fill="${color}" stroke="var(--surface, #fff)"
          stroke-width="1.5">${tip(s.name, cats[i], v, s.fmt, many)}</circle>`;
      });
    }
    body += referenceSVG(spec, frame, width, fmt);
    body += watermarkSVG(spec, width, height);
    return svgOpen(spec, width, height) + body + "</svg>";
  }

  // horizontal bars: categories down the left, the value axis along
  // the bottom; long labels get their room, the tail never clips
  function horizontalChart(spec, width, height) {
    const { cats, series } = prepSeries(spec);
    const all = series.flatMap((s) => s.values).filter(Number.isFinite);
    if (!all.length || !cats.length) return "<svg></svg>";
    const fmt = yFormat(spec, series);
    const many = series.length > 1;
    const rowH = many ? 14 * series.length + 10 : 22;
    const padT = many ? 30 : 14;
    const total = Math.max(height, padT + rowH * cats.length + 36);
    const labelW = Math.min(180, Math.max(...cats.map(textW)) + 10);
    const pad = { l: labelW + 8, r: 16, t: padT, b: 30 };
    const ticks = niceTicks(Math.min(0, ...all), Math.max(...all));
    const tfmt = tickFormat(fmt, ticks);
    const xMin = ticks[0];
    const xMax = ticks[ticks.length - 1];
    const plotW = width - pad.l - pad.r;
    const px = (v) => pad.l + plotW * ((v - xMin) / (xMax - xMin || 1));
    const py = (i) => pad.t + i * rowH;
    let body = "";
    for (const v of ticks) {
      const x = px(v);
      body += `<line x1="${x.toFixed(1)}" y1="${pad.t}" x2="${x.toFixed(1)}"
        y2="${total - pad.b}" class="grid${v === 0 ? " zero" : ""}"/>
        <text x="${x.toFixed(1)}" y="${total - pad.b + 14}" class="tick"
        text-anchor="middle">${esc(formatValue(v, tfmt))}</text>`;
    }
    cats.forEach((c, i) => {
      const label = c.length > 28 ? c.slice(0, 27) + "…" : c;
      body += `<text x="${pad.l - 8}" y="${(py(i) + rowH / 2 + 4).toFixed(1)}"
        class="tick" text-anchor="end"><title>${esc(c)}</title>${esc(label)}</text>`;
      const bh = Math.max(3, Math.min(24, (rowH - 6) / series.length - 1));
      series.forEach((s, si) => {
        const v = s.values[i];
        if (!Number.isFinite(v)) return;
        const y = py(i) + 3 + si * (bh + 1);
        body += `<rect class="chart-bar hbar" x="${Math.min(px(0), px(v)).toFixed(1)}"
          y="${y.toFixed(1)}" width="${Math.max(0.5, Math.abs(px(v) - px(0))).toFixed(1)}"
          height="${bh.toFixed(1)}" fill="${PALETTE[si % PALETTE.length]}"
          style="animation-delay:${i * 12}ms">${tip(s.name, c, v, s.fmt, many)}</rect>`;
      });
    });
    if (many) {
      body += legendSVG(series.map((s, si) => ({
        name: s.name, color: PALETTE[si % PALETTE.length] })), pad.l, 12);
    }
    if (spec.reference && Number.isFinite(Number(spec.reference.value))) {
      const v = Number(spec.reference.value);
      if (v >= xMin && v <= xMax) {
        body += `<line class="reference" x1="${px(v).toFixed(1)}" y1="${pad.t}"
          x2="${px(v).toFixed(1)}" y2="${total - pad.b}"/><text class="tick reference-label"
          x="${(px(v) + 4).toFixed(1)}" y="${pad.t + 10}">${
          esc(spec.reference.label || "")} ${esc(formatValue(v, fmt))}</text>`;
      }
    }
    body += watermarkSVG(spec, width, total);
    return svgOpen(spec, width, total) + body + "</svg>";
  }

  // one measure against another, one point per entity
  function scatterChart(spec, width, height) {
    const series = (spec.series || []).map((s) => ({
      name: s.name,
      points: (s.points || []).filter((p) => isNum(p[0]) && isNum(p[1]))
        .map((p) => [Number(p[0]), Number(p[1])]) }));
    const pts = series.flatMap((s) => s.points);
    if (!pts.length) return "<svg></svg>";
    const xFmt = numberFormat(pts.map((p) => p[0]),
      { name: spec.x_label, format: spec.x_format });
    const yFmt = numberFormat(pts.map((p) => p[1]),
      { name: spec.y_label, unit: spec.unit, format: spec.format });
    const xt = niceTicks(Math.min(...pts.map((p) => p[0])),
                         Math.max(...pts.map((p) => p[0])));
    const yt = niceTicks(Math.min(0, ...pts.map((p) => p[1])),
                         Math.max(...pts.map((p) => p[1])));
    const xtf = tickFormat(xFmt, xt);
    const ytf = tickFormat(yFmt, yt);
    const many = series.length > 1;
    const padL = Math.ceil(Math.max(...yt.map((t) => textW(formatValue(t, ytf)))) + 14);
    const pad = { l: padL, r: 12, t: many ? 30 : 16, b: 34 };
    const px = (v) => pad.l + (width - pad.l - pad.r)
      * ((v - xt[0]) / (xt[xt.length - 1] - xt[0] || 1));
    const py = (v) => pad.t + (height - pad.t - pad.b)
      * (1 - (v - yt[0]) / (yt[yt.length - 1] - yt[0] || 1));
    let body = "";
    for (const v of yt) {
      body += `<line x1="${pad.l}" y1="${py(v).toFixed(1)}" x2="${width - pad.r}"
        y2="${py(v).toFixed(1)}" class="grid${v === 0 ? " zero" : ""}"/>
        <text x="${pad.l - 6}" y="${(py(v) + 4).toFixed(1)}" class="tick"
        text-anchor="end">${esc(formatValue(v, ytf))}</text>`;
    }
    for (const v of xt) {
      body += `<text x="${px(v).toFixed(1)}" y="${height - 12}" class="tick"
        text-anchor="middle">${esc(formatValue(v, xtf))}</text>`;
    }
    series.forEach((s, si) => {
      const color = PALETTE[si % PALETTE.length];
      s.points.forEach(([x, y], i) => {
        body += `<circle class="dot" cx="${px(x).toFixed(1)}" cy="${
          py(y).toFixed(1)}" r="4" fill="${color}" fill-opacity="0.8"
          stroke="var(--surface, #fff)" stroke-width="1"
          style="animation-delay:${Math.min(i, 40) * 8}ms"><title>${
          many ? esc(s.name) + " · " : ""}${esc(spec.x_label || "x")}: ${
          esc(formatValue(x, xFmt))}, ${esc(spec.y_label || "y")}: ${
          esc(formatValue(y, yFmt))}</title></circle>`;
      });
    });
    if (many) {
      body += legendSVG(series.map((s, si) => ({
        name: s.name, color: PALETTE[si % PALETTE.length], glyph: "●" })),
        pad.l, 12);
    }
    if (spec.x_label) {
      body += `<text x="${width - pad.r}" y="${height - 2}" class="tick axis"
        text-anchor="end">${esc(spec.x_label)}</text>`;
    }
    if (spec.reference && Number.isFinite(Number(spec.reference.value))) {
      const v = Number(spec.reference.value);
      if (v >= yt[0] && v <= yt[yt.length - 1]) {
        body += `<line class="reference" x1="${pad.l}" y1="${py(v).toFixed(1)}"
          x2="${width - pad.r}" y2="${py(v).toFixed(1)}"/><text class="tick reference-label"
          x="${width - pad.r}" y="${(py(v) - 4).toFixed(1)}" text-anchor="end">${
          esc(spec.reference.label || "")} ${esc(formatValue(v, yFmt))}</text>`;
      }
    }
    body += watermarkSVG(spec, width, height);
    return svgOpen(spec, width, height) + body + "</svg>";
  }

  // a matrix: one hue, light to dark for magnitude, a legend that
  // says what the ends are; an empty cell stays the surface
  function heatmapChart(spec, width, height) {
    const xs = spec.x || [];
    const ys = spec.y || [];
    const rows = spec.values || [];
    const all = rows.flat().filter((v) => isNum(v)).map(Number);
    if (!xs.length || !ys.length || !all.length) return "<svg></svg>";
    const fmt = numberFormat(all, { name: spec.name || spec.y_label,
                                    unit: spec.unit, format: spec.format });
    const lo = Math.min(...all);
    const hi = Math.max(...all);
    const labelW = Math.min(140, Math.max(...ys.map(textW)) + 10);
    const pad = { l: labelW + 6, r: 12, t: 22, b: 40 };
    const cellW = (width - pad.l - pad.r) / xs.length;
    const cellH = Math.max(16, Math.min(34, (height - pad.t - pad.b) / ys.length));
    const total = pad.t + cellH * ys.length + pad.b;
    const shade = (v) => (hi === lo ? 0.7 : 0.08 + 0.87 * ((v - lo) / (hi - lo)));
    let body = "";
    const xStep = Math.max(1, Math.ceil(textW(xs.reduce((a, b) =>
      (String(b).length > String(a).length ? b : a), "")) / cellW));
    xs.forEach((x, i) => {
      if (i % xStep !== 0) return;
      body += `<text x="${(pad.l + (i + 0.5) * cellW).toFixed(1)}" y="${pad.t - 8}"
        class="tick" text-anchor="middle">${esc(String(x))}</text>`;
    });
    ys.forEach((y, j) => {
      body += `<text x="${pad.l - 6}" y="${(pad.t + (j + 0.5) * cellH + 4).toFixed(1)}"
        class="tick" text-anchor="end">${esc(String(y))}</text>`;
      xs.forEach((x, i) => {
        const v = (rows[j] || [])[i];
        const has = isNum(v);
        body += `<rect class="cell${has ? "" : " empty"}" x="${
          (pad.l + i * cellW + 1).toFixed(1)}" y="${(pad.t + j * cellH + 1).toFixed(1)}"
          width="${Math.max(1, cellW - 2).toFixed(1)}" height="${(cellH - 2).toFixed(1)}"
          rx="2"${has ? ` style="fill-opacity:${shade(Number(v)).toFixed(3)}"` : ""}
          ><title>${esc(String(y))} · ${esc(String(x))}: ${
          esc(has ? formatValue(Number(v), fmt) : NULL_TEXT)}</title></rect>`;
        if (has && cellW >= 34 && cellH >= 16) {
          const dark = shade(Number(v)) > 0.55;
          body += `<text class="cell-label${dark ? " on-dark" : ""}" x="${
            (pad.l + (i + 0.5) * cellW).toFixed(1)}" y="${
            (pad.t + (j + 0.5) * cellH + 3.5).toFixed(1)}" text-anchor="middle">${
            esc(formatValue(Number(v), { ...fmt, compact: true }))}</text>`;
        }
      });
    });
    // the scale: five swatches from light to dark, the ends named
    const swatches = 5;
    const sw = 18;
    const lx = width - pad.r - swatches * sw;
    const ly = total - 22;
    for (let k = 0; k < swatches; k++) {
      body += `<rect class="cell" x="${lx + k * sw}" y="${ly}" width="${sw - 1}"
        height="10" style="fill-opacity:${(0.08 + 0.87 * (k / (swatches - 1))).toFixed(3)}"/>`;
    }
    body += `<text class="tick" x="${lx - 6}" y="${ly + 9}" text-anchor="end">${
      esc(formatValue(lo, { ...fmt, compact: true }))}</text>
      <text class="tick" x="${lx + swatches * sw + 4}" y="${ly + 9}">${
      esc(formatValue(hi, { ...fmt, compact: true }))}</text>`;
    body += watermarkSVG(spec, width, total);
    return svgOpen(spec, width, total) + body + "</svg>";
  }

  // small multiples: one small chart per series on shared scales,
  // the facet named above each; lines over dates, bars otherwise
  function facetChart(spec, width, height) {
    const { cats, series, dated } = prepSeries(spec);
    const all = series.flatMap((s) => s.values).filter(Number.isFinite);
    if (!all.length || !cats.length) return "<svg></svg>";
    const fmt = yFormat(spec, series);
    const n = series.length;
    const cols = Math.min(4, Math.max(1, Math.ceil(Math.sqrt(n))));
    const rowsN = Math.ceil(n / cols);
    const facetH = Math.max(90, Math.min(140, height / rowsN));
    const total = rowsN * facetH + 10;
    const ticks = niceTicks(Math.min(0, ...all), Math.max(...all), 3);
    const tfmt = tickFormat(fmt, ticks);
    const padL = Math.ceil(Math.max(...ticks.map((t) => textW(formatValue(t, tfmt)))) + 10);
    const facetW = width / cols;
    let body = "";
    series.forEach((s, si) => {
      const ox = (si % cols) * facetW;
      const oy = Math.floor(si / cols) * facetH;
      const pad = { l: padL, r: 10, t: 18, b: 18 };
      const plotW = facetW - pad.l - pad.r;
      const plotH = facetH - pad.t - pad.b;
      const yMin = ticks[0];
      const yMax = ticks[ticks.length - 1];
      const px = (i) => ox + pad.l + (dated
        ? (cats.length < 2 ? 0 : (i * plotW) / (cats.length - 1))
        : ((i + 0.5) * plotW) / cats.length);
      const py = (v) => oy + pad.t + plotH * (1 - (v - yMin) / (yMax - yMin || 1));
      body += `<text class="tick facet-title" x="${(ox + pad.l).toFixed(1)}"
        y="${oy + 12}">${esc(s.name)}</text>`;
      for (const v of ticks) {
        body += `<line class="grid${v === 0 ? " zero" : ""}" x1="${(ox + pad.l).toFixed(1)}"
          y1="${py(v).toFixed(1)}" x2="${(ox + facetW - pad.r).toFixed(1)}" y2="${
          py(v).toFixed(1)}"/>`;
        if (si % cols === 0) {
          body += `<text class="tick" x="${(ox + pad.l - 4).toFixed(1)}" y="${
            (py(v) + 3).toFixed(1)}" text-anchor="end">${esc(formatValue(v, tfmt))}</text>`;
        }
      }
      const color = PALETTE[0];
      if (dated) {
        for (const run of runs(s.values)) {
          if (run.length < 2) continue;
          body += `<path class="line" fill="none" stroke="${color}" stroke-width="1.6"
            d="${run.map((i, k) => `${k ? "L" : "M"}${px(i).toFixed(1)},${
            py(s.values[i]).toFixed(1)}`).join(" ")}"/>`;
        }
        s.values.forEach((v, i) => {
          if (!Number.isFinite(v)) return;
          body += `<circle class="dot" cx="${px(i).toFixed(1)}" cy="${py(v).toFixed(1)}"
            r="2" fill="${color}">${tip(s.name, cats[i], v, s.fmt, true)}</circle>`;
        });
      } else {
        const bw = Math.max(2, Math.min(18, (plotW / cats.length) * 0.7));
        s.values.forEach((v, i) => {
          if (!Number.isFinite(v)) return;
          body += `<rect class="chart-bar" x="${(px(i) - bw / 2).toFixed(1)}" y="${
            Math.min(py(v), py(0)).toFixed(1)}" width="${bw.toFixed(1)}" height="${
            Math.max(0.5, Math.abs(py(v) - py(0))).toFixed(1)}" fill="${color}">${
            tip(s.name, cats[i], v, s.fmt, true)}</rect>`;
        });
      }
      if (Math.floor(si / cols) === rowsN - 1 || si >= n - cols) {
        const first = cats[0];
        const last = cats[cats.length - 1];
        body += `<text class="tick" x="${px(0).toFixed(1)}" y="${
          (oy + facetH - 4).toFixed(1)}" text-anchor="start">${esc(first)}</text>`;
        if (cats.length > 1) {
          body += `<text class="tick" x="${px(cats.length - 1).toFixed(1)}" y="${
            (oy + facetH - 4).toFixed(1)}" text-anchor="end">${esc(last)}</text>`;
        }
      }
    });
    body += watermarkSVG(spec, width, total);
    return svgOpen(spec, width, total) + body + "</svg>";
  }

  /** Any chart spec the validator passed → SVG markup. */
  function chartSVG(spec, width = 520, height = 280) {
    const kind = spec.kind || "line";
    if (kind === "hbar") return horizontalChart(spec, width, height);
    if (kind === "scatter") return scatterChart(spec, width, height);
    if (kind === "heatmap") return heatmapChart(spec, width, height);
    if (kind === "small_multiples") return facetChart(spec, width, height);
    return categoryChart(spec, width, height);
  }

  // the one sentence the heuristic gave for the kind, under the chart
  // and on hover
  function chartReason(spec) {
    if (!spec.reason) return "";
    return `<div class="chart-reason muted" title="${esc(spec.reason)}">${
      esc(spec.reason)}</div>`;
  }

  // ── tables ──────────────────────────────────────────────

  function columnKinds(cols, rows) {
    return cols.map((c) => {
      const vals = rows.map((r) => r[c.key])
        .filter((v) => v !== null && v !== undefined && v !== "");
      const fmt = numberFormat(vals, { name: c.key, label: c.label,
                                       unit: c.unit, format: c.format });
      const nums = vals.filter(isNum).length;
      const kind = NUMERIC_KINDS.includes(fmt.kind)
        && vals.length && nums / vals.length >= 0.8 ? "num"
        : fmt.kind === "date" ? "date" : "text";
      return { key: c.key, label: c.label, kind, fmt, unit: c.unit || "" };
    });
  }

  function sparkline(values, w = 140, h = 28) {
    const nums = values.map(Number).filter(Number.isFinite);
    if (nums.length < 2) return "";
    const min = Math.min(...nums);
    const span = (Math.max(...nums) - min) || 1;
    const pts = nums.map((v, i) => [
      (i / (nums.length - 1)) * w,
      h - 2 - ((v - min) / span) * (h - 4)]);
    const line = pts.map((pt) => pt.map((n) => n.toFixed(1)).join(","))
      .join(" ");
    return `<svg class="stat-spark" viewBox="0 0 ${w} ${h}"
      preserveAspectRatio="none" aria-hidden="true">
      <polygon class="area" points="0,${h} ${line} ${w},${h}"/>
      <polyline points="${line}"/></svg>`;
  }

  // the summary strip: the rows and their span, then each numeric
  // column's total (its mean for a percent or a ratio, where a total
  // says nothing), range and mean with its shape — every value
  // computed from the artifact's own rows, nothing estimated
  function reportStrip(spec, kinds) {
    const rows = spec.rows || [];
    const cols = spec.columns || [];
    const stats = [];
    const countFmt = { kind: "count", decimals: 0, grouping: true,
                       compact: false, unit: "", scale: 1, sign: "auto",
                       null: NULL_TEXT };
    const first = { label: "rows", value: formatValue(rows.length, countFmt),
      sub: `${cols.length} column${cols.length === 1 ? "" : "s"}` };
    const dateCol = kinds.find((k) => k.kind === "date");
    if (dateCol) {
      const ds = rows.map((r) => String(r[dateCol.key] || ""))
        .filter(Boolean).sort();
      const today = new Date().toISOString().slice(0, 10);
      const future = ds.filter((d) => d.slice(0, 10) > today).length;
      if (ds.length) first.sub += ` · ${ds[0]} → ${ds[ds.length - 1]}`;
      if (future) {
        first.warn = true;
        first.sub += ` · ${future} dated after today`;
      }
    }
    if (spec.watermark) first.sub += ` · ${spec.watermark}`;
    stats.push(first);
    for (const k of kinds.filter((x) => x.kind === "num").slice(0, 4)) {
      const nums = rows.map((r) => asNumber(r[k.key]))
        .filter((n) => n !== null);
      if (!nums.length) continue;
      const total = nums.reduce((a, b) => a + b, 0);
      const mean = total / nums.length;
      const meanful = k.fmt.kind === "percent" || k.fmt.kind === "ratio";
      const head = meanful ? mean : total;
      const headFmt = { ...k.fmt, compact: k.fmt.compact || Math.abs(head) >= 1e5 };
      stats.push({ label: k.label + (meanful ? " (avg)" : ""), value: formatValue(head, headFmt),
        exact: head, fmt: headFmt,
        title: formatValue(head, { ...k.fmt, compact: false }),
        sub: `min ${formatValue(Math.min(...nums), headFmt)} · max ${
          formatValue(Math.max(...nums), headFmt)}${
          meanful ? "" : ` · avg ${formatValue(mean, headFmt)}`}`,
        spark: sparkline(nums) });
    }
    return `<div class="report-strip">${stats.map((st, i) => `
      <div class="stat${st.warn ? " warn" : ""}" style="--i:${i}">
        <div class="stat-label" title="${esc(st.label)}">${
          esc(st.label)}</div>
        <div class="stat-value"${typeof st.exact === "number"
          ? ` data-n="${st.exact}" data-fmt="${esc(JSON.stringify(st.fmt))}" title="${
              esc(st.title)}"` : ""}>${esc(st.value)}</div>
        <div class="stat-sub">${esc(st.sub)}</div>
        ${st.spark || ""}
      </div>`).join("")}</div>`;
  }

  // the rows: a sticky header naming each column's unit, every cell
  // through its column's format, the first `limit` shown and the
  // rest behind Show all — the toggle's state lives on the wrapper
  // (data-state), never in the button's words
  function tableReport(spec, opts = {}) {
    const { summary = true, limit = 50 } = opts;
    const cols = spec.columns || [];
    const rows = spec.rows || [];
    const kinds = columnKinds(cols, rows);
    const strip = summary && rows.length ? reportStrip(spec, kinds) : "";
    const head = kinds.map((k) => {
      const col = cols.find((c) => c.key === k.key) || {};
      const unit = k.kind === "num" && k.fmt.unit
        && !["percent", "currency"].includes(k.fmt.kind) ? k.fmt.unit : "";
      return `<th class="${k.kind}" data-key="${esc(k.key)}">${esc(k.label)}${
        unit ? ` <span class="muted">(${esc(unit)})</span>` : ""}${
        col.status ? ` <span class="muted">(${esc(col.status)})</span>` : ""}</th>`;
    }).join("");
    const cell = (k, v) => (k.kind === "num" && (isNum(v) || v === null
                                                  || v === undefined || v === "")
      ? `<td class="num"${isNum(v) ? ` title="${esc(String(v))}"` : ""}>${
          esc(formatValue(v, k.fmt))}</td>`
      : `<td class="${k.kind}">${esc(String(v ?? ""))}</td>`);
    const body = rows.map((r, i) =>
      `<tr${i >= limit ? " hidden" : ""}>${
        kinds.map((k) => cell(k, r[k.key])).join("")}</tr>`).join("");
    const count = formatValue(rows.length, { kind: "count", decimals: 0,
      grouping: true, compact: false, unit: "", scale: 1 });
    const more = rows.length > limit ? `<div class="table-more">
        <span class="table-count">Showing the first ${limit} of ${count} rows</span>
        <button type="button" class="btn table-all" aria-expanded="false">Show all</button>
      </div>` : "";
    return `${strip}<div class="tablev3" data-limit="${limit}" data-rows="${
      rows.length}" data-state="some"><table class="sortable">
      <thead><tr>${head}</tr></thead>
      <tbody>${body}</tbody></table></div>${more}`;
  }

  function tableHTML(spec) {
    return tableReport(spec, { summary: false, limit: 20 });
  }

  /** Show all / Show fewer: the state on the wrapper decides what the
   * rows, the count line, the button and the scroll box show; a click
   * flips it and the first revealed row scrolls into view. */
  function bindTable(container) {
    for (const btn of container.querySelectorAll(".table-all")) {
      const more = btn.closest(".table-more");
      const wrap = more && more.previousElementSibling;
      if (!wrap || !wrap.classList.contains("tablev3")) continue;
      const count = more.querySelector(".table-count");
      const limit = Number(wrap.dataset.limit) || 0;
      const total = Number(wrap.dataset.rows) || 0;
      const totalText = formatValue(total, { kind: "count", decimals: 0,
        grouping: true, compact: false, unit: "", scale: 1 });
      const apply = () => {
        const all = wrap.dataset.state === "all";
        wrap.querySelectorAll("tbody tr").forEach((tr, i) => {
          tr.hidden = !all && i >= limit;
        });
        wrap.classList.toggle("all", all);
        btn.textContent = all ? "Show fewer" : "Show all";
        btn.setAttribute("aria-expanded", String(all));
        if (count) {
          count.textContent = all ? `All ${totalText} rows`
            : `Showing the first ${limit} of ${totalText} rows`;
        }
      };
      btn.addEventListener("click", () => {
        const all = wrap.dataset.state !== "all";
        wrap.dataset.state = all ? "all" : "some";
        apply();
        const target = all ? wrap.querySelectorAll("tbody tr")[limit] : wrap;
        if (target && typeof target.scrollIntoView === "function") {
          target.scrollIntoView({ block: "nearest" });
        }
      });
      apply();
    }
  }

  // numbers count up to their value once, never past it; reduced
  // motion shows the value at once
  function animateNumbers(container) {
    const reduce = typeof window !== "undefined" && window.matchMedia
      && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    for (const node of container.querySelectorAll("[data-n]")) {
      const target = Number(node.dataset.n);
      const final = node.textContent;
      if (!Number.isFinite(target) || reduce) continue;
      let fmt = null;
      try { fmt = JSON.parse(node.dataset.fmt || "null"); } catch { fmt = null; }
      if (!fmt || typeof fmt !== "object") fmt = numberFormat([target]);
      const start = performance.now();
      const step = (now) => {
        const t = Math.min(1, (now - start) / 700);
        const eased = 1 - Math.pow(1 - t, 3);
        node.textContent = t < 1 ? formatValue(target * eased, fmt) : final;
        if (t < 1) requestAnimationFrame(step);
      };
      requestAnimationFrame(step);
    }
  }

  // ── tiles ───────────────────────────────────────────────

  function kpiTile(spec) {
    const numeric = typeof spec.value === "number"
      && Number.isFinite(spec.value);
    const fmt = numberFormat([spec.value], { name: spec.label,
      unit: spec.unit, format: spec.format });
    // the delta in the value's unit unless the spec formats it apart
    // (a -4.2% change under a dollar figure)
    const deltaFmt = spec.delta_format
      ? { ...numberFormat([spec.delta], { name: "delta", format: spec.delta_format }),
          sign: "always", compact: false }
      : { ...fmt, sign: "always", compact: false };
    const delta = typeof spec.delta === "number"
      ? `<span class="kpi-delta ${spec.delta >= 0 ? "up" : "down"}" title="${
          esc(spec.compare_label || "change")}">${
          spec.delta >= 0 ? "▲" : "▼"} ${
          esc(formatValue(Math.abs(spec.delta), deltaFmt).replace(/^\+/, ""))}${
          spec.compare_label ? ` <span class="muted">${
            esc(spec.compare_label)}</span>` : ""}</span>` : "";
    const shown = numeric ? formatValue(spec.value, fmt)
      : String(spec.value ?? NULL_TEXT);
    const unitShown = spec.unit && numeric && fmt.kind === "count";
    return `<div class="kpi-tile">
      ${spec.label ? `<div class="kpi-label">${esc(spec.label)}</div>`
                   : ""}
      <div class="kpi-value"${numeric
        ? ` data-n="${spec.value}" data-fmt="${esc(JSON.stringify(fmt))}" title="${
            esc(formatValue(spec.value, { ...fmt, compact: false }))}"` : ""}>${
        esc(shown)}${
        unitShown ? `<span class="kpi-unit">${esc(spec.unit)}</span>` : ""}${
        delta}</div>
    </div>`;
  }

  function tileFooter(spec) {
    const prov = spec.provenance;
    return `<div class="tile-footer">
      ${statusChip(prov)}
      ${spec.watermark ? `<span class="status-chip s-exploratory">${
        esc(spec.watermark)}</span>` : ""}
      ${meridian(prov)}
    </div>`;
  }

  // ── diagrams ────────────────────────────────────────────

  const TIER_STROKE = { certified: ["#2e7d32", ""],
                        witnessed: ["#8a6d1a", "6 3"] };

  function diagramSVG(spec, width = 640) {
    const nodes = spec.nodes || [];
    const lanes = { metric: [], concept: [], table: [] };
    for (const n of nodes) {
      (lanes[n.kind] || lanes.concept).push(n);
    }
    const cols = [lanes.metric, lanes.concept, lanes.table]
      .filter((lane) => lane.length);
    const rowH = 54;
    const height = Math.max(...cols.map((c) => c.length), 1)
      * rowH + 30;
    const pos = new Map();
    cols.forEach((lane, ci) => {
      const x = 90 + ci * ((width - 180) / Math.max(cols.length - 1,
                                                    1));
      lane.forEach((n, ri) => {
        const y = 30 + ri * rowH
          + (height - 40 - lane.length * rowH) / 2;
        pos.set(n.id, [cols.length === 1 ? width / 2 : x, y]);
      });
    });
    let body = "";
    for (const e of spec.edges || []) {
      const a = pos.get(e.a); const b = pos.get(e.b);
      if (!a || !b) continue;
      const [stroke, dash] = TIER_STROKE[e.tier]
        || ["#9a938a", "2 4"];
      body += `<line x1="${a[0]}" y1="${a[1]}" x2="${b[0]}"
        y2="${b[1]}" stroke="${stroke}" stroke-width="1.6"
        ${dash ? `stroke-dasharray="${dash}"` : ""}>
        <title>${esc(e.rel || "")}${e.tier ? ` (${esc(e.tier)})`
                                           : ""}</title></line>`;
    }
    const DOT = { certified: "#2e7d32", pending: "#b07d1e" };
    for (const n of nodes) {
      const p = pos.get(n.id);
      if (!p) continue;
      const label = n.label.length > 26
        ? n.label.slice(0, 25) + "…" : n.label;
      body += `<g class="dg-node dg-${esc(n.kind || "other")}">
        <rect x="${p[0] - 78}" y="${p[1] - 15}" width="156"
          height="30" rx="8"/>
        ${n.status ? `<circle cx="${p[0] - 66}" cy="${p[1]}" r="4"
          fill="${DOT[n.status] || "#9a938a"}"><title>${
          esc(statusLabel(n.status))}</title></circle>` : ""}
        <text x="${p[0] + (n.status ? 6 : 0)}" y="${p[1] + 4}"
          text-anchor="middle"><title>${esc(n.id)}</title>${
          esc(label)}</text></g>`;
    }
    return `<svg viewBox="0 0 ${width} ${height}" class="diagramv2"
      xmlns="http://www.w3.org/2000/svg">${body}</svg>`;
  }

  // ── the artifact body ───────────────────────────────────

  function panelBody(type, spec) {
    const mark = spec.watermark
      ? `<div class="watermark-band">${esc(spec.watermark)}</div>`
      : "";
    if (type === "chart") return mark + chartSVG(spec, 460, 230) + chartReason(spec);
    if (type === "table") return mark + tableHTML(spec);
    if (type === "kpi") return mark + kpiTile(spec);
    if (type === "document") {
      return mark + `<div class="md docview">${
        renderMarkdown(spec.markdown || "", "md")}</div>`;
    }
    return "";
  }

  // a dashboard's panels on a grid: the spec's grid (1 to 3 columns)
  // when it names one, else as many as fit; a chart or a table takes
  // two columns unless the panel says otherwise
  function dashboardHTML(spec) {
    const filters = (spec.filters || []).map((f) => `
      <span class="dash-filter" data-slot="${esc(f.slot)}">
        <span class="muted">${esc(f.label || f.slot)}:</span>
        ${f.options.map((o) => `<button type="button" class="filter-opt${
          o === f.active ? " active" : ""}" data-slot="${
          esc(f.slot)}" data-value="${esc(o)}">${esc(o)}</button>`)
          .join("")}
      </span>`).join("");
    const grid = Number(spec.grid) || 0;
    const panels = (spec.panels || []).map((p, i) => {
      const wide = p.type === "chart" || p.type === "table";
      const span = Math.max(1, Math.min(grid || 3,
        Number(p.span) || (wide ? 2 : 1)));
      return `
      <div class="dash-panel dash-${esc(p.type)}" style="--i:${i};--span:${span}">
        ${p.title ? `<div class="dash-panel-title">${
          esc(p.title)}</div>` : ""}
        ${panelBody(p.type, p.spec || {})}
        ${tileFooter(p.spec || {})}
      </div>`;
    }).join("");
    return `${filters ? `<div class="dash-filters">${filters}
      </div>` : ""}
      <div class="dash-grid${grid ? " fixed" : ""}"${
        grid ? ` style="--cols:${grid}"` : ""}>${panels}</div>
      ${spec.notes ? `<div class="dash-notes md">${
        renderMarkdown(spec.notes, "md")}</div>` : ""}`;
  }

  function renderArtifactBody(row) {
    const spec = row.spec || {};
    const prov = spec.provenance;
    const footer = `
      <div class="artifact-footer">
        ${statusChip(prov)}
        ${spec.watermark && !prov
          ? `<span class="status-chip s-exploratory">${
              esc(spec.watermark)}</span>` : ""}
        ${meridian(prov)}
        <span class="muted">build ${esc(spec.build_id || "?")}
          · v${row.version}</span>
      </div>`;
    const mark = spec.watermark
      ? `<div class="watermark-band">${esc(spec.watermark)}</div>`
      : "";
    if (row.type === "chart") {
      return chartSVG(spec) + chartReason(spec) + footer;
    }
    if (row.type === "table") {
      return mark + tableReport(spec, { summary: true, limit: 50 })
        + footer;
    }
    if (row.type === "document") {
      return mark + `<div class="md docview">${
        renderMarkdown(spec.markdown || "", "md")}</div>` + footer;
    }
    if (row.type === "kpi") {
      return kpiTile(spec) + footer;
    }
    if (row.type === "dashboard") {
      return dashboardHTML(spec) + footer;
    }
    if (row.type === "diagram") {
      if (spec.kind === "mermaid") {
        return `<pre class="mermaid-src">${esc(spec.source || "")
          }</pre><div class="muted" style="font-size:12px">mermaid
          source — export .mmd to render elsewhere</div>` + footer;
      }
      return diagramSVG(spec) + footer;
    }
    return `<pre>${esc(JSON.stringify(spec, null, 1))}</pre>`;
  }

  return { statusChip, chartSVG, chartReason, columnKinds, sparkline,
           reportStrip, tableReport, tableHTML, bindTable, animateNumbers,
           kpiTile, tileFooter, diagramSVG, panelBody, dashboardHTML,
           renderArtifactBody, numberFormat, formatValue };
}
