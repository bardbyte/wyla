// The node side of test_artifacts_render.py: import a surface's
// js/artifacts-render.js, run the shared number-format cases through
// its twin, render every spec it is handed, and report as JSON.
//   node artifacts_render_check.mjs <module.js> <cases.json> <specs.json>
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const [modPath, casesPath, specsPath] = process.argv.slice(2);
const mod = await import(pathToFileURL(modPath).href);
const cases = JSON.parse(readFileSync(casesPath, "utf8"));
const specs = JSON.parse(readFileSync(specsPath, "utf8"));

const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const r = mod.createArtifactRenderer({
  esc, prose: esc, statusLabel: (s) => s,
  renderMarkdown: (t) => `<p>${esc(t)}</p>`,
  meridian: (prov) => (prov ? `<span class="meridian" title="${
    esc(prov.meridian_line || "")}">Prepared by test.</span>` : ""),
});

const out = { kinds: mod.CHART_KINDS, cases: [], charts: {}, bodies: {} };
for (const c of cases) {
  const fmt = mod.numberFormat(c.values, c.hint);
  out.cases.push({
    name: c.name,
    format: { kind: fmt.kind, decimals: fmt.decimals, scale: fmt.scale,
              compact: fmt.compact, unit: fmt.unit, grouping: fmt.grouping },
    formatted: [...c.values, null].map((v) => mod.formatValue(v, fmt)) });
}
for (const [kind, spec] of Object.entries(specs.charts || {})) {
  const svg = r.chartSVG(spec);
  out.charts[kind] = {
    svg: svg.startsWith("<svg viewBox"),
    clean: !/NaN|undefined/.test(svg),
    tooltips: (svg.match(/<title>/g) || []).length,
    grid: /class="grid/.test(svg),
    legend: /class="tick legend"/.test(svg),
    reference: /class="reference"/.test(svg),
    reason: /<title>[^<]*<\/title>/.test(svg.slice(0, 400)),
    length: svg.length };
}
for (const [type, spec] of Object.entries(specs.bodies || {})) {
  const html = r.renderArtifactBody({ type, spec, version: 1 });
  out.bodies[type] = { html, clean: !/NaN|undefined/.test(html) };
}
process.stdout.write(JSON.stringify(out));
