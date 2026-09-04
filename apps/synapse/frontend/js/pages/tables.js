/** Data Products: every table the build can serve, as a card: what
 * it is, who owns it, how big, how fresh, what it measures, what it
 * joins. Search across names, lines of business and descriptions; a
 * card opens the product profile. Every word on a card is read from
 * the compiled build (the card, the tables index, the joins), never
 * invented here. */

import { api } from "../api.js";
import { card, esc, loading, unavailable } from "../ui.js";

const fmt = (n) => (n === null || n === undefined || n === "")
  ? "?" : new Intl.NumberFormat().format(n);
const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

export function productCard(r) {
  const stats = [
    [fmt(r.rows), "rows"], [r.columns, "columns"],
    [r.metrics_here, "metrics"], [r.joins, "join edges"]];
  const meta = [
    r.business_unit ? `business unit ${esc(r.business_unit)}` : "",
    r.owner ? `owner ${esc(r.owner)}` : "",
    r.layer ? `${esc(r.layer)} layer` : "",
    r.latest_partition ? `data to ${esc(r.latest_partition)}` : "",
    r.lifecycle && r.lifecycle !== "unknown" ? esc(r.lifecycle) : "",
    r.tickets ? `<span class="warn">${plural(r.tickets, "DQ ticket")}</span>`
              : "",
  ].filter(Boolean);
  return `
    <a class="card product-card" href="#/product/${
      encodeURIComponent(r.physical)}" title="${esc(r.physical)}">
      <div class="product-head">
        <span class="product-name">${esc(r.short
          || r.physical.split(".").pop())}</span>
        ${r.lob ? `<span class="chip">${esc(r.lob)}</span>`
                : `<span class="chip muted">unmapped</span>`}
      </div>
      <span class="mono muted product-physical">${esc(r.physical)}</span>
      <p class="product-desc">${esc(r.description
        || "No description on record yet: the archive and the "
           + "stewards have not described this table.")}</p>
      <div class="product-stats">${stats.map(([v, k]) =>
        `<span><b class="mono">${esc(String(v))}</b> ${k}</span>`)
        .join("")}</div>
      ${meta.length ? `<div class="product-meta muted">${
        meta.map((m) => `<span>${m}</span>`).join("")}</div>` : ""}
      ${(r.metric_names || []).length ? `<div class="product-metrics">${
        r.metric_names.map((m) => `<span class="pill">${esc(m)}</span>`)
          .join("")}</div>` : ""}
      ${(r.join_partners || []).length ? `<div class="product-joins muted">
        joins ${r.join_partners.map((p) =>
          `<span class="mono">${esc(p.split(".").pop())}</span>`)
          .join(", ")}</div>` : ""}
    </a>`;
}

export async function renderProducts(outlet) {
  outlet.innerHTML = `
    <div class="library-page">
      <div class="page-head">
        <h1>Data Products</h1>
        <p class="muted">The tables this build can serve, with what the
        archive and the stewards say about each one. Open a card for
        the columns, the joins on record and the metrics computed on
        it.</p>
      </div>
      <div class="library-tools">
        <input class="search" id="p-search"
          placeholder="search name, line of business, description…" />
        <span class="muted" id="p-count"></span>
      </div>
      <div class="card-grid" id="p-grid">${loading()}</div>
    </div>`;

  const payload = await api.tables();
  const grid = outlet.querySelector("#p-grid");
  if (!grid) return;
  if (!payload.available) {
    grid.innerHTML = unavailable(payload.reason);
    return;
  }
  const draw = (needle = "") => {
    const q = needle.trim().toLowerCase();
    const rows = payload.rows.filter((r) => !q
      || [r.physical, r.lob, r.description, r.business_unit, r.owner,
          ...(r.metric_names || [])]
        .some((v) => (v || "").toLowerCase().includes(q)));
    outlet.querySelector("#p-count").textContent =
      `${rows.length} of ${plural(payload.rows.length, "data product")}`;
    grid.innerHTML = rows.length
      ? rows.map(productCard).join("")
      : card("", `<p class="muted">no data product matches
          "${esc(needle)}"</p>`, "empty");
  };
  let debounce = 0;
  outlet.querySelector("#p-search").addEventListener("input", (e) => {
    clearTimeout(debounce);
    debounce = setTimeout(() => draw(e.target.value), 200);
  });
  draw();
}
