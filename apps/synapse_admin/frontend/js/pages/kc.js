/** KC Enrichment: the graph's knowledge about one table, shaped for
 * the Knowledge Catalog a person maintains.
 *   #/kc              the catalog-enabled tables with coverage, gate, cache, last push
 *   #/kc/dictionary   the two-way Meridian ↔ catalog terminology map
 *   #/kc/<physical>   the translation ledger, the bundle cards in entry
 *                     order, the suggestions, and the right rail
 * Deterministic cards render at once; the model-written cards stream
 * from the server (which holds the model contract; this page holds no
 * key and knows no endpoint). Every value comes from the bundle. */

import { api } from "../api.js";
import { copyText } from "../pullout.js";
import { card, esc, feedbackBar, loading, prose, unavailable } from "../ui.js";

const GATE_CLASS = { copy: "copy", review: "review", halt: "halt" };
const gateBadge = (tier) => {
  const t = tier || "not run";
  const cls = GATE_CLASS[t] ?? "notrun";
  const word = t === "copy" ? "gate: copy-ready" : t === "review"
    ? "gate: review first" : t === "halt" ? "gate: halted" : "gate: not run";
  return `<span class="kc-gate ${cls}" title="blind gate tier for this build and prompt version">${word}</span>`;
};
const pct = (n) => `${Math.round(n)}%`;
const chip = (text, cls = "") => `<span class="chip ${cls}">${esc(text)}</span>`;

/* ── #/kc: the tables ─────────────────────────────────────── */
export async function renderKc(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Govern · KC Enrichment</span>
      <span class="spacer"></span>
      <a class="linklike" href="#/kc/dictionary">terminology map →</a>
    </div>
    <div id="kc-body">${loading()}</div>`;
  const [tables, coverage] = await Promise.all([api.kcTables(), api.kcCoverage()]);
  const body = outlet.querySelector("#kc-body");
  if (!body) return;
  if (!tables.available) { body.innerHTML = unavailable(tables.reason); return; }

  const cov = coverage.available ? coverage.counts : null;
  const covCard = cov ? card("COVERAGE: every graph item rowed against its catalog home", `
    <div class="proof-counts">
      rows <b class="mono">${cov.rows}</b> · items in this build <b class="mono">${cov.observed}</b>
      · targeted <b class="mono">${pct(cov.pct_targeted)}</b>
      · suggestion-only <b class="mono">${pct(cov.pct_suggestion_only)}</b>
      · excluded with a reason <b class="mono">${pct(cov.pct_excluded_with_reason)}</b>
      · unrowed <b class="mono">${coverage.missing.length}</b>
    </div>
    ${coverage.missing.length ? `<div class="warn">gate red: ${coverage.missing.slice(0, 6).map(esc).join(", ")}</div>` : ""}
    <div class="legend"><a class="linklike" href="#/kc/dictionary">open the terminology map</a></div>`)
    : card("COVERAGE", `<p class="muted">${prose(coverage.reason)}</p>`, "empty");

  const rows = tables.rows ?? [];
  body.innerHTML = `${covCard}
    ${card(`TABLES: ${rows.length} ${tables.fallback ? "in the build" : "catalog-enabled"}`, `
      ${tables.note ? `<div class="muted">${prose(tables.note)}</div>` : ""}
      <div class="kc-row head"><span>table</span><span>facts</span><span>copy</span>
        <span>review</span><span>never</span><span>state</span></div>
      ${rows.length === 0 ? `<p class="muted">no table in this build</p>` : rows.map((r) => `
      <div class="kc-row">
        <span>
          <a class="linklike mono" href="#/kc/${encodeURIComponent(r.physical)}">${esc(r.physical)}</a>
          ${r.lob ? chip(r.lob) : ""}
          <div class="bar" title="copy ${r.pct_copy}% · review ${r.pct_review}% · never ${r.pct_never}%">
            <i style="width:${r.pct_copy}%"></i></div>
        </span>
        <span class="mono">${r.facts}</span>
        <span class="mono">${pct(r.pct_copy)}</span>
        <span class="mono">${pct(r.pct_review)}</span>
        <span class="mono">${pct(r.pct_never)}</span>
        <span>${gateBadge(r.gate)}<br>
          ${r.cached ? chip("model cached", "tier-gr") : chip("model not run", "tier-gu")}<br>
          ${r.last_push ? `<span class="muted" title="${esc(r.last_push.ts)}">pushed by ${esc(r.last_push.actor)} · ${esc(r.last_push.build)}</span>`
            : `<span class="muted">not pushed</span>`}
        </span>
      </div>`).join("")}`)}
    <div class="legend"><span class="muted">build ${esc(tables.build_id)} · copy = may be entered as is ·
      review = a human decides first · never = stays in the graph</span></div>`;
}

/* ── #/kc/dictionary ───────────────────────────────────────── */
export async function renderKcDictionary(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <a class="linklike" href="#/kc">← KC Enrichment</a>
      <span class="muted">› Terminology map</span>
      <input class="search" id="kc-dict-q" placeholder="filter either side…" />
    </div>
    <div id="kc-dict">${loading()}</div>`;
  const payload = await api.kcCoverage();
  const host = outlet.querySelector("#kc-dict");
  if (!host) return;
  if (!payload.available) { host.innerHTML = unavailable(payload.reason); return; }
  const dict = payload.dictionary;
  const draw = (needle = "") => {
    const q = needle.trim().toLowerCase();
    const fwd = dict.forward.filter((r) => !q || r.meridian.toLowerCase().includes(q)
      || r.kc.toLowerCase().includes(q));
    const rev = dict.reverse.filter((r) => !q || r.kc.toLowerCase().includes(q)
      || r.meridian.some((m) => m.toLowerCase().includes(q)));
    host.innerHTML = `<div class="kc-dict">
      ${card(`MERIDIAN → CATALOG: ${fwd.length} rows`, `
        <table class="kc-ledger"><thead><tr><th>Meridian item</th><th>catalog construct</th>
          <th>as</th><th>statuses</th></tr></thead><tbody>
        ${fwd.map((r) => `<tr><td class="mono">${esc(r.meridian)}</td>
          <td>${esc(r.kc)}${r.reason ? `<div class="muted">${esc(r.reason)}</div>` : ""}</td>
          <td>${esc(r.representation)}</td><td class="muted">${esc(r.status_rule)}</td></tr>`).join("")}
        </tbody></table>`)}
      ${card(`CATALOG → MERIDIAN: ${rev.length} constructs`, `
        <table class="kc-ledger"><thead><tr><th>catalog construct</th><th>Meridian items</th></tr></thead><tbody>
        ${rev.map((r) => `<tr><td>${esc(r.kc)}</td>
          <td class="mono" style="white-space:normal">${r.meridian.map(esc).join(" · ")}</td></tr>`).join("")}
        </tbody></table>`)}
    </div>
    <div class="legend"><span class="muted">rendered from the coverage registry alone (sahs/kc/coverage.py);
      build ${esc(payload.build_id)}</span></div>`;
  };
  outlet.querySelector("#kc-dict-q").addEventListener("input", (e) => draw(e.target.value));
  draw();
}

/* ── #/kc/<table>: the bundle ─────────────────────────────── */
const LLM_SECTIONS = new Set(["description", "overview", "columns", "glossary"]);

export async function renderKcTable(outlet, physical) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <a class="linklike" href="#/kc">← KC Enrichment</a>
      <span class="muted">› bundle</span>
      <select class="search" id="kc-pick" title="another table"></select>
      <span class="spacer"></span>
      <span id="kc-live" class="kc-live"></span>
      <button class="btn" id="kc-regen" title="re-run the model sections only">regenerate</button>
    </div>
    <div id="kc-page">${loading()}</div>`;
  const [bundle, tables] = await Promise.all([api.kcBundle(physical), api.kcTables()]);
  const page = outlet.querySelector("#kc-page");
  if (!page) return;

  const pick = outlet.querySelector("#kc-pick");
  const all = tables.available ? tables.rows.map((r) => r.physical) : [];
  if (all.length) {
    pick.innerHTML = all.map((t) => `<option value="${esc(t)}" ${t === physical ? "selected" : ""}>${esc(t)}</option>`).join("");
    pick.addEventListener("change", () => { location.hash = `#/kc/${encodeURIComponent(pick.value)}`; });
  } else { pick.hidden = true; }

  if (!bundle.available) { page.innerHTML = unavailable(bundle.reason); return; }
  if (!bundle.found) {
    page.innerHTML = card("", `<p>${esc(physical)} is not in the promoted build</p>
      <a class="btn" href="#/kc">← back</a>`, "empty");
    return;
  }

  let state = bundle;
  let source = null;
  const live = outlet.querySelector("#kc-live");
  const setLive = (text, on) => {
    live.innerHTML = text ? `${on ? '<span class="dot"></span>' : ""}${esc(text)}` : "";
  };

  const draw = () => { page.innerHTML = bundleHtml(state, physical); wire(); };

  const stream = (regenerate) => {
    if (source) source.close();
    setLive(regenerate ? "regenerating the model sections…" : "writing the model sections…", true);
    source = new EventSource(api.kcStreamUrl(physical, regenerate ? 1 : 0));
    source.addEventListener("kc_started", (e) => {
      const d = JSON.parse(e.data);
      setLive(`model writing from ${d.facts} facts (prompt ${d.prompt_version})…`, true);
    });
    source.addEventListener("kc_section", (e) => {
      const d = JSON.parse(e.data);
      setLive(`${d.section}: ${d.status}${d.reason ? ` (${d.reason})` : ""}`, d.status === "written");
      const label = page.querySelector(`[data-llm-status="${d.section}"]`);
      if (label) label.textContent = `model: ${d.status}`;
    });
    source.addEventListener("kc_verified", (e) => {
      const d = JSON.parse(e.data);
      setLive(`verified: ${d.dropped} sentence(s) dropped`, true);
    });
    source.addEventListener("error", (e) => {
      if (e.data) { try { setLive(`error: ${JSON.parse(e.data).reason}`, false); } catch { /* transport */ } }
    });
    source.addEventListener("bundle", (e) => {
      const fresh = JSON.parse(e.data);
      source.close(); source = null;
      if (fresh.available && fresh.found) { state = fresh; draw(); }
      const llm = fresh.llm ?? {};
      setLive(llm.generated ? `model done: ${llm.calls} call(s), ${llm.dropped} sentence(s) dropped by the verifier`
        : (llm.reason || "model sections unchanged"), false);
    });
  };

  const wire = () => {
    page.querySelectorAll("[data-copy]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const sec = state.sections[btn.dataset.copy];
        const text = btn.dataset.copyKind === "aspect-types"
          ? JSON.stringify(state.aspect_types, null, 1) : (sec?.text ?? "");
        const ok = await copyText(text);
        btn.textContent = ok ? "copied ✓" : "copy failed";
        setTimeout(() => { btn.textContent = "copy"; }, 1600);
      });
    });
    page.querySelectorAll(".kc-ledger tr.clickable").forEach((tr) => {
      tr.addEventListener("click", () => {
        const next = tr.nextElementSibling;
        if (next && next.classList.contains("kc-expand")) next.hidden = !next.hidden;
      });
    });
    const filter = page.querySelector("#kc-ledger-q");
    if (filter) {
      filter.addEventListener("input", () => {
        const q = filter.value.trim().toLowerCase();
        page.querySelectorAll(".kc-ledger tbody tr.clickable").forEach((tr) => {
          const hit = !q || tr.textContent.toLowerCase().includes(q);
          tr.hidden = !hit;
          const next = tr.nextElementSibling;
          if (next && next.classList.contains("kc-expand")) next.hidden = true;
        });
      });
    }
    const pushBtn = page.querySelector("#kc-push");
    if (pushBtn) {
      pushBtn.addEventListener("click", async () => {
        const sections = [...page.querySelectorAll(".kc-check input:checked")].map((i) => i.value);
        const actor = page.querySelector("#kc-actor").value.trim();
        const note = page.querySelector("#kc-note").value.trim();
        const out = page.querySelector("#kc-push-out");
        if (!sections.length || !actor) {
          out.textContent = "tick at least one section and name yourself: the record is a signature";
          return;
        }
        pushBtn.disabled = true;
        const res = await api.kcPush(physical, { sections, actor, note });
        pushBtn.disabled = false;
        if (res.recorded) {
          out.textContent = `recorded: ${res.message}`;
          const fresh = await api.kcBundle(physical);
          if (fresh.available && fresh.found) { state = fresh; draw(); }
        } else {
          out.textContent = `not recorded: ${res.reason ?? "unknown"}`;
        }
      });
    }
    page.querySelector("#kc-fb")?.replaceWith(feedbackBar("kc_bundle", physical, api.feedback));
  };

  draw();
  outlet.querySelector("#kc-regen").addEventListener("click", () => stream(true));
  const llm = state.llm ?? {};
  if (llm.enabled && !llm.cached && !llm.generated && state.gate?.tier !== "halt"
      && !(llm.reason || "").startsWith("model unavailable")) {
    stream(false);
  } else if (llm.reason) {
    setLive(llm.reason, false);
  } else if (llm.cached) {
    setLive(`model sections cached (${llm.generated_at}); 0 calls this load`, false);
  }
  return () => { if (source) source.close(); };
}

/* ── the bundle page body ─────────────────────────────────── */
function factsList(state, ids) {
  const byId = new Map(state.facts.map((f) => [f.id, f]));
  return `<ul>${ids.map((id) => {
    const f = byId.get(id);
    if (!f) return `<li class="mono">${esc(id)}</li>`;
    return `<li><span class="mono">${esc(id)}</span> ${esc(f.text)}
      <span class="muted">[${esc(f.witness)} · ${esc(f.status)}${f.prov?.evidence ? ` · ${esc(f.prov.evidence)}` : ""}${f.prov?.support ? ` · support ${f.prov.support}` : ""}]</span></li>`;
  }).join("")}</ul>`;
}

function citeLinks(text) {
  // [f12,f40] → clickable ids that open the facts list below the card
  return esc(text).replace(/\[((?:f\d+)(?:,\s*f\d+)*)\]/g, (_m, ids) =>
    `<span class="kc-cite" title="facts used">[${esc(ids)}]</span>`);
}

function overviewHtml(sec) {
  return `<div class="kc-block prose">${sec.text.split("\n").map((line) => {
    if (line.startsWith("## ")) return `<h2>${esc(line.slice(3))}</h2>`;
    if (!line.trim()) return "";
    return `<p>${citeLinks(line.replace(/^- /, ""))}</p>`;
  }).join("")}</div>`;
}

function sectionCard(state, key) {
  const sec = state.sections[key];
  const isLlm = LLM_SECTIONS.has(key);
  const counts = Object.entries(sec.status_counts ?? {});
  const statusChips = counts.map(([s, n]) => chip(`${s} ×${n}`,
    s === "certified" ? "tier-gr" : (s === "pending" || s === "mined" || s === "contested") ? "tier-in" : "")).join("");
  const witnessChips = (sec.witnesses ?? []).map((w) => chip(w)).join("");
  const body = !sec.text
    ? `<p class="muted">${prose(sec.empty_reason || "nothing on record")}</p>`
    : key === "overview" ? overviewHtml(sec)
    : key === "description" ? `<div class="kc-block prose"><p>${citeLinks(sec.text)}</p></div>`
    : `<pre class="kc-block">${esc(sec.text)}</pre>`;
  const paragraphs = (sec.paragraphs ?? []).filter((p) => p.fact_ids?.length);
  return `<div class="card kc-card" id="kc-sec-${esc(key)}">
    <div class="card-head">
      <span class="card-label">${esc(sec.title.toUpperCase())}</span>
      ${sec.review?.length ? chip(`${sec.review.length} need review`, "tier-in") : ""}
      ${isLlm ? (sec.llm ? chip("model-written, verified", "tier-gr")
        : `<span class="chip tier-gu" data-llm-status="${esc(key)}">model: not written</span>`) : ""}
      ${isLlm && sec.gate_tier ? gateBadge(sec.gate_tier) : ""}
      <span class="spacer"></span>
      ${sec.text ? `<button class="btn" data-copy="${esc(key)}">copy</button>` : ""}
    </div>
    <div class="legend">${statusChips}${witnessChips}</div>
    ${body}
    ${sec.facts_used?.length ? `<details class="kc-facts"><summary>facts used (${sec.facts_used.length})</summary>${factsList(state, sec.facts_used)}</details>` : ""}
    ${paragraphs.length > 1 ? `<details class="kc-facts"><summary>facts per paragraph</summary>
      ${paragraphs.map((p) => `<div><b>${esc(p.section ?? "")}</b> <span class="mono muted">[${p.fact_ids.map(esc).join(",")}]</span>${p.llm ? " · model" : ""}</div>`).join("")}</details>` : ""}
    ${sec.review?.length ? `<div class="kc-review"><b>needs review</b><ul>${sec.review.map((r) =>
      `<li><span class="mono">${esc(r.id)}</span> ${esc(r.text)} <span class="muted">— ${esc(r.reason)}</span></li>`).join("")}</ul></div>` : ""}
  </div>`;
}

function ledgerHtml(state) {
  const rows = state.ledger ?? [];
  const total = rows.reduce((a, r) => a + r.count, 0);
  return card(`TRANSLATION LEDGER: ${total} facts → ${rows.length} rows`, `
    <div class="legend"><input class="search" id="kc-ledger-q" placeholder="filter by object, construct, status…" />
      <span class="muted">click a row for its facts</span></div>
    <table class="kc-ledger"><thead><tr>
      <th>Meridian object</th><th>catalog construct</th><th>statuses flowing</th><th>as</th>
      <th class="num">translated</th><th class="num">review</th><th class="num">withheld</th></tr></thead>
    <tbody>${rows.map((r) => `
      <tr class="clickable">
        <td><b>${esc(r.meridian_kind)}</b> <span class="mono muted">×${r.count}</span></td>
        <td>${esc(r.kc_construct)}</td>
        <td>${r.statuses.map((s) => chip(s)).join(" ")}</td>
        <td class="muted">${esc(r.representation)}</td>
        <td class="num">${r.translated}</td>
        <td class="num">${r.review}</td>
        <td class="num">${r.withheld}${Object.keys(r.reasons).length ? `<div class="muted" style="text-align:left;white-space:normal">${Object.entries(r.reasons).map(([k, n]) => `${esc(k)} ×${n}`).join("; ")}</div>` : ""}</td>
      </tr>
      <tr class="kc-expand" hidden><td colspan="7">${factsList(state, r.fact_ids)}
        ${r.coverage_items.length ? `<div class="muted mono">coverage: ${r.coverage_items.map(esc).join(" · ")}</div>` : ""}</td></tr>`).join("")}
    </tbody></table>`);
}

function suggestionsHtml(state) {
  const groups = state.suggestions ?? [];
  if (!groups.length) {
    return card("SUGGESTIONS", `<p class="muted">nothing proposed: every fact on this table has a home or a verdict</p>`, "empty");
  }
  return card(`SUGGESTIONS: ${groups.reduce((a, g) => a + g.items.length, 0)} proposals, shown for approval, never in a copy block`, `
    ${groups.map((g) => `<div class="kc-suggest">
      <b>${esc(g.title)}</b> <span class="muted">×${g.items.length}</span>
      <ul>${g.items.slice(0, 40).map((i) => `<li>${esc(i.text)}
        <div class="why">why not by default: ${esc(i.why_not_default)} · [${esc(i.witness)} · ${esc(i.status)}]${i.evidence?.evidence ? ` · ${esc(i.evidence.evidence)}` : ""} <span class="mono">${esc(i.id)}</span></div></li>`).join("")}
      ${g.items.length > 40 ? `<li class="muted">… ${g.items.length - 40} more in the JSON export</li>` : ""}</ul>
    </div>`).join("")}
    <p class="muted">approving is a human act: a term or metric approved here files a review item through the clerk and moves to a copy block on the next load</p>`);
}

function railHtml(state, physical) {
  const llm = state.llm ?? {};
  const gate = state.gate ?? {};
  const sections = state.section_order.filter((k) => k !== "push" && k !== "review");
  const exportBtn = (fmt, label) =>
    `<a class="btn" href="${api.kcExportUrl(physical, fmt)}" download>${label}</a>`;
  return `<aside class="kc-rail">
    <div class="card">
      <h4>Export</h4>
      <div class="legend">${exportBtn("md", "md")}${exportBtn("json", "json")}${exportBtn("csv", "csv")}${exportBtn("sheet", "glossary sheet")}${exportBtn("zip", "zip")}</div>
      <p>build ${esc(state.build_id)} · digest ${esc(state.digest)} · prompt ${esc(state.prompt_version)}</p>
    </div>
    <div class="card">
      <h4>Model</h4>
      <p>${gateBadge(gate.tier)} ${gate.line ? esc(gate.line) : (gate.reason ? esc(gate.reason) : "")}</p>
      <p>${llm.enabled ? (llm.cached ? "cached: 0 calls this load" : llm.generated ? `${llm.calls} call(s) this load` : "not written yet") : "off for this load"}
        ${llm.model ? ` · ${esc(llm.model)}` : ""}
        ${llm.usage?.prompt_tokens ? ` · ${llm.usage.prompt_tokens} prompt / ${llm.usage.output_tokens ?? 0} output tokens` : ""}
        ${llm.budget?.cost_usd != null ? ` · $${llm.budget.cost_usd}` : llm.budget?.cost_note ? ` · ${esc(llm.budget.cost_note)}` : ""}
        ${llm.invalid_json ? ` · ${llm.invalid_json} malformed answer(s)` : ""}
        ${llm.dropped ? ` · ${llm.dropped} sentence(s) dropped by the verifier` : ""}
        ${llm.stopped ? ` · stopped: ${esc(llm.stopped)}` : ""}</p>
      ${llm.dropped_sentences?.length ? `<details class="kc-facts"><summary>dropped sentences</summary><ul>${llm.dropped_sentences.slice(0, 30).map((d) =>
        `<li><b>${esc(d.section)}</b> ${esc(d.sentence)} <span class="muted">— ${esc(d.reason)}</span></li>`).join("")}</ul></details>` : ""}
    </div>
    <div class="card">
      <h4>How to enter each section into the catalog</h4>
      ${(state.guide ?? []).map((g) => `<p><b>${esc(state.sections[g.section]?.title ?? g.section)}</b> → ${esc(g.construct)}<br>${esc(g.path)}</p>`).join("")}
    </div>
    <div class="card">
      <div class="card-head"><h4>Aspect types to create once</h4><span class="spacer"></span>
        <button class="btn" data-copy="aspect-types" data-copy-kind="aspect-types">copy</button></div>
      <p>${(state.aspect_types ?? []).map((a) => esc(a.aspect_type_id)).join(" · ")}</p>
      <details class="kc-facts"><summary>metadataTemplate JSON</summary>
        <pre class="kc-block">${esc(JSON.stringify(state.aspect_types, null, 1))}</pre></details>
    </div>
    <div class="card">
      <h4>Mark pushed</h4>
      <div class="kc-check">${sections.map((k) => `<label><input type="checkbox" value="${esc(k)}" ${state.sections[k]?.text ? "" : "disabled"}> ${esc(state.sections[k]?.title ?? k)}</label>`).join("")}</div>
      <input class="search" id="kc-actor" placeholder="your name (the record is a signature)" />
      <input class="search" id="kc-note" placeholder="note (optional)" />
      <button class="btn primary" id="kc-push">Mark pushed</button>
      <p id="kc-push-out" class="muted">${state.push_records?.length ? `${state.push_records.length} record(s); last by ${esc(state.push_records.at(-1).actor)} on ${esc(state.push_records.at(-1).ts)}` : "nothing recorded yet"}</p>
    </div>
  </aside>`;
}

function bundleHtml(state, physical) {
  const order = state.section_order ?? Object.keys(state.sections);
  return `<div class="kc-layout">
    <div class="kc-stack">
      <div class="card">
        <div class="profile-head">
          <span class="profile-title mono">${esc(physical)}</span>
          <span class="muted">${state.facts.length} facts · ${state.counts.copy ?? 0} copy · ${state.counts.review ?? 0} review · ${state.counts.never ?? 0} never</span>
          <span class="spacer"></span>${gateBadge(state.gate?.tier)}
        </div>
      </div>
      ${ledgerHtml(state)}
      ${order.map((k) => sectionCard(state, k)).join("")}
      ${suggestionsHtml(state)}
      <div class="legend"><span class="muted">every line carries [witness · status] and its fact ids; nothing here is written from outside the graph</span><span class="spacer"></span><span id="kc-fb"></span></div>
    </div>
    ${railHtml(state, physical)}
  </div>`;
}
