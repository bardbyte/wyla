/** KC Enrichment: the graph's knowledge about every table in the build,
 * shaped for the Knowledge Catalog a person maintains.
 *   #/kc              every table in scope: readiness per section, gate, cache, last push
 *   #/kc/dictionary   the two-way Meridian ↔ catalog terminology map
 *   #/kc/glossary     every term across every table, merged the way the catalog holds them
 *   #/kc/<physical>   the translation ledger, the bundle cards in entry order with
 *                     per-item copy, the suggestions, and the right rail
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
const mini = (text, label = "copy") =>
  `<button class="kc-mini" data-copy-text="${esc(text ?? "")}" title="copy this item">${esc(label)}</button>`;

// the sections a person enters into the catalog, in that order; the
// review and push cards are the bundle's own bookkeeping
const ENTRY_SECTIONS = ["description", "overview", "columns", "glossary",
  "related_entries", "aspects", "dq", "queries", "contacts"];
const SECTION_TITLE = {
  description: "Description", overview: "Overview", columns: "Column descriptions",
  glossary: "Glossary terms", related_entries: "Related entries", aspects: "Aspects",
  dq: "DQ rules", queries: "Queries", contacts: "Contacts", review: "Needs review",
  push: "Push record",
};

/* copy: whole-section buttons carry data-copy=<section>; per-item
 * buttons carry the text itself; both flash the outcome */
async function flashCopy(btn, text) {
  const ok = await copyText(text);
  const was = btn.textContent;
  btn.textContent = ok ? "copied ✓" : "copy failed";
  setTimeout(() => { btn.textContent = was; }, 1400);
}

/* ── #/kc: every table in scope ───────────────────────────── */
export async function renderKc(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Govern · KC Enrichment</span>
      <input class="search" id="kc-q" placeholder="filter tables…" />
      <span class="spacer"></span>
      <a class="linklike" href="#/kc/glossary">glossary across tables →</a>
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
    ${coverage.missing.length ? `<div class="warn">gate red: ${coverage.missing.slice(0, 6).map(esc).join(", ")}
      (run pipeline.py kc-coverage for row stubs)</div>` : ""}`)
    : card("COVERAGE", `<p class="muted">${prose(coverage.reason)}</p>`, "empty");

  const rows = tables.rows ?? [];
  const totals = tables.totals ?? {};
  let lob = "";
  let sort = "name";
  const draw = () => {
    const q = (outlet.querySelector("#kc-q")?.value ?? "").trim().toLowerCase();
    let shown = rows.filter((r) => (!q || r.physical.toLowerCase().includes(q))
      && (!lob || r.lob === lob));
    const key = { name: (r) => r.physical, copy: (r) => -r.pct_copy,
      facts: (r) => -r.facts, review: (r) => -r.review }[sort];
    shown = shown.slice().sort((a, b) => (key(a) > key(b) ? 1 : key(a) < key(b) ? -1 : 0));
    const list = body.querySelector("#kc-rows");
    if (!list) return;
    body.querySelector("#kc-count").textContent = `${shown.length} of ${rows.length} tables`;
    list.innerHTML = shown.length === 0 ? `<p class="muted">no table matches</p>` : shown.map((r) => `
      <div class="kc-row">
        <span>
          <a class="linklike mono" href="#/kc/${encodeURIComponent(r.physical)}">${esc(r.physical)}</a>
          ${r.lob ? chip(r.lob) : ""}
          <span class="muted">${r.columns} cols · ${r.metrics} metrics</span><br>
          <span class="kc-dots" title="what each section yields">${ENTRY_SECTIONS.map((k) => {
            const s = r.sections?.[k];
            const cls = !s ? "empty" : s.ready ? (s.review ? "review" : "ready") : "empty";
            const tip = `${SECTION_TITLE[k]}: ${!s ? "" : s.ready
              ? `ready (${s.facts} facts${s.review ? `, ${s.review} to review` : ""})`
              : `empty: ${s.empty_reason}`}`;
            return `<i class="kc-dot ${cls}" title="${esc(tip)}"></i>`;
          }).join("")}</span>
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
      </div>`).join("");
  };

  body.innerHTML = `${covCard}
    <div class="card">
      <div class="card-head kc-tools">
        <span class="card-label">TABLES IN SCOPE: ${rows.length}</span>
        <span class="muted" id="kc-count"></span>
        <span class="spacer"></span>
        <select class="search" id="kc-sort" title="sort">
          <option value="name">sort: name</option>
          <option value="copy">sort: copy-ready %</option>
          <option value="facts">sort: facts</option>
          <option value="review">sort: needs review</option>
        </select>
        <a class="btn" href="${api.kcExportAllUrl()}" download
           title="one zip: a folder per table plus the merged glossary">export everything (zip)</a>
      </div>
      <div class="muted">${prose(tables.note)}</div>
      <div class="proof-counts">facts <b class="mono">${totals.facts ?? 0}</b> ·
        copy-ready <b class="mono">${totals.copy ?? 0}</b> · need review <b class="mono">${totals.review ?? 0}</b> ·
        stay in the graph <b class="mono">${totals.never ?? 0}</b></div>
      ${(tables.lobs ?? []).length ? `<div class="pills" id="kc-lobs">
        <button class="pill on" data-lob="">all LOBs</button>
        ${tables.lobs.map((l) => `<button class="pill" data-lob="${esc(l)}">${esc(l)}</button>`).join("")}
      </div>` : ""}
      <div class="kc-row head"><span>table · sections it yields</span><span>facts</span><span>copy</span>
        <span>review</span><span>never</span><span>state</span></div>
      <div id="kc-rows"></div>
      <div class="legend"><span class="kc-dots"><i class="kc-dot ready"></i></span> ready to copy
        <span class="kc-dots"><i class="kc-dot review"></i></span> ready, some items wait for review
        <span class="kc-dots"><i class="kc-dot empty"></i></span> nothing on record
        <span class="muted">· order: ${ENTRY_SECTIONS.map((k) => SECTION_TITLE[k]).join(" · ")}</span></div>
    </div>
    <div class="legend"><span class="muted">build ${esc(tables.build_id)} · copy = may be entered as is ·
      review = a human decides first · never = stays in the graph</span></div>`;
  outlet.querySelector("#kc-q").addEventListener("input", draw);
  body.querySelector("#kc-sort").addEventListener("change", (e) => { sort = e.target.value; draw(); });
  body.querySelector("#kc-lobs")?.addEventListener("click", (e) => {
    const l = e.target?.dataset?.lob;
    if (l === undefined) return;
    lob = l;
    body.querySelectorAll("#kc-lobs .pill").forEach((b) => b.classList.toggle("on", b.dataset.lob === l));
    draw();
  });
  draw();
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
          <td class="wrap">${esc(r.kc)}${r.reason ? `<div class="muted">${esc(r.reason)}</div>` : ""}</td>
          <td>${esc(r.representation)}</td><td class="muted wrap">${esc(r.status_rule)}</td></tr>`).join("")}
        </tbody></table>`)}
      ${card(`CATALOG → MERIDIAN: ${rev.length} constructs`, `
        <table class="kc-ledger"><thead><tr><th>catalog construct</th><th>Meridian items</th></tr></thead><tbody>
        ${rev.map((r) => `<tr><td>${esc(r.kc)}</td>
          <td class="mono wrap">${r.meridian.map(esc).join(" · ")}</td></tr>`).join("")}
        </tbody></table>`)}
    </div>
    <div class="legend"><span class="muted">rendered from the coverage registry alone (sahs/kc/coverage.py);
      build ${esc(payload.build_id)}</span></div>`;
  };
  outlet.querySelector("#kc-dict-q").addEventListener("input", (e) => draw(e.target.value));
  draw();
}

/* ── #/kc/glossary: every term, merged ────────────────────── */
export async function renderKcGlossary(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <a class="linklike" href="#/kc">← KC Enrichment</a>
      <span class="muted">› Glossary across tables</span>
      <input class="search" id="kc-g-q" placeholder="filter terms, categories, tables…" />
      <span class="spacer"></span>
      <a class="btn" href="${api.kcGlossaryExportUrl("jsonl")}" download title="glossary import file">glossary jsonl</a>
      <a class="btn" href="${api.kcGlossaryExportUrl("links")}" download title="term ↔ asset links">entry links</a>
      <a class="btn" href="${api.kcGlossaryExportUrl("sheet")}" download title="the sheet columns">sheet csv</a>
    </div>
    <div id="kc-g">${loading()}</div>`;
  const payload = await api.kcGlossary();
  const host = outlet.querySelector("#kc-g");
  if (!host) return;
  if (!payload.available) { host.innerHTML = unavailable(payload.reason); return; }
  const terms = payload.terms ?? [];
  const cats = payload.categories ?? [];
  let category = "";
  const draw = () => {
    const q = (outlet.querySelector("#kc-g-q")?.value ?? "").trim().toLowerCase();
    const shown = terms.filter((t) => (!category || t.category === category)
      && (!q || t.term.toLowerCase().includes(q) || t.category.toLowerCase().includes(q)
        || (t.definition || "").toLowerCase().includes(q)
        || t.tables.some((x) => x.toLowerCase().includes(q))));
    const list = host.querySelector("#kc-g-rows");
    host.querySelector("#kc-g-count").textContent = `${shown.length} of ${terms.length} terms`;
    list.innerHTML = shown.length === 0 ? `<tr><td colspan="7" class="muted">no term matches</td></tr>`
      : shown.map((t) => `<tr>
        <td>${esc(t.category)}</td>
        <td><b>${esc(t.term)}</b><br>${chip(t.status || "", t.status === "certified" ? "tier-gr"
          : (t.status === "pending" || t.status === "mined") ? "tier-in" : "")}</td>
        <td class="wrap">${esc(t.definition_llm || t.definition || "")}
          ${t.definition_llm ? `<div class="muted">model-written; the deterministic definition: ${esc(t.definition)}</div>` : ""}</td>
        <td class="wrap">${t.synonyms.map(esc).join("; ") || "-"}<br>
          <span class="muted">${t.related_terms.map(esc).join("; ")}</span></td>
        <td class="wrap mono">${t.related_entries.map(esc).join("<br>")}</td>
        <td class="wrap"><span class="muted">${t.tables.length} table(s)</span><br>
          ${t.tables.map((x) => `<a class="linklike mono" href="#/kc/${encodeURIComponent(x)}">${esc(x.split(".").pop())}</a>`).join(" ")}
          <div class="muted">${esc(t.witness)}</div></td>
        <td>${mini(t.definition_llm || t.definition, "copy definition")}<br>
          ${mini(t.term, "copy name")}</td>
      </tr>`).join("");
  };
  host.innerHTML = `
    ${card(`GLOSSARY: ${terms.length} terms across ${payload.tables.length} tables, merged as the catalog holds them`, `
      <p class="muted">a catalog glossary is one per project: a term two tables share appears once, with both as
        related entries; a LOB category appears once. ${payload.glossary_path
          ? `target: <span class="mono">${esc(payload.glossary_path)}</span>` : "no glossary path in config/kc.yaml yet"}</p>
      <div class="pills" id="kc-g-cats">
        <button class="pill on" data-cat="">all categories</button>
        ${Object.entries(payload.by_category ?? {}).map(([c, n]) =>
          `<button class="pill" data-cat="${esc(c)}">${esc(c)} <span class="mono">${n}</span></button>`).join("")}
      </div>
      <div class="legend"><span class="muted" id="kc-g-count"></span></div>
      <table class="kc-ledger"><thead><tr><th>category</th><th>term</th><th class="wrap">definition</th>
        <th class="wrap">synonyms · related</th><th>related entries</th><th>from</th><th></th></tr></thead>
        <tbody id="kc-g-rows"></tbody></table>`)}
    ${card(`CATEGORIES TO CREATE: ${cats.length}`, cats.length ? `
      <table class="kc-ledger"><thead><tr><th>category</th><th>parent</th><th>description</th><th>from</th><th></th></tr></thead><tbody>
      ${cats.map((c) => `<tr><td><b>${esc(c.name)}</b></td><td>${esc(c.parent || "-")}</td>
        <td class="wrap">${esc(c.description || "")}</td>
        <td class="muted">${c.tables.length} table(s)</td><td>${mini(c.name, "copy name")}</td></tr>`).join("")}
      </tbody></table>` : `<p class="muted">no category on record</p>`)}
    ${(payload.review ?? []).length ? card(`NEEDS REVIEW BEFORE IT ENTERS THE GLOSSARY: ${payload.review.length}`, `
      <ul class="kc-review">${payload.review.map((r) => `<li><span class="mono">${esc(r.table)} ${esc(r.id)}</span>
        ${esc(r.text)} <span class="muted">— ${esc(r.reason)}</span></li>`).join("")}</ul>`) : ""}
    <div class="legend"><span class="muted">build ${esc(payload.build_id)} · the exports above carry every term on this page</span></div>`;
  outlet.querySelector("#kc-g-q").addEventListener("input", draw);
  host.querySelector("#kc-g-cats").addEventListener("click", (e) => {
    const c = e.target.closest("[data-cat]")?.dataset?.cat;
    if (c === undefined) return;
    category = c;
    host.querySelectorAll("#kc-g-cats .pill").forEach((b) => b.classList.toggle("on", b.dataset.cat === c));
    draw();
  });
  host.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-copy-text]");
    if (btn) flashCopy(btn, btn.dataset.copyText);
  });
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

  // the router owns location.hash, so in-page jumps scroll instead
  const jump = (key) => {
    const target = page.querySelector(`#kc-sec-${key}`) ?? page.querySelector(`#kc-${key}`);
    if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const wire = () => {
    page.addEventListener("click", async (e) => {
      const sectionBtn = e.target.closest("[data-copy]");
      if (sectionBtn) {
        const key = sectionBtn.dataset.copy;
        const text = key === "aspect-types" ? JSON.stringify(state.aspect_types, null, 1)
          : key === "all" ? allCopyReady(state)
          : (state.sections[key]?.text ?? "");
        flashCopy(sectionBtn, text);
        return;
      }
      const itemBtn = e.target.closest("[data-copy-text]");
      if (itemBtn) { flashCopy(itemBtn, itemBtn.dataset.copyText); return; }
      const jumpBtn = e.target.closest("[data-jump]");
      if (jumpBtn) { jump(jumpBtn.dataset.jump); return; }
      const tr = e.target.closest(".kc-ledger tr.clickable");
      if (tr && !e.target.closest("button")) {
        const next = tr.nextElementSibling;
        if (next && next.classList.contains("kc-expand")) next.hidden = !next.hidden;
      }
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

/* every copy-ready section as one document, in entry order */
function allCopyReady(state) {
  return ENTRY_SECTIONS.filter((k) => state.sections[k]?.text)
    .map((k) => `## ${SECTION_TITLE[k]}\n\n${state.sections[k].text}`).join("\n\n");
}

/* ── the bundle page body ─────────────────────────────────── */
function factsList(state, ids) {
  const byId = new Map(state.facts.map((f) => [f.id, f]));
  return `<ul>${ids.map((id) => {
    const f = byId.get(id);
    if (!f) return `<li class="mono">${esc(id)}</li>`;
    return `<li><span class="mono">${esc(id)}</span> ${esc(f.text)}
      <span class="muted">[${esc(f.witness)} · ${esc(f.status)}${f.prov?.evidence ? ` · ${esc(f.prov.evidence)}` : ""}${f.prov?.support ? ` · support ${f.prov.support}` : ""}]</span>
      ${mini(f.text)}</li>`;
  }).join("")}</ul>`;
}

function citeLinks(text) {
  return esc(text).replace(/\[((?:f\d+)(?:,\s*f\d+)*)\]/g, (_m, ids) =>
    `<span class="kc-cite" title="facts used">[${esc(ids)}]</span>`);
}

const disclosure = (r) => `<span class="muted">[${esc(r.witness ?? "")} · ${esc(r.status ?? "")}]</span>
  <span class="mono muted">${esc(r.fact_id ?? (r.fact_ids ?? []).join(","))}</span>`;

/* per-item views: the shape the catalog console takes input in */
function itemsHtml(state, key, sec) {
  const rows = sec.items ?? [];
  if (key === "overview") {
    return `<div class="kc-block prose">${sec.text.split("\n").map((line) => {
      if (line.startsWith("## ")) return `<h2>${esc(line.slice(3))}</h2>`;
      if (!line.trim()) return "";
      return `<p>${citeLinks(line.replace(/^- /, ""))}</p>`;
    }).join("")}</div>`;
  }
  if (key === "description") {
    return `<div class="kc-block prose"><p>${citeLinks(sec.text)} ${mini(sec.text.replace(/\s*\[f[\d,\s]*\]/g, ""), "copy without citations")}</p></div>`;
  }
  if (key === "columns") {
    return `<table class="kc-ledger"><thead><tr><th>column</th><th>type</th><th class="wrap">description to enter</th>
      <th>term</th><th>sensitive</th><th>witness · status</th><th></th></tr></thead><tbody>
      ${rows.map((r) => {
        const text = r.description_llm || r.description || (r.nested_path ? `nested at ${r.nested_path}` : "");
        const full = r.supplementary ? (text ? `${text} | ${r.supplementary}` : r.supplementary) : text;
        return `<tr><td class="mono">${esc(r.column)}</td><td class="mono muted">${esc(r.type || "")}</td>
          <td class="wrap">${esc(full)}${r.description_llm ? `<div class="muted">model-written; deterministic: ${esc(r.description)}</div>` : ""}</td>
          <td class="wrap">${esc(r.term || "-")}</td><td>${r.sensitive ? chip("SENSITIVE", "tier-in") : "-"}</td>
          <td>${disclosure({ witness: (r.witness ?? []).join(","), status: r.status, fact_ids: r.fact_ids })}</td>
          <td>${mini(full)}</td></tr>`;
      }).join("")}</tbody></table>`;
  }
  if (key === "glossary") {
    const cats = rows[0]?.categories ?? [];
    const terms = rows[0]?.terms ?? [];
    return `${cats.length ? `<div class="legend">categories: ${cats.map((c) =>
      `${chip(c.parent ? `${c.parent} › ${c.name}` : c.name)}`).join(" ")}</div>` : ""}
      <table class="kc-ledger"><thead><tr><th>category</th><th>term</th><th class="wrap">definition to enter</th>
        <th class="wrap">synonyms · related terms</th><th>related entries</th><th></th></tr></thead><tbody>
      ${terms.map((t) => `<tr><td>${esc(t.category)}</td><td><b>${esc(t.term)}</b><br>${disclosure(t)}</td>
        <td class="wrap">${esc(t.definition_llm || t.definition)}</td>
        <td class="wrap">${t.synonyms.map(esc).join("; ") || "-"}<br><span class="muted">${t.related_terms.map(esc).join("; ")}</span></td>
        <td class="mono wrap">${t.related_entries.map(esc).join("<br>")}</td>
        <td>${mini(t.definition_llm || t.definition, "copy definition")}<br>${mini(t.term, "copy name")}</td></tr>`).join("")}
      </tbody></table>`;
  }
  if (key === "related_entries") {
    return `<table class="kc-ledger"><thead><tr><th>term</th><th>→ entry (table or table.column)</th><th>witness · status</th><th></th></tr></thead><tbody>
      ${rows.map((r) => `<tr><td>${esc(r.term)}</td><td class="mono">${esc(r.target)}</td>
        <td>${disclosure(r)}</td><td>${mini(`${r.term} → ${r.target}`)}</td></tr>`).join("")}</tbody></table>`;
  }
  if (key === "aspects") {
    return rows.map((a) => `<details class="kc-facts" open><summary><b>${esc(a.aspect)}</b>
        <span class="muted">${Object.keys(a.data ?? {}).length} field(s)</span> ${mini(JSON.stringify({ aspects: { [a.aspect]: { data: a.data } } }, null, 1), "copy this aspect")}</summary>
      <pre class="kc-block compact">${esc(JSON.stringify(a.data, null, 1))}</pre></details>`).join("");
  }
  if (key === "dq") {
    return `<table class="kc-ledger"><thead><tr><th>rule</th><th class="wrap">statement</th><th>witness · status</th><th></th></tr></thead><tbody>
      ${rows.map((r) => `<tr><td>${esc(r.rule)}</td><td class="wrap">${esc(r.text)}</td>
        <td>${disclosure(r)}</td><td>${mini(r.text)}</td></tr>`).join("")}</tbody></table>`;
  }
  if (key === "queries") {
    return `<table class="kc-ledger"><thead><tr><th class="wrap">description</th><th class="wrap">SQL (source: User)</th><th>witness · status</th><th></th></tr></thead><tbody>
      ${rows.map((r) => `<tr><td class="wrap">${esc(r.description)}<br><span class="muted">${esc(r.kind || "")}</span></td>
        <td><span class="kc-sql">${esc(r.sql)}</span></td><td>${disclosure(r)}</td>
        <td>${mini(r.sql, "copy SQL")}<br>${mini(r.description, "copy description")}</td></tr>`).join("")}</tbody></table>`;
  }
  if (key === "contacts") {
    return `<table class="kc-ledger"><thead><tr><th>role</th><th>identity</th><th>for</th><th>witness · status</th><th></th></tr></thead><tbody>
      ${rows.map((r) => `<tr><td>${esc(r.role)}</td><td class="mono">${esc(r.name)}</td><td class="muted">${esc(r.metric || "")}</td>
        <td>${disclosure(r)}</td><td>${mini(r.name)}</td></tr>`).join("")}</tbody></table>`;
  }
  return `<pre class="kc-block">${esc(sec.text)}</pre>`;
}

function sectionCard(state, key) {
  const sec = state.sections[key];
  const isLlm = LLM_SECTIONS.has(key);
  const counts = Object.entries(sec.status_counts ?? {});
  const statusChips = counts.map(([s, n]) => chip(`${s} ×${n}`,
    s === "certified" ? "tier-gr" : (s === "pending" || s === "mined" || s === "contested") ? "tier-in" : "")).join("");
  const witnessChips = (sec.witnesses ?? []).map((w) => chip(w)).join("");
  const structured = !["overview", "description", "review", "push"].includes(key) && sec.items?.length;
  const body = !sec.text
    ? `<p class="muted">${prose(sec.empty_reason || "nothing on record")}</p>`
    : itemsHtml(state, key, sec);
  const paragraphs = (sec.paragraphs ?? []).filter((p) => p.fact_ids?.length);
  return `<div class="card kc-card" id="kc-sec-${esc(key)}">
    <div class="card-head">
      <span class="card-label">${esc(sec.title.toUpperCase())}</span>
      ${sec.review?.length ? chip(`${sec.review.length} need review`, "tier-in") : ""}
      ${isLlm ? (sec.llm ? chip("model-written, verified", "tier-gr")
        : `<span class="chip tier-gu" data-llm-status="${esc(key)}">model: not written</span>`) : ""}
      ${isLlm && sec.gate_tier ? gateBadge(sec.gate_tier) : ""}
      <span class="spacer"></span>
      ${sec.text ? `<button class="btn" data-copy="${esc(key)}" title="copy the whole section as one block">copy section</button>` : ""}
    </div>
    <div class="legend">${statusChips}${witnessChips}</div>
    ${body}
    ${structured && sec.text ? `<details class="kc-facts"><summary>the section as one text block</summary><pre class="kc-block">${esc(sec.text)}</pre></details>` : ""}
    ${sec.facts_used?.length ? `<details class="kc-facts"><summary>facts used (${sec.facts_used.length})</summary>${factsList(state, sec.facts_used)}</details>` : ""}
    ${paragraphs.length > 1 ? `<details class="kc-facts"><summary>facts per paragraph</summary>
      ${paragraphs.map((p) => `<div><b>${esc(p.section ?? "")}</b> <span class="mono muted">[${p.fact_ids.map(esc).join(",")}]</span>${p.llm ? " · model" : ""}</div>`).join("")}</details>` : ""}
    ${sec.review?.length ? `<div class="kc-review"><b>needs review</b><ul>${sec.review.map((r) =>
      `<li><span class="mono">${esc(r.id)}</span> ${esc(r.text)} <span class="muted">— ${esc(r.reason)}</span></li>`).join("")}</ul></div>` : ""}
  </div>`;
}

function subnavHtml(state) {
  const order = state.section_order ?? Object.keys(state.sections);
  return `<nav class="kc-subnav" aria-label="sections">
    <button data-jump="ledger">ledger<b>${(state.ledger ?? []).length}</b></button>
    ${order.map((k) => {
      const sec = state.sections[k];
      const n = k === "review" ? (sec.items ?? []).length : (sec.facts_used ?? []).length;
      return `<button data-jump="sec-${esc(k)}" class="${sec.text ? "" : "empty"}"
        title="${esc(sec.text ? `${n} facts` : sec.empty_reason)}">${esc(SECTION_TITLE[k] ?? k)}<b>${n}</b></button>`;
    }).join("")}
    <button data-jump="suggestions">suggestions<b>${(state.suggestions ?? []).reduce((a, g) => a + g.items.length, 0)}</b></button>
  </nav>`;
}

function ledgerHtml(state) {
  const rows = state.ledger ?? [];
  const total = rows.reduce((a, r) => a + r.count, 0);
  return `<div id="kc-ledger">${card(`TRANSLATION LEDGER: ${total} facts → ${rows.length} rows`, `
    <div class="legend"><input class="search" id="kc-ledger-q" placeholder="filter by object, construct, status…" />
      <span class="muted">click a row for its facts; the construct jumps to its card</span></div>
    <table class="kc-ledger"><thead><tr>
      <th>Meridian object</th><th>catalog construct</th><th>statuses flowing</th><th>as</th>
      <th class="num">translated</th><th class="num">review</th><th class="num">withheld</th></tr></thead>
    <tbody>${rows.map((r) => `
      <tr class="clickable">
        <td><b>${esc(r.meridian_kind)}</b> <span class="mono muted">×${r.count}</span></td>
        <td><button class="linklike" data-jump="sec-${esc(r.section)}" title="jump to ${esc(SECTION_TITLE[r.section] ?? r.section)}">${esc(r.kc_construct)} →</button></td>
        <td>${r.statuses.map((s) => chip(s)).join(" ")}</td>
        <td class="muted">${esc(r.representation)}</td>
        <td class="num">${r.translated}</td>
        <td class="num">${r.review}</td>
        <td class="num">${r.withheld}${Object.keys(r.reasons).length ? `<div class="muted" style="text-align:left;white-space:normal">${Object.entries(r.reasons).map(([k, n]) => `${esc(k)} ×${n}`).join("; ")}</div>` : ""}</td>
      </tr>
      <tr class="kc-expand" hidden><td colspan="7">${factsList(state, r.fact_ids)}
        ${r.coverage_items.length ? `<div class="muted mono">coverage: ${r.coverage_items.map(esc).join(" · ")}</div>` : ""}</td></tr>`).join("")}
    </tbody></table>`)}</div>`;
}

function suggestionsHtml(state) {
  const groups = state.suggestions ?? [];
  const inner = !groups.length
    ? `<p class="muted">nothing proposed: every fact on this table has a home or a verdict</p>`
    : `${groups.map((g) => `<div class="kc-suggest">
      <b>${esc(g.title)}</b> <span class="muted">×${g.items.length}</span>
      <ul>${g.items.slice(0, 40).map((i) => `<li>${esc(i.text)} ${mini(i.text)}
        <div class="why">why not by default: ${esc(i.why_not_default)} · [${esc(i.witness)} · ${esc(i.status)}]${i.evidence?.evidence ? ` · ${esc(i.evidence.evidence)}` : ""} <span class="mono">${esc(i.id)}</span></div></li>`).join("")}
      ${g.items.length > 40 ? `<li class="muted">… ${g.items.length - 40} more in the JSON export</li>` : ""}</ul>
    </div>`).join("")}
    <p class="muted">approving is a human act: a term or metric approved here files a review item through the clerk and moves to a copy block on the next load</p>`;
  return `<div id="kc-suggestions">${card(`SUGGESTIONS: ${groups.reduce((a, g) => a + g.items.length, 0)} proposals, shown for approval, never in a copy block`, inner)}</div>`;
}

function railHtml(state, physical) {
  const llm = state.llm ?? {};
  const gate = state.gate ?? {};
  const sections = state.section_order.filter((k) => k !== "push" && k !== "review");
  const exportBtn = (fmt, label) =>
    `<a class="btn" href="${api.kcExportUrl(physical, fmt)}" download>${label}</a>`;
  return `<aside class="kc-rail">
    <div class="card">
      <h4>Export this table</h4>
      <div class="legend">${exportBtn("md", "md")}${exportBtn("json", "json")}${exportBtn("csv", "csv")}${exportBtn("sheet", "glossary sheet")}${exportBtn("zip", "zip")}</div>
      <p>build ${esc(state.build_id)} · digest ${esc(state.digest)} · prompt ${esc(state.prompt_version)}</p>
      <p><a class="linklike" href="#/kc/glossary">glossary across every table →</a></p>
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
      ${(state.guide ?? []).map((g) => `<p><button class="linklike" data-jump="sec-${esc(g.section)}">${esc(state.sections[g.section]?.title ?? g.section)}</button> → ${esc(g.construct)}<br>${esc(g.path)}</p>`).join("")}
    </div>
    <div class="card">
      <div class="card-head"><h4>Aspect types to create once</h4><span class="spacer"></span>
        <button class="btn" data-copy="aspect-types">copy</button></div>
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
  const ready = ENTRY_SECTIONS.filter((k) => state.sections[k]?.text).length;
  return `<div class="kc-layout">
    <div class="kc-stack">
      <div class="card">
        <div class="profile-head">
          <span class="profile-title mono">${esc(physical)}</span>
          <span class="muted">${state.facts.length} facts · ${state.counts.copy ?? 0} copy · ${state.counts.review ?? 0} review · ${state.counts.never ?? 0} never</span>
          <span class="spacer"></span>${gateBadge(state.gate?.tier)}
          <button class="btn primary" data-copy="all" title="every copy-ready section, in entry order, as one document">copy every copy-ready section (${ready})</button>
        </div>
      </div>
      ${subnavHtml(state)}
      ${ledgerHtml(state)}
      ${order.map((k) => sectionCard(state, k)).join("")}
      ${suggestionsHtml(state)}
      <div class="legend"><span class="muted">every line carries [witness · status] and its fact ids; nothing here is written from outside the graph</span><span class="spacer"></span><span id="kc-fb"></span></div>
    </div>
    ${railHtml(state, physical)}
  </div>`;
}
