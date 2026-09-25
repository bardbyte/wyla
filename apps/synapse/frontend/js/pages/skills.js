/** Skills: the shelf, the way a settings page lists it, with the
 * approval workflow on top. One list of what Radix knows how to do
 * (the doctrine packs: built in, yours, shared) and what it knows
 * about (the knowledge files the graph is built from), each with its
 * status, when it was last written and who wrote it. A file a person
 * brings in — Bring a file, Write a skill, Draft with Synapse — is a
 * submission: it goes to the submitter's manager, shows here as
 * Pending manager approval with Radix's read beside it, and reaches
 * the agent only once the manager approves (Published); a rejection
 * carries the manager's comments and the person updates and
 * resubmits. The notices the PRD names land at the top of the page
 * and on the nav badge. A row opens in the reader on the right: a
 * pack with Use in chat, a submission with its review panel. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { createPullout } from "../pullout.js";
import { esc, loading } from "../ui.js";
import { openAddSkill } from "./addskill.js";

const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
const when = (iso) => {
  const t = Date.parse(iso || "");
  return Number.isFinite(t)
    ? new Date(t).toLocaleDateString(undefined,
        { year: "2-digit", month: "numeric", day: "numeric" }) : "";
};
const whenLong = (iso) => {
  const t = Date.parse(iso || "");
  return Number.isFinite(t) ? new Date(t).toLocaleString() : "";
};
const KIND_LABEL = { skill: "skill", knowledge: "knowledge",
                     reference: "reference" };
export const STATUS = {
  pending: { label: "Pending approval", cls: "s-pending" },
  published: { label: "Published", cls: "s-published" },
  rejected: { label: "Rejected", cls: "s-rejected" },
  withdrawn: { label: "Withdrawn", cls: "s-withdrawn" },
  builtin: { label: "Built in", cls: "s-builtin" },
  shared: { label: "Shared", cls: "s-shared" },
};
const REC = { ready: "Ready for approval", minor: "Needs minor updates",
              significant: "Requires significant revision" };
const CATEGORY = { ambiguous: "Ambiguous statement",
                   inconsistent_terminology: "Inconsistent terminology",
                   contradictory: "Contradictory information",
                   missing_context: "Missing context",
                   duplicate: "Duplicate or repetitive content",
                   outdated: "Potentially outdated reference" };
const statusChip = (key) => {
  const s = STATUS[key] || { label: key, cls: "" };
  return `<span class="sub-status ${s.cls}">${esc(s.label)}</span>`;
};

// the rows of the shelf: the two shelves the server keeps, and the
// submissions on the board folded onto them
function shelfRows(packs, files, submissions) {
  const rows = [];
  const subs = submissions.filter((s) => s.status !== "withdrawn");
  const byPack = new Map(subs.filter((s) => s.kind === "skill")
    .map((s) => [s.name, s]));
  const byFile = new Map(subs.filter((s) => s.kind === "knowledge")
    .map((s) => [`artifacts/${String(s.business_unit).toLowerCase()}_${s.name}.${s.ext || "md"}`, s]));
  const placed = new Set();
  for (const p of packs) {
    const sub = p.mine ? byPack.get(p.name) : null;
    if (sub && sub.status === "published") placed.add(sub.id);
    rows.push({
      id: `pack:${p.name}`, kind: "skill", name: p.title || p.name,
      sub: `/${p.name}${p.description ? ` · ${p.description}` : ""}`,
      updated: p.updated || "", author: p.author || "", pack: p,
      status: p.origin === "built-in" ? "builtin" : p.mine ? "published" : "shared",
      submission: sub && sub.status === "published" ? sub : null,
      mine: !!p.mine, sort: p.mine ? 1 : p.origin === "built-in" ? 2 : 3,
    });
  }
  for (const f of files) {
    if (f.family === "pack") continue;      // listed through the packs
    const sub = byFile.get(f.rel);
    if (sub && sub.status === "published") placed.add(sub.id);
    rows.push({
      id: `file:${f.rel}`,
      kind: f.family === "reference" ? "reference" : "knowledge",
      name: f.title || f.name,
      sub: `${f.rel}${f.staged ? " · staged for the next build" : ""}`,
      updated: f.updated || "", author: f.author || "", file: f,
      status: "published",
      submission: sub && sub.status === "published" ? sub : null,
      mine: false, sort: 4,
    });
  }
  for (const s of subs) {
    if (placed.has(s.id)) continue;
    rows.push({
      id: `sub:${s.id}`, kind: s.kind, name: s.title || s.name,
      sub: `/${s.name} · v${s.version} · ${s.status === "pending"
        ? `awaiting ${s.approver?.name || "the manager"}` : s.status_label}`,
      updated: s.updated_at || "", author: "You", submission: s,
      status: s.status, mine: true, sort: 0,
    });
  }
  return rows.sort((a, b) => a.sort - b.sort
    || String(b.updated).localeCompare(String(a.updated))
    || a.name.localeCompare(b.name));
}

function rowHtml(r) {
  return `
    <tr class="shelf-row" data-id="${esc(r.id)}" tabindex="0">
      <td><span class="shelf-name">${esc(r.name)}</span>
        <span class="shelf-sub" title="${esc(r.sub)}">${esc(r.sub)}</span></td>
      <td class="shelf-kind"><span class="chip">${esc(KIND_LABEL[r.kind])}</span></td>
      <td class="shelf-status">${statusChip(r.status)}</td>
      <td class="shelf-when" title="${esc(r.updated)}">${esc(when(r.updated))}</td>
      <td class="shelf-author">${esc(r.author)}</td>
    </tr>`;
}

// ── the review panel: the submission, Synapse's read, the decision ──
function reviewHtml(s, board) {
  const approver = s.approver || board.approver || {};
  const canDecide = (approver.band ?? 0) >= (board.min_band || 40);
  const r = s.ai_review;
  const read = !r ? `<p class="muted">Radix has not read it yet.</p>` : `
    <div class="review-rec r-${esc(r.recommendation)}">
      <b>${esc(REC[r.recommendation] || r.recommendation)}</b>
      ${r.reason ? `<span>${esc(r.reason)}</span>` : ""}
    </div>
    <dl class="review-summary">
      <dt>Executive summary</dt><dd>${esc(r.summary?.executive || "—")}</dd>
      <dt>Key business topics</dt><dd>${(r.summary?.topics || []).length
        ? (r.summary.topics).map((t) => `<span class="chip">${esc(t)}</span>`).join(" ") : "—"}</dd>
      <dt>Intended purpose</dt><dd>${esc(r.summary?.purpose || s.purpose || "—")}</dd>
      <dt>Suggested audience</dt><dd>${esc(r.summary?.audience || "—")}</dd>
    </dl>
    ${(r.insights || []).length ? `
      <table class="review-insights">
        <thead><tr><th>Needs attention</th><th>Confidence</th><th>Finding</th><th>Where</th></tr></thead>
        <tbody>${r.insights.map((i) => `
          <tr><td>${esc(CATEGORY[i.category] || i.category)}</td>
            <td><span class="conf conf-${esc(i.confidence)}">${esc(i.confidence)}</span></td>
            <td>${esc(i.explanation)}</td><td class="muted">${esc(i.reference || "")}</td></tr>`).join("")}
        </tbody></table>`
      : `<p class="muted">Nothing to point at.</p>`}`;
  const readNote = s.ai_status === "running"
    ? `<span class="think-orb">✳</span> Radix is reading it; these are the checks so far`
    : s.ai_status === "failed"
      ? `the model was not available (${esc(s.ai_reason || "no read")}); these are the checks`
      : r ? `by ${esc(r.by)} · advisory: the manager decides` : "";
  const history = (s.comments || []).length ? `
    <ul class="review-history">${s.comments.map((c) => `
      <li><b>${esc(c.event)}</b> · ${esc(c.by || "")} · ${esc(whenLong(c.at))}${
        c.text ? `<div>${esc(c.text)}</div>` : ""}</li>`).join("")}</ul>`
    : `<p class="muted">No decisions yet.</p>`;
  return `
    <div class="review" data-id="${esc(s.id)}">
      <div class="review-head">
        ${statusChip(s.status)}
        <span class="muted">v${s.version} · submitted by ${esc(s.submitter || "you")} · ${esc(whenLong(s.submitted_at))}</span>
      </div>
      <div class="shelf-actions">
        <a class="btn" href="${esc(api.chatReviewFileUrl(s.id))}" download>Download the file</a>
        ${s.status === "published" && s.kind === "skill"
          ? `<button class="btn primary sk-use" data-name="${esc(s.name)}">Use in chat</button>` : ""}
        ${s.status === "pending" ? `<button class="btn rv-withdraw" data-id="${esc(s.id)}">Withdraw</button>` : ""}
      </div>
      <section class="review-block">
        <h4>Submission</h4>
        <dl class="review-summary">
          <dt>Name</dt><dd><code>${esc(s.name)}</code> · ${esc(s.kind)}${
            s.business_unit ? ` · business unit ${esc(s.business_unit)}` : ""}</dd>
          <dt>Description</dt><dd>${esc(s.description || "—")}</dd>
          <dt>Intended purpose</dt><dd>${esc(s.purpose || "—")}</dd>
          <dt>Approver</dt><dd>${esc(approver.name || "the manager")} · band ${esc(String(approver.band ?? "?"))}${
            approver.self_review ? ` <span class="muted">(${esc(approver.note || "self review")})</span>` : ""}</dd>
        </dl>
      </section>
      <section class="review-block" id="rv-read">
        <h4>Radix's read <span class="muted">${readNote}</span></h4>
        ${read}
      </section>
      ${s.status === "pending" ? `
      <section class="review-block">
        <h4>Manager review <span class="muted">${esc(approver.name || "the manager")} decides</span></h4>
        <textarea class="input rv-comment" id="rv-comment" rows="3"
          placeholder="comments for the submitter (a rejection needs one)"></textarea>
        ${canDecide ? "" : `<p class="muted rv-refuse">The approver must be band ${esc(String(board.min_band || 40))} or above; ${esc(approver.name || "the approver")} is band ${esc(String(approver.band ?? "?"))}.</p>`}
        <div class="shelf-actions">
          <button class="btn primary rv-decide" data-decision="approve" data-id="${esc(s.id)}"${canDecide ? "" : " disabled"}>Approve and publish</button>
          <button class="btn rv-decide" data-decision="reject" data-id="${esc(s.id)}"${canDecide ? "" : " disabled"}>Reject</button>
        </div>
      </section>` : ""}
      ${s.status === "rejected" ? `
      <section class="review-block">
        <h4>Update and resubmit</h4>
        <textarea class="input as-editor rv-text" id="rv-text">${esc(s.text || "")}</textarea>
        <input class="search" id="rv-note" placeholder="what changed (optional)" />
        <div class="shelf-actions">
          <button class="btn primary rv-resubmit" data-id="${esc(s.id)}">Resubmit for approval</button>
        </div>
      </section>` : ""}
      <section class="review-block">
        <h4>History</h4>
        ${history}
      </section>
      <details class="review-file"${s.status === "pending" ? " open" : ""}>
        <summary>The file</summary>
        <div class="md">${renderMarkdown(s.text || "", "md")}</div>
      </details>
    </div>`;
}

export async function renderSkills(outlet) {
  outlet.innerHTML = `
    <div class="library-page skills-page">
      <div class="shelf-toolbar">
        <h1>Skills</h1>
        <span class="muted" id="sk-count"></span>
        <span class="spacer"></span>
        <button class="btn sk-queue" id="sk-queue" hidden aria-pressed="false"
          title="the submissions awaiting a decision"></button>
        <button class="icon-btn" id="sk-search-btn" title="search"
          aria-expanded="false">⌕</button>
        <input class="search" id="sk-search"
          placeholder="search skills and knowledge…" hidden />
        <button class="btn" id="sk-browse"
          title="bring a markdown file of your own: it goes to your manager for approval">Browse</button>
        <button class="btn primary" id="sk-add">Add</button>
      </div>
      <p class="muted shelf-intro">What Radix knows how to do, and what
        it knows about. A <b>skill</b> is doctrine: the moves for a kind
        of ask, loaded by itself when a question calls for it (type
        <code>/</code> in the chat to pin one on a conversation); yours
        load for you alone. A <b>knowledge</b> file is reference the graph
        is built from. Anything you add goes to your manager first: it
        shows here as <b>Pending approval</b> with Radix's read beside
        it, and reaches the agent once it is <b>Published</b>.</p>
      <div class="notices" id="sk-notices" hidden></div>
      <table class="shelf-table">
        <thead><tr><th>Skill</th><th>Kind</th><th>Status</th><th>Last updated</th><th>Author</th></tr></thead>
        <tbody id="sk-rows"><tr><td colspan="5">${loading()}</td></tr></tbody>
      </table>
      <p class="muted" id="sk-note"></p>
    </div>`;
  const pullout = createPullout(outlet);
  const $ = (id) => outlet.querySelector(`#${id}`);
  const tbody = $("sk-rows");
  const note = $("sk-note");
  let rows = [];
  let board = { submissions: [], notices: [], approver: {}, pending: 0, unread: 0 };
  let needle = "";
  let queueOnly = false;
  let openId = null;
  let poll = null;
  const pingBadge = () => window.dispatchEvent(new CustomEvent("synapse:reviews"));

  function paint() {
    const q = needle.trim().toLowerCase();
    const shown = rows.filter((r) => !queueOnly || r.status === "pending")
      .filter((r) => !q || [r.name, r.sub, r.author, r.kind, STATUS[r.status]?.label]
        .some((v) => String(v || "").toLowerCase().includes(q)));
    tbody.innerHTML = shown.length ? shown.map(rowHtml).join("")
      : `<tr><td colspan="5" class="shelf-empty">${q
          ? `nothing matches "${esc(needle)}"`
          : queueOnly ? "nothing awaits a decision"
          : "nothing on the shelf yet: Add one, or Browse for a file"}</td></tr>`;
    const skills = rows.filter((r) => r.kind === "skill").length;
    const knowledge = rows.filter((r) => r.kind === "knowledge").length;
    const reference = rows.filter((r) => r.kind === "reference").length;
    const pending = rows.filter((r) => r.status === "pending").length;
    $("sk-count").textContent = rows.length
      ? `${plural(skills, "skill")} · ${plural(knowledge, "knowledge file")}${
          reference ? ` · ${plural(reference, "reference doc")}` : ""}${
          pending ? ` · ${pending} pending` : ""}` : "";
    const queue = $("sk-queue");
    queue.hidden = !pending && !queueOnly;
    queue.textContent = `Awaiting review · ${pending}`;
    queue.classList.toggle("on", queueOnly);
    queue.setAttribute("aria-pressed", String(queueOnly));
    if (tbody.querySelector(`.shelf-row[data-id="${openId ? CSS.escape(openId) : "-"}"]`)) {
      tbody.querySelector(`.shelf-row[data-id="${CSS.escape(openId)}"]`).classList.add("on");
    }
  }
  function paintNotices() {
    const box = $("sk-notices");
    const unread = (board.notices || []).filter((n) => n.unread);
    box.hidden = unread.length === 0;
    if (box.hidden) return;
    box.innerHTML = `
      <div class="notices-head"><b>${plural(unread.length, "notice")}</b>
        <span class="spacer"></span>
        <button class="btn" id="sk-notices-read">mark all read</button></div>
      <ul>${unread.slice(0, 8).map((n) => `
        <li><button class="notice-link" data-id="${esc(n.id)}">${esc(n.text)}</button>
          <span class="muted">${esc(whenLong(n.at))}</span></li>`).join("")}</ul>`;
    box.querySelector("#sk-notices-read").addEventListener("click", async () => {
      await api.chatReviewsSeen().catch(() => ({}));
      pingBadge();
      await load();
    });
    for (const b of box.querySelectorAll(".notice-link")) {
      b.addEventListener("click", () => {
        const row = rows.find((r) => r.submission && r.submission.id === b.dataset.id);
        if (row) openRow(row.id);
      });
    }
  }
  async function load() {
    const [packs, shelf, got] = await Promise.all([
      api.chatSkills().catch(() => ({})),
      api.artifacts().catch(() => ({})),
      api.chatReviews().catch(() => ({})),
    ]);
    if (!tbody.isConnected) return;
    if (got && got.available) board = got;
    rows = shelfRows(packs.available ? packs.skills || [] : [],
                     shelf.files || [], board.submissions || []);
    if (!rows.length && (shelf.files_reason || packs.reason)) {
      note.textContent = shelf.files_reason || packs.reason || "";
    }
    paint();
    paintNotices();
  }

  // ── the reader: a row opens on the right ──
  function stopPoll() { if (poll) { clearTimeout(poll); poll = null; } }
  async function openSubmission(sid, r) {
    const got = await api.chatReview(sid);
    if (!got.available) {
      pullout.open({ title: "not available", kind: "!", sub: sid,
                     raw: got.reason ?? "could not open it", onClose: clear });
      return;
    }
    const s = got.submission;
    pullout.open({
      title: s.title || s.name, kind: `${s.kind} · ${STATUS[s.status]?.label || s.status}`,
      sub: `/${s.name} · v${s.version}`,
      html: reviewHtml(s, board), raw: s.text || "", onClose: () => { stopPoll(); clear(); } });
    stopPoll();
    if (s.ai_status === "running") {
      // the model's read lands in the background: the panel refreshes
      // itself until it does
      let tries = 0;
      const tick = async () => {
        poll = null;
        if (openId !== r.id || tries++ > 40) return;
        const again = await api.chatReview(sid).catch(() => ({}));
        if (!again.available || openId !== r.id) return;
        if (again.submission.ai_status !== "running") {
          const block = outlet.querySelector("#rv-read");
          if (block) block.outerHTML = reviewHtml(again.submission, board)
            .split('<section class="review-block" id="rv-read">')[1]
            .split("</section>")[0].replace(/^/, '<section class="review-block" id="rv-read">') + "</section>";
          return;
        }
        poll = setTimeout(tick, 3000);
      };
      poll = setTimeout(tick, 3000);
    }
  }
  const clear = () => {
    openId = null;
    tbody.querySelector(".shelf-row.on")?.classList.remove("on");
  };
  async function openRow(id) {
    const r = rows.find((x) => x.id === id);
    if (!r) return;
    for (const tr of tbody.querySelectorAll(".shelf-row.on")) tr.classList.remove("on");
    tbody.querySelector(`.shelf-row[data-id="${CSS.escape(id)}"]`)?.classList.add("on");
    openId = id;
    if (r.submission && (r.status !== "published" || !r.pack)) {
      await openSubmission(r.submission.id, r);
      return;
    }
    if (r.pack) {
      const p = r.pack;
      pullout.open({
        title: p.title || p.name, kind: r.mine ? "your skill" : `skill · ${p.author}`,
        sub: `/${p.name}`,
        html: `
          <div class="shelf-actions">
            <button class="btn primary sk-use" data-name="${esc(p.name)}">Use in chat</button>
            ${r.submission ? `<button class="btn sk-review" data-sid="${esc(r.submission.id)}">Review record</button>` : ""}
            ${r.mine ? `<button class="btn sk-delete" data-name="${esc(p.name)}">Delete</button>` : ""}
          </div>
          <div class="md">${renderMarkdown(p.text || "", "md")}</div>`,
        raw: p.text || "", onClose: clear });
      return;
    }
    const file = await api.artifactFile(r.file.rel);
    if (!file.found) {
      pullout.open({ title: "not available", kind: "!", sub: r.file.rel,
                     raw: file.reason ?? "could not open it", onClose: clear });
      return;
    }
    pullout.open({
      title: r.name, kind: `${KIND_LABEL[r.kind]} · ${r.author || "unsigned"}`,
      sub: r.file.rel,
      html: `${r.submission ? `<div class="shelf-actions"><button class="btn sk-review" data-sid="${esc(r.submission.id)}">Review record</button></div>` : ""}${
        renderMarkdown(file.content, file.kind)}`,
      raw: file.content, onClose: clear });
  }
  tbody.addEventListener("click", (e) => {
    const tr = e.target.closest(".shelf-row");
    if (tr) openRow(tr.dataset.id);
  });
  tbody.addEventListener("keydown", (e) => {
    const tr = e.target.closest(".shelf-row");
    if (tr && (e.key === "Enter" || e.key === " ")) {
      e.preventDefault(); openRow(tr.dataset.id);
    }
  });
  const reopen = async (sid) => {
    await load();
    const row = rows.find((r) => r.submission && r.submission.id === sid);
    if (row) await openRow(row.id); else pullout.close();
    pingBadge();
  };
  // the doors in the reader: the aside sits beside the page in the
  // outlet, so the listener is on the outlet — and comes off with the
  // page, or the next render's clicks would answer twice
  const onClick = async (e) => {
    if (!tbody.isConnected) return;
    const use = e.target.closest(".sk-use");
    if (use) {
      try { sessionStorage.setItem("synapse.prefill", `/${use.dataset.name} `); }
      catch { /* storage refused: the slash menu still works */ }
      location.hash = "#/chat/new";
      return;
    }
    const rec = e.target.closest(".sk-review");
    if (rec) {
      const row = rows.find((r) => r.submission && r.submission.id === rec.dataset.sid);
      if (row) await openSubmission(row.submission.id, row);
      return;
    }
    const del = e.target.closest(".sk-delete");
    if (del) {
      await api.chatDeleteSkill(del.dataset.name).catch(() => ({}));
      pullout.close();                    // the reader was showing it
      await load();
      pingBadge();
      return;
    }
    const decide = e.target.closest(".rv-decide");
    if (decide && !decide.disabled) {
      const comment = outlet.querySelector("#rv-comment")?.value || "";
      decide.disabled = true;
      const got = await api.chatDecideReview(decide.dataset.id, decide.dataset.decision, comment);
      if (!got.available) {
        decide.disabled = false;
        note.textContent = got.reason || "no decision";
        const refuse = outlet.querySelector(".rv-refuse") || document.createElement("p");
        refuse.className = "muted rv-refuse";
        refuse.textContent = got.reason || "no decision";
        decide.closest(".review-block")?.appendChild(refuse);
        return;
      }
      note.textContent = decide.dataset.decision === "approve"
        ? `published: ${got.submission.title} is live for the agent`
        : `rejected with your comments: ${got.submission.submitter} can update and resubmit`;
      await reopen(decide.dataset.id);
      return;
    }
    const resubmit = e.target.closest(".rv-resubmit");
    if (resubmit) {
      const text = outlet.querySelector("#rv-text")?.value || "";
      const comment = outlet.querySelector("#rv-note")?.value || "";
      resubmit.disabled = true;
      const got = await api.chatResubmitReview(resubmit.dataset.id, { text, comment });
      if (!got.available) { resubmit.disabled = false; note.textContent = got.reason || "not resubmitted"; return; }
      note.textContent = `resubmitted as v${got.submission.version}: pending approval again`;
      await reopen(resubmit.dataset.id);
      return;
    }
    const withdraw = e.target.closest(".rv-withdraw");
    if (withdraw) {
      await api.chatWithdrawReview(withdraw.dataset.id).catch(() => ({}));
      pullout.close();
      await load();
      pingBadge();
    }
  };
  outlet.addEventListener("click", onClick);

  // ── search and the queue ──
  $("sk-search-btn").addEventListener("click", () => {
    const box = $("sk-search");
    box.hidden = !box.hidden;
    $("sk-search-btn").setAttribute("aria-expanded", String(!box.hidden));
    if (!box.hidden) box.focus();
    else { needle = ""; box.value = ""; paint(); }
  });
  $("sk-search").addEventListener("input", (e) => { needle = e.target.value; paint(); });
  $("sk-queue").addEventListener("click", () => { queueOnly = !queueOnly; paint(); });

  // ── Browse and Add: one pop-up, three ways in, one door out ──
  const afterSubmit = async (submission) => {
    const to = submission.approver?.name || "your manager";
    note.textContent = submission.resubmitted
      ? `resubmitted as v${submission.version}: pending ${to}'s approval`
      : `submitted to ${to} for approval: Radix is reading it`;
    await load();
    pingBadge();
    const row = rows.find((r) => r.submission && r.submission.id === submission.id);
    if (row) openRow(row.id);
  };
  $("sk-browse").addEventListener("click", () => openAddSkill("upload", afterSubmit, board.approver));
  $("sk-add").addEventListener("click", () => openAddSkill("draft", afterSubmit, board.approver));

  await load();
  return () => {
    outlet.removeEventListener("click", onClick);
    stopPoll();
    pullout.teardown();
  };
}
