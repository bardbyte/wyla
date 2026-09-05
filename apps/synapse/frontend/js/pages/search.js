/** Search chats: its own page. Every conversation on this machine,
 * newest first, and a search box that finds them fuzzily: a typo, a
 * partial word or a different inflection still finds the chat, and
 * the lines that matched come back under it with the words lit.
 * Enter opens the first result. The server does the finding
 * (/api/chat/search); this page only renders. */

import { api } from "../api.js";
import { esc } from "../ui.js";

const when = (iso) => {
  const then = Date.parse(iso);
  if (!Number.isFinite(then)) return "";
  const mins = Math.round((Date.now() - then) / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  if (mins < 60 * 24) return `${Math.round(mins / 60)}h ago`;
  return `${Math.round(mins / 1440)}d ago`;
};

// escape first, then light the matched words (longest first, so a
// word never gets marked inside a longer one twice)
function lit(text, hits) {
  let out = esc(text);
  const words = [...new Set(hits || [])].filter(Boolean)
    .sort((a, b) => b.length - a.length);
  for (const word of words) {
    const safe = esc(word).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    out = out.replace(new RegExp(`(${safe})`, "gi"), "<mark>$1</mark>");
  }
  return out;
}

const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

export async function renderSearch(outlet, initial = "") {
  outlet.innerHTML = `
    <div class="search-page">
      <div class="page-head">
        <h1>Search chats</h1>
        <p class="muted">Every conversation on this machine. The search
        reads titles and messages and forgives a typo; the lines that
        matched show under each chat.</p>
      </div>
      <input class="search big" id="chat-search-q" autocomplete="off"
        placeholder="Search chats… Enter opens the first result"
        value="${esc(initial)}" />
      <div class="muted" id="chat-search-count"></div>
      <div class="search-results" id="chat-search-results">
        <p class="muted">loading…</p>
      </div>
    </div>`;
  const box = outlet.querySelector("#chat-search-q");
  const results = outlet.querySelector("#chat-search-results");
  const count = outlet.querySelector("#chat-search-count");
  let seq = 0;

  function hit(row, query, i) {
    const title = row.title || "New chat";
    const snippets = query
      ? (row.snippets || []).map((s) => `
        <div class="search-snippet">
          <span class="role">${s.role === "user" ? "you" : "synapse"}</span>
          <span>${lit(s.text, s.hits)}</span>
        </div>`).join("")
      : (row.preview ? `
        <div class="search-snippet">
          <span class="role">you</span><span>${esc(row.preview)}</span>
        </div>` : "");
    const more = query && row.matched > (row.snippets || []).length
      ? `<div class="muted small">+${
          plural(row.matched - row.snippets.length, "more matching message")
        }</div>` : "";
    return `
      <a class="search-hit${i === 0 && query ? " top" : ""}"
         href="#/chat/${esc(row.id)}">
        <div class="search-hit-head">
          <span class="search-title">${row.starred ? "★ " : ""}${
            query ? lit(title, row.title_hits) : esc(title)}</span>
          <span class="muted">${esc(when(row.updated_at))} · ${
            plural(row.messages, "message")}${
            row.running ? " · working" : ""}</span>
        </div>
        ${snippets}${more}
      </a>`;
  }

  async function draw() {
    const mine = ++seq;
    const query = box.value.trim();
    const got = await api.chatSearch(query);
    if (mine !== seq || !results.isConnected) return;
    if (!got.available) {
      results.innerHTML = `<p class="chats-note">${
        esc(got.reason || "chats unavailable")}</p>`;
      return;
    }
    const rows = got.sessions || [];
    count.textContent = query
      ? `${plural(rows.length, "chat")} match “${query}”`
      : plural(rows.length, "chat");
    results.innerHTML = rows.length
      ? rows.map((row, i) => hit(row, query, i)).join("")
      : `<p class="chats-note">${query
          ? "Nothing matches, even fuzzily. Try fewer words."
          : "No conversations yet. New chat starts one."}</p>`;
  }

  let debounce = 0;
  box.addEventListener("input", () => {
    clearTimeout(debounce);
    debounce = setTimeout(draw, 160);
  });
  box.addEventListener("keydown", (e) => {
    if (e.key !== "Enter") return;
    const top = results.querySelector(".search-hit");
    if (top) location.hash = top.getAttribute("href");
  });
  await draw();
  box.focus();
}
