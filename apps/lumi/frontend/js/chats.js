/** The chats shelf in the sidebar (§8): one nav, not two. Starred
 * chats, then recents — newest first, searchable, star and archive
 * on hover. "New ask" in the navlist starts a fresh conversation; a
 * row reopens a kept one with its artifacts. A session appears the
 * moment it is created and earns its name from the first thing you
 * asked. (Projects exist in the store and API but stay out of the
 * surface for now — deliberately.) */

import { api } from "./api.js";
import { esc } from "./ui.js";

const shelf = document.querySelector(".chats");
const body = shelf?.querySelector(".chats-body");
const search = shelf?.querySelector(".chats-search");

const when = (iso) => {
  const then = Date.parse(iso);
  if (!Number.isFinite(then)) return "";
  const mins = Math.round((Date.now() - then) / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m`;
  if (mins < 60 * 24) return `${Math.round(mins / 60)}h`;
  return `${Math.round(mins / 1440)}d`;
};

function chatRow(row, current) {
  return `
    <a class="chat-row${row.id === current ? " on" : ""}"
       href="#/chat/${esc(row.id)}" title="${esc(row.title || row.id)}">
      <span class="chat-title">${esc(row.title || "New chat")}</span>
      <span class="row-tools">
        <button class="row-btn star${row.starred ? " on" : ""}"
          data-id="${esc(row.id)}" data-star="${row.starred ? 0 : 1}"
          title="${row.starred ? "unstar" : "star"}">${
          row.starred ? "★" : "☆"}</button>
        <button class="row-btn" data-id="${esc(row.id)}"
          data-archive="1" title="archive">⌫</button>
      </span>
      <span class="chat-when">${row.running ? "·" : esc(when(
        row.updated_at))}</span>
    </a>`;
}

export async function refreshChats() {
  if (!body) return;
  const payload = await api.chatSessions(30);
  const rows = payload.available ? payload.sessions || [] : [];
  if (!payload.available) {
    body.innerHTML = `<p class="chats-note">${
      esc(payload.reason || "sessions unavailable")}</p>`;
    return;
  }
  const needle = (search?.value || "").trim().toLowerCase();
  const kept = rows.filter((row) => !needle
    || (row.title || "untitled").toLowerCase().includes(needle));
  const current = localStorage.getItem("synapse-chat-session");

  const parts = [];
  const starred = kept.filter((r) => r.starred);
  if (starred.length) {
    parts.push('<div class="shelf-head">Starred</div>');
    parts.push(starred.map((r) => chatRow(r, current)).join(""));
  }
  const rest = kept.filter((r) => !r.starred);
  parts.push('<div class="shelf-head">Recent</div>');
  parts.push(rest.map((r) => chatRow(r, current)).join("")
    || `<p class="chats-note">${needle
      ? "Nothing matches."
      : "No conversations yet. New ask starts one; it lands here "
        + "with its artifacts."}</p>`);
  body.innerHTML = parts.join("");

  for (const btn of body.querySelectorAll(".row-btn[data-star]")) {
    btn.addEventListener("click", async (e) => {
      e.preventDefault(); e.stopPropagation();
      await api.chatStar(btn.dataset.id, btn.dataset.star === "1");
      refreshChats();
    });
  }
  for (const btn of body.querySelectorAll(".row-btn[data-archive]")) {
    btn.addEventListener("click", async (e) => {
      e.preventDefault(); e.stopPropagation();
      await api.chatArchive(btn.dataset.id, true);
      refreshChats();
    });
  }
}

search?.addEventListener("input", refreshChats);
window.addEventListener("synapse:sessions", refreshChats);
window.addEventListener("hashchange", () => {
  // keep the "you are here" mark honest as you move between sessions
  const current = localStorage.getItem("synapse-chat-session");
  body?.querySelectorAll(".chat-row").forEach((a) =>
    a.classList.toggle("on",
                       a.getAttribute("href") === `#/chat/${current}`));
});
