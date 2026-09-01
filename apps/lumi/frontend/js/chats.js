/** The chats shelf in the sidebar: the Synapse conversations on this
 * machine, newest first, filling the nav below the sections — one nav,
 * not two. "New ask" in the navlist starts a fresh one; a row reopens
 * a kept conversation with its artifacts. A session appears the moment
 * it is created and earns its name from the first thing you asked, so
 * an untitled row means a conversation that never got a subject. */

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
  if (!kept.length) {
    body.innerHTML = `<p class="chats-note">${needle
      ? "Nothing matches."
      : "No conversations yet. New ask starts one; it lands here "
        + "with its artifacts."}</p>`;
    return;
  }
  const current = localStorage.getItem("synapse-chat-session");
  body.innerHTML = kept.map((row) => `
    <a class="chat-row${row.id === current ? " on" : ""}"
       href="#/chat/${esc(row.id)}" title="${esc(row.title || row.id)}">
      <span class="chat-title">${esc(row.title || "New chat")}</span>
      <span class="chat-when">${row.running ? "·" : esc(when(
        row.updated_at))}</span>
    </a>`).join("");
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
