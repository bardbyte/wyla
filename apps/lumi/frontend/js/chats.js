/** The chats shelf in the sidebar: the Ask sessions on this machine,
 * newest first. It is a list of what really happened — a session
 * appears the moment it is created and earns its name from the first
 * plan that bound a metric, so an untitled row means a conversation
 * that never got as far as a subject. */

import { api } from "./api.js";
import { esc } from "./ui.js";

const shelf = document.querySelector(".chats");
const body = shelf?.querySelector(".chats-body");

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
  const payload = await api.askSessions(12);
  const rows = payload.available ? payload.sessions || [] : [];
  if (!payload.available) {
    body.innerHTML = `<p class="chats-note">${
      esc(payload.reason || "sessions unavailable")}</p>`;
    return;
  }
  if (!rows.length) {
    body.innerHTML = `<p class="chats-note">No conversations yet. Ask a
      question and it lands here, with its plan and its receipts.</p>`;
    return;
  }
  const current = localStorage.getItem("synapse-ask-session");
  body.innerHTML = rows.map((row) => `
    <a class="chat-row${row.id === current ? " on" : ""}"
       href="#/ask/${esc(row.id)}" title="${esc(row.title || row.id)}">
      <span class="chat-title">${esc(row.title || "untitled")}</span>
      <span class="chat-when">${row.running ? "·" : esc(when(
        row.updated_at))}</span>
    </a>`).join("");
}

window.addEventListener("synapse:sessions", refreshChats);
window.addEventListener("hashchange", () => {
  // keep the "you are here" mark honest as you move between sessions
  const current = localStorage.getItem("synapse-ask-session");
  body?.querySelectorAll(".chat-row").forEach((a) =>
    a.classList.toggle("on", a.getAttribute("href") === `#/ask/${current}`));
});
