/** The account page: who you are here, what you may do, the Google
 * connection BigQuery runs under when the server delegates to people,
 * and the two ways out. */

import { apiFetch, can, roleWord, signOut, toSignin, whoami, SURFACE } from "../session.js";
import { card, esc, prose } from "../ui.js";

const PERMISSION_WORDS = {
  "chat.use": "open a chat and ask",
  "chat.autopilot": "run queries under the limits without handing over",
  "skills.own": "save skills that load for oneself",
  "skills.share": "promote an own skill to the shared shelf",
  "knowledge.stage": "stage a knowledge file for a business unit",
  "metrics.certify": "move a metric to certified or back",
  "graph.build": "run build-graph and compile",
  "sources.manage": "add, patch, retire sources",
  "users.manage": "invite, grant roles, lock, disable",
  "audit.read": "read the audit",
};

const when = (iso) => {
  if (!iso) return "never";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? String(iso) : d.toLocaleString();
};

export async function renderAccount(outlet) {
  const who = await whoami({ fresh: true });
  if (!who.user) { toSignin(); return; }
  const u = who.user;
  const roles = (u.roles || []).map((r) => `<span class="chip acc">${esc(r)}</span>`).join(" ");
  const permissions = (u.permissions || []).map((p) =>
    `<li><b class="mono">${esc(p)}</b> <span class="muted">${esc(PERMISSION_WORDS[p] || "")}</span></li>`)
    .join("");
  const manage = SURFACE === "admin" && can("users.manage")
    ? `<p><a class="btn" href="#/users">Manage people</a></p>` : "";

  outlet.innerHTML = `
    <section class="page-sec account-page">
      <div class="hero">
        <h1>${esc(u.name || u.email)}</h1>
        <p>${esc(u.email || "")}${u.email ? " · " : ""}${esc(roleWord(u.roles))}${
          u.last_login_at ? ` · last sign-in ${esc(when(u.last_login_at))}` : ""}</p>
      </div>
      <div class="grid2">
        ${card("YOUR ACCESS", `
          <p class="stack">${roles || '<span class="muted">no roles yet</span>'}</p>
          <ul class="permissions">${permissions || '<li class="muted">nothing yet</li>'}</ul>
          <p class="muted">Surfaces: ${esc((u.surfaces || []).join(", ") || "none")}.
          Roles come from your Okta groups; ask an admin to be added to one.</p>
          ${manage}`)}
        ${card("GOOGLE BIGQUERY", `<div id="google-conn"><p class="muted">loading…</p></div>`)}
        ${card("SESSIONS", `
          <p class="muted">Sign out here, or everywhere you are signed in.</p>
          <p class="stack">
            <button class="btn" id="signout">Sign out</button>
            <button class="btn" id="signout-all">Sign out everywhere</button>
          </p>`)}
      </div>
    </section>`;

  outlet.querySelector("#signout").addEventListener("click", () => signOut());
  outlet.querySelector("#signout-all").addEventListener("click", () => signOut({ everywhere: true }));

  const host = outlet.querySelector("#google-conn");
  let popup = null;
  let poll = 0;
  const stopPolling = () => { if (poll) { clearInterval(poll); poll = 0; } };

  async function drawConnection() {
    let got = {};
    try {
      const r = await apiFetch("/api/auth/google/connection");
      got = await r.json().catch(() => ({}));
      if (!r.ok) got = { available: false, reason: got.detail || `${r.status}` };
    } catch { got = { available: false, reason: "console unreachable" }; }
    if (!got.available) {
      host.innerHTML = `<p class="muted">${prose(got.reason || "unavailable")}</p>`;
      return;
    }
    if (!got.requires_user_oauth) {
      host.innerHTML = `<p class="muted">Queries run as this server's own service
        account here; there is nothing to connect.</p>`;
      return;
    }
    if (got.connected) {
      host.innerHTML = `
        <p>Connected as <b>${esc(got.email || "your Google account")}</b>. Queries run
        as you, with your BigQuery access.</p>
        <p><button class="btn" id="google-disconnect">Disconnect</button></p>`;
      host.querySelector("#google-disconnect").addEventListener("click", async () => {
        await apiFetch("/api/auth/google/connection", { method: "DELETE" });
        drawConnection();
      });
      return;
    }
    host.innerHTML = `
      <p>Queries run as you: connect the Google account that holds your BigQuery
      access. Google asks once; the server keeps a refresh token, encrypted.</p>
      <p><button class="btn primary" id="google-connect">Connect Google</button></p>`;
    host.querySelector("#google-connect").addEventListener("click", () => {
      popup = window.open("/api/auth/google/start?popup=1", "google-connect",
                          "width=520,height=680,menubar=no,toolbar=no");
      if (!popup) {                     // popups blocked: go there in this tab
        location.href = "/api/auth/google/start";
        return;
      }
      stopPolling();
      poll = setInterval(() => {        // no message arrived: notice the window closing
        if (popup && popup.closed) { stopPolling(); drawConnection(); }
      }, 1500);
    });
  }

  const onMessage = (e) => {
    if (e.origin !== location.origin || e.data?.type !== "google-connected") return;
    stopPolling();
    drawConnection();
  };
  window.addEventListener("message", onMessage);
  drawConnection();
  return () => { window.removeEventListener("message", onMessage); stopPolling(); };
}
