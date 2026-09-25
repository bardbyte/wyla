/** People and access, for whoever holds users.manage: every account
 * with its roles, a role granted or taken back in place, an account
 * disabled or restored, and the access contexts (IP, browser, OS)
 * behind each person's sign-ins. */

import { apiFetch, can, person, toSignin, whoami } from "../session.js";
import { card, esc, prose } from "../ui.js";

const ROLES = ["analyst", "steward", "admin"];

const when = (iso) => {
  if (!iso) return "never";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? String(iso) : d.toLocaleString();
};

async function call(url, init) {
  try {
    const r = await apiFetch(url, init);
    const payload = await r.json().catch(() => ({}));
    if (!r.ok) return { available: false, reason: payload.detail || `${url} → ${r.status}` };
    return payload;
  } catch {
    return { available: false, reason: "console unreachable: is the server running?" };
  }
}

const userRow = (u, me) => {
  const mine = u.user_id === me;
  const roles = (u.roles || []).map((r) => `
    <span class="chip acc">${esc(r)}
      <button class="linklike revoke" data-user="${esc(u.user_id)}" data-role="${esc(r)}"
              title="take ${esc(r)} back" aria-label="revoke ${esc(r)}">×</button>
    </span>`).join(" ");
  const grantable = ROLES.filter((r) => !(u.roles || []).includes(r));
  const grant = grantable.length ? `
    <select class="grant" data-user="${esc(u.user_id)}" aria-label="grant a role">
      <option value="">grant…</option>
      ${grantable.map((r) => `<option value="${esc(r)}">${esc(r)}</option>`).join("")}
    </select>` : "";
  const status = u.status === "active" ? "active" : u.status;
  const toggle = mine ? "" : (u.status === "active"
    ? `<button class="linklike status" data-user="${esc(u.user_id)}" data-status="disabled">disable</button>`
    : `<button class="linklike status" data-user="${esc(u.user_id)}" data-status="active">restore</button>`);
  const remove = mine ? "" :
    `<button class="linklike danger remove" data-user="${esc(u.user_id)}" data-name="${esc(u.name || u.email)}">delete</button>`;
  return `
    <tr data-user="${esc(u.user_id)}">
      <td><b>${esc(u.name || "")}</b>${mine ? ' <span class="muted">(you)</span>' : ""}
          <div class="muted">${esc(u.email)}</div></td>
      <td><span class="chip status-${esc(status)}">${esc(status)}</span></td>
      <td class="roles">${roles} ${grant}</td>
      <td class="tokens" title="${esc(usageTitle(u.usage))}">${esc(tokensCell(u.usage))}</td>
      <td class="muted">${esc(when(u.last_login_at))}</td>
      <td class="actions">${toggle} ${remove}</td>
    </tr>`;
};

/* what a person's chats cost, across every chat of theirs (GET
 * /api/admin/users carries the aggregate): the compact total in the
 * cell, the breakdown on hover */
const fmtN = (n) => new Intl.NumberFormat().format(Number(n) || 0);
const tokensCell = (usage) => {
  const u = usage || {};
  const tokens = Number(u.tokens) || 0;
  if (!tokens && !(Number(u.turns) || 0)) return "—";
  return new Intl.NumberFormat(undefined, { notation: "compact",
    maximumFractionDigits: 1 }).format(tokens);
};
const usageTitle = (usage) => {
  const u = usage || {};
  if (!(Number(u.tokens) || 0) && !(Number(u.turns) || 0)) return "no chat turn yet";
  return `${fmtN(u.tokens_in)} in · ${fmtN(u.tokens_out)} out · ${
    fmtN(u.model_calls)} model calls · ${fmtN(u.turns)} turns across ${
    fmtN(u.chats)} chat${Number(u.chats) === 1 ? "" : "s"} · ${
    ((Number(u.elapsed_ms) || 0) / 1000).toFixed(1)}s`;
};

/* one person from /api/admin/access: their sign-in count and the
 * contexts (IP, browser, OS) those sign-ins came from */
const accessRow = (u) => {
  const contexts = u.contexts || [];
  return `
    <tr>
      <td><b>${esc(u.name || "")}</b><div class="muted">${esc(u.email || "")}</div></td>
      <td>${esc(String(u.successful_login_count ?? 0))}</td>
      <td>${contexts.length ? contexts.map((c) => `
        <div class="context"><b class="mono">${esc(c.ip || "?")}</b>
          <span class="muted">${esc(c.browser || "")} · ${esc(c.operating_system || "")}
          · ${esc(String(c.login_count || 0))}×</span></div>`).join("")
        : '<span class="muted">no sign-ins yet</span>'}</td>
      <td class="muted">${esc(when(u.last_login_at))}</td>
    </tr>`;
};

export async function renderUsers(outlet) {
  const who = await whoami();
  if (!who.user) { toSignin(); return; }
  if (!can("users.manage")) {
    outlet.innerHTML = card("PEOPLE", `<p class="muted">This page is for admins:
      it needs the <b class="mono">users.manage</b> permission.</p>`, "empty");
    return;
  }
  outlet.innerHTML = `
    <section class="page-sec users-page">
      <div class="hero">
        <h1>People</h1>
        <p>Every account on this Synapse, the roles each holds, and where they sign in from.
        Roles from Okta groups are set again at every sign-in; grant here for what Okta does not know.</p>
      </div>
      <div id="people">${card("PEOPLE", '<p class="muted">loading…</p>')}</div>
      <div id="access">${card("ACCESS CONTEXTS", '<p class="muted">loading…</p>')}</div>
    </section>`;
  const people = outlet.querySelector("#people");
  const access = outlet.querySelector("#access");

  async function drawPeople() {
    const got = await call("/api/admin/users?limit=500");
    if (!got.available) {
      people.innerHTML = card("PEOPLE", `<p class="muted">${prose(got.reason)}</p>`, "empty");
      return;
    }
    const me = person()?.user_id;
    people.innerHTML = card(`PEOPLE · ${got.users.length}`, `
      <div class="tablewrap">
        <table class="result users">
          <thead><tr><th>who</th><th>status</th><th>roles</th><th
            title="What their chats cost: tokens in and out, model calls, turns — hover a number">tokens</th><th>last sign-in</th><th></th></tr></thead>
          <tbody>${got.users.map((u) => userRow(u, me)).join("")}</tbody>
        </table>
      </div>
      <p class="signin-error" role="alert" hidden></p>`);
  }

  async function drawAccess() {
    const got = await call("/api/admin/access?limit=5000");
    if (!got.available) {
      access.innerHTML = card("ACCESS CONTEXTS", `<p class="muted">${prose(got.reason)}</p>`, "empty");
      return;
    }
    access.innerHTML = card("ACCESS CONTEXTS", `
      <p class="muted">${prose(got.note || "")}</p>
      <div class="tablewrap">
        <table class="result access">
          <thead><tr><th>who</th><th>sign-ins</th><th>contexts</th><th>last seen</th></tr></thead>
          <tbody>${(got.users || []).map(accessRow).join("")}</tbody>
        </table>
      </div>`);
  }

  const complain = (text) => {
    const note = people.querySelector(".signin-error");
    if (!note) return;
    note.textContent = text;
    note.hidden = !text;
  };

  people.addEventListener("click", async (e) => {
    const revoke = e.target.closest("button.revoke");
    const status = e.target.closest("button.status");
    const remove = e.target.closest("button.remove");
    if (revoke) {
      const got = await call(`/api/admin/users/${encodeURIComponent(revoke.dataset.user)}/roles/${
        encodeURIComponent(revoke.dataset.role)}`, { method: "DELETE" });
      complain(got.available ? "" : got.reason);
      drawPeople();
    } else if (status) {
      const got = await call(`/api/admin/users/${encodeURIComponent(status.dataset.user)}`, {
        method: "PATCH", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: status.dataset.status }),
      });
      complain(got.available ? "" : got.reason);
      drawPeople(); drawAccess();
    } else if (remove) {
      if (!window.confirm(`Delete ${remove.dataset.name}? Their sessions end now; the audit keeps their history.`)) return;
      const got = await call(`/api/admin/users/${encodeURIComponent(remove.dataset.user)}`, { method: "DELETE" });
      complain(got.available ? "" : got.reason);
      drawPeople(); drawAccess();
    }
  });
  people.addEventListener("change", async (e) => {
    const grant = e.target.closest("select.grant");
    if (!grant || !grant.value) return;
    const got = await call(`/api/admin/users/${encodeURIComponent(grant.dataset.user)}/roles`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ role: grant.value }),
    });
    complain(got.available ? "" : got.reason);
    drawPeople();
  });

  drawPeople();
  drawAccess();
}
