/** The signed-in person, kept in one place for the whole surface.
 *
 * Three jobs. Every call a page makes goes through apiFetch, which adds
 * the CSRF header the server wants on anything that changes state and
 * turns a 401 into the sign-in page. whoami() answers who is here, one
 * request and then remembered. The account row in the sidebar is drawn
 * from that answer.
 *
 * Under SAHS_STORE=local the server answers whoami with the local
 * developer and never asks for a cookie, so a laptop without an
 * identity store never sees the sign-in page. */

const CSRF_COOKIE = "synapse_csrf";
const CSRF_HEADER = "X-CSRF-Token";
const SAFE = new Set(["GET", "HEAD", "OPTIONS"]);

/* this surface's doors: where a sign-in lands, and where it starts */
export const HOME = "#/chat";
export const SIGNIN = "#/signin";
export const SURFACE = "synapse";

export function readCookie(name) {
  const hit = document.cookie.split(";").map((c) => c.trim())
    .find((c) => c.startsWith(`${name}=`));
  return hit ? decodeURIComponent(hit.slice(name.length + 1)) : "";
}

let current = null;        // the person, once known
let pending = null;        // the whoami request in flight

/** fetch, with the CSRF header on state-changing API calls and the
 * sign-in page on a 401. The auth routes are exempt from the redirect:
 * a wrong password is a 401 the sign-in page itself must show. */
export async function apiFetch(url, init = {}) {
  const method = String(init.method || "GET").toUpperCase();
  const headers = new Headers(init.headers || {});
  const path = String(url);
  if (!SAFE.has(method) && path.startsWith("/api/")) {
    const token = readCookie(CSRF_COOKIE);
    if (token && !headers.has(CSRF_HEADER)) headers.set(CSRF_HEADER, token);
  }
  const r = await fetch(url, { ...init, headers, credentials: "same-origin" });
  if (r.status === 401 && !path.startsWith("/api/auth/")) toSignin();
  return r;
}

/** Only a hash on this surface may be a "next": never another site. */
export function safeHash(value) {
  const s = String(value || "");
  return s.startsWith("#/") && !s.includes("//") ? s : "";
}

export function toSignin(reason = "") {
  if (location.hash.startsWith(SIGNIN)) return;
  const next = safeHash(location.hash) || HOME;
  const q = new URLSearchParams({ next });
  if (reason) q.set("error", reason);
  current = null;
  location.hash = `${SIGNIN}?${q}`;
}

/** { user, status, reason }: user is null when nobody is signed in. */
export async function whoami({ fresh = false } = {}) {
  if (current && !fresh) return { user: current, status: 200, reason: "" };
  if (!pending) {
    pending = fetch("/api/auth/me", { credentials: "same-origin" })
      .then(async (r) => {
        if (r.status === 401) return { user: null, status: 401, reason: "sign in required" };
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          return { user: null, status: r.status, reason: body.detail || `${r.status}` };
        }
        const got = await r.json();
        return { user: got.user || null, status: 200, reason: "" };
      })
      .catch(() => ({ user: null, status: 0,
                      reason: "console unreachable: is the server running?" }))
      .finally(() => { pending = null; });
  }
  const answer = await pending;
  current = answer.user;
  return answer;
}

export const person = () => current;
export const forget = () => { current = null; };
export const can = (permission) =>
  Boolean(current && (current.permissions || []).includes(permission));

export const ROLE_ORDER = ["admin", "steward", "analyst"];
export const roleWord = (roles = []) => {
  const top = ROLE_ORDER.find((r) => roles.includes(r)) || roles[0] || "";
  return top ? top[0].toUpperCase() + top.slice(1) : "";
};
export const initials = (name = "") => {
  const words = String(name).trim().split(/[\s@._-]+/).filter(Boolean);
  const letters = words.length > 1 ? words[0][0] + words[words.length - 1][0]
    : String(words[0] || "?").slice(0, 2);
  return letters.toUpperCase();
};

/** The account row in the sidebar: avatar, name, role; a link to the
 * account page. Nothing known yet reads as such, never a made-up name. */
export function drawAccount(user) {
  const name = document.getElementById("account-name");
  const role = document.getElementById("account-role");
  const avatar = document.getElementById("account-avatar");
  if (!name || !role || !avatar) return;
  if (!user) {
    avatar.textContent = "?";
    name.textContent = "Not signed in";
    role.textContent = "Sign in";
    return;
  }
  const label = user.name || user.email || user.user_id;
  avatar.textContent = initials(label);
  name.textContent = label;
  role.textContent = roleWord(user.roles) || "Signed in";
}

/** Nav entries that carry data-needs="<permission>" show only to a
 * person who holds it. */
export function gateNav(user) {
  document.querySelectorAll("[data-needs]").forEach((el) => {
    const ok = Boolean(user && (user.permissions || []).includes(el.dataset.needs));
    el.hidden = !ok;
  });
}

export async function signOut({ everywhere = false } = {}) {
  const url = everywhere ? "/api/auth/logout-all" : "/api/auth/logout";
  try { await apiFetch(url, { method: "POST" }); } catch { /* the cookie is gone either way */ }
  forget();
  location.hash = SIGNIN;
  location.reload();
}
