/** The front door. Okta when the server has it; the email-and-password
 * form only when a deployment turned it on (AUTH_LOCAL_LOGIN=1); a plain
 * sentence when neither is configured. A person already signed in is
 * sent straight to where they were going. */

import { apiFetch, forget, safeHash, whoami, HOME } from "../session.js";
import { esc, prose } from "../ui.js";

const LOCAL_FORM = `
  <form class="signin-form" id="local-login" autocomplete="on">
    <div class="sec-label">Email and password</div>
    <label class="field">Email
      <input class="search" type="email" name="email" required autocomplete="username" />
    </label>
    <label class="field">Password
      <input class="search" type="password" name="password" required minlength="8"
             autocomplete="current-password" />
    </label>
    <div class="field names" hidden>
      <label class="field">First name
        <input class="search" type="text" name="first_name" maxlength="100" autocomplete="given-name" />
      </label>
      <label class="field">Last name
        <input class="search" type="text" name="last_name" maxlength="100" autocomplete="family-name" />
      </label>
    </div>
    <div class="signin-actions">
      <button class="btn primary" type="submit" data-mode="login">Sign in</button>
      <button class="linklike" type="button" id="toggle-signup">First time here? Create an account</button>
    </div>
    <p class="signin-error" role="alert" hidden></p>
  </form>`;

export async function renderSignin(outlet, query = "") {
  const params = new URLSearchParams(query || "");
  const next = safeHash(params.get("next")) || HOME;
  const error = params.get("error") || "";
  forget();
  document.body.classList.add("signed-out");

  const [who, status] = await Promise.all([
    whoami({ fresh: true }),
    apiFetch("/api/auth/okta").then((r) => r.json()).catch(() => ({})),
  ]);
  if (who.user) {                       // already in: straight through
    document.body.classList.remove("signed-out");
    location.hash = next;
    return;
  }

  const startUrl = `${status.start || "/api/auth/okta/start"}?next=${
    encodeURIComponent(location.pathname + next)}`;
  const nothing = !status.configured && !status.local_login;
  outlet.innerHTML = `
    <section class="signin">
      <div class="card signin-card">
        <div class="brand">
          <span class="wordmark">SYNAPSE</span>
          <span class="tagline">powered by Lumi</span>
        </div>
        <h1>Sign in</h1>
        ${error ? `<p class="signin-error" role="alert">${prose(error)}</p>` : ""}
        ${status.configured ? `
          <a class="btn primary okta" id="okta-start" href="${esc(startUrl)}">Continue with Okta</a>
          <p class="muted">Your ${esc(status.issuer_host || "Okta")} account. Nothing to remember here.</p>` : ""}
        ${status.configured && status.local_login ? `<div class="signin-or"><span>or</span></div>` : ""}
        ${status.local_login ? LOCAL_FORM : ""}
        ${nothing ? `
          <p class="muted">Sign-in is not configured on this server: set the
          <b class="mono">OKTA_*</b> variables, or <b class="mono">AUTH_LOCAL_LOGIN=1</b>
          for the email-and-password path on a laptop.</p>
          ${who.reason && who.status !== 401
            ? `<p class="signin-error" role="alert">${prose(who.reason)}</p>` : ""}` : ""}
      </div>
    </section>`;

  const form = outlet.querySelector("#local-login");
  if (!form) return;
  const names = form.querySelector(".names");
  const submit = form.querySelector("button[type=submit]");
  const toggle = form.querySelector("#toggle-signup");
  const note = form.querySelector(".signin-error");
  toggle.addEventListener("click", () => {
    const signup = submit.dataset.mode !== "signup";
    submit.dataset.mode = signup ? "signup" : "login";
    submit.textContent = signup ? "Create account" : "Sign in";
    names.hidden = !signup;
    toggle.textContent = signup ? "Have an account? Sign in" : "First time here? Create an account";
    form.password.autocomplete = signup ? "new-password" : "current-password";
  });
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    note.hidden = true;
    submit.disabled = true;
    const signup = submit.dataset.mode === "signup";
    const body = { email: form.email.value.trim(), password: form.password.value };
    if (signup) {
      body.first_name = form.first_name.value.trim();
      body.last_name = form.last_name.value.trim();
    }
    try {
      const r = await apiFetch(signup ? "/api/auth/signup" : "/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const payload = await r.json().catch(() => ({}));
      if (!r.ok) {
        note.textContent = payload.detail || `sign-in failed (${r.status})`;
        note.hidden = false;
        return;
      }
      forget();
      document.body.classList.remove("signed-out");
      location.hash = next;
      location.reload();                // the shell boots as this person
    } catch {
      note.textContent = "console unreachable: is the server running?";
      note.hidden = false;
    } finally {
      submit.disabled = false;
    }
  });
}
