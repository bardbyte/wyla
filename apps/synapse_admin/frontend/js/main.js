/** Synapse by Lumi: shell: hash router over the left sidebar,
 * theme toggle.
 * Routes: #/ask #/ask/<session> #/home #/semantics #/tables #/cosmos
 *         #/artifacts #/operate #/metric/<id> #/table/<physical>
 *         #/kc #/kc/dictionary #/kc/glossary #/kc/<physical>
 *         #/signin?next= #/account #/users
 * Deep links work: a metric profile is a URL you can send someone.
 * The shell boots as the signed-in person (js/session.js): with an
 * identity store and nobody signed in, every route is the sign-in page. */

import { renderHome } from "./pages/home.js";
import { renderSemantics } from "./pages/semantics.js";
import { renderMetric } from "./pages/metric.js";
import { renderTable } from "./pages/table.js";
import { renderTables } from "./pages/tables.js";
import { renderCosmos } from "./pages/cosmos.js";
import { renderArtifacts } from "./pages/artifacts.js";
import { renderOperate } from "./pages/operate.js";
import { renderAsk } from "./pages/ask.js";
import { refreshChats } from "./chats.js";
import { renderChat } from "./pages/chat.js";
import { renderSkills } from "./pages/skills.js";
import {
  renderKc, renderKcDictionary, renderKcGlossary, renderKcTable,
} from "./pages/kc.js";
import { renderSignin } from "./pages/signin.js";
import { renderAccount } from "./pages/account.js";
import { renderUsers } from "./pages/users.js";
import { drawAccount, gateNav, whoami, HOME, SIGNIN } from "./session.js";

const outlet = document.getElementById("outlet");
let teardown = null;

function parseRoute() {
  const raw = location.hash.replace(/^#\/?/, "");
  const [path, query = ""] = raw.split("?");
  const hash = path || "home";
  const [page, ...rest] = hash.split("/");
  return { page, arg: decodeURIComponent(rest.join("/")), query };
}

async function route() {
  if (teardown) { try { teardown(); } catch { /* page gone */ } }
  teardown = null;
  const { page, arg, query } = parseRoute();
  if (page !== "signin" && signedOut) {         // nobody here: the door first
    location.hash = `${SIGNIN}?${new URLSearchParams({ next: location.hash || HOME })}`;
    return;
  }
  const tab = page === "metric" ? "semantics"
    : page === "table" ? "tables" : page;
  document.querySelectorAll(".navlist a[data-tab]").forEach((a) =>
    a.classList.toggle("active", a.dataset.tab === tab));
  outlet.classList.toggle("wide", page === "cosmos");
  outlet.classList.toggle("chat", page === "ask");
  outlet.classList.toggle("chatv2page", page === "chat");
  outlet.innerHTML = "";
  const pages = {
    home: renderHome,
    semantics: renderSemantics,
    tables: renderTables,
    metric: () => renderMetric(outlet, arg),
    table: () => renderTable(outlet, arg),
    ask: () => renderAsk(outlet, arg),
    chat: () => renderChat(outlet, arg),
    skills: renderSkills,
    cosmos: renderCosmos,
    artifacts: renderArtifacts,
    operate: renderOperate,
    kc: () => (arg === "" ? renderKc(outlet)
      : arg === "dictionary" ? renderKcDictionary(outlet)
      : arg === "glossary" ? renderKcGlossary(outlet)
      : renderKcTable(outlet, arg)),
    signin: () => renderSignin(outlet, query),
    account: () => renderAccount(outlet),
    users: () => renderUsers(outlet),
  };
  const render = pages[page] ?? renderHome;
  teardown = await (page === "metric" || page === "table" || page === "ask"
    || page === "kc" || page === "signin" || page === "account" || page === "users"
    ? render()
    : render(outlet)) ?? null;
}

/* who is here: the account row, the gated nav entries, and whether the
 * shell may show anything but the sign-in page */
let signedOut = false;
async function boot() {
  const who = await whoami();
  signedOut = !who.user && who.status === 401;
  document.body.classList.toggle("signed-out", signedOut);
  drawAccount(who.user);
  gateNav(who.user);
  if (who.user && !(who.user.surfaces || []).includes("admin")) {
    // this console is the admins'; everyone else has Synapse
    location.replace("/synapse/");
    return;
  }
  await route();
  if (!signedOut) refreshChats();
}

window.addEventListener("hashchange", route);

/* theme toggle: explicit choice wins over system */
const toggle = document.getElementById("theme-toggle");
const applyThemeGlyph = () => {
  const dark =
    document.documentElement.dataset.theme === "dark" ||
    (!document.documentElement.dataset.theme &&
      matchMedia("(prefers-color-scheme: dark)").matches);
  toggle.textContent = dark ? "☀" : "☾";
};
toggle.addEventListener("click", () => {
  const dark =
    document.documentElement.dataset.theme === "dark" ||
    (!document.documentElement.dataset.theme &&
      matchMedia("(prefers-color-scheme: dark)").matches);
  const next = dark ? "light" : "dark";
  document.documentElement.dataset.theme = next;
  try { localStorage.setItem("synapse-theme", next); } catch { /* fine */ }
  applyThemeGlyph();
});
applyThemeGlyph();

boot();
