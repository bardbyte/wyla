/** Synapse by Lumi: the chat first, the library under it. A hash
 * router over the left sidebar, a theme toggle.
 * Routes: #/chat #/chat/<session> #/search #/products
 *         #/product/<physical> #/metrics #/metric/<id> #/skills #/memory
 *         #/signin?next= #/account
 *         (#/knowledge and #/artifacts are Skills' old names and still answer;
 *         the library routes answer by URL only — their Explore shelf is
 *         off this surface for now and lives on in the admin console)
 * Deep links work: a metric profile is a URL you can send someone.
 * The shell boots as the signed-in person (js/session.js): with an
 * identity store and nobody signed in, every route is the sign-in page. */

import { renderChat } from "./pages/chat.js";
import { renderSearch } from "./pages/search.js";
import { renderProducts } from "./pages/tables.js";
import { renderTable } from "./pages/table.js";
import { renderMetrics } from "./pages/semantics.js";
import { renderMetric } from "./pages/metric.js";
import { renderSkills } from "./pages/skills.js";
import { renderMemory } from "./pages/memory.js";
import { renderSignin } from "./pages/signin.js";
import { renderAccount } from "./pages/account.js";
import { refreshChats } from "./chats.js";
import { api } from "./api.js";
import { drawAccount, gateNav, whoami, HOME, SIGNIN } from "./session.js";

const outlet = document.getElementById("outlet");
let teardown = null;

function parseRoute() {
  const raw = location.hash.replace(/^#\/?/, "");
  const [path, query = ""] = raw.split("?");
  const hash = path || "chat";
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
  const tab = page === "metric" ? "metrics"
    : page === "product" ? "products"
    : (page === "artifacts" || page === "knowledge") ? "skills" : page;
  document.querySelectorAll(".navlist a[data-tab]").forEach((a) =>
    a.classList.toggle("active", a.dataset.tab === tab));
  outlet.classList.toggle("chatv2page", page === "chat");
  outlet.innerHTML = "";
  const pages = {
    chat: () => renderChat(outlet, arg),
    search: () => renderSearch(outlet, arg),
    products: () => renderProducts(outlet),
    product: () => renderTable(outlet, arg),
    metrics: () => renderMetrics(outlet),
    metric: () => renderMetric(outlet, arg),
    skills: () => renderSkills(outlet),
    memory: () => renderMemory(outlet),
    knowledge: () => renderSkills(outlet),         // the old names
    artifacts: () => renderSkills(outlet),
    signin: () => renderSignin(outlet, query),
    account: () => renderAccount(outlet),
  };
  const render = pages[page] ?? pages.chat;
  teardown = await render() ?? null;
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

/* the brand: an image logo named by SYNAPSE_LOGO in the silo .env
 * replaces the words once it has loaded; without one, or when the
 * file cannot be read, the words stay */
async function brandLogo() {
  const brand = document.getElementById("brand");
  if (!brand) return;
  let got = {};
  try {
    got = await fetch("/api/synapse/brand").then((r) => r.json());
  } catch { return; }
  if (!got.logo) {
    // the words stay; the reason is a page away (/api/synapse/brand)
    // and in the console, never a broken image in the header
    if (got.configured) console.warn(`SYNAPSE_LOGO: ${got.reason}`);
    return;
  }
  const img = new Image();
  img.className = "brand-logo";
  img.alt = "Synapse by Lumi";
  img.onerror = () => console.warn(
    "SYNAPSE_LOGO: the browser could not decode the image the server "
    + "sent; open /api/synapse/brand for what the file's bytes are");
  img.onload = () => {
    const link = document.createElement("a");
    link.className = "brand-link";
    link.href = "#/chat";
    link.title = "Synapse by Lumi";
    link.appendChild(img);
    brand.replaceChildren(link);
    brand.classList.add("has-logo");
  };
  img.src = `/api/synapse/logo?v=${encodeURIComponent(got.stamp || "")}`;
}

/* the Skills badge: what waits on the approval board — submissions
 * awaiting review first, unread notices otherwise */
async function refreshReviewsBadge() {
  const badge = document.getElementById("nav-skills-badge");
  if (!badge) return;
  const got = await api.chatReviews().catch(() => ({}));
  if (!got || !got.available) { badge.hidden = true; return; }
  const pending = got.pending || 0;
  const unread = got.unread || 0;
  const n = pending || unread;
  badge.hidden = n === 0;
  badge.textContent = n ? String(n) : "";
  badge.title = pending
    ? `${pending} awaiting review` : `${unread} unread`;
}
window.addEventListener("synapse:reviews", refreshReviewsBadge);

/* who is here: the account row, the gated nav entries, and whether the
 * shell may show anything but the sign-in page */
let signedOut = false;
async function boot() {
  const who = await whoami();
  signedOut = !who.user && who.status === 401;
  document.body.classList.toggle("signed-out", signedOut);
  drawAccount(who.user);
  gateNav(who.user);
  await route();
  brandLogo();
  if (!signedOut) { refreshChats(); refreshReviewsBadge(); }
}

boot();
