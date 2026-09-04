/** Synapse Semantic Intelligence: the chat first, the library under
 * it. A hash router over the left sidebar, a theme toggle.
 * Routes: #/chat #/chat/<session> #/search #/products
 *         #/product/<physical> #/metrics #/metric/<id> #/artifacts
 * Deep links work: a metric profile is a URL you can send someone. */

import { renderChat } from "./pages/chat.js";
import { renderSearch } from "./pages/search.js";
import { renderProducts } from "./pages/tables.js";
import { renderTable } from "./pages/table.js";
import { renderMetrics } from "./pages/semantics.js";
import { renderMetric } from "./pages/metric.js";
import { renderArtifacts } from "./pages/artifacts.js";
import { refreshChats } from "./chats.js";

const outlet = document.getElementById("outlet");
let teardown = null;

function parseRoute() {
  const hash = location.hash.replace(/^#\/?/, "") || "chat";
  const [page, ...rest] = hash.split("/");
  return { page, arg: decodeURIComponent(rest.join("/")) };
}

async function route() {
  if (teardown) { try { teardown(); } catch { /* page gone */ } }
  teardown = null;
  const { page, arg } = parseRoute();
  const tab = page === "metric" ? "metrics"
    : page === "product" ? "products" : page;
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
    artifacts: () => renderArtifacts(outlet),
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
  try { localStorage.setItem("lumi-theme", next); } catch { /* fine */ }
  applyThemeGlyph();
});
applyThemeGlyph();

route();
refreshChats();
