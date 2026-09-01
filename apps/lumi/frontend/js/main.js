/** Synapse by Lumi: shell: hash router over the left sidebar,
 * theme toggle.
 * Routes: #/ask #/ask/<session> #/home #/semantics #/tables #/cosmos
 *         #/artifacts #/operate #/metric/<id> #/table/<physical>
 * Deep links work: a metric profile is a URL you can send someone. */

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

const outlet = document.getElementById("outlet");
let teardown = null;

function parseRoute() {
  const hash = location.hash.replace(/^#\/?/, "") || "home";
  const [page, ...rest] = hash.split("/");
  return { page, arg: decodeURIComponent(rest.join("/")) };
}

async function route() {
  if (teardown) { try { teardown(); } catch { /* page gone */ } }
  teardown = null;
  const { page, arg } = parseRoute();
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
    cosmos: renderCosmos,
    artifacts: renderArtifacts,
    operate: renderOperate,
  };
  const render = pages[page] ?? renderHome;
  teardown = await (page === "metric" || page === "table" || page === "ask"
    ? render()
    : render(outlet)) ?? null;
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
