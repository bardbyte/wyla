/** Memory: what Synapse remembers about you, as a document you can
 * read and edit. One line per memory — a preference or a choice you
 * settled in chat. Save it: a line you add is remembered, a line you
 * remove is retired, and the next chat reads it. Never a metric
 * definition or a number: those live in the graph. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { esc } from "../ui.js";

export async function renderMemory(outlet) {
  outlet.innerHTML = `
    <div class="library-page memory-page">
      <div class="shelf-toolbar">
        <h1>Memory</h1>
        <span class="muted" id="mem-count"></span>
        <span class="spacer"></span>
        <button class="btn" id="mem-preview" aria-pressed="false">preview</button>
        <button class="btn primary" id="mem-save" disabled>Save</button>
      </div>
      <p class="muted shelf-intro">What Synapse has recorded so far, as
        <code>memory.md</code>. Edit the lines and save: a line you add is
        remembered, a line you remove is retired, and the next chat reads
        it. Synapse adds a line itself when you settle a preference in
        chat, and says so inline with an undo.</p>
      <textarea class="input memory-editor" id="mem-text" spellcheck="false"></textarea>
      <div class="md memory-rendered" id="mem-rendered" hidden></div>
      <p class="muted" id="mem-note"></p>
    </div>`;
  const $ = (id) => outlet.querySelector(`#${id}`);
  const text = $("mem-text");
  let loaded = "";
  const paint = (got) => {
    text.value = got.text || "";
    loaded = text.value;
    $("mem-count").textContent = `${got.count ?? 0} ${got.count === 1 ? "memory" : "memories"}`;
    $("mem-save").disabled = true;
    if (!$("mem-rendered").hidden) $("mem-rendered").innerHTML = renderMarkdown(text.value, "md");
  };
  const got = await api.chatMemoryDoc().catch(() => ({}));
  if (!text.isConnected) return;
  if (!got.available) {
    $("mem-note").textContent = got.reason || "memory is not available";
    return;
  }
  paint(got);
  text.addEventListener("input", () => { $("mem-save").disabled = text.value === loaded; });
  $("mem-preview").addEventListener("click", () => {
    const showing = !$("mem-rendered").hidden;
    $("mem-rendered").hidden = showing;
    text.hidden = !showing;
    $("mem-preview").setAttribute("aria-pressed", String(!showing));
    $("mem-preview").textContent = showing ? "preview" : "edit";
    if (!showing) $("mem-rendered").innerHTML = renderMarkdown(text.value, "md");
  });
  $("mem-save").addEventListener("click", async () => {
    $("mem-save").disabled = true;
    const saved = await api.chatSaveMemoryDoc(text.value)
      .catch((err) => ({ available: false, reason: String(err) }));
    if (!saved.available) {
      $("mem-note").textContent = saved.reason || "not saved";
      $("mem-save").disabled = false;
      return;
    }
    paint(saved);
    $("mem-note").textContent = `saved: ${saved.added} added, ${saved.retired} retired — the next chat reads it`;
  });
}
