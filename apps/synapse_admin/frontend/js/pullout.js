/** The pullout: a Claude-style panel sliding in from the right with
 * copy-to-clipboard and close (button or Esc). One implementation,
 * used by the Artifacts reader and the Table Profile's agent card. */

import { esc } from "./ui.js";

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    // headless / permission-less fallback
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    let ok = false;
    try { ok = document.execCommand("copy"); } catch { ok = false; }
    ta.remove();
    return ok;
  }
}

/** Mounts a pullout into `outlet`. Returns {open, close, teardown}.
 * open({title, kind, sub, html, raw, onClose}) renders `html` in the
 * body and wires copy to the `raw` text. */
export function createPullout(outlet) {
  const panel = document.createElement("aside");
  panel.className = "artifact-panel";
  panel.hidden = true;
  panel.setAttribute("aria-label", "Reader");
  panel.innerHTML = `
    <div class="artifact-panel-head">
      <span class="profile-title clip" data-po="title"></span>
      <span class="chip" data-po="kind"></span>
      <span class="spacer"></span>
      <button class="btn" data-po="copy"
        title="Copy to the clipboard">copy</button>
      <button class="icon-btn" data-po="close"
        aria-label="Close the reader">✕</button>
    </div>
    <div class="muted mono clip" data-po="sub"></div>
    <div class="artifact-panel-body md" data-po="body"></div>`;
  outlet.appendChild(panel);

  let raw = "";
  let onClose = null;
  const close = () => {
    panel.classList.remove("open");     // slide out ...
    setTimeout(() => { panel.hidden = true; }, 260);  // ... then hide
    if (onClose) onClose();
  };
  const open = (spec) => {
    raw = spec.raw ?? "";
    onClose = spec.onClose ?? null;
    panel.querySelector('[data-po="title"]').textContent =
      spec.title ?? "";
    panel.querySelector('[data-po="kind"]').textContent =
      spec.kind ?? "";
    panel.querySelector('[data-po="sub"]').textContent =
      spec.sub ?? "";
    panel.querySelector('[data-po="body"]').innerHTML =
      spec.html ?? `<pre class="md-code">${esc(raw)}</pre>`;
    panel.hidden = false;
    requestAnimationFrame(() => panel.classList.add("open"));
  };
  panel.querySelector('[data-po="close"]')
    .addEventListener("click", close);
  const copyBtn = panel.querySelector('[data-po="copy"]');
  copyBtn.addEventListener("click", async () => {
    const ok = await copyText(raw);
    copyBtn.textContent = ok ? "copied ✓" : "copy failed";
    setTimeout(() => { copyBtn.textContent = "copy"; }, 1600);
  });
  const onKey = (e) => { if (e.key === "Escape") close(); };
  window.addEventListener("keydown", onKey);
  const teardown = () =>
    window.removeEventListener("keydown", onKey);
  return { open, close, teardown };
}
