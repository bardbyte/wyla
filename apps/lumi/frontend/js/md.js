/** Minimal Markdown renderer for the knowledge shelf: headings, code
 * fences, inline code, bold/italic, lists, blockquotes, links, rules,
 * paragraphs. Everything is escaped FIRST; the transforms below only
 * ever emit our own tags. Non-markdown files (yaml, sql) render as
 * one code block. */

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;",
    '"': "&quot;", "'": "&#39;",
  }[c]));

const inline = (s) => s
  .replace(/`([^`]+)`/g, "<code>$1</code>")
  .replace(/\*\*([^*]+)\*\*/g, "<b>$1</b>")
  .replace(/(^|[^*])\*([^*\n]+)\*/g, "$1<i>$2</i>")
  .replace(/\[([^\]]+)\]\((https?:[^)\s]+)\)/g,
    '<a href="$2" target="_blank" rel="noopener">$1</a>');

export function renderMarkdown(text, kind = "md") {
  if (kind !== "md" && kind !== "markdown") {
    return `<pre class="md-code"><code>${esc(text)}</code></pre>`;
  }
  const out = [];
  const lines = String(text ?? "").split("\n");
  let i = 0;
  let list = null;              // "ul" | "ol" | null
  const closeList = () => {
    if (list) { out.push(`</${list}>`); list = null; }
  };
  while (i < lines.length) {
    const raw = lines[i];
    const line = esc(raw);
    if (/^\s*```/.test(raw)) {
      closeList();
      const buf = [];
      i += 1;
      while (i < lines.length && !/^\s*```/.test(lines[i])) {
        buf.push(esc(lines[i]));
        i += 1;
      }
      i += 1;
      out.push(`<pre class="md-code"><code>${buf.join("\n")}</code></pre>`);
      continue;
    }
    const heading = raw.match(/^(#{1,4})\s+(.*)$/);
    if (heading) {
      closeList();
      const level = heading[1].length;
      out.push(`<h${level + 1} class="md-h${level}">${
        inline(esc(heading[2]))}</h${level + 1}>`);
      i += 1;
      continue;
    }
    if (/^\s*([-*+])\s+/.test(raw)) {
      if (list !== "ul") { closeList(); out.push("<ul>"); list = "ul"; }
      out.push(`<li>${inline(line.replace(/^\s*[-*+]\s+/, ""))}</li>`);
      i += 1;
      continue;
    }
    if (/^\s*\d+\.\s+/.test(raw)) {
      if (list !== "ol") { closeList(); out.push("<ol>"); list = "ol"; }
      out.push(`<li>${inline(line.replace(/^\s*\d+\.\s+/, ""))}</li>`);
      i += 1;
      continue;
    }
    if (/^\s*>\s?/.test(raw)) {
      closeList();
      out.push(`<blockquote>${
        inline(line.replace(/^\s*&gt;\s?/, ""))}</blockquote>`);
      i += 1;
      continue;
    }
    if (/^\s*(-{3,}|\*{3,})\s*$/.test(raw)) {
      closeList();
      out.push("<hr>");
      i += 1;
      continue;
    }
    if (raw.trim() === "") {
      closeList();
      i += 1;
      continue;
    }
    closeList();
    const para = [line];
    while (i + 1 < lines.length && lines[i + 1].trim() !== ""
      && !/^(#{1,4}\s|\s*[-*+]\s|\s*\d+\.\s|\s*```|\s*>)/.test(lines[i + 1])) {
      i += 1;
      para.push(esc(lines[i]));
    }
    out.push(`<p>${inline(para.join(" "))}</p>`);
    i += 1;
  }
  closeList();
  return out.join("\n");
}
