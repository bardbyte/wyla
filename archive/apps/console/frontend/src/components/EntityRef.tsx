/** Every known graph object mentioned in an answer is a door.
 * EntityRef renders the token; RefText finds them (budget-capped,
 * answers only) and passes everything else through RichText. */

import { useEffect, useRef, useState } from "react";
import { ENTITY } from "../lib/copy";
import { useNav } from "../lib/nav";
import type { LexiconEntry } from "../lib/types";
import { useLexicon } from "../lib/useLexicon";
import { RichText } from "./ui";

const MAX_TOKENS_PER_BLOCK = 6;

export function EntityRef({ entry, label }: {
  entry: LexiconEntry; label: string;
}) {
  const nav = useNav();
  const [open, setOpen] = useState(false);
  const wrap = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (!wrap.current?.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    window.addEventListener("mousedown", onDown);
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("mousedown", onDown);
      window.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <span className="entity-wrap" ref={wrap}>
      <button type="button" className="entity-ref"
        onClick={() => setOpen((o) => !o)}>
        {label}
      </button>
      {open && (
        <span className="entity-menu" role="menu">
          <button type="button" role="menuitem" onClick={() => {
            setOpen(false);
            nav.openEvidence(entry.ref);
          }}>
            {ENTITY.evidence}
          </button>
          {entry.kind === "table" && (
            <button type="button" role="menuitem" onClick={() => {
              setOpen(false);
              nav.goToGraph(entry.name);
            }}>
              {ENTITY.explore}
            </button>
          )}
          <button type="button" role="menuitem" onClick={() => {
            setOpen(false);
            nav.askAbout(ENTITY.askPrefix + entry.name);
          }}>
            {ENTITY.ask}
          </button>
        </span>
      )}
    </span>
  );
}

/** RichText + linkification. Used ONLY on answer bodies — never user
 * messages, SQL, or the work log. Bold/code spans are tokenized first:
 * a code span whose text is exactly a known name becomes a door, any
 * other span passes through untouched — the regex never reaches inside
 * a span, so it can't strand markdown delimiters. */
export function RefText({ text }: { text: string }) {
  const lex = useLexicon();
  if (!lex?.regex) return <RichText text={text} />;

  let left = MAX_TOKENS_PER_BLOCK;
  const out: JSX.Element[] = [];
  let k = 0;
  for (const chunk of text.split(/(\*\*[^*]+\*\*|`[^`]+`)/)) {
    if (!chunk) continue;
    if (chunk.startsWith("`") && chunk.endsWith("`")) {
      const inner = chunk.slice(1, -1);
      const entry = left > 0
        ? lex.byName.get(inner.toLowerCase()) : undefined;
      if (entry) {
        out.push(<EntityRef key={k++} entry={entry} label={inner} />);
        left -= 1;
      } else {
        out.push(<code key={k++}>{inner}</code>);
      }
    } else if (chunk.startsWith("**")) {
      out.push(<RichText key={k++} text={chunk} />);
    } else {
      // plain segment: regex sweep with whatever budget remains
      let last = 0;
      lex.regex.lastIndex = 0;
      for (let m = lex.regex.exec(chunk); m !== null;
           m = lex.regex.exec(chunk)) {
        if (left <= 0) break;
        const entry = lex.byName.get(m[1].toLowerCase());
        if (!entry) continue;
        if (m.index > last) {
          out.push(<RichText key={k++} text={chunk.slice(last, m.index)} />);
        }
        out.push(<EntityRef key={k++} entry={entry} label={m[1]} />);
        last = m.index + m[1].length;
        left -= 1;
      }
      if (last < chunk.length) {
        out.push(<RichText key={k++} text={chunk.slice(last)} />);
      }
    }
  }
  return <>{out}</>;
}
