# Skill retrieval: a library with a card catalogue

A skill pack can be a book. A doctrine pack or a runtime knowledge
bundle runs to a few megabytes — around 650,000 tokens — which no
engine configured here reads whole, and which does not belong in a
prompt even where it would fit: a prompt is a briefing, not a library.

The mental model is a library. A pack within the whole-load ceiling is
a briefing and is pasted into the prompt verbatim, exactly as before.
A pack over the ceiling is shelved as a **library**: the model gets
the **card catalogue** (the table of contents — every heading path
with its chunk count and size) and the **pages that matched this
message** (the top chunks for the ask, ranked), under a per-engine
budget. When the answer may sit on a page it does not hold, it looks
it up (`skill_search`) and reads it (`skill_read`), and cites the
breadcrumb it read. Nothing is ever truncated in silence: what the
model holds is labeled as the catalogue and the pages it asked for,
never as the whole book.

The code: `sahs/loop/skill_index.py` (the chunker and the index),
`sahs/loop/skills.py` (`split_by_ceiling`, `render_searchable_skills`),
`sahs/assistant/skills_loader.py` (`skill_context`: the per-turn split
and the fitted block), `sahs/assistant/kit.py` (the three tools),
`sahs/util/profiles.py` (the budgets), `scripts/skill_index_check.py`.

## Whole-load is the correctness path

A skill that fits loads whole, verbatim, byte-identical to what the
prompt carried before any of this existed (a pinned test holds the
rendering to a golden string). What "fits" means is now per model:

- **The engine's whole-load budget** comes from its context window in
  `sahs/util/profiles.py` (`context_tokens` on the engine map): the
  window in tokens × 4 characters a token × `WHOLE_LOAD_SHARE` (0.5),
  the rest left for the digest, the conversation, tool results and the
  answer. Every Gemini 3.x engine in the table has a 1,048,576-token
  window, so its whole-load budget is 2,097,152 characters and a
  650,000-character bundle loads whole. An engine the table does not
  know is assumed to have 131,072 tokens (262,144 characters) — small
  on purpose.
- **`SAHS_MAX_SKILL_CHARS` is the global ceiling on top** (default
  4,000 characters, the briefing size). The turn's limit is the
  smaller of the two (`skills_loader.whole_load_limit`). To let the
  big engines take a bundle whole, set `SAHS_MAX_SKILL_CHARS=650000`;
  the small engines still fall to the library or refuse, below.
- `SAHS_MAX_LOADED_SKILLS` still caps how many packs one chat loads at
  once, whole or library.

The v1 navigator (`sahs.ask`), which has no lookup tools, keeps the
old refusal (`SkillTooLarge`).

## The skill's own word: frontmatter policy

Over the whole-load limit, the library path is allowed only when the
skill's frontmatter permits it. Two keys, read from the leading
`---` block (stdlib parse, no YAML dependency; the block stays part of
the text that loads):

```markdown
---
description: Settlement windows and the reconciliation runs
aliases: [settle, recon, late close]
runtime_loading: sectioned        # or full_file_required
truncation_allowed: true          # or false
---
```

- `runtime_loading: sectioned` (the default when absent) allows the
  library path.
- `runtime_loading: full_file_required`, or `truncation_allowed:
  false`, means the skill must load whole or not at all. Over the
  turn's whole-load limit it **fails closed**: refused by name with
  the reason — `skill 'x' needs N chars whole (runtime_loading:
  full_file_required), this model's (gemini-…) whole-load budget is M;
  switch to a model with a larger window or mark the skill
  sectioned` — never partially loaded, never silently sent to search.

Where the refusal lands: at pin time (the session picker, a slash
command, a project's pinned packs) the global ceiling decides, since
no engine can exceed it (`load_packs` raises `SkillRefused`, a
`SkillTooLarge`, and the pickers show the reason). At turn time the
engine's budget decides: the turn ends before any model call with an
`error` event (`code: skill_refused`, the reason, the ways out) and a
`turn_done` of status `error`. `load_skill` on such a pack refuses the
same way; the library tools never serve a page of it.

## The loader record

Every turn emits one `skills_loaded` event right after
`turn_started`, the loader's record of what it did:

| field | meaning |
|---|---|
| `skills[]` | one row per pinned or slash-loaded skill: `skill_name`, `source_chars` (the file), `rendered_chars` (what the renderer made of it: the whole text, or the contents plus every passage the search found), `sent_chars` (what reached the prompt after the budget), `mode` (`whole` / `sectioned` / `refused`), `truncated` (always false for whole and refused) |
| `skills_loaded` | the names that loaded (whole or sectioned) |
| `aggregate_skill_chars` | the sum of `sent_chars` this turn |
| `whole_load_limit` | this turn's whole-load limit in characters |
| `retrieval_budget`, `retrieval_chunks` | the library fold at this depth |

The chat page subscribes to it (the record lives in the transcript for
Operate); a refusal also shows as the error card that follows.

## The routing hint: which skill, before any load

The same sqlite file carries a second FTS5 table of every pack on the
shelf, whatever its size: its frontmatter `description`, its
`aliases` (a list, when present), its title and its section headings,
keyed by the content hash like the pages (an unchanged pack costs a
hash; nothing is chunked for this). `SkillIndex.rank_skills(question,
k)` ranks the shelf for a question — aliases weighted 3, description
and title 2, headings 1, any term matching, absent terms dropped —
and the turn lists the likely packs first on the "Skills on demand"
shelf, marked `(likely)`, with one sentence saying it is a hint. The
model still decides what to load. With no hint the shelf is
byte-identical to before. `scripts/skill_index_check.py` prints the
same ranking as `Likely skills`.

Ten questions over five small frontmatter'd packs plus the synthetic
bundle rank the right pack first in `tests/test_skill_index.py`; a
real turn shows `kyc-vocab … (likely)` first in `tests/test_v3_skills.py`.

## Chunking rules

The chunker (`chunk_markdown`) reads the Markdown structure and
nothing else. It is deterministic: the same text always chunks the
same way.

- **Headings first.** Every heading opens a section; text before the
  first heading is the `(preamble)`. Each section gets an id `s<N>` in
  document order and a heading path `H1 > H2 > H3`, which every chunk
  carries as its breadcrumb.
- **Then paragraph boundaries.** Within a section, blocks (paragraphs,
  tables, fenced code) pack greedily into chunks of about 1,200 tokens
  (4,800 characters at the harness's 4-chars-a-token estimate), with
  about 150 tokens (600 characters) of overlap taken as whole trailing
  blocks from the previous chunk. Chunk ids are `c<N>`, document order.
- **Never split a table row or a fenced code block.** A fence is
  atomic whatever its size. A table stays whole when it fits; over the
  target it splits between rows, never inside one. A paragraph over
  the target splits on line boundaries, and only a single line over
  the target is cut hard.
- **Offsets are exact.** Every chunk is a slice of the original text
  (`start`, `end`), so a read is always the pack's own words.

## The index

One sqlite file at `<graph>/runs/skill_index.sqlite3` — derived data,
safe to delete; it rebuilds on the next load. It holds a `skills`
table (name, content hash, chunker version, sizes, the text), a
`sections` table (the table of contents), a `chunks` table (the
pages), and an FTS5 table over the chunks ranked by BM25 (the
section's own title weighted 2, the inherited breadcrumb 0.5, the
text 1).

It is keyed by (skill name, content hash, chunker version), built
lazily on first use, and incremental: an unchanged pack costs a hash
and nothing more; a changed pack re-indexes on its next load, and
only itself. The index takes any object with `name`, `title` and
`text` (a file-backed pack, or a store-backed row: an `updated`,
`version` or `mtime` is recorded, but the content hash decides).

Queries are lexical and deterministic. The ask's words, minus a small
stopword list, each expanded to the term plus a few folded inflections
(plural, -ing, -ed) and an FTS5 prefix on the stem; a term that occurs
nowhere in the searched packs is dropped rather than vetoing the
match; every remaining term is required first, and any-term matches
fill what is left. `Embedder` in `skill_index.py` is the seam for an
embedding-based reranker later — it is `None` by default, and nothing
calls an embedding service.

## Per-model budgets

Two budgets per engine, both in `sahs/util/profiles.py` next to the
engine maps. The **whole-load budget** (`whole_load_chars`, from
`context_tokens`) says what loads whole; the **library budget**
(`skill_budget`) bounds the prompt block for library packs (catalogue
plus pages), and the depth dial folds onto it through
`RETRIEVAL_FOLD`, keyed by the level the engine actually runs at (so
Quick on 3.5 Flash, which folds to medium, gets medium's share).

| Engine | window (tokens) | whole-load budget (chars) | `skill_budget` (chars) |
|---|---|---|---|
| Gemini 3.1 Pro | 1,048,576 | 2,097,152 | 120,000 |
| Gemini 3.7 Flash | 1,048,576 | 2,097,152 | 80,000 |
| Gemini 3.5 Flash | 1,048,576 | 2,097,152 | 80,000 |
| Gemini 3.1 Flash Lite | 1,048,576 | 2,097,152 | 40,000 |
| an unnamed Gemini 3 model | 1,048,576 | 2,097,152 | 80,000 |
| Gemini 2.5 | 1,048,576 | 2,097,152 | 40,000 |
| unknown | 131,072 | 262,144 | 32,000 |

The turn's whole-load limit is `min(SAHS_MAX_SKILL_CHARS, whole-load
budget)`; the scripted test engine is "unknown".

| Level the engine runs at | passages in the prompt | share of the budget |
|---|---|---|
| minimal | 2 | 25% |
| low (Quick) | 3 | 40% |
| medium (Standard) | 5 | 60% |
| high (Deep, Extra deep) | 8 | 100% |

So Standard on 3.7 Flash gives at most 5 passages within 48,000
characters; Deep on 3.1 Pro gives at most 8 within 120,000. The table
of contents takes up to 35% of the turn's share and folds to shallower
headings when the full list does not fit (a line says how many were
left out and that `skill_toc` lists them); the pages fill the rest in
rank order, each whole or not at all. There is no environment knob:
the table is the place to tune an engine.

## The tools

Declared to the model whenever a pack over the ceiling is in reach
(loaded this turn, or on the shelf). Every result carries the
breadcrumb and the character offsets, so the answer can cite the
section.

- `skill_toc(name, under?)` — the catalogue: every heading path with
  its chunk count and size. `under` opens one branch when the list
  folded.
- `skill_search(query, skill?, k=8)` — ranked passages with breadcrumb,
  offsets, score and snippet. Lexical: use the pack's own words, and
  try a second phrasing before concluding it is silent.
- `skill_read(name, section, max_chars=6000, offset?)` — one section
  or passage as written: a heading, a heading path, an `s<N>` from the
  contents or a `c<N>` from a search. Longer sections come back in
  pages with the next offset named.

`load_skill(name)` on an oversized pack no longer refuses: it returns
the catalogue and says the pack is loaded as a library.

## Checking retrieval on your own packs

```bash
python scripts/skill_index_check.py graph/skills "how is spend reconciled"
python scripts/skill_index_check.py graph/skills "fiscal quarter" --skill fiscal-notes --toc-depth 3
python scripts/skill_index_check.py /path/to/packs "settlement window" --k 5 --memory
```

It prints the index path, each pack's size in characters, sections
and chunks with whether it was indexed or unchanged, the contents to
the depth asked, and the top hits with score, breadcrumb, offsets and
snippet — the same ranking the chat uses. Exit 1 means no passage
matched. `--memory` writes nothing.

## What the tests prove

`tests/test_skill_index.py` builds a synthetic 2.5 MB pack (hundreds of
sections under a dozen parts, each owning two invented terms, with
near-duplicate twins around other terms, settings tables and fenced
SQL) and asserts: twenty asks phrased unlike the headings rank their
section first (and one ask per section across the whole pack, the
number the test prints); no chunk splits a table row or a fence;
re-indexing an unchanged pack calls the chunker zero times; a changed
pack re-indexes only itself. `tests/test_v3_skills.py` drives the
real loop with the scripted agent: a pinned pack over the ceiling
reaches the model as its contents and matched passages within the
engine's budget, Quick holds fewer passages than Deep, the prefix
before the skills section is unchanged, `skill_search` then
`skill_read` return breadcrumbs and offsets, and the `skills_loaded`
record carries the row. The same file proves the frontmatter parse
and its defaults; the whole-load budget per engine (a 620,000-character
bundle loads whole on 3.7 Flash with `SAHS_MAX_SKILL_CHARS=650000` and
sectioned on the unknown engine); a `full_file_required` pack over
the budget refused with the exact reason, at pin time by the global
ceiling and at turn time by the engine's budget with no model call;
and the shelf listing the likely pack first. The routing hint ranks
the right pack first for ten questions in `tests/test_skill_index.py`.
