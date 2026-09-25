# Report: skill retrieval (workstream "skill retrieval")

Branch: `worktree-agent-a088816e83c8240ea`
Worktree: `/home/user/wyla/.claude/worktrees/agent-a088816e83c8240ea`
Commits on the branch (nothing pushed, no PR):

- `231218a` — Skill retrieval: a pack over the ceiling loads as a
  searchable library, not a refusal (the original brief).
- `b74ad72` — Skill loading policy: whole-load per engine window,
  frontmatter fail-closed, the loader record, and a routing hint (the
  five-point scope adjustment).
- this report, as a third commit.

## What changed, file by file

All paths relative to `synapse-agentic-harness-system/` unless noted.

- **`sahs/loop/skill_index.py` (new).** The chunker and the index.
  `chunk_markdown(text)` → (sections, chunks): headings open sections
  (`s<N>`, heading path `H1 > H2 > H3` on every chunk); blocks pack
  greedily to ~1,200 tokens (4,800 chars at 4 chars/token) with ~150
  tokens (600 chars) of overlap taken as whole trailing blocks; a fence
  is atomic at any size; a table stays whole when it fits and otherwise
  splits between rows, never inside one; every chunk is an exact slice
  (`start`, `end`) of the original. `SkillIndex(path)` over sqlite at
  `<graph>/runs/skill_index.sqlite3` (`index_path`, `open_index`;
  memory when there is no graph root): tables `skills` (name, content
  hash, chunker version, sizes, text), `sections` (the table of
  contents), `chunks`, FTS5 `chunks_fts` (section title weight 2,
  inherited breadcrumb 0.5, text 1, BM25), plus `routing` /
  `routing_fts` for the routing hint (title 2, description 2, aliases
  3, headings 1). `ensure(sources)` keys on (name, sha256,
  `CHUNKER_VERSION`) and returns indexed / reindexed / unchanged per
  skill; `ensure_routing(sources)` does the same for the routing rows
  without chunking. Sources are any objects with `name`, `title`,
  `text` (optional `updated` / `version` / `mtime` recorded), so
  store-backed packs index the same way. Queries: `toc(skill, under?,
  max_level?)`, `search(query, skills=None, k=8)` → `Hit` rows with
  score, breadcrumb, snippet, offsets; `read(skill, section_or_chunk,
  max_chars, offset)` with paging; `resolve` (chunk id, section id,
  heading path, heading, or a unique fragment; ambiguity lists
  candidates); `rank_skills(question, k)` → likely skills with score
  and a why-snippet. Expansion is lexical and deterministic: stopwords
  dropped, `stem_variants` folds plural/-ing/-ed, an FTS5 prefix on the
  stem, terms occurring nowhere in the searched packs dropped before
  matching, all-terms AND first, any-term OR to fill. `Embedder`
  protocol is the marked seam (default None; a blended cosine rerank
  runs only when one is passed; nothing calls an embedding service).
- **`sahs/loop/skills.py`.** Docstring pin updated (whole, or as a
  library; v1 keeps `SkillTooLarge`). Added `is_searchable`,
  `split_by_ceiling`, `SearchableSkill` and `render_searchable_skills`
  (the prompt block; `header=False` renders a pack's own share for the
  loader record). New: `frontmatter(text)` (stdlib parse of the leading
  `---` block: scalars, bools, inline and block lists),
  `strip_frontmatter`, `LoadPolicy` / `policy_of` (`runtime_loading:
  sectioned | full_file_required`, default sectioned;
  `truncation_allowed`, default true; `description`; `aliases`),
  `parse_skill(name, raw)` (title after the frontmatter, description
  from `description:` when present; the text stays whole, frontmatter
  included), and `SkillRefused(SkillTooLarge)` with the required
  message. `render_skills`, `check_size`, `SkillTooLarge`,
  `load_skills` and the env knobs are untouched.
- **`sahs/assistant/skills_loader.py`** (loading/budget functions
  only). `load_packs` no longer raises for a sectioned oversized pack;
  it raises `SkillRefused` for a `full_file_required` /
  `truncation_allowed: false` pack over the global ceiling (the
  pickers already catch `SkillTooLarge`). New `whole_load_limit(model)`
  = min(`SAHS_MAX_SKILL_CHARS`, the engine's `whole_load_chars`);
  `split_by_policy(packs, limit, model)` (raises `SkillRefused` naming
  the engine's budget); `loader_record(pack, mode, rendered, sent)`;
  `SkillContext` now carries `limit`, `records`, `likely`,
  `aggregate_skill_chars` and `event()` (the `skills_loaded` fields);
  `skill_context(graph_root, packs, query, model_name, stop, shelf=…)`
  splits at the per-model limit, attaches the partial context to the
  refusal as `.context`, builds the fitted block (TOC ≤ 35% of the
  budget via `toc_lines`, passages in rank order each whole or not at
  all) and, with `shelf`, ranks the shelf (`rank_shelf`).
  `render_skill_index(packs, exclude, likely=())` lists the likely
  packs first, marked `(likely)`, with one sentence saying it is a
  hint; byte-identical without it. Module docstring updated.
- **`sahs/assistant/kit.py`.** `build_kit(..., retriever=None,
  searchable=None, skill_limit=None, model_name="")`. `load_skill` on a
  pack over this turn's whole-load limit loads it as a library
  (contents returned, recorded as loaded) unless its frontmatter
  requires the whole file, which returns the refusal as the error.
  Three tools in the kit's description style — `skill_toc(name,
  under?)`, `skill_search(query, skill?, k?)` (k ≤ 20),
  `skill_read(name, section, max_chars=6000, offset?)` — declared
  whenever a sectioned-allowed pack over the limit is loaded this turn
  or on the shelf; every result carries breadcrumb and offsets; a
  `full_file_required` pack never serves a page.
- **`sahs/util/profiles.py`.** `skill_budget` (chars, the library
  budget) and `context_tokens` on `ModelProfile` next to the engine
  maps; `whole_load_chars` = context × 4 × `WHOLE_LOAD_SHARE` (0.5).
  All Gemini 3.x rows and the Gemini 2.5 family: 1,048,576 tokens →
  2,097,152 chars whole-load; unknown engine: 131,072 → 262,144.
  Library budgets: 3.1 Pro 120,000; 3.7 Flash 80,000; 3.5 Flash
  80,000; Flash Lite 40,000; unnamed Gemini 3 80,000; Gemini 2.5
  40,000; unknown 32,000. `RETRIEVAL_FOLD` by the level the engine runs
  at: minimal (2 passages, 25%), low (3, 40%), medium (5, 60%),
  high/max (8, 100%). `ModelProfile.retrieval(stop)`,
  `skill_retrieval_for`, `whole_load_chars_for`; `as_row` carries
  `skill_budget`, `context_tokens`, `whole_load_chars`. No env knob
  added.
- **`sahs/assistant/loop.py`** (localized). `system_prompt` gains
  `retrieval`, `library` and `likely`: the block rides inside the
  existing `<skills>` section between the whole skills and the shelf;
  library packs are not re-offered; likely packs are listed first.
  `run_assistant_turn` calls `skill_context(..., shelf=…)` once; on
  `SkillRefused` it emits the `skills_loaded` record (mode refused),
  an `error` (`code: skill_refused`, the reason, next actions) and
  `turn_done` status `error`, and returns before any model call;
  otherwise it emits `skills_loaded` and passes the split to the kit
  and the prompt. `tool_input` keys and `summarize` rows for the three
  tools. The prefix before `<skills>` is unchanged (the pinned prefix
  tests in `test_v3_loop.py` and `test_model_catalog.py` stay green; a
  new test asserts the prefix is identical at Quick and Deep).
- **`sahs/assistant/events.py`.** `skills_loaded` added to
  `ASSISTANT_EVENTS` after `turn_started` (the chat surface test
  enumerates it).
- **`apps/synapse_admin/frontend/js/pages/chat.js`.** A
  `case "skills_loaded"` arm (the record lives in the transcript; a
  refusal already shows as the error card) and the SSE subscription
  list, so the surface test's two checks hold.
- **`scripts/skill_index_check.py` (new).** `python scripts/
  skill_index_check.py <skills dir> "<query>" [--k N] [--skill NAME]
  [--toc-depth N] [--index PATH] [--memory]`: every `*.md` under the
  dir (nested folders, name = relative path), index at
  `<dir>/../runs/skill_index.sqlite3` by default; prints index path,
  per-pack chars/sections/chunks/status, the contents to the depth
  asked, the `Likely skills` routing line, and the top hits with score,
  breadcrumb, offsets, snippet. Exit 1 when nothing matched.
- **`tests/synthetic_skill.py` (new helper).** `build_pack(target_chars
  =2_500_000, seed=7)` writes a deterministic 2,565,177-char pack: 13
  parts, 564 ground-truth sections (432 originals at H3, H4 edge-case
  subsections, 30+ "(legacy)" twins with the same prose around other
  terms), settings tables every third section, fenced SQL every
  fourth, cross-reference noise lines; every topic sentence appears
  once per section so a query's right section is exact. `queries
  (truths, 20)` gives twenty asks phrased unlike the headings.
- **`tests/test_skill_index.py` (new).** Chunk structure (breadcrumbs
  match ground truth, slices on line boundaries, no fence or row split,
  overlap ≤ 600 within a section, determinism); an over-target fence
  and table stay intact; query expansion strings; the accuracy test;
  table cells and code identifiers found; absent words do not veto;
  zero work on an unchanged pack (chunker monkeypatched to raise); a
  changed pack re-indexes only itself and the file rebuilds when
  deleted; toc/read breadcrumbs, offsets, paging, ambiguity; the
  embedder seam; the check script's output (subprocess); the routing
  hint on ten questions over five frontmatter'd packs plus the bundle,
  keyed by hash, changed pack re-routes alone.
- **`tests/test_v3_skills.py`.** The two refusal tests replaced by
  the library policy tests (runtime pins as a library; the tool loads
  as a library and the library tools work; a small pack is told it is
  not a library pack; raising the ceiling makes it whole again). Added:
  the byte-identical whole-load pin; the loop test (Quick and Deep
  turns with the scripted agent calling `skill_search` then
  `skill_read`: block ≤ the fold's budget, right section first, Quick <
  Deep, prefix identical, results carry breadcrumb/offsets/paging, the
  `skills_loaded` record with its fields); frontmatter parse and
  defaults; the per-engine whole-load budget and the global ceiling on
  top; a 620K bundle whole on 3.7 Flash with `SAHS_MAX_SKILL_CHARS=
  650000` and sectioned on the unknown engine; a `full_file_required`
  pack over the budget refused with the exact message (through
  `skill_context`, `set_skills`, `load_skill`, and a real turn that
  never calls the model and ends with `error`/`skill_refused`); the
  shelf listing the likely pack first, and byte-identical without a
  hint.
- **Docs.** `docs/skill-retrieval.md` (new: the library mental model,
  whole-load as the correctness path with the per-engine window,
  frontmatter policy and fail-closed refusal, the loader record, the
  routing hint, chunking rules, the index, the budget tables, the
  tools, the check script, what the tests prove);
  `docs/model-playbook.md` (engine table gains window → whole-load and
  library budgets, a knob row for `SAHS_MAX_SKILL_CHARS`, the "not here
  yet" skills bullet replaced with the embedding-rerank seam and a
  link); `.env.example` comment (the ceiling is the global whole-load
  ceiling on top of each engine's window; sectioned packs become a
  library, full-file packs are refused; the index path);
  `apps/synapse_admin/README.md` skills paragraph links to the doc.

## Tests and exit codes

- `python -m pytest -q synapse-agentic-harness-system/tests -p no:cacheprovider`
  (from inside `synapse-agentic-harness-system`) → exit 0, all green,
  run after every code change (last run on the final code).
- `PYTHONPATH=synapse-agentic-harness-system python -m pytest -q
  apps/synapse_admin/tests -p no:cacheprovider` from the repo root →
  145 passed, 2 skipped, exit 0 (last run on the final code; it
  includes the chat surface test that enumerates `ASSISTANT_EVENTS`).
- Focused: `test_v3_skills.py` + `test_skill_index.py` → 29 passed,
  exit 0.

## Accuracy numbers (synthetic 2.5 MB pack)

- Pack: 2,565,177 characters, 577 chunker sections, 1,022 chunks (max
  4,797 chars, mean ~2,500); chunking 0.04 s, indexing 0.06 s; a second
  `ensure` 0.004 s and zero chunker calls.
- hits@1 = **20/20** on the twenty rephrased asks (the test prints
  `[skill retrieval] hits@1 20/20 on the rephrased asks; 432/432 across
  every section (100.0%)`).
- hits@1 = **432/432 (100%)** with one ask per original section
  (rotating eight templates phrased from the body, not the heading);
  the test asserts ≥ 97%.
- Twins (same prose, other terms) never outrank the original (20
  checked). Table cells and code identifiers resolve to their section.
- Routing hint: **10/10** questions rank the right pack first over
  five frontmatter'd packs plus the 2.5 MB bundle; a bundle heading
  routes to the bundle; unknown words route nothing.
- Search cost ~3–5 ms per query on the 1,022-chunk index.
- Two ranking changes were made after measurement and are in the
  code: the section's own title is weighted above the inherited
  breadcrumb (else every subsection outranked its parent on the
  parent's words), and a query term that occurs nowhere in the searched
  packs is dropped before the strict match (a word like "versus"
  absent from the pack caused all twelve residual misses; OR-only
  ranking scored 385/432 by comparison, so the strict-then-fill order
  stays).

## Not finished / caveats

- The `Embedder` seam is implemented (blend of normalized BM25 and
  cosine over the lexical candidates, tested with a fake) but no
  embedding service is wired, as specified.
- Token counts are a 4-chars-a-token estimate throughout, not a
  tokenizer; `context_tokens` per engine are the model pages' 1M
  window for every Gemini 3.x row (no probe distinguishes a smaller
  one yet), and the unknown-engine default of 131,072 is the
  conservative guess the design asked for. No env override for the
  window was added (the brief's "only if unavoidable"); the table is
  the place to tune an engine.
- `skill_search` without `skill` searches the packs loaded this turn
  (pinned, slash-loaded, or `load_skill`-ed); it does not search the
  whole shelf unasked — the error names the library packs on the
  shelf. The routing hint covers the whole shelf instead.
- The `skills_loaded` record covers pinned, project and slash-loaded
  packs (what the turn starts with); a pack the model pulls mid-turn
  with `load_skill` shows in `turn_done.skills_loaded` as before, not
  in the turn-start record.
- The per-model refusal at turn time is an `error` event plus
  `turn_done` status `error`, mirroring the model-unavailable path; the
  pin-time refusal reaches the pickers through the existing
  `SkillTooLarge` catches in `runtime.set_skills` and
  `apps/synapse_admin/backend/chat.py` (subclassed, so neither file
  changed). The v1 navigator (`sahs/ask`) keeps the old refusal on
  purpose: it has no lookup tools.
- The `tool_input`/`summarize` additions in `loop.py` are a few lines
  beyond the prompt section and the turn wiring; localized.

## Files outside my ownership touched

- `sahs/assistant/events.py` (one tuple entry) and
  `apps/synapse_admin/frontend/js/pages/chat.js` (one `case` arm and
  one name in the SSE subscription list) — both named by the scope
  adjustment for the `skills_loaded` event.
- `apps/synapse_admin/README.md` (the skills paragraph link) and
  `.env.example` were named in the brief.
- `sahs/assistant/runtime.py` was not touched.

## No refusals

Branch: `worktree-agent-a67c6daaf6a665657`
Worktree: `/home/user/wyla/.claude/worktrees/agent-a67c6daaf6a665657`
(branched from `claude/production-ready` at `44a268e`; nothing pushed,
no PR). Commits: `e500e74` — the change; this section as a second
commit.

The repo owner's decision: there is no refusal anywhere in skill
loading. Every verbatim of every skill is always usable by the
harness. "Never silently truncate" stays true; the answer to "it does
not fit whole" is "load it as a library and say so", never "refuse
the turn". What that meant, file by file (paths relative to
`synapse-agentic-harness-system/` unless noted):

- **`sahs/loop/skills.py`.** `SkillTooLarge`, `SkillRefused` and
  `check_size` deleted. `Skill` gains `error: str = ""`; `_parse`
  turns an unreadable or non-UTF-8 file into a listed skill with an
  empty text and the reason (never decoded with substitutions), and
  `SkillUnreadable` / `check_readable` are the one refusal left —
  broken input, never size. `load_skills` returns every named skill
  whole (no size check; the prompt builder splits at the limit).
  `SearchableSkill` gains `preferred` and `reason`;
  `PREFERRED_WHOLE` / `PREFERRED_SECTIONED`; `library_reason(skill,
  limit, model_name, why)` states the sizes and names the limit that
  bound (the ceiling when `SAHS_MAX_SKILL_CHARS` is it, else "this
  model's (name) whole-load limit", "unnamed engine" under a scripted
  model). `render_searchable_skills` writes `mode: library ·
  preferred: <whole|sectioned> — <reason>` under every pack heading
  and takes `tools=False` for the navigator (same block, no lookup
  tool named). Docstring pin rewritten.
- **`sahs/assistant/skills_loader.py`.** `load_packs` returns every
  pack whole and raises only `SkillUnreadable`; `_packs` carries
  `error` into `Pack`. `split_by_policy` is a plain size split;
  `preference_of(pack)` → (preferred, why); `MODE_WHOLE` /
  `MODE_LIBRARY`; `loader_record` gains `preferred`, `reason`, and
  takes `limit` / `model_name`; `SkillContext.event()` lists every
  record in `skills_loaded`; `skill_context` has no try/except,
  attaches the preference and the reason to every library view and
  record, and takes `tools`.
- **`sahs/assistant/kit.py`.** `load_skill` never refuses: a broken
  file returns an error by name; an oversized pack (whatever its
  frontmatter) returns the contents with `mode: library`,
  `preferred`, `reason`, and a note that says when the frontmatter
  preferred the whole file. `_library_pack` is size only;
  `_in_library` lost its full-file branch; the three tools serve
  every oversized pack.
- **`sahs/assistant/loop.py`.** The `SkillRefused` branch in
  `run_assistant_turn` is gone (no `error` / `skill_refused` ending);
  the import too.
- **`sahs/assistant/runtime.py`** `set_skills` and
  **`apps/synapse_admin/backend/chat.py`** `post_message` catch
  `SkillUnreadable` instead of `SkillTooLarge`. Audit of what can
  still raise at pin time: only `_parse` on a file that cannot be
  read or decoded; a large-but-valid pack cannot reach the catch.
- **`sahs/ask/runtime.py`.** `set_skills` catches `SkillUnreadable`;
  `start_turn` builds the static library with
  `skill_context(graph_root, loaded, text, self.model_name, "medium",
  tools=False)` and stores the whole skills, the block, the library
  names and the record on the session dict; `model_name` property
  (the `.env`'s `VERTEX_MODEL`; '' under a scripted factory).
- **`sahs/ask/loop.py`, `sahs/loop/loop.py`, `sahs/loop/prompt.py`.**
  `navigate_loop(skill_library=…, skills_library=…)` renders the
  block after the whole skills (`system_prompt(..., skill_library)`;
  byte-identical when empty) and `loop_started` lists the library
  packs in `skills` and `skills_library`.
- **`sahs/loop/skill_index.py`.** A module logger and a warn-once
  set. `SkillIndex.__init__` falls back to the process's in-memory
  index for the path (`_shared_memory_db`, one per path, never closed
  by `close()`) when the file cannot be opened; `ensure` and
  `ensure_routing` do the same when a write fails and re-run over
  every source in memory; `_chunk` wraps `chunk_markdown` and, when it
  throws, indexes the pack as one section and one exact chunk with
  `FALLBACK_CHUNKER_VERSION = 0` so the next load re-tries.
  `fallback` and `single_chunk` say which happened.
- **`tests/test_v3_skills.py`.** The size-ceiling refusal test is now
  its no-refusal twin (the bundle loads whole in the list and splits
  to the library side at the ceiling; whole again when the env raises
  it); the full-file fail-closed test is now "loads as a library and
  says so" (`mode: library`, `preferred: whole`, the reason with the
  sizes and the limit that bound, `truncation_allowed: false` the
  same, a sectioned pack recorded as `preferred: sectioned`); the
  pickers-and-tool refusal test is now the pin-and-run twin (a
  full-file pack over the global ceiling pins, `load_skill` returns
  the contents with the preference, `skill_read` serves a section,
  and the turn on the 262,144-character scripted engine runs to
  completion with the block, the three tools and the record). Added:
  the broken-file test (listed with the reason; refused by name in
  the v1 loader, `load_packs`, both pickers and the tool). The library
  pin test asserts the `mode: library · preferred: sectioned` line;
  the loop and the 650K tests read `mode == "library"` and
  `preferred`. The byte-identical whole-load pin and the prefix pins
  are untouched and green.
- **`tests/test_loop_prompt.py`.** The v1 navigator turn: a pinned
  150,000-character pack beside a small one renders the contents and
  the right passage first within the medium budget, names no lookup
  tool, pastes the small skill whole, reports both on `loop_started`,
  and the turn completes with no error.
- **`tests/test_skill_index.py`.** The two fallbacks: a corrupt file,
  an uncreatable path and a file that stops taking writes each fall
  to the in-memory index (one warning per path, the pack still
  found, the file left alone); a pack the chunker cannot read is
  indexed as one exact chunk, logged once, and re-chunked properly on
  the next load.
- **Docs.** `docs/skill-retrieval.md` (the policy section is "whole
  when it fits, a library when it does not, never a refusal"; the v1
  static-retrieval paragraph; the loader record's `mode` /
  `preferred` / `reason`; index resilience; "how to verify on the
  real packs" with `scripts/skill_index_check.py` against
  `MERIDIAN_SKILLS_DIR`; what the tests prove), `docs/model-playbook.md`
  (the budgets paragraph and the knob row), `.env.example` (the
  ceiling comment). No new environment variable.

Tests and exit codes (on the final code):

- `cd synapse-agentic-harness-system && set -o pipefail; python -m
  pytest -q tests -p no:cacheprovider 2>&1 | tail -20; echo
  exit=${PIPESTATUS[0]}` → all green, `exit=0` (the accuracy line
  still prints `hits@1 20/20 … 432/432`).
- `set -o pipefail; PYTHONPATH=synapse-agentic-harness-system python
  -m pytest -q apps/synapse_admin/tests -p no:cacheprovider 2>&1 |
  tail -20; echo exit=${PIPESTATUS[0]}` from the repo root → 153
  passed, 2 skipped, `exit=0`.
- Focused: `tests/test_v3_skills.py` + `tests/test_skill_index.py` +
  `tests/test_loop_prompt.py` → 44 passed, `exit=0`.
