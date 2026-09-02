# Synapse v2 — A Claude-class assistant over Meridian

> Landed verbatim as the governing design (the author's words below,
> unedited), superseding the Agent Loop v1 turn pipeline. Everything
> underneath survives; the front pipeline is replaced by the
> assistant. Build order is §13; `sahs/assistant/` is the
> implementation.

The pivot: from a governed pipeline with a model inside, to a general assistant with governed tools around it. September 1, 2026.

## 0 · What we got wrong, in one paragraph
We optimized for governance first and built the harness as a pipeline — classify, apply, resolve, contract, verify, render — with the model confined to composing SQL inside a plan schema. That produces trustworthy single-metric answers and a product that feels like a query console. Claude's harness proves the other order works: a strong general model, a thin loop, tools that are truthful, artifacts the user can see and keep, skills loaded on demand, memory across chats — and verification as something the model does with tools plus a few schema-enforced rendering rules. Governance becomes a property of the tools and the artifacts, not a gate in front of the conversation. We keep everything we built underneath (graph, build, tools, verifier, budgets, store, events) and replace the front pipeline with an assistant.

## 1 · The shape

```
system prompt: who it is · how it reasons · the Meridian world · rendering & disclosure rules
        │
   assistant loop (model drives; thin harness: budgets, compaction, streaming, artifacts)
        │
   tools ── Meridian toolkit (find truth) · Analysis toolkit (compute) · Artifact toolkit (show)
        │   · Verification toolkit (prove) · Memory & session tools · "magic" tools
        │
   skills (on-demand SKILL.md packs: analysis playbooks, domain rules, dashboard design, exec summary)
        │
   UI: Claude-shaped — sidebar/projects/recents, chat stream with tool activity, artifact panel
```

The model is a general reasoner first. It answers "what's a good way to think about merchant churn" with no tools; it answers "show me approval rate by segment" with the Meridian toolkit; it answers "build me a Q2 dashboard for the CFO" with all five toolkits and two skills. Same loop, same voice.

## 2 · The loop (thin, Claude-shaped)

```python
while budget.ok():
    step = model(context)                      # reasoning + (tool_call | text | artifact | done)
    if step.tool: context.append(compact(run(step)))   # results → artifacts, refs in context
    if step.artifact: panel.render(step)               # streams to the artifact panel
    if step.text: stream(step)                         # streams to the chat
    if step.done: break
```

No classify step, no pre-resolve, no contract gate. The harness contributes: budgets and breakers in code, compaction (tool results → artifact refs; conversation summary on pressure), streaming, prompt caching of the system prompt + tool schemas + world digest, the artifact panel, session/project persistence, and three rendering rules enforced by schema (§6). Everything else is the model reasoning with good tools.

## 3 · The Meridian toolkit (find the truth)
Same eight-plus tools, now framed as capabilities, with descriptions written for a smart colleague: `search_semantics` (ranked meaning) · `grep_cards` (exact text across all cards) · `list_tables` · `read_card` · `resolve` (the deterministic binder — still refuses to guess; the model decides when to call it) · `sample_values` · `get_join_paths` (tiered) · `get_definition_line` · `subgraph(ids)` (the nodes/edges behind a set of facts, for the constellation and for citation). Governance rides in the results: every row carries status, witnesses, tier, provenance. The model reads "pending ×412 · agreement 1" and reasons about it like an analyst would.

## 4 · The Analysis toolkit (compute) — this is what makes it capable

* `run_sql(sql, mode=dry_run|snapshot|live, limit)` — validated, cost-gated, ACL-enforced; errors teach. Live mode remains policy-gated (persona floor + steward-enabled tables).
* `python(code)` — Claude's analysis tool: a sandbox with pandas/numpy/plotting and the `meridian` SDK preloaded, query results available as DataFrames, files written to the session workspace. This is where decomposition, variance analysis, cohorts, forecasts, anomaly checks, and "why did it move" actually happen. Snapshot by default; no credentials in the sandbox; budgeted.
* `check(kind, …)` — the verification primitives as callable tools the model uses mid-analysis: `reconcile(composed, certified_metric)`, `part_whole(breakdown, total)`, `fanout(join)`, `coverage(query)`, `crosscheck(a, b)`. Results are facts the model can cite.
* `compare(plan_a, plan_b)` — same measure across two periods/cohorts/definitions, returned as an aligned frame (the "same for Canada, side by side" primitive).
* `whatif(plan, patch)` — re-run a query with one slot changed; the cheapest magic there is.

## 5 · The Artifact toolkit (show) — Claude's artifact panel, for analytics
Artifacts are standalone, versioned, side-panel outputs the user keeps. Types:

* `chart` (line/bar/scatter/area; Amex-branded theme; data + provenance embedded)
* `dashboard` — a multi-panel React/HTML artifact: KPI tiles with meridian lines, charts, a filter bar wired to `whatif`, a notes column; iterates in place ("make the second chart a cohort view"); every tile carries its definition status
* `diagram` — Mermaid/SVG: lineage, join topology, funnel/flow, decision trees; plus `constellation(subgraph)` — the cosmos view of what the answer used
* `table` — sortable, with provenance per column
* `document` — executive summary / memo / methodology note (markdown → docx/pdf)
* `notebook` — the analysis script + outputs, watermarked EXPLORATORY until promoted
* `export(artifact, format)` — PNG/SVG for charts, CSV/XLSX for tables, PDF/DOCX for documents, PPTX for dashboards (branded deck, one panel per slide, meridian lines in the notes), HTML bundle for dashboards; all downloads carry build id + provenance footer. Artifacts are governed output the same way A2UI components were: any artifact showing a number carries its definition status and meridian line in-schema, or renders the EXPLORATORY watermark.

## 6 · Governance as rendering rules (the only "gates" left)

1. Any number shown to a user carries its status + meridian line (certified / pending / composed / exploratory), enforced by the artifact and message schemas — the model can't omit it because the renderer refuses.
2. Composed numbers require a passing `reconcile` or `crosscheck` fact to lose the EXPLORATORY watermark — the harness checks that the check result exists in the trajectory before rendering a governed number.
3. Nothing writes to truth except the clerk; variants and compositions become candidates + ReviewItems exactly as before (the flywheel is unchanged). Plus budgets/breakers in code, the persona floor filtering what tools may return, and the fresh-context verifier — now a `verify_answer` tool the model calls before it renders a governed number (and the harness runs automatically on any answer marked "publish"). The plan schema survives as the working note the SQL-over-Meridian skill recommends, not as a gate.

## 7 · Skills (on-demand, progressive disclosure — exactly Claude's model)
Loaded when relevant, ≤2K tokens each, versioned, witnessed:

* meridian-sql — the search doctrine: resolve first, grep for exact tokens, read a card before using it, sample before filtering, join paths before joining, prefer certified → say so for pending → reconcile anything composed; keep a working plan note.
* analysis-playbooks — decomposition (rate vs mix), variance bridges, cohort/retention, funnel, seasonality/forecast basics, anomaly triage; each with the checks it must run.
* domain packs — TLS rulebook, GMNS conventions, CFR definitions (the existing skill_contract witnesses).
* dashboard-design — Amex brand tokens, tile grammar, chart choice rules, "one question per panel," disclosure placement.
* executive-summary — the memo shape leaders read: headline, three drivers, one risk, definitions footer.
* user-added skills enter through the E14 door: usable immediately as "unreviewed," governed after review.

## 8 · Memory, projects, chats (Claude's organization)

* Sidebar: Projects (a folder with its own context + pinned skills + artifacts), Recents (auto-titled), Starred, search across chats, rename/archive, New chat always one tap.
* Project memory: persistent instructions + files + the artifacts produced there; a CFO project carries its dashboards and its persona floor.
* User memory (scoped, statused, disclosed — the rules we pinned): preferences, disambiguation choices ("by Canada you mean merchant country"), never variants (those are candidates in the graph).
* Chat memory: full message + tool + artifact history; compaction summary on pressure; the working plan note persists.

## 9 · The "magic" — what makes people keep coming back
Grounded delight, each one a tool or a rendering habit:

* Follow-up chips after every answer ("break down by channel", "same for last year", "why?") generated from the subgraph and the playbooks — one tap continues the analysis.
* "Why?" as a first-class move: triggers the decomposition playbook on the last number.
* `whatif` sliders on dashboards: change a filter, every tile re-runs on the snapshot.
* "Explain this to my VP": rewrites the current answer with the executive-summary skill, numbers untouched.
* Constellation on demand: "show me what you used" renders the subgraph — the receipts as a picture.
* Export anything, branded: chart → PNG, dashboard → PPTX, analysis → PDF memo, in one tap, provenance in the footer.
* Proactive honesty: when a number is pending or composed, the assistant says so in one clause and offers "use certified only" — trust as a feature people can feel.
* Session handoff: reopen tomorrow, it says where you left off and what it was checking.

## 10 · UI — Claude-shaped, artifact-first
Left sidebar (projects/recents/starred/search/new chat) · center chat stream (streamed markdown, collapsed tool activity, inline chips, inline small charts) · right artifact panel (the current artifact, version history, export, "open in dashboard") · masthead (build id, persona floor, budget meter). Mobile: chat-first with artifacts as swipe-over. Everything the A2UI component work produced survives as artifact/message types; nothing is wasted.

## 11 · Evals (outcome-graded, plus artifact quality)
E19's capability matrix and two-number line stay. Add: artifact tasks (dashboard/chart/document graded by a rubric + structural checks: every tile disclosed, export valid, data matches the query), reasoning tasks (no-tool business reasoning questions graded by a calibrated judge), playbook tasks (a "why" question must run the decomposition with its checks), and trajectory hygiene (sampled-before-filtered, read-before-used). Weekly transcript reading, unchanged.

## 12 · Migration from what's built (nothing thrown away that works)
Keep: graph, compiler, build, the eight tools, store, events bus, budgets/breakers, fresh-context verifier (as a tool + auto on publish), typecheck (now inside `plan_set`/`run_sql` validation), the chat surface's streaming/SSE, session store, A2UI components → artifact types. Remove from the turn path: classify/apply/resolve pre-steps, the contract gate before generation. Add: `python`, `check.*`, `compare`, `whatif`, artifact tools + export, skills loader, projects/sidebar, artifact panel.

## 13 · Build order

1. The thin loop + `python` + artifacts (`chart`, `table`, `document`) + export — the Claude-feel arrives here.
2. Meridian toolkit re-exposed with colleague-grade descriptions; `verify_answer` + `check.*` as tools; rendering rules 1–3 enforced.
3. Skills loader + meridian-sql + analysis-playbooks + dashboard-design.
4. `dashboard` and `diagram` artifacts, `whatif`, `compare`, follow-up chips, constellation.
5. Projects/sidebar/memory; session handoff; PPTX export.
6. E19 + artifact/reasoning suites; real-Vertex baseline; iterate from transcripts.
