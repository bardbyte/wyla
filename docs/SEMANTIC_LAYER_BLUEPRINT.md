# Enterprise Semantic Layer — System Blueprint

**Status:** v1 — matches what ships in this branch. Companion docs: `synapse/docs/MCP_SERVER_SPEC.md` (tool contracts), `synapse/docs/DEEP_RESEARCH_AGENT_SPEC.md` (research loop), `docs/SECURITY_AUDIT.md` (risk register), `synapse/docs/UX_*` (visual system).

**North star:** every analytical question against the warehouse gets answered by an agent that (1) knows what the data *means*, not just what it's called, (2) can prove where every fact came from, and (3) is constrained by the same guardrails the best human analysts follow — exposed to people through whatever surface they already work in.

---

## 1. The courtroom, completed: five witnesses

The PoC began with three signal sources. This branch adds the remaining two, completing the witness bench:

| # | Witness | Nature | Trust prior | Coverage | Adapter |
|---|---|---|---|---|---|
| 1 | **MDM API** | Government records — declared, curated | high (structural), can be stale | broad | `synapse/loaders/mdm_loader.py` (+ lumi digest) |
| 2 | **BigQuery via SVC ID** | Surveillance footage — observed behavior | high (it happened), needs interpretation | whatever the SA sees | `semantic-graph/scripts/bq_batch_extract.py` → `synapse/loaders/bq_loader.py` |
| 3 | **Skills library** | Sworn expert testimony — curated playbooks | highest short of steward sign-off | narrow (11 skills, 2 domains) | **NEW** `synapse/loaders/skills_loader.py` |
| 4 | **Gold-SQL corpus** | Work product — what analysts provably do | medium per query, compounds with repetition | wherever analysts wrote SQL | **NEW** `synapse/loaders/gold_sql_loader.py` (standalone sqlglot; `lumi_loader` remains the deep path) |
| 5 | **LLM enrichment (Vertex SVC ID)** | Forensic analyst — inference, labeled as such | lowest until corroborated | anywhere, on demand | `synapse/curation/` + `semantic-graph/enrichment/` (existing) |

The five don't vote equally. `SOURCE_WEIGHTS` in `synapse/graph/store.py` encodes the priors (human approval 10 · skills 7 · catalogs 5 · BQ/DQ 4 · MDM/LookML 3 · corpus/usage/LLM 1 per observation), and the fusion rule prizes **distinct-source breadth over single-source depth**: five independent witnesses agreeing once beats one witness shouting five times.

## 2. The thin waist: assertions in, compiled graph out

The one architectural rule everything obeys:

```
every fact lands as   (subject, predicate, object, source, confidence, observed_at)
the graph is          COMPILED from those assertions — never hand-written
```

Concretely: loaders emit canonical per-source artifacts (witness statements); `build_graph_from_sources()` replays them in a fixed order into typed nodes/edges; every `upsert` stamps the contributing source into the fact's `Provenance` envelope (sources, evidence counts, first/last observed, calibrated tier, conflicts); `confidence_from_sources()` recomputes the tier on every touch. Deleting the snapshot and recompiling is always safe — state lives in the witness statements, not the graph. (`lumi_final`'s append-only `OntologyEvent` JSONL store is the same idea event-sourced; it remains the write-ahead log for the AGE scale-out path, §7.)

Snapshots are **content-addressed**: `GraphStore.save_json()` stamps `snapshot_version = <date>-<sha256[:12]>`, and every tool response echoes it — an agent's answer is reproducible against the exact graph that produced it.

## 3. The layered graph

```
L3 Semantic     Skill, Guardrail, Metric contracts, Synonyms      ← skills, catalogs, LLM
L2 Behavioral   joins observed, filters, failed-query corrections,
                top users, co-query clusters, cost                ← gold SQL, BQ JOBS
L1 Governance   ownership, sensitivity/PII, lineage, lifecycle,
                DQ rules                                          ← MDM, DDL-mined RLS
L0 Physical     tables, columns, types, DDL, partitions, profile  ← BQ INFORMATION_SCHEMA
Spine           stable IDs: table/column URIs today; MDM dataset_parent_id /
                appflow_parent_id join keys as MDM coverage lands
```

New in this branch, **guardrails are first-class L3 nodes** (`Guardrail` with `CONSTRAINS` edges), not doc prose: "never LAG() over `common.roll_rate_calc`", "never expose `cm11_encrypted`", "COUNT(DISTINCT na_pcn_no), never rows" are queryable (`get_guardrails`), ride along on every `inspect_table`, and the machine-checkable subset is *enforced* by `validate_sql_plan` before any SQL reaches a human. Skills also mint `Metric` nodes from `metric_contracts.yaml` (`DEFINED_BY` → skill), so "what is the approval rate" resolves to an executable contract with provenance.

## 4. Compile: one trigger

```bash
python synapse/scripts/pipeline.py --demo                  # offline, committed fixtures
python synapse/scripts/pipeline.py \
    --skills-dir ~/Downloads/skills \
    --gold-sql-dir lumi_final/data/gold_queries \
    --bq-extract-dir ~/synapse_bq_outputs \
    --lumi-session lumi_final/data/session1_output.json \
    --mdm-cache-dir lumi_final/data/mdm_cache
```

Any subset works; missing sources are skipped, not fatal — the graph degrades gracefully exactly like the courtroom does when a witness is unavailable. Output: `graph_snapshot.json` (versioned) + `run_manifest.json` (which loaders ran, per-source outcomes, stats). This is the `pipelines/` consolidation the session log deferred — one entry point, N witnesses.

## 5. Serve: one service, two transports

```
                       GraphService  (synapse/mcp/service.py — the ONLY implementation)
                      /            \
        MCP server (stdio/http)     ADK FunctionTools
        synapse/mcp/server.py       synapse/mcp/adk_tools.py
        Claude Desktop · Claude     apps/analyst (Gemini/Vertex)
        Code · any MCP host         apps/curator · future Radix
```

15 task-shaped, read-only tools (per the MCP spec's tenets — verbs an agent needs, not graph primitives; provenance mandatory; structured errors with suggestions; bounded outputs; loop-breakers built in):

`search_entities · list_tables_for_domain · inspect_table · find_columns_for_concept · get_filter_values · resolve_code · get_join_path · get_lineage · get_metric · get_skill · get_guardrails · get_dq_status · explain_confidence · disambiguate_term · validate_sql_plan`

plus resources (`synapse://catalog|metrics|guardrails|skills`) and the `analyst_workflow` prompt. Three tools are additions over the spec's twelve, all skills-powered: `get_skill` (fetch the playbook before improvising), `get_guardrails` (constraints before SQL), `validate_sql_plan` (static enforcement after drafting). Writes stay out of the MCP surface in v1 — curation and steward review own mutation, exactly as specced.

**Why MCP and not just in-process tools:** the graph outlives any one agent framework. MCP makes the semantic layer a *product* any host can consume — Claude Desktop for zero-cost frontier-model demos today, the ADK/Vertex agents for the sanctioned in-house path, partner agents later — while `GraphService` guarantees they all see identical answers.

## 6. The agent: a top-tier analyst, built the way Anthropic builds agents

The analyst (`apps/analyst/`) applies the same discipline Anthropic applies to Claude-with-skills:

- **Skills-first, progressive disclosure.** Rule 2 of the instruction: if `get_skill` covers the question, the skill's definitions and contracts override the model's priors — the model brings reasoning, the skill brings the domain. Skill knowledge loads on demand (excerpt in the graph, files on disk), not stuffed into the system prompt.
- **Small prompt, hard tools.** The instruction carries *when* to use which tool (15 ordered rules) and behavioral invariants; *what* each tool does lives in the tool docstrings ADK/MCP surface natively. Anti-patterns are explicit (no SELECT *, no averaged sub-rates, no invented joins, ≤12 tool calls).
- **Evidence > priors, provenance in the answer.** Fixed output contract: Answer → How I got there → Citations (fact → tier → sources) → Governance & caveats → status stamp (✅/⚠/ℹ). `guessed`-tier facts must be labeled guesses or become questions.
- **Guardrails as law, enforced twice.** Once at research time (`get_guardrails` per table) and once at draft time (`validate_sql_plan` blocks on violations).
- **Sandboxed computation.** `run_python_analysis` — scrubbed-env, rlimited, time-boxed subprocess (`synapse/utils/sandbox.py`) for rate math, contribution decomposition, mini-forecasts on in-conversation numbers. Honest scope: accident containment now, Vertex Code Interpreter/gVisor at production. **The agent never executes warehouse SQL** — it emits validated, ready-to-run SQL; execution stays with humans or a sanctioned executor with its own audit trail. That separation is what makes a bank deployment approvable.
- **The bundle is the contract.** For deep questions, the Plan→Act→Critique research loop (`DEEP_RESEARCH_AGENT_SPEC.md`) emits a typed `ResearchBundle` any downstream agent (Radix NL→SQL, dashboards, narrative writers) can consume. Research is its own surface; SQL generation is a consumer.
- **Eval before scale.** The spec's three-tier eval (component / bundle / human) with a 50→200-question golden set and regression gates is the graduation criterion for each rollout phase.

## 7. UX: what should this agent be *exposed as*?

Evaluated against who operates a bank's data: analysts (deep, daily), business operators (occasional, trust-sensitive), engineers (integration), stewards (curation).

| Option | Strength | Weakness | Verdict |
|---|---|---|---|
| **A. MCP into Claude Desktop/Code** | zero UI to build; frontier model; perfect for the PoC demo & power users | per-seat tooling, not org-wide | **Ship now (it ships in this branch)** |
| **B. `adk web` chat (Gemini/Vertex)** | sanctioned stack, SVC-ID auth, shows the in-house path | dev-tool aesthetics | **Ship now (it ships in this branch)** |
| **C. Slack/Teams analyst** | meets operators where they already ask these questions; threads = natural audit trail; shareable answers | needs bot infra + SSO review | **Target org surface, phase 3** |
| D. Streamlit "X-ray" UI | already exists (`synapse_ui.py`, Obsidian viz); great for stewards & trust-building | browse tool, not an analyst | Keep as the *steward/governance* surface |
| E. Embedded in BI (Looker ext) | zero new habit for dashboard users | heaviest build; couples to one vendor | Later, via the bundle API |
| F. Notebook magic (`%%analyst`) | quants love it | tiny audience vs. cost | Opportunistic |

**Decision:** the durable product is not the chat — it's the **analysis artifact** (question → validated SQL → numbers/chart → citations → guardrails honored → snapshot version) that a conversation produces. Chat is how you *ask*; the artifact is what you *keep, share, and audit*. So: A+B now (both ride the same GraphService), C as the organizational rollout where each thread ends in a pinned artifact, D stays the steward console. Every surface renders the same bundle; none get private capabilities.

## 8. Consolidation map (from the three subsystem deep-reads)

**Keep (load-bearing):** `synapse/graph/*` (store = the platform), `synapse/loaders/*` + registries + curation, `lumi_final/lumi/sql_to_context.py` (richest SQL extractor), `lumi_final/lumi/mdm.py` (cleanest MDM client), `lumi_final` ontology event store (write-ahead log for scale-out), `semantic-graph/scripts/bq_*` extractors, all four synapse specs, both skill.md rulebooks.

**Wrap (this branch did):** graph behind `GraphService` → MCP + ADK; skills & gold-SQL behind loaders; sources behind `pipeline.py`; sandbox behind a tool contract.

**Retire next (in order, each its own small PR):**
1. `semantic-graph`'s six `sys.path parents[4]` hacks → depend on `synapse` as an installed package; its 13 agent tools delegate to `GraphService` (they're the prototype of it).
2. `synapse/enrichment/` (unwired duplicate) folds into `curation/`; one `EntityProposal` model.
3. Two `BQClient`s → one shared REST client module; parameterize the f-string SQL (audit P2-1).
4. `builder._ingest_corpus` regex path retires in favor of `gold_sql_loader`/`lumi_signals` (structured).
5. Graph substrate decision at scale: **snapshot stays the serving format**; AGE/Postgres (lumi `semantic_graph/`) becomes the compile-side store once the graph outgrows memory (~50+ tables ≈ 25k nodes is still fine in-memory). Fix its 3-of-23 projector dispatch gap before promoting it.
6. TLS kill-switch deletion (audit P1-1) + internal-identifier scrub (P1-2).

## 9. What ships in this branch vs. next

**Shipped:** skills + gold-SQL witnesses (loaders, fixtures, ingest passes, 33 tests) · Skill/Guardrail first-class node types · content-addressed snapshots · `GraphService` + MCP server (15 tools/4 resources/1 prompt, boots and answers) · ADK adapter · analyst agent app with sandbox · one-trigger pipeline with run manifest · security fixes F1-F5 · this blueprint + audit.

**Next (ordered):** (1) work-laptop run: `pipeline.py` over the 53-table extraction + real skills dir + real MDM cache → first full five-witness graph; (2) MCP server into Claude Desktop for the stakeholder demo; (3) golden-set eval harness (spec §14) over the analyst; (4) consolidation steps §8.1-8.3; (5) Slack surface piloting the artifact-per-thread pattern; (6) steward write-path (curation queue → `human_approval` facts).
