# Security & Code-Health Audit — wyla monorepo

**Date:** 2026-07-06 · **Scope:** `lumi_final/`, `semantic-graph/`, `synapse/`, `apps/`, `agent_test/`, `scripts/` · **Method:** full-tree grep sweep (secrets, TLS, injection, hardcoding) + three subsystem deep-reads + new-code review.

**Headline:** no committed credentials anywhere (good hygiene: env-var tokens, scripts that refuse in-repo SA keys, thorough root `.gitignore`). The real risks are (1) a global TLS kill-switch pattern copy-pasted across 8 files, (2) internal hostnames/project IDs woven through committed docs and defaults, and (3) gaps that would have let real extraction data land in git. Item 3 and the worst of item 2 are **fixed in this PR**; the rest are ranked below.

---

## Fixed in this PR

| # | Finding | Fix |
|---|---|---|
| F1 | `synapse/.gitignore` ignored only `data/demo/` — a real `.env`, `data/real/` extraction (MDM business names, user emails from `top_users`, sample queries), LLM proposals, or `review_queue/` could be committed by accident | Ignore list now covers `data/real|cache|traces|proposals`, `review_queue/`, `.env*`, key files |
| F2 | `synapse/loaders/mdm_loader.py` hardcoded the internal MDM hostname as the endpoint | Endpoint now read from `SYNAPSE_MDM_ENDPOINT` env (legacy literal kept as fallback only so the work laptop keeps working; scrub tracked as P1-2) |
| F3 | `lumi_final/lumi/pipeline.py:172` called `record_corpus_facts` without importing it — **every `python -m lumi plan` run crashes** with `NameError` (correctness, found during audit) | Import added; parse-verified |
| F4 | No isolation story for agent-run analysis code | New sandbox (`synapse/synapse/utils/sandbox.py`): scrubbed env (no creds/proxy inherit), `python -I`, CPU/memory/proc rlimits, wall-clock timeout, path-traversal-safe file mounts, output caps — with an **honest docstring** that this is accident-containment, not an adversarial boundary; production swap is Vertex Code Interpreter / gVisor |
| F5 | PII/guardrail rules lived only in prose where an agent can ignore them | Guardrails are now graph nodes; `validate_sql_plan` statically **fails** SQL that references privacy-guarded columns (e.g. `cm11_encrypted`), applies `LAG()` over pre-lagged tables, or violates count-distinct contracts |

## P1 — fix before wider distribution

**P1-1 · Global TLS-verification kill switch (8 files).**
`lumi_final/lumi/plan_builder.py:814-876` (`_maybe_disable_tls`, mirrored in `ontology_builder.py:101`) monkeypatches `ssl`, `requests.Session`, `httpx.Client/AsyncClient`, and google-auth `AuthorizedSession` to `verify=False` process-wide and sets `PYTHONHTTPSVERIFY=0` when `LUMI_INSECURE_TLS=1`. Similar blocks in `agent_test/run.py:84-122` (`--insecure`), `scripts/check_vertex_gemini.py`, `lumi_final/scripts/{check_bq_access,probe_enrich,run_phase1,explore_mdm_payload}.py`. Opt-in and logged — but once flipped, **every** HTTPS call in the process (including MDM calls carrying tokens) is MITM-able, and the copy-paste spread means one more paste lands it in a service someday.
*Fix:* delete the bypass from library code; standardize on `truststore.inject_into_ssl()` (already the default path) + `REQUESTS_CA_BUNDLE`/`SSL_CERT_FILE` for the corp root CA; if an escape hatch must exist, keep it in ONE dev-only script, never importable.

**P1-2 · Internal identifiers committed across ~28 files.**
Real MDM hostname, `axp-lumi` / `prj-p-lumi-gpt` project IDs, PSC endpoints (`bigquery-prod.p.googleapis.com` as *defaults* in `synapse/utils/auth.py:62,68`, `bq_batch_extract.py:105`), enterprise Confluence links, and SA email in `wl.md`/`session.md`/specs/`.env.example`s/gold SQL. Fine for a private PoC repo; a leak vector the moment the repo is shared or mirrored.
*Fix:* one scrub pass replacing literals with env-var reads + `<REDACTED-EXAMPLE>` placeholders in docs; make public endpoints the code defaults and enterprise PSC the env override. (Started: F2.)

## P2 — fix soon

**P2-1 · f-string SQL identifier interpolation** in `semantic-graph/scripts/bq_batch_extract.py` (~15 probes, e.g. `:328`, worst `LIKE '%{t.name}%'` `:541`) and `bq_capabilities_probe.py:566`. Inputs come from `tables.yaml`/CLI today (operator-controlled), so exploitability is low — but the pattern is one refactor away from a user-facing injection.
*Fix:* validate table identifiers against `^[A-Za-z0-9_$.-]+$` at config-load, use BigQuery named query parameters for values, and keep identifiers out of LIKE literals.

**P2-2 · MDM client trusts the network absolutely.** No auth header support in `synapse/loaders/mdm_loader.py` (docstring: "No auth required on VPN"); `lumi_final/lumi/mdm.py` same. Combined with env-configurable endpoints this is SSRF-shaped if any untrusted input ever reaches the endpoint/table args.
*Fix:* optional bearer support (env `MDM_TOKEN` — pattern already exists in `scripts/check_mdm_access.py`), host allowlist (`*.aexp.com`), and keep the 10-30s timeouts.

**P2-3 · Sensitive MDM surfaces must never be ingested.** `ApiPollingInfo` (`/api-polling-info*`) carries endpoint tokens, header tokens, `uid`, `pwd`. Loaders currently only call `/datasets/schemas` — correct — but nothing *prevents* a future crawler from walking the full OpenAPI surface.
*Fix (policy as code):* maintain an explicit endpoint allowlist in the MDM adapter; deny-by-default for anything matching `api-polling-info|keys|key-schema-mappings`; never log raw responses. (Documented as a blueprint invariant; enforce when the crawler generalizes.)

**P2-4 · Graph content is untrusted model input.** Table/column descriptions arrive from MDM, LookML, failed queries, and LLM enrichment — any of them can carry adversarial instructions that a consuming agent might follow (prompt injection via metadata). Current mitigations already in place: agents are read-only (no SQL execution tool), SQL is statically validated, MCP surface is read-only, human executes queries. *Add:* strip/flag imperative-verb content in descriptions at compile time; keep `llm_generated` facts capped at `inferred` tier (already enforced by source weights).

## P3 — hygiene backlog

- Default Postgres password `"lumi"` in `lumi_final/lumi/semantic_graph/config.py:39` flows into a libpq conninfo — require env in non-dev.
- Business data baked into code: `semantic-graph/src/semantic_graph/tools/graph_tools.py:614-621` hardcodes naming-correction pairs — belongs in the graph (the skills/corpus witnesses now carry this).
- Cypher string-building in `lumi_final/lumi/semantic_graph/projector.py` (`_safe`, `:96`) is fine for controlled events; never route user text through it.
- Dual env vars for one endpoint (`LUMI_MDM_API_BASE` vs `LUMI_MDM_ENDPOINT`); nonexistent default model id `gemini-3.1-pro-preview` hardcoded in 5 places; `synapse/sql/bq_table_extraction.sql` declares variables it never uses (hand-edit trap).
- `semantic-graph`'s six `sys.path.insert(parents[4]/"synapse")` hacks — relocation breaks imports silently; make `synapse` an installed dependency (blueprint step 3).

## Duplication the restructure retires (risk = drift, not exploit)

Three parallel graph stacks (lumi AGE event store · synapse in-memory store · semantic-graph wrapper), two LLM-proposal stacks (`synapse/curation` wired vs `synapse/enrichment` unwired), two near-identical `BQClient`s, two `validate_sql` tools with different semantics, duplicated confidence lattices. The blueprint (`docs/SEMANTIC_LAYER_BLUEPRINT.md` §7) sequences the consolidation; this PR establishes the target seams (GraphService + loaders) without deleting anything mid-flight.

## Agent-layer threat model (new surfaces added by this PR)

| Surface | Threat | Control |
|---|---|---|
| MCP server | data egress to any connected host | read-only tools; no row-level data in the graph by construction; per-response `tenant_id`; stdio default (process boundary), bearer allowlist planned for HTTP (spec §6) |
| `validate_sql_plan` | false sense of safety | responses label it *advisory static analysis*; violations block, absence of violations ≠ proof |
| Sandbox | code escapes / secret theft | scrubbed env, rlimits, timeout, tempdir teardown; explicitly documented as non-adversarial isolation; production = managed code-interpreter |
| Skills loader | poisoned skill package injects a hostile "guardrail" | skills dir is a curated, access-controlled path; loader never executes skill SQL; guardrails only *constrain* (they can forbid, never authorize) |
| Analyst agent | hallucinated joins/filters | tool contract: empty `get_join_path` → must tell user; literals verified via `get_filter_values`; provenance tiers mandatory in output contract |
