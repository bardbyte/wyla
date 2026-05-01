# LUMI — LookML Understanding & Metric Intelligence

Multi-agent system that enriches LookML views from gold NL→SQL queries +
MDM business metadata + LLM reasoning, so a downstream NL2SQL agent can
answer **any** question about those views.

This file is loaded into every Claude Code session. Treat its facts as
verified ground truth — don't re-derive what's listed here unless you've
got new evidence the world changed.

---

## Status — what's verified vs. built vs. ahead

| Integration | Status | Where verified |
|---|---|---|
| Vertex AI / Gemini 3.1 Pro | ✓ Confirmed working end-to-end | `scripts/check_vertex_gemini.py` returns final answer |
| GitHub Enterprise PAT (read repo + paths) | ✓ Confirmed | `scripts/check_github_access.py` against `amex-eng/prj-d-lumi-gpt-semantic` |
| MDM API (full schema for one table) | ✓ Confirmed — 193-col response captured | `scripts/check_mdm_access.py`, fixture saved in original run |
| ADK runtime + tool calling | ✓ Confirmed | `apps/vertex_smoke` (dice + prime ReAct loop) |
| ADK web UI | ✓ Confirmed | `adk web apps/` shows both apps; events/trace/state panels work |
| Excel auditor agent (`apps/curator`) | ~ Built, runs locally on fixture; not yet run on real gold-query Excel | tools standalone-tested |
| LookML view fetcher (`.view.lkml` from GHE) | ✗ Not built | — |
| BigQuery INFORMATION_SCHEMA + SELECT DISTINCT | ✗ Not built | — |
| Enrichment agent (combines all sources → enriched LookML) | ✗ Not built | — |
| Coverage validator | ✗ Not built | — |

---

## Architecture (current — direct Vertex AI; no SafeChain)

We tried SafeChain. We removed it. The active path is:

  - Service-account JSON for `prj-d-ea-poc` → `GOOGLE_APPLICATION_CREDENTIALS`
  - `GOOGLE_GENAI_USE_VERTEXAI=true` selects the Vertex backend
  - `GOOGLE_CLOUD_PROJECT=prj-d-ea-poc`, `GOOGLE_CLOUD_LOCATION=global`
  - ADK's google-genai client picks all four up automatically
  - Model: `gemini-3.1-pro-preview`

**Don't reintroduce SafeChain** — adapter complexity, auth indirection, and
async-path issues that don't exist when ADK talks to Vertex directly.

---

## What we learned from each probe

### MDM API (`scripts/check_mdm_access.py`)

- **Endpoint:** `https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas?tableName=<table>`
- Query parameter (`?tableName=`), NOT path-based as our archive design assumed.
- **No auth required** on the corporate intranet (VPN-gated).
- **Response shape:** the top level is an **array of length 1**. Real data lives at `[0]`. Always peel that wrapper.
- **Column metadata** at `[0].schema.schema_attributes` — list of column dicts. Example: 193 columns for `custins_customer_insights_cardmember`.
- Each column has 12 top-level keys; the **business-meaningful fields** are nested under `attribute_details` (22 sub-keys including `business_name`, `attribute_desc`, `attribute_type`, `is_partitioned`, `derived_logic`).
- **Sensitivity flags** at `[i].sensitivity_details` (`is_pii`, `is_gdpr`, `is_critical_data_element`, `pii_role_id`, etc.).
- **Table-level metadata** at `[0].dataset_details` (15 keys: `business_name`, `data_desc`, `data_category`, `data_sub_category`, `feed_type`, `table_type`, `is_internal`, etc.).
- **Source location** at `[0].dataset_source_details` (`project_id` is templated as `@context.system/project_id`, `dataset_name="DATA"`, real `table_name`). Storage type `BigQuery`, load type `FULL_REFRESH`.
- **Ownership** at `[0].ownership_details` (`aim_id`, `imr_queue`, `business_contacts[]`, `tech_contacts[]`).
- **What MDM does NOT have:** per-column synonyms, allowed_values. We need BigQuery `SELECT DISTINCT` for the latter, and gold queries for the former.
- **What MDM SOMETIMES has:** `external_reference_details` for cross-table relationships. Don't assume empty for all tables — extract per-table.

### GitHub Enterprise (`scripts/check_github_access.py`)

- **API base:** `https://github.aexp.com/api/v3` (Enterprise, not github.com).
- **Repo confirmed accessible:** `amex-eng/prj-d-lumi-gpt-semantic`.
- **Auth:** Classic PAT with `repo` + `read:org` scopes works. Fine-grained PATs add admin-approval delay for org access.
- **CRITICAL silent failure mode:** PAT must be **explicitly authorized for SAML SSO** against the `amex-eng` org. Without it, the API returns **404** (not 403) on private repos to prevent existence enumeration. This is the #1 cause of "I have access but the API says it doesn't exist."
- **Diagnosis recipe:** if PAT auth-checks pass (`/user` returns 200) but `/repos/{owner}/{name}` returns 404, run `/user/orgs` — if that returns `[]`, it's almost certainly missing SSO authorization (or missing `read:org` scope hiding the orgs).

### Vertex AI / Gemini (`scripts/check_vertex_gemini.py`)

- **Project:** `prj-d-ea-poc` (the certified-access project for Gemini 3.x previews).
- **Location:** `global` — region-specific endpoints (`us-central1`, etc.) don't have the model grant for our project.
- **Model ID:** `gemini-3.1-pro-preview` (current as of April 2026 docs). The earlier `gemini-3-pro-preview` was discontinued.
- **Service account:** `svc-d-lumigct-hyd@prj-d-ea-poc.iam.gserviceaccount.com`, requires `roles/aiplatform.user`.
- **Auth:** point `GOOGLE_APPLICATION_CREDENTIALS` at the SA JSON. The `google-auth` library handles the OAuth dance automatically.
- **Corporate-MITM TLS:** the Amex network re-signs TLS with an internal root CA. Python's bundled certifi doesn't trust it → `SSLCertVerificationError`. Fix: `pip install truststore` and call `truststore.inject_into_ssl()` (we do this in each `apps/<agent>/__init__.py`). Truststore reads from macOS Keychain, where the corporate root CA is pre-installed on Amex laptops.
- **Last-resort TLS bypass:** `--insecure` flag in `check_vertex_gemini.py` monkey-patches stdlib ssl + httpx + google-auth's AuthorizedSession. Works but skips verification — only for confirming the rest of the pipeline.

### Google ADK (`apps/vertex_smoke`, `apps/curator`)

- **Discovery layout:** `adk web AGENTS_DIR` expects `AGENTS_DIR/<agent_name>/{__init__.py, agent.py}` where `agent.py` exposes a module-level `root_agent`.
- **Sidebar app name = directory name** (`apps/curator/` shows as "curator" in the UI). The `Agent(name=...)` parameter is what shows in event traces and tool calls.
- **Tools are auto-introspected** from Python function signatures + docstrings into JSON Schemas the LLM sees. Good docstrings are not optional — they're the tool description.
- **Don't attach files via the paperclip in adk web chat.** Gemini rejects `.xlsx` and other Office MIME types as multimodal input (`400 Invalid argument: mime type ... not supported`). Type the path; tools read from disk.

---

## Sharp edges (things that bit us; pin these in memory)

1. **Hyphens kill Python imports.** A directory named `agent-test/` can't be `import agent-test`. Use underscores: `agent_test/`. Required for any package, including ADK app dirs.

2. **Apps wrapper must NOT share a name with the real package.** If `apps/curator/agent.py` does `from gold_curator.agent import root_agent` and the wrapper dir is also named `gold_curator/`, Python's module cache returns the partially-initialized wrapper → circular import. Either make names distinct OR (better) inline tools INTO the apps subdirectory.

3. **Cross-package imports with sys.path tricks fail at ADK web's per-request agent resolution** even when they pass at module-import time. ADK re-resolves the agent's origin module on each chat message, and `sys.path` mutations from import time don't persist. **Lesson: keep all agent code (tools + agent definition) inside the single `apps/<agent>/` directory.** Use relative imports (`from .tools import ...`).

4. **The four GOOGLE_* env vars must be set BEFORE any `google.*` import.** ADK's google-genai client reads them on first use. `setup_vertex_env.sh` exists for this — `source` it before `adk web`.

5. **`gemini-3-pro-preview` is gone.** Use `gemini-3.1-pro-preview`. April 2026 Vertex AI docs explicitly call this out. Don't trust older code samples.

6. **Vertex location `global` is the right default for `prj-d-ea-poc`.** `us-central1` 404s on the model grant.

7. **PAT 404 is ambiguous.** Could be: typo in owner/repo, missing scope, OR missing SAML SSO authorization. Always probe with `/user/orgs` to disambiguate.

8. **MDM response is an array, not a dict.** Always start with `data[0]`.

9. **The classic PAT vs. fine-grained PAT trade-off:** fine-grained needs org-admin approval for org repos (slow). Classic + SSO authorization is faster for personal-laptop dev work.

10. **`adk web`'s file-attach button** sends the binary to Gemini as multimodal. Office docs aren't supported. Path-in-chat-only.

---

## Three sources of truth (architecture for the enrichment pipeline)

| Source | What it provides | What it doesn't |
|---|---|---|
| **Existing LookML** (auto-generated by Looker from the BQ table) | All 193 fields already declared. Dimensions, measures, sql expressions. | Rich descriptions, synonyms, business labels. |
| **MDM API** | Canonical business names, descriptions, sensitivity flags, ownership, partitioning. Sometimes relationships. | Synonyms, allowed values, field-frequency / usage signal. |
| **BigQuery `INFORMATION_SCHEMA` + `SELECT DISTINCT`** | Authoritative types, nullability, partition columns. Observed allowed values for low-cardinality fields. | Synonyms (data is values, not metadata). |
| **Gold queries (137)** | User vocabulary (NAA, AIF, etc.), join paths, default filters (`data_source='cornerstone'` in 90%+), missing measures, derived dims from CASE WHEN. | Coverage on rarely-queried fields. |

We **enhance, not replace**. The agent reads existing LookML and adds the
descriptions/labels/tags/measures the four sources collectively suggest.

---

## Repo layout (current)

```
.
├── CLAUDE.md                       this file
├── README.md                       project front door
├── ONBOARDING.md                   Slack-shareable setup template
├── apps/                           ADK AGENTS_DIR
│   ├── vertex_smoke/               canonical dice + prime smoke test
│   │   ├── __init__.py
│   │   └── agent.py                root_agent (re-exports from agent_test/)
│   └── curator/                    gold-query Excel auditor
│       ├── __init__.py             truststore inject
│       ├── tools.py                7 tools (list/preview/read/summarize/
│       │                            validate_sql/analyze_for_lookml/extract)
│       ├── agent.py                Agent + root_agent (uses `from .tools`)
│       └── README.md
├── agent_test/                     CLI runner for vertex_smoke
│   ├── __init__.py                 truststore inject
│   ├── agent.py                    Agent + root_agent
│   ├── run.py                      python agent_test/run.py --key-file ...
│   ├── setup_vertex_env.sh         source me before adk web
│   └── README.md
├── scripts/                        one-shot probes (no LLM, deterministic)
│   ├── check_github_access.py      PAT + repo + path probe (incl. SSO diag)
│   ├── check_mdm_access.py         MDM schema fetcher + structural digest
│   └── check_vertex_gemini.py      Vertex Gemini end-to-end (with TLS opts)
└── archive/                        previous full implementation, reference only
```

---

## Working set of rules (post-cleanup, current reality)

1. **SQL parsing = `sqlglot`.** Never regex. Never LLM.
2. **LookML parsing = `lkml` library.** Never regex. Never LLM.
3. **Deterministic logic = tool function or `CustomAgent`.** Never `LlmAgent`.
4. **Temperature = 0** for all LlmAgents (set via `generate_content_config=GenerateContentConfig(temperature=0.0)`).
5. **Every field in every view gets enriched** — not just the ones gold queries touch.
6. **Type hints on all functions.** ADK uses signatures + docstrings to generate tool schemas the LLM sees.
7. **Tool docstrings must be precise.** They are the tool description for Gemini.
8. **LLM never sees raw `.lkml` or raw Excel binary.** Tools parse first; LLM gets structured output.
9. **No paperclip-attaches in adk web** — paths in chat, tools read from disk.
10. **All agent code lives inside `apps/<agent>/`.** No cross-package imports, no sys.path tricks. Tools beside agent.py, relative imports only.
11. **Vertex direct, no SafeChain.** Service-account JSON + four GOOGLE_* env vars.
12. **`pip install truststore` on every Amex laptop** — corporate-MITM TLS only works that way.

---

## Setup invariants

```
Service account JSON: outside the repo (e.g. ~/Downloads/key.json)
$GOOGLE_APPLICATION_CREDENTIALS  → that path
$GOOGLE_GENAI_USE_VERTEXAI       = true
$GOOGLE_CLOUD_PROJECT            = prj-d-ea-poc
$GOOGLE_CLOUD_LOCATION           = global
Model                            = gemini-3.1-pro-preview

pip install: google-adk truststore openpyxl sqlglot
```

`source agent_test/setup_vertex_env.sh ~/Downloads/key.json` exports the four
env vars in one shot.

---

## What to build next (in order)

1. **Run `apps/curator/` against the real gold-query Excel.** Get a verdict from the agent. Confirms the auditor pattern works on production data.
2. **Build a LookML fetcher** — fetch a `.view.lkml` from GHE via PAT API, parse with `lkml`, return structured fields. We have the access; just need a script + tool wrapper.
3. **Build a BigQuery probe** — `INFORMATION_SCHEMA.COLUMNS` + a small `SELECT DISTINCT col FROM table LIMIT 50` for low-cardinality columns. Same SA we already use for Vertex.
4. **Build a "view context bundle" assembler** — for one view, combine: existing LookML + MDM + BQ types/values + gold queries that touch it. Single payload for the enrichment agent.
5. **Build the enrichment agent** (one view first) — Gemini 3.1 Pro via ADK, takes the bundle, emits enriched view (descriptions, labels, tags, missing measures, derived dims).
6. **Coverage validator** — for each gold query, can the enriched view answer it? Deterministic, no LLM.
7. **Scale to all 30 views** — parallelize, run pipeline, ship.

Steps 1–4 are deterministic plumbing. Steps 5–7 are where the LLM does work.

---

## Code style

- pathlib for paths. logging not print(). Line length 99.
- Imports: stdlib → third-party → local.
- Tool functions return `dict` with `status: "ok" | "error"` + `error: str | None` field — uniform contract Gemini expects.
- Docstrings in Google style (Args/Returns) — ADK introspects them into JSON Schema.
