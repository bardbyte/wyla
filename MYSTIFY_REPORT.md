# Mystify report

Sanitisation pass over the living tree (`synapse-agentic-harness-system/`,
`apps/`, `docs/`, root files). `archive/` was out of scope by request and is
untouched: it still names the enterprise, its systems, and coding agents
throughout. `.claude/skills/` was kept by request.

Baseline before any change (`.mystify/baseline.txt`, at `2f085e1`): silo
suite 394 tests green, admin app 65 tests green, `compileall` clean. Every
batch below was re-run against the same suites before it was committed; the
final tree passes both suites for the same reasons.

## Terms

Seeded from the request, then widened by the scan and a read of the tree:

```
paste back · work laptop · lumi · american express · americanexpress · amex ·
aexp · axp · eag · safechain · oneidentity · acropedia · prj-d-ea-poc · saheb ·
bardbyte · claude · anthropic · copilot · codex · aider · windsurf · cline ·
devin · co-authored-by
```

`safechain` never occurred. `cursor` was checked by hand: every hit is a DB
or pointer cursor.

## Decisions taken with the user

| question | decision |
|---|---|
| "Lumi" is the product's own brand word | keep the phrase **Synapse by Lumi** (and its tagline "powered by Lumi", health slug `synapse-by-lumi`); every other Lumi becomes Synapse |
| `.claude/skills` (two project skills) | keep as is; reported as a residual |
| design research docs that compare named products | move to `archive/design/`, keep the wireframes |
| the logged-in account name | keep the user's own name |

## Rename map (applied)

| from | to | where |
|---|---|---|
| `apps/lumi/` | `apps/synapse_admin/` | package, imports, sys.path segments, docs. `apps/synapse` already existed (the second surface), hence the suffix |
| `tests/test_lumi_app.py` | `tests/test_synapse_admin_app.py` | |
| `Lumi {Home,Components,Principles,Table Profile}.dc.html` | `Admin ….dc.html` | wireframes and every href; "Admin" because `Table Profile.dc.html` is already the Synapse-branded twin |
| `sahs/assistant/skills/lumi-data-connect.md` | `synapse-data-connect.md` | skill pack id in kit, runtime, tests |
| `sahs/util/eag.py`, `scripts/eag_check.py`, `tests/test_eag_check.py` | `gateway.py`, `gateway_check.py`, `test_gateway_check.py` | `EagError` → `GatewayError` |
| `scripts/laptop.py`, `docs/runbooks/laptop_end_to_end.md` | `pipeline.py`, `pipeline_end_to_end.md` | by request (third sweep); every invocation, import, runbook and error message; argparse `prog` |
| `/api/lumi/{planes,brand,logo}` | `/api/synapse/…` | both frontends, backend, tests |
| `lumi-theme` (localStorage) | `synapse-theme` | |
| `LUMI_*` env vars (10) | `SYNAPSE_*` | auth.py, app.py, checks, tests, .env.example, runbooks |
| `EAG_*` (6), `ONEID_*` (2) | `GATEWAY_*`, `IDP_*` | gateway module, check, tests, .env.example |
| gateway/identity default URLs (`*.aexp.com`) | `gateway.example.com`, `idp.example.com` | hardcoded defaults in `gateway.py` and `.env.example` |
| `axp-lumi`, `prj-p-lumi-gpt`, `prj-d-ea-poc` | `demo-warehouse`, `demo-billing`, `demo-vertex` | tests, fixtures, docstrings, .env.example |
| `lumi-ds-NNN` | `synapse-ds-NNN` | crosswalk fixtures and tests |
| source family `acropedia` / display `Acropedia` | `glossary` / `Glossary` | display.py, home.js, tests |
| `/certs/amex-root.pem` | `/certs/corp-root.pem` | one test |
| `"NGBD - Lumi Metadata Management"` | `"Metadata Management"` | fixture label, layout doc |
| `realm=eag`, `source="oneidentity"` | `realm=gateway`, `source="idp"` | a test header, a report label |
| display strings "Lumi Warehouse…", "Lumi Value Meanings" | "Synapse Warehouse…", "Synapse Value Meanings" | display.py, test |
| system-prompt chain "Lumi, the product" | "Synapse, the product" | `assistant/loop.py` CHAIN and the v3 spec (a prompt-text change; the model sees the new word) |

Prose rewrites, no logic: EAG → "the gateway", OneIdentity → "the identity
service", the MDM vendor → "the MDM", Acropedia → "the glossary", Amex brand
notes → "brand", "Claude-Code-class / Claude-shaped / the Claude way" →
"coding-agent-class / chat-assistant-shaped / the chat-assistant way",
`PASTE THIS BACK TO CLAUDE` → `REPORT BLOCK`. Eighteen transcript paste lines
carrying a personal cloud-drive path were removed from
`docs/contracts/semantic_sources_explained.md`.

## Commits

| hash | phase | what |
|---|---|---|
| `2d1b218` | 0–1 | mystify skill installed under `.claude/skills/mystify`; scanner's Python 3.11 f-string fix; baseline, terms, scan, rename map |
| `593de11` | 0 | silo baseline result recorded |
| `80b660c` | 2 | package, file, module, skill-id, route renames (95 files) |
| `c9fde48` | 5 | runtime identifiers: env vars, project ids, gateway URLs, glossary family (53 files) |
| `a712636` | 5 | `g19_dotted_table` golden fingerprint reminted with `SAHS_REGEN_GOLDENS=1` (its SQL literal changed); `canon_version` and the other 24 goldens unchanged |
| `0c1e97d` | 3–4 | comments, docstrings, docs, wireframe marks; research docs to `archive/design/` (75 files) |
| `1f7bf12` | 7 | new root README; three test names |
| `7bfe218` | 8 | one more test name; this report |
| `64da742` | merge | `main` (#127–#129) merged, six conflicts resolved to main's logic, the arrivals swept (third sweep below) |
| this commit | 2 | `laptop.py` → `pipeline.py`, its runbook, 46 files of references; this report |

## Verification

- Scanner on the living tree: 815 term hits before, 110 after. Every one of
  the 110 is in the residual list below.
- Suites: silo 394 green, admin 65 green, `compileall` clean, `bash -n` on
  `preflight.sh` clean. Same counts as the baseline.
- Diff audit: every changed line is a rename, a comment/docstring, a doc, a
  config value, or one of the runtime strings listed above. The two output
  labels that changed (`=== Gateway check ===`, `source="idp"`) are covered by
  `test_gateway_check.py`.
- Fresh clone of the branch: `pip install -e "…[sql,dev,assistant]" fastapi
  uvicorn httpx`, admin tests green, `uvicorn apps.synapse_admin.backend.app:app`
  serves `/` and `/synapse/` with 200 and the designed empty states, `/health`
  and `/api/synapse/planes` answer. The README quick start is what was run.
- Agent sweep: the only hits outside `.gitignore`/`.claude` are the word
  "declined" matching the `cline` pattern (false positives) — see residuals.
- Secrets sweep (key/token shapes): nothing in the living tree.

## Residuals (cannot or should not be scrubbed here)

1. **`lumi` as a graph witness key.** The provenance vocabulary names the MDM
   plane `lumi` (`sahs/graph/quads.py`, `merge_policy.yaml`, `mdm46.py`,
   `crosswalk.py` and the `lumi_asset_id` crosswalk key, card text
   `| lumi:`, fixture values `"dataserver": "Lumi"`, and the witness list in
   `db/spanner/003_graph.sql`). These are data-contract
   values: every compiled build and every laptop `graph/*.jsonl` carries them.
   Renaming them changes compiled output and breaks existing builds, so they
   were left as data. If you want them gone, it is a remint: rename the key,
   `SAHS_REGEN_GOLDENS=1`, recompile from sources.
2. **Business-unit table names in fixtures** (`gms_transaction`,
   `wwcas_authorization`, `tlsarpt_travel_sales`, `CFR/TLS` folders). Tests
   assert on them; replacing them with synthetic names is possible but is a
   fixture rewrite, not a rename. "Atlas" was also left (ambiguous with the
   public Apache Atlas).
3. **The kept brand phrase** "Synapse by Lumi" and its tagline, by decision.
4. **The user's name** in the account row, two tests, `.env.example`, one spec,
   and the `bardbyte/wyla` line in `design/wireframes/github.md`, by decision.
5. **`.claude/skills/`** and its `.gitignore` lines, by decision. A reader
   will infer an agent was used. The `synapse-ui-designer` skill still says
   "Synapse by Lumi" and names a design tool.
6. **`archive/`** is out of scope and unsanitised.
7. **The word "laptop"** is the repo's term for the machine with warehouse
   access. The script and its runbook are now `pipeline.py` and
   `pipeline_end_to_end.md` (third sweep); the word stays in prose (about
   120 mentions). It is not company-specific; the scanner will keep flagging
   it.
8. **Git history.** The working tree is clean; `git log` is not. 213 of 450
   commits are authored `Claude <noreply@anthropic.com>`, 416 commit bodies
   carry `Co-Authored-By: Claude …` or `Claude-Session:` trailers, every old
   name is in every old diff, and the remote is `github.com/bardbyte/wyla`.
   Two ways out:
   - Fresh repo: copy the tree without `.git`, `git init`, one commit under
     your own name. Simplest; history is lost.
   - `git filter-repo --replace-text .mystify/terms.txt --message-callback …`
     to strip trailers and rewrite terms in place, then re-point the remote.
   Either way, confirm with
   `git log --format='%an <%ae>%n%b' | grep -iE 'claude|anthropic|copilot|co-authored'`
   returning nothing. Note this branch's own commits carry the same trailers.
9. The repo name `wyla` carries no company or agent term. The GitHub
   description and topics were not checked from here.

Mystify removes identifiers, not obligations: confirm you have the right to
publish this code, and that nowhere you publish it requires disclosing tool
use.

## Second sweep: the EAG plane from #124

After the pass above, `main` merged #124 (the chat's second model plane
through the gateway). The merge was taken into this branch and the same
rules were applied to what arrived, in one batch:

| from | to |
|---|---|
| `sahs/enrich/eag_client.py`, `tests/test_eag_plane.py` | `gateway_client.py`, `test_gateway_plane.py` |
| `EagClient`, `EagAgent`, `EagError` | `GatewayClient`, `GatewayAgent`, `GatewayError` |
| `EAG_THINKING_BUDGETS`, `EAG_JSON_THINKING_BUDGET`, `EAG_MODEL` | `GATEWAY_*` |
| plane value `"eag"` (`SAHS_MODEL_PLANE`, `/api/synapse/planes`, `model_plane()`, log prefix) | `"gateway"` |
| prose: EAG, OneIdentity | the gateway, the identity service |

The plane value is a runtime string: `SAHS_MODEL_PLANE=gateway` is what a
`.env` written against #124 must now say, and the planes endpoint reports a
`gateway` key where it reported `eag`. The one merge conflict (the planes
assertion in the admin app test) was resolved by taking #124's assertion
with the renamed route and plane key. After the merge the full silo suite is
402 green (394 plus the eight #124 brought) and the admin app is 65 green.

## Third sweep: #127–#129 (the composer's model dial, files in the chat, skills, Spanner)

`main` moved again (three PRs, 57 files, 7,200 lines). Merged with six
content conflicts, every one the same shape: main changed a line this branch
had renamed. Each was resolved to main's logic, then the rules above were
re-applied to everything the merge brought in. New entries only:

| from | to |
|---|---|
| `eag_configured`, `FakeEag` (test helper main's new tests import) | `gateway_configured`, `FakeGateway` |
| `PLANE_IDS = ("vertex", "eag")`, dial rows `"id": "eag"`, `"plane_name": "EAG"` | `"gateway"`, `"Gateway"` |
| `CHECK (Model IN ('', 'vertex', 'eag'))` in `db/spanner/002_chat.sql` | `'gateway'` |
| `SYNAPSE_SPANNER_SA_KEY` was `LUMI_SPANNER_SA_KEY` (new, nothing reads it yet) | `SYNAPSE_*` |
| surface id `'lumi'` in `db/spanner/001_identity.sql` (role → surfaces seed), its docs and test | `'admin'` — the same object `apps/lumi` became `apps/synapse_admin`; `synapse` was taken by the other surface |
| table page label `<b>Lumi:</b>` (the MDM's supplementary meaning) | `<b>MDM:</b>` — a display string; "Synapse:" would have named the wrong source |
| prose: EAG, OneIdentity, "Lumi's supplementary meaning", "the Lumi console" | the gateway, the identity service, the MDM's, the admin console |

Not touched: `'lumi'` in the graph schema's witness list (`003_graph.sql`),
residual 1. Two of the runtime strings are ask-first under the skill and were
taken on the standing decision ("every other Lumi becomes Synapse"): the
surface id `admin` and the label `MDM:`. Both are one-line reverts.

Then, by request, `scripts/laptop.py` → `scripts/pipeline.py` and
`docs/runbooks/laptop_end_to_end.md` → `pipeline_end_to_end.md`, with all 46
files that name them (tests that spawn it, the runbooks, `.env.example`, the
two READMEs, the error messages that tell a user what to run, `argparse
prog`). No other "laptop" changed.

After both: silo suite 419 green, which is what `main` collects; admin app 79
green, likewise; `compileall` clean; scanner 144 term hits in the living tree,
all in the residual list (90 `lumi` witness-key and brand hits, 47 the user's
name, 6 the `.gitignore` lines, 1 the `bardbyte/wyla` provenance line).
