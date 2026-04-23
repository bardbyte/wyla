# LUMI — Work Laptop Bootstrap

Everything you need to go from `git clone` → enriched LookML on your Amex work laptop.

## 0. Prerequisites

- Python 3.11+ (`python --version`)
- `git` on PATH
- VPN on — required for GitHub internal repo and MDM endpoint
- **SafeChain package installed** — provides `src.adapters.model_adapter.get_model`. This is an Amex-internal package; install per your team's onboarding. If `python -c "from src.adapters.model_adapter import get_model"` fails, stop and resolve this before proceeding.
- CIBIS portal credentials (ask team lead if you don't have them)

## 1. Clone + install

```bash
git clone https://github.com/bardbyte/wyla.git lumi
cd lumi
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

If `pip install -e .` resolves the `google-adk`, `lkml`, `sqlglot`, `openpyxl` etc. cleanly but you see an import error later for `src.adapters.model_adapter`, that's SafeChain — install it separately from Amex internal sources and add to your venv.

## 2. Configure

```bash
cp .env.example .env
cp lumi_config.example.yaml lumi_config.yaml
```

Edit `.env`:
- `CIBIS_CONSUMER_INTEGRATION_ID` — from the CIBIS portal
- `CIBIS_CONSUMER_SECRET` — from the CIBIS portal
- `CONFIG_PATH` — path to the SafeChain `config/config.yml` on your machine
- `MDM_BEARER_TOKEN` — only if the MDM endpoint requires auth

Edit `lumi_config.yaml`:
- `git.repo` — full URL of the internal Looker project
- `git.view_files` — paths of the 30 views you care about
- `gold_queries.file` — path to your gold-query Excel
- `mdm.endpoint` + `mdm.view_to_mdm_entity` — MDM base URL and the view→entity map
- Leave `llm.strong_model_idx="1"` and `llm.fast_model_idx="3"` unless your SafeChain config differs

## 3. Preflight (4 scripts, must all pass)

```bash
./scripts/preflight_deps.sh          # Python + packages + SafeChain import
./scripts/preflight_github.sh        # git ls-remote on configured repo
./scripts/preflight_mdm.sh           # GET /{first_entity}; saves sample to fixtures/
./scripts/preflight_llm.sh           # raw SafeChain + ADK adapter smoke test
```

Each script exits non-zero on failure with a clear message. Fix the first one that fails before running the next.

## 4. Run the pipeline

```bash
python -m lumi --config lumi_config.yaml
```

What happens:
1. `DataLoader` — clones the repo, parses gold queries, queries MDM per view, groups queries by view, extracts join patterns. Populates `session.state`. (~30 seconds.)
2. `ParallelAgent(ViewEnricher × N)` — one Gemini 2.5 Pro call per view via SafeChain. Emits `EnrichedView` (structured output). (~1-2 minutes for 30 views.)
3. `Aggregator` — gathers the enriched views into one state key.
4. `ExploreBuilder` — one Pro call to write the `.model.lkml` explores.
5. `VocabChecker` — one Flash call to flag vocabulary drift.
6. `Validator` — deterministic coverage check against all gold queries. Writes enriched `.view.lkml` files, the model file, and JSON reports.

Total: ~3-5 minutes, ~$1.10 in SafeChain token costs, ~32 LLM calls.

## 5. Inspect the output

```
output/
├── views/
│   └── <view_name>.view.lkml              # 30 enriched view files
├── models/
│   └── analytics.model.lkml               # generated explores
└── reports/
    ├── coverage_report.json               # per-query pass/fail
    ├── gap_report.json                    # failure-reason summary
    └── vocab_report.json                  # vocabulary issues
```

Open `coverage_report.json` first. The headline is `coverage_pct`. Anything below 95% means you've got real gaps to iterate on.

## 6. Iterate

If coverage is low:
1. Look at `gap_report.json` → which failure reasons dominate?
2. If `missing_measure` / `missing_dimension` — tune `lumi/prompts/view_enricher.md` to be more aggressive about creating missing fields.
3. If `schema_gap` — a gold query references a table not in `git.view_files`. Add the view to your config.
4. If `missing_explore` — check `lumi/prompts/explore_builder.md`.
5. Re-run `python -m lumi`.

## 7. Commit enriched files

```bash
cd output/views
git init
git remote add origin <your-looker-project-repo>
git checkout -b lumi/enrichment-v1
git add .
git commit -m "Enriched LookML via LUMI"
git push -u origin lumi/enrichment-v1
```

Open a PR. The descriptions + labels + tags + generated measures/dimensions are the value — reviewers should focus on the prose quality and the generated SQL in `measures_added` / `derived_dimensions_added`.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ImportError: src.adapters.model_adapter` | SafeChain not installed | Install the Amex SafeChain package |
| `KeyError: 'CIBIS_...'` | `.env` not loaded | Ensure `.env` is in the repo root; `python -m lumi` calls `load_dotenv(find_dotenv())` |
| `ConfigError: Config not found` | Missing `lumi_config.yaml` | `cp lumi_config.example.yaml lumi_config.yaml` |
| `git clone failed` | VPN off or auth | Connect VPN; verify you can manually `git clone` the repo |
| HTTP 401 / 403 from MDM | Bearer token missing/invalid | Set `MDM_BEARER_TOKEN` in `.env`; `auth_env: MDM_BEARER_TOKEN` in `lumi_config.yaml` |
| HTTP 429 from SafeChain | Rate limit on parallel view enrichment | Lower `max_llm_calls` or serialize via `SequentialAgent` temporarily |
| `output_schema` validation error | ViewEnricher emitted malformed JSON | Inspect the logs; usually a prompt issue. Tune `view_enricher.md`. |
| "Runner hangs, no events" | `SafeChainLlm` raised before yielding | Already guarded by try/except — check for `error_code` in log output |

## Where code lives

```
lumi/
├── agent.py                     # root agent composition
├── __main__.py                  # `python -m lumi` entry point
├── config.py                    # YAML → LumiConfig loader
├── tools/                       # 6 deterministic tools
├── agents/                      # 5 ADK agents (1 parallel, 3 LLM, 2 custom)
├── prompts/                     # 3 prompt files (view_enricher, explore_builder, vocab_checker)
└── schemas/                     # Pydantic models

src/adapters/adk_safechain_llm.py   # SafeChain → ADK BaseLlm bridge
tests/                              # 35+ pytest tests, all green on main
```

## What runs without SafeChain / Amex infra (for local dev)

On any machine with Python 3.11+ and `pip install -e ".[dev]"`:
```bash
pytest tests/                      # 35 tests, covers schemas + all 6 deterministic tools
ruff check lumi/                   # lint
mypy --strict lumi/                # type check
```

Only `python -m lumi` needs SafeChain. Everything else (parsing, grouping, validation) is pure Python and runs anywhere.
