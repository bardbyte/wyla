# synapse

Confidence-typed knowledge graph for the enterprise data warehouse.
Fuses signals from MDM, the analytical SQL corpus, BigQuery, baseline
LookML, and three curated catalogs (acronym glossary, metric catalog,
table catalog) into one graph that powers Radix retrieval and any
downstream agentic consumer.

## Pre-work (the only thing in this commit)

Before any extractor or graph projector runs, we mint the canonical
entity backbone with human approval. That's what this directory ships:

```
1. assemble evidence    — all 7 sources → EvidenceBundle
2. prompt the LLM       — deterministic, SHA-anchored
3. parse the proposal   — typed CurationProposal
4. render review .md    — humans tick approve / modify / reject
5. finalize             — approved entities → registry yaml (next step)
```

## Layout

```
synapse/
├── synapse/                          # the package
│   ├── registry/                     # typed loaders
│   │   ├── schemas.py                # pydantic models
│   │   ├── glossary.py
│   │   ├── metric_catalog.py
│   │   ├── table_catalog.py
│   │   └── corpus_signals.py         # reuses lumi_final's sqlglot parser
│   └── curation/                     # the pre-work pipeline
│       ├── bundle.py                 # assemble all 7 sources
│       ├── prompt.py                 # deterministic prompt builder
│       ├── llm.py                    # Vertex Gemini wrapper
│       ├── parser.py                 # LLM YAML → CurationProposal
│       └── review.py                 # → ENTITIES_FOR_REVIEW.md
├── scripts/
│   ├── curate_entities.py            # the main entry
│   └── probe_curation.py             # end-to-end probe
├── tests/                            # pytest, with fixtures
│   ├── conftest.py
│   ├── fixtures/                     # CSV + YAML samples
│   ├── test_glossary_loader.py
│   ├── test_metric_catalog_loader.py
│   ├── test_table_catalog_loader.py
│   ├── test_bundle_assembly.py
│   ├── test_prompt_determinism.py
│   ├── test_parser.py
│   ├── test_review_render.py
│   └── test_llm_caller.py
├── data/
│   ├── registries/raw/               # drop the 3 CSVs here
│   └── proposals/                    # LLM artifacts per run
├── review_queue/                     # ENTITIES_FOR_REVIEW.md lands here
└── pyproject.toml
```

## Setup

```bash
pip install -e ./synapse
pip install -e ./synapse[all]   # adds sqlglot + google-genai + openpyxl + pytest
```

Or minimal install — only the registry loaders / prompt / parser:

```bash
pip install -e ./synapse                   # pydantic + pyyaml only
pip install sqlglot                        # for corpus_signals
pip install google-genai                   # for live Vertex calls
pip install openpyxl                       # if reading xlsx
```

Env vars (all reused from lumi_final's setup):

```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/path/to/sa-key.json
export LUMI_VERTEX_PROJECT=your-vertex-project
export LUMI_VERTEX_LOCATION=global
```

## Drop the three CSVs

Place each at:

```
synapse/data/registries/raw/glossary.csv
synapse/data/registries/raw/metric_catalog.csv
synapse/data/registries/raw/table_catalog.csv
```

The loaders are tolerant of column-name variation (Symbol/Acronym,
BU/BusinessUnit, etc.) — they'll match common headers automatically.

## Run the probe first (no LLM cost)

```bash
python synapse/scripts/probe_curation.py
```

Validates every layer independently:
1. Loaders parse the fixture CSVs
2. MDM digest loader reads `lumi_final/data/mdm_cache/`
3. Evidence bundle assembles cleanly
4. Prompt builds deterministically (same SHA twice)
5. LLM dry-run returns the prompt as response
6. Parser handles the fixture LLM response
7. Review renderer produces well-formed markdown

If all pass: green light to run the real curation.

## Run the curation

```bash
# Dry-run first — writes prompt to disk, doesn't call Vertex:
python synapse/scripts/curate_entities.py --dry-run

# Real call (costs ~$0.05):
python synapse/scripts/curate_entities.py
```

Outputs:
- `synapse/data/proposals/<ts>__prompt.txt`     exact prompt sent
- `synapse/data/proposals/<ts>__response.txt`   raw LLM output
- `synapse/data/proposals/<ts>__proposal.json`  parsed CurationProposal
- `synapse/review_queue/ENTITIES_FOR_REVIEW.md` human review surface

Open the markdown, tick approve/modify/reject per entity, resolve
flagged ambiguities. The next script (to be built) reads back the
finalized markdown and writes `data/registries/entities.yaml` — the
authoritative backbone every graph extractor will reference.

## Run the tests

```bash
pytest synapse/tests/ -v
```

All tests are deterministic — no network, no LLM calls, no real BQ.
The LLM live path is exercised by `probe_curation.py --live-llm`.

## What's deliberately NOT here

- Graph projection (next batch — `synapse/graph/`)
- Calibration / promotion / decay (Phase 3)
- LookML renderer (Phase 4)
- Radix API surface (Phase 5)

The entity registry has to be approved before any of those have meaningful targets.

## Reuse from `lumi_final/`

We import from `lumi_final/lumi/` directly (added to `sys.path`) for:
- `lumi.sql_to_context.parse_sqls` — the sqlglot extractor
- `lumi_final/data/mdm_cache/` — the existing MDM digests
- `lumi_final/data/gold_queries/` — the SQL corpus

`lumi_final/` is **not deprecated**. It stays alongside `synapse/` as
the source of canonical data and the reference implementation for the
extraction layer.
