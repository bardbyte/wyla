# lookml-enrichment-pipeline

Multi-agent pipeline that enriches LookML views from analytical SQL queries +
MDM business metadata + LLM reasoning, projects the resulting signals into a
queryable semantic graph (Apache AGE on Postgres), and renders production-grade
LookML as the first downstream consumer.

## Layout

```
lumi_final/               the active codebase
├── lumi/                 pipeline modules (parse, enrich, validate, publish)
│   ├── sql_to_context.py sqlglot-based extraction (Layer 1-4 signals)
│   ├── mdm.py            MDM client (cached + http)
│   ├── ontology_store.py event store + hook emitters
│   └── semantic_graph/   Apache AGE projection (schema, projector, replay)
├── apps/                 ADK apps (curator, vertex_smoke)
├── scripts/              probes (mdm, bq, age, corpus phase 0/1/2)
└── tests/                pytest suite
```

## Setup

```bash
pip install -e lumi_final
pip install "psycopg[binary]" truststore openpyxl sqlglot google-cloud-bigquery
```

Set the following env vars (see `lumi_final/lumi/config.py` for the full list):

```bash
export LUMI_VERTEX_PROJECT=your-vertex-project
export LUMI_BQ_PROJECT=your-bq-project
export LUMI_MDM_API_BASE=https://your-mdm-endpoint/...
export LUMI_GITHUB_API_BASE=https://api.github.com  # or your GHE
export LUMI_GITHUB_REPO=owner/repo
export GOOGLE_APPLICATION_CREDENTIALS=~/path/to/sa-key.json
```

## Run the pipeline

```bash
cd lumi_final

# Full pipeline: Excel of gold queries → SQLs → MDM refresh → events → AGE graph
LUMI_AGE_ENABLED=1 python scripts/probe_corpus_phase012.py \
    --from-excel ~/path/to/gold_queries.xlsx \
    --refresh-mdm \
    --fresh
```

## Tests

```bash
pytest lumi_final/tests/ -v
```
