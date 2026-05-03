# LUMI pipeline — implementation context

Loaded when working in `lumi/`. Parent CLAUDE.md has the rules.
This file has the reference data you need to write correct code.

## API shapes (from real probe runs)

### MDM
```
Endpoint: https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas?tableName=<table>
No auth (VPN-gated). Response is ARRAY[1], not dict.

data = response.json()[0]
columns   = data["schema"]["schema_attributes"]       # list of column dicts
col_desc  = col["attribute_details"]["attribute_desc"] # the useful description
col_name  = col["attribute_details"]["business_name"]  # canonical name
col_type  = col["attribute_details"]["attribute_type"]
table_desc = data["dataset_details"]["data_desc"]
table_name = data["dataset_details"]["business_name"]
pii_flag   = col["sensitivity_details"]["is_pii"]
```
~60-70% of columns have meaningful `attribute_desc`. No synonyms. No allowed_values.
Sometimes has `external_reference_details` for cross-table relationships.

### GitHub Enterprise
```
API base: https://github.aexp.com/api/v3
Repo: amex-eng/prj-d-lumi-gpt-semantic
Auth: Classic PAT with repo + read:org scopes, SSO-authorized for amex-eng org
```
PAT returns 404 (not 403) without SSO authorization. Diagnose: `/user/orgs` returns `[]`.

### Vertex AI
```
Project: prj-d-ea-poc
Location: global (NOT us-central1)
Model: gemini-3.1-pro-preview (NOT gemini-3-pro-preview)
SA: svc-d-lumigct-hyd@prj-d-ea-poc.iam.gserviceaccount.com
```
Corporate TLS: `truststore.inject_into_ssl()` in `__init__.py`.

## Schema quick reference

TableContext: table_name, columns_referenced, aggregations, case_whens,
  ctes_referencing_this, joins_involving_this, filters_on_this,
  date_functions, mdm_columns, existing_view_lkml, queries_using_this

EnrichedOutput: view_lkml, derived_table_views, explore_lkml,
  filter_catalog, metric_catalog, nl_questions

CoverageReport: total_queries, covered, coverage_pct, per_query,
  all_lookml_valid, top_gaps

See schemas.py for full definitions.

## Guardrail summary

Stage 1 (parse): SQL parses, CTEs complete, DAG acyclic, CTE tables discovered
Stage 2 (enrich): LookML syntax, descriptions 15-200ch, derived tables for CTEs,
  primary_key present, dates as dimension_group, join order, sql_table_name,
  value_format on measures, NL questions generated
Stage 3 (evaluate): coverage ≥90%, no regressions, structural filters baked, joins complete
Stage 4 (publish): all LookML lints, JSON valid, diff <50% per view

See guardrails.py for implementation.
