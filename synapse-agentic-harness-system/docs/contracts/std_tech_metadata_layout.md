# std_tech_metadata — per-table layout contract (authoritative)

Provided by the user from the real laptop files, 2026-08-26. One file
per table: `sources/std_tech_metadata/<table_name>.json`. The loader
(`sahs/loaders/sources/vocab.py`) harvests entries by signature and
MUST keep parsing exactly this shape; fixtures mirror it.

## Layer 1 — envelope

| key | example | purpose |
|---|---|---|
| `dataset` | `acqdw_acquisition_us` | table name |
| `appl_id` | `600001868` | MDM application registry id |
| `page_info` | `{total_pages: 1, downloaded_elements: 2}` | MDM API pagination |
| `tech_metadata_list` | `[ {...} ]` | the payload — one or more tech entries |

## Layer 2 — `tech_metadata_list[i]` (table-level metadata)

`datasource` (GCP project, e.g. `axp-lumi`) · `datasetGroup` (BQ
dataset) · `dataserver` (`Lumi`) · `technology` (`BigQuery`) ·
`isActive` (`Y`) · `isLineageExist` (`Y`) · `datasystem`
(`NGBD – Lumi Metadata Management`) — plus Layers 3 and 4 nested here.

## Layer 3 — `datasetAttribute` (properties & governance)

- identity: `table_name`, `business_name`, `description`,
  `data_category` → `data_sub_category`
- technical: `type` (`DERIVED`…), `load_type`
  (`SNAPSHOT_DEDUPE_MAX`…), `is_partitioned`, `data_type_name`
  (`ODL`/`SOR` — the layer type), `target_system`
- sensitivity: `has_pii`, `has_oncop`, `has_gdpr` (each feeds the E1
  union-most-restrictive plane; oncop/gdpr also emit `has_policy`
  edges)
- `ownership` dict: DI tech owner, VP, business owner, VP

## Layer 4 — `pde[]` (one element per column)

- `pdeAttribute`: `column_name`, `data_type_name`, `description`,
  `business_name`, `position`, `column_length_number`,
  `nullable_indicator`, `primary_key_indicator`,
  `partition_indicator`, `pii_role_id` (null when not PII),
  `derived_logic` (SQL if computed — unparsed today; a future
  semantic source)
- `businessMetadata[]`: `businessTermName`, `businessTermDescription`,
  `businessTermId` (null where no formal id assigned), `sourceName`
  (`LumiMDM`), `sourceType` (`Declared`), `confidenceScore`

## Loader contract

The harvest accepts BOTH this envelope (table name at Layer 1, payload
at Layer 2+) and a flat entry (`dataset` + `pde`/`datasetAttribute`
in one object), under any outer wrapper. A file yielding zero
signature matches quarantines loudly. The per-table directory and the
combined `std_tech_metadata_all.json` are both accepted; the combined
file wins when both exist — delete the stale combined file when the
per-table directory is the operative source.
