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
  `derived_logic` (SQL if computed — retained whole as a doc node,
  unparsed; a future semantic source)
- `businessMetadata[]`: `businessTermName`, `businessTermDescription`,
  `businessTermId` (null where no formal id assigned), `sourceName`
  (`LumiMDM`), `sourceType` (`Declared`), `confidenceScore`

## Field utilization (pinned)

Every field above reaches the graph. `page_info` alone is excluded —
it describes the API call, not the table. Where each lands:

| field | lands as |
|---|---|
| `dataset` | table identity (through the E1 crosswalk) |
| `appl_id` | `appl_id` prop |
| `datasource` / `datasetGroup` / `dataserver` / `datasystem` / `technology` | `project_atlas` / `dataset_group_atlas` / `data_server_atlas` / `data_system_atlas` / `technology_atlas` props |
| `isActive` / `isLatest` / `isLineageExist` | `is_active_atlas` / `is_latest_atlas` / `is_lineage_exist_atlas` props |
| `table_name` / `description` / `business_name` | `table_name_atlas` / `description_atlas` / `business_name_atlas` props |
| `data_category` / `data_sub_category` | props of the same name |
| `type` / `load_type` / `is_partitioned` / `target_system` | `table_type_atlas` / `load_type_atlas` / `is_partitioned_atlas` / `target_system_atlas` props |
| `data_type_name` (Layer 3) | `layer_type` prop |
| `has_pii` / `has_oncop` / `has_gdpr` | `has_*_atlas` props **and** a `has_policy` edge each |
| `ownership` | `ownership_atlas` prop **and** an `owned_by` edge per key naming an owner or a VP (an id such as `car_id` stays a prop — an identifier is not a person) |
| `pii_columns[]` | per column: `pii_role_id` where the pde listing carried none, `pii_role_id_table_declared` where the two disagree, a `has_policy` edge, and a minted column node where the pde listing missed it entirely |
| `pdeRelPath` | column identity |
| `column_name` / `position` / `column_length_number` | `column_name_atlas` / `ordinal_atlas` / `column_length` props |
| `nullable_indicator` / `primary_key_indicator` / `partition_indicator` | `nullable_atlas` / `is_primary_key_atlas` / `is_partitioning_atlas` props |
| `description` / `business_name` / `data_type_name` (Layer 4) | `description_atlas` / `business_name_atlas` / `data_type_atlas` props |
| `pii_role_id` / `sde_group` | props of the same name (+ `has_policy` edge) |
| `derived_logic` | a `doc:derived_logic_<fp>` node behind a `described_by` edge — retained whole, unparsed, ready for a canon pass |
| `businessTermId` / `businessTermName` / `businessTermDescription` | the `term:` node's identity / `name` / `description`. **The id resolves first**, the name is the fallback, and the resolution used is recorded on the edge as `matched_on` |
| `sourceName` / `sourceType` / `confidenceScore` | `mapped_term` edge props |

Two conventions hold throughout:

- **`_atlas` suffix** marks a fact another witness (BQ or Lumi) also
  asserts, so E1 can arbitrate; an Atlas-only fact carries a bare name.
- **Absent is unknown, never false.** `isActive` unsent stays `None` and
  the prop is omitted — a fold is last-wins per key, so writing an empty
  value would erase what another registration did carry.

Fields the feed documents but does not send are simply absent; nothing
needs changing when they start arriving.

### What the feed sends that this table does not name

The loader picks fields by name, so a key the real feed carries that
the fixture never had is dropped at the record boundary — with no
trace, unless something enumerates the feed and diffs it. That is
`scripts/std_tech_keys.py`: it walks every entry the loader would
harvest, counts every key at every layer, and marks each against
`STD_TECH_CONSUMED_KEYS` (pinned in `sahs/loaders/sources/vocab.py`,
the one list the loader and the census share):

```bash
python scripts/std_tech_keys.py $SRC/std_tech_metadata          # the real archive
python scripts/std_tech_keys.py tests/fixtures/sources/std_tech_metadata --strict   # CI: 0 UNCONSUMED
```

An `UNCONSUMED` row is a decision to make — a prop, an edge, or a
pinned deferral with a reason — never a silent drop. `ownership` keys
report `edge+prop` (recognised as a person → `owned_by` edge) or
`prop only` (kept whole in `ownership_atlas`, not an owner node): a
steward or a custodian the heuristic does not know shows up there.

## Loader contract

The harvest accepts BOTH this envelope (table name at Layer 1, payload
at Layer 2+) and a flat entry (`dataset` + `pde`/`datasetAttribute`
in one object), under any outer wrapper. A file yielding zero
signature matches quarantines loudly. The per-table directory and the
combined `std_tech_metadata_all.json` are both accepted; the combined
file wins when both exist — delete the stale combined file when the
per-table directory is the operative source.
