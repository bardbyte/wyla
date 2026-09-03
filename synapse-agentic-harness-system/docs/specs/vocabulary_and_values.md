# Vocabulary and values — five sources, one understanding of the ask

Status: BUILT, first cut (2026-09-03). How the acronym corpus, its two
generated views, the governed term catalog, and the value-meaning
index enter the graph, and how the agent uses them to understand a
question before it touches a table.

## 1 · The sources and what each one is

| file | rows | what it is | verdict |
| --- | ---: | --- | --- |
| `data_cleaned.csv` | 12,331 | the combined vocabulary: Symbol + Definition + Business Unit + Global Region + Entry Type (10,536 acronyms, 1,795 glossary terms; 1,692 symbols with several meanings) | **source of record** — already loaded as `acr:<symbol>@<bu>@<region>` nodes, witness `atlas` |
| `business_terms.csv` | 3,040 | the governed Atlas catalog: id, name, status (Approved 1,821 · Candidate 1,107 · Under Review 95 · Rejected 17) | **source of record** — already loaded as `term:atlas:<id>` nodes; `std_tech_metadata` links columns to them (`mapped_term`) |
| `potential_common_word_acronyms.csv` | 743 | acronyms whose symbols are ordinary words (CARE, FIRST, REST, SMART, YES); every symbol already in the corpus | **a guard list, never a second vocabulary** — becomes `common_word: true` on the acronym node |
| `glossary_terms.csv` | 1,794 | the corpus filtered to Entry Type = Glossary Term; drifts from it by non-breaking spaces, whitespace, blank business units (1,712 of 1,775 rows match exactly) | **a generated view** — never loaded twice; the drift is counted in the build report and the file is ledgered as deferred |
| `low_cardinality_synonyms_index.json` | 4,612 mappings | what a low-cardinality stored value MEANS in one column of one table: `"1"` in `kyc_check_confirmed__c` is "KYC done"; 52 tables, 658 columns, 3,276 table/value keys | **source of record for meanings** — `meanings` on the column's `domain:` node, witness `lumi` |

The rule that decides the table: a file is a source when it carries a
fact no other file carries; it is a view when every fact in it is
already in a source. Views are counted, never loaded, so the census
stays honest and nothing is double-witnessed.

## 2 · Ingestion — where each fact lands in the graph

```
data_cleaned.csv ──► acr:<symbol>@<bu>@<region>   props: symbol, definition,
                                                  entry_type, business_unit,
                                                  region, common_word
                     acr ──alias_of──► term:atlas:<id>   when the expansion IS
                                                          a governed term's name
potential_common_word_acronyms.csv ──► the common_word flag above (+ a count
                                       of symbols the corpus lacks)
glossary_terms.csv ──► report only: matched_exact / drifted / missing
business_terms.csv ──► term:atlas:<id>   props: name, status
std_tech_metadata  ──► col ──mapped_term──► term
low_cardinality_synonyms_index.json ──► domain:<table>.<column>   props: meanings [{value, synonym}]
                       (beside the profiler's observed values; a column the
                        profiler never domained gets its domain minted from
                        the lookup, with its has_domain edge — counted)
```

- **Scope is identity.** The same symbol keeps every meaning it has,
  one node per (symbol, business unit, region). Nothing collapses
  "ACE" across GMNS and USCS; the agent picks by scope.
- **Witnesses.** Vocabulary and the guard list testify as `atlas`;
  the value lookup as `lumi`. Both are full graph citizens (census,
  cards, steward evidence) and neither feeds a resolver-ranked
  feature — they steer understanding, they never rank metrics.
- **Unknown is skipped, never invented.** A lookup entry naming a
  table or column the graph does not know is counted
  (`skipped_unknown_table`, `skipped_unknown_column`) and left out.
- **The ledger.** `potential_common_word_acronyms.csv` and
  `low_cardinality_synonyms_index.json` (or `value_lookup.json`) are
  consumed; `glossary_terms.csv` is deferred
  with its reason; a census run counts all five.

## 3 · What the build serves

- `indexes/vocab.jsonl` — acronym rows now carry `common_word`.
- `indexes/domains.jsonl` — each domain row carries `meanings`.
- `indexes/value_meanings.jsonl` (new) — one flat row per (table,
  column, value, synonym), the index a phrase resolves against.
- The digest's **words** section tells the model the three rules
  below, with the counts of this build.

## 4 · How the agent uses them to understand the ask

1. **Acronyms are scoped.** `search(kind="vocab")` returns every
   meaning with its business unit and region; when the ask names a
   business area (the business map), the agent prefers that area's
   meaning and says which one it used. A settled disambiguation
   ("by ACE you mean …") is a memory, never a graph edit.
2. **Common words are not acronyms until written as one.** A
   guard-listed symbol expands only when the ask writes it in capitals
   (REST, CARE) or asks for vocabulary explicitly; "rest of the
   quarter" stays English. The hit carries the guard so the model
   knows why.
3. **Phrases are stored codes.** `search(kind="values")` turns "KYC
   done" or "Approved" into the column, the code, and the predicate
   (`kyc_check_confirmed__c = '1'`). `sample_values` shows a column's
   codes beside their meanings. The literal hook on `run_sql` catches
   the other direction: a WHERE literal that is a meaning on record
   ("… = 'Approved'") comes back as a warning naming the code. The
   answer says the meaning; the SQL filters on the code.
4. **Glossary definitions answer "what is X".** A glossary term's
   definition is the answer to a vocabulary question, with its scope,
   before any table is named.

## 5 · What is deliberately not done yet

- The result renderer does not yet decode codes into meanings in
  tables and charts (a `'D'` column shows `'D'`); the model says the
  meaning in prose. Decoding in the artifact renderer is the next
  step once the meanings prove stable.
- Ranking vocabulary hits by the ask's business area is a preference
  in the prompt, not a scorer; the memory of a disambiguation is the
  agent's, per user.
- The Atlas ↔ vocabulary link is exact-name only (one edge on the
  real data, "Product Description"); a fuzzy link is a steward
  decision, not a loader's.
