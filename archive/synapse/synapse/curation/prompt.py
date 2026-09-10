"""Deterministic prompt builder for entity curation.

The prompt is a function of the EvidenceBundle. Same bundle → same
bytes → same SHA256. That's the reproducibility anchor: any time we
re-run with the same evidence, we know the LLM saw the exact same
input, so any output drift is the model's, not ours.

Structure:
    1. System role — "you propose entities from grounded evidence,
       never invent"
    2. Output contract — exact YAML schema + example
    3. Section A: Table catalog (compact)
    4. Section B: MDM table digests (table-by-table; only sample cols)
    5. Section C: Glossary table (compact, ambiguous symbols highlighted)
    6. Section D: Metric catalog table
    7. Section E: Top noun-frequency from corpus

The system role is explicit about the four rules:
    - Every proposed entity must cite source_evidence
    - No invention beyond the evidence
    - PascalCase canonical names
    - Flag every ambiguity rather than hide it
"""

from __future__ import annotations

import hashlib

from synapse.registry import EvidenceBundle


_SYSTEM_ROLE = """\
You are the lead knowledge-graph engineer responsible for proposing the
canonical entity registry for an enterprise data warehouse. You synthesise
signals from a curated glossary, a curated metric catalog, a table catalog,
per-table MDM digests, and aggregated corpus signals from real analyst
queries.

You operate under four hard rules:

1. GROUNDING. Every proposed entity MUST cite specific source evidence
   (tables, columns, metrics, acronyms, data_categories that point at it).
   No entity without provenance.

2. NO INVENTION. Only propose entities the evidence supports. If you can
   only justify an entity from a single weak signal, flag it as
   `llm_confidence < 0.5` and write a clear human_review_note.

3. NAMING. Canonical names are PascalCase, singular nouns
   (Cardmember, not Cardmembers; CardProduct, not card_product). Descriptions
   are one to two sentences.

4. DISAMBIGUATION. When the same symbol or term means different things in
   different business units (the glossary shows this for AA, ABP, ACS, ADR,
   ADS), flag each meaning as a separate entity OR explicitly call out the
   ambiguity in `ambiguities_flagged`.

You will receive evidence in five sections (A-E). You will produce one
YAML document matching the OUTPUT CONTRACT below. The YAML must parse — no
prose around it, no markdown fences.
"""


_OUTPUT_CONTRACT = """\
OUTPUT CONTRACT — return ONLY a YAML document with this exact shape:

```yaml
proposed_entities:
  - canonical_name: <PascalCase>
    description: <1-2 sentences>
    source_evidence:
      tables: [<table_name>, ...]
      columns: [<column_name>, ...]
      metrics: [<metric_technical_name>, ...]
      acronyms: [<symbol>, ...]
      data_categories: [<MDM data_category>, ...]
    properties:
      id_columns: [<column_name>, ...]
      common_attributes: [<column_name>, ...]
      coded_values: ["<raw> = <meaning>", ...]   # if any code resolution surfaced
    parent_entity: <PascalCase or null>
    relationships:
      - type: has_many   # has_many | has_one | many_to_many | is_a | part_of | describes
        target_entity: <PascalCase>
        via_column: <column_name or null>
        cardinality_evidence: <free text or null>
    llm_confidence: <float 0..1>
    human_review_notes: <free text — flag ambiguities, missing evidence, anything>

ambiguities_flagged:
  - <one-sentence statement of an ambiguity the human must resolve>

scope_observations:
  - <one-sentence observation about coverage, missing tables, etc.>
```

Return the YAML alone. No preamble. No closing remarks. No code fences.
"""


def _fmt_table_catalog(bundle: EvidenceBundle) -> str:
    lines = ["### Section A — Table catalog (scope + domain)"]
    lines.append("| table | IS IN DMP | company_domain | data_domain |")
    lines.append("|---|---|---|---|")
    for t in bundle.table_catalog:
        lines.append(
            f"| `{t.table_name}` | "
            f"{'Yes' if t.is_in_dmp else 'No'} | "
            f"{t.company_domain or '—'} | "
            f"{t.data_domain or '—'} |"
        )
    return "\n".join(lines)


def _fmt_mdm_digests(bundle: EvidenceBundle) -> str:
    lines = ["### Section B — MDM table digests"]
    lines.append("_Per-table: business name, description, data_category, "
                 "key columns (PK / dedupe), PII columns, sample columns._\n")
    for d in bundle.mdm_digests:
        lines.append(f"#### `{d.table_name}`  ({d.n_columns} cols)")
        lines.append(f"- bq_fqn: `{d.bq_fqn or '—'}`")
        lines.append(f"- business_name: {d.table_business_name or '—'}")
        lines.append(f"- description: {(d.table_description or '—')[:300]}")
        lines.append(f"- data_category: {d.data_category or '—'}  | "
                     f"sub: {d.data_sub_category or '—'}")
        if d.key_columns:
            lines.append("- **key columns**:")
            for k in d.key_columns:
                lines.append(
                    f"    - `{k.get('name')}` ({k.get('role')}): "
                    f"{k.get('business_name') or ''} — "
                    f"{(k.get('description') or '')[:140]}"
                )
        if d.pii_columns:
            lines.append(f"- PII columns: {', '.join(d.pii_columns[:10])}"
                         f"{' …' if len(d.pii_columns) > 10 else ''}")
        if d.sample_columns:
            lines.append("- sample columns:")
            for c in d.sample_columns:
                lines.append(
                    f"    - `{c.get('name')}` ({c.get('type') or '?'}): "
                    f"{c.get('business_name') or ''} — "
                    f"{(c.get('description') or '')[:120]}"
                )
        lines.append("")
    return "\n".join(lines)


def _fmt_glossary(bundle: EvidenceBundle) -> str:
    lines = ["### Section C — Glossary (acronyms + synonyms with context)"]
    lines.append("| symbol | definition | business_unit | region | type |")
    lines.append("|---|---|---|---|---|")
    for g in bundle.glossary:
        lines.append(
            f"| `{g.symbol}` | {g.definition[:120]} | "
            f"{g.business_unit or '—'} | "
            f"{g.region or '—'} | "
            f"{g.entry_type or '—'} |"
        )
    # Note ambiguous symbols explicitly
    seen: dict[str, set[str]] = {}
    for g in bundle.glossary:
        seen.setdefault(g.symbol.lower(), set()).add(g.definition)
    ambiguous = sorted(s for s, defs in seen.items() if len(defs) > 1)
    if ambiguous:
        lines.append("")
        lines.append(f"**Ambiguous symbols (multiple defs):** "
                     f"{', '.join(f'`{s}`' for s in ambiguous[:20])}"
                     f"{' …' if len(ambiguous) > 20 else ''}")
    return "\n".join(lines)


def _fmt_metric_catalog(bundle: EvidenceBundle) -> str:
    lines = ["### Section D — Metric catalog"]
    lines.append("| technical_name | business_name | domain | grain | "
                 "primary_data_product | synonyms |")
    lines.append("|---|---|---|---|---|---|")
    for m in bundle.metric_catalog:
        lines.append(
            f"| `{m.technical_name}` | "
            f"{m.business_name or '—'} | "
            f"{m.associated_domain or '—'} | "
            f"{m.metric_grain or '—'} | "
            f"{m.primary_data_product or '—'} | "
            f"{', '.join(m.business_synonyms[:5]) or '—'} |"
        )
    # Include calculation_logic separately (often long)
    has_calc = [m for m in bundle.metric_catalog if m.calculation_logic]
    if has_calc:
        lines.append("\n_Calculation logic (sampled):_")
        for m in has_calc[:30]:
            calc = (m.calculation_logic or "")[:240]
            lines.append(f"- `{m.technical_name}`: `{calc}`")
    return "\n".join(lines)


def _fmt_corpus_signals(bundle: EvidenceBundle) -> str:
    lines = ["### Section E — Corpus noun-frequency "
             f"(top {len(bundle.corpus_signals)} of "
             f"{bundle.n_queries_analyzed} queries)"]
    lines.append("| token | total | top roles | top tables |")
    lines.append("|---|---:|---|---|")
    for s in bundle.corpus_signals[:80]:
        # Top 3 roles by count
        top_roles = sorted(
            s.role_counts.items(), key=lambda kv: -kv[1],
        )[:3]
        roles_str = ", ".join(f"{r}:{n}" for r, n in top_roles)
        tables_str = ", ".join(s.appears_in_tables[:3])
        if len(s.appears_in_tables) > 3:
            tables_str += f" (+{len(s.appears_in_tables) - 3})"
        lines.append(
            f"| `{s.token}` | {s.occurrence_count} | {roles_str} | "
            f"{tables_str} |"
        )
    return "\n".join(lines)


def build_prompt(bundle: EvidenceBundle) -> str:
    """Render the full prompt deterministically."""
    parts = [
        _SYSTEM_ROLE.strip(),
        "",
        "## SCOPE",
        bundle.scope_description,
        f"_Tables in catalog: {len(bundle.table_catalog)} "
        f"({sum(1 for t in bundle.table_catalog if t.is_in_dmp)} in DMP scope). "
        f"MDM digests: {len(bundle.mdm_digests)}. "
        f"Glossary entries: {len(bundle.glossary)}. "
        f"Metrics: {len(bundle.metric_catalog)}. "
        f"Corpus queries analyzed: {bundle.n_queries_analyzed}._",
        "",
        "## EVIDENCE",
        "",
        _fmt_table_catalog(bundle),
        "",
        _fmt_mdm_digests(bundle),
        "",
        _fmt_glossary(bundle),
        "",
        _fmt_metric_catalog(bundle),
        "",
        _fmt_corpus_signals(bundle),
        "",
        "## OUTPUT",
        _OUTPUT_CONTRACT.strip(),
    ]
    return "\n".join(parts)


def prompt_sha256(prompt: str) -> str:
    """Deterministic hash of the prompt — proves reproducibility."""
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()
