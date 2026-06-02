"""End-to-end probe for the entity-curation pre-step.

Validates every layer in isolation, then runs the full pipeline
with a deterministic fixture so you see exactly what would land on
the LLM (without paying for the call).

Sections (each independently PASS / FAIL):

    1. Loader smoke      — each registry loader works on fixture CSVs
    2. Bundle assembly   — EvidenceBundle assembles from real or fixture data
    3. Prompt build      — prompt is built deterministically; SHA stable
    4. LLM call          — dry-run path returns the prompt; real path
                           gated on env + flag
    5. Parse              — fixture LLM response parses cleanly into
                           CurationProposal
    6. Review render      — markdown render produces well-formed output

Usage:

    # Default — uses lumi_final caches + repo fixtures, no LLM call:
    python scripts/probe_curation.py

    # Test the real LLM call (costs ~$0.05):
    python scripts/probe_curation.py --live-llm

    # Use specific paths for real registry CSVs:
    python scripts/probe_curation.py \\
        --glossary data/registries/raw/glossary.csv \\
        --metric-catalog data/registries/raw/metric_catalog.csv \\
        --table-catalog data/registries/raw/table_catalog.csv

Exit codes:
    0 — all sections passed
    1 — config / inputs broken (missing files etc.)
    2 — a section failed
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
sys.path.insert(0, str(SYNAPSE_ROOT))

from synapse.curation import (
    build_prompt,
    parse_llm_output,
    prompt_sha256,
    render_review_markdown,
)
from synapse.curation.bundle import assemble_evidence_bundle, load_mdm_digests
from synapse.curation.llm import call_gemini
from synapse.registry import EvidenceBundle
from synapse.registry.glossary import (
    ambiguous_symbols,
    index_by_symbol,
    load_glossary,
)
from synapse.registry.metric_catalog import load_metric_catalog
from synapse.registry.table_catalog import in_scope, load_table_catalog


# ── Pretty print ──────────────────────────────────────────────


def _hdr(msg: str) -> None:
    print(f"\n\033[1;36m═══ {msg} ═══\033[0m")


def _sec(name: str) -> None:
    print(f"\n\033[1;34m── {name} ──\033[0m")


def _pass(msg: str) -> None:
    print(f"  \033[1;32m✓\033[0m {msg}")


def _fail(msg: str) -> None:
    print(f"  \033[1;31m✗\033[0m {msg}")


def _info(msg: str) -> None:
    print(f"    \033[2m{msg}\033[0m")


def _warn(msg: str) -> None:
    print(f"  \033[1;33m!\033[0m {msg}")


# ── Section runners ───────────────────────────────────────────


def section_loaders(
    glossary_path: Path,
    metric_catalog_path: Path,
    table_catalog_path: Path,
) -> int:
    """Verify each loader returns well-formed pydantic objects."""
    _sec("1. Loader smoke")
    failures = 0

    # Glossary
    try:
        g = load_glossary(glossary_path)
        _pass(f"glossary: {len(g)} entries")
        if g:
            grouped = index_by_symbol(g)
            _info(f"unique symbols: {len(grouped)}")
            ambig = ambiguous_symbols(g)
            if ambig:
                _info(f"ambiguous (≥2 defs): {len(ambig)} symbols — "
                      f"examples: {list(ambig)[:5]}")
    except Exception as e:  # noqa: BLE001
        _fail(f"glossary: {type(e).__name__}: {e}")
        failures += 1

    # Metric catalog
    try:
        m = load_metric_catalog(metric_catalog_path)
        _pass(f"metric_catalog: {len(m)} metrics")
        if m:
            with_calc = sum(1 for x in m if x.calculation_logic)
            with_syns = sum(1 for x in m if x.business_synonyms)
            _info(f"with calculation_logic: {with_calc}")
            _info(f"with business_synonyms: {with_syns}")
    except Exception as e:  # noqa: BLE001
        _fail(f"metric_catalog: {type(e).__name__}: {e}")
        failures += 1

    # Table catalog
    try:
        t = load_table_catalog(table_catalog_path)
        in_dmp = in_scope(t, require_dmp=True)
        finance = in_scope(t, require_dmp=True, company_domains={"Finance"})
        _pass(f"table_catalog: {len(t)} total, {len(in_dmp)} in DMP, "
              f"{len(finance)} in Finance+DMP")
    except Exception as e:  # noqa: BLE001
        _fail(f"table_catalog: {type(e).__name__}: {e}")
        failures += 1

    return failures


def section_mdm(mdm_cache_dir: Path) -> int:
    _sec("2. MDM digest loader")
    if not mdm_cache_dir.exists():
        _warn(f"mdm_cache_dir does not exist ({mdm_cache_dir}) — skipping")
        return 0
    try:
        d = load_mdm_digests(mdm_cache_dir, max_sample_columns=20)
        _pass(f"loaded {len(d)} MDM digests")
        if d:
            with_keys = sum(1 for x in d if x.key_columns)
            with_pii = sum(1 for x in d if x.pii_columns)
            _info(f"with key_columns (PK / dedupe): {with_keys}")
            _info(f"with PII columns flagged: {with_pii}")
        return 0
    except Exception as e:  # noqa: BLE001
        _fail(f"mdm digest load: {type(e).__name__}: {e}")
        return 1


def section_bundle(
    glossary_path: Path,
    metric_catalog_path: Path,
    table_catalog_path: Path,
    mdm_cache_dir: Path,
    sql_corpus_dir: Path,
) -> tuple[int, EvidenceBundle | None]:
    _sec("3. Evidence bundle assembly")
    try:
        bundle = assemble_evidence_bundle(
            glossary_path=glossary_path,
            metric_catalog_path=metric_catalog_path,
            table_catalog_path=table_catalog_path,
            mdm_cache_dir=mdm_cache_dir,
            sql_corpus_dir=sql_corpus_dir,
            scope_description="probe — enterprise-wide curation",
        )
        _pass("bundle assembled cleanly")
        _info(f"tables={len(bundle.table_catalog)} "
              f"glossary={len(bundle.glossary)} "
              f"metrics={len(bundle.metric_catalog)} "
              f"mdm={len(bundle.mdm_digests)} "
              f"corpus_tokens={len(bundle.corpus_signals)} "
              f"queries={bundle.n_queries_analyzed}")
        return 0, bundle
    except Exception as e:  # noqa: BLE001
        _fail(f"bundle assembly: {type(e).__name__}: {e}")
        return 1, None


def section_prompt(bundle: EvidenceBundle) -> tuple[int, str, str]:
    _sec("4. Prompt build (determinism check)")
    try:
        prompt_a = build_prompt(bundle)
        prompt_b = build_prompt(bundle)
        sha_a = prompt_sha256(prompt_a)
        sha_b = prompt_sha256(prompt_b)
        if prompt_a != prompt_b:
            _fail("prompt is NOT deterministic across builds")
            return 1, prompt_a, sha_a
        if sha_a != sha_b:
            _fail("SHA256 disagreement (impossible — bug here)")
            return 1, prompt_a, sha_a
        _pass(f"prompt built deterministically — {len(prompt_a):,} chars, "
              f"sha256={sha_a[:16]}…")
        return 0, prompt_a, sha_a
    except Exception as e:  # noqa: BLE001
        _fail(f"prompt build: {type(e).__name__}: {e}")
        return 1, "", ""


def section_llm(prompt: str, *, live: bool) -> tuple[int, str, str]:
    _sec("5. LLM call")
    result = call_gemini(prompt, dry_run=not live)
    if result.dry_run:
        _warn(f"dry-run path — {result.error or 'no LLM call made'}")
        _info("To exercise the live path: --live-llm "
              "(needs GOOGLE_APPLICATION_CREDENTIALS)")
        return 0, "", result.model
    if result.error:
        _fail(f"LLM call failed: {result.error}")
        return 2, "", result.model
    _pass(f"LLM responded — {len(result.response_text):,} chars from "
          f"{result.model}")
    return 0, result.response_text, result.model


def section_parser(
    raw_response: str, model: str, sha: str, *, fixture_path: Path,
) -> int:
    _sec("6. Parser")
    text = raw_response
    if not text:
        # Use the fixture if there was no live response
        if not fixture_path.exists():
            _warn(f"no response text and fixture missing ({fixture_path}) — "
                  f"skipping")
            return 0
        text = fixture_path.read_text(encoding="utf-8")
        _info(f"using fixture: {fixture_path}")
    try:
        proposal = parse_llm_output(text, model_used=model, prompt_sha=sha)
        _pass(f"parsed {len(proposal.proposed_entities)} entities, "
              f"{len(proposal.ambiguities_flagged)} ambiguities, "
              f"{len(proposal.scope_observations)} observations")
        return 0
    except ValueError as e:
        _fail(f"parse failed: {e}")
        return 2


def section_review(
    raw_response: str, model: str, sha: str, *, fixture_path: Path,
) -> int:
    _sec("7. Review markdown render")
    text = raw_response or (
        fixture_path.read_text(encoding="utf-8")
        if fixture_path.exists() else ""
    )
    if not text:
        _warn("no LLM response and no fixture — skipping render check")
        return 0
    try:
        proposal = parse_llm_output(text, model_used=model, prompt_sha=sha)
        md = render_review_markdown(proposal)
        # Sanity checks on the markdown
        required = ["# Synapse — Entity Registry Proposal", "## Proposed entities"]
        missing = [r for r in required if r not in md]
        if missing:
            _fail(f"markdown missing required sections: {missing}")
            return 2
        _pass(f"markdown rendered ({len(md):,} chars) with all "
              f"required sections")
        return 0
    except Exception as e:  # noqa: BLE001
        _fail(f"review render: {type(e).__name__}: {e}")
        return 2


# ── Main ──────────────────────────────────────────────────────


def main() -> int:
    DEFAULT_GLOSSARY = SYNAPSE_ROOT / "tests" / "fixtures" / "glossary_sample.csv"
    DEFAULT_METRIC = (
        SYNAPSE_ROOT / "tests" / "fixtures" / "metric_catalog_sample.csv"
    )
    DEFAULT_TABLE = (
        SYNAPSE_ROOT / "tests" / "fixtures" / "table_catalog_sample.csv"
    )
    DEFAULT_RESPONSE_FIXTURE = (
        SYNAPSE_ROOT / "tests" / "fixtures" / "llm_response_sample.yaml"
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--glossary", type=Path, default=DEFAULT_GLOSSARY)
    parser.add_argument("--metric-catalog", type=Path, default=DEFAULT_METRIC)
    parser.add_argument("--table-catalog", type=Path, default=DEFAULT_TABLE)
    parser.add_argument(
        "--mdm-cache-dir", type=Path,
        default=REPO_ROOT / "lumi_final" / "data" / "mdm_cache",
    )
    parser.add_argument(
        "--sql-corpus-dir", type=Path,
        default=REPO_ROOT / "lumi_final" / "data" / "gold_queries",
    )
    parser.add_argument(
        "--response-fixture", type=Path, default=DEFAULT_RESPONSE_FIXTURE,
    )
    parser.add_argument(
        "--live-llm", action="store_true",
        help="Actually call Vertex Gemini (costs ~$0.05). Default is dry-run.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    _hdr("Probing synapse curation pipeline")
    _info(f"repo root: {REPO_ROOT}")
    _info(f"synapse root: {SYNAPSE_ROOT}")

    total_failures = 0

    # 1. Loaders
    f = section_loaders(args.glossary, args.metric_catalog, args.table_catalog)
    total_failures += f
    if f:
        _fail("Loader section failed — fix CSV inputs before proceeding")
        return 2

    # 2. MDM (optional)
    section_mdm(args.mdm_cache_dir)

    # 3. Bundle
    f, bundle = section_bundle(
        args.glossary, args.metric_catalog, args.table_catalog,
        args.mdm_cache_dir, args.sql_corpus_dir,
    )
    total_failures += f
    if f or bundle is None:
        return 2

    # 4. Prompt
    f, prompt, sha = section_prompt(bundle)
    total_failures += f
    if f or not prompt:
        return 2

    # 5. LLM
    f, response, model = section_llm(prompt, live=args.live_llm)
    total_failures += f

    # 6. Parser (uses fixture if no live response)
    total_failures += section_parser(
        response, model, sha, fixture_path=args.response_fixture,
    )

    # 7. Review render
    total_failures += section_review(
        response, model, sha, fixture_path=args.response_fixture,
    )

    _hdr("Summary")
    if total_failures == 0:
        _pass("all sections passed")
        _info("Ready to run scripts/curate_entities.py on real inputs.")
        return 0
    _fail(f"{total_failures} section(s) failed")
    return 2


if __name__ == "__main__":
    sys.exit(main())
