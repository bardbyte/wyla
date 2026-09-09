"""Entity curation — the pre-Week-1 step.

Builds an EvidenceBundle from every source, prompts Vertex Gemini for
a structured entity proposal, parses it, and renders a markdown for
human review.

Usage:

    # All defaults (uses LUMI_BQ_PROJECT etc. + cache dirs from lumi_final):
    python scripts/curate_entities.py

    # Dry run (don't hit Vertex; just write the prompt to disk so you
    # can paste it into a chat and inspect):
    python scripts/curate_entities.py --dry-run

    # Override paths if your CSVs live elsewhere:
    python scripts/curate_entities.py \\
        --glossary path/to/glossary.csv \\
        --metric-catalog path/to/metrics.csv \\
        --table-catalog path/to/tables.xlsx

Outputs (all under synapse/):
    data/proposals/<timestamp>__prompt.txt              the exact prompt
    data/proposals/<timestamp>__response.txt            raw LLM output
    data/proposals/<timestamp>__proposal.json           parsed CurationProposal
    review_queue/ENTITIES_FOR_REVIEW.md                 human review surface

Symlinks/copies the latest as `data/proposals/latest__*` for convenience.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
sys.path.insert(0, str(SYNAPSE_ROOT))

from synapse.curation import (
    assemble_evidence_bundle,
    build_prompt,
    parse_llm_output,
    prompt_sha256,
    render_review_markdown,
)
from synapse.curation.llm import DEFAULT_MODEL, call_gemini


# Defaults — point at lumi_final's existing caches/queries so we reuse data
DEFAULT_GLOSSARY = SYNAPSE_ROOT / "data" / "registries" / "raw" / "glossary.csv"
DEFAULT_METRIC_CATALOG = (
    SYNAPSE_ROOT / "data" / "registries" / "raw" / "metric_catalog.csv"
)
DEFAULT_TABLE_CATALOG = (
    SYNAPSE_ROOT / "data" / "registries" / "raw" / "table_catalog.csv"
)
DEFAULT_MDM_CACHE = REPO_ROOT / "lumi_final" / "data" / "mdm_cache"
DEFAULT_SQL_CORPUS = REPO_ROOT / "lumi_final" / "data" / "gold_queries"
DEFAULT_OUT_DIR = SYNAPSE_ROOT / "data" / "proposals"
DEFAULT_REVIEW_DIR = SYNAPSE_ROOT / "review_queue"


def _hdr(msg: str) -> None:
    print(f"\n\033[1;36m══ {msg} ══\033[0m")


def _pass(msg: str) -> None:
    print(f"  \033[1;32m✓\033[0m {msg}")


def _info(msg: str) -> None:
    print(f"    \033[2m{msg}\033[0m")


def _warn(msg: str) -> None:
    print(f"  \033[1;33m!\033[0m {msg}")


def _fail(msg: str) -> None:
    print(f"  \033[1;31m✗\033[0m {msg}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--glossary", type=Path, default=DEFAULT_GLOSSARY)
    parser.add_argument(
        "--metric-catalog", type=Path, default=DEFAULT_METRIC_CATALOG,
    )
    parser.add_argument(
        "--table-catalog", type=Path, default=DEFAULT_TABLE_CATALOG,
    )
    parser.add_argument("--mdm-cache-dir", type=Path, default=DEFAULT_MDM_CACHE)
    parser.add_argument("--sql-corpus-dir", type=Path, default=DEFAULT_SQL_CORPUS)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--review-dir", type=Path, default=DEFAULT_REVIEW_DIR)
    parser.add_argument(
        "--scope",
        default="Enterprise-wide entity curation (no domain filter)",
        help="One-line description of the scoped slice; goes into the prompt.",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Do not call the LLM; just write the prompt + a stub proposal "
             "you can hand-fill or paste into a chat.",
    )
    parser.add_argument(
        "--corpus-top-n", type=int, default=200,
        help="How many top noun-tokens from the corpus to include in the prompt.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # ── Verify inputs ──
    _hdr("Inputs")
    for label, path in (
        ("glossary", args.glossary),
        ("metric_catalog", args.metric_catalog),
        ("table_catalog", args.table_catalog),
    ):
        if not path.exists():
            _fail(f"{label}: not found at {path}")
            _info(f"Place the file under {path.parent} or pass --{label}")
            return 1
        _pass(f"{label}: {path}")
    if not args.mdm_cache_dir.exists():
        _warn(f"mdm_cache_dir not found ({args.mdm_cache_dir}) — "
              f"proceeding without MDM signal")
    else:
        n_mdm = len(list(args.mdm_cache_dir.glob("*.json")))
        _pass(f"mdm_cache: {n_mdm} table digests at {args.mdm_cache_dir}")
    if not args.sql_corpus_dir.exists():
        _warn("sql_corpus_dir not found — corpus signal will be empty")
    else:
        n_sql = len(list(args.sql_corpus_dir.glob("*.sql")))
        _pass(f"sql_corpus: {n_sql} SQL files at {args.sql_corpus_dir}")

    # ── Assemble bundle ──
    _hdr("Assembling evidence bundle")
    bundle = assemble_evidence_bundle(
        glossary_path=args.glossary,
        metric_catalog_path=args.metric_catalog,
        table_catalog_path=args.table_catalog,
        mdm_cache_dir=args.mdm_cache_dir,
        sql_corpus_dir=args.sql_corpus_dir,
        scope_description=args.scope,
        corpus_top_n=args.corpus_top_n,
    )
    _pass(f"glossary: {len(bundle.glossary)} entries")
    _pass(f"metric_catalog: {len(bundle.metric_catalog)} metrics")
    _pass(f"table_catalog: {len(bundle.table_catalog)} tables "
          f"({sum(1 for t in bundle.table_catalog if t.is_in_dmp)} in DMP)")
    _pass(f"mdm_digests: {len(bundle.mdm_digests)} tables")
    _pass(f"corpus_signals: top {len(bundle.corpus_signals)} tokens "
          f"from {bundle.n_queries_analyzed} queries")

    # ── Build prompt ──
    _hdr("Building prompt")
    prompt = build_prompt(bundle)
    sha = prompt_sha256(prompt)
    _pass(f"prompt size: {len(prompt):,} chars")
    _pass(f"prompt sha256: {sha[:16]}…")

    # ── Write outputs ──
    args.out_dir.mkdir(parents=True, exist_ok=True)
    args.review_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    prompt_path = args.out_dir / f"{ts}__prompt.txt"
    response_path = args.out_dir / f"{ts}__response.txt"
    proposal_path = args.out_dir / f"{ts}__proposal.json"
    review_path = args.review_dir / "ENTITIES_FOR_REVIEW.md"

    prompt_path.write_text(prompt, encoding="utf-8")
    _pass(f"prompt saved → {prompt_path}")

    # ── Call LLM (or dry-run) ──
    _hdr("LLM call")
    llm = call_gemini(prompt, model=args.model, dry_run=args.dry_run)
    if llm.dry_run:
        _warn(f"DRY-RUN mode ({llm.error or 'requested'}) — "
              f"no LLM call made")
        _info("To call the model, set GOOGLE_APPLICATION_CREDENTIALS and "
              "re-run without --dry-run")
        return 0
    if llm.error:
        _fail(f"LLM call failed: {llm.error}")
        return 2

    response_path.write_text(llm.response_text, encoding="utf-8")
    _pass(f"response saved → {response_path}")
    _info(f"model: {llm.model}")

    # ── Parse ──
    _hdr("Parsing LLM output")
    try:
        proposal = parse_llm_output(
            llm.response_text, model_used=llm.model, prompt_sha=sha,
        )
    except ValueError as e:
        _fail(f"parse failed: {e}")
        _info(f"raw response saved at {response_path} — inspect and re-run")
        return 3
    _pass(f"parsed: {len(proposal.proposed_entities)} entities")
    _pass(f"ambiguities flagged: {len(proposal.ambiguities_flagged)}")
    _pass(f"scope observations: {len(proposal.scope_observations)}")

    proposal_path.write_text(
        json.dumps(proposal.model_dump(), indent=2, default=str),
        encoding="utf-8",
    )
    _pass(f"proposal JSON → {proposal_path}")

    # ── Render review markdown ──
    _hdr("Rendering review surface")
    md = render_review_markdown(proposal)
    review_path.write_text(md, encoding="utf-8")
    _pass(f"review markdown → {review_path}")

    _hdr("Next step")
    _info(f"Open {review_path} and tick approve / modify / reject per entity.")
    _info("Once finalized, run the (next) finalize_entities.py to commit "
          "the approved registry to data/registries/entities.yaml.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
