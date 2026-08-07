#!/usr/bin/env python3
"""One-trigger pipeline: sources → assertions → compiled graph snapshot.

The consolidation point the repo has been converging on — every witness
lands in one canonical sources dir, one builder compiles the graph, one
snapshot serves every consumer (MCP server, ADK agent, Streamlit UI).

    graph is COMPILED, not written:
      loaders emit canonical artifacts (per-source witness statements)
      → build_graph_from_sources() fuses them with per-fact provenance
      → snapshot JSON + assertion log for audit/replay

Offline demo (no network, no credentials — uses committed fixtures):

    python synapse/scripts/pipeline.py --demo

Real run (any subset; missing sources are skipped, not fatal):

    python synapse/scripts/pipeline.py \
        --skills-dir ~/Downloads/skills \
        --gold-sql-dir lumi_final/data/gold_queries \
        --bq-extract-dir ~/synapse_bq_outputs \
        --lumi-session lumi_final/data/session1_output.json \
        --mdm-cache-dir lumi_final/data/mdm_cache

Catalog exports fuse as weighted witnesses — full builds AND appends:

    python synapse/scripts/pipeline.py \
        --append-to synapse/data/cache/graph_snapshot.json \
        --dmp-export metrics_dmp.json \
        --measures-catalog measures_catalog.json \
        [--table-aliases aliases.json] [--measures-min-confidence low] \
        [--weights-override '{"usage_mined": 3}']

Then serve it:

    SYNAPSE_GRAPH_PATH=synapse/data/cache/graph_snapshot.json \
        python -m synapse.mcp.server
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
sys.path.insert(0, str(SYNAPSE_ROOT))

from synapse.graph.builder import build_graph_from_sources  # noqa: E402
from synapse.loaders.gold_sql_loader import load_gold_sql_corpus  # noqa: E402
from synapse.loaders.skills_loader import load_skills_library  # noqa: E402

_DEFAULT_SOURCES_DIR = str(SYNAPSE_ROOT / "data" / "cache" / "sources")
_DEFAULT_OUT = str(SYNAPSE_ROOT / "data" / "cache" / "graph_snapshot.json")


def _stage(msg: str) -> None:
    print(f"\n\033[1;36m═══ {msg} ═══\033[0m")


def _note(msg: str) -> None:
    print(f"  {msg}")


def run_pipeline(args: argparse.Namespace) -> Path:
    sources_dir = Path(args.sources_dir).expanduser()

    # ── 0a. Source-weights override (experiment knob) ────────
    #    Storage stays Knowledge-Vault-shaped (every fact kept with its
    #    calibrated score); experiments retune the WEIGHTS for one build.
    #    Applied before any stage so every tier computes under the tuned
    #    prior. Flag wins over the SYNAPSE_SOURCE_WEIGHTS env fallback.
    overrides_raw = (getattr(args, "weights_override", "")
                     or os.environ.get("SYNAPSE_SOURCE_WEIGHTS", ""))
    if overrides_raw:
        from synapse.graph.store import apply_weight_overrides
        text = overrides_raw.strip()
        if not text.startswith("{"):
            text = Path(text).expanduser().read_text(encoding="utf-8")
        overrides = json.loads(text)
        _stage("Source-weights override (experiment)")
        previous = apply_weight_overrides(overrides)
        for name in sorted(overrides):
            _note(f"{name}: {previous[name]} → {overrides[name]}")

    # ── 0. Append mode ───────────────────────────────────────
    #    Load an existing snapshot and fuse ONLY what this run stages on
    #    top of it — new tables land as new nodes, a known source seen
    #    again fuses as one more witness, tiers recompute, and nothing
    #    pre-existing is pruned or regressed. No full rebuild.
    base_store = None
    _append_tmp = None
    if getattr(args, "append_to", ""):
        from synapse.graph.store import GraphStore
        snap = Path(args.append_to).expanduser()
        if not snap.exists():
            raise SystemExit(f"--append-to: no snapshot at {snap}")
        _stage("Append mode: loading the existing snapshot")
        base_store = GraphStore.load_json(snap)
        s0 = base_store.stats()
        _note(f"base: {snap}")
        _note(f"nodes: {s0['n_nodes']}  edges: {s0['n_edges']}  "
              f"(version {base_store.snapshot_version})")
        # stage this run's artifacts in a scratch dir, not the shared
        # sources dir — an append fuses what YOU pass, not old artifacts
        if args.sources_dir == _DEFAULT_SOURCES_DIR:
            import tempfile
            _append_tmp = tempfile.TemporaryDirectory(
                prefix="synapse_append_")
            sources_dir = Path(_append_tmp.name)
            _note(f"staging: {sources_dir} (scratch)")
        # default output = write the same snapshot forward
        if args.out == _DEFAULT_OUT:
            args.out = str(snap)
    sources_dir.mkdir(parents=True, exist_ok=True)
    run_report: dict[str, dict] = {}

    # One manifest scopes every stage that can be scoped: the lumi session
    # split, the MDM crawl, and (by default) enrichment.
    manifest_path = args.manifest or args.mdm_manifest
    manifest_names: set[str] = set()
    if manifest_path:
        from synapse.graph.store import normalize_table_name
        from synapse.utils.manifest import read_tables_manifest
        manifest_names = {
            normalize_table_name(t["name"])
            for t in read_tables_manifest(manifest_path)
        }

    # ── 1. Skills library (semantic witness) ─────────────────
    skills_dir = args.skills_dir
    if args.demo and not skills_dir:
        skills_dir = str(SYNAPSE_ROOT / "tests" / "fixtures" / "skills_library")
    if skills_dir:
        _stage("Skills library → canonical artifacts")
        result = load_skills_library(Path(skills_dir), out_dir=sources_dir)
        run_report["skills"] = result.model_dump(mode="json")
        _note(f"{result.status}: {result.records_count} skill package(s)")
        for skill_id, outcome in sorted(
                (result.metadata.get("skills") or {}).items()):
            _note(f"  {'✓' if outcome == 'ok' else '✗'} {skill_id}"
                  + ("" if outcome == "ok" else f" — {outcome}"))
        for warning in result.warnings:
            _note(f"⚠ {warning}")

    # ── 2. Gold SQL corpus (behavioral witness) ──────────────
    gold_dir = args.gold_sql_dir
    if args.demo and not gold_dir:
        candidate = REPO_ROOT / "lumi_final" / "data" / "gold_queries"
        gold_dir = str(candidate) if candidate.is_dir() else ""
    if gold_dir:
        _stage("Gold SQL corpus → sqlglot signals")
        result = load_gold_sql_corpus(Path(gold_dir), out_dir=sources_dir)
        run_report["gold_sql"] = result.model_dump(mode="json")
        _note(f"{result.status}: {result.records_count} queries parsed, "
              f"{len(result.metadata.get('tables_discovered', []))} tables")
        for warning in result.warnings[:5]:
            _note(f"⚠ {warning}")

    # ── 3. BQ batch-extraction output (physical witness) ─────
    if args.bq_extract_dir:
        _stage("BigQuery extraction → canonical artifacts")
        from synapse.loaders.bq_loader import load_bq_for_table
        bq_root = Path(args.bq_extract_dir).expanduser()
        table_dirs = [d for d in sorted(bq_root.iterdir())
                      if d.is_dir() and not d.name.startswith("_")]
        outcomes = []
        for tdir in table_dirs:
            # loader contract: source_dir is the PARENT containing <table>/
            # One malformed artifact must not kill the whole run — catch,
            # report, keep going (matches the sparse-tolerant MDM stage).
            try:
                result = load_bq_for_table(
                    tdir.name, source_dir=bq_root, out_dir=sources_dir)
                status, note = result.status, (result.error or "")
            except Exception as exc:
                status, note = "error", f"{type(exc).__name__}: {exc}"
            outcomes.append({"table": tdir.name, "status": status,
                             "note": note[:120]})
            if status == "error":
                _note(f"  ✗ {tdir.name}: {note[:80]}")
        n_ok = sum(1 for o in outcomes if o["status"] in ("ok", "partial"))
        run_report["bq"] = {"tables": outcomes}
        _note(f"{n_ok}/{len(outcomes)} table extraction folder(s) staged"
              + (f" · {len(outcomes) - n_ok} failed" if n_ok < len(outcomes)
                 else ""))

    # ── 4. Lumi fused output (governance + corpus witness) ───
    if args.lumi_session:
        _stage("Lumi session output → canonical artifacts")
        from synapse.graph.store import normalize_table_name
        from synapse.loaders.lumi_loader import load_lumi_for_table
        session_path = Path(args.lumi_session).expanduser()
        blob = json.loads(session_path.read_text(encoding="utf-8"))
        session_tables = sorted(blob)
        if manifest_names:  # scope to the manifest — not the whole session
            in_scope = [t for t in session_tables
                        if normalize_table_name(t) in manifest_names]
            skipped = len(session_tables) - len(in_scope)
            if skipped:
                _note(f"manifest scope: {len(in_scope)} of "
                      f"{len(session_tables)} session table(s) selected "
                      f"({skipped} outside tables.yaml skipped)")
            session_tables = in_scope
        outcomes = []
        for table_name in session_tables:
            result = load_lumi_for_table(
                table_name, lumi_path=session_path, out_dir=sources_dir)
            outcomes.append({"table": table_name, "status": result.status})
        run_report["lumi"] = {"tables": outcomes}
        _note(f"{len(outcomes)} table(s) split from session output")

    # ── 5. MDM cache (declared-metadata witness, legacy single-blob) ──
    if args.mdm_cache_dir:
        _stage("MDM cache → canonical artifacts")
        from synapse.loaders.mdm_loader import load_mdm_for_table
        mdm_root = Path(args.mdm_cache_dir).expanduser()
        outcomes = []
        for cached in sorted(mdm_root.glob("*.json")):
            result = load_mdm_for_table(
                cached.stem.replace("__mdm_raw", ""),
                source_dir=mdm_root, out_dir=sources_dir, dry_run=False)
            outcomes.append({"table": cached.stem, "status": result.status})
        run_report["mdm"] = {"tables": outcomes}
        _note(f"{len(outcomes)} MDM digest(s) staged")

    # ── 5b. MDM crawler (full read-side pull: spine, ownership,
    #        pipeline governance, lineage both ways, attribute lineage,
    #        lifecycle). Live on VPN via --mdm-base / SYNAPSE_MDM_BASE;
    #        fully offline from a --mdm-raw-dir per-endpoint cache. ──
    if args.mdm_crawl or args.mdm_manifest:
        _stage("MDM crawler → metadata spine")
        from synapse.loaders.mdm_crawler import crawl_mdm_for_table
        tables = [t.strip() for t in (args.mdm_crawl or "").split(",")
                  if t.strip()]
        if manifest_path:
            from synapse.utils.manifest import read_tables_manifest
            manifest_tables = read_tables_manifest(manifest_path)
            _note(f"manifest: {len(manifest_tables)} table(s) from "
                  f"{manifest_path}")
            tables += [t["name"] for t in manifest_tables
                       if t["name"] not in tables]
        raw_dir = (Path(args.mdm_raw_dir).expanduser()
                   if args.mdm_raw_dir
                   else sources_dir / "mdm_raw")
        outcomes = []
        # sparse-tolerant spine matrix: MDM data is genuinely absent for
        # some tables (probe-verified) — show exactly what each one gave us
        _note(f"{'table':34} {'cols':>5} {'BU':10} {'pipe':4} "
              f"{'lin↑':>4} {'lin↓':>4} {'life':4}")
        for table in tables:
            result = crawl_mdm_for_table(
                table, out_dir=sources_dir,
                base_url=args.mdm_base or None,
                cache_dir=raw_dir, refresh=args.mdm_refresh)
            meta = result.metadata
            fetch = meta.get("fetch_report") or {}
            ok = ("ok", "cached")
            _note(f"{table[:34]:34} {result.records_count:>5} "
                  f"{(meta.get('business_unit') or '–')[:10]:10} "
                  f"{'✓' if fetch.get('pipeline') in ok else '–':4} "
                  f"{meta.get('n_upstream', 0):>4} "
                  f"{meta.get('n_downstream', 0):>4} "
                  f"{'✓' if fetch.get('lifecycle') in ok else '–':4}"
                  + ("   ✗ " + (result.error or "")[:40]
                     if result.status == "error" else ""))
            outcomes.append({
                "table": table, "status": result.status,
                "business_unit": meta.get("business_unit"),
                "fetch_report": fetch,
            })
        n_err = sum(1 for o in outcomes if o["status"] == "error")
        _note(f"{len(outcomes)} table(s) crawled · {n_err} schema failure(s)"
              " · '–' = MDM has no record (sparse, not an error)")
        run_report["mdm_crawl"] = {"tables": outcomes}

    # ── 6. Compile ───────────────────────────────────────────
    _stage("Compile: assertions → typed graph")
    # Witness #6 — steward-approved entities: stage the approvals file
    # (explicit --entities, else the persisted default) into the
    # compile's sources so build_graph_from_sources ingests it
    entities_src = (Path(args.entities).expanduser() if args.entities
                    else REPO_ROOT / "semantic-graph" / "config"
                    / "entities.yaml")
    if entities_src.exists():
        import shutil
        sources_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(entities_src, sources_dir / "entities.yaml")
        _note(f"entities: staged steward approvals from {entities_src}")
    # When a manifest scopes the run, use it as the builder allowlist too —
    # only these tables' nodes survive; CTE aliases and template
    # placeholders from corpus/skills SQL are pruned instead of minted.
    build_allowlist = manifest_names or None
    if build_allowlist:
        _note(f"allowlist: graph scoped to {len(build_allowlist)} manifest "
              f"table(s); out-of-scope + CTE-noise nodes pruned")
    store = build_graph_from_sources(sources_dir, allowlist=build_allowlist,
                                     into=base_store)
    stats = store.stats()
    _note(f"nodes: {stats['n_nodes']}  edges: {stats['n_edges']}")
    _note(f"by type: {stats['nodes_by_type']}")
    _note(f"by tier: {stats['nodes_by_confidence_tier']}")

    # ── 6a. External catalog connectors (weighted witnesses) ─
    #        A Collibra export fuses in AFTER compile so its testimony
    #        lands on the fused nodes: known tables gain a distinct
    #        witness (tier recomputes), unknown tables are minted at the
    #        single-witness floor. Works in full builds and appends.
    if getattr(args, "collibra_export", ""):
        _stage("Collibra export → collibra witness (weight 5)")
        from synapse.loaders.collibra_loader import load_collibra_export
        col_res = load_collibra_export(
            store, Path(args.collibra_export).expanduser())
        _note(f"{col_res['tables']} table asset(s) · "
              f"{col_res['columns']} column asset(s) fused")
        for reason in col_res["skipped"][:8]:
            _note(f"⚠ skipped: {reason}")
        run_report["collibra"] = col_res
        stats = store.stats()
        _note(f"by tier now: {stats['nodes_by_confidence_tier']}")

    # Shared alias map for the two catalog stages below — mined exports
    # carry table-name spelling drift (e.g. offer/offr); one map serves
    # both loaders so the drifted spellings fuse onto one node.
    catalog_aliases = None
    if getattr(args, "table_aliases", "") and (
            getattr(args, "dmp_export", "")
            or getattr(args, "measures_catalog", "")):
        from synapse.loaders.measures_catalog_loader import load_table_aliases
        catalog_aliases = load_table_aliases(
            Path(args.table_aliases).expanduser())

    # ── 6b. DMP metric catalog (curated-family witness) ──────
    #        Author-owned Data Marketplace metrics fuse BEFORE the mined
    #        pass so curated wording lands first and discovery only fills
    #        the gaps — order encodes authority, same as the full build.
    if getattr(args, "dmp_export", ""):
        _stage("DMP metric catalog → dmp witness (weight 5)")
        from synapse.loaders.dmp_loader import load_dmp_export
        if catalog_aliases:
            _note(f"table aliases: {len(catalog_aliases)} mapping(s)")
        dmp_res = load_dmp_export(
            store, Path(args.dmp_export).expanduser(),
            aliases=catalog_aliases)
        _note(f"{dmp_res['metrics']} curated metric(s) fused · "
              f"{dmp_res['tables_minted']} unknown table(s) minted")
        for reason in dmp_res["skipped"][:8]:
            _note(f"⚠ skipped: {reason}")
        run_report["dmp"] = dmp_res
        stats = store.stats()
        _note(f"by tier now: {stats['nodes_by_confidence_tier']}")

    # ── 6c. Mined measures catalog (behavioral witness) ──────
    #        Metrics distilled from historical query execution: weight 2,
    #        evidence-scaled by user_count, JOINS_WITH co-occurrence
    #        edges. Below-threshold candidates are counted, never silent.
    if getattr(args, "measures_catalog", ""):
        _stage("Mined measures → usage_mined witness (weight 2)")
        from synapse.loaders.measures_catalog_loader import (
            load_measures_catalog)
        if catalog_aliases:
            _note(f"table aliases: {len(catalog_aliases)} mapping(s)")
        mm_res = load_measures_catalog(
            store, Path(args.measures_catalog).expanduser(),
            min_confidence=args.measures_min_confidence,
            aliases=catalog_aliases)
        _note(f"{mm_res['metrics']} mined metric(s) at confidence ≥ "
              f"{args.measures_min_confidence} fused across "
              f"{mm_res['tables_witnessed']} table(s)")
        _note(f"{mm_res['tables_minted']} unknown table(s) minted · "
              f"{mm_res['join_edges']} JOINS_WITH co-occurrence edge(s)")
        if mm_res["below_threshold"]:
            _note("below threshold (not ingested as metrics; their join "
                  f"evidence still counted): {mm_res['below_threshold']}")
        for reason in mm_res["skipped"][:8]:
            _note(f"⚠ skipped: {reason}")
        run_report["measures"] = mm_res
        stats = store.stats()
        _note(f"by tier now: {stats['nodes_by_confidence_tier']}")

    # ── 6d. LLM enrichment (witness #5) — batched, opt-in, budgeted ──
    #        One Gemini call per ≤batch-size columns; wide tables get
    #        multiple calls, merged into one bundle per table. Facts land
    #        as `llm_generated` (tier-capped at inferred until other
    #        witnesses corroborate). Runs on the BUILT graph so the LLM
    #        sees the fused multi-witness context, then re-stats.
    if args.enrich:
        _stage("LLM enrichment → llm_generated facts (batched)")
        try:
            from synapse.enrichment.enricher import (
                collect_enrichment_failures, enrich_graph, plan_enrichment,
                propose_entities)
            from synapse.enrichment.vertex_client import (
                TieredLLMClient, VertexLLMClient)
            if args.enrich_strategy == "tiered":
                client = TieredLLMClient()
                _note(f"strategy: tiered · pro={client.pro_model} · "
                      f"flash={client.flash_model or 'UNSET → all pro'}")
            else:
                client = VertexLLMClient()
                _note(f"strategy: pro-only · model={client.model}")
            if getattr(client, "tls_mode", "default") != "default":
                detail = getattr(client, "tls_detail", None) or {}
                _note(f"tls: {client.tls_mode} · "
                      f"truststore={'yes' if detail.get('truststore') else 'no'}"
                      " · sdk-verify="
                      + ("direct (HttpOptions.client_args)"
                         if getattr(client, "tls_sdk_direct", False)
                         else "patched only (old SDK — upgrade "
                              "google-genai if TLS errors persist)"))
        except RuntimeError as exc:
            _note(f"⚠ enrichment skipped: {exc}")
            run_report["enrichment"] = {"status": "skipped",
                                        "reason": str(exc)}
        else:
            only = ([t.strip() for t in args.enrich_tables.split(",")
                     if t.strip()]
                    if args.enrich_tables
                    else (sorted(manifest_names) if manifest_names else None))
            out_root = Path(args.out).expanduser().parent
            grounding: dict[str, dict] = {}
            # Prioritize: spend LLM calls on the high-value head of each
            # table (identifiers → analyst-salient → coded → MDM-gap) and
            # leave the numeric tail grounded-by-BQ, not LLM-narrated. Runs
            # as the enricher's skip_columns complement; --enrich-all-columns
            # restores the exhaustive sweep.
            skip_columns: dict[str, set[str]] | None = None
            if not args.enrich_all_columns:
                from collections import Counter
                from synapse.enrichment.prioritize import (
                    prioritize_columns, select_for_enrichment)
                from synapse.enrichment.signals import (
                    cross_table_column_counts)
                cap = args.enrich_max_columns or None
                cross = cross_table_column_counts(store)
                targets = only or [
                    str(n.properties.get("table_name"))
                    for n in store.nodes_by_type("Table")
                    if n.properties.get("table_name")]
                skip_columns = {}
                _note("prioritization — LLM spent on the high-value head "
                      "(tail stays grounded-by-BQ):")
                for tbl in sorted(set(targets)):
                    ranked = prioritize_columns(store, tbl, cross_counts=cross)
                    if not ranked:
                        continue
                    keep = select_for_enrichment(ranked, max_columns=cap)
                    if not keep:            # guarantee a table-identity pass
                        keep = [p.column for p in ranked[:12]]
                    keep_l = {c.lower() for c in keep}
                    all_l = {p.column.lower() for p in ranked}
                    skip = all_l - keep_l
                    if skip:
                        skip_columns[tbl.lower()] = skip
                    tiers = Counter(
                        p.tier for p in ranked if p.column.lower() in keep_l)
                    _note(f"  {tbl[:34]:34} {len(keep):>4}/{len(all_l):<6} "
                          + ", ".join(f"{k}:{v}"
                                      for k, v in tiers.most_common()))
            # Pre-run work plan — the horizon before a single Gemini call
            # (the top-up's UX: know how big the run is, and how far along
            # you are, so a multi-hour laptop run never feels like a hang).
            plan = plan_enrichment(
                store, only_tables=only,
                column_batch_size=args.enrich_batch_size,
                max_calls=args.enrich_max_calls,
                skip_columns=skip_columns)
            _note(f"enrichment plan — {len(plan['per_table'])} table(s), "
                  f"{plan['total_columns']} column(s):")
            _note(f"  {'table':38} {'cols':>6}  ~calls")
            for tname in sorted(plan["per_table"]):
                row = plan["per_table"][tname]
                _note(f"  {tname[:38]:38} {row['columns']:>6}  ~{row['calls']}")
            _note(f"  total: {plan['total_columns']} column(s) → "
                  f"~{plan['total_calls']} call(s) "
                  f"(budget {args.enrich_max_calls} → will spend "
                  f"~{plan['budget_capped_calls']})")
            bundles = enrich_graph(
                store, client,
                only_tables=only,
                column_batch_size=args.enrich_batch_size,
                max_calls=args.enrich_max_calls,
                memory_out=out_root / "enrichment_memory.json",
                grounding_reports=grounding,
                # real analyst SQL staged by the session split — Gemini's
                # strongest grounding evidence
                evidence_dir=sources_dir / "mdm_cache",
                demo_out=out_root / "demo_questions.json",
                verbose=True,
                planned_calls=plan["budget_capped_calls"],
                skip_columns=skip_columns,
            )
            n_obs = sum(len(b.column_observations) for b in bundles.values())
            n_syn = sum(len(b.candidate_synonyms) for b in bundles.values())
            _note(f"{len(bundles)} table(s) enriched · {n_obs} column "
                  f"observations · {n_syn} synonym candidates")
            # the grounding gate's verdict — accuracy enforced, not assumed
            totals: dict[str, int] = {}
            for table_report in grounding.values():
                for key, value in table_report.items():
                    totals[key] = totals.get(key, 0) + value
            _note("grounding gate: "
                  f"{totals.get('applied_descriptions', 0)} descriptions "
                  f"applied · held {totals.get('held_low_confidence', 0)} "
                  f"low-confidence + {totals.get('held_no_evidence', 0)} "
                  "no-evidence · dropped "
                  f"{totals.get('dropped_imagined_columns', 0)} imagined "
                  f"columns, {totals.get('dropped_ungrounded_synonyms', 0)} "
                  "ungrounded synonyms, "
                  f"{totals.get('dropped_ungrounded_code_resolutions', 0)} "
                  "ungrounded code resolutions · "
                  f"{totals.get('ambiguity_flags', 0)} ambiguity flags")
            _note(f"relations: {totals.get('applied_relations', 0)} new "
                  "cross-table edge(s) applied · "
                  f"{totals.get('skipped_existing_relations', 0)} already "
                  "corpus-witnessed (skipped) · "
                  f"{totals.get('dropped_ungrounded_relations', 0)} "
                  "ungrounded (dropped)")
            _note(f"demo pack: {totals.get('applied_demo_questions', 0)} "
                  "verified-answerable question(s) · "
                  f"{totals.get('held_unanswerable_demo_questions', 0)} "
                  "held (capability missing) · "
                  f"{totals.get('dropped_ungrounded_demo_questions', 0)} "
                  "dropped ungrounded → "
                  f"{out_root / 'demo_questions.md'}")
            if getattr(client, "stats", None):
                _note(f"gemini: {client.stats.get('calls', 0)} call(s) · "
                      f"{client.stats.get('corrective_retries', 0)} "
                      "corrective retr(ies) · "
                      f"{client.stats.get('call_retries', 0)} call "
                      "retr(ies) · "
                      f"{client.stats.get('thinking_fallbacks', 0)} "
                      "thinking fallback(s) · "
                      f"{client.stats.get('context_truncations', 0)} "
                      "context truncation(s)")
                run_report["enrichment_client_stats"] = dict(client.stats)
            # in-band failures must never be invisible: when bundles came
            # back empty, say so AND say why, right here in the console
            failures = collect_enrichment_failures(bundles)
            if failures["empty_bundles"]:
                _note(f"⚠ {failures['empty_bundles']}/"
                      f"{failures['n_bundles']} bundle(s) came back EMPTY"
                      " — reasons:")
                for note_text, count in failures["notes"][:5]:
                    _note(f"    {count}× {note_text}")
                if not failures["notes"]:
                    _note("    (no error notes: the model returned "
                          "schema-valid but EMPTY bundles — evidence "
                          "may not be reaching it, or it over-abstained;"
                          " inspect enrichment_memory.json)")
            run_report["enrichment_failures"] = failures
            run_report["enrichment_grounding"] = {
                "totals": totals, "per_table": grounding}
            # Auto-understand entities the cardmember way: a single table is
            # enough to surface one (min_supporting_tables=1) — it strengthens
            # as more tables share the key.
            proposals = propose_entities(
                bundles, min_supporting_tables=1, min_aggregate_confidence=0.6)
            (out_root / "entity_proposals.json").write_text(
                json.dumps([p.model_dump() for p in proposals], indent=2),
                encoding="utf-8")
            from synapse.graph.entities import auto_materialize_entities
            ent_report = auto_materialize_entities(store, proposals)
            _note(f"{ent_report['entities_added']} entit(y/ies) auto-materialized "
                  f"(inferred) + {ent_report['edges_added']} IDENTIFIES edge(s); "
                  f"a steward may upgrade to human_asserted")
            run_report["enrichment"] = {
                "tables": sorted(bundles), "column_observations": n_obs,
                "entity_proposals": len(proposals),
                "entities_materialized": ent_report["entities_added"],
            }
            stats = store.stats()
            _note(f"post-enrichment tiers: "
                  f"{stats['nodes_by_confidence_tier']}")

    # ── 6e. Context-readiness scorecard ──────────────────────
    #        Node counts don't measure retrieval quality. Per manifest
    #        table: can the graph actually answer questions about it?
    #        Watch these numbers run over run — THIS is "rich enough".
    scorecard_tables = (sorted(manifest_names) if manifest_names else [
        str(n.properties.get("table_name"))
        for n in store.nodes_by_type("Table")
        if n.properties.get("table_name")
    ][:20])
    if scorecard_tables:
        from synapse.graph.inspector import context_readiness
        _stage("Context readiness — the graph as a context machine")
        _note(f"{'table':34} {'cols':>5} {'mean%':>5} {'rel':>4} "
              f"{'met':>4} {'code':>4} {'gov':>3} {'lin':>3} tier")
        readiness = context_readiness(store, scorecard_tables)
        for row in readiness:
            if not row.get("in_graph"):
                _note(f"{row['table'][:34]:34} NOT IN GRAPH")
                continue
            _note(f"{row['table'][:34]:34} {row['n_columns']:>5} "
                  f"{row['pct_columns_with_meaning']:>4}% "
                  f"{row['n_related_tables']:>4} {row['n_metrics']:>4} "
                  f"{row['n_code_resolutions']:>4} "
                  f"{'✓' if row['has_governance'] else '–':>3} "
                  f"{'✓' if row['has_lineage'] else '–':>3} "
                  f"{row['confidence_tier']}")
        run_report["context_readiness"] = readiness

    # ── 7. Snapshot + run manifest ───────────────────────────
    snapshot_path = Path(args.out).expanduser()
    store.save_json(snapshot_path)
    manifest_path = snapshot_path.with_name("run_manifest.json")
    manifest_path.write_text(json.dumps({
        "snapshot_version": store.snapshot_version,
        "sources_dir": str(sources_dir),
        "stats": stats,
        "loaders": run_report,
    }, indent=2, default=str), encoding="utf-8")
    _stage("Done")
    _note(f"snapshot: {snapshot_path}  (version {store.snapshot_version})")
    _note(f"manifest: {manifest_path}")
    _note("serve it:  SYNAPSE_GRAPH_PATH="
          f"{snapshot_path} python -m synapse.mcp.server")
    return snapshot_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compile the semantic graph from all available sources")
    parser.add_argument("--demo", action="store_true",
                        help="use committed fixtures (offline, no creds)")
    parser.add_argument("--skills-dir", default="",
                        help="skills library root (dirs with skill.yaml)")
    parser.add_argument("--gold-sql-dir", default="",
                        help="directory of gold *.sql files")
    parser.add_argument("--bq-extract-dir", default="",
                        help="bq_batch_extract.py output root")
    parser.add_argument("--lumi-session", default="",
                        help="lumi_final session1_output.json")
    parser.add_argument("--mdm-cache-dir", default="",
                        help="directory of cached raw MDM JSON (legacy "
                             "single-blob path)")
    parser.add_argument("--mdm-crawl", default="",
                        help="comma-separated tables for the FULL MDM "
                             "read-side crawl (spine, ownership, pipeline, "
                             "lineage, lifecycle)")
    parser.add_argument("--mdm-manifest", default="",
                        help="tables.yaml path — crawl EVERY table in the "
                             "extraction manifest (combines with --mdm-crawl)")
    parser.add_argument("--mdm-base", default="",
                        help="MDM API base url (or SYNAPSE_MDM_BASE)")
    parser.add_argument("--mdm-raw-dir", default="",
                        help="per-endpoint raw cache for the crawler "
                             "(offline replay / resumable laptop runs)")
    parser.add_argument("--mdm-refresh", action="store_true",
                        help="refetch even when a raw cache entry exists")
    parser.add_argument("--manifest", default="",
                        help="tables.yaml scope for EVERY scopeable stage "
                             "(lumi session split, MDM crawl, enrichment). "
                             "--mdm-manifest remains an alias.")
    parser.add_argument("--enrich", action="store_true",
                        help="run the batched LLM enrichment pass "
                             "(witness #5; needs Vertex creds; costs money)")
    parser.add_argument("--enrich-batch-size", type=int, default=40,
                        help="columns per LLM call (wide tables → many "
                             "calls, merged per table)")
    parser.add_argument("--enrich-max-calls", type=int, default=80,
                        help="hard LLM-call budget for the whole run; "
                             "tables that don't fit are reported, not "
                             "silently dropped")
    parser.add_argument("--enrich-all-columns", action="store_true",
                        help="enrich EVERY column (disable prioritization — "
                             "the exhaustive, expensive sweep). Default is "
                             "the prioritized head only.")
    parser.add_argument("--enrich-max-columns", type=int, default=0,
                        help="per-table cap on prioritized columns sent to "
                             "the LLM (0 = no cap; identifiers always kept)")
    parser.add_argument("--enrich-tables", default="",
                        help="comma list to enrich (default: manifest "
                             "tables, else every table in the graph)")
    parser.add_argument("--enrich-strategy", default="pro-only",
                        choices=["pro-only", "tiered"],
                        help="tiered = chunk 1 + narrow tables on "
                             "$GEMINI_MODEL_PRO, chunks 2..N of wide "
                             "tables on $GEMINI_MODEL_FLASH (run "
                             "scripts/probe_vertex_readiness.py first to "
                             "pick the models)")
    parser.add_argument("--entities", default="",
                        help="steward-approved entities YAML (default: "
                             "semantic-graph/config/entities.yaml when it "
                             "exists; produced by scripts/entities.py apply)")
    parser.add_argument("--append-to", default="",
                        help="INCREMENTAL mode: load this existing snapshot "
                             "and fuse only what this run stages on top of "
                             "it — new tables mint, known nodes gain the new "
                             "witnesses, tiers recompute, nothing pre-"
                             "existing is pruned. Writes back to the same "
                             "path unless --out is given. No full rebuild.")
    parser.add_argument("--collibra-export", default="",
                        help="Collibra asset export (JSON) fused as the "
                             "'collibra' witness (weight 5) — works in full "
                             "builds and appends")
    parser.add_argument("--dmp-export", default="",
                        help="Data Marketplace metric catalog (JSON, "
                             "{'metric_catalog': [...]}) fused as the 'dmp' "
                             "witness (weight 5) — works in full builds and "
                             "appends")
    parser.add_argument("--measures-catalog", default="",
                        help="mined measures catalog (JSON, {'measures': "
                             "[...]}) fused as the 'usage_mined' witness "
                             "(weight 2, user_count-scaled, JOINS_WITH "
                             "edges) — works in full builds and appends")
    parser.add_argument("--measures-min-confidence", default="medium",
                        choices=["low", "medium", "high"],
                        help="ingest mined measures at or above this "
                             "mining confidence (default medium; 'low' "
                             "ingests everything — join evidence is "
                             "counted from ALL rows regardless)")
    parser.add_argument("--table-aliases", default="",
                        help="JSON {alias: canonical} table-name map "
                             "shared by --dmp-export/--measures-catalog "
                             "(fuses spelling drift like offer/offr onto "
                             "one node)")
    parser.add_argument("--weights-override", default="",
                        help="experiment knob: JSON object or path "
                             "retuning SOURCE_WEIGHTS for THIS build "
                             "(e.g. '{\"usage_mined\": 3}'); env fallback "
                             "SYNAPSE_SOURCE_WEIGHTS")
    parser.add_argument("--sources-dir",
                        default=_DEFAULT_SOURCES_DIR,
                        help="staging dir for canonical artifacts")
    parser.add_argument("--out", default=_DEFAULT_OUT)
    args = parser.parse_args()
    if not any([args.demo, args.skills_dir, args.gold_sql_dir,
                args.bq_extract_dir, args.lumi_session, args.mdm_cache_dir,
                args.mdm_crawl, args.mdm_manifest, args.collibra_export,
                args.dmp_export, args.measures_catalog]):
        parser.error("nothing to load — pass --demo or at least one source")
    run_pipeline(args)


if __name__ == "__main__":
    main()
