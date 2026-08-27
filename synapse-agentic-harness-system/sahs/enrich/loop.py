"""The B1 loop: plan → blind gate (A5) → enrich → witnessed writes.

Reads ONLY the compiled build (the enricher sees what the serving agent
sees) plus the graph fold for the never-clobber guard; writes ordinary
append-only quads under ``witness: llm_enriched``. The A5 gate runs
before any write: <60% blind name recovery halts the run with nothing
written — you iterate the prompt, never the graph.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from sahs.enrich.client import VertexClient, parse_json_answer
from sahs.enrich.prompts import (
    PROMPT_VERSION,
    SYSTEM,
    blind_name_prompt,
    concept_description_prompt,
    metric_semantics_prompt,
)
from sahs.graph.ids import concept_id
from sahs.graph.quads import GraphDir, NodeRecord, Prov
from sahs.graph.review import emit_review_item
from sahs.tools.api import Build, _tokens

SOURCE = "llm_enricher"                 # → witness "llm_enriched"

# A5 (pinned): blind-test recovery ≥80% ⇒ batch-tier review eligible;
# 60–80% ⇒ item-review only; <60% ⇒ do not run at scale.
TIER_BATCH = 0.80
TIER_ITEM = 0.60
# v1 recovery grader (deterministic proxy, revisit per A5 trigger):
# ≥50% of the true label's content tokens appear in the prediction.
RECOVERY_TOKEN_SHARE = 0.5


def _prov(run_id: str, model: str, extra: str = "") -> Prov:
    return Prov(source=SOURCE, run=run_id, witness="llm_enriched",
                evidence=f"vertex:{model}"
                         + (f"#{extra}" if extra else ""))


def _normalize_question(text: str) -> str:
    return " ".join((text or "").lower().strip().strip("?!. ").split())


def _table_purposes(build: Build) -> dict[str, str]:
    purposes: dict[str, str] = {}
    for card in sorted((build.root / "cards" / "tables").glob("*.md")):
        physical = card.stem.replace("__", ".")
        for line in card.read_text(encoding="utf-8").splitlines():
            if line.startswith("- purpose: "):
                purposes[physical] = line[len("- purpose: "):].split(
                    " [prov:")[0]
                break
    return purposes


def _lob_by_table(build: Build) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in build.lob:
        label = row.get("code") or row.get("lob", "")
        if row.get("name"):
            label = f"{label} — {row['name']}"
        for physical in row.get("tables", []):
            out.setdefault(physical, label)
    return out


def _referenced_columns(sql: str, build: Build, table: str) -> list[str]:
    columns = build.schema.get(table, {})
    sql_tokens = {t for t in
                  "".join(c if (c.isalnum() or c == "_") else " "
                          for c in sql.lower()).split()}
    return sorted(c for c in columns if c.lower() in sql_tokens)[:12]


def _context(row: dict[str, Any], build: Build,
             purposes: dict[str, str],
             lob_of: dict[str, str]) -> dict[str, Any]:
    table = row.get("table", "")
    return {
        "table": table,
        "table_purpose": purposes.get(table, ""),
        "line_of_business": (row.get("line_of_business")
                             or lob_of.get(table, "")),
        "domain": row.get("domain", ""),
        "columns": _referenced_columns(row.get("canonical_sql", ""),
                                       build, table),
        "sql": row.get("canonical_sql", ""),
        "label": row.get("label") or "",
    }


# ── planning ─────────────────────────────────────────────────


def plan_metric_items(build: Build, folded_nodes: dict,
                      limit: int) -> list[dict[str, Any]]:
    """Mined metrics missing BOTH a question and a grain — highest
    support first (deterministic). The never-clobber guard checks the
    GRAPH fold, not just the build, so a re-run before recompile still
    skips already-enriched nodes."""
    purposes = _table_purposes(build)
    lob_of = _lob_by_table(build)
    items = []
    for row in sorted(build.metrics,
                      key=lambda r: (-r.get("support", 0), r["id"])):
        if row.get("question") and row.get("grain"):
            continue
        record = folded_nodes.get(row["id"])
        props = record.props if record is not None else {}
        if props.get("question_answered") and props.get("grain"):
            continue
        if props.get("description"):
            continue        # the catalog documented it by hand —
                            # never ask the model to redo a steward
        if props.get("question_enriched") or props.get("grain_enriched"):
            continue                     # enriched in a prior run
        if not row.get("canonical_sql"):
            continue
        item = {"kind": "metric", "id": row["id"], "fp": row["fp"],
                **_context(row, build, purposes, lob_of),
                "support": row.get("support", 0),
                "has_question": bool(props.get("question_answered")),
                "has_grain": bool(props.get("grain"))}
        items.append(item)
        if len(items) >= limit:
            break
    return items


def plan_concept_items(build: Build, folded_nodes: dict,
                       limit: int) -> list[dict[str, Any]]:
    """Concept labels with no description anywhere — total binding
    support first."""
    by_label: dict[str, list[dict[str, Any]]] = {}
    for row in build.bindings:
        by_label.setdefault(row["label"], []).append(row)
    items = []
    for label, rows in sorted(
            by_label.items(),
            key=lambda kv: (-sum(r.get("support", 0) for r in kv[1]),
                            kv[0])):
        enriched = any(
            (folded_nodes.get(concept_id(label, r["table"])) is not None
             and folded_nodes[concept_id(label, r["table"])]
             .props.get("description_enriched"))
            for r in rows)
        if enriched:
            continue
        items.append({
            "kind": "concept", "label": label,
            "tables": sorted({r["table"] for r in rows}),
            "bindings": [{"table": r["table"],
                          "sql": r["canonical_sql"],
                          "support": r.get("support", 0)}
                         for r in sorted(rows,
                                         key=lambda r: -r.get(
                                             "support", 0))]})
        if len(items) >= limit:
            break
    return items


# ── A5 blind gate ────────────────────────────────────────────


def blind_items(build: Build) -> list[dict[str, Any]]:
    """The certified/pending metrics WITH names — the ground truth the
    enricher must recover blind (names withheld from the prompt)."""
    purposes = _table_purposes(build)
    lob_of = _lob_by_table(build)
    items = []
    for row in sorted(build.metrics, key=lambda r: r["id"]):
        if row.get("status") not in ("certified", "pending"):
            continue
        if not row.get("label") or not row.get("canonical_sql"):
            continue
        item = _context(row, build, purposes, lob_of)
        item.update({"id": row["id"], "true_label": row["label"]})
        item["label"] = ""               # withheld — that's the test
        items.append(item)
    return items


def grade_recovery(true_label: str, predicted: str) -> bool:
    truth = _tokens(true_label)
    if not truth:
        return False
    overlap = len(truth & _tokens(predicted))
    return overlap / len(truth) >= RECOVERY_TOKEN_SHARE


def run_blind_gate(build: Build, client: VertexClient, out_dir: Path,
                   sample_n: int = 0,
                   log: Callable[[str], None] = print) -> dict[str, Any]:
    items = blind_items(build)
    if sample_n:
        items = items[:sample_n]
    results = []
    recovered = 0
    for item in items:
        answer = parse_json_answer(client.generate(
            blind_name_prompt(item), system=SYSTEM)) or {}
        predicted = str(answer.get("name") or "")
        ok = grade_recovery(item["true_label"], predicted)
        recovered += int(ok)
        results.append({"id": item["id"], "true": item["true_label"],
                        "predicted": predicted, "recovered": ok})
    rate = round(recovered / len(items), 4) if items else 0.0
    tier = ("batch" if rate >= TIER_BATCH else
            "item" if rate >= TIER_ITEM else "halt")
    (out_dir / "blind_results.jsonl").write_text(
        "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n"
                for r in results), encoding="utf-8")
    log(f"blind gate: {recovered}/{len(items)} recovered "
        f"({rate:.0%}) → tier {tier}")
    return {"n": len(items), "recovered": recovered, "rate": rate,
            "tier": tier}


# ── enrichment writes ────────────────────────────────────────


def _checkpoint(out_dir: Path) -> tuple[set[str], Path]:
    """Resume state — reset happens ONCE in run_enrich on --fresh (a
    per-phase reset would wipe the metric phase's progress before the
    concept phase runs)."""
    path = out_dir / "enrich_checkpoint.jsonl"
    done: set[str] = set()
    if path.exists():
        for line in path.read_text(encoding="utf-8").split("\n"):
            if line.strip():
                done.add(json.loads(line)["id"])
    return done, path


def enrich_metric_items(items: list[dict], client: VertexClient,
                        graph: GraphDir, build: Build, run_id: str,
                        out_dir: Path, report: dict,
                        log: Callable[[str], None] = print) -> None:
    done, checkpoint = _checkpoint(out_dir)
    taken_questions = {
        _normalize_question(r["question"])
        for r in build.metrics
        if r.get("status") in ("certified", "pending")
        and r.get("question")}
    model = client.connection.model
    for n, item in enumerate(items, 1):
        if item["id"] in done:
            report["resumed_skips"] += 1
            continue
        answer = parse_json_answer(client.generate(
            metric_semantics_prompt(item), system=SYSTEM))
        if (not answer or not str(answer.get("question") or "").strip()
                or not str(answer.get("grain") or "").strip()):
            report["invalid_json"] += 1
            continue
        question = str(answer["question"]).strip()
        grain = str(answer["grain"]).strip()
        key = _normalize_question(question)
        if key in taken_questions:
            # the model says a MINED metric answers a CERTIFIED
            # question — that is a variant signal for a steward, never
            # an automatic write over the certified plane
            emit_review_item(
                graph, kind="metric_conflict", subject=item["id"],
                proposal=(f"enriched question duplicates an existing "
                          f"certified/pending question: {question!r}"),
                evidence=[f"vertex:{model}#prompt={PROMPT_VERSION}"],
                run_id=run_id, source=SOURCE, witness="llm_enriched",
                support_effective=item.get("support", 1),
                blast_radius=3,
                agent_recommendation="consider variant_of the certified "
                                     "metric instead of a new question")
            report["collisions"] += 1
            continue
        taken_questions.add(key)
        props: dict[str, Any] = {
            "enrich_confidence": float(answer.get("confidence") or 0.0),
            "enrich_prompt_version": PROMPT_VERSION,
        }
        if not item.get("has_question"):
            props["question_enriched"] = question
        if not item.get("has_grain"):
            props["grain_enriched"] = grain
        if str(answer.get("caveat") or "").strip():
            props["enrich_caveat"] = str(answer["caveat"]).strip()
        graph.append_node(NodeRecord(
            id=item["id"], props=props,
            prov=_prov(run_id, model, f"prompt={PROMPT_VERSION}")))
        with checkpoint.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"id": item["id"]}) + "\n")
        report["metrics_enriched"] += 1
        if n % 25 == 0:
            log(f"  enriched {n}/{len(items)} metric items")


def enrich_concept_items(items: list[dict], client: VertexClient,
                         graph: GraphDir, run_id: str, out_dir: Path,
                         report: dict,
                         log: Callable[[str], None] = print) -> None:
    done, checkpoint = _checkpoint(out_dir)
    model = client.connection.model
    for n, item in enumerate(items, 1):
        key = f"concept::{item['label']}"
        if key in done:
            report["resumed_skips"] += 1
            continue
        answer = parse_json_answer(client.generate(
            concept_description_prompt(item), system=SYSTEM))
        if not answer or not str(
                answer.get("description") or "").strip():
            report["invalid_json"] += 1
            continue
        props: dict[str, Any] = {
            "description_enriched":
                str(answer["description"]).strip(),
            "enrich_confidence": float(answer.get("confidence") or 0.0),
            "enrich_prompt_version": PROMPT_VERSION,
        }
        if str(answer.get("disambiguation") or "").strip():
            props["disambiguation_enriched"] = str(
                answer["disambiguation"]).strip()
        for table in item["tables"]:
            graph.append_node(NodeRecord(
                id=concept_id(item["label"], table), props=dict(props),
                prov=_prov(run_id, model, f"prompt={PROMPT_VERSION}")))
        with checkpoint.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"id": key}) + "\n")
        report["concepts_enriched"] += 1
        if n % 25 == 0:
            log(f"  enriched {n}/{len(items)} concept items")


# ── orchestration ────────────────────────────────────────────


def run_enrich(*, graph_root: Path, builds_root: Path, out_dir: Path,
               run_id: str, limit: int = 200,
               targets: tuple[str, ...] = ("metrics", "concepts"),
               plan_only: bool = False, blind_sample: int = 0,
               fresh: bool = False, client: VertexClient | None = None,
               log: Callable[[str], None] = print) -> dict[str, Any]:
    """→ the enrich report (also written to <out>/enrich_report.json).
    ``plan_only`` builds and writes the plan without any model call —
    review what WOULD be asked before spending a token."""
    out_dir.mkdir(parents=True, exist_ok=True)
    build = Build.open(builds_root)
    graph = GraphDir(graph_root)
    folded = graph.fold_nodes()

    report: dict[str, Any] = {
        "prompt_version": PROMPT_VERSION, "build": build.version,
        "planned_metrics": 0, "planned_concepts": 0,
        "metrics_enriched": 0, "concepts_enriched": 0,
        "collisions": 0, "invalid_json": 0, "resumed_skips": 0,
        "blind": None, "usage": None, "plan_only": plan_only,
    }
    metric_items = (plan_metric_items(build, folded, limit)
                    if "metrics" in targets else [])
    concept_items = (plan_concept_items(build, folded, limit)
                     if "concepts" in targets else [])
    report["planned_metrics"] = len(metric_items)
    report["planned_concepts"] = len(concept_items)
    (out_dir / "plan.jsonl").write_text(
        "".join(json.dumps(i, ensure_ascii=False, sort_keys=True) + "\n"
                for i in metric_items + concept_items),
        encoding="utf-8")
    log(f"planned: {len(metric_items)} metric + "
        f"{len(concept_items)} concept items (limit {limit})")

    if not plan_only:
        if fresh:
            stale = out_dir / "enrich_checkpoint.jsonl"
            if stale.exists():
                stale.unlink()
        if client is None:
            from sahs.util.auth import VertexConnection
            client = VertexClient(VertexConnection.from_env())
        report["blind"] = run_blind_gate(build, client, out_dir,
                                         blind_sample, log)
        if report["blind"]["tier"] == "halt":
            log("A5 HALT: blind recovery under 60% — nothing written; "
                "iterate the prompt, not the graph")
        else:
            enrich_metric_items(metric_items, client, graph, build,
                                run_id, out_dir, report, log)
            enrich_concept_items(concept_items, client, graph, run_id,
                                 out_dir, report, log)
        report["usage"] = dict(client.usage)

    (out_dir / "enrich_report.json").write_text(
        json.dumps(report, indent=1, sort_keys=True) + "\n",
        encoding="utf-8")
    return report
