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
# v1.1 recovery grader (deterministic proxy, revisit per A5 trigger):
# ≥50% of the true label's content tokens appear in the prediction,
# AND polarity agrees — the b1.1 smoke passed a Card Present /
# Card Not Present SWAP on token overlap alone, the one error class
# that would actually poison the graph. Strictly harder, never softer.
RECOVERY_TOKEN_SHARE = 0.5
_NEGATIONS = {"not", "non", "excluding", "without"}


def _version_num(version) -> tuple[int, int]:
    """'b1.2' → (1, 2); anything unparsable → (0, 0) so it always
    re-enriches under a real version."""
    try:
        major, minor = str(version or "").lstrip("b").split(".")
        return (int(major), int(minor))
    except (ValueError, AttributeError):
        return (0, 0)


def _enriched_current(props: dict) -> bool:
    """Version-aware idempotency: presence alone must not freeze a
    metric on the draft we least liked — a b1.2 prompt improvement
    re-enriches b1.1 output (append-only keeps the old draft as
    history; the fold's last-wins serves the newest)."""
    return (_version_num(props.get("enrich_prompt_version"))
            >= _version_num(PROMPT_VERSION))


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


def _vocab_index(build: Build) -> tuple[dict[str, str],
                                        list[tuple[str, str]]]:
    """The company's own reference shelf — Atlas business terms + the
    acropedia glossary, already compiled into the vocab plane — split
    for fast per-item lookup: single-token entries (acronyms, symbols)
    keyed exactly; multi-word terms matched by phrase."""
    single: dict[str, str] = {}
    multi: list[tuple[str, str]] = []
    for row in build.vocab:
        text = str(row.get("text") or "").strip()
        if not text or len(text) < 2:
            continue
        definition = str(row.get("definition") or "").strip()
        entry = text + (f" — {definition[:140]}" if definition else "")
        if " " in text:
            multi.append((text.lower(), entry))
        else:
            single.setdefault(text.lower(), entry)
    return single, multi


# SQL keywords and function names must never hit the acronym shelf —
# the b1.3 field run fed CASE→"Computer Aided Software Engineering"
# and AS→"Actual Start date" into real prompts as authoritative
# vocabulary, and two-letter column fragments (cr, dr, am, cd, dw)
# pulled unrelated corporate acronyms in front of the model (cr/dr
# are credit/debit indicators, not Customer Reference / Disaster
# recovery). Every 2-letter match in that run was wrong; the shortest
# true positive (ALIF) has four letters.
_SQL_NOISE = frozenset((
    "case", "when", "then", "else", "end", "and", "not", "sum",
    "count", "countif", "avg", "min", "max", "distinct", "trim",
    "nullif", "coalesce", "cast", "safe_cast", "like", "between",
    "select", "from", "where", "join", "left", "right", "inner",
    "outer", "group", "having", "order", "over", "partition",
    "null", "true", "false", "date", "timestamp", "extract",
    "interval", "day", "month", "year", "week"))


def _vocab_for(item: dict[str, Any], single: dict[str, str],
               multi: list[tuple[str, str]],
               cap: int = 6) -> list[str]:
    """→ the glossary entries this metric's own text touches (label,
    SQL, columns, table, filters) — snake_case split so `alif_cnt`
    finds ALIF, trailing-s stripped so `naas_ct` still finds NAA.
    Multi-word Atlas entries first (they match exactly and were the
    only genuinely useful hits in the b1.3 field run), then acronyms
    of 3+ letters, longest first. Capped; deterministic."""
    blob = " ".join((item.get("label", ""), item.get("sql", ""),
                     item.get("table", ""),
                     " ".join(item.get("columns", []) or []),
                     " ".join(item.get("filters", []) or []))).lower()
    tokens = set("".join(c if (c.isalnum() or c == "_") else " "
                         for c in blob).split())
    for token in list(tokens):
        tokens.update(token.split("_"))
    picked: list[str] = []
    for phrase, entry in multi:
        if phrase in blob and entry not in picked:
            picked.append(entry)
            if len(picked) >= cap:
                return picked
    for token in sorted(tokens, key=lambda t: (-len(t), t)):
        if len(token) < 3 or token in _SQL_NOISE:
            continue
        entry = single.get(token)
        if entry is None and len(token) > 3 and token.endswith("s") \
                and not token.endswith("ss"):
            entry = single.get(token[:-1])      # naas → NAA
        if entry and entry not in picked:
            picked.append(entry)
            if len(picked) >= cap:
                break
    return picked


def _referenced_columns(sql: str, build: Build, table: str) -> list[str]:
    columns = build.schema.get(table, {})
    sql_tokens = {t for t in
                  "".join(c if (c.isalnum() or c == "_") else " "
                          for c in sql.lower()).split()}
    return sorted(c for c in columns if c.lower() in sql_tokens)[:12]


def _context(row: dict[str, Any], build: Build,
             purposes: dict[str, str],
             lob_of: dict[str, str],
             vocab: tuple[dict[str, str],
                          list[tuple[str, str]]] | None = None
             ) -> dict[str, Any]:
    table = row.get("table", "")
    item = {
        "table": table,
        "table_purpose": purposes.get(table, ""),
        "line_of_business": (row.get("line_of_business")
                             or lob_of.get(table, "")),
        "domain": row.get("domain", ""),
        "columns": _referenced_columns(row.get("canonical_sql", ""),
                                       build, table),
        # the b1.2 field lesson: the discriminating literals (a page
        # name, a channel, a method flag) live in the FILTERS, not the
        # aggregate expression — without them the model cannot name
        # what the metric is scoped to
        "filters": [str(f) for f in
                    (row.get("common_filters") or [])[:4]],
        "sql": row.get("canonical_sql", ""),
        "label": row.get("label") or "",
    }
    if vocab is not None:
        # the company's own reference shelf, scoped to THIS metric's
        # text — acronym expansions + business-term definitions the
        # model cannot know from generic reasoning
        item["vocab"] = _vocab_for(item, vocab[0], vocab[1])
    return item


# ── planning ─────────────────────────────────────────────────


def plan_metric_items(build: Build, folded_nodes: dict,
                      limit: int,
                      prefer: tuple[str, ...] = ()
                      ) -> list[dict[str, Any]]:
    """Mined metrics missing BOTH a question and a grain — highest
    support first (deterministic). The never-clobber guard checks the
    GRAPH fold, not just the build, so a re-run before recompile still
    skips already-enriched nodes."""
    purposes = _table_purposes(build)
    lob_of = _lob_by_table(build)
    vocab = _vocab_index(build)
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
        if (props.get("question_enriched")
                or props.get("grain_enriched")) \
                and _enriched_current(props):
            continue          # enriched by THIS prompt version or newer
        if not row.get("canonical_sql"):
            continue
        item = {"kind": "metric", "id": row["id"], "fp": row["fp"],
                **_context(row, build, purposes, lob_of, vocab),
                "support": row.get("support", 0),
                "has_question": bool(props.get("question_answered")),
                "has_grain": bool(props.get("grain")),
                "grain_observed": str(props.get("grain_observed")
                                      or "")}
        items.append(item)
        # with a preference list, over-collect so the sort has a pool
        # to promote from; without one, stop exactly at the limit
        if len(items) >= (limit * 4 if prefer else limit):
            break
    # demand seeding (opt-in): items matching a preferred term float to
    # the front — the first tranche covers what users actually ask
    # about; support still orders within each group
    if prefer:
        terms = tuple(t.lower() for t in prefer if t)
        items.sort(key=lambda i: (
            0 if any(t in (i["label"] + " " + i["sql"]).lower()
                     for t in terms) else 1,
            -i["support"], i["id"]))
    return items[:limit]


def plan_concept_items(build: Build, folded_nodes: dict,
                       limit: int) -> list[dict[str, Any]]:
    """Concept labels with no description anywhere — total binding
    support first."""
    by_label: dict[str, list[dict[str, Any]]] = {}
    for row in build.bindings:
        by_label.setdefault(row["label"], []).append(row)
    vocab = _vocab_index(build)
    items = []
    for label, rows in sorted(
            by_label.items(),
            key=lambda kv: (-sum(r.get("support", 0) for r in kv[1]),
                            kv[0])):
        def _current(record) -> bool:
            return (record is not None
                    and record.props.get("description_enriched")
                    and _enriched_current(record.props))
        if any(_current(folded_nodes.get(concept_id(label, r["table"])))
               for r in rows):
            continue
        binding_sql = " ".join(r["canonical_sql"] for r in rows[:6])
        items.append({
            "kind": "concept", "label": label,
            "tables": sorted({r["table"] for r in rows}),
            "vocab": _vocab_for({"label": label, "sql": binding_sql},
                                vocab[0], vocab[1]),
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
    vocab = _vocab_index(build)
    items = []
    for row in sorted(build.metrics, key=lambda r: r["id"]):
        if row.get("status") not in ("certified", "pending"):
            continue
        if not row.get("label") or not row.get("canonical_sql"):
            continue
        # the exam takes the SAME configuration the enricher runs
        # with — vocab included — but the label is scrubbed from the
        # lookup text below by rebuilding vocab AFTER withholding it
        item = _context({**row, "label": ""}, build, purposes, lob_of,
                        vocab)
        item.update({"id": row["id"], "true_label": row["label"]})
        item["label"] = ""               # withheld — that's the test
        items.append(item)
    return items


def _key_parts(true_label: str) -> list[str]:
    """Answer-key normalization (grader v1.2): the catalog writes some
    entries as TWO names joined by ' / ' ('Submitter Merchant Count /
    Submitter Active Locations in Force') — matching either whole part
    is a recovery. Split only on spaced slashes: 'Local/Foreign' is one
    name, never two."""
    parts = [p.strip() for p in str(true_label).split(" / ")
             if p.strip()]
    return parts or [str(true_label)]


def recovery_share(true_label: str, predicted: str) -> float:
    best = 0.0
    for part in _key_parts(true_label):
        truth = _tokens(part)
        if truth:
            best = max(best, len(truth & _tokens(predicted))
                       / len(truth))
    return round(best, 3)


def grade_recovery(true_label: str, predicted: str) -> bool:
    predicted_tokens = _tokens(predicted)
    for part in _key_parts(true_label):
        truth = _tokens(part)
        if not truth:
            continue
        # negation veto: 'Card Not Present X' vs 'Card Present X' are
        # OPPOSITE metrics — polarity disagreement fails the part no
        # matter how much else overlaps (b1.1 field false-positive;
        # the veto caught the swap again in the b1.2 run)
        if (truth & _NEGATIONS) != (predicted_tokens & _NEGATIONS):
            continue
        if len(truth & predicted_tokens) / len(truth) \
                >= RECOVERY_TOKEN_SHARE:
            return True
    return False


def run_blind_gate(build: Build, client: VertexClient, out_dir: Path,
                   sample_n: int = 0,
                   log: Callable[[str], None] = print) -> dict[str, Any]:
    items = blind_items(build)
    if sample_n:
        items = items[:sample_n]
    results = []
    recovered = 0
    leaky = 0
    for item in items:
        # leakage instrumentation: how much of the WITHHELD name the
        # assembled context already gives away (purpose/domain/lob
        # labels — a card purpose that names its flagship metric turns
        # recovery into a measurement of leakage). Published per item;
        # a high rate with high leakage is not a pass to trust.
        context_text = " ".join((item.get("table_purpose", ""),
                                 item.get("domain", ""),
                                 item.get("line_of_business", "")))
        leak = recovery_share(item["true_label"], context_text)
        leaky += int(leak >= 0.5)
        answer = parse_json_answer(client.generate(
            blind_name_prompt(item), system=SYSTEM)) or {}
        predicted = str(answer.get("name") or "")
        ok = grade_recovery(item["true_label"], predicted)
        recovered += int(ok)
        results.append({"id": item["id"], "true": item["true_label"],
                        "predicted": predicted, "recovered": ok,
                        "share": recovery_share(item["true_label"],
                                                predicted),
                        "context_leak": leak})
    rate = round(recovered / len(items), 4) if items else 0.0
    tier = ("batch" if rate >= TIER_BATCH else
            "item" if rate >= TIER_ITEM else "halt")
    (out_dir / "blind_results.jsonl").write_text(
        "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n"
                for r in results), encoding="utf-8")
    log(f"blind gate: {recovered}/{len(items)} recovered "
        f"({rate:.0%}) → tier {tier} · leaky contexts {leaky}")
    return {"n": len(items), "recovered": recovered, "rate": rate,
            "tier": tier, "leaky_contexts": leaky,
            "grader": "v1.2 (token-share + negation veto + "
                      "answer-key normalization)"}


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
        observed = str(item.get("grain_observed") or "").strip()
        if observed and _normalize_question(grain) \
                != _normalize_question(observed):
            # two witnesses disagreeing about grain is exactly the
            # class of finding this system exists to surface — both
            # values stay recorded, a steward decides
            emit_review_item(
                graph, kind="witness_divergence", subject=item["id"],
                proposal=(f"enriched grain {grain!r} disagrees with "
                          f"studio-observed grain {observed!r}"),
                evidence=[f"vertex:{model}#prompt={PROMPT_VERSION}",
                          "studio:grain_observed"],
                run_id=run_id, source=SOURCE, witness="llm_enriched",
                support_effective=item.get("support", 1),
                blast_radius=1,
                agent_recommendation="prefer the observed grain — it "
                                     "came from real certified SQL")
            report["grain_divergences"] += 1
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
               fresh: bool = False, prefer: tuple[str, ...] = (),
               client: VertexClient | None = None,
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
        "grain_divergences": 0,
        "blind": None, "usage": None, "plan_only": plan_only,
    }
    metric_items = (plan_metric_items(build, folded, limit,
                                      prefer=prefer)
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
