"""Retrieval eval — is the graph findable the way analysts actually ask?

Node counts and readiness scorecards measure richness; neither measures
RETRIEVAL: type what an analyst would type, check whether the right node
comes back on top. This module closes that gap with a gold set extracted
from evidence the graph already holds — the two ingested catalogs double
as labeled retrieval data:

    DMP rows        questionAnswered → metric → table   (natural language)
    mined measures  measure name → table                (vocabulary-shaped)
    business names  table/metric display names          (curated phrasing)

Gold-example kinds, honest about what each proves:

    dmp_question           the curated question verbatim. The question
                           text IS indexed (question_answered is a search
                           haystack), so this measures resolver behavior
                           — the regression floor, not generalization.
    dmp_question_heldout   the question with the metric's own name tokens
                           REMOVED — can surrounding vocabulary still
                           route it? This is the generalization probe.
    metric_name            curated metric display name → metric node.
    mined_measure_name     usage-mined measure name → metric node.
    table_business_name    curated table display name → table node.
    domain_route           the domain label itself → its Domain node
                           (only when the rollup ran).

Scoring: an example is credited at the rank of the FIRST hit whose uri is
in its expected set. hit@1/3/5 + MRR overall and per kind; failures carry
what outranked the expected node, so a bad score is immediately
diagnosable. Deterministic end to end — same snapshot, same numbers.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any

from synapse.graph.store import GraphStore, canonical_uri

_HIT_KS = (1, 3, 5)


@dataclass
class GoldExample:
    """One labeled retrieval case: type ``query``, expect one of
    ``expect`` (canonical URIs) near the top."""

    query: str
    expect: list[str]
    kind: str
    note: str = ""


@dataclass
class KindStats:
    n: int = 0
    mrr_sum: float = 0.0
    hits: dict[int, int] = field(
        default_factory=lambda: {k: 0 for k in _HIT_KS})

    def credit(self, rank: int | None) -> None:
        self.n += 1
        if rank is None:
            return
        self.mrr_sum += 1.0 / rank
        for k in _HIT_KS:
            if rank <= k:
                self.hits[k] += 1

    def report(self) -> dict[str, Any]:
        n = max(self.n, 1)
        return {
            "n": self.n,
            "mrr": round(self.mrr_sum / n, 3),
            **{f"hit@{k}": round(self.hits[k] / n, 3) for k in _HIT_KS},
        }


def _tokens(text: str) -> set[str]:
    return {t for t in "".join(
        c if c.isalnum() else " " for c in text.lower()).split() if t}


def extract_gold_set(store: GraphStore) -> list[GoldExample]:
    """Pull every labeled (query → expected node) pair the graph's own
    evidence supports. No files re-read, no LLM, no randomness — the
    same snapshot always yields the same gold set."""
    gold: list[GoldExample] = []
    seen: set[tuple[str, tuple[str, ...]]] = set()

    def add(query: str, expect: list[str], kind: str, note: str = "") -> None:
        query = " ".join(str(query).split())
        if not query or not expect:
            return
        key = (query.lower(), tuple(sorted(expect)))
        if key in seen:
            return
        seen.add(key)
        gold.append(GoldExample(query=query, expect=expect,
                                kind=kind, note=note))

    for m in store.nodes_by_type("Metric"):
        props = m.properties
        table = str(props.get("sourced_from_table") or "")
        expect = [m.canonical_uri]
        if table:
            expect.append(canonical_uri("table", table))
        name = str(props.get("business_name") or "")
        mined = "usage_mined" in m.provenance.sources
        if name:
            add(name, expect,
                "mined_measure_name" if mined else "metric_name")
        question = str(props.get("question_answered") or "")
        if question:
            add(question, expect, "dmp_question")
            # held-out variant: strip the metric's own name tokens; what
            # remains must route through OTHER vocabulary. ≥2 content
            # tokens or the probe is noise, not signal.
            name_tokens = _tokens(name) | _tokens(
                m.canonical_uri.rsplit("/", 1)[-1].replace("_", " "))
            remaining = [t for t in _tokens(question)
                         if t not in name_tokens]
            if len(remaining) >= 2:
                add(" ".join(sorted(remaining)), expect,
                    "dmp_question_heldout",
                    note=f"question minus name tokens of {name!r}")

    for t in store.nodes_by_type("Table"):
        biz = str(t.properties.get("business_name") or "")
        raw = str(t.properties.get("table_name") or "")
        if biz and biz.lower() != raw.lower():
            add(biz, [t.canonical_uri], "table_business_name")

    for u in store.nodes_by_type("Domain"):
        name = str(u.properties.get("name") or "")
        if name:
            add(name, [u.canonical_uri], "domain_route")

    return gold


def evaluate_retrieval(service: Any, gold: list[GoldExample],
                       top_k: int = 10,
                       max_failures: int = 20) -> dict[str, Any]:
    """Run every gold query through ``service.search_entities`` and score
    where the expected node landed. ``service`` is a GraphService (or
    anything with the same search contract returning hits with ``uri``)."""
    started = time.monotonic()
    overall = KindStats()
    by_kind: dict[str, KindStats] = {}
    failures: list[dict[str, Any]] = []

    for ex in gold:
        resp = service.search_entities(ex.query, top_k=top_k)
        hits = ((resp.get("data") or {}).get("hits", [])
                if isinstance(resp, dict) else [])
        expected = set(ex.expect)
        rank = None
        for i, h in enumerate(hits, start=1):
            if h.get("uri") in expected:
                rank = i
                break
        overall.credit(rank)
        by_kind.setdefault(ex.kind, KindStats()).credit(rank)
        if (rank is None or rank > 3) and len(failures) < max_failures:
            failures.append({
                "query": ex.query,
                "kind": ex.kind,
                "expected": ex.expect,
                "rank": rank,
                "top_hits": [
                    {"uri": h.get("uri"), "name": h.get("name"),
                     "score": h.get("score")} for h in hits[:3]],
            })

    return {
        "n_examples": overall.n,
        "top_k": top_k,
        "overall": overall.report(),
        "by_kind": {k: s.report() for k, s in sorted(by_kind.items())},
        "failures": failures,
        "snapshot_version": getattr(
            getattr(service, "store", None), "snapshot_version", ""),
        "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
    }


def format_report(report: dict[str, Any]) -> str:
    """The scoreboard, human-readable — what you watch run over run."""
    lines = [
        f"retrieval eval — {report['n_examples']} gold examples "
        f"(top_k={report['top_k']}, "
        f"snapshot {report.get('snapshot_version') or 'unversioned'})",
        f"{'kind':24} {'n':>4} {'MRR':>6} {'h@1':>6} {'h@3':>6} {'h@5':>6}",
    ]

    def row(label: str, stats: dict[str, Any]) -> str:
        return (f"{label:24} {stats['n']:>4} {stats['mrr']:>6.3f} "
                f"{stats['hit@1']:>6.3f} {stats['hit@3']:>6.3f} "
                f"{stats['hit@5']:>6.3f}")

    lines.append(row("OVERALL", report["overall"]))
    for kind, stats in report["by_kind"].items():
        lines.append(row(kind, stats))
    if report["failures"]:
        lines.append(f"worst failures ({len(report['failures'])} shown):")
        for f in report["failures"][:10]:
            got = ", ".join(str(h["name"]) for h in f["top_hits"]) or "—"
            lines.append(f"  [{f['kind']}] {f['query'][:60]!r} "
                         f"rank={f['rank']} got: {got[:70]}")
    return "\n".join(lines)


def gold_to_json(gold: list[GoldExample]) -> list[dict[str, Any]]:
    return [asdict(g) for g in gold]
