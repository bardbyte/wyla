"""E23 — Knowledge Catalog enrichment: the module's contract, against
the REAL compiled fixture build. Import boundary, coverage gate,
extractors, determinism, include rules, verifier, malformed answers,
the blind gate, cache idempotency, ledger reconciliation, suggestions,
exports against checked-in samples, the actor-signed push record, the
read-back loader, budgets, events, and the CLI."""

from __future__ import annotations

import csv
import io
import json
import re
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.compiler.compile import compile_build                # noqa: E402
from sahs.graph.quads import GraphDir                          # noqa: E402
from sahs.graph.validate import validate_graph                 # noqa: E402
from sahs.kc import coverage                                   # noqa: E402
from sahs.kc.assemble import (COPY, NEVER, REVIEW, TIER_CANDIDATE,  # noqa: E402
                              Fact, FactSet, assemble, include_of)
from sahs.kc.bundle import (build_bundle, coverage_payload, export_all,  # noqa: E402
                            glossary_across, glossary_export, list_tables,
                            record_push)
from sahs.kc.config import KcConfig, load_config               # noqa: E402
from sahs.kc.export import (SHEET_COLUMNS, entry_links_jsonl,  # noqa: E402
                            entry_patch_payload, export, glossary_import_jsonl)
from sahs.kc.render import construct_of, section_of            # noqa: E402
from sahs.kc.verify import verify_output, verify_text          # noqa: E402
from sahs.kc.witness import import_kc_export                   # noqa: E402
from sahs.kc.write import (Writer, cache_path, gate_tier, leakage,  # noqa: E402
                           run_gate, token_f1)
from sahs.tools.api import Build                               # noqa: E402

FX = SILO / "tests" / "fixtures"
TABLE = "dw.gms_transaction"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory) -> dict:
    tmp = tmp_path_factory.mktemp("kc")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"), "build-graph",
         "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "kc_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    # one enriched metric on the table, as the B1 loop would leave it:
    # the fixture sources carry no enrichment run, and the module's
    # "Suggested — unreviewed" path must be exercised on real records
    from sahs.graph.quads import NodeRecord, Prov
    graph = GraphDir(graph_dir)
    target = next(n for n in graph.fold_nodes().values()
                  if n.id.startswith("metric:") and n.props.get("label") == "local_spend_method_a")
    graph.append_node(NodeRecord(id=target.id, props={
        "question_enriched": "How much was spent locally, by method a?",
        "enrich_prompt_version": "b1.4", "enrich_confidence": 0.7},
        prov=Prov(source="llm_enricher", run="kc_r1_enrich", witness="llm_enriched",
                  evidence="vertex:fixture")))
    builds = tmp / "builds"
    _dir, _manifest, failures = compile_build(graph_dir, builds)
    assert not failures
    return {"builds": builds, "graph": graph_dir, "tmp": tmp,
            "build": Build.open(builds)}


@pytest.fixture()
def cfg(compiled) -> KcConfig:
    c = load_config(SILO / "config" / "kc.yaml")
    c.project, c.location, c.glossary = "demo-proj", "us", "meridian"
    return c


class FakeClient:
    """Answers every prompt with sentences built from the prompt's own
    facts (so the verifier keeps them); counts calls; can be told to
    answer junk once, or to echo a withheld description for the gate."""

    def __init__(self, junk_first: bool = False, echo: dict | None = None) -> None:
        self.usage = {"calls": 0, "prompt_tokens": 0, "output_tokens": 0,
                      "thought_tokens": 0}
        self.junk_first = junk_first
        self.echo = echo or {}
        self.prompts: list[str] = []

        class connection:                          # noqa: N801
            model = "fake-model"
        self.connection = connection

    def generate(self, prompt: str, *, system: str = "", temperature: float = 0.2,
                 max_output_tokens: int = 1024) -> str:
        self.usage["calls"] += 1
        self.usage["prompt_tokens"] += len(prompt) // 4
        self.usage["output_tokens"] += 80
        self.prompts.append(prompt)
        if self.junk_first:
            self.junk_first = False
            return "this is not json {"
        facts = re.findall(r"^\[(f\d+)\] (.+?) \[", prompt, flags=re.M)
        if "Write a one-sentence description of the table" in prompt:
            cols = [{"name": k, "text": v} for k, v in self.echo.items() if k != "__table__"]
            return json.dumps({"description": self.echo.get("__table__", ""),
                               "columns": cols, "confidence": 0.9, "caveat": ""})
        sentences = [f"{text.rstrip('.')} [{fid}]." for fid, text in facts[:6]]
        columns = []
        for fid, text in facts:
            m = re.match(r"^([A-Za-z0-9_.]+) \(", text)
            if m:
                columns.append({"name": m.group(1), "text": f"{text.rstrip('.')} [{fid}].",
                                "fact_ids": [fid]})
        glossary = []
        for fid, text in facts:
            if ":" in text and "›" not in text:
                term = text.split(":", 1)[0].strip()
                glossary.append({"term": term, "definition": f"{text.rstrip('.')} [{fid}].",
                                 "synonyms": [], "fact_ids": [fid], "status_note": ""})
        return json.dumps({
            "description": sentences[0] if sentences else "",
            "description_fact_ids": [facts[0][0]] if facts else [],
            "overview_sections": {"purpose": {"text": " ".join(sentences[:2]),
                                              "fact_ids": [f for f, _ in facts[:2]]}},
            "columns": columns[:20], "glossary": glossary[:20],
            "confidence": 0.8, "caveat": ""})


# ── 1 · import boundary ──────────────────────────────────────────
def test_import_boundary():
    allowed_ask = {"sahs.ask.budget", "sahs.ask.events"}
    for path in (SILO / "sahs" / "kc").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        for m in re.finditer(r"^\s*(?:from|import)\s+(sahs\.[\w.]+)", text, flags=re.M):
            mod = m.group(1)
            if mod.startswith("sahs.ask"):
                assert mod in allowed_ask, f"{path.name} imports {mod}"
            assert not mod.startswith("sahs.assistant"), f"{path.name} imports {mod}"
            assert not mod.startswith("sahs.loop"), f"{path.name} imports {mod}"


# ── 2 · coverage completeness gate ───────────────────────────────
def test_coverage_gate_complete(compiled):
    missing = coverage.missing_rows(compiled["graph"], compiled["build"].root)
    assert missing == [], f"unrowed graph items: {missing[:10]}"
    report = coverage.coverage_report(compiled["graph"], compiled["build"].root)
    assert report["counts"]["rows"] == len(coverage.ROWS)
    assert report["counts"]["pct_targeted"] > 50
    # a fake item is caught
    assert coverage.row_for("node:table.made_up_prop_zz") is None


# ── 3 · every row has an extractor and a rendered destination ────
def test_rows_extractors_destinations(compiled, cfg):
    from sahs.kc.assemble import EXTRACTOR_FUNCS
    for row in coverage.ROWS:
        if row.representation != coverage.EXCLUDED:
            assert row.extractor in EXTRACTOR_FUNCS, row.item
    assert set(EXTRACTOR_FUNCS) == set(coverage.EXTRACTORS)
    fs = assemble(compiled["build"], compiled["graph"], TABLE)
    fired = {f.extractor for f in fs.facts}
    assert fired == set(coverage.EXTRACTORS), set(coverage.EXTRACTORS) - fired
    bundle = build_bundle(TABLE, use_llm=False, builds_root=compiled["builds"],
                          graph_root=compiled["graph"], config=cfg)
    placed = {fid for sec in bundle.sections.values() for fid in sec.facts_used}
    placed |= {fid for sec in bundle.sections.values() for r in sec.review for fid in [r["id"]]}
    placed |= {i["id"] for g in bundle.suggestions for i in g["items"]}
    unplaced = [f for f in fs.facts if f.id not in placed]
    assert not unplaced, [(f.id, f.kc_target, f.include) for f in unplaced[:8]]
    for f in fs.facts:
        assert construct_of(f.kc_target) != f.kc_target or f.kc_target == "query"


# ── 4 · determinism ──────────────────────────────────────────────
def test_assemble_deterministic(compiled):
    a = assemble(compiled["build"], compiled["graph"], TABLE)
    b = assemble(compiled["build"], compiled["graph"], TABLE)
    assert a.digest() == b.digest()
    assert [f.to_dict() for f in a.facts] == [f.to_dict() for f in b.facts]
    assert all(f.id == f"f{i}" for i, f in enumerate(a.facts, 1))


# ── 5 · include rules: every never and review case ───────────────
def _fact(**kw) -> Fact:
    base = dict(id="f1", kind="metric", text="x", kc_target="glossary.term",
                representation=coverage.GLOSSARY, witness="dmp", status="certified",
                prov={})
    base.update(kw)
    return Fact(**base)


def test_include_rules():
    assert include_of(_fact(witness="user_variant"))[0] == NEVER
    assert include_of(_fact(kind="acronym", kc_target="glossary.synonym",
                            data={"common_word": True}))[0] == NEVER
    assert include_of(_fact(kind="query", kc_target="query",
                            data={"gold": True, "attested": False}))[0] == NEVER
    assert include_of(_fact(kind="query", kc_target="query", status="observed",
                            data={"gold": True, "attested": True}))[0] == COPY
    assert include_of(_fact(kind="join", kc_target="aspect.join-paths.paths",
                            status="observed", data={"tier": TIER_CANDIDATE}))[0] == NEVER
    assert include_of(_fact(status="rejected"))[0] == NEVER
    assert include_of(_fact(status="deprecated"))[0] == NEVER
    assert include_of(_fact(witness="llm_enriched", text="plain model text",
                            status="unreviewed"))[0] == NEVER
    assert include_of(_fact(witness="llm_enriched", status="unreviewed",
                            text="Suggested — unreviewed: model text"))[0] == REVIEW
    assert include_of(_fact(kc_target="suggestion.metric", status="mined"))[0] == REVIEW
    assert include_of(_fact(status="contested", kc_target="aspect.concept-bindings.bindings",
                            representation=coverage.ASPECT))[0] == REVIEW
    assert include_of(_fact(kind="policy", status="unknown_policy",
                            text="row-access policy unknown"))[0] == NEVER
    assert include_of(_fact(kind="policy", status="unknown_policy",
                            text="row-access policy unknown — treat as restricted"))[0] == COPY
    assert include_of(_fact(status="pending", text="a pending metric"))[0] == REVIEW
    assert include_of(_fact(status="pending", text="pending · catalog ×3 — grain"))[0] == COPY
    assert include_of(_fact(status="pending", kc_target="aspect.governed-metrics.metrics",
                            representation=coverage.ASPECT))[0] == COPY
    # the enriched metric on the fixture table rides only under its flag
    assert include_of(_fact(status="mined", kc_target="glossary.related_term"))[0] == REVIEW
    assert include_of(_fact(representation=coverage.EXCLUDED, kc_target="excluded"))[0] == NEVER
    assert include_of(_fact())[0] == COPY


def test_include_rules_hold_on_the_real_table(compiled):
    fs = assemble(compiled["build"], compiled["graph"], TABLE)
    for f in fs.facts:
        verdict, _ = include_of(f)
        assert f.include == verdict
        if f.kc_target.startswith("suggestion."):
            assert f.include != COPY and f.showcase
        if isinstance(f.data, dict) and f.data.get("tier") == TIER_CANDIDATE \
                and f.kc_target.startswith("aspect.join-paths"):
            assert f.include == NEVER
    enriched = [f for f in fs.facts if "llm_enriched" in f.witness]
    assert enriched and all(f.include != COPY and "Suggested — unreviewed" in f.text
                            for f in enriched)
    # the unknown-policy table carries the restricted note
    fs2 = assemble(compiled["build"], compiled["graph"], "dw.wwcas_authorization")
    policy = [f for f in fs2.facts if f.status == "unknown_policy"]
    assert policy and all("treat as restricted" in f.text and f.include == COPY for f in policy)


# ── 6 · the verifier ─────────────────────────────────────────────
def test_grounding_verifier(compiled):
    fs = assemble(compiled["build"], compiled["graph"], TABLE)
    good = next(f for f in fs.facts if f.kc_target == "entry.description")
    ok = verify_text(f"{good.text.rstrip('.')} [{good.id}].", fs)
    assert ok["kept"] and not ok["dropped"]
    foreign_col = verify_text(f"The column zz_made_up.value drives it [{good.id}].", fs)
    assert foreign_col["dropped"][0]["reason"].startswith("identifier not in the facts")
    foreign_num = verify_text(f"It holds 987654 rows [{good.id}].", fs)
    assert foreign_num["dropped"][0]["reason"].startswith("number not in the facts")
    mismatch = verify_text(f"This table is pending review [{good.id}].", fs)
    assert "status word" in mismatch["dropped"][0]["reason"]
    no_cite = verify_text("A sentence with no citation.", fs)
    assert no_cite["dropped"][0]["reason"] == "no fact citation"
    unknown = verify_text("Cited wrongly [f99999].", fs)
    assert "unknown fact id" in unknown["dropped"][0]["reason"]
    forbidden = verify_text(f"Taken from the answer key [{good.id}].", fs)
    assert "forbidden" in forbidden["dropped"][0]["reason"]
    out = verify_output({"description": "no cite here.", "columns": [
        {"name": "cm13", "text": f"{good.text.rstrip('.')} [{good.id}]."}]}, fs)
    assert out["description"] == "" and out["columns"] and out["dropped"]


# ── 7 · malformed JSON is counted, the run continues ─────────────
def test_malformed_json_refusal(compiled, cfg):
    fs = assemble(compiled["build"], compiled["graph"], TABLE)
    client = FakeClient(junk_first=True)
    record = Writer(client, cfg).write(fs)
    assert record["invalid_json"] == 1
    assert record["calls"] == 3                      # overview junk, columns + glossary fine
    assert record["overview_sections"] == {} and record["description"] == ""
    assert record["columns"]                         # later sections still landed


# ── 8 · gate math + leakage grep ─────────────────────────────────
def test_gate_math(compiled, cfg):
    assert token_f1("card member number", "the card member number") > 0.8
    assert token_f1("card member number", "spend in usd") == 0.0
    assert leakage("Global merchant transaction spine", "the merchant transaction spine here")
    assert not leakage("Global merchant transaction spine", "merchant spend by transaction")
    assert gate_tier(0.5, cfg) == "halt" and gate_tier(0.7, cfg) == "review" \
        and gate_tier(0.9, cfg) == "copy"
    fs = assemble(compiled["build"], compiled["graph"], TABLE)
    truth = _truth(fs)
    result = run_gate(fs, FakeClient(echo=truth), cfg)
    assert result["n"] == len(truth) and result["recovered"] == result["n"]
    assert result["tier"] == "copy" and result["line"].startswith("kc gate:")
    # the withheld table description is not in what the model saw; every
    # score reports its leaks (a list), so a leaky context is visible
    assert result["scores"]["__table__"]["leaks"] == []
    assert all(isinstance(s["leaks"], list) for s in result["scores"].values())
    assert result["leaky_contexts"] == sum(1 for s in result["scores"].values() if s["leaks"])
    blank = run_gate(fs, FakeClient(echo={}), cfg)
    assert blank["tier"] == "halt" and blank["recovered"] == 0


# ── 9 · cache idempotency: second load = 0 model calls ───────────
def _truth(fs: FactSet) -> dict[str, str]:
    truth = {}
    for f in fs.facts:
        if f.kc_target == "entry.description":
            truth.setdefault("__table__", f.text)
        elif f.kc_target == "column.description" and isinstance(f.data, dict) \
                and f.data.get("description") and not f.data.get("supplementary"):
            truth.setdefault(f.data["column"], f.data["description"])
    return truth


def test_cache_idempotency(compiled, cfg):
    fs = assemble(compiled["build"], compiled["graph"], TABLE)
    client = FakeClient(echo=_truth(fs))          # passes the gate, then writes
    first = build_bundle(TABLE, use_llm=True, client=client, builds_root=compiled["builds"],
                         graph_root=compiled["graph"], config=cfg)
    assert first.gate["tier"] == "copy", first.gate
    assert first.llm["generated"], first.llm
    assert client.usage["calls"] >= 2                                  # gate + sections
    assert cache_path(compiled["graph"], cfg, TABLE, first.build_id).exists()
    calls = client.usage["calls"]
    second = build_bundle(TABLE, use_llm=True, client=client, builds_root=compiled["builds"],
                          graph_root=compiled["graph"], config=cfg)
    assert client.usage["calls"] == calls and second.llm["cached"]
    assert second.sections["overview"].llm and second.sections["description"].llm
    third = build_bundle(TABLE, use_llm=True, regenerate=True, client=client,
                         builds_root=compiled["builds"], graph_root=compiled["graph"],
                         config=cfg)
    assert client.usage["calls"] > calls and third.llm["generated"]
    # deterministic sections are identical with and without the model
    plain = build_bundle(TABLE, use_llm=False, builds_root=compiled["builds"],
                         graph_root=compiled["graph"], config=cfg)
    assert plain.sections["aspects"].text == third.sections["aspects"].text
    assert plain.digest == third.digest


# ── 10 · ledger reconciles with the bundle ───────────────────────
def test_ledger_reconciles(compiled, cfg):
    bundle = build_bundle(TABLE, use_llm=False, builds_root=compiled["builds"],
                          graph_root=compiled["graph"], config=cfg)
    total = sum(r["count"] for r in bundle.ledger)
    assert total == len(bundle.facts)
    assert sum(r["translated"] for r in bundle.ledger) == bundle.counts.get("copy", 0)
    assert sum(r["review"] for r in bundle.ledger) == bundle.counts.get("review", 0)
    assert sum(r["withheld"] for r in bundle.ledger) == bundle.counts.get("never", 0)
    for r in bundle.ledger:
        assert r["translated"] + r["review"] + r["withheld"] == r["count"]
        assert len(r["fact_ids"]) == r["count"]


# ── 11 · suggestions never in copy blocks ────────────────────────
def test_suggestions_never_in_copy_blocks(compiled, cfg):
    bundle = build_bundle(TABLE, use_llm=False, builds_root=compiled["builds"],
                          graph_root=compiled["graph"], config=cfg)
    suggested = {i["id"] for g in bundle.suggestions for i in g["items"]}
    assert suggested
    for key, sec in bundle.sections.items():
        if key in ("review", "push"):
            continue
        assert not (set(sec.facts_used) & suggested), key
    by_id = {f["id"]: f for f in bundle.facts}
    assert all(by_id[i]["include"] != COPY and by_id[i]["showcase"] for i in suggested)


# ── 12 · the dictionary renders from coverage only ───────────────
def test_dictionary_from_coverage_only(compiled):
    payload = coverage_payload(builds_root=compiled["builds"], graph_root=compiled["graph"])
    forward = payload["dictionary"]["forward"]
    assert [r["meridian"] for r in forward] == [r.item for r in coverage.ROWS]
    assert {r["kc"] for r in forward} == {r.kc_target for r in coverage.ROWS}
    reverse_items = {i for r in payload["dictionary"]["reverse"] for i in r["meridian"]}
    assert reverse_items == {r.item for r in coverage.ROWS}


# ── 13 · export shapes against checked-in samples ────────────────
def _keys(obj: dict, prefix: str = "") -> set[str]:
    out = set()
    for k, v in obj.items():
        if k.startswith("_"):
            continue
        out.add(prefix + k)
        if isinstance(v, dict) and k not in ("data", "aspects", "entrySource"):
            out |= _keys(v, prefix + k + ".")
    return out


def test_export_shapes_vs_samples(compiled, cfg):
    bundle = build_bundle(TABLE, use_llm=False, builds_root=compiled["builds"],
                          graph_root=compiled["graph"], config=cfg)
    sample_dir = FX / "kc"
    term_sample = json.loads((sample_dir / "glossary_entry.sample.json").read_text())
    lines = [json.loads(l) for l in glossary_import_jsonl(bundle, cfg).splitlines()[1:]]
    terms = [l for l in lines if l["entry"]["entryType"].endswith("glossary-term")]
    assert terms
    for line in terms:
        assert _keys(line["entry"]) >= _keys(term_sample["entry"]) - {"aspects"}
        assert "dataplex-types.global.glossary-term-aspect" in line["entry"]["aspects"]
        assert line["entry"]["name"].startswith("projects/demo-proj/locations/us/")
    link_sample = json.loads((sample_dir / "entry_link.sample.json").read_text())
    links = [json.loads(l) for l in entry_links_jsonl(bundle, cfg).splitlines()[1:]]
    assert links
    for link in links:
        assert _keys(link["entryLink"]) >= _keys(link_sample["entryLink"])
        assert {r["type"] for r in link["entryLink"]["entryReferences"]} == {"SOURCE", "TARGET"}
        assert link["entryLink"]["entryLinkType"].rsplit("/", 1)[1] in ("definition", "synonym", "related")
    column_links = [l for l in links if any("path" in r for r in l["entryLink"]["entryReferences"])]
    assert column_links, "a term → column link carries the column path"
    patch_sample = json.loads((sample_dir / "entry_patch.sample.json").read_text())
    patch = entry_patch_payload(bundle, cfg)
    assert _keys(patch) >= _keys(patch_sample) - {"aspects"}
    assert any(k.startswith("demo-proj.us.") for k in patch["aspects"])
    assert "dataplex-types.global.overview" in patch["aspects"]
    aspect_sample = json.loads((sample_dir / "aspect_type.sample.json").read_text())
    for template in bundle.aspect_types:
        assert _keys(template["metadataTemplate"]) >= _keys(aspect_sample["metadataTemplate"])
        for fld in template["metadataTemplate"]["recordFields"]:
            assert {"name", "type", "index", "annotations"} <= set(fld)
    sheet, _media, _name = export(bundle, "sheet", cfg)
    header = next(csv.reader(io.StringIO(sheet.decode("utf-8"))))
    assert tuple(header) == SHEET_COLUMNS
    assert header == next(csv.reader((sample_dir / "glossary_sheet.header.csv").open()))
    for fmt, media in (("md", "text/markdown"), ("json", "application/json"),
                       ("csv", "text/csv"), ("zip", "application/zip")):
        data, m, name = export(bundle, fmt, cfg)
        assert m == media and data and name.endswith(f".{fmt}" if fmt != "csv" else ".kc.csv")
    with zipfile.ZipFile(io.BytesIO(export(bundle, "zip", cfg)[0])) as zf:
        assert {n.rsplit("/", 1)[1] for n in zf.namelist()} >= {
            "bundle.md", "bundle.json", "facts.csv", "glossary_sheet.csv",
            "glossary_import.jsonl", "entry_links.jsonl", "entry_patch.json",
            "aspect_types.json"}
    with pytest.raises(ValueError):
        export(bundle, "docx", cfg)


# ── 14 · the push record is an actor-signed quad ─────────────────
def test_push_record_actor_signed(compiled, cfg):
    refused = record_push(TABLE, sections=["description"], actor="   ",
                          builds_root=compiled["builds"], graph_root=compiled["graph"],
                          config=cfg)
    assert refused["recorded"] is False and "actor" in refused["reason"]
    result = record_push(TABLE, sections=["description", "aspects", "push", "bogus"],
                         actor="jane.steward", note="entered by hand",
                         builds_root=compiled["builds"], graph_root=compiled["graph"],
                         config=cfg)
    assert result["recorded"] and result["record"]["sections"] == ["aspects", "description"]
    quads = [q for q in GraphDir(compiled["graph"]).iter_edges("kc_pushed")]
    assert quads and quads[-1].prov.source == "clerk" and quads[-1].prov.actor == "jane.steward"
    assert quads[-1].prov.witness == "steward" and quads[-1].o.startswith("run:kc_")
    assert quads[-1].props["build"] == compiled["build"].version
    assert set(quads[-1].props["hashes"]) == {"aspects", "description"}
    assert validate_graph(compiled["graph"]).ok
    bundle = build_bundle(TABLE, use_llm=False, builds_root=compiled["builds"],
                          graph_root=compiled["graph"], config=cfg)
    assert bundle.push_records and bundle.push_records[-1]["actor"] == "jane.steward"
    assert bundle.sections["push"].status_counts == {"pushed": len(bundle.push_records)}
    # the read-back and the record are rowed: the coverage gate stays green
    assert coverage.missing_rows(compiled["graph"], compiled["build"].root) == []


# ── 15 · the read-back loader files a pending kc witness ─────────
def test_witness_import_stub(compiled):
    report = import_kc_export(compiled["graph"], "glossary", {"items": [
        {"term": "Spend", "description": "Money moved", "table": TABLE},
        {"term": "Nope", "table": "zz.unknown"}, "junk"]},
        run_id="kc_import_t", actor="jane", known_tables=set(compiled["build"].schema))
    assert report["nodes"] == 1 and report["edges"] == 1 and len(report["skipped"]) == 2
    report = import_kc_export(compiled["graph"], "descriptions", {"items": [
        {"table": TABLE, "column": "cm13", "description": "Card member id (catalog)"}]},
        run_id="kc_import_t", known_tables=set(compiled["build"].schema))
    assert report["nodes"] == 1
    report = import_kc_export(compiled["graph"], "dq", {"items": [
        {"table": TABLE, "rule": "uniqueness", "column": "se_no", "passed": True}]},
        run_id="kc_import_t", known_tables=set(compiled["build"].schema))
    assert report["edges"] == 1
    assert import_kc_export(compiled["graph"], "weird", {}, run_id="x")["skipped"]
    graph = GraphDir(compiled["graph"])
    kc_nodes = [n for n in graph.fold_nodes().values() if n.prov.witness == "kc"]
    assert len(kc_nodes) == 3 and all(n.props["review_status"] == "pending" for n in kc_nodes)
    assert all(n.prov.source == "kc_export" for n in kc_nodes)
    assert validate_graph(compiled["graph"]).ok
    assert coverage.missing_rows(compiled["graph"], compiled["build"].root) == []


# ── 16 · the budget cap fires cleanly ────────────────────────────
def test_budget_cap_fires_cleanly(compiled):
    fs = assemble(compiled["build"], compiled["graph"], TABLE)
    tight = KcConfig(turn_calls=1, turn_tokens=10_000)
    record = Writer(FakeClient(), tight).write(fs)
    assert record["calls"] == 1 and record["stopped"].endswith("model-call cap")
    assert record["overview_sections"] and record["columns"] == []


# ── 17 · events ride the shared bus and land in the file ─────────
def test_events_family_and_file(compiled, cfg, tmp_path):
    from sahs.ask.events import EventBus
    from sahs.kc.write import KC_EVENTS
    fs = assemble(compiled["build"], compiled["graph"], TABLE)
    bus = EventBus("kc:test", path=tmp_path / "events.jsonl", events=KC_EVENTS)
    Writer(FakeClient(), cfg, bus=bus).write(fs)
    names = [e["ev"] for e in bus.since(0)]
    assert names[0] == "kc_started" and names[-1] == "kc_done"
    assert "kc_verified" in names and "kc_section" in names and "budget_tick" in names
    lines = (tmp_path / "events.jsonl").read_text().splitlines()
    assert len(lines) == len(names)
    with pytest.raises(ValueError):
        bus.emit("plan_delta")


# ── 18 · the CLI: kc-coverage and kc --no-llm ────────────────────
def test_cli_kc_coverage_and_bundle(compiled, tmp_path):
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"), "kc-coverage",
         "--graph", str(compiled["graph"]), "--builds", str(compiled["builds"]),
         "--docs", str(tmp_path / "docs"), "--out", str(tmp_path / "run"), "--plain"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-600:]
    assert (tmp_path / "docs" / "kc_coverage.md").exists()
    payload = json.loads((tmp_path / "docs" / "kc_coverage.json").read_text())
    assert payload["missing"] == [] and payload["counts"]["rows"] == len(coverage.ROWS)
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"), "kc", "--table", TABLE,
         "--no-llm", "--graph", str(compiled["graph"]), "--builds", str(compiled["builds"]),
         "--out", str(tmp_path / "kc"), "--plain"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-600:]
    assert (tmp_path / "kc" / "dw__gms_transaction.kc.md").exists()
    assert "kc_facts" in result.stderr + result.stdout


# ── 19 · config: the empty list falls back honestly ──────────────
def test_config_fallback_and_unknown_tables(compiled, cfg):
    listing = list_tables(builds_root=compiled["builds"], graph_root=compiled["graph"],
                          config=cfg)
    assert listing["fallback"] and listing["scope"] == "all" and "scope: every table" in listing["note"]
    assert {r["physical"] for r in listing["rows"]} == set(compiled["build"].schema)
    row = listing["rows"][0]
    assert row["copy"] + row["review"] + row["never"] == row["facts"]
    cfg.tables = [TABLE, "zz.missing"]
    listing = list_tables(builds_root=compiled["builds"], graph_root=compiled["graph"],
                          config=cfg)
    assert not listing["fallback"] and listing["unknown"] == ["zz.missing"]
    assert [r["physical"] for r in listing["rows"]] == [TABLE]
    with pytest.raises(KeyError):
        build_bundle("zz.missing", use_llm=False, builds_root=compiled["builds"],
                     graph_root=compiled["graph"], config=cfg)


# ── 21 · the list is a cache per build, and each row says what it yields ──
def test_list_summaries_cached_and_section_readiness(compiled, cfg, monkeypatch):
    import sahs.kc.bundle as kc_bundle
    kc_bundle._SUMMARIES.clear()
    calls = {"n": 0}
    real = kc_bundle.assemble

    def counting(*args, **kwargs):
        calls["n"] += 1
        return real(*args, **kwargs)
    monkeypatch.setattr(kc_bundle, "assemble", counting)
    first = list_tables(builds_root=compiled["builds"], graph_root=compiled["graph"], config=cfg)
    assert calls["n"] == len(first["rows"])
    second = list_tables(builds_root=compiled["builds"], graph_root=compiled["graph"], config=cfg)
    assert calls["n"] == len(first["rows"])              # no re-assembly
    assert second["scope"] == "all" and "scope: every table" in second["note"]
    assert second["totals"]["facts"] == sum(r["facts"] for r in second["rows"])
    assert set(second["lobs"]) == {r["lob"] for r in second["rows"] if r["lob"]}
    row = next(r for r in second["rows"] if r["physical"] == TABLE)
    assert set(row["sections"]) == {"description", "overview", "columns", "glossary",
                                    "related_entries", "aspects", "dq", "queries",
                                    "contacts", "review"}
    for key, sec in row["sections"].items():
        assert sec["ready"] or sec["empty_reason"], key
    assert row["sections"]["columns"]["review"] > 0     # the no-witness columns
    assert row["columns"] == len(compiled["build"].schema[TABLE])


# ── 22 · the glossary merges across tables the way the catalog holds it ──
def test_glossary_across_tables_merges(compiled, cfg):
    merged = glossary_across(builds_root=compiled["builds"], graph_root=compiled["graph"],
                             config=cfg)
    assert merged["tables"] == sorted(compiled["build"].schema)
    keys = [(t["category"], t["term"]) for t in merged["terms"]]
    assert len(keys) == len(set(keys))                   # one row per term
    gmns = next(c for c in merged["categories"] if c["name"] == "GMNS")
    assert len(gmns["tables"]) >= 2                      # a shared LOB category, once
    per_table = {}
    for t in merged["tables"]:
        b = build_bundle(t, use_llm=False, builds_root=compiled["builds"],
                         graph_root=compiled["graph"], config=cfg)
        for term in (b.sections["glossary"].items[0]["terms"] if b.sections["glossary"].items else []):
            per_table.setdefault((term["category"], term["term"]), set()).add(t)
    for t in merged["terms"]:
        assert set(t["tables"]) == per_table[(t["category"], t["term"])]
        assert set(t["fact_ids"]) == set(t["tables"])
        for e in t["related_entries"]:
            assert e.split(".")[0] + "." + e.split(".")[1] in compiled["build"].schema
    assert sum(merged["by_category"].values()) == len(merged["terms"])
    for fmt, name in (("jsonl", "glossary_all.jsonl"), ("links", "entry_links_all.jsonl"),
                      ("sheet", "glossary_all.csv")):
        data, _m, filename = glossary_export(fmt, builds_root=compiled["builds"],
                                             graph_root=compiled["graph"], config=cfg)
        assert filename == name and data
    lines = [json.loads(l) for l in glossary_export(
        "jsonl", builds_root=compiled["builds"], graph_root=compiled["graph"],
        config=cfg)[0].decode().splitlines()]
    assert lines[0]["scope"].startswith("merged glossary")
    names = [l["entry"]["name"] for l in lines[1:]]
    assert len(names) == len(set(names))                 # no duplicate entries
    with pytest.raises(ValueError):
        glossary_export("xml", builds_root=compiled["builds"], graph_root=compiled["graph"],
                        config=cfg)


# ── 23 · one zip for everything ───────────────────────────────────
def test_export_all_zip(compiled, cfg):
    data, media, filename = export_all(builds_root=compiled["builds"],
                                       graph_root=compiled["graph"], config=cfg)
    assert media == "application/zip" and filename.startswith("kc_all_")
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = zf.namelist()
        folders = {n.split("/")[0] for n in names if "/" in n}
        assert folders == {t.replace(".", "__") for t in compiled["build"].schema}
        assert {"glossary_all.jsonl", "entry_links_all.jsonl", "glossary_all.csv",
                "manifest.json"} <= set(names)
        manifest = json.loads(zf.read("manifest.json"))
        assert set(manifest["tables"]) == set(compiled["build"].schema)
        for folder in folders:
            assert f"{folder}/entry_patch.json" in names and f"{folder}/bundle.md" in names


# ── 24 · the ledger knows which card each row lands in ───────────
def test_ledger_rows_carry_their_section(compiled, cfg):
    bundle = build_bundle(TABLE, use_llm=False, builds_root=compiled["builds"],
                          graph_root=compiled["graph"], config=cfg)
    for r in bundle.ledger:
        assert r["section"] in bundle.sections, r
    assert section_of("entry.description") == "description"
    assert section_of("entry.overview.joins") == "overview"
    assert section_of("glossary.related_entry") == "related_entries"
    assert section_of("aspect.join-paths.paths") == "aspects"
    assert section_of("suggestion.metric") == "review"


# ── 25 · a red gate hands you the rows to write ──────────────────
def test_coverage_stub_rows():
    text = coverage.stub_rows(["node:table.new_prop", "edge:joins_via.extra",
                               "report:run.reports.newloader"])
    assert '"node:table.new_prop"' in text and '"identity"' in text
    assert '"edge:joins_via.extra"' in text and '"joins"' in text
    assert 'EXCLUDED' in text and "newloader" in text
    assert text.count("_r(") == 3


# ── 20 · schema touches ──────────────────────────────────────────
def test_kc_witness_and_relation_registered():
    from sahs.graph.quads import RANKING_WITNESSES, RELATIONS, SOURCE_WITNESS, WITNESSES
    assert "kc" in WITNESSES and "kc" not in RANKING_WITNESSES
    assert SOURCE_WITNESS["kc_export"] == "kc"
    assert RELATIONS["kc_pushed"] == ({"table"}, {"run"})
