"""std_tech_keys.py — the feed-vs-loader key census.

Holds two things: the fixture (the contract) has ZERO unconsumed keys,
and a key the real feed might add that the loader does not name is
reported, counted and sampled — never silently absent."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.loaders.sources.vocab import (            # noqa: E402
    STD_TECH_CONSUMED_KEYS,
    load_std_tech_metadata,
)
from scripts.std_tech_keys import Census, render    # noqa: E402

FX = SILO / "tests" / "fixtures" / "sources" / "std_tech_metadata"


def test_fixture_is_fully_consumed_and_layers_are_walked():
    c = Census()
    c.load(FX)
    rep = c.report()
    assert rep["unconsumed"] == [], rep["unconsumed"]
    # every documented layer was seen, and every consumed key the
    # loader names is present in the fixture — the fixture IS the
    # contract, so a pick with no fixture key would be untested
    seen = {(r["layer"], r["key"]) for r in rep["rows"]}
    # dataset / appl_id ride on the ENVELOPE in the real shape and on
    # the entry only in the flat shape; the loader accepts both
    envelope_carried = {("entry", "dataset"), ("entry", "appl_id")}
    for layer, keys in STD_TECH_CONSUMED_KEYS.items():
        missing = {k for k in keys if (layer, k) not in seen
                   and (layer, k) not in envelope_carried}
        assert not missing, f"{layer}: loader names {missing}, fixture lacks"
    statuses = {(r["layer"], r["key"]): r["status"] for r in rep["rows"]}
    assert statuses[("envelope", "page_info")] == "deferred"
    assert statuses[("ownership", "business_owner")] == "edge+prop"
    assert statuses[("ownership", "vp")] == "edge+prop"
    assert statuses[("ownership", "car_id")] == "prop only"
    text = render(rep)
    assert "every key the feed sends is consumed" in text


def test_unknown_keys_are_reported_not_dropped(tmp_path: Path):
    src = json.loads((FX / "gms_transaction.json").read_text())
    item = src["tech_metadata_list"][0]
    item["retention_class"] = "7Y"                        # entry layer
    item["datasetAttribute"]["refresh_frequency"] = "DAILY"
    item["datasetAttribute"]["ownership"]["data_steward"] = "stew_a@corp"
    item["pde"][0]["pdeAttribute"]["sensitivity_level"] = "HIGH"
    item["pde"][0]["businessMetadata"][0]["lastReviewed"] = "2026-08-01"
    (tmp_path / "gms.json").write_text(json.dumps(src))

    c = Census()
    c.load(tmp_path)
    rep = c.report()
    assert rep["unconsumed"] == [
        "entry.retention_class", "datasetAttribute.refresh_frequency",
        "pdeAttribute.sensitivity_level", "businessMetadata[].lastReviewed"]
    rows = {(r["layer"], r["key"]): r for r in rep["rows"]}
    assert rows[("datasetAttribute", "refresh_frequency")]["samples"] == \
        ["DAILY"]
    assert rows[("pdeAttribute", "sensitivity_level")]["carriers"] == 1
    assert rows[("pdeAttribute", "sensitivity_level")]["layer_carriers"] \
        == sum(len(i.get("pde") or []) for i in src["tech_metadata_list"])
    # a role the heuristic does not know is visible as prop-only, and
    # the loader really does keep it whole (never lost, never an edge)
    assert rows[("ownership", "data_steward")]["status"] == "prop only"
    entries, _ = load_std_tech_metadata(tmp_path)
    assert entries[0].ownership["data_steward"] == "stew_a@corp"
    assert "! refresh_frequency" in render(rep)


def test_cli_strict_exits_two_on_unconsumed(tmp_path: Path):
    src = json.loads((FX / "gms_transaction.json").read_text())
    src["tech_metadata_list"][0]["datasetAttribute"]["sla_hours"] = 4
    (tmp_path / "gms.json").write_text(json.dumps(src))
    out = tmp_path / "census.json"
    r = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "std_tech_keys.py"),
         str(tmp_path), "--strict", "--json", str(out), "--quiet"],
        capture_output=True, text=True, cwd=SILO)
    assert r.returncode == 2, r.stderr[-400:]
    payload = json.loads(out.read_text())
    assert payload["schema"] == "meridian.std_tech_key_census/1"
    assert payload["unconsumed"] == ["datasetAttribute.sla_hours"]
    r = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "std_tech_keys.py"),
         str(FX), "--strict", "--quiet"],
        capture_output=True, text=True, cwd=SILO)
    assert r.returncode == 0, r.stderr[-400:]
