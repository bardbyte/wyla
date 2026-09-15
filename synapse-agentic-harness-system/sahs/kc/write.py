"""The writer: model calls that turn facts into prose, the blind gate
that decides whether that prose may be copied, the cache that keeps a
second load at zero calls, and the events the tab streams.

The model is the proven Vertex client (``sahs.enrich.client``): Gemini
3.1 Pro on Vertex by default (``VERTEX_MODEL``), JSON mode, temperature
0.2, retries and MAX_TOKENS self-heal inside the client. A test injects
any object with a ``generate(prompt, *, system, temperature,
max_output_tokens) -> str`` method and a ``usage`` dict."""

from __future__ import annotations

import datetime as _dt
import json
import re
from pathlib import Path
from typing import Any, Callable

from sahs.ask.budget import Budget
from sahs.ask.events import EventBus
from sahs.kc import prompts
from sahs.kc.assemble import COPY, FactSet
from sahs.kc.config import KcConfig
from sahs.kc.render import OVERVIEW_SECTIONS
from sahs.kc.verify import verify_output

KC_EVENTS: tuple[str, ...] = ("kc_started", "kc_section", "kc_verified",
                              "kc_done", "budget_tick", "error")
TEMPERATURE = 0.2
MAX_OUTPUT_TOKENS = 4096


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def bundle_dir(graph_root: Path, cfg: KcConfig, table: str, build_id: str) -> Path:
    return Path(graph_root) / cfg.push_record_dir / table.replace(".", "__") / build_id


def cache_path(graph_root: Path, cfg: KcConfig, table: str, build_id: str) -> Path:
    return bundle_dir(graph_root, cfg, table, build_id) / f"llm_{cfg.prompt_version}.json"


def gate_path(graph_root: Path, cfg: KcConfig, build_id: str) -> Path:
    return Path(graph_root) / cfg.push_record_dir / "_gate" / build_id / f"{cfg.prompt_version}.json"


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=1, ensure_ascii=False, sort_keys=True) + "\n",
                    encoding="utf-8")


def parse_json(text: str) -> dict[str, Any] | None:
    """Strict: one JSON object, or None. A fenced block is unwrapped
    once; anything else malformed is counted, never repaired."""
    body = (text or "").strip()
    if body.startswith("```"):
        body = re.sub(r"^```(?:json)?\s*|\s*```$", "", body)
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def default_client(log: Callable[[str], None] | None = None):
    from sahs.enrich.client import VertexClient
    from sahs.util.auth import VertexConnection
    return VertexClient(VertexConnection.from_env(), log=log)


class Writer:
    """One table, one turn: three sections, each one call, budgeted."""

    def __init__(self, client: Any, cfg: KcConfig, bus: EventBus | None = None,
                 budget: Budget | None = None,
                 log: Callable[[str], None] | None = None) -> None:
        self.client = client
        self.cfg = cfg
        self.bus = bus
        self.budget = budget or Budget(turn_tokens=cfg.turn_tokens,
                                       turn_calls=cfg.turn_calls,
                                       session_tokens=cfg.turn_tokens,
                                       session_calls=cfg.turn_calls)
        self.log = log or (lambda _m: None)
        self.invalid_json = 0
        self.calls = 0
        self.stopped = ""

    def _emit(self, ev: str, **fields: Any) -> None:
        if self.bus is not None:
            self.bus.emit(ev, **fields)

    def _call(self, prompt: str) -> dict[str, Any] | None:
        tripped = self.budget.exceeded()
        if tripped:
            self.stopped = tripped
            return None
        before = dict(getattr(self.client, "usage", {}) or {})
        text = self.client.generate(prompt, system=prompts.SYSTEM,
                                    temperature=TEMPERATURE,
                                    max_output_tokens=MAX_OUTPUT_TOKENS)
        after = dict(getattr(self.client, "usage", {}) or {})
        self.calls += 1
        self.budget.charge(
            tokens_in=int(after.get("prompt_tokens", 0)) - int(before.get("prompt_tokens", 0)),
            tokens_out=int(after.get("output_tokens", 0)) - int(before.get("output_tokens", 0)))
        self._emit("budget_tick", **self.budget.tick())
        parsed = parse_json(text)
        if parsed is None:
            self.invalid_json += 1
            self.log("    [kc] malformed JSON from the model: section left empty")
        return parsed

    def write(self, fs: FactSet) -> dict[str, Any]:
        """→ the verified writer output plus its accounting."""
        self.budget.start_turn()
        self._emit("kc_started", table=fs.table, build=fs.build_id,
                   prompt_version=self.cfg.prompt_version, facts=len(fs.facts))
        raw: dict[str, Any] = {"overview_sections": {}, "columns": [], "glossary": []}
        copyable = [f for f in fs.facts if f.include == COPY]
        overview_facts = [f for f in copyable if f.kc_target.startswith("entry.")]
        column_facts = [f for f in copyable if f.kc_target.startswith("column.")]
        glossary_facts = [f for f in copyable if f.kc_target.startswith("glossary.")]
        plan = (("overview", prompts.overview_prompt(fs.table, overview_facts,
                                                     list(OVERVIEW_SECTIONS)), overview_facts),
                ("columns", prompts.columns_prompt(fs.table, column_facts), column_facts),
                ("glossary", prompts.glossary_prompt(fs.table, glossary_facts), glossary_facts))
        for section, prompt, facts in plan:
            if not facts:
                self._emit("kc_section", section=section, status="skipped",
                           reason="no copyable facts for this section")
                continue
            parsed = self._call(prompt)
            if self.stopped:
                self._emit("kc_section", section=section, status="stopped",
                           reason=self.stopped)
                break
            if parsed is None:
                self._emit("kc_section", section=section, status="invalid_json")
                continue
            if section == "overview":
                raw["description"] = parsed.get("description", "")
                raw["description_fact_ids"] = parsed.get("description_fact_ids", [])
                raw["overview_sections"] = parsed.get("overview_sections") or {}
            else:
                raw[section] = parsed.get(section) or []
            raw["confidence"] = parsed.get("confidence")
            raw["caveat"] = parsed.get("caveat", "")
            self._emit("kc_section", section=section, status="written")
        verified = verify_output(raw, fs)
        self._emit("kc_verified", dropped=len(verified["dropped"]),
                   kept_sections=sorted(k for k in verified["overview_sections"]),
                   columns=len(verified["columns"]), glossary=len(verified["glossary"]))
        usage = dict(getattr(self.client, "usage", {}) or {})
        record = {
            **verified,
            "prompt_version": self.cfg.prompt_version,
            "model": getattr(getattr(self.client, "connection", None), "model", ""),
            "table": fs.table, "build_id": fs.build_id,
            "facts_digest": fs.digest(), "generated_at": now_iso(),
            "calls": self.calls, "invalid_json": self.invalid_json,
            "stopped": self.stopped, "usage": usage,
            "budget": self.budget.tick(),
        }
        self._emit("kc_done", calls=self.calls, invalid_json=self.invalid_json,
                   stopped=self.stopped, cost_usd=self.budget.cost())
        return record


# ── the blind gate ───────────────────────────────────────────────

def _tokens(text: str) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(t) > 2}


def token_f1(truth: str, predicted: str) -> float:
    a, b = _tokens(truth), _tokens(predicted)
    if not a or not b:
        return 0.0
    overlap = len(a & b)
    if overlap == 0:
        return 0.0
    precision, recall = overlap / len(b), overlap / len(a)
    return 2 * precision * recall / (precision + recall)


def _ordered_tokens(text: str) -> list[str]:
    return [t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(t) > 2]


def leakage(withheld: str, shown: str, n: int = 3) -> list[str]:
    """Phrases of the withheld text (runs of ``n`` tokens) that appear
    verbatim in what the model was shown: a recovery with leakage
    measures copying, not understanding. Single shared words (a LOB
    name, a column's noun) are not leaks; a copied phrase is."""
    words = _ordered_tokens(withheld)
    if len(words) < n:
        grams = [" ".join(words)] if words else []
    else:
        grams = [" ".join(words[i:i + n]) for i in range(len(words) - n + 1)]
    haystack = " ".join(_ordered_tokens(shown))
    return sorted({g for g in grams if g and g in haystack})


def gate_tier(rate: float, cfg: KcConfig) -> str:
    if rate < cfg.gate_halt_below:
        return "halt"
    if rate < cfg.gate_review_below:
        return "review"
    return "copy"


def run_gate(fs: FactSet, client: Any, cfg: KcConfig,
             log: Callable[[str], None] | None = None) -> dict[str, Any]:
    """Withhold every human description, generate from the remaining
    facts, score token-set F1 and grep for leakage. One model call."""
    log = log or (lambda _m: None)
    truth: dict[str, str] = {}
    shown = []
    for f in fs.facts:
        if f.include != COPY:
            continue
        if f.kc_target == "entry.description":
            truth.setdefault("__table__", f.text)
            continue
        if f.kc_target == "column.description" and isinstance(f.data, dict) \
                and f.data.get("description") and not f.data.get("supplementary"):
            truth.setdefault(f.data["column"], f.data["description"])
            continue
        if f.kc_target.startswith("entry.overview.purpose") \
                or f.kc_target.startswith("entry.overview.columns_that_matter") \
                or f.kc_target == "aspect.meridian-provenance.served_purpose":
            continue
        # a business term mapped onto a column defines that column in
        # other words: withheld too, or the exam grades copying
        if f.kc_target == "glossary.term" and isinstance(f.data, dict) \
                and f.data.get("category") == "Business terms":
            continue
        shown.append(f)
    targets = [c for c in truth if c != "__table__"]
    result: dict[str, Any] = {"prompt_version": cfg.prompt_version,
                              "build_id": fs.build_id, "table": fs.table,
                              "n": len(truth), "recovered": 0, "rate": 0.0,
                              "tier": "halt", "leaky_contexts": 0,
                              "scores": {}, "ran_at": now_iso()}
    if not truth:
        result["reason"] = "no human description to withhold: the gate cannot measure recovery"
        result["tier"] = "review"
        return result
    text = client.generate(prompts.gate_prompt(fs.table, shown, targets),
                           system=prompts.SYSTEM, temperature=TEMPERATURE,
                           max_output_tokens=MAX_OUTPUT_TOKENS)
    parsed = parse_json(text) or {}
    predicted = {"__table__": str(parsed.get("description") or "")}
    for col in parsed.get("columns") or []:
        if isinstance(col, dict) and col.get("name"):
            predicted[col["name"]] = str(col.get("text") or "")
    shown_text = prompts.fact_lines(shown)
    recovered = 0
    leaky = 0
    for key, answer in truth.items():
        score = token_f1(answer, predicted.get(key, ""))
        leaks = leakage(answer, shown_text)
        if leaks:
            leaky += 1
        result["scores"][key] = {"f1": round(score, 3), "leaks": leaks}
        if score >= 0.5:
            recovered += 1
    result.update(recovered=recovered, rate=round(recovered / len(truth), 3),
                  leaky_contexts=leaky)
    result["tier"] = gate_tier(result["rate"], cfg)
    line = (f"kc gate: {recovered}/{len(truth)} recovered "
            f"({result['rate']:.0%}) → {result['tier']}"
            + (f" · {leaky} leaky context(s)" if leaky else ""))
    result["line"] = line
    log(line)
    return result
