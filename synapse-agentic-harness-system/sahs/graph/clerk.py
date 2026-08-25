"""The governance clerk — append-with-validate, a human signature required.

    python -m sahs.graph.clerk --graph graph/ \
        --subject metric:ab12cd34ef56 --set-status certified \
        --actor jane.steward --note "approved in review 2026-09-02"

The clerk is the ONLY human write path into L2 (E7): it appends a
``certified_as`` quad with ``source=clerk`` and a mandatory ``actor``,
refuses illegal lattice transitions BEFORE writing, and re-validates the
graph after. Status truth flows: clerk edge → next compile → resolver
behavior. Governance is a write path into the same truth the resolver
reads — not a side channel.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sahs.graph.ids import LEGAL_TRANSITIONS, STATUS_STATES  # noqa: E402
from sahs.graph.quads import GraphDir, Prov, Quad            # noqa: E402
from sahs.graph.validate import validate_graph               # noqa: E402


def set_status(graph_root: Path, subject: str, state: str, actor: str,
               note: str = "") -> tuple[bool, str]:
    if state not in STATUS_STATES:
        return False, (f"unknown state {state!r} — legal: "
                       + ", ".join(STATUS_STATES))
    graph = GraphDir(graph_root)
    history = graph.governance_history().get(subject, [])
    if history:
        current = history[-1]
        if state != current and state not in LEGAL_TRANSITIONS.get(
                current, set()):
            return False, (f"illegal transition {current} → {state} "
                           f"(legal: {sorted(LEGAL_TRANSITIONS.get(current, set()))})")
    graph.append_edge(Quad(
        s=subject, r="certified_as", o=f"status:{state}",
        props={"note": note} if note else {},
        prov=Prov(source="clerk",
                  run=f"clerk_{_dt.date.today().isoformat()}",
                  retrieved=_dt.datetime.now(
                      _dt.timezone.utc).isoformat(timespec="seconds"),
                  actor=actor)))
    report = validate_graph(graph_root)
    if not report.ok:
        return False, ("appended but graph now INVALID — fix before "
                       "compiling:\n" + "\n".join(report.errors[:5]))
    return True, f"{subject} → {state} (by {actor})"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="clerk")
    parser.add_argument("--graph", required=True)
    parser.add_argument("--subject", required=True)
    parser.add_argument("--set-status", required=True, dest="state")
    parser.add_argument("--actor", required=True)
    parser.add_argument("--note", default="")
    args = parser.parse_args(argv)
    ok, message = set_status(Path(args.graph), args.subject, args.state,
                             args.actor, args.note)
    print(message, file=sys.stderr)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
