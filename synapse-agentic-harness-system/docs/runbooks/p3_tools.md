# P3 runbook — serving tools + the resolver floor (laptop)

Measure the deterministic resolver on the real build and **publish the
number with its triage** (E5). Then smoke the tool surface the agent
will stand on. The floor is what the system does with zero LLM — every
point of it is deterministic capability the model never has to spend
tokens reproducing.

## Prereqs

- P2 complete: `builds/CURRENT` names a real build.
- Curated tasks present (`tests/tasks/curated/curated.jsonl`) plus the
  gold set from P0.

## 1. The floor run

```bash
cd synapse-agentic-harness-system

python scripts/run_evals.py \
  --tasks graph/runs/p0_census/tasks/gold.jsonl \
  --tasks tests/tasks/curated/curated.jsonl \
  --sut resolver:builds \
  --out graph/runs/p3_floor --json
```

- The excluded lines print on every run — copy
  `excluded (coverage=external): N of M` into the commit message.
- The deterministic resolver BINDS; it never generates SQL. nl2sql
  gold tasks are excluded loudly (`excluded (kind outside sut
  capability): N`) — they are the ground for generation-capable SUTs
  (the agent with tools), not the binding floor. The floor number is
  pass@1 over resolve_bind / disambiguate / abstain.
- **0.90 pass@1 is a tripwire, not a gate**: below it, triage is
  mandatory before exit; above it, spot-triage 10 failures anyway.
- **The abstention floor is hard**: `disambiguate` and `abstain` must
  read 1.000 — a confident wrong bind on a should-not-answer task is a
  correctness bug, full stop.

## 2. Triage every failure (human step — E5)

Failures land pre-seeded in
`graph/runs/p3_floor/triage/floor_failures.jsonl`. For each row set
`"category"` to exactly one of:

- `resolver_bug` — the resolver is wrong; **open items of this kind
  block P3 exit** (fix, recompile if needed, re-run);
- `gold_defect` — the task/gold is wrong; fix the task, note it;
- `coverage_gap` — the graph genuinely lacks the fact; file it against
  the corpus, the task stays failing honestly.

Set `"triage"` to `done` when categorized. Commit the file — the floor
number without its triage table is not a published number.

## 3. Sandbox live-path check (dry-run only — assumption A1)

```bash
# snapshot mode: always available, zero rows, discloses policy_unknown
python - <<'PY'
from pathlib import Path
from sahs.tools.api import Build
from sahs.tools.sandbox import execute_sandboxed
build = Build.open(Path("builds"))
out = execute_sandboxed(build, "SELECT 1 FROM <a real table> WHERE part_dt = '<recent>'")
print(out["status"], out["meta"])
PY
```

Live mode stays default-deny (`SAHS_ALLOW_LIVE=1` to enable, and the
cost gate + row cap + ledger still apply). Tables whose row-access
policy could not be read at extraction are **denied live regardless of
the flag** — resolve the ticket, recompile, retry. The decision ledger
appends to `builds/sandbox_ledger.jsonl`; check it after any session.

## 4. Serve (optional smoke)

```bash
pip install -e ".[mcp]"
python scripts/serve_mcp.py --builds builds
```

Eight tools under one envelope
(`{status, data, error, meta{tool, build_version, latency_ms}}`),
resolved through `builds/CURRENT` (E4).

## Commit

```bash
git add graph/runs/p3_floor
git commit -m "meridian: P3 floor — pass@1 <n> (excluded <N> of <M>), triage table committed"
```

Exit criteria met when: floor + triage committed with zero open
`resolver_bug` items; abstention floor 1.000; sandbox denies the
policy-unknown table in live mode; tools answer through CURRENT.
