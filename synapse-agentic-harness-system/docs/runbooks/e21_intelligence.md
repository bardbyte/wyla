# E21 — compiling the intelligence into the harness

The build order: E20 (what the coding harnesses teach) and the
answering ladder land on the Stage A-C loop as Steps 0-8, one PR per
step, each gated on the E19 capability suite and reporting its delta
on the same line.

## Step 0a — the real-Vertex run (LAPTOP; blocks everything)

Nothing past Step 0 starts until a real model has answered once. On
the machine with the Vertex contract in the silo `.env`:

```bash
cd synapse-agentic-harness-system
python scripts/vertex_check.py            # the contract, preflighted

python scripts/ask_demo.py "acquirer net spend by day" \
       --pick 1 --then "same for Canada" \
       --report graph/runs/vertex_r1

git add graph/runs/vertex_r1 && git commit -m "E21 0a: vertex run report"
```

`--report` wraps the model with a recorder OUTSIDE the product (the
same seam the tests use) and writes `report.json` + `report.md`:
per-step timings, tokens in/out/thought per call, time-to-first-token
for the streamed composition, and strict-JSON drift (a `json()` call
whose text did not parse — the judge fails closed on it, and the
report counts it). Paste the report back; whatever broke gets fixed
before Step 1.

If Vertex is not configured the conversation still runs its
deterministic half, the turn ends in an honest `model_unavailable`
card, and the report records zero model calls — that report is the
diagnosis, not a failure of the tooling.

## Step 0b — the E19 baseline

**Reconstruction, loudly labelled:** the E19 instruction is cited by
E20/E21 but its text never landed in this repo. `sahs/evals/
capability.py` rebuilds the suite from those citations — tiers T1-T7
and T10 over the built loop, T8/T9 reported absent, the two-number
line, and the three-configuration ablation (resolver margin via a
copied manifest, never a ranker edit). If the real E19 differs,
reconcile the module; a test pins the label until then.

```bash
python scripts/e19_baseline.py                 # scripted transport
python scripts/e19_baseline.py --real          # Vertex (laptop)
```

Outputs `docs/evals/e19_baseline{,_vertex}.{md,json}`. The scripted
fixture baseline is committed: 20/20 across the built tiers,
answered 100 / wrong 0 / false-abstain 0 / false-answer 0, flat
across pinned/looser/strict (no fixture task sits near the margin
boundary; the full graph is expected to differentiate).

## The steps (each lands only after Step 0)

| step | component | gate |
|---|---|---|
| 1 | `sahs/ask/typecheck.py` — the compiler in the loop | wrong-answer count down on T2/T3/T7, answer rate flat |
| 2 | `SYNAPSE.md` digest + session skills | skill changes ranking/rules visibly; certified numbers identical |
| 3 | the ladder as policy + `subgraph_used` | zero over-climbs on T1 tasks |
| 4 | explore phase + Table Scout + compaction | table recall/precision on wide questions; 20-turn coherence |
| 5 | L3 composition + test-driven checks | every L3 answer shows ≥3 checks incl. reconciliation |
| 6 | L4 exploratory lane | F7 demo on the real snapshot |
| 7 | search constellation | driven by `subgraph_used`, non-blocking |
| 8 | flywheel + sessions + hybrid recall | L0 lift on paraphrase tasks |

Standing pins: one PR per step with the E19 delta line in its body ·
no worker beyond the scout · no live execution in L4 · no write to
truth outside the clerk · budgets/breakers in code, the stop button
and the breaker sharing one abort path.
