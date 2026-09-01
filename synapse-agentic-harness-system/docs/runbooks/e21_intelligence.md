# E21 — compiling the intelligence into the harness

The build order: E20 (what the coding harnesses teach) and the
answering ladder land on the Stage A-C loop as Steps 0-8, one PR per
step, each gated on the E19 capability suite and reporting its delta
on the same line.

> **Superseded in sequencing by the Agent Loop v1 spec**
> (`docs/specs/agent_loop_v1.md`). Every turn becomes the agent loop;
> determinism relocates into the tools; the spec's §9 build order
> REPLACES Step 4's "explore.py" and re-sequences Steps 1-8 (the
> mapping is under the step table below). Step 0 stands unchanged:
> nothing ships against latency or navigation targets until a real
> model has answered once, because the spec's own pins (p50 <3s
> single-hop, <15s navigation) are only measurable against Vertex.

## Step 0a — the real-Vertex run (LAPTOP; blocks everything)

Nothing past Step 0 starts until a real model has answered once. On
the machine with the Vertex contract in the silo `.env`:

```bash
cd synapse-agentic-harness-system
python scripts/vertex_check.py            # the contract, preflighted

python scripts/ask_demo.py "acquirer net spend by day" \
       --pick 1 --then "same for Canada" \
       --report graph/runs/vertex_r1

cat graph/runs/vertex_r1/report.md
```

The laptop cannot push, so the report travels by PASTE: run the
`cat`, copy the output into the session. Same for the resolver trace
when a bind looks wrong:
`grep resolve_result graph/runs/ask/events/<session>.jsonl | head -1`.

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

### Where each step lives under the Agent Loop (spec §9)

| E21 step | its home in the loop build order |
|---|---|
| 1 typecheck | INSIDE `plan_set` — runs on every call, teaching errors as tool results (§9.1, **landed**: `sahs/loop/tools.py`) |
| 2 digest + skills | the system prompt's world digest + skills slot (§9.3) |
| 3 ladder + subgraph_used | stop conditions in the prompt; the sub-graph records itself on the loop state (§9.1 records, §9.2 discloses) |
| 4 explore + scout + compaction | REPLACED: the loop IS the explore phase (§9.2); scout is §9.5 |
| 5 L3 checks | the plan's `checks` slot, written via `plan_set`, read by the verifier |
| 6 exploratory lane | the loop with `run_sql` snapshot on (§9.5) |
| 7 constellation | unchanged, non-blocking, still driven by subgraph_used |
| 8 flywheel | after §9.4's navigation tasks and trajectory ritual |

Standing pins: one PR per step with the E19 delta line in its body ·
no worker beyond the scout · no live execution in L4 · no write to
truth outside the clerk · budgets/breakers in code, the stop button
and the breaker sharing one abort path.
