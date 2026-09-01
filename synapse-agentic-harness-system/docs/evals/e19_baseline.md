# E19 capability baseline (E21 Step 0b)

- build: `b_2cd603279061` · transport: **scripted**
- RECONSTRUCTION: the E19 instruction is cited by E20/E21 but its text never landed in this repo; this suite rebuilds it from those citations (see `sahs/evals/capability.py`). Reconcile against the real E19 if it differs.

## the two-number line, per configuration

| config | margin | answered% | wrong-when-answered% | false-abstain% | false-answer% |
|---|---|---|---|---|---|
| pinned | 0.15 (shipped) | 100.0 | 0.0 | 0.0 | 0.0 |
| looser | 0.05 | 100.0 | 0.0 | 0.0 | 0.0 |
| strict | 0.3 | 100.0 | 0.0 | 0.0 | 0.0 |

## tiers (pinned configuration)

| tier | capability | score |
|---|---|---|
| T1 | vocabulary (L0) | 4/4 |
| T2 | certified bind (L1) | 2/2 |
| T3 | clarification | 4/4 |
| T4 | single-slot mutation | 1/1 |
| T5 | contract, default-FAIL | 2/2 |
| T6 | join and grain preview | 3/3 |
| T7 | answer receipts | 1/1 |
| T8 | composition (L3) | absent — not built, not scored |
| T9 | exploratory (L4) | absent — not built, not scored |
| T10 | abstention and honesty | 3/3 |
| T11 | conversation quality (E22) | 9/9 |

**Ablation note:** the three configurations produce identical lines on this build — none of these tasks sits near the margin boundary at this fixture's scale. On the full graph (3,000+ mined classes) the margin knob is expected to differentiate; a flat line there would be a finding about the knob, not the suite.

Every later E21 step re-runs this suite and reports its delta against this file on the same line.
