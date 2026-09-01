# The trajectory ritual (Agent Loop v1 §8)

Claude Code improves its prompt and its tools from transcripts, and
so does this harness. Twenty trajectories read weekly; every bad turn
becomes a system-prompt example or a tool-description fix. This page
is the how.

## Where trajectories live

Every navigated turn writes its whole trajectory into the session's
events file (`graph/runs/ask/events/<session>.jsonl`):

- `loop_started` — why navigation engaged, the prompt version, the
  skills loaded
- `loop_prompt` — EXACTLY what the model saw (`kind: system` once,
  `kind: step` per look)
- `loop_step` — each look: tool, args, the ≤3-line summary kept in
  context, the model's `think`
- `loop_artifact` — the full tool result behind each summary
- `loop_done` — outcome (answered / ask / partial), step count, and
  `subgraph_used` (every card read, every binding committed)

The same record renders in the product: any navigated turn has a
**"what the model saw"** button under the answer. The panel is built
purely from these events, so reading it in the browser and reading
the file are the same read.

## The weekly read (20 trajectories)

Pull the most recent sessions, prefer the ones that ended `partial`
or `clarify`, and for each ask five questions:

1. **Did it look before it bound?** A `plan_set` with no prior look
   is a guess that happened to typecheck.
2. **Did it check literals?** A filter without `sample_values` ahead
   of it is the wrong-country bug waiting.
3. **Was the ask necessary?** If a card the loop already read
   settles the question it asked, the doctrine or a trace needs to
   say so louder.
4. **Did an error teach?** When a tool refused, did the next step
   use the hint? If not, the hint is written for us, not the model —
   rewrite the tool's error.
5. **Where did the budget go?** Steps spent re-reading or
   re-searching the same thing mean the summary the context kept was
   too thin — fix `compact_result`, not the budget.

## Where fixes go

- A wrong turn that better WORDS would prevent → edit
  `sahs/loop/prompt.py` (doctrine, stop conditions, or swap a trace
  for the real transcript, trimmed). **Bump `PROMPT_VERSION`** — it
  travels on `loop_started`, so next week's read can tell whether
  the new words changed the behavior.
- A wrong turn that a better TOOL RESULT would prevent → edit the
  tool's error/hint in `sahs/loop/tools.py` (the description itself
  is spec-pinned; the teaching text is yours to sharpen).
- A wrong turn no words prevent → it becomes a navigation task in
  `tests/tasks/navigation/navigation.jsonl`, so the fix has a number
  to move.

## The numbers around the read

`python scripts/nav_eval.py --real` runs the 30-task set on the
laptop and writes `docs/evals/navigation_baseline_vertex.md` — found%
/ wrong-when-found% / precision violations, plus the soft hygiene
line (steps per task, asks, literal-check rate, read-before-use rate,
budget stops). The laptop cannot push: **paste the .md back into the
session.** Hygiene never gates a PR; outcome numbers move the
`SYNAPSE_NAVIGATE` default when they hold the bar.
