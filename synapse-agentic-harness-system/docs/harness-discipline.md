# How this harness gets built

Ten principles, applied from the public "building effective agents"
school of thought. They are not style preferences: each one is a rule
that has already changed a decision in this repo, and every harness PR
is expected to hold to them or say why not.

## 1 · Don't build an agent where a workflow will do

Most of Synapse is a workflow. classify, apply, resolve, typecheck,
contract, verify are code paths with known shapes. The model is
invited only where the path is genuinely open: composing, exploring,
explaining, chatting. Every time someone proposes "let the agent
decide," the first question is whether code already knows the answer.
Usually it does.

*In this repo:* a session's first turn, every chip answer, and every
greeting classify with **no model call**. The resolver is a scored
ranking, not a prompt.

## 2 · Keep it simple: environment, tools, system prompt

Iterate on those three and nothing else. The build is the environment;
the tools are the agent-computer interface; the prompt is versioned
like code (`b1.1 → b1.2` is a diff you can read). Complexity is added
only when an eval delta demands it, and removed when a model upgrade
makes it dead weight. The assumption register is the removal schedule.

## 3 · Think like the agent: literally read its context window

The single most useful ritual. At each step of a real turn, dump
exactly what the model saw and ask: *could I answer correctly with
only this?* Most harness bugs are context bugs, and you cannot fix
what you have not read.

*Shipping form:* an admin-only "What the model saw" panel per turn,
which doubles as the transparency surface that earns trust.

## 4 · Tools get the care of a user interface

Every tool description passes the new-hire test: could a smart
colleague use it correctly from the description alone? Error messages
teach the right call. Arguments are poka-yoked (qualified ids, enums
drawn from compiled domains). Time spent on tool ergonomics beats time
spent on prompt wording, consistently.

## 5 · Start with the API, never a framework

The loop is ours, in `sahs`, in-process. Every abstraction that hides
a prompt or a response is a debugging tax paid forever.

## 6 · Evals are the loss function; transcripts are the gradient

No component ships without its E19 delta. Every week someone reads
twenty transcripts and turns what they find into tasks. **A score
nobody has read the transcripts for is not a score.**

*In this repo:* the first real-graph run produced no eval delta at all
and was still the most valuable run to date, because reading it found
three transport bugs no scripted suite could see.

## 7 · Budget everything in code

Tokens, turns, tool calls, cost, enforced by the loop, with the stop
button and the breaker on one abort path. The model is never asked to
be frugal; the harness makes frugality unavoidable.

## 8 · Make the environment truthful before making the model clever

Compiled build, real values, honest empty states, fail-closed
unknowns. Intelligence on a lying environment is confident nonsense;
intelligence on a truthful one compounds. This is why an answer whose
verdict is `fail` is still shown, with the failure showing.

## 9 · Ship the smallest honest thing, then measure, then add

Stage A before Stage B. A curl-able loop before a chat. A baseline
before a type checker. The order is not caution, it is how you learn
which change did what.

## 10 · Delete on upgrade

Every model release: re-run the ablation, strip what no longer moves
the suite, log the deletion. A harness that only grows is a harness
nobody understands.

---

## The PR checklist

Every harness PR body carries:

- [ ] **The E19 delta line** (answered% / wrong-when-answered%, against
      `docs/evals/e19_baseline.md`), or an explicit statement that the
      change is not measurable by the suite and why.
- [ ] **Which principle above justified the component**, and what would
      have to be true to delete it again.
- [ ] **A transcript** read, not just a score: what the model actually
      saw at the step this PR changes.
