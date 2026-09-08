# Synthesis — mock v1 × evaluative study × clean-room vision

Three inputs, deliberately produced under different bias conditions:

1. **Mock v1** (`console-mock.html`) — our design, chat + trace rail.
2. **Evaluative study** (`ux-research-notes.md`) — expert review OF the
   mock (anchored on it by design; finds flaws, can't challenge the frame).
3. **Greenfield vision** (`greenfield-vision.md`) — clean-room study from
   the problem statement only; never saw our work; explicitly licensed
   to reject chat.

This document is the comparison neither agent could do. Convergence =
independently reached = validated without echo. Divergence = the
findings that survive the anchoring objection.

## Convergences — independently reached, treat as settled

| Decision | Mock v1 | Greenfield (blind) | Verdict |
|---|---|---|---|
| Narrate the wait with REAL tool events, never a spinner | trace rail ("latency is narrated") | "the wait" as defining moment #2: live timeline + honest count-up | **Settled.** Both studies also demand the in-flight grammar the mock omitted (eval S0-1). |
| Refusal as a designed institutional beat + compliant alternative | refusal card, "machine-checked, not model judgment" | moment #5, near-verbatim: "enforced below this assistant," alternative one click | **Settled.** Strongest three-way agreement in the corpus. |
| Authorization as a first-class ceremony (cost + named checks + ledger) | gate card | moment #4 "signing ceremony" | **Settled in kind**; greenfield hardens the mechanics (below). |
| Evidence chip on every claim; audience-split rendering | provenance chips + persona toggle | phrase-level evidence binding + three projections | **Settled in kind**; mechanism differs (below). |
| Seed first questions from verified content | composer chips from demo_questions.json | moment #1: completed example Briefs, "ask something you already know" | **Settled — and ours is pre-built**: the demo pack is gate-verified; pre-run 3 of them as the empty state. |

## Divergences — the real findings, with recommendations

### D1. System of record: transcript vs Brief  *(the big one)*
Greenfield's core claim: in a bank, the scroll is the wrong system of
record — you cannot audit, share, version, or stamp a scroll position.
The unit of work should be a **Brief**: a durable, claim-structured
answer artifact; chat survives only as the steering thread.
**Recommendation — synthesis, not surrender:** keep the chat-steered
surface for the PoC (build cost, demo timeline) but give the answer
card the Brief's DNA now: stable identity (`brief_id`), exportable with
chips + provenance appendix + ledger stamp ("no naked numbers leave the
building"), versioned on revision. The README's graduation path
(chat → canvas) already points here; the Brief becomes the canvas's
native object later. **User decision required.**

### D2. Never stream the answer — stream the work
Greenfield's most contrarian call, and it's right for this domain:
streamed prose is fluency, fluency reads as confidence, and confidence
before evidence is exactly the failure mode a credit-risk tool must not
have. It also *dissolves* eval finding S0-2 (Text→Answer double-render)
— no prose streaming, no reconciliation problem — and is cheaper to
build. **Recommendation: adopt outright.** Contract: `thinking` +
`tool_call`/`tool_result` stream (the work); `text` deltas are no longer
emitted for answers; the `answer` event lands whole/atomic.

### D3. Trust vocabulary: graph tiers + scores vs four plain words
Greenfield: four fixed words — **Corroborated / Sourced / Reported /
Inferred** — as word + fill-glyph chips, never color-alone, never
percentages (numeric confidence miscalibrates lay readers). One nuance
they lacked context for: our scores are *evidence-structural* (source
fusion), not LLM self-confidence, so the miscalibration critique only
half-applies. **Recommendation: adopt as a display vocabulary.**
Canonical graph tiers stay in the payload; the UI maps
human_asserted/grounded→Corroborated, strong-single-source→Sourced,
usage/weak-single→Reported, llm_generated→Inferred. The number moves
one level down, into the witness panel (chip-click). Fill-glyph +
word also clears eval S1-5 (color-alone, contrast) in one move.

### D4. The gate's mechanics
Greenfield hardens what we sketched: approval via a **deliberate
keyboard chord (never Enter)**, diff-based re-approval when SQL
changes, scoped session consent — anti-habituation borrowed from
aviation. **Recommendation: adopt chord + diff-re-approval in v2;
defer session consent to Phase 3** (needs the resumable loop anyway).

### D5. Persona toggle vs projections
Same insight, better frame: not "hide panes for execs" but **three
projections of one object** — thread (analyst), Brief top sheet
(exec), Ledger (compliance). **Recommendation: adopt the framing** —
the toggle switches projection; exports inherit it. Ledger projection
is Phase 3+ (needs gate/ledger accumulation).

### D6. The repair moment  *(we had nothing)*
Challenge any claim → agent proposes the cheapest verification →
corrections versioned and promoted, with attribution, into curated
knowledge. Our substrate already has the receiving end (steward
review_queue, human_asserted tier). **Recommendation: graduation item;
add "Challenge" affordance stub to v2 so the slot exists.**

### D7. Release gate = calibration metrics
Both studies converge on measurable trust calibration (planted-error
detection ≥75% on Inferred, ≥90% acceptance on Corroborated; chip-only
reliability ranking τ ≥ 0.8). **Recommendation: adopt as the v1
acceptance bar in the validation plan.**

## What this means for mock v2 (pending user approval)

1. Work-streams, answer lands whole (D2) — also closes eval S0-1/S0-2
   via the in-flight grammar (active verb row, count-up timer, caret on
   the *work log* not the answer).
2. Answer card becomes a mini-Brief: id, export, ledger stamp (D1-lite).
3. Chips: word + fill-glyph vocabulary, click-through witness panel,
   score demoted to panel; one deliberately **Inferred** answer added to
   the golden pack so viewers learn by contrast (D3 + eval S1-4).
4. Gate: keyboard chord + named policy ids + diff-re-approval (D4).
5. Projections framing for the toggle (D5); contrast fixes (eval S1-5).
6. Empty state: three pre-run example Briefs from the verified demo
   pack (settled convergence).
7. Challenge affordance stub (D6).

Open with the user: **D1 proper** (commit to Brief-workspace as target
architecture now vs chat-steered v2 with Brief DNA and graduate) — and
the four v1 decisions still pending (brand, default lens, gate mode,
live suggestions), two of which greenfield now answers (gate: manual +
ceremony; suggestions: yes, as pre-run example Briefs).
