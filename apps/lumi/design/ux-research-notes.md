# Synapse Console — UX Research Notes (mock v1 expert evaluation)

**Author:** Senior UX research, CEE product tradition (JetBrains / Skype / Grammarly / Wise school)
**Date:** 2026-07-07 · **Object under evaluation:** `apps/console/design/console-mock.html` (v1), against `apps/console/README.md`, `backend/events.py`, `backend/runner.py`, and the existing Synapse UX system (`synapse/docs/UX_INFORMATION_ARCHITECTURE.md`, `UX_VISUAL_SYSTEM.md`).
**Method:** state-of-the-art pattern survey (web, cited) → tradition-derived operating principles → heuristic evaluation (Nielsen 10 + tradition additions) → cognitive walkthrough of the three golden flows as both personas → prioritized recommendations + lean validation plan.

---

## 0. Executive summary (one page)

**Verdict: approve the direction, block the build on six items.** The bones are right — the answer contract, verbs-not-JSON at the protocol level, refusal as a designed beat, and an offline ScriptedRunner as living spec are all better than most 2025-26 commercial agent consoles. But the mock shows only the *finished* state of a product whose entire premise is *watching it work*, and the trust machinery (chips, gate, trace) is currently decorative where it must be operative. All numbers in the mock were audited: the chart geometry, MoM math, and row counts are honest — that engineering honesty must now be extended to the states the mock omits.

**Top findings (full table in §3):**

| # | Sev | Finding |
|---|-----|---------|
| F1 | **S0** | **No in-flight state exists anywhere.** "Latency is narrated" is principle #2, yet the mock renders only completed turns: no active tool row, no elapsed timer, no streaming-text caret, no thinking-in-progress state. Phase 1 (streaming) is unbuildable from this mock without improvisation — and the improvised part is the product's core claim. |
| F2 | **S0** | **The stream duplicates the answer with no reconciliation rule.** `runner.py` emits `Text` deltas ("New Accounts (SBS) owns…") *and* an `Answer` event carrying the same sentence. Undefined whether the streamed text morphs into the answer card or renders twice. This will ship as a visible double-answer or a jarring rewrite flash. |
| F3 | **S1** | **The gate has a dead end and no resolution event.** After "Hold," CSS removes the Approve button while the copy promises "approve any time to run it." And `events.py` has no `gate_resolved` event — approver identity, timestamp, and ledger id have no typed channel (ScriptedRunner hacks a `ToolResult` with the gate's id). |
| F4 | **S1** | **Provenance chips decorate; they do not calibrate.** Chips are inert (no drill-down to witnesses), citations carry no target refs in the protocol, and the demo shows only `grounded` — users can never learn the scale. The literature is blunt: naked confidence displays fail to prevent overreliance; explanation-on-demand is what calibrates. This also silently drops the semantic layer's own hero feature (per-fact "why do we believe this" panel, IA doc §5). |
| F5 | **S1** | **Accessibility fails at bank-procurement level.** Measured: `--ink-3` metadata text 3.41:1 (light) / 4.19:1 (dark) at 10–12.5px — WCAG AA fail on ~15 element classes including the governance line and audit-ledger reassurance; gate badge 3.68:1; no `aria-live`/`role=log` story for a streaming trace; keyboard focus is destroyed when the gate resolves (`display:none` on the focused button). |

**Three highest-leverage recommendations (full list §4):**

1. **Design the in-flight grammar before React Phase 1** — active-verb row with live elapsed timer, thinking shimmer, streamed-answer caret, "follow" pin — and specify Text→Answer replace-in-place reconciliation. This is the demo's wow *and* the labor-illusion payoff; it is currently 0% specified. (F1, F2 · patterns P5, P1)
2. **Amend the event contract now, while it's cheap:** `gate_resolved{decision, actor, ts, ledger_id}`, citation refs that resolve to graph nodes, a closed tier enum (incl. `human_asserted`/`deprecated`) with a defined fallback, wall-clock timestamps, and optional result-rows payload. Every S1 trust finding traces to a missing field, not a missing pixel. (F3, F4, F11, F16)
3. **Make every chip a door:** click → witness panel (tier, score, contributing witnesses w/ evidence counts, non-contributing, conflicts), tier → behavior mapping ("cite freely / verify first / don't ship"), and add one `inferred` answer to the golden pack so trust has a contrast class. Then verify with the chip-only ranking test (§4b, M1). (F4, F18 · P6, P8)

**What already wins (keep, do not regress):** answer-leads contract with citations surviving both lenses; refusal card citing the machine-checked rule + one-click compliant alternative ("machine-checked rule — not model judgment" is the best line in the product); verbs in the protocol, not the frontend; honest chart pixels (audited: 9.67px/1k, all 8 bars correct; 8.4% MoM = (11,792−10,878)/10,878 ✓); `prefers-reduced-motion` and `:focus-visible` present in a *mock*, which most teams skip.

---

## 1. State of the art — patterns from 2025-26 agentic interfaces

Named, reusable patterns with sources. Each is stated as: **Pattern → what the best implementations do → what it implies for Synapse.** Sources collected in §5.

**P1 · Collapsed-Summary Reasoning.** Claude renders extended thinking as a collapsible block above the answer, *summarized* rather than raw, streamable, labeled with duration ("Thought for Xs"); Claude Code hides thinking by default and cites latency + clarity ("show the conclusion, not the search") as the reasons [S1, S2, S3]. ChatGPT's reasoning models do the same summarized-disclosure move. DeepSeek-style raw CoT exposure is the outlier, not the norm. → Synapse's trace-rail `thinking` notes are directionally right, but should be **summarized/labeled model notes, not raw CoT** (see F20), and turns should carry a "thought for n s" line so totals reconcile (F16).

**P2 · Verb-First Step Trace with Jump-to-Artifact.** Devin's workspace shows Shell/Browser/Editor/Planner tabs that switch as the agent uses them; "Follow Devin" highlights each action and lets a human click from a step directly into the editor/terminal where it happened [S4, S5]. Palantir AIP's trace view gives timeline bars, parent-child hierarchy, and per-step token/latency detail for enterprise audit [S6]. → The rail's human verbs are correct and ahead of most; what's missing is the **bidirectional link** (answer chip ⇄ trace row ⇄ artifact) and any timeline affordance at scale (F4, F8).

**P3 · Plan-Before-Run (Co-Planning).** Gemini Deep Research presents an editable research plan and waits for approval before executing; editing the plan is "the biggest lever you have over output quality" [S7, S8]. Microsoft's Magentic-UI names this **co-planning** and pairs it with **co-tasking** (interrupt/redirect mid-run) [S9, S10]. → The SQL gate is a plan-approval moment for *one step*; analysts will immediately want the co-planning half: edit the SQL (or the plan) before approving, not just approve/hold (F21).

**P4 · Action Guards with Scoped Context.** Magentic-UI asks permission only for actions it deems consequential ("action guards") [S9, S10]; Cursor routes everything through a review layer of inspectable diffs; Windsurf's Cascade emphasizes diff-staging and per-step approval [S11, S12]. Enterprise HITL guidance converges: the gate must live outside the model; vague approval prompts breed automation bias — "make the human's job smaller and clearer" [S13, S14]. → The gate's checks-passed chips are the right instinct; what's missing is the *decision-grade* context: plain-language paraphrase of the SQL, cost in money/time not just GB, and who is authorized to approve (F12).

**P5 · Narrated Latency (the Labor Illusion, used honestly).** Buell & Norton: showing the work during a wait *increases* perceived value — users prefer a transparent wait over an instant identical result [S15]. Perplexity and both Deep Research products run a live activity feed ("reading X…") precisely for this [S7, S16]. → Direct empirical warrant for "latency is narrated" — but the effect depends on the narration being *real work in real time*. A mock with no in-flight states has not designed this yet (F1). Caveat from the same literature: fake or padded progress destroys the effect; every narrated step must map to an actual event.

**P6 · Claim-Anchored Citations.** Perplexity attaches numbered citations at the claim ("inline citations bring the source to the claim, exactly where doubt arises"), with hover cards and an expandable source list — a three-layer disclosure [S17, S18]. → Synapse's `mdm:ownership` chips sit at the answer level, not the claim level, and open nothing. Fused-witness answers ("multi-witness — no model guesswork") *need* claim-level anchoring more than web search does, because different sentences in one answer can carry different tiers (F4).

**P7 · Artifact Surface Beside the Chat.** Claude Artifacts / ChatGPT Canvas / Gemini Canvas all separate durable outputs into a side surface — versioned, exportable, editable — instead of burying them in the scroll [S19, S20]. → Inline chart cards are right for Phase 2, but the Graduation phase ("chat + canvas") should adopt this now in the information architecture: give every artifact an id and a "pin to workspace" affordance so 40-turn sessions don't bury the one chart the VP wants (F8, F11).

**P8 · Confidence Display ≠ Trust Calibration.** The research is consistent and sobering: showing model confidence, in any tested presentation, did not prevent overreliance — participants switched to agree with wrong AI 73% of the time regardless [S21]; miscalibrated confidence goes undetected by users [S22]; high confidence increases trust *and* degrades accuracy via automation bias [S23]; what helps is uncertainty paired with explanations and decision-stage support [S24]. NN/g-adjacent guidance: users rely more on systems that explain reasoning than on black boxes, but calibration comes from transparency + humility, not scores [S25, S26]. → "grounded · 0.90" as a passive pill is exactly the pattern the literature says fails. The chip must become an *explanation trigger* and a *behavior instruction*, and the UI needs contrast classes (inferred/guessed answers) for users to calibrate against (F4, F18).

**P9 · Approval Audit Trail as UI Object.** Enterprise HITL frameworks render each approval as a ledger record: who, when, what scope, with sampled approval for low-risk actions [S13, S14]. Palantir AIP catalogs every action in expressive audit logging [S6]. → The mock's "ledger #4821" is the right object; surface actor + timestamp in the resolved line, and treat the ledger id as a link, not a string (F3, F16).

**P10 · Streaming Accessibility Contract.** Consensus practice: an `aria-live="polite"` region (or `role="log"`) announcing *message-level* completions — never token-level deltas, which are unreadable noise in a screen reader; focus must move intentionally at state changes; hover-only disclosures need keyboard equivalents [S27, S28, S29]. → Currently absent; specify it as part of the event→component map so it's a property of the protocol renderer, not a retrofit (F6, F7).

**P11 · Capability Disclosure via Seeded Prompts.** Helpful assistants state what they can do and offer grounded prompt suggestions [S26, S30]. → The composer's "Verified answerable" seeds from the gate-checked demo pack are a genuinely strong version of this pattern — better than the generic version, because the suggestions are *provably* answerable. Keep; expose the endpoint (mock note #9).

---

## 2. The tradition, applied — seven operating principles

The Central/Eastern European school (JetBrains, Skype, Grammarly, Wise, Readdle): density in service of professionals, honesty as an engineering property, typography as structure, keyboard respect, and zero tolerance for ornament or manipulation. Each principle below ends in a testable implication for *this* console.

**T1 · Density is respect (JetBrains).** Professionals are not overwhelmed by information; they are overwhelmed by *unstructured* information. IDEA shows hundreds of signals through alignment, monospace, and stable columns. → Trace rows keep a fixed grammar — status dot · verb · duration — with sub-detail on one indented line, ≤2 lines per row, tabular numerals. **Test:** an analyst locates the tool call that produced ledger #4821 in a 40-turn session in <20s.

**T2 · Progressive disclosure with stable geography (JetBrains tool windows).** Panels have permanent homes and keyboard toggles; disclosure never relocates content or shifts layout under the cursor. → Rail toggle gets a shortcut; gate resolution must not reflow the conversation above it; expanding "How I got there" pushes only downward. **Test:** cumulative layout shift ≈ 0 during a full streamed turn; users can re-find a trace entry after collapse/expand without scanning.

**T3 · The UI never pretends (IntelliJ's "Indexing…" honesty; Skype's utilitarian presence states).** Every rendered state maps to a real system state; unaccounted time is labeled, features that can't work yet say so. → Narrate thinking time ("thought 2.6s") so answer meta (4.2s) reconciles with rail parts (0.9 + 0.7); live elapsed counters on active steps; a real error card for every `ErrorEvent`; never a spinner that could mean anything. **Test:** Σ(rail durations + labeled thinking) = turn meta total ± 0.1s on every golden flow.

**T4 · Trust is calibrated, not asserted (Grammarly).** Grammarly's suggestion cards earn acceptance by explaining *why* — "we need people to understand, relate, and feel supported" [S31, S32] — and its tone detector translates model output into human consequence, not scores. → Every provenance signal must (a) open its evidence on demand and (b) map to an action: grounded→"cite freely", inferred→"verify before external use", guessed→"treat as a lead". **Test:** M1/M2 in §4b — chip-only reliability ranking ≥ Kendall τ 0.8; verification behavior differs ≥40pp between inferred and grounded answers.

**T5 · Typography is the structure (constructivist).** Hierarchy from scale, weight, and family — not boxes, emoji, or decorative glyphs; color is a scarce semantic resource. → Enforce the mock's own rule #04: one semantic palette. Today `--good` *is* `--grounded` (#0E9463) and is spent on a "live" status dot and a chart delta; amber means inferred *and* awaiting-approval *and* cost; "▲" means danger (refusal shield) *and* growth (delta). Fix the collisions; kill the `◈` ornament on governance lines. **Test:** a grayscale screenshot remains fully legible; every color instance on screen can be named with exactly one meaning.

**T6 · The keyboard is a first-class citizen (JetBrains / Readdle).** The IA doc already calls ⌘K "non-negotiable" for this user base; the console currently has zero keyboard surface. → Approve/Hold reachable by keys with visible hints; rail toggle, j/k trace navigation, Esc closes panels; ⌘K over tables/columns/turns by Phase 4. **Test:** complete golden flow 2 (gate → approve → chart) mouse-free.

**T7 · Refusal with a path; costs stated plainly (Wise's mid-flow fee honesty; anti-dark-pattern).** Never euphemize a block and never hide a cost inside jargon. The cm11 refusal already does the first half well. → The gate must state cost in decision units (≈ $, seconds, "2% of daily budget"), not only "1.24 GB scanned"; the refusal keeps its cited rule + one-click alternative. **Test:** after 5s exposure, a VP can restate *why* it refused and *what to do instead* (§4b, scenario 3).

---

## 3. Expert evaluation of mock v1

### 3a. Findings table

Severity: **S0** blocker (resolve before React build proceeds) · **S1** major (must fix within the phase that touches it) · **S2** minor (should fix) · **S3** polish. Heuristics: N1–N10 = Nielsen; T1–T7 = tradition principles above. Evidence cites the mock's markup/CSS, `events.py` (E), `runner.py` (R), and the measured contrast audit (§3c).

| ID | Sev | Area | Violates | Evidence | Fix |
|----|-----|------|----------|----------|-----|
| F1 | **S0** | In-flight states | N1 visibility of status; T3 | Entire mock renders post-hoc turns. No active tool row, no elapsed timer, no streamed-text caret, no thinking-in-progress, no state between "Approve" click and results (real gap ≈ 7s: execute 6.4s + sandbox + chart). Principle #2 card even says "the rail is the loading state" — that state is undesigned. | Specify the in-flight grammar: active row = pulsing status dot + verb + live mm:ss.s timer; thinking = labeled shimmer row; answer streams with caret; on approve, gate button → inline progress ("running · 3.2s…"). One storyboard per golden flow, keyed to event arrival order. |
| F2 | **S0** | Stream reconciliation | N4 consistency; T3 | R lines 104–115: `Text` deltas stream the answer sentence, then `Answer` re-emits the same content with sections. No rule for replace vs append. | Contract rule: per turn, `Text` deltas render a provisional answer body that the `Answer` event **replaces in place** (same component slot, no scroll jump, diff-morph not flash). Document in events.py docstring + README table. |
| F3 | **S1** | Gate lifecycle | N3 user control; N5 error prevention; T3 | CSS `.gate.declined .actions{display:none}` removes Approve while `.declined-line` promises "approve any time to run it." E has no `gate_resolved` event; R fakes it via `ToolResult(call_id="g1")`. No states for stale/superseded gates, no behavior if user types a new message while a gate is pending. | Keep an Approve affordance in the held state. Add `gate_resolved{gate_id, decision, actor, ts, ledger_id}` to the protocol. Define the gate state machine: pending → approved / held (re-approvable) / superseded (new turn) / expired, each with a rendering. |
| F4 | **S1** | Provenance chips inert | N1; N10 help; T4; IA doc §5 (its own hero feature); P6, P8 | Chips (`.chip`, `.chip.cite`) are `<span>`s — no click, no hover detail, no witness list. E `Provenance` carries `sources[]` + `evidence_count` that the UI never shows; `citations` are bare strings with no resolvable target. All three flows show only `grounded`/`blocked` — no contrast class to calibrate against. Research: passive confidence displays fail to prevent overreliance [S21–S24]. | Make chips buttons → right-side witness panel (fact, tier, score, contributing witnesses w/ evidence counts, non-contributing, conflicts — the IA doc §5 panel, reused). Protocol: citations become `{label, ref}` resolving to graph nodes. Add one honest `inferred` answer to the golden pack. Tier tooltip carries the behavior mapping (T4). |
| F5 | **S1** | Tier vocabulary unclosed | N5; N4 | E: `tier: str` ("grounded \| inferred \| guessed \| ...") is open; graph also emits `human_asserted`, `deprecated`. CSS styles only `.tier-grounded/.tier-inferred/.tier-blocked`; **no `.tier-guessed` rule exists** — its dot renders with no background. `--guessed:#7E8CA1` is byte-identical to `--ink-3` (metadata text), 1.00:1 vs itself: "guessed" literally cannot be seen as a state. | Close the enum in `events.py` (6 values incl. blocked), define all six chip styles + an explicit unknown-tier fallback ("unrated" neutral w/ warning glyph in dev). Give guessed its own hue or (better) adopt the IA doc's dot-meter so tier ≠ color-only. |
| F6 | **S1** | Contrast | N8 aesthetic ≠ illegible; WCAG 1.4.3 | Measured (§3c): `--ink-3` on panel **3.41:1** light / **4.19:1** dark at 10–12.5px — used by `.meta`, `.dur`, `.tool`, `.gov` governance line, `.tgroup`, `.think`, `.src`, `.xlabels`, gate's "every decision lands on the audit ledger". Gate badge (inferred on white) **3.68:1**; resolved-line grounded **3.86:1**. None qualify as WCAG "large text." Projector demo rooms make this worse. | Darken `--ink-3` → ≈`#5D6C82` (light) / lighten dark-mode equivalent; or promote compliance-bearing lines (governance, ledger) to `--ink-2`. Re-run the contrast script (§3c) as a CI check on tokens. |
| F7 | **S1** | Screen-reader & focus architecture | WCAG 4.1.3, 2.4.3, 1.1.1; N7 | No `aria-live`/`role="log"` anywhere; a streaming trace is silence to a screen reader. On Approve, `.actions` gets `display:none` → focus dropped to `<body>`. Rail is an unlabeled `<aside>`; app has no heading/landmark structure; bar values are hover-only tooltips; chart has no text alternative; spec pins are `title=` on non-focusable divs. | Conversation = `role="log"` + polite announcements at message/turn completion (never per-token) [S27–S29]; on gate resolve, move focus to the resolved-line; label landmarks (main/aside/form); every chart ships a visually-hidden table (data already in the payload); keyboard-reachable tooltips. |
| F8 | **S1** | Trace rail at scale (40 turns) | N7 flexibility/efficiency; T1, T2 | Mock shows 3 turns ≈ 10 rows. At 40 turns: ~150–250 rows, all expanded, one flat scroll; no per-turn collapse, no follow-latest pin, no filter (by tool, by blocked, by turn), no link from an answer to its trace segment, no virtualization plan. `rhead` says "every step, evidenced" — a slogan where a control should be. | Collapse all past turns to one summary row (`Turn 12 · live query · 5 calls · 11.6s · ✓`); current turn expanded + sticky; "follow" auto-scroll pin that disengages on manual scroll; clicking an answer's meta/chips scrolls-and-highlights its trace segment (two-way ids exist: `turn_id`, `call_id`); virtualize the list. |
| F9 | **S1** | Error / empty / reconnect states | N9 recover from errors; T3 | E defines `ErrorEvent` ("never silent"); README maps it to a "legible failure card" — the only mapped event with **no mock**. Also unmocked: t=0 empty conversation, SSE disconnect/reconnect, artifact iframe failure, sandbox failure (`Sandbox.ok=False`), dry-run failure. | Mock the error card (message + recoverable → retry affordance + "what I was doing" context); empty state = capability statement + seeded questions (P11); connection banner with auto-retry countdown; artifact fallback = payload summary + download. |
| F10 | **S1** | No stop/cancel | N3 user control | A running turn cannot be interrupted anywhere in the mock or protocol. Long Gemini turns + paid queries make this mandatory; every surveyed product ships stop. | Stop button in composer during streaming (Esc = stop, with confirm if a query is executing); protocol: client → `POST /cancel`, stream ends with a `turn_end` carrying `cancelled: true`. |
| F11 | **S1** | Result data invisible | N1; analyst persona core task | The approved query returns 8 rows; the UI shows a chart + one sandbox-derived number. The rows themselves are nowhere — no table, no CSV, no way for an analyst to verify the chart or reuse the data. E has no rows payload on execute results. | Results-table component (collapsed to 5 rows, expandable, copy/CSV), fed by `ToolResult.payload.rows` (row-capped data is already small by construction). Analyst lens shows it by default; exec lens keeps it one gesture away. |
| F12 | **S1** | Gate legibility under demo pressure | N2 match system/world; T7; P4 [S13, S14] | Gate shows raw SQL (VPs can't parse), "1.24 GB scanned" with no anchor (is that big? costly?), no expected runtime, no row-count expectation, no statement of who may approve. Vague approval context is the documented recipe for rubber-stamping (automation bias). | Add one plain-language line above the SQL: "Counts new accounts per month (8 months) — read-only, ≤100 rows, ≈ $0.006, ~6s." Keep the SQL for analysts. Cost chip gets a denominator ("2% of daily budget"). Resolved line gains actor + timestamp (see F16). |
| F13 | **S2** | Semantic-palette self-violations | Mock's own principle #04; T5 | `--good` ≡ `--grounded` (#0E9463): green also means "runner connected" (`.live`) and "metric up" (`.delta .up`). Amber = inferred tier + awaiting-approval state + cost chip tint. "▲" = refusal shield (danger) and delta (growth). | Give status/positive-delta their own non-tier treatments (neutral dot + label for runner; delta arrow in `--ink` with tabular nums — the number is the signal); pick a distinct pending-approval hue (accent works); shield glyph ≠ trend glyph. |
| F14 | **S2** | Persona lens spec vs implementation | N4; T3 | Notes promise exec mode "compresses the gate to its resolved line" — no code does this (`.exec` class styles nothing; only `.analyst-only` display toggles + rail hide). Flipping personas clobbers a manually-toggled rail. Label "Executive" is rank-flattery, not content description; discoverability for a first-time VP is unexamined. | Implement or delete the promised compressions (T3). Persist rail preference per persona. Rename lenses by content: **"Answer"** / **"Full trace"** (JetBrains names modes by what they show). Consider defaulting by route/share-link rather than a toggle a VP must find. |
| F15 | **S2** | Vocabulary drift | N4 consistency | "5 witnesses" (appbar) vs "2 sources" (answer meta) vs "multi-witness" (how-I-got-there) vs IA doc "10 sources" vs README "7-source". Mock's grounded = green; visual-system doc's grounded = cyan (green reserved for `human_asserted`); IA doc prescribes dot-meter, no visible decimals — mock shows decimals. | One glossary, one palette owner. Decide: "witness" is the product word (it's better — adversarial, legal, memorable); use it everywhere. Reconcile the console palette with `UX_VISUAL_SYSTEM.md` or record an explicit, justified divergence in both docs. |
| F16 | **S2** | Time & attribution | N1; audit-grade claim; T3 | No wall-clock timestamps anywhere in the conversation of a *bank audit* console. "Approved by you" — no principal, no time. Turn meta 4.2s ≠ rail parts 1.6s; the 2.6s of thinking is unlabeled. | Timestamps on turn boundaries (hover-precise, relative by default); resolved line = "Approved by s.singh · 14:32:07Z · ledger #4821" (ledger id links to the record); add "thought for 2.6s" rows so totals reconcile (T3 test). |
| F17 | **S2** | Keyboard layer absent | N7; T6; IA doc §9.8 ("non-negotiable") | Zero shortcuts: gate has no keys, rail toggle mouse-only, no palette, no Esc semantics, no `?` help. | Phase 1: rail toggle + Esc-closes-panel + focus-visible order audit. Phase 3: gate keys (visible kbd hints on buttons, e.g. ⌘⏎ approve — require modifier to prevent reflex-Enter approval). Phase 4: ⌘K over tables/columns/turns; j/k in rail. |
| F18 | **S2** | Chip grammar & first-run vocabulary | N2; N10; P8 | Chip slot 2 is sometimes a score ("grounded · 0.90"), sometimes a phrase ("grounded · live query"). Decimals demand parsing (IA doc §6.4 explicitly moved scores to tooltips). No first-run explanation of tiers anywhere in-product; the notes section explains them — to us, not to users. | Fix the grammar: tier [· n witnesses]; score lives in the witness panel/tooltip. First-run affordance: tier legend popover on the first chip the user hovers + a "provenance" entry under `?`. |
| F19 | **S2** | <1000px behavior | N1; responsive honesty | Media query sets `.rail{display:none}` — the toggle button remains, now a lie. Split-screen laptop (the analyst's normal state, per visual-system doc §3.3) loses the trace entirely. | Below the breakpoint, rail becomes a bottom sheet / overlay triggered by the same button (visual-system doc already prescribes exactly this for the source panel). Button state must reflect reality. |
| F20 | **S2** | Raw CoT in a bank console | Governance; P1 [S1, S3] | `Thinking` deltas stream model-internal reasoning verbatim to the rail. Industry has moved to summarized thinking for latency *and* safety; raw CoT can contain speculation about restricted data, and reads as evidence when placed beside evidenced tool rows. | Summarize/curate the thinking channel server-side (it's already a distinct event); label it visually as "model notes — not evidence" (it currently shares the rail's evidentiary framing); exec lens hides it. |
| F21 | **S2** | No edit-before-approve / no copy | P3 co-planning [S7–S10]; T6 | Gate offers approve/hold only. Analysts will want to tweak the SQL (add a WHERE, change the window); today the only path is decline-and-rephrase. SQL and identifiers aren't copyable (visual-system doc calls copy-on-click its highest-utility micro-interaction). | Phase 3: "Edit query" opens the SQL editable; edited SQL re-runs validation + dry-run before the gate re-arms (the guardrail pipeline already exists). Phase 1: copy affordance on every `pre` and identifier. |
| F22 | **S2** | Compliance text is the faintest text | N8; T5 hierarchy honesty | The governance line ("No PII columns were touched…"), the audit-ledger note, and the refusal's "machine-checked rule" meta are all `--ink-3` 11.5–12px — the least legible class in the app carries the most trust-critical content. | These lines earn `--ink-2` + a stable slot in the answer contract. The `◈` ornament goes (T5); the words are the signal. |
| F23 | **S3** | Markup semantics | N4 | SQL keywords via `<b>` (screen readers may stress them); sandbox shows `mom = …` but result key `mom_pct` and R uses `pct`; snapshot date "20260706" unformatted; stray spec-pin div in appbar markup. | Span-based tokens in the build; align fixture names; "graph 2026-07-06"; pins are mock chrome — ensure none leak into components. |
| F24 | **S3** | Cost chip masquerades as a check | N4; Gestalt (IA doc kill-list logic) | `.cost` extends `.check` and sits in the checks row: three verified-safe pills + one judgment-required fact, same shape. | Move cost out of the checks row into the gate header line (it's the headline of the decision), or give it a distinct "estimate" shape. |
| F25 | **S3** | Approve idempotency | N5 | Mock button lacks disabled/loading state; double-click double-fires in a real build. | Disable on click + optimistic "running…" state; server-side gate idempotency by `gate_id`. |
| F26 | **S3** | Chart micro-issues | N7; dataviz a11y | Values hover-only except the endpoint; 8 x-labels near collision at narrower widths; no keyboard/touch path to values. | Analyst lens: value labels on all bars (they fit); or an axis-free table toggle. Tooltips focusable. (Geometry itself audited honest — keep.) |
| F27 | **S3** | "Run with masked key" unspecified | N3 | The refusal's alternative button has no defined next state (new gate? auto-run? new turn?). | Define: it drafts the masked-key SQL and opens a fresh gate — the alternative also passes through the same approval discipline (good demo beat, reinforces the gate). |

### 3b. Cognitive walkthroughs — three golden flows × two personas

Method: for each action step — will the user try it, see the control, understand the feedback, know they're progressing? Failures reference findings.

**Flow 1 · Governance answer ("Who owns sbs_new_accounts…")**
*Analyst:* Reads answer ✓ → wants to verify → sees chips → **clicks "grounded · 0.90" → nothing happens (F4)**. Falls back to "How I got there" — gets prose, not evidence. Wants "who else queries this" → composer seed exists ✓. Wants to copy the table name → no copy affordance (F21). *Verdict:* answer lands; verification dead-ends at the exact moment trust is being formed.
*VP:* Bold sentence works in one saccade ✓. "grounded · 0.90" — 0.90 of *what*? No legend, no tooltip (F18). Governance line reassures but is the faintest text on the card (F22, F6) — invisible from across a demo room. *Verdict:* trusts the sentence because the presenter says to, not because the chip taught them anything.

**Flow 2 · Live SQL with gate ("How many new accounts per month? Chart it.")**
*Analyst:* Watches trace verbs ✓ (strong: "the loop is paused, not guessing" is exactly right). At the gate: reads SQL ✓, notes row cap ✓ — wants to extend the window → no edit path (F21). Approves → **instant results in mock; in reality a ~7s hole with no designed state (F1)**. Gets chart → wants the 8 rows → not available (F11). Tries "Hold" on a second run → approve affordance disappears despite "approve any time" (F3).
*VP (watching, projector):* Gate appears — can they say aloud what will happen if the presenter clicks Approve? SQL is Greek; "1.24 GB" has no anchor; no ≈$ or duration (F12). The badge "QUERY GATE" is 10.5px amber at 3.68:1 — from the back of the room it's a smudge (F6). The *pause itself* reads well (the manual-approval choice in Open Decisions is correct — keep it), but the moment's content is analyst-grade only. After approve: "Approved by you… ledger #4821" — by *whom*, at *what time*? The audit claim is asserted, not shown (F16).

**Flow 3 · Guardrail refusal ("Break spend down by cm11_encrypted…")**
*Analyst:* Refusal names the rule with a resolvable-looking citation `skill:SBS_RollRates/guardrail#cm11` ✓ — clicks it → inert (F4). Alternative path offered ✓ but its behavior is undefined (F27).
*VP:* This is the best 10 seconds in the product. "Blocked by governance guardrail" + "machine-checked rule — not model judgment" + a compliant path = exactly the trust beat claimed. Two risks: the shield glyph "▲" just meant *growth* one card earlier (F13), and if the presenter is in Executive lens the trace's blocked row is hidden — fine — but the refusal card's meta line (the "machine-checked" claim) is `--ink-3` again (F22).
*Both personas, 40-turn projection:* conversation is an undifferentiated card column with no timestamps or turn anchors (F16); rail is a flat 200-row scroll (F8); the one chart that matters is 30 screens up (P7 gap).

### 3c. Contrast audit (measured, WCAG 2.x relative luminance)

Script + all pairs preserved for CI re-use. Key failures at the sizes used (10–12.5px = "normal text," AA requires 4.5:1):

| Pair (usage) | Ratio | AA |
|---|---|---|
| `--ink-3` #7E8CA1 on #FFFFFF — meta, durations, tool names, governance line, tgroups, think notes, x-labels | **3.41:1** | **fail** |
| `--ink-3` on `--panel-2` #FAFBFE (chip labels' container) | **3.30:1** | **fail** |
| inferred #B9770C on #FFFFFF — "QUERY GATE" badge | **3.68:1** | **fail** |
| grounded #0E9463 on #FFFFFF — "✓ Approved by you" | **3.86:1** | **fail** |
| dark `--ink-3` #6E7E96 on #141B26 | **4.19:1** | **fail** |
| guessed dot #7E8CA1 vs `--ink-3` text | **1.00:1** | identical value |
| `--ink-2`, accent, blocked, on-accent, dark ink-2/tiers | 5.0–8.0:1 | pass |

---

## 4. Recommendations + validation plan

### 4a. Prioritized changes for the React Phase 1 build

**MUST-FIX (before/during Phase 1 — blockers and contract changes that get expensive later):**

1. **In-flight grammar spec + build** — active tool row w/ live timer, thinking shimmer, streaming caret, approve→running progress, follow-latest pin. One storyboard per golden flow keyed to event order. *(F1 · P1, P5, T3)*
2. **Text→Answer replace-in-place reconciliation rule**, documented in `events.py` + README. *(F2 · T3)*
3. **Protocol amendments now:** `gate_resolved{decision, actor, ts, ledger_id}`; citations as `{label, ref}`; closed tier enum + fallback rendering; turn timestamps; `ToolResult.payload.rows` for execute; `cancelled` on `turn_end`. *(F3, F4, F5, F10, F11, F16)*
4. **Accessibility foundation:** darken `--ink-3` (≈#5D6C82) or re-tier compliance text; `role="log"` + polite message-complete announcements; focus moves to resolved-line on gate resolution; landmarks + headings; contrast script into CI. *(F6, F7, F22 · P10)*
5. **Trace rail scale mechanics:** past-turns collapsed to summary rows, sticky current turn, follow pin, answer⇄trace linking on existing ids, virtualized list. *(F8 · T1, T2 · P2)*
6. **Error/empty/disconnect cards + Stop.** Error card per `ErrorEvent`, t=0 empty state with seeded questions, reconnect banner, stop control. *(F9, F10 · P11)*

**SHOULD-FIX (Phase 2–3, the trust phases):**

7. Chip → witness panel (reuse IA doc §5 slide-over spec); tier behavior-mapping tooltips; one `inferred` flow added to the golden pack. *(F4, F18 · P6, P8, T4)*
8. Gate decision-grade context: plain-language paraphrase, ≈$ + runtime + budget-fraction, approver identity in resolved line; held-state keeps Approve; gate state machine incl. supersession. *(F3, F12, F16 · P4, T7)*
9. Results table + copy-SQL/identifiers + CSV export; "Edit query" re-validating loop. *(F11, F21 · P3)*
10. Semantic-palette cleanup (`--good` split from `--grounded`; pending ≠ inferred; shield ≠ delta glyph) and vocabulary unification (witness, everywhere) reconciled with `UX_VISUAL_SYSTEM.md`. *(F13, F15 · T5)*
11. Persona lens: implement the promised exec compressions or cut them; rename to content-based labels ("Answer" / "Full trace"); persist rail state. *(F14 · T3)*
12. Keyboard layer v1: gate keys w/ visible hints (modifier-guarded), Esc semantics, rail toggle. *(F17 · T6)*
13. Thinking channel: summarize server-side, label "model notes — not evidence." *(F20 · P1)*

**LATER (Phase 4 / Graduation):**

14. ⌘K palette; j/k rail navigation; `?` shortcut overlay. *(F17 · T6, IA §9.8)*
15. Artifact workspace: ids, pin-to-canvas, version list — the chat+canvas graduation. *(P7, F8)*
16. Responsive trace bottom-sheet <1000px. *(F19)*
17. Sampled-approval policy UI + authz display on gates; ledger id → ledger record view. *(P9, S13)*
18. Chart value labels / table toggle per lens; tooltip keyboard access. *(F26)*

### 4b. Lean validation plan (runnable this month, no lab)

**Protocol A — Analyst hallway test (n=5, 30 min each, live ScriptedRunner build).**
Five users catch ~85% of what a big-N test catches at this fidelity [S33]; recruit from the credit-risk analyst pool, think-aloud, screen-recorded. Tasks:
- **A1** "Find who owns `sbs_new_accounts` and *prove it to me*." (Success = reaches witness evidence ≤2 clicks, <10s from answer — the IA doc's north star applied to the console.)
- **A2** "Get monthly new accounts, charted." Then: "Hold the next query, change your mind, run it." (Probes gate comprehension + the held→approve path, F3.)
- **A3** "Ask for spend by `cm11_encrypted`." (Success = user explains the block in their own words + takes the masked-key path.)
- **A4** In a seeded 40-turn session: "Find the step that produced ledger #4821." (Target <20s, T1 test; probes rail scent, F8.)
- **A5** "Give me the numbers behind that chart in a spreadsheet." (F11.)
Measures: task success, time-to-evidence, single-ease-question per task, severity-coded observation log (S0–S3, same scale as §3). Two rounds: pre-Phase-2 (A1–A3 on the shell) and pre-demo (all).

**Protocol B — VP demo-trust probe (3 scenarios, n=3–5 execs or chief-of-staff proxies, 10 min each).**
Participant *drives* (not watches) each golden flow in Answer lens, presenter silent. After each scenario, three questions:
1. "What did the system just do?" (unaided recall)
2. "Would you repeat this number to the CRO tomorrow? What, if anything, would you check first?"
3. Scenario-specific: **B-gate** — participant must click Approve/Hold themselves; record time-to-decision (<3s = rubber-stamp signal [S13]), and whether they can state cost + read-only + row cap unaided (target ≥2 of 3 facts for 4/5 participants). **B-refusal** — "Did the system fail or succeed just now?" (target: ≥4/5 describe the block as the system *working*).
Also probe lens discoverability: "There's more detail behind this answer — find it" (F14).

**Trust-calibration metrics (measurable, repeatable):**
- **M1 · Chip-only reliability ranking.** 6 printed answer cards, identical prose, differing only in provenance stamps (grounded·3 witnesses / grounded·live query / inferred·0.6 / guessed·1 witness / blocked / unrated). Task: rank by "safe to put in a board deck," then state what you'd *do* differently for the middle ones. Pass: Kendall τ ≥ 0.8 vs ground truth AND a correct behavior statement for `inferred` from ≥4/5. If chips fail this on paper, they are decoration (F4).
- **M2 · Appropriate-reliance delta.** Seeded session containing one plausible-but-wrong `inferred` answer and three correct `grounded` answers. Measure verification acts (opened witness panel / cross-checked / verbalized doubt) per tier. Calibrated if verify-rate(inferred) − verify-rate(grounded) ≥ 40pp; failure mode to watch: uniform trust regardless of tier — the documented overreliance pattern [S21–S23].
- **M3 · Gate comprehension under pressure.** Immediately post-approval (Protocol B): unaided recall of scope (which table), cost bound, and safety properties (read-only/row cap). ≥2/3 facts for ≥4/5 participants; log time-to-approve distribution across sessions as the standing rubber-stamp telemetry.

---

## 5. Sources

**Reasoning disclosure & thinking UI:** [S1] Anthropic — Extended thinking, platform docs: https://platform.claude.com/docs/en/build-with-claude/extended-thinking · [S2] Claude Help Center — Using extended thinking (collapsible Thinking section): https://support.claude.com/en/articles/10574485-using-extended-thinking · [S3] HypoGray — "Why Claude Code hides its thinking": https://hypogray.com/stories/claude-code-hides-thinking

**Tool-trace & agent workspaces:** [S4] Cognition — Introducing Devin (workspace, Follow Devin): https://cognition.com/blog/introducing-devin · [S5] Devin Docs — intro (Shell/Browser/Editor/Planner tabs): https://docs.devin.ai/get-started/devin-intro · [S6] Palantir — AIP observability trace view & overview: https://www.palantir.com/docs/foundry/aip-observability/trace-view, https://www.palantir.com/docs/foundry/aip-observability/overview

**Plan approval & deep research UX:** [S7] Google — Gemini Deep Research overview (plan review/edit/approve, live feed): https://gemini.google/overview/deep-research/ · [S8] Section — ChatGPT vs Gemini Deep Research (plan hidden in expandable widget; clarifying questions): https://www.sectionai.com/blog/chatgpt-vs-gemini-deep-research

**Human-in-the-loop mechanics:** [S9] Microsoft Research — Magentic-UI (co-planning, co-tasking, action guards, memory): https://www.microsoft.com/en-us/research/blog/magentic-ui-an-experimental-human-centered-web-agent/ · [S10] arXiv 2507.22358 — Magentic-UI: Towards Human-in-the-loop Agentic Systems: https://arxiv.org/abs/2507.22358 · [S11] Builder.io — Windsurf vs Cursor (diff review layer, per-step approval): https://www.builder.io/blog/windsurf-vs-cursor · [S12] Descope — Cursor vs Windsurf (review-layer UX): https://www.descope.com/blog/post/cursor-vs-windsurf · [S13] TeamCopilot — HITL agents: approvals, permissions, audit trails (automation bias, vague prompts): https://teamcopilot.ai/blog/human-in-the-loop-ai-agents-approvals-permissions-audit-trails · [S14] Permit.io — HITL for AI agents, best practices (gate outside the model; risk-scoped approval): https://www.permit.io/blog/human-in-the-loop-for-ai-agents-best-practices-frameworks-use-cases-and-demo

**Latency & operational transparency:** [S15] Buell & Norton — "The Labor Illusion: How Operational Transparency Increases Perceived Value," *Management Science* 57(9), 2011: https://pubsonline.informs.org/doi/10.1287/mnsc.1110.1376 · [S16] LivePlan — ChatGPT vs Gemini deep research (activity feed behavior): https://www.liveplan.com/blog/planning/deep-research-chatgpt-vs-gemini

**Citation & trust UI:** [S17] AI UX Playground — Perplexity citations case study (hover/click/expanded layers): https://www.aiuxplayground.com/gallery/perplexity-citations/ · [S18] AYDesign — AI citation & source UI patterns 2026 (claim-anchored inline citations): https://www.aydesign.ai/blog/ai-citation-source-ui-patterns-2026

**Artifact/canvas surfaces:** [S19] poltextLAB — Canvas and Artifacts in GenAI interfaces: https://promptrevolution.poltextlab.com/enhancing-research-productivity-a-comprehensive-guide-to-canvas-and-artifacts-in-genai-interfaces/ · [S20] Unmarkdown — Claude Artifacts vs ChatGPT Canvas: https://unmarkdown.com/blog/claude-artifacts-vs-chatgpt-canvas

**Confidence display & trust calibration research:** [S21] Zhang, Liao, Bellamy — "Effect of Confidence and Explanation on Accuracy and Trust Calibration in AI-Assisted Decision Making" (arXiv 2001.02114): https://arxiv.org/pdf/2001.02114 · [S22] "Understanding the Effects of Miscalibrated AI Confidence on User Trust, Reliance, and Decision Efficacy" (arXiv 2402.07632): https://arxiv.org/html/2402.07632v4 · [S23] "Explainability and AI Confidence in Clinical Decision Support Systems… Breast Cancer Care," IJHCI 2025: https://www.tandfonline.com/doi/full/10.1080/10447318.2025.2539458 · [S24] "Designing for Appropriate Reliance…" (arXiv 2401.05612): https://arxiv.org/pdf/2401.05612

**General AI-UX guidance:** [S25] UXmatters — Design psychology of trust in AI (NN/g-cited stats on transparency/confidence): https://www.uxmatters.com/mt/archives/2025/11/the-design-psychology-of-trust-in-ai-crafting-experiences-users-believe-in.php · [S26] NN/g — Prioritize Smarts over Sentience to Increase Trust with AI: https://www.nngroup.com/articles/smarts-emotion-trust-ai/ · [S30] ParallelHQ — Chatbot UX patterns 2026 (capability disclosure, prompt suggestions): https://www.parallelhq.com/blog/chatbot-ux-design

**Streaming accessibility:** [S27] Sara Soueidan — Accessible notifications with ARIA live regions: https://www.sarasoueidan.com/blog/accessible-notifications-with-aria-live-regions-part-1/ · [S28] Centre for Excellence in Universal Design — ARIA for announcing updates (announce latest message only): https://universaldesign.ie/communications-digital/web-and-mobile-accessibility/web-accessibility-techniques/developers-introduction-and-index/use-aria-appropriately/use-aria-to-announce-updates-and-messaging · [S29] A11Y Pros — Accessible AI: WCAG compliance in chatbots & generative UIs: https://a11ypros.com/blog/accessible-ai

**Tradition exemplars:** [S31] Grammarly Engineering — How suggestions work in the Grammarly Editor (suggestion cards, instant-apply UX): https://www.grammarly.com/blog/engineering/how-suggestions-work-grammarly-editor/ · [S32] Grammarly — Tone detector (model output translated to human consequence): https://www.grammarly.com/blog/product/tone-detector/ · [S33] NN/g — Why You Only Need to Test with 5 Users: https://www.nngroup.com/articles/why-you-only-need-to-test-with-5-users/

**Pattern catalogs consulted:** Agentic Design — UI/UX & progressive disclosure patterns: https://agentic-design.ai/patterns/ui-ux-patterns · Awesome Agentic Patterns — HITL approval framework: https://agentic-patterns.com/patterns/human-in-loop-approval-framework/

---

## Appendix — contrast audit script

Kept for CI reuse: computes WCAG relative-luminance ratios for the token pairs in §3c. Location during this review: scratchpad `contrast.py`; recommend committing under `apps/console/design/tools/` and wiring to the token file so F6 cannot regress.
