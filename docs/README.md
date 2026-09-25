# docs/

Repo-wide documentation. The silo's own operational docs live in
`synapse-agentic-harness-system/docs/` (runbooks, export contracts, design
specs, eval baselines).

| what | where | for |
|---|---|---|
| **The sheet** | [`architecture/`](architecture/README.md) | the whole system on one page: sources → graph → build → the two turn engines → both surfaces, with every gate drawn. Send this to someone with ten minutes. |
| **The wiki** | [`wiki/`](wiki/README.md) | 14 pages, one per subsystem, with source citations and diagrams. Start at the index. |
| **The paper** | [`paper/compiling-a-semantic-layer.md`](paper/compiling-a-semantic-layer.md) | How the graph is built, why each mechanism is the way it is, what the numbers are, and what is still a bet. |
| **The deck** | [`presentation/synapse-system-overview.html`](presentation/synapse-system-overview.html) | 31 slides: graph → harness → generative UI → capabilities and proof. Open it in a browser; `←` `→` to move, `O` for the overview. |
| **The story deck** | [`presentation/radix-graph-end-to-end.pptx`](presentation/radix-graph-end-to-end.pptx) | 11 slides for a room: one question with three right answers, why the usual fix decays, the turn, then the architecture at three depths. [Source and how to rebuild it.](presentation/radix-graph-deck/README.md) |
| **The mark** | [`brand/radix-graph/`](brand/radix-graph/README.md) | the Radix Graph logo — SVG masters and PNGs, with the geometry it was built from |
| Design sweep | [`design_inventory.md`](design_inventory.md) | the sweep of the design files |
| The enterprise port | [`enterprise-port.md`](enterprise-port.md) | what came across from the enterprise branch, every deviation, and how a change is carried back |
| Spanner wiring | [`spanner-wiring.md`](spanner-wiring.md) | every path the app persists, the store class behind it, and the Spanner table it lands in under `SAHS_STORE=spanner`, or the gap |
| Deploying | [`deploy.md`](deploy.md) | the one flag (`SAHS_ENV_FILE`), the four profiles, what each readiness check proves, the DDL order, the tenancy decision, day one on a new environment |

## Reading order

New to the repo: the root [`README.md`](../README.md), then the
[end-to-end sheet](architecture/README.md), then
[wiki page 1](wiki/01-overview.md) and [page 2](wiki/02-architecture.md).

Here to change the graph or the compiler: the paper first, then wiki pages
[3](wiki/03-canon-and-identity.md)–[6](wiki/06-compiler.md), then
`docs/runbooks/p2_build.md` in the silo.

Here to change the agent: wiki pages [7](wiki/07-serving.md)–[9](wiki/09-ask-lane-a2ui.md),
then `docs/specs/synapse_v3_harness.md` and `docs/harness-discipline.md` in the silo.

Presenting it to someone: the deck.
