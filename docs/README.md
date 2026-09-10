# docs/

Repo-wide documentation. The silo's own operational docs live in
`synapse-agentic-harness-system/docs/` (runbooks, export contracts, design
specs, eval baselines).

| what | where | for |
|---|---|---|
| **The wiki** | [`wiki/`](wiki/README.md) | 14 pages, one per subsystem, with source citations and diagrams. Start at the index. |
| **The paper** | [`paper/compiling-a-semantic-layer.md`](paper/compiling-a-semantic-layer.md) | How the graph is built, why each mechanism is the way it is, what the numbers are, and what is still a bet. |
| **The deck** | [`presentation/synapse-system-overview.html`](presentation/synapse-system-overview.html) | 31 slides: graph → harness → generative UI → capabilities and proof. Open it in a browser; `←` `→` to move, `O` for the overview. |
| Design sweep | [`design_inventory.md`](design_inventory.md) | the sweep of the design files |

## Reading order

New to the repo: the root [`README.md`](../README.md), then
[wiki page 1](wiki/01-overview.md) and [page 2](wiki/02-architecture.md).

Here to change the graph or the compiler: the paper first, then wiki pages
[3](wiki/03-canon-and-identity.md)–[6](wiki/06-compiler.md), then
`docs/runbooks/p2_build.md` in the silo.

Here to change the agent: wiki pages [7](wiki/07-serving.md)–[9](wiki/09-ask-lane-a2ui.md),
then `docs/specs/synapse_v3_harness.md` and `docs/harness-discipline.md` in the silo.

Presenting it to someone: the deck.
