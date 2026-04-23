Create or update the LUMI implementation plan.

1. Read CLAUDE.md for architecture and build order
2. Read docs/DESIGN.md for full pipeline specification
3. Read all existing code in tools/, agents/, schemas/
4. Identify what's already built vs. what remains

Think hard about:
- Dependencies between modules (what must be built first?)
- Which tools each agent needs (build tools before agents)
- Test fixtures needed (create early, use everywhere)
- Edge cases specific to this project:
  - 14K+ line view files
  - MDM returning empty for some tables
  - sqlglot failing on non-standard BigQuery SQL
  - Gold queries with CTEs and CASE WHEN

Output a prioritized task list to docs/PLAN.md with:
- Task ID, description, dependencies, estimated time
- Current status (done/in-progress/not-started)
- Which session it belongs to
