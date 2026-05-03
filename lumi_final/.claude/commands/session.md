Run LUMI build session $ARGUMENTS.

1. Read CLAUDE.md
2. Read LUMI_BUILD_PLAN.md, find session $ARGUMENTS
3. Read all previous docs/SESSION_*.md for context
4. Follow TDD: write failing tests FIRST, then implement
5. Run full suite after each change: pytest tests/ -v
6. If ANY previous test breaks, fix it before new code
7. Write docs/SESSION_$ARGUMENTS.md with what was built and learned
8. Update CLAUDE.md status + completed sessions
9. Commit: "session $ARGUMENTS: [one-line summary]"
10. Push

If stuck on an API shape, write a probe script in scripts/ and ask
Saheb to run it. Don't guess.
