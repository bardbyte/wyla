Build a LUMI tool function. Follow this sequence exactly:

1. Read docs/DESIGN.md to understand where this tool fits
2. Read any existing tools this one depends on
3. Write the Pydantic schema if this tool introduces new data structures
4. Write pytest tests FIRST in tests/test_tools/test_$ARGUMENTS.py:
   - Happy path with fixture data
   - Error case (file not found, API timeout, malformed input)
   - Edge case (empty input, large input)
5. Run: pytest tests/test_tools/test_$ARGUMENTS.py -v
   Confirm all tests FAIL (no implementation yet)
6. Implement in tools/$ARGUMENTS.py:
   - Full type hints on all parameters and return
   - Detailed docstring (ADK uses this for tool selection)
   - Return dict with "status" key ("success" or "error")
   - Use pathlib for paths, logging not print()
   - NEVER use regex for SQL (sqlglot) or LookML (lkml)
7. Run: pytest tests/test_tools/test_$ARGUMENTS.py -v
   All tests must PASS
8. Run: ruff check lumi/tools/$ARGUMENTS.py --fix
9. Report what was built and any design decisions
