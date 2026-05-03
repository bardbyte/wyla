Run all quality checks and report status.

1. pytest tests/ -v (full suite)
2. If output/ has LookML: lint all with lkml parser
3. If output/ has coverage_report.json: show coverage %
4. If any failures: identify root cause, suggest fix
5. One-paragraph summary: what works, what's broken, what's next
