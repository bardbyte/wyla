Validate LUMI enrichment output. Check these in order:

1. Read output/reports/coverage_report.json
   Report: X/137 queries fully resolvable

2. For any FAIL queries, identify:
   - Missing measure (SQL aggregation with no LookML measure)
   - Missing dimension (column in WHERE/GROUP BY with no dimension)
   - Missing explore (join pattern with no explore definition)
   - Schema gap (column doesn't exist in view at all)

3. Read output/reports/vocab_report.json
   Report any vocabulary inconsistencies across views

4. Check that data_source='cornerstone' is mentioned in ALL explore descriptions

5. Count fields by enrichment source:
   - Gold-query-informed (highest quality)
   - MDM-informed (good quality)
   - LLM-inferred (needs review)

6. List any views with <50% enrichment coverage

7. Summarize: what's working, what needs fixing, recommended next action
