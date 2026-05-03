"""Stage 3: Validate coverage + evaluate quality (LoopAgent).

Built in Session 3. Evaluator-optimizer pattern:
  1. Coverage checker (deterministic) — checks all input SQLs
  2. Gap fixer (LlmAgent) — re-enriches tables with gaps
  3. Loop until coverage >= 90% or max_iterations = 3

Key functions:
  check_coverage(sqls, enriched_outputs, fingerprints) -> CoverageReport
  run_evaluation_loop(sqls, outputs, config) -> CoverageReport
"""

# TODO: Session 3 — implement here
