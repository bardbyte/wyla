Build a LUMI ADK agent. Follow this sequence exactly:

1. Read docs/DESIGN.md for this agent's role, inputs, and outputs
2. Read the tools this agent will use (in tools/*.py)
3. Determine type:
   - LlmAgent: only if task requires LLM reasoning
   - CustomAgent: if task is deterministic Python
4. For LlmAgent:
   - model: read from config (strong_model or fast_model)
   - temperature: 0 (always)
   - output_key: set for session.state writing
   - instruction: load from prompts/$ARGUMENTS.md
   - tools: list only what this agent needs
5. For CustomAgent:
   - Implement _run_async_impl
   - Read from ctx.session.state
   - Write to ctx.session.state
   - Yield Event with progress
6. Write the prompt file at prompts/$ARGUMENTS.md
7. Write integration test at tests/test_agents/test_$ARGUMENTS.py:
   - Pre-populate session.state with fixture data
   - Run agent
   - Assert output structure in session.state
8. Run: pytest tests/test_agents/test_$ARGUMENTS.py -v
9. Report what was built
