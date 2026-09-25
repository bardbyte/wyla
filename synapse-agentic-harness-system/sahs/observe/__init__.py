"""Observability as a MIRROR of the record (docs/runbooks/langfuse.md).

Every Langfuse object is derived from something Synapse already writes:
the assistant's event stream becomes traces, the eval harness's
verdicts become dataset-run scores, the task files become datasets.
Nothing in the product or the suite reads Langfuse back — if it is
switched off (the default) or unreachable, a turn is a turn and a
score is a score.

The translator (``TurnTracer``) is a pure function of event records,
so replaying an events file gives the trace the live turn gave.
"""

from .setup import langfuse_observer                 # noqa: F401
from .tracer import Recorder, TurnTracer, replay_file  # noqa: F401
