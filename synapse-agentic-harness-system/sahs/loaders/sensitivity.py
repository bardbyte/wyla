"""Whether catalog sensitivity enters the graph at all — one switch.

THE HOLD. A sensitive column today produces a ``sensitive_column``
VIOLATION in ``validate_sql``, and a violation is a REFUSAL: the agent
cannot project the column at all. That is stricter than the intent,
which was to FLAG a column as sensitive while still answering over it.
Until the loader decides what sensitivity should mean downstream, none
of it is loaded — a fact the graph does not hold cannot be enforced
wrongly, and an append-only store cannot un-say what it has said.

WHAT IS WITHHELD. Every compliance DECLARATION the catalogs make:

  Atlas (``std_tech_metadata``)  ``has_pii`` / ``has_gdpr`` /
    ``has_oncop`` on the table, the three parallel declaration lists
    (``pii_columns`` / ``gdpr_columns`` / ``oncop_columns``), and
    ``pii_role_id`` / ``sde_group`` on the column itself.
  MDM (``mdm46``)  ``is_pii`` on the column, and the ``policy:pii``
    edge it mints.

WHAT THIS DOES NOT TOUCH. Row-access policy is NOT sensitivity. The
``policy:unknown_denied`` and ``policy:row_access_N`` edges come from
BigQuery's own ``bq_extraction`` and describe access control the
WAREHOUSE enforces: a query really does fail for a caller outside the
grant. That is the one legitimate live-execution gate and it stays.
``data_classification`` is a table-wide handling label (Internal /
Confidential), not a statement about any column's contents, and it
also stays.

REVERSING IT. Set ``SAHS_LOAD_SENSITIVITY=1`` in the environment, or
edit the default below; nothing else needs editing. The env form
exists so one build can be run with sensitivity on — to see what the
declarations would say — without a commit. The graph is append-only,
so the next build with the hold lifted ADDS the declarations rather
than needing a rebuild; but a graph built under the hold has never
held them, so lifting it is not retroactive for rows nobody
re-registers.
"""
from __future__ import annotations

import os

# The hold. The default is the decision; the env var exists so a
# single run can lift it without a commit.
LOAD_SENSITIVITY = os.environ.get("SAHS_LOAD_SENSITIVITY", "") == "1"
