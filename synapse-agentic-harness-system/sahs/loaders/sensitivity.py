"""Whether ATLAS catalog sensitivity enters the graph — one switch.

SCOPE. This holds back one source: ``std_tech_metadata``, the Atlas
catalog. Nothing else. The MDM plane's ``is_pii`` and the
``policy:pii`` edge it mints flow exactly as they always have, and so
does every BigQuery row-access policy. The hold is about what the
CATALOG declares, not about what sensitivity means downstream.

WHAT IS WITHHELD — all of it from ``std_tech_metadata``:

  table   ``has_pii`` / ``has_gdpr`` / ``has_oncop``, and the three
          parallel declaration lists ``pii_columns`` /
          ``gdpr_columns`` / ``oncop_columns``
  column  ``pii_role_id`` / ``sde_group`` on the pde attribute

WHAT THIS DOES NOT TOUCH.

  MDM (``mdm46``)  ``is_pii`` on the column and its ``policy:pii``
    edge. The MDM plane is the registry these declarations are relayed
    FROM; holding the relay back does not mean doubting the source.
  BigQuery (``bq_extraction``)  ``policy:unknown_denied`` and
    ``policy:row_access_N``. Row-access policy is NOT sensitivity: it
    describes access control the WAREHOUSE enforces — a query really
    does fail for a caller outside the grant. That is the one
    legitimate live-execution gate.
  ``data_classification``  a table-wide handling label, not a
    statement about any column's contents.

CONSEQUENCE WORTH KNOWING. E1's D5 compares the two planes' answers
about a column. With the Atlas side withheld, a column MDM calls
sensitive has no second opinion — so D5 stops reporting divergence and
the MDM answer stands alone. That is a real loss of a cross-check, not
a side effect: the disagreement still exists in the feeds, it is just
no longer visible while the hold is on.

REVERSING IT. Set ``SAHS_LOAD_SENSITIVITY=1`` in the environment, or
edit the default below; nothing else needs editing. The env form
exists so one build can be run with the catalog's declarations on — to
see what they would say — without a commit. The graph is append-only,
so the next build with the hold lifted ADDS them rather than needing a
rebuild; but a graph built under the hold has never held them, so
lifting it is not retroactive for rows nobody re-registers.
"""
from __future__ import annotations

import os

# The hold on std_tech_metadata sensitivity. The default is the
# decision; the env var exists so a single run can lift it without a
# commit.
LOAD_SENSITIVITY = os.environ.get("SAHS_LOAD_SENSITIVITY", "") == "1"
