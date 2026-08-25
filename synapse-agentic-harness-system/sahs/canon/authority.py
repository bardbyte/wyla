"""The authority lattice — pinned here, imported everywhere, defined once.

    certified(dmp) ≻ pending(extended_gmns) ≻ skill-contract
        ≻ mined(support, recency) ≻ snippet

Within a tier, support and recency break ties; across tiers, nothing does —
the resolver sorts lexicographically on (authority, everything else), so
support can never outvote certification. Census ranks by the same constant,
which is why it lives in canon/ (P0) and not the compiler (P2).
"""

from __future__ import annotations

from enum import IntEnum


class Authority(IntEnum):
    SNIPPET = 1          # blue_business_insights — tribal fragments
    MINED = 2            # measures_catalog — distilled query history
    SKILL_CONTRACT = 3   # skill-pack metric_contracts
    PENDING = 4          # extended_gmns_semantics — submitted for approval
    CERTIFIED = 5        # metrics_dmp — the meridian line


# Source-name → tier. Adapters stamp records with these names verbatim.
SOURCE_AUTHORITY: dict[str, Authority] = {
    "metrics_dmp": Authority.CERTIFIED,
    "extended_gmns": Authority.PENDING,
    "skill_contract": Authority.SKILL_CONTRACT,
    "measures_catalog": Authority.MINED,
    "blue_insights": Authority.SNIPPET,
    "gold_queries": Authority.SKILL_CONTRACT,  # human-verified references
}

# Resolver confidence ceilings per tier (E6/A2: uncalibrated bets — the
# constants are versioned in every build manifest).
TIER_CEILING: dict[Authority, float] = {
    Authority.CERTIFIED: 0.95,
    Authority.PENDING: 0.80,
    Authority.SKILL_CONTRACT: 0.70,
    Authority.MINED: 0.55,
    Authority.SNIPPET: 0.40,
}


def authority_for(source: str) -> Authority:
    try:
        return SOURCE_AUTHORITY[source]
    except KeyError:
        raise ValueError(
            f"unknown source {source!r} — known: "
            + ", ".join(sorted(SOURCE_AUTHORITY))) from None
