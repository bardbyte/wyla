"""Resolver constants (A2 — uncalibrated bets, versioned everywhere).

Every build manifest embeds this block; every resolve() response cites
``constants_version``. Changing any value bumps the version — a wrong
bind must always be traceable to the constants that produced it.
"""

RESOLVER_CONSTANTS = {
    "version": "rc1",
    "weights": {"support": 0.4, "recency": 0.3, "context_fit": 0.3},
    "recency_half_life_days": 90,
    "margin_threshold": 0.15,
    "tier_ceiling": {"5": 0.95, "4": 0.80, "3": 0.70, "2": 0.55,
                     "1": 0.40},
    "fuzzy_reach_forces_ask": True,
}
