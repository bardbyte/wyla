"""Fingerprints — 12-hex identity of a canonical text.

fp = sha256(canonical_text ⊕ NUL ⊕ dialect ⊕ NUL ⊕ canon_version)[:12]

Embedding dialect and canon_version means a fingerprint can never
accidentally collide across rulesets or sqlglot upgrades — an upgrade
changes every fp, which is exactly the loud behavior we want (the remint
migration re-derives IDs from the stored canonical text).
"""

from __future__ import annotations

import hashlib

FP_LEN = 12


def fingerprint(canonical_text: str, dialect: str, canon_version: str) -> str:
    payload = "\x00".join((canonical_text, dialect, canon_version))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:FP_LEN]
