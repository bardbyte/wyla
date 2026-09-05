"""Search across the chats: every session, its title and its messages,
fuzzy — a typo, a partial word or a different inflection still finds
the chat, and the lines that matched come back as snippets so the
results page can show WHERE it matched.

Pure functions over the store's rows: the backend is a door, the
frontend a renderer. The scoring is plain and legible: the phrase
itself outranks every word matched, which outranks some words
matched; the title outranks a message; a fuzzy word (a prefix, a
substring, an edit-distance neighbour) scores below an exact one.
Nothing here is a ranker anyone tunes — it is a finder.
"""

from __future__ import annotations

import difflib
import re
from typing import Any, Callable

_WORD = re.compile(r"[a-z0-9]+")
_MARKUP = re.compile(r"[*_`#>]+")
FUZZY_FLOOR = 0.75          # the SequenceMatcher ratio a typo must reach
SNIPPET_WIDTH = 150


def tokens(text: str) -> list[str]:
    return _WORD.findall((text or "").lower())


def word_match(token: str, word: str) -> float:
    """How well one query token matches one word of the text: 1 for
    the word itself, .9 for a prefix, .8 for a substring, up to .8 for
    an edit-distance neighbour (a typo), 0 otherwise."""
    if token == word:
        return 1.0
    if len(token) >= 2 and word.startswith(token):
        return 0.9
    if len(token) >= 3 and token in word:
        return 0.8
    if len(token) >= 4 and abs(len(token) - len(word)) <= 2:
        ratio = difflib.SequenceMatcher(None, token, word).ratio()
        if ratio >= FUZZY_FLOOR:
            return round(ratio * 0.8, 3)
    return 0.0


def match(query: str, text: str) -> tuple[float, list[str]]:
    """→ (score, the words of the text that matched); (0, []) when
    nothing did. The whole phrase found verbatim scores 2; every token
    matched scores their mean plus a bonus; some tokens matched score
    their mean scaled by the fraction that matched."""
    q = tokens(query)
    if not q or not text:
        return 0.0, []
    words = tokens(text)
    if not words:
        return 0.0, []
    if " ".join(q) in " ".join(words):
        return 2.0, sorted(set(q))
    per: list[float] = []
    hits: list[str] = []
    distinct = set(words)
    for token in q:
        best, best_word = 0.0, ""
        for word in distinct:
            score = word_match(token, word)
            if score > best:
                best, best_word = score, word
        per.append(best)
        if best_word:
            hits.append(best_word)
    matched = [p for p in per if p > 0]
    if not matched:
        return 0.0, []
    mean = sum(matched) / len(matched)
    fraction = len(matched) / len(per)
    score = mean * fraction + (0.5 if fraction == 1.0 else 0.0)
    return round(score, 3), sorted(set(hits))


def snippet(text: str, hits: list[str],
            width: int = SNIPPET_WIDTH) -> str:
    """A window of the text around the first matched word; an ellipsis
    marks each edge that was cut."""
    # prose, not markup: the emphasis and code marks of an answer do
    # not belong in a one-line result
    text = " ".join(_MARKUP.sub("", text or "").split())
    if not text:
        return ""
    low = text.lower()
    at = -1
    for hit in hits:
        at = low.find(hit)
        if at >= 0:
            break
    if at < 0:
        return text[:width] + ("…" if len(text) > width else "")
    start = max(0, at - width // 3)
    end = min(len(text), start + width)
    return (("…" if start > 0 else "") + text[start:end]
            + ("…" if end < len(text) else ""))


def search_sessions(sessions: list[dict[str, Any]],
                    messages_of: Callable[[str], list[dict[str, Any]]],
                    query: str, *, limit: int = 200,
                    per_session: int = 3) -> list[dict[str, Any]]:
    """Every session that matches, best first (newest first among
    equals); with an empty query every session in the order given,
    each with a preview of its first ask."""
    query = (query or "").strip()
    out: list[dict[str, Any]] = []
    for session in sessions:
        texts = [(str(m.get("role") or ""), str(m.get("text") or ""))
                 for m in (messages_of(session["id"]) or [])
                 if str(m.get("text") or "").strip()]
        first_ask = next((t for role, t in texts if role == "user"), "")
        base = {
            "id": session["id"],
            "title": session.get("title") or "",
            "updated_at": session.get("updated_at") or "",
            "created_at": session.get("created_at") or "",
            "starred": bool(session.get("starred")),
            "running": bool(session.get("running")),
            "messages": len(texts),
            "preview": snippet(first_ask, [], 120),
        }
        if not query:
            out.append({**base, "score": 0.0, "title_hits": [],
                        "matched": 0, "snippets": []})
            continue
        title_score, title_hits = match(query, base["title"])
        scored = []
        for role, text in texts:
            score, hits = match(query, text)
            if score > 0:
                scored.append((score, role, text, hits))
        if title_score == 0 and not scored:
            continue
        scored.sort(key=lambda s: -s[0])
        total = title_score * 1.5 + sum(s[0] for s in scored[:per_session])
        out.append({**base,
                    "score": round(total, 3),
                    "title_hits": title_hits,
                    "matched": len(scored),
                    "snippets": [{"role": role, "text": snippet(text, hits),
                                  "hits": hits}
                                 for _s, role, text, hits
                                 in scored[:per_session]]})
    if query:
        out.sort(key=lambda r: r["updated_at"], reverse=True)
        out.sort(key=lambda r: -r["score"])
    return out[:limit]
