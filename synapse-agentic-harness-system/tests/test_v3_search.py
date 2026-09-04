"""Search across the chats: fuzzy, legible, a finder not a ranker."""

from __future__ import annotations

from sahs.assistant.search import match, search_sessions, snippet, word_match


def test_word_match_grades_exact_prefix_substring_and_typo():
    assert word_match("churn", "churn") == 1.0
    assert word_match("chu", "churn") == 0.9
    assert word_match("urn", "churn") == 0.8
    assert 0.6 < word_match("merchnt", "merchant") <= 0.8   # a typo
    assert word_match("zebra", "churn") == 0.0
    assert word_match("a", "apple") == 0.0            # one letter is noise


def test_match_ranks_phrase_over_all_words_over_some_words():
    phrase, _ = match("merchant churn", "how should I think about merchant churn?")
    all_words, hits = match("churn merchant", "merchant churn, the framing")
    some, _ = match("merchant churn tax", "merchant churn, the framing")
    none, none_hits = match("zebra quantum", "merchant churn, the framing")
    assert phrase == 2.0
    assert 1.0 < all_words < phrase
    assert 0 < some < all_words
    assert none == 0.0 and none_hits == []
    assert hits == ["churn", "merchant"]


def test_match_forgives_a_typo_and_names_the_word_it_hit():
    score, hits = match("merchnt", "the merchant left")
    assert score > 0 and hits == ["merchant"]


def test_snippet_windows_around_the_first_hit():
    text = "x " * 200 + "the merchant left in June " + "y " * 200
    out = snippet(text, ["merchant"], width=60)
    assert "merchant" in out and out.startswith("…") and out.endswith("…")
    assert len(out) <= 62
    assert snippet("short", [], 60) == "short"
    assert snippet("a **rate** and a `mix`", ["rate"], 60) == "a rate and a mix"


def test_search_sessions_lists_everything_or_finds_fuzzily():
    sessions = [
        {"id": "s1", "title": "Merchant churn framing", "updated_at": "2026-09-02"},
        {"id": "s2", "title": "Spend by day", "updated_at": "2026-09-03"},
        {"id": "s3", "title": "", "updated_at": "2026-09-01"},
    ]
    messages = {
        "s1": [{"role": "user", "text": "how should I think about merchant churn?"},
               {"role": "assistant", "text": "Churn splits into a rate question and a mix question."}],
        "s2": [{"role": "user", "text": "chart spend by day"},
               {"role": "assistant", "text": "Spend by day is in the panel."}],
        "s3": [],
    }
    everything = search_sessions(sessions, messages.get, "")
    assert [r["id"] for r in everything] == ["s1", "s2", "s3"]
    assert everything[0]["preview"].startswith("how should I think")
    assert everything[0]["messages"] == 2 and everything[2]["messages"] == 0

    hits = search_sessions(sessions, messages.get, "merchnt chrn")
    assert [r["id"] for r in hits] == ["s1"]
    top = hits[0]
    assert top["matched"] == 2 and len(top["snippets"]) == 2
    assert "merchant" in top["snippets"][0]["hits"]
    assert "churn" in top["snippets"][0]["text"].lower()

    by_day = search_sessions(sessions, messages.get, "day")
    assert [r["id"] for r in by_day] == ["s2"]
    assert search_sessions(sessions, messages.get, "zebra quantum") == []
