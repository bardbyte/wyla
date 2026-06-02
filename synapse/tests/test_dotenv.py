"""Tests for the minimal .env loader."""

from __future__ import annotations

from pathlib import Path

from synapse.utils.dotenv import _parse_line, load_dotenv_file


def test_parse_simple_kv():
    assert _parse_line("KEY=value") == ("KEY", "value")


def test_parse_strips_export_prefix():
    assert _parse_line("export FOO=bar") == ("FOO", "bar")


def test_parse_handles_quotes():
    assert _parse_line('KEY="quoted value"') == ("KEY", "quoted value")
    assert _parse_line("KEY='single quoted'") == ("KEY", "single quoted")


def test_parse_skips_comments_and_blanks():
    assert _parse_line("# this is a comment") is None
    assert _parse_line("") is None
    assert _parse_line("   ") is None


def test_parse_skips_lines_without_equals():
    assert _parse_line("not an env line") is None


def test_parse_allows_equals_in_value():
    assert _parse_line("URL=https://x.y/?a=1&b=2") == (
        "URL", "https://x.y/?a=1&b=2",
    )


def test_load_dotenv_file_respects_existing_env(monkeypatch, tmp_path: Path):
    """Existing env vars are NOT overwritten unless override=True."""
    monkeypatch.setenv("ALREADY_SET", "from_shell")
    env_path = tmp_path / ".env"
    env_path.write_text("ALREADY_SET=from_file\nNEW_KEY=fresh\n")
    applied = load_dotenv_file(env_path)
    assert "ALREADY_SET" not in applied  # not overwritten
    assert applied.get("NEW_KEY") == "fresh"


def test_load_dotenv_file_override_replaces(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("KEY_X", "old")
    env_path = tmp_path / ".env"
    env_path.write_text("KEY_X=new\n")
    applied = load_dotenv_file(env_path, override=True)
    assert applied["KEY_X"] == "new"
    import os
    assert os.environ["KEY_X"] == "new"


def test_load_dotenv_missing_file_returns_empty(tmp_path: Path):
    applied = load_dotenv_file(tmp_path / "does_not_exist.env")
    assert applied == {}
