"""The .env loader and the small helpers: precedence, values, routes."""

from __future__ import annotations

import os

from kcx.env import (describe_route, dotenv_value, env_flag, env_int,
                     first_env, google_proxies, load_dotenv, load_env,
                     redact_url)


def test_values_quoted_inline_comment_export_and_the_shell_wins(tmp_path,
                                                                monkeypatch):
    f = tmp_path / ".env"
    f.write_text('A="quoted value"\nB=/path/to/key.json # a note\n'
                 'export C=exported\n# comment\nD=\n\nnot a line\n')
    monkeypatch.setenv("A", "from-shell")
    for name in ("B", "C", "D"):
        monkeypatch.delenv(name, raising=False)
    assert load_dotenv(f) == ["B", "C", "D"]
    assert os.environ["A"] == "from-shell"
    assert os.environ["B"] == "/path/to/key.json"
    assert os.environ["C"] == "exported"
    assert os.environ["D"] == ""
    assert dotenv_value("'x # not a comment'") == "x # not a comment"


def test_a_missing_file_loads_nothing_and_falls_back_to_nothing(tmp_path):
    assert load_dotenv(tmp_path / "nope.env") == []
    report = load_env(tmp_path)
    assert report["kc"]["found"] is False and report["kc"]["loaded"] == []
    assert report["extra"]["found"] is False


def test_load_env_reads_the_kc_file_then_the_extra_file(tmp_path,
                                                        monkeypatch):
    kc, extra = tmp_path / "kc.env", tmp_path / "extra.env"
    kc.write_text("KC_PROJECT_ID=from-kc\nVERTEX_MODEL=from-kc\n")
    extra.write_text("VERTEX_MODEL=from-extra\nVERTEX_PROJECT_ID=from-extra\n"
                     "KC_PROJECT_ID=from-extra\nAPP_ID=from-extra\n")
    monkeypatch.setenv("KC_ENV_FILE", str(kc))
    monkeypatch.setenv("KC_EXTRA_ENV_FILE", str(extra))
    monkeypatch.setenv("VERTEX_PROJECT_ID", "from-shell")
    report = load_env(tmp_path)
    assert report["kc"]["loaded"] == ["KC_PROJECT_ID", "VERTEX_MODEL"]
    assert report["extra"]["loaded"] == ["APP_ID"]
    assert os.environ["KC_PROJECT_ID"] == "from-kc"        # kc beats extra
    assert os.environ["VERTEX_MODEL"] == "from-kc"
    assert os.environ["VERTEX_PROJECT_ID"] == "from-shell"  # shell beats all
    assert os.environ["APP_ID"] == "from-extra"


def test_the_extra_file_may_be_named_inside_the_kc_file_relative_to_the_folder(
        tmp_path, monkeypatch):
    monkeypatch.delenv("KC_ENV_FILE")
    monkeypatch.delenv("KC_EXTRA_ENV_FILE")
    folder, sibling = tmp_path / "folder", tmp_path / "sibling"
    folder.mkdir()
    sibling.mkdir()
    (folder / ".env").write_text("KC_EXTRA_ENV_FILE=../sibling/.env\n")
    (sibling / ".env").write_text("APP_ID=from-sibling\n")
    report = load_env(folder)
    assert report["kc"]["path"] == str((folder / ".env").resolve())
    assert report["extra"]["path"] == str((sibling / ".env").resolve())
    assert report["extra"]["found"] is True
    assert os.environ["APP_ID"] == "from-sibling"


def test_first_env_flags_and_ints(monkeypatch):
    monkeypatch.setenv("KC_A", "  ")
    monkeypatch.setenv("KC_B", " b ")
    assert first_env("KC_A", "KC_B") == "b"
    assert first_env("KC_A") is None
    assert env_flag("KC_NOPE") is False and env_flag("KC_NOPE", True) is True
    for value in ("1", "true", "YES", "on"):
        monkeypatch.setenv("KC_F", value)
        assert env_flag("KC_F") is True
    monkeypatch.setenv("KC_F", "0")
    assert env_flag("KC_F", True) is False
    monkeypatch.setenv("KC_N", "7")
    assert env_int("KC_N", 1) == 7
    monkeypatch.setenv("KC_N", "seven")
    assert env_int("KC_N", 1) == 1


def test_google_hosts_ride_https_proxy_unless_disabled_and_never_read_no_proxy(
        monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://user:pw@proxy:8080")
    monkeypatch.setenv("NO_PROXY", "googleapis.com")
    assert google_proxies("KC_DISABLE_PROXY") == {
        "https": "http://user:pw@proxy:8080"}
    monkeypatch.setenv("KC_DISABLE_PROXY", "1")
    assert google_proxies("KC_DISABLE_PROXY") == {}
    assert describe_route({"https": "http://user:pw@proxy:8080"}) == \
        "via http://proxy:8080"
    assert describe_route({}) == "direct"
    assert redact_url("http://user:p%40ss@proxy.example.com:8080") == \
        "http://proxy.example.com:8080"
    assert redact_url("proxy.example.com:8080") == "proxy.example.com:8080"
