"""load_dotenv reads a .env file once per process (the app calls it on
every request and every settings read): the parse is cached by path
and stamp, os.environ is consulted fresh each call, an edited file is
re-read, and reset_dotenv_cache forgets everything."""

from __future__ import annotations

import os

from sahs.util import auth
from sahs.util.auth import load_dotenv, reset_dotenv_cache


def test_the_file_is_parsed_once_and_the_environment_is_read_fresh(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("ONE_SETTING=file\nTWO_SETTING=file\n", encoding="utf-8")
    for name in ("ONE_SETTING", "TWO_SETTING"):
        monkeypatch.delenv(name, raising=False)
    reset_dotenv_cache()
    reads = 0
    real = auth.Path.read_text

    def counting(self, *a, **k):
        nonlocal reads
        if self == env:
            reads += 1
        return real(self, *a, **k)

    monkeypatch.setattr(auth.Path, "read_text", counting)
    assert set(load_dotenv(env)) == {"ONE_SETTING", "TWO_SETTING"}
    assert os.environ["ONE_SETTING"] == "file" and reads == 1
    # the shell (a test) changes its mind: the next call honours it
    monkeypatch.setenv("ONE_SETTING", "shell")
    monkeypatch.delenv("TWO_SETTING")
    assert load_dotenv(env) == ["TWO_SETTING"]
    assert os.environ["ONE_SETTING"] == "shell" and os.environ["TWO_SETTING"] == "file"
    assert reads == 1                                         # no second read
    # override still wins, from the cached pairs
    assert set(load_dotenv(env, override=True)) == {"ONE_SETTING", "TWO_SETTING"}
    assert os.environ["ONE_SETTING"] == "file" and reads == 1


def test_an_edited_file_is_read_again_and_reset_forgets(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("EDITED_SETTING=one\n", encoding="utf-8")
    monkeypatch.delenv("EDITED_SETTING", raising=False)
    reset_dotenv_cache()
    assert load_dotenv(env) == ["EDITED_SETTING"] and os.environ["EDITED_SETTING"] == "one"
    monkeypatch.delenv("EDITED_SETTING")
    env.write_text("EDITED_SETTING=two\n", encoding="utf-8")
    stat = env.stat()
    os.utime(env, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    assert load_dotenv(env) == ["EDITED_SETTING"] and os.environ["EDITED_SETTING"] == "two"
    assert str(env.resolve()) in auth._DOTENV_CACHE
    reset_dotenv_cache()
    assert auth._DOTENV_CACHE == {}
    assert load_dotenv(tmp_path / "absent.env") in ([], load_dotenv(None))
