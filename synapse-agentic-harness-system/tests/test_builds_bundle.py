"""A compiled build as one deterministic bundle: pack, chunk, unpack."""

from __future__ import annotations

import json

import pytest

from sahs.builds import bundle


def _build(tmp_path, name="b1"):
    root = tmp_path / name
    (root / "cards" / "concepts").mkdir(parents=True)
    (root / "manifest.json").write_text(json.dumps({"build_id": name}))
    (root / "cards" / "concepts" / "spend.md").write_text("# spend\n")
    return root


def test_pack_is_deterministic_and_unpack_round_trips(tmp_path):
    root = _build(tmp_path)
    blob1, meta1 = bundle.pack(root)
    blob2, meta2 = bundle.pack(root)
    assert blob1 == blob2 and meta1 == meta2 and meta1.file_count == 2
    parts = bundle.chunks(blob1, 16)
    assert b"".join(parts) == blob1 and len(parts) > 1
    dest = bundle.unpack(blob1, tmp_path / "out" / "b1", expected_sha256=meta1.sha256)
    assert (dest / "cards" / "concepts" / "spend.md").read_text() == "# spend\n"
    with pytest.raises(bundle.BundleError, match="digest mismatch"):
        bundle.unpack(blob1, tmp_path / "out2" / "b1", expected_sha256="0" * 64)


def test_unsafe_and_windows_hostile_names_are_refused(tmp_path):
    root = _build(tmp_path)
    (root / "cards" / "con:cept.md").write_text("x")
    with pytest.raises(bundle.BundleError, match="Windows"):
        bundle.pack(root)
    with pytest.raises(bundle.BundleError):
        bundle._check_name("../escape")
    with pytest.raises(bundle.BundleError, match="reserved"):
        bundle._check_name("cards/NUL.md")
    with pytest.raises(bundle.BundleError, match="not a compiled build"):
        bundle.pack(tmp_path / "nowhere")
