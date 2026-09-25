"""A compiled build as one portable blob.

The build directory is ~10k small files whose only reader is the
filesystem, so it travels as a single deterministic gzipped tar rather
than as rows. Deterministic: names sorted and POSIX, owner and mtime
zeroed, modes fixed — the same build packs to the same bytes on a
laptop and in a container, which makes the SHA-256 a real witness.

Unpacking is hostile-input safe (no absolute paths, no ``..``, no
Windows-illegal names) and writes through a staging directory, so a
half-finished extraction is never visible under the build id.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import os
import shutil
import stat
import tarfile
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

FORMAT = "tar.gz"

# Windows refuses these as path components whatever the extension, and
# rejects these characters; a bundle carrying one could never unpack on
# half the fleet, so it is refused on both halves.
_WINDOWS_DEVICE_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{n}" for n in range(1, 10)),
    *(f"LPT{n}" for n in range(1, 10)),
}
_ILLEGAL_CHARS = set('<>:"\\|?*')


class BundleError(ValueError):
    """Raised when a bundle, or a directory offered as one, is unusable."""


@dataclass(frozen=True, slots=True)
class BundleMeta:
    """What the store records beside the bytes."""

    size_bytes: int
    sha256: str
    file_count: int
    format: str = FORMAT


def _members(build_dir: Path) -> list[tuple[str, Path]]:
    """→ (posix name, path) for every file, sorted, names validated."""
    out: list[tuple[str, Path]] = []
    for path in build_dir.rglob("*"):
        if path.is_symlink():
            raise BundleError(
                f"symlink in build: {path} — builds are plain files so "
                "they mean the same thing on every host")
        if not path.is_file():
            continue
        name = PurePosixPath(path.relative_to(build_dir).as_posix())
        _check_name(str(name))
        out.append((str(name), path))
    out.sort(key=lambda item: item[0])
    return out


def _check_name(name: str) -> None:
    if not name or name.startswith("/") or name.startswith("../"):
        raise BundleError(f"unsafe path in bundle: {name!r}")
    parts = PurePosixPath(name).parts
    if any(part in ("..", ".") for part in parts):
        raise BundleError(f"unsafe path in bundle: {name!r}")
    if len(name) > 1 and name[1] == ":":
        raise BundleError(f"drive-qualified path in bundle: {name!r}")
    for part in parts:
        if _ILLEGAL_CHARS & set(part):
            raise BundleError(
                f"path in bundle cannot exist on Windows: {name!r}")
        if part.split(".")[0].upper() in _WINDOWS_DEVICE_NAMES:
            raise BundleError(
                f"reserved Windows name in bundle: {name!r}")
        if part != part.rstrip(" ."):
            raise BundleError(
                f"path in bundle cannot exist on Windows: {name!r}")


def pack(build_dir: Path) -> tuple[bytes, BundleMeta]:
    """→ (bundle bytes, metadata) for a compiled build directory."""
    build_dir = Path(build_dir)
    if not (build_dir / "manifest.json").is_file():
        raise BundleError(
            f"not a compiled build: {build_dir / 'manifest.json'} missing")
    members = _members(build_dir)
    raw = io.BytesIO()
    # mtime=0 and no filename: the gzip header carries no host detail
    with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6,
                       mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w", format=tarfile.PAX_FORMAT
                          ) as tar:
            for name, path in members:
                info = tarfile.TarInfo(name)
                info.size = path.stat().st_size
                info.mtime = 0
                info.mode = 0o644
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.type = tarfile.REGTYPE
                with path.open("rb") as handle:
                    tar.addfile(info, handle)
    blob = raw.getvalue()
    return blob, BundleMeta(size_bytes=len(blob),
                            sha256=hashlib.sha256(blob).hexdigest(),
                            file_count=len(members))


def chunks(blob: bytes, chunk_bytes: int) -> list[bytes]:
    """Slice a bundle for storage; the store commits one slice at a time."""
    if chunk_bytes < 1:
        raise BundleError("chunk_bytes must be positive")
    return [blob[start:start + chunk_bytes]
            for start in range(0, len(blob), chunk_bytes)] or [b""]


def unpack(blob: bytes, dest: Path, *, expected_sha256: str = "") -> Path:
    """Materialise a bundle at ``dest``, atomically and idempotently.

    An existing ``dest`` is left alone: a build id names immutable
    content, so a directory that is already there is already right.
    """
    dest = Path(dest)
    if expected_sha256:
        actual = hashlib.sha256(blob).hexdigest()
        if actual != expected_sha256:
            raise BundleError(
                f"bundle digest mismatch: stored {expected_sha256}, "
                f"read {actual} — the upload is incomplete or corrupt")
    if (dest / "manifest.json").is_file():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    staging = dest.parent / f".staging-{uuid.uuid4().hex[:12]}"
    try:
        staging.mkdir(parents=True)
        with gzip.GzipFile(fileobj=io.BytesIO(blob), mode="rb") as gz:
            with tarfile.open(fileobj=gz, mode="r|") as tar:
                for info in tar:
                    if not info.isfile():
                        raise BundleError(
                            f"bundle holds a non-file member: {info.name}")
                    _check_name(info.name)
                    target = staging.joinpath(*PurePosixPath(info.name).parts)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source = tar.extractfile(info)
                    if source is None:
                        raise BundleError(f"unreadable member: {info.name}")
                    with target.open("wb") as handle:
                        shutil.copyfileobj(source, handle)
        try:
            os.rename(staging, dest)
        except OSError:
            # another process won the race; its copy is the same bytes
            if not (dest / "manifest.json").is_file():
                raise
    finally:
        _remove(staging)
    return dest


def _remove(path: Path) -> None:
    """Delete a tree, including files Windows marked read-only."""
    if not path.exists():
        return
    for child in path.rglob("*"):
        try:
            os.chmod(child, stat.S_IWRITE)
        except OSError:
            pass
    shutil.rmtree(path, ignore_errors=True)
