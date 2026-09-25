"""The promoted build, kept in Spanner and materialised on any host.

``publish`` packs a compiled build directory and writes it to
``Builds`` / ``BuildBundles`` / ``BuildBundleChunks``
(``003_graph.sql`` and ``005_build_bundles.sql``),
marking it promoted only after the last chunk lands. ``ensure_local``
does the other half: read the promoted build id, unpack it into a
local cache once, and write that cache's ``CURRENT`` — after which
``Build.open`` and every existing reader work unchanged.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any

from sahs.builds import bundle
from sahs.builds.bundle import BundleError
from sahs.spanner import SpannerSettings, grpc_endpoint

# Spanner caps a single value at 10 MiB; 4 MiB leaves room for the
# rest of the mutation and keeps each commit comfortably small.
CHUNK_BYTES = 4 * 1024 * 1024


class BuildSyncError(RuntimeError):
    """Raised when the promoted build cannot be published or fetched."""


def _database(settings: SpannerSettings) -> Any:
    try:
        from google.api_core.client_options import ClientOptions
        from google.cloud import spanner
    except ModuleNotFoundError as exc:      # pragma: no cover - env guard
        raise BuildSyncError("google-cloud-spanner is required when the "
                             "build source is spanner") from exc
    from sahs.util.network import apply_grpc_proxy
    apply_grpc_proxy(settings.proxies)
    client_kwargs: dict[str, Any] = {
        "project": settings.project_id,
        "client_options": ClientOptions(
            api_endpoint=grpc_endpoint(settings.endpoint)),
        # local/offline dev has no path to Cloud Monitoring
        "disable_builtin_metrics": True,
    }
    if settings.credentials_data is not None:
        from google.oauth2 import service_account
        client_kwargs["credentials"] = (
            service_account.Credentials.from_service_account_info(
                settings.credentials_data))
    elif settings.credentials_path is not None:
        from google.oauth2 import service_account
        client_kwargs["credentials"] = (
            service_account.Credentials.from_service_account_file(
                settings.credentials_path))
    client = spanner.Client(**client_kwargs)
    return client.instance(settings.instance_id).database(
        settings.database_id)


class SpannerBuildStore:
    """Publish a compiled build to Spanner; materialise it anywhere."""

    def __init__(self, settings: SpannerSettings | None = None, *,
                 database: Any | None = None,
                 chunk_bytes: int = CHUNK_BYTES) -> None:
        self.chunk_bytes = chunk_bytes
        self._database = database if database is not None else _database(
            settings or SpannerSettings.from_env())

    @classmethod
    def from_env(cls) -> "SpannerBuildStore":
        return cls(SpannerSettings.from_env())

    # ── reads ───────────────────────────────────────────────

    def _query(self, sql: str, params: dict[str, Any] | None = None
               ) -> list[dict[str, Any]]:
        from google.cloud.spanner import param_types

        params = params or {}
        types = {name: (param_types.INT64 if isinstance(value, int)
                        and not isinstance(value, bool) else param_types.STRING)
                 for name, value in params.items()}
        with self._database.snapshot() as snapshot:
            rows = snapshot.execute_sql(sql, params=params, param_types=types)
            values = list(rows)
            names = [field.name for field in rows.fields]
        return [dict(zip(names, tuple(row), strict=True)) for row in values]

    def promoted(self) -> dict[str, Any] | None:
        """→ the newest promoted build that has a complete bundle."""
        rows = self._query(
            "SELECT b.BuildId, n.Sha256, n.SizeBytes, n.Format, "
            "       n.ChunkCount, n.FileCount "
            "FROM Builds AS b JOIN BuildBundles AS n "
            "  ON n.BuildId = b.BuildId "
            "WHERE b.Promoted = TRUE AND n.Complete = TRUE "
            "ORDER BY b.PromotedAt DESC LIMIT 1")
        return rows[0] if rows else None

    def fetch(self, build_id: str) -> bytes:
        """→ the bundle bytes for one build, chunks concatenated in order."""
        rows = self._query(
            "SELECT Chunk FROM BuildBundleChunks WHERE BuildId = @build "
            "ORDER BY Seq", {"build": build_id})
        if not rows:
            raise BuildSyncError(f"no bundle stored for build {build_id}")
        return b"".join(
            base64.b64decode(row["Chunk"])
            if isinstance(row["Chunk"], str) else bytes(row["Chunk"])
            for row in rows)

    def promote(self, build_id: str, *, actor: str = "") -> dict[str, Any]:
        """Move the promotion pointer to an already complete bundle.

        This is also the rollback operation: prior immutable bundles stay in
        Spanner and can be re-promoted without being uploaded again.
        """
        rows = self._query(
            "SELECT BuildId FROM BuildBundles "
            "WHERE BuildId = @build AND Complete = TRUE",
            {"build": build_id})
        if not rows:
            raise BuildSyncError(
                f"build {build_id} has no complete bundle to promote")

        def move(transaction: Any) -> None:
            transaction.execute_update(
                "UPDATE Builds SET Promoted = FALSE, PromotedAt = NULL "
                "WHERE Promoted = TRUE AND BuildId != @build",
                params={"build": build_id},
                param_types={"build": _string()})
            changed = transaction.execute_update(
                "UPDATE Builds SET Promoted = TRUE, "
                "PromotedAt = CURRENT_TIMESTAMP(), PromotedBy = @actor "
                "WHERE BuildId = @build",
                params={"build": build_id, "actor": actor or ""},
                param_types={"build": _string(), "actor": _string()})
            if changed != 1:
                raise BuildSyncError(f"build {build_id} is not registered")

        self._database.run_in_transaction(move)
        return {"build_id": build_id, "promoted": True}

    # ── publish ─────────────────────────────────────────────

    def publish(self, build_dir: Path, *, promote: bool = True,
                actor: str = "", run_id: str = "") -> dict[str, Any]:
        """Store a compiled build; promote it once every chunk has landed.

        Re-publishing the same build id replaces its chunks, so a run
        interrupted halfway is repaired by running it again.
        """
        from google.cloud import spanner

        build_dir = Path(build_dir)
        manifest = json.loads(
            (build_dir / "manifest.json").read_text(encoding="utf-8"))
        build_id = str(manifest.get("build_id") or "")
        if not build_id:
            raise BundleError(f"{build_dir}/manifest.json carries no build_id")
        blob, meta = bundle.pack(build_dir)
        slices = bundle.chunks(blob, self.chunk_bytes)

        def head(transaction: Any) -> None:
            transaction.execute_update(
                "DELETE FROM Builds WHERE BuildId = @build",
                params={"build": build_id},
                param_types={"build": _string()})
            transaction.insert(
                "Builds",
                ("BuildId", "RunId", "Manifest", "Promoted", "BuiltAt"),
                [(build_id, run_id or None, json.dumps(manifest), False,
                  spanner.COMMIT_TIMESTAMP)])
            transaction.insert(
                "BuildBundles",
                ("BuildId", "Format", "SizeBytes", "Sha256", "FileCount",
                 "ChunkBytes", "ChunkCount", "Complete", "PublishedBy",
                 "PublishedAt"),
                [(build_id, meta.format, meta.size_bytes, meta.sha256,
                  meta.file_count, self.chunk_bytes, len(slices), False,
                  actor or None, spanner.COMMIT_TIMESTAMP)])

        self._database.run_in_transaction(head)
        for seq, chunk in enumerate(slices):
            with self._database.batch() as batch:
                batch.insert("BuildBundleChunks",
                             ("BuildId", "Seq", "Chunk"),
                             [(build_id, seq,
                               base64.b64encode(chunk).decode("ascii"))])

        def finish(transaction: Any) -> None:
            transaction.execute_update(
                "UPDATE BuildBundles SET Complete = TRUE "
                "WHERE BuildId = @build",
                params={"build": build_id},
                param_types={"build": _string()})
            if promote:
                transaction.execute_update(
                    "UPDATE Builds SET Promoted = FALSE, PromotedAt = NULL "
                    "WHERE Promoted = TRUE AND BuildId != @build",
                    params={"build": build_id},
                    param_types={"build": _string()})
                transaction.execute_update(
                    "UPDATE Builds SET Promoted = TRUE, "
                    "PromotedAt = CURRENT_TIMESTAMP(), PromotedBy = @actor "
                    "WHERE BuildId = @build",
                    params={"build": build_id, "actor": actor or ""},
                    param_types={"build": _string(), "actor": _string()})

        self._database.run_in_transaction(finish)
        return {"build_id": build_id, "sha256": meta.sha256,
                "size_bytes": meta.size_bytes, "files": meta.file_count,
                "chunks": len(slices), "promoted": promote}

    # ── materialise ─────────────────────────────────────────

    def ensure_local(self, cache_root: Path) -> Path:
        """Unpack the promoted build under ``cache_root`` → that root.

        The returned directory is a builds root in the shape every
        reader already knows: ``CURRENT`` naming a sibling build
        directory. Already-materialised builds are not re-downloaded.
        """
        row = self.promoted()
        if row is None:
            raise BuildSyncError(
                "no promoted build in Spanner: run `pipeline.py "
                "publish-build` from a host that has compiled one")
        build_id = str(row["BuildId"])
        if str(row.get("Format") or "") != bundle.FORMAT:
            raise BuildSyncError(
                f"build {build_id} is stored as {row.get('Format')!r}; "
                f"this reader knows {bundle.FORMAT!r}")
        cache_root = Path(cache_root)
        target = cache_root / build_id
        if not (target / "manifest.json").is_file():
            bundle.unpack(self.fetch(build_id), target,
                          expected_sha256=str(row["Sha256"]))
        write_current(cache_root, build_id)
        return cache_root


def _string() -> Any:
    from google.cloud.spanner import param_types
    return param_types.STRING


def write_current(builds_root: Path, build_id: str) -> None:
    """Point a builds root at one build, atomically on POSIX and Windows.

    ``os.replace`` is atomic for files on both; the temporary lands in
    the same directory so the replace never crosses a filesystem.
    """
    builds_root = Path(builds_root)
    builds_root.mkdir(parents=True, exist_ok=True)
    current = builds_root / "CURRENT"
    if current.exists() and current.read_text(
            encoding="utf-8").strip() == build_id:
        return
    tmp = builds_root / f"CURRENT.{os.getpid()}.tmp"
    tmp.write_bytes((build_id + "\n").encode("utf-8"))
    os.replace(tmp, current)
