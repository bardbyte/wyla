"""One process, both planes, in the order the chat uses them: a
BigQuery dry run first, then the model plane the chat rides on this
machine (Vertex, or EAG when SAHS_MODEL_PLANE / its credentials say
so): a token and one tiny model call.

    python scripts/planes_check.py

Separate check scripts never shared the bug: each process starts
clean. The chat is ONE process, and its first dry run used to write
googleapis.com into NO_PROXY and send every later model call direct
into the corporate blackhole. This is the laptop proof that the routes
stay pinned on their connections across planes. Exit 0 both planes
answered · 1 a plane failed or the environment changed between them ·
3 a plane is not configured.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.util.auth import (                                # noqa: E402
    AuthError,
    BQConnection,
    VertexConnection,
    load_dotenv,
)
from sahs.util.console import EXIT_ENV_AUTH                 # noqa: E402

_WATCHED = ("NO_PROXY", "no_proxy", "HTTPS_PROXY", "https_proxy",
            "HTTP_PROXY", "http_proxy")


def _env() -> dict[str, str | None]:
    return {name: os.environ.get(name) for name in _WATCHED}


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    print(time.strftime("%H:%M:%S"))
    print("one process, both planes, in the chat's order:")
    try:
        bq = BQConnection.from_env()
    except AuthError as e:
        print(f"✗ BigQuery not configured: {e}", file=sys.stderr)
        return EXIT_ENV_AUTH
    print(f"  BigQuery  {bq.route()} · {bq.endpoint}")
    from sahs.evals.substrate import BQDryRun
    before = _env()
    started = time.perf_counter()
    outcome = BQDryRun(bq).dry_run("SELECT 1")
    seconds = time.perf_counter() - started
    if not outcome.valid:
        print(f"✗ the dry run failed after {seconds:.1f}s: "
              f"{outcome.error}", file=sys.stderr)
        return 1
    print(f"  ✓ dry run valid in {seconds:.1f}s")
    if _env() != before:
        changed = [k for k in _WATCHED if _env()[k] != before[k]]
        print("✗ the environment changed during the BigQuery plane: "
              + ", ".join(changed) + " — a route leaked into the "
              "process; the Vertex plane would inherit it",
              file=sys.stderr)
        return 1
    from sahs.util.eag import model_plane, plane_note
    if model_plane() == "eag":
        # the chat's model calls ride EAG on this machine: prove that
        # plane after the dry run, in the chat's order
        from sahs.enrich.eag_client import EagClient
        from sahs.util.eag import EagError
        try:
            client = EagClient.from_env()
        except EagError as e:
            print(f"✗ EAG not configured: {e}", file=sys.stderr)
            return EXIT_ENV_AUTH
        print(f"  EAG       {client.cfg.model} · {client.cfg.base_url} · "
              f"the chat's plane ({plane_note()})")
        started = time.perf_counter()
        try:
            client.tokens.token()
            print(f"  ✓ {client.tokens.describe()}")
            text = client.generate('Return exactly this JSON: {"ok": true}',
                                   max_output_tokens=512)
        except Exception as e:                             # noqa: BLE001
            print(f"✗ EAG failed {time.perf_counter() - started:.1f}s "
                  f"after the dry run: {e}", file=sys.stderr)
            return 1
        print(f"  ✓ model answered {time.perf_counter() - started:.1f}s "
              f"after the dry run: {' '.join(text.split())[:60]}")
        if _env() != before:
            print("✗ the environment changed during the EAG plane",
                  file=sys.stderr)
            return 1
        from sahs.tools.sandbox import (human_bytes, live_switch_note,
                                        scan_ceiling)
        print(f"  live      {live_switch_note()} · scan ceiling "
              f"{human_bytes(scan_ceiling())}")
        print("both planes hold in one process: the chat's first dry run "
              "cannot reroute its model calls")
        return 0
    try:
        vertex = VertexConnection.from_env()
    except AuthError as e:
        print(f"✗ Vertex not configured: {e}", file=sys.stderr)
        return EXIT_ENV_AUTH
    print(f"  Vertex    {vertex.route()} · {vertex.endpoint}")
    from sahs.enrich.client import VertexClient
    client = VertexClient(vertex)
    started = time.perf_counter()
    try:
        client._token()                                    # noqa: SLF001
        text = client.generate('Return exactly this JSON: {"ok": true}',
                               max_output_tokens=512)
    except Exception as e:                                 # noqa: BLE001
        print(f"✗ Vertex failed {time.perf_counter() - started:.1f}s "
              f"after the dry run: {e}", file=sys.stderr)
        print("  the same process just finished a BigQuery call: if "
              "vertex_check.py passes on its own, a route is leaking "
              "between the planes", file=sys.stderr)
        return 1
    print(f"  ✓ model answered {time.perf_counter() - started:.1f}s "
          f"after the dry run: {' '.join(text.split())[:60]}")
    from sahs.tools.sandbox import (human_bytes, live_switch_note,
                                    scan_ceiling)
    print(f"  live      {live_switch_note()} · scan ceiling "
          f"{human_bytes(scan_ceiling())}")
    print("both planes hold in one process: the chat's first dry run "
          "cannot reroute its model calls")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
