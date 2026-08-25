#!/usr/bin/env python3
"""Serve the eight Meridian tools over MCP (stdio).

    python scripts/serve_mcp.py --builds builds/

Resolves the build through builds/CURRENT (E4). Requires the ``mcp``
extra: ``pip install -e .[mcp]``. Exit 3 on missing build/env.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sahs.util.console import EXIT_ENV_AUTH                       # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="serve_mcp.py")
    parser.add_argument("--builds", default="builds")
    args = parser.parse_args(argv)
    try:
        from sahs.tools.mcp_server import build_server
        server = build_server(Path(args.builds))
    except FileNotFoundError as e:
        print(f"no build: {e}", file=sys.stderr)
        return EXIT_ENV_AUTH
    except ImportError as e:
        print(f"mcp extra not installed ({e}) — "
              "pip install -e .[mcp]", file=sys.stderr)
        return EXIT_ENV_AUTH
    server.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
