"""Debug — dump the inspector output for the configured table."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from semantic_graph.config import load_config
from semantic_graph.graph import load_cached_graph
from semantic_graph.tools import inspect_table


def main() -> int:
    cfg = load_config()
    # Warm the lazy global the tool uses
    _ = load_cached_graph(cfg)
    print(json.dumps(inspect_table(cfg.table_name), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
