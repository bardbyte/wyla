"""Real-data loaders for Synapse.

Each loader is a plain function that mirrors the synthetic generator's
output JSON layout — so `build_graph_from_sources(dir)` works identically
on real OR synthetic data. The loader's job is to read its source's
native format (BQ INFORMATION_SCHEMA CSVs, MDM JSON API responses, GHE
LookML files, …) and emit the same JSON files the synthetic generator
produces.

Loader contract (every loader follows this):

    def load_X_for_table(
        table_id: str,
        *,
        source_dir: Path,        # where to read raw inputs (BQ console CSVs, etc.)
        out_dir: Path,           # where to write the canonical JSON the builder reads
        force_refresh: bool = False,
        dry_run: bool = False,
    ) -> LoadResult:
        ...

`LoadResult` is a typed Pydantic model with status + path + diagnostics.
Same shape regardless of source. Trivially wrappable as an ADK FunctionTool.
"""

from synapse.loaders.bq_loader import load_bq_for_table
from synapse.loaders.lumi_loader import load_lumi_for_table
from synapse.loaders.mdm_loader import load_mdm_for_table
from synapse.loaders.types import LoadResult

__all__ = [
    "load_bq_for_table",
    "load_lumi_for_table",
    "load_mdm_for_table",
    "LoadResult",
]
