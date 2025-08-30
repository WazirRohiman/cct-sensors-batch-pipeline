"""Data quality checks for counts, nulls, value ranges."""

from typing import Dict


def dq_summary(duckdb_path: str) -> Dict[str, int]:
    """Return a small summary (e.g., row counts per source/metric)."""
    # TODO: implement
    return {}
