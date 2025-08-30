"""Staging utilities (Excel/ZIP to Parquet/CSV)."""

from typing import Optional


def stage_file_to_parquet(src_path: str, dest_dir: str) -> Optional[str]:
    """Convert supported inputs (.xlsx/.xls/.csv) into columnar Parquet file in dest_dir.

    Returns staged Parquet path or None on failure.
    """
    # TODO: implement with pandas/pyarrow or duckdb.read_xlsx
    return None
