"""Normalization utilities to transform wide sensor data to tall schema."""

from typing import Optional


def normalize_to_tall(parquet_path: str, dest_dir: str) -> Optional[str]:
    """Read staged Parquet and produce normalized tall-format Parquet.

    Returns output path or None on failure.
    """
    # TODO: implement transformation and unit/timestamp standardization
    return None
