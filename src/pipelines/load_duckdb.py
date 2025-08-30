"""Load normalized data into DuckDB with idempotency."""


def load_normalized_to_duckdb(parquet_path: str, duckdb_path: str) -> bool:
    """Load Parquet into DuckDB fact_measurement using an upsert pattern.

    Returns True on success.
    """
    # TODO: implement with duckdb SQL (DELETE + INSERT) or MERGE pattern
    return False
