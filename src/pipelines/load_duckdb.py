"""Load normalized data into DuckDB with idempotency."""


def load_normalized_to_duckdb(parquet_path: str, duckdb_path: str) -> bool:
    """
    Load normalized Parquet data into DuckDB's `fact_measurement` table using an idempotent upsert.
    
    Intended to read rows from the Parquet file at `parquet_path` and apply them to the DuckDB database at `duckdb_path`
    using an upsert pattern (e.g., MERGE or DELETE + INSERT) so repeated runs do not create duplicates.
    
    Parameters:
        parquet_path (str): Filesystem path to the source Parquet file containing normalized measurement records.
        duckdb_path (str): Filesystem path to the DuckDB database file.
    
    Returns:
        bool: True if the upsert completed successfully, False otherwise.
    
    Note:
        This function is currently a placeholder and always returns False until the upsert logic is implemented.
    """
    # TODO: implement with duckdb SQL (DELETE + INSERT) or MERGE pattern
    return False
