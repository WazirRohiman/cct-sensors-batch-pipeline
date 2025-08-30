import os

import duckdb


def ensure_dirs(path: str) -> None:
    """
    Ensure the directory portion of the given filesystem path exists.
    
    Creates the parent directory (and any missing intermediate directories) for `path` using `os.makedirs(..., exist_ok=True)`. If the directory already exists this is a no-op.
    
    Parameters:
        path (str): Filesystem path (file or directory); the directory portion of this path will be created if missing.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the required database schema (tables) for the application.
    
    Executes SQL to ensure two tables exist:
    - dim_station: station_id (TEXT PRIMARY KEY), name (TEXT), latitude (DOUBLE), longitude (DOUBLE), elevation (DOUBLE).
    - fact_measurement: station_id (TEXT), ts (TIMESTAMP), metric (TEXT), value (DOUBLE), unit (TEXT), source (TEXT), with a composite primary key (station_id, ts, metric).
    
    The operation is idempotent (uses IF NOT EXISTS).
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_station (
            station_id TEXT PRIMARY KEY,
            name TEXT,
            latitude DOUBLE,
            longitude DOUBLE,
            elevation DOUBLE
        );

        CREATE TABLE IF NOT EXISTS fact_measurement (
            station_id TEXT,
            ts TIMESTAMP,
            metric TEXT,
            value DOUBLE,
            unit TEXT,
            source TEXT,
            PRIMARY KEY (station_id, ts, metric)
        );
        """
    )


def main() -> None:
    """
    Initialize a DuckDB database file and ensure required schema exists.
    
    Reads the DUCKDB_PATH environment variable (defaults to "./data/duckdb/cct_env.duckdb"), creates any missing parent directories, opens a DuckDB connection at that path, and runs schema creation (via create_schema). Prints a confirmation message and always closes the connection when finished.
    
    No return value.
    """
    duckdb_path = os.environ.get("DUCKDB_PATH", "./data/duckdb/cct_env.duckdb")
    ensure_dirs(duckdb_path)
    con = duckdb.connect(duckdb_path)
    try:
        create_schema(con)
        # Future: seed small lookup tables if needed
        print(f"Initialized DuckDB at {duckdb_path}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
