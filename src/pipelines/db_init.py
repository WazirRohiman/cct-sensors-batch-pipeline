import os

import duckdb


def ensure_dirs(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
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
