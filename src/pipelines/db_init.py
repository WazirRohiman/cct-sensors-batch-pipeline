"""DuckDB database initialization with surrogate key schema.

Creates star schema optimized for environmental sensor data:
- dim_station: Station master data with surrogate keys
- fact_measurement: Time-series measurements with foreign keys
- Indexes for query performance
- Initial station data population
"""

import os
from typing import Any, Dict

import duckdb
import yaml


def ensure_dirs(path: str) -> None:
    """Ensure directory structure exists for DuckDB file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)


def load_station_data() -> Dict[str, Any]:
    """Load station configuration data."""
    config_path = "/opt/airflow/src/configs/station_mapping.yaml"

    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"⚠️  Station mapping config not found: {config_path}")
        return {}


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create optimized star schema with surrogate keys."""

    # Create dimension table for stations
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_station (
            station_pk INTEGER PRIMARY KEY,
            station_code VARCHAR(10) UNIQUE NOT NULL,
            station_name VARCHAR(100) NOT NULL,
            location_type VARCHAR(20),
            latitude DECIMAL(10,6),
            longitude DECIMAL(10,6),
            elevation DECIMAL(8,2),
            description TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    )

    # Create fact table for measurements
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_measurement (
            measurement_pk BIGINT PRIMARY KEY,
            station_fk INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            metric VARCHAR(50) NOT NULL,
            value DECIMAL(12,4),
            unit VARCHAR(20),
            source VARCHAR(20),
            quality_flag VARCHAR(10) DEFAULT 'VALID',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            -- Foreign key reference (enforced by application, not DB constraint)
            -- FOREIGN KEY (station_fk) REFERENCES dim_station(station_pk)
        );
    """
    )

    # Create indexes for performance
    con.execute(
        """
        -- Station lookup index
        CREATE INDEX IF NOT EXISTS idx_station_code ON dim_station(station_code);

        -- Time-series query indexes
        CREATE INDEX IF NOT EXISTS idx_measurement_station_time
            ON fact_measurement(station_fk, timestamp);
        CREATE INDEX IF NOT EXISTS idx_measurement_metric
            ON fact_measurement(metric);
        CREATE INDEX IF NOT EXISTS idx_measurement_timestamp
            ON fact_measurement(timestamp);

        -- Combined index for common queries
        CREATE INDEX IF NOT EXISTS idx_measurement_station_metric_time
            ON fact_measurement(station_fk, metric, timestamp);
    """
    )

    # Create sequence for measurement primary keys
    con.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS seq_measurement_pk START 1;
    """
    )

    print("✅ Star schema created successfully")


def populate_station_data(con: duckdb.DuckDBPyConnection) -> None:
    """Populate dim_station with initial station data."""

    station_config = load_station_data()

    if not station_config or "wind_stations" not in station_config:
        print("⚠️  No station configuration found, skipping station population")
        return

    # Clear existing data for fresh start
    con.execute("DELETE FROM dim_station;")

    # Insert station data
    stations = station_config["wind_stations"]["station_mappings"]

    for station in stations:
        con.execute(
            """
            INSERT OR REPLACE INTO dim_station (
                station_pk, station_code, station_name, location_type, description
            ) VALUES (?, ?, ?, ?, ?)
        """,
            [
                station["station_pk"],
                station["station_code"],
                station["station_name"],
                station["location_type"],
                station["description"],
            ],
        )

    # Verify insertion
    result = con.execute("SELECT COUNT(*) FROM dim_station").fetchone()
    station_count = result[0] if result else 0

    print(f"✅ Populated {station_count} stations in dim_station")

    # Show sample data
    sample_stations = con.execute(
        """
        SELECT station_pk, station_code, station_name
        FROM dim_station
        ORDER BY station_pk
        LIMIT 5
    """
    ).fetchall()

    print("Sample stations:")
    for station in sample_stations:
        print(f"  {station[0]}: {station[1]} - {station[2]}")


def create_views(con: duckdb.DuckDBPyConnection) -> None:
    """Create useful views for data analysis."""

    # View joining station info with measurements
    con.execute(
        """
        CREATE OR REPLACE VIEW v_measurements AS
        SELECT
            m.measurement_pk,
            s.station_code,
            s.station_name,
            m.timestamp,
            m.metric,
            m.value,
            m.unit,
            m.source,
            m.quality_flag,
            m.created_at
        FROM fact_measurement m
        JOIN dim_station s ON m.station_fk = s.station_pk
        ORDER BY m.timestamp DESC;
    """
    )

    # View for wind data specifically
    con.execute(
        """
        CREATE OR REPLACE VIEW v_wind_data AS
        SELECT
            s.station_code,
            s.station_name,
            m.timestamp,
            MAX(CASE WHEN m.metric = 'wind_direction' THEN m.value END) as wind_direction_deg,
            MAX(CASE WHEN m.metric = 'wind_speed' THEN m.value END) as wind_speed_ms
        FROM fact_measurement m
        JOIN dim_station s ON m.station_fk = s.station_pk
        WHERE m.source = 'wind'
        GROUP BY s.station_code, s.station_name, m.timestamp
        ORDER BY m.timestamp DESC;
    """
    )

    # Data quality summary view
    con.execute(
        """
        CREATE OR REPLACE VIEW v_data_quality_summary AS
        SELECT
            s.station_name,
            m.metric,
            COUNT(*) as total_records,
            COUNT(CASE WHEN m.quality_flag = 'VALID' THEN 1 END) as valid_records,
            COUNT(CASE WHEN m.value IS NULL THEN 1 END) as null_values,
            MIN(m.timestamp) as earliest_record,
            MAX(m.timestamp) as latest_record
        FROM fact_measurement m
        JOIN dim_station s ON m.station_fk = s.station_pk
        GROUP BY s.station_name, m.metric
        ORDER BY s.station_name, m.metric;
    """
    )

    print("✅ Created analytical views")


def verify_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Verify schema creation and show summary."""

    # Check tables
    tables = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """
    ).fetchall()

    print(f"📊 Database contains {len(tables)} tables:")
    for table in tables:
        print(f"  - {table[0]}")

    # Check views
    views = con.execute(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'main'
        ORDER BY table_name
    """
    ).fetchall()

    print(f"📊 Database contains {len(views)} views:")
    for view in views:
        print(f"  - {view[0]}")

    # Check indexes
    indexes = con.execute(
        """
        SELECT index_name, table_name
        FROM duckdb_indexes()
        WHERE database_name = 'main'
        ORDER BY table_name, index_name
    """
    ).fetchall()

    print(f"🔍 Database contains {len(indexes)} indexes:")
    for index in indexes:
        print(f"  - {index[0]} on {index[1]}")


def main() -> None:
    """Initialize DuckDB with complete star schema."""

    duckdb_path = os.environ.get("DUCKDB_PATH", "/opt/airflow/data/duckdb/cct_env.duckdb")

    print(f"🔄 Initializing DuckDB at {duckdb_path}")

    ensure_dirs(duckdb_path)
    con = duckdb.connect(duckdb_path)

    try:
        # Create schema
        create_schema(con)

        # Populate station data
        populate_station_data(con)

        # Create analytical views
        create_views(con)

        # Verify everything was created
        verify_schema(con)

        print(f"✅ DuckDB initialization complete: {duckdb_path}")

    except Exception as e:
        print(f"❌ Error initializing DuckDB: {e}")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
