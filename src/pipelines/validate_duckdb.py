"""DuckDB database validation and summary reporting."""

import os
from pathlib import Path
from typing import Any, Dict

import duckdb


def validate_duckdb_state(duckdb_path: str) -> Dict[str, Any]:
    """Validate DuckDB database state and generate summary report.

    Args:
        duckdb_path: Path to DuckDB database file

    Returns:
        Dictionary with validation results and database summary
    """
    if not Path(duckdb_path).exists():
        raise FileNotFoundError(f"DuckDB file not found: {duckdb_path}")

    con = duckdb.connect(duckdb_path)

    try:
        print("🔍 Validating DuckDB database state...")

        # Check file size
        db_size = Path(duckdb_path).stat().st_size
        db_size_mb = db_size / (1024 * 1024)
        print(f"📦 Database size: {db_size_mb:.1f} MB")

        # Get table counts
        tables = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
        """
        ).fetchall()

        table_info = {}
        print(f"\n📊 Database contains {len(tables)} tables:")
        for table in tables:
            table_name = table[0]
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            table_info[table_name] = count
            print(f"   - {table_name}: {count:,} rows")

        # Get view counts
        views = con.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'main'
            ORDER BY table_name
        """
        ).fetchall()

        custom_views = [v[0] for v in views if v[0].startswith("v_")]
        print(f"\n📊 Database contains {len(custom_views)} custom views:")
        for view in custom_views:
            print(f"   - {view}")

        # Get index counts
        indexes = con.execute(
            """
            SELECT COUNT(*)
            FROM duckdb_indexes()
            WHERE database_name = 'main'
        """
        ).fetchone()[0]

        print(f"\n🔍 Database contains {indexes} indexes")

        # Sample query test
        print("\n🧪 Running sample query...")
        sample = con.execute(
            """
            SELECT
                s.station_name,
                COUNT(*) as measurement_count
            FROM fact_measurement f
            JOIN dim_station s ON f.station_fk = s.station_pk
            GROUP BY s.station_name
            ORDER BY measurement_count DESC
            LIMIT 5
        """
        ).fetchdf()

        print("\nTop 5 stations by measurement count:")
        print(sample.to_string(index=False))

        # Data quality summary
        data_quality = con.execute(
            """
            SELECT
                COUNT(*) as total_measurements,
                COUNT(CASE WHEN value IS NOT NULL THEN 1 END) as valid_values,
                COUNT(CASE WHEN value IS NULL THEN 1 END) as null_values,
                COUNT(CASE WHEN quality_flag = 'VALID' THEN 1 END) as valid_flags,
                COUNT(CASE WHEN quality_flag = 'NODATA' THEN 1 END) as nodata_flags
            FROM fact_measurement
        """
        ).fetchone()

        total_measurements = data_quality[0]
        valid_values = data_quality[1]
        null_values = data_quality[2]
        valid_flags = data_quality[3]
        nodata_flags = data_quality[4]

        print("\n📈 Data Quality Summary:")
        print(f"   - Total measurements: {total_measurements:,}")
        print(f"   - Valid values: {valid_values:,} ({valid_values/total_measurements*100:.1f}%)")
        print(f"   - NULL values: {null_values:,} ({null_values/total_measurements*100:.1f}%)")
        print(f"   - VALID flags: {valid_flags:,}")
        print(f"   - NODATA flags: {nodata_flags:,}")

        print("\n✅ DuckDB validation complete")

        return {
            "database_size_mb": db_size_mb,
            "table_count": len(tables),
            "view_count": len(custom_views),
            "index_count": indexes,
            "table_info": table_info,
            "total_measurements": total_measurements,
            "valid_values": valid_values,
            "null_values": null_values,
            "top_stations": sample.to_dict("records"),
            "validation_success": True,
        }

    except Exception as e:
        print(f"❌ DuckDB validation failed: {e}")
        return {"validation_success": False, "error": str(e)}

    finally:
        con.close()


if __name__ == "__main__":
    """Run validation when executed directly."""
    duckdb_path = os.environ.get("DUCKDB_PATH", "/opt/airflow/data/duckdb/cct_env.duckdb")

    print(f"🚀 Validating DuckDB database at: {duckdb_path}")

    try:
        results = validate_duckdb_state(duckdb_path)

        if results["validation_success"]:
            print("\n🎯 VALIDATION SUCCESS")
            exit(0)
        else:
            print(f"\n❌ VALIDATION FAILED: {results.get('error', 'Unknown error')}")
            exit(1)

    except Exception as e:
        print(f"\n💥 Fatal validation error: {e}")
        exit(1)
