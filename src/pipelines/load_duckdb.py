"""Load normalised Parquet files into DuckDB star schema with idempotent upserts."""

import os
from pathlib import Path
from typing import Dict

import duckdb
import pandas as pd


def _get_duckdb_connection(duckdb_path: str) -> duckdb.DuckDBPyConnection:
    """Establish connection to DuckDB file."""
    if not Path(duckdb_path).exists():
        raise FileNotFoundError(
            f"DuckDB file not found: {duckdb_path}. " "Run db_init.py first to create schema."
        )
    return duckdb.connect(duckdb_path)


def load_station_dimension(station_parquet_path: str, duckdb_path: str) -> Dict[str, int]:
    """Load station dimension from Parquet to DuckDB (idempotent).

    Args:
        station_parquet_path: Path to station_dim.parquet
        duckdb_path: Path to DuckDB database file

    Returns:
        Dictionary with load statistics
    """
    con = _get_duckdb_connection(duckdb_path)

    try:
        # Load station dimension from Parquet
        station_df = pd.read_parquet(station_parquet_path)

        print(f"📥 Loading {len(station_df)} stations from {station_parquet_path}")

        # Get existing station PKs to determine if UPDATE or INSERT
        existing_pks = set(
            con.execute("SELECT station_pk FROM dim_station").fetchdf()["station_pk"]
        )

        stations_updated = 0
        stations_inserted = 0

        for _, row in station_df.iterrows():
            if row["station_pk"] in existing_pks:
                # Update existing station
                con.execute(
                    """
                    UPDATE dim_station
                    SET station_code = ?,
                        station_name = ?,
                        location_type = ?,
                        description = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE station_pk = ?
                """,
                    [
                        row["station_code"],
                        row["station_name"],
                        row["location_type"],
                        row["description"],
                        row["station_pk"],
                    ],
                )
                stations_updated += 1
            else:
                # Insert new station
                con.execute(
                    """
                    INSERT INTO dim_station (
                        station_pk, station_code, station_name,
                        location_type, description
                    ) VALUES (?, ?, ?, ?, ?)
                """,
                    [
                        row["station_pk"],
                        row["station_code"],
                        row["station_name"],
                        row["location_type"],
                        row["description"],
                    ],
                )
                stations_inserted += 1

        print(
            f"✅ Station dimension loaded: {stations_inserted} inserted, {stations_updated} updated"
        )

        return {
            "stations_inserted": stations_inserted,
            "stations_updated": stations_updated,
            "total_stations": len(station_df),
        }

    finally:
        con.close()


def load_normalised_to_duckdb(parquet_path: str, duckdb_path: str) -> bool:
    """Load normalised Parquet file into DuckDB using optimal bulk operations.

    Uses DuckDB's native Parquet reading and bulk SQL operations for maximum performance.

    Idempotency Strategy:
    1. DELETE existing records by natural key using efficient subquery
    2. INSERT via SELECT from parquet file directly

    Performance Optimizations:
    - Uses read_parquet() function for direct file access
    - Single DELETE with IN subquery
    - Single bulk INSERT from SELECT
    - Transaction wrapping for consistency
    - Automatic NaN handling in SQL

    Args:
        parquet_path: Path to normalised Parquet file
        duckdb_path: Path to DuckDB database file

    Returns:
        True if successful, False otherwise
    """
    con = _get_duckdb_connection(duckdb_path)

    try:
        # Get row count directly from parquet
        row_count = con.execute(
            f"""
            SELECT COUNT(*) FROM read_parquet('{parquet_path}')
        """
        ).fetchone()[0]

        print(f"📥 Loading {row_count:,} measurements from {Path(parquet_path).name}")

        if row_count == 0:
            print("⚠️  Empty parquet file, skipping load")
            return True

        # Start transaction for atomicity
        con.execute("BEGIN TRANSACTION")

        try:
            # Step 1: Delete existing measurements by natural key (EFFICIENT)
            # Single DELETE using subquery - leverages DuckDB's optimized execution
            print("🗑️  Removing existing measurements for this file...")

            delete_result = con.execute(
                f"""
                DELETE FROM fact_measurement
                WHERE (timestamp, station_fk, metric, source) IN (
                    SELECT DISTINCT
                        datetime as timestamp,
                        station_pk as station_fk,
                        metric,
                        source
                    FROM read_parquet('{parquet_path}')
                )
            """
            )

            deleted_count = delete_result.rowcount or 0
            print(f"🗑️  Deleted {deleted_count:,} existing measurements")

            # Step 2: Bulk insert directly from parquet
            # Uses DuckDB's native bulk operations
            print(f"💾 Bulk inserting {row_count:,} measurements...")

            con.execute(
                f"""
                INSERT INTO fact_measurement (
                    measurement_pk,
                    station_fk,
                    timestamp,
                    metric,
                    value,
                    unit,
                    source,
                    quality_flag
                )
                SELECT
                    nextval('seq_measurement_pk'),
                    station_pk,
                    datetime,
                    metric,
                    CASE
                        WHEN isnan(value) OR isinf(value) THEN NULL
                        ELSE value
                    END,
                    unit,
                    source,
                    quality_flag
                FROM read_parquet('{parquet_path}')
            """
            )

            con.execute("COMMIT")
            print(f"✅ Loaded {row_count:,} measurements from {Path(parquet_path).name}")

        except Exception as e:
            con.execute("ROLLBACK")
            raise e

        # Quick verification (optional - could be removed for even better performance)
        total_count = con.execute("SELECT COUNT(*) FROM fact_measurement").fetchone()[0]
        print(f"📊 Total measurements in database: {total_count:,}")

        return True

    except Exception as e:
        print(f"❌ Error loading {parquet_path}: {e}")
        return False

    finally:
        con.close()


def load_all_normalised_ultra_fast(
    normalised_dir: str = "/opt/airflow/data/normalised",
    duckdb_path: str = "/opt/airflow/data/duckdb/cct_env.duckdb",
) -> Dict[str, any]:
    """Load all normalised files using DuckDB's ultra-fast bulk operations.

    This version leverages DuckDB's ability to read multiple parquet files
    at once using glob patterns for maximum performance.
    """
    normalised_path = Path(normalised_dir)

    if not normalised_path.exists():
        raise FileNotFoundError(f"normalised directory not found: {normalised_dir}")

    con = _get_duckdb_connection(duckdb_path)

    try:
        # Step 1: Load station dimension
        station_file = normalised_path / "station_dim.parquet"
        if not station_file.exists():
            raise FileNotFoundError(f"Station dimension not found: {station_file}")

        station_stats = load_station_dimension(str(station_file), duckdb_path)

        # Step 2: Ultra-fast bulk load of ALL fact files at once
        fact_pattern = str(normalised_path / "*_normalised.parquet")

        print(f"🚀 Ultra-fast bulk loading from pattern: {fact_pattern}")

        # Get total row count across all files
        total_rows = con.execute(
            f"""
            SELECT COUNT(*) FROM read_parquet('{fact_pattern}')
        """
        ).fetchone()[0]

        print(f"📊 Found {total_rows:,} total measurements across all files")

        if total_rows == 0:
            print("⚠️  No data found in normalised files")
            return {
                "station_stats": station_stats,
                "files_processed": 0,
                "successful_loads": 0,
                "failed_loads": 0,
                "total_measurements": 0,
                "failed_files": [],
            }

        # Start massive transaction
        con.execute("BEGIN TRANSACTION")

        try:
            # Clear existing data that matches any of the files
            print("🗑️  Clearing existing measurements...")
            delete_result = con.execute(
                f"""
                DELETE FROM fact_measurement
                WHERE (timestamp, station_fk, metric, source) IN (
                    SELECT DISTINCT
                        datetime as timestamp,
                        station_pk as station_fk,
                        metric,
                        source
                    FROM read_parquet('{fact_pattern}')
                )
            """
            )

            deleted_count = delete_result.rowcount or 0
            print(f"🗑️  Deleted {deleted_count:,} existing measurements")

            # Bulk insert ALL files at once
            print(f"💾 Bulk inserting {total_rows:,} measurements from ALL files...")

            con.execute(
                f"""
                INSERT INTO fact_measurement (
                    measurement_pk,
                    station_fk,
                    timestamp,
                    metric,
                    value,
                    unit,
                    source,
                    quality_flag
                )
                SELECT
                    nextval('seq_measurement_pk'),
                    station_pk,
                    datetime,
                    metric,
                    CASE
                        WHEN isnan(value) OR isinf(value) THEN NULL
                        ELSE value
                    END,
                    unit,
                    source,
                    quality_flag
                FROM read_parquet('{fact_pattern}')
            """
            )

            con.execute("COMMIT")
            print(f"✅ Successfully loaded {total_rows:,} measurements in single bulk operation!")

            # Count files processed
            fact_files = list(normalised_path.glob("*_normalised.parquet"))
            if "station_dim.parquet" in [f.name for f in fact_files]:
                fact_files = [f for f in fact_files if "station_dim" not in f.name]

            return {
                "station_stats": station_stats,
                "files_processed": len(fact_files),
                "successful_loads": len(fact_files),
                "failed_loads": 0,
                "total_measurements": total_rows,
                "failed_files": [],
            }

        except Exception as e:
            con.execute("ROLLBACK")
            print(f"❌ Ultra-fast load failed, falling back to file-by-file loading: {e}")
            # Fall back to original method
            return load_all_normalised_file_by_file(normalised_dir, duckdb_path)

    finally:
        con.close()


def load_all_normalised(
    normalised_dir: str = "/opt/airflow/data/normalised",
    duckdb_path: str = "/opt/airflow/data/duckdb/cct_env.duckdb",
) -> Dict[str, any]:
    """Load all normalised files - tries ultra-fast method first, falls back if needed."""
    try:
        return load_all_normalised_ultra_fast(normalised_dir, duckdb_path)
    except Exception as e:
        print(f"⚠️  Ultra-fast loading failed: {e}")
        print("🔄 Falling back to file-by-file loading...")
        return load_all_normalised_file_by_file(normalised_dir, duckdb_path)


def load_all_normalised_file_by_file(
    normalised_dir: str = "/opt/airflow/data/normalised",
    duckdb_path: str = "/opt/airflow/data/duckdb/cct_env.duckdb",
) -> Dict[str, any]:
    """Load all normalised Parquet files into DuckDB.

    Args:
        normalised_dir: Directory containing normalised Parquet files
        duckdb_path: Path to DuckDB database file

    Returns:
        Dictionary with load statistics
    """
    normalised_path = Path(normalised_dir)

    if not normalised_path.exists():
        raise FileNotFoundError(f"normalised directory not found: {normalised_dir}")

    # Step 1: Load station dimension
    station_file = normalised_path / "station_dim.parquet"
    if not station_file.exists():
        raise FileNotFoundError(f"Station dimension not found: {station_file}")

    station_stats = load_station_dimension(str(station_file), duckdb_path)

    # Step 2: Load all normalised fact files
    fact_files = [
        f
        for f in sorted(normalised_path.glob("*_normalised.parquet"))
        if "station_dim" not in f.name
    ]

    print(f"\n📦 Found {len(fact_files)} normalised files to load")

    successful_loads = 0
    failed_loads = 0
    failed_files = []

    for fact_file in fact_files:
        success = load_normalised_to_duckdb(str(fact_file), duckdb_path)
        if success:
            successful_loads += 1
        else:
            failed_loads += 1
            failed_files.append(fact_file.name)

    # Final statistics
    con = _get_duckdb_connection(duckdb_path)
    try:
        total_measurements = con.execute("SELECT COUNT(*) FROM fact_measurement").fetchone()[0]

        stats_by_source = con.execute(
            """
            SELECT source, COUNT(*) as count
            FROM fact_measurement
            GROUP BY source
            ORDER BY source
        """
        ).fetchdf()

        print("\n" + "=" * 80)
        print("LOAD SUMMARY")
        print("=" * 80)
        print("\nStation dimension:")
        print(f"  - Inserted: {station_stats['stations_inserted']}")
        print(f"  - Updated: {station_stats['stations_updated']}")
        print(f"  - Total: {station_stats['total_stations']}")

        print("\nFact measurements:")
        print(f"  - Files processed: {len(fact_files)}")
        print(f"  - Successful loads: {successful_loads}")
        print(f"  - Failed loads: {failed_loads}")
        print(f"  - Total measurements: {total_measurements:,}")

        print("\nBy source:")
        for _, row in stats_by_source.iterrows():
            print(f"  - {row['source']}: {row['count']:,}")

        if failed_files:
            print("\n⚠️  Failed files:")
            for file in failed_files:
                print(f"    - {file}")

        return {
            "station_stats": station_stats,
            "files_processed": len(fact_files),
            "successful_loads": successful_loads,
            "failed_loads": failed_loads,
            "total_measurements": total_measurements,
            "failed_files": failed_files,
        }

    finally:
        con.close()


def verify_data_integrity(duckdb_path: str) -> Dict[str, any]:
    """Run data integrity checks on loaded data.

    Args:
        duckdb_path: Path to DuckDB database file

    Returns:
        Dictionary with integrity check results
    """
    con = _get_duckdb_connection(duckdb_path)

    try:
        print("\n" + "=" * 80)
        print("DATA INTEGRITY CHECKS")
        print("=" * 80)

        # Check 1: Orphaned measurements (station_fk not in dim_station)
        orphans = con.execute(
            """
            SELECT COUNT(*) as orphan_count
            FROM fact_measurement f
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_station s
                WHERE s.station_pk = f.station_fk
            )
        """
        ).fetchone()[0]

        print(f"\n1. Orphaned measurements (missing station): {orphans}")
        if orphans > 0:
            print("   ⚠️  WARNING: Some measurements reference non-existent stations")

        # Check 2: Duplicate natural keys
        duplicates = con.execute(
            """
            SELECT COUNT(*) as dup_count FROM (
                SELECT timestamp, station_fk, metric, source, COUNT(*) as cnt
                FROM fact_measurement
                GROUP BY timestamp, station_fk, metric, source
                HAVING COUNT(*) > 1
            )
        """
        ).fetchone()[0]

        print(f"2. Duplicate natural keys: {duplicates}")
        if duplicates > 0:
            print("   ⚠️  WARNING: Duplicate measurements detected (re-run load?)")

        # Check 3: Null values in required fields
        null_checks = con.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE timestamp IS NULL) as null_timestamps,
                COUNT(*) FILTER (WHERE station_fk IS NULL) as null_stations,
                COUNT(*) FILTER (WHERE metric IS NULL) as null_metrics,
                COUNT(*) FILTER (WHERE source IS NULL) as null_sources
            FROM fact_measurement
        """
        ).fetchone()

        print("3. Null values in required fields:")
        print(f"   - timestamp: {null_checks[0]}")
        print(f"   - station_fk: {null_checks[1]}")
        print(f"   - metric: {null_checks[2]}")
        print(f"   - source: {null_checks[3]}")

        # Check 4: Value statistics
        value_stats = con.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE value IS NULL) as null_values,
                COUNT(*) FILTER (WHERE value IS NOT NULL) as valid_values,
                COUNT(*) FILTER (WHERE quality_flag = 'NODATA') as nodata_flags,
                COUNT(*) FILTER (WHERE quality_flag = 'VALID') as valid_flags
            FROM fact_measurement
        """
        ).fetchone()

        print("\n4. Value statistics:")
        print(f"   - NULL values: {value_stats[0]:,}")
        print(f"   - Valid values: {value_stats[1]:,}")
        print(f"   - NODATA flags: {value_stats[2]:,}")
        print(f"   - VALID flags: {value_stats[3]:,}")

        # Check 5: Temporal coverage
        coverage = con.execute(
            """
            SELECT
                source,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                COUNT(DISTINCT DATE_TRUNC('day', timestamp)) as days_covered
            FROM fact_measurement
            GROUP BY source
            ORDER BY source
        """
        ).fetchdf()

        print("\n5. Temporal coverage:")
        for _, row in coverage.iterrows():
            print(
                f"   - {row['source']}: {row['earliest']} to {row['latest']} "
                f"({row['days_covered']} days)"
            )

        integrity_ok = (
            orphans == 0
            and duplicates == 0
            and null_checks[0] == 0
            and null_checks[1] == 0
            and null_checks[2] == 0
            and null_checks[3] == 0
        )

        if integrity_ok:
            print("\n✅ All integrity checks PASSED")
        else:
            print("\n⚠️  Some integrity issues detected - review above")

        return {
            "orphan_measurements": orphans,
            "duplicate_keys": duplicates,
            "null_timestamps": null_checks[0],
            "null_stations": null_checks[1],
            "null_metrics": null_checks[2],
            "null_sources": null_checks[3],
            "integrity_ok": integrity_ok,
        }

    finally:
        con.close()


if __name__ == "__main__":
    """Run loading pipeline when executed directly."""
    import sys

    duckdb_path = os.environ.get("DUCKDB_PATH", "/opt/airflow/data/duckdb/cct_env.duckdb")
    normalised_dir = os.environ.get("NORMALISED_DIR", "/opt/airflow/data/normalised")

    print("🚀 Starting DuckDB loading pipeline")
    print(f"   DuckDB: {duckdb_path}")
    print(f"   Data: {normalised_dir}")
    try:
        # Load all data
        stats = load_all_normalised(normalised_dir, duckdb_path)

        # Verify integrity
        integrity = verify_data_integrity(duckdb_path)

        # Exit with appropriate code
        if stats["failed_loads"] > 0 or not integrity["integrity_ok"]:
            print("\n❌ Load completed with errors")
            sys.exit(1)
        else:
            print("\n✅ Load completed successfully")
            sys.exit(0)

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
