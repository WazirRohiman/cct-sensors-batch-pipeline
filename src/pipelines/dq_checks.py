"""Data quality checks for sensor measurements.

Provides comprehensive data quality validation including:
- Row count validation
- Null value analysis
- Range checks per metric type
- Temporal coverage verification
- Quality flag distribution
- Cross-dataset consistency checks
"""

from typing import Dict

import duckdb


def _get_connection(duckdb_path: str) -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection."""
    return duckdb.connect(duckdb_path)


def dq_summary(duckdb_path: str) -> Dict[str, int]:
    """Return comprehensive data quality summary.

    Args:
        duckdb_path: Path to DuckDB database file

    Returns:
        Dictionary with DQ metrics
    """
    con = _get_connection(duckdb_path)

    try:
        # Basic counts
        total_rows = con.execute("SELECT COUNT(*) FROM fact_measurement").fetchone()[0]
        total_stations = con.execute("SELECT COUNT(*) FROM dim_station").fetchone()[0]

        # Quality flag distribution
        quality_dist = con.execute(
            """
            SELECT
                quality_flag,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM fact_measurement
            GROUP BY quality_flag
            ORDER BY count DESC
        """
        ).fetchdf()

        # Source distribution
        source_dist = con.execute(
            """
            SELECT source, COUNT(*) as count
            FROM fact_measurement
            GROUP BY source
            ORDER BY count DESC
        """
        ).fetchdf()

        # Null value analysis
        null_stats = con.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE value IS NULL) as null_values,
                COUNT(*) FILTER (WHERE value IS NOT NULL) as valid_values,
                CASE
                    WHEN COUNT(*) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(*) FILTER (WHERE value IS NULL) * 100.0 / COUNT(*),
                        2
                    )
                END as null_percentage
            FROM fact_measurement
        """
        ).fetchone()

        return {
            "total_measurements": total_rows,
            "total_stations": total_stations,
            "quality_distribution": quality_dist.to_dict("records"),
            "source_distribution": source_dist.to_dict("records"),
            "null_values": null_stats[0],
            "valid_values": null_stats[1],
            "null_percentage": null_stats[2],
        }

    finally:
        con.close()


def check_row_counts(duckdb_path: str) -> Dict[str, any]:
    """Validate row counts per source and metric.

    Args:
        duckdb_path: Path to DuckDB database

    Returns:
        Dictionary with row count statistics
    """
    con = _get_connection(duckdb_path)

    try:
        print("\n" + "=" * 80)
        print("DATA QUALITY CHECK: ROW COUNTS")
        print("=" * 80)

        # Overall counts
        total = con.execute("SELECT COUNT(*) FROM fact_measurement").fetchone()[0]
        print(f"\nTotal measurements: {total:,}")

        # By source
        by_source = con.execute(
            """
            SELECT source, COUNT(*) as count
            FROM fact_measurement
            GROUP BY source
            ORDER BY source
        """
        ).fetchdf()

        print("\nBy source:")
        for _, row in by_source.iterrows():
            pct = (row["count"] / total) * 100
            print(f"  - {row['source']}: {row['count']:,} ({pct:.1f}%)")

        # By metric
        by_metric = con.execute(
            """
            SELECT
                metric,
                COUNT(*) as count,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM fact_measurement
            GROUP BY metric
            ORDER BY count DESC
        """
        ).fetchdf()

        print("\nBy metric (top 10):")
        for _, row in by_metric.head(10).iterrows():
            print(f"  - {row['metric']}: {row['count']:,} ({row['earliest']} to {row['latest']})")

        # By station
        by_station = con.execute(
            """
            SELECT
                s.station_name,
                COUNT(*) as count
            FROM fact_measurement f
            JOIN dim_station s ON f.station_fk = s.station_pk
            GROUP BY s.station_name
            ORDER BY count DESC
        """
        ).fetchdf()

        print("\nBy station:")
        for _, row in by_station.iterrows():
            pct = (row["count"] / total) * 100
            print(f"  - {row['station_name']}: {row['count']:,} ({pct:.1f}%)")

        return {
            "total": total,
            "by_source": by_source.to_dict("records"),
            "by_metric": by_metric.to_dict("records"),
            "by_station": by_station.to_dict("records"),
        }

    finally:
        con.close()


def check_null_rates(duckdb_path: str) -> Dict[str, any]:
    """Analyze null value rates per metric and station.

    Args:
        duckdb_path: Path to DuckDB database

    Returns:
        Dictionary with null rate statistics
    """
    con = _get_connection(duckdb_path)

    try:
        print("\n" + "=" * 80)
        print("DATA QUALITY CHECK: NULL RATES")
        print("=" * 80)

        # Overall null rate
        overall = con.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE value IS NULL) as nulls,
                CASE
                    WHEN COUNT(*) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(*) FILTER (WHERE value IS NULL) * 100.0 / COUNT(*),
                        2
                    )
                END as null_pct
            FROM fact_measurement
        """
        ).fetchone()

        print("\nOverall:")
        print(f"  Total: {overall[0]:,}")
        print(f"  Nulls: {overall[1]:,} ({overall[2]}%)")

        # By metric
        by_metric = con.execute(
            """
            SELECT
                metric,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE value IS NULL) as nulls,
                CASE
                    WHEN COUNT(*) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(*) FILTER (WHERE value IS NULL) * 100.0 / COUNT(*),
                        2
                    )
                END as null_pct
            FROM fact_measurement
            GROUP BY metric
            ORDER BY null_pct DESC
        """
        ).fetchdf()

        print("\nBy metric:")
        for _, row in by_metric.iterrows():
            print(
                f"  - {row['metric']}: {row['nulls']:,}/{row['total']:,} ({row['null_pct']}% null)"
            )

        # By station
        by_station = con.execute(
            """
            SELECT
                s.station_name,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE f.value IS NULL) as nulls,
                CASE
                    WHEN COUNT(*) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(*) FILTER (WHERE f.value IS NULL) * 100.0 / COUNT(*),
                        2
                    )
                END as null_pct
            FROM fact_measurement f
            JOIN dim_station s ON f.station_fk = s.station_pk
            GROUP BY s.station_name
            ORDER BY null_pct DESC
        """
        ).fetchdf()

        print("\nBy station:")
        for _, row in by_station.iterrows():
            print(
                f"  - {row['station_name']}: {row['nulls']:,}/{row['total']:,} "
                f"({row['null_pct']}% null)"
            )

        # Identify high null rate issues (>50%)
        high_null_metrics = by_metric[by_metric["null_pct"] > 50.0]
        if not high_null_metrics.empty:
            print("\n⚠️  HIGH NULL RATE METRICS (>50%):")
            for _, row in high_null_metrics.iterrows():
                print(f"  - {row['metric']}: {row['null_pct']}%")

        return {
            "overall_null_pct": overall[2],
            "by_metric": by_metric.to_dict("records"),
            "by_station": by_station.to_dict("records"),
            "high_null_metrics": (
                high_null_metrics["metric"].tolist() if not high_null_metrics.empty else []
            ),
        }

    finally:
        con.close()


def check_value_ranges(duckdb_path: str) -> Dict[str, any]:
    """Validate value ranges for known metric types.

    Args:
        duckdb_path: Path to DuckDB database

    Returns:
        Dictionary with range validation results
    """
    con = _get_connection(duckdb_path)

    try:
        print("\n" + "=" * 80)
        print("DATA QUALITY CHECK: VALUE RANGES")
        print("=" * 80)

        # Expected ranges per metric
        expected_ranges = {
            "wind_speed": (0, 50),  # m/s
            "wind_direction": (0, 360),  # degrees
            "no2": (0, 1000),  # μg/m³
            "o3": (0, 500),  # μg/m³
            "pm10": (0, 2000),  # μg/m³
            "pm25": (0, 1000),  # μg/m³
            "so2": (0, 1000),  # μg/m³
        }

        range_results = []

        for metric, (min_exp, max_exp) in expected_ranges.items():
            stats = con.execute(
                f"""
                SELECT
                    '{metric}' as metric,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE value IS NOT NULL) as non_null,
                    MIN(value) as min_val,
                    MAX(value) as max_val,
                    ROUND(AVG(value), 2) as avg_val,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_val,
                    COUNT(*) FILTER (WHERE value < {min_exp}) as below_min,
                    COUNT(*) FILTER (WHERE value > {max_exp}) as above_max
                FROM fact_measurement
                WHERE metric = '{metric}'
            """
            ).fetchone()

            if stats and stats[1] > 0:  # Has data for this metric
                print(f"\n{metric.upper()}:")
                print(f"  Total: {stats[1]:,} | Non-null: {stats[2]:,}")
                print(f"  Range: {stats[3]:.2f} - {stats[4]:.2f} (expected: {min_exp}-{max_exp})")
                print(f"  Avg: {stats[5]:.2f} | Median: {stats[6]:.2f}")

                if stats[7] > 0:
                    print(f"  ⚠️  Below min ({min_exp}): {stats[7]:,} values")
                if stats[8] > 0:
                    print(f"  ⚠️  Above max ({max_exp}): {stats[8]:,} values")

                range_results.append(
                    {
                        "metric": metric,
                        "total": stats[1],
                        "non_null": stats[2],
                        "min": stats[3],
                        "max": stats[4],
                        "avg": stats[5],
                        "median": stats[6],
                        "below_min": stats[7],
                        "above_max": stats[8],
                        "expected_min": min_exp,
                        "expected_max": max_exp,
                    }
                )

        # Identify outliers
        outliers_exist = any(r["below_min"] > 0 or r["above_max"] > 0 for r in range_results)
        if outliers_exist:
            print("\n⚠️  Outliers detected - review data quality")
        else:
            print("\n✅ All values within expected ranges")

        return {
            "range_checks": range_results,
            "outliers_exist": outliers_exist,
        }

    finally:
        con.close()


def check_temporal_coverage(duckdb_path: str) -> Dict[str, any]:
    """Analyze temporal coverage and gaps.

    Args:
        duckdb_path: Path to DuckDB database

    Returns:
        Dictionary with temporal coverage statistics
    """
    con = _get_connection(duckdb_path)

    try:
        print("\n" + "=" * 80)
        print("DATA QUALITY CHECK: TEMPORAL COVERAGE")
        print("=" * 80)

        # Overall coverage
        coverage = con.execute(
            """
            SELECT
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                COUNT(DISTINCT DATE_TRUNC('day', timestamp)) as days_covered,
                COUNT(DISTINCT DATE_TRUNC('hour', timestamp)) as hours_covered
            FROM fact_measurement
        """
        ).fetchone()

        print("\nOverall coverage:")
        print(f"  Earliest: {coverage[0]}")
        print(f"  Latest: {coverage[1]}")
        print(f"  Days covered: {coverage[2]:,}")
        print(f"  Hours covered: {coverage[3]:,}")

        # By source
        by_source = con.execute(
            """
            SELECT
                source,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                COUNT(DISTINCT DATE_TRUNC('day', timestamp)) as days
            FROM fact_measurement
            GROUP BY source
            ORDER BY source
        """
        ).fetchdf()

        print("\nBy source:")
        for _, row in by_source.iterrows():
            print(f"  - {row['source']}: {row['earliest']} to {row['latest']} ({row['days']} days)")

        # By metric
        by_metric = con.execute(
            """
            SELECT
                metric,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                COUNT(DISTINCT DATE_TRUNC('day', timestamp)) as days
            FROM fact_measurement
            GROUP BY metric
            ORDER BY metric
        """
        ).fetchdf()

        print("\nBy metric:")
        for _, row in by_metric.iterrows():
            print(f"  - {row['metric']}: {row['earliest']} to {row['latest']} ({row['days']} days)")

        return {
            "earliest": str(coverage[0]),
            "latest": str(coverage[1]),
            "days_covered": coverage[2],
            "hours_covered": coverage[3],
            "by_source": by_source.to_dict("records"),
            "by_metric": by_metric.to_dict("records"),
        }

    finally:
        con.close()


def check_quality_flags(duckdb_path: str) -> Dict[str, any]:
    """Analyze quality flag distribution.

    Args:
        duckdb_path: Path to DuckDB database

    Returns:
        Dictionary with quality flag statistics
    """
    con = _get_connection(duckdb_path)

    try:
        print("\n" + "=" * 80)
        print("DATA QUALITY CHECK: QUALITY FLAGS")
        print("=" * 80)

        # Overall distribution
        overall = con.execute(
            """
            SELECT
                quality_flag,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM fact_measurement
            GROUP BY quality_flag
            ORDER BY count DESC
        """
        ).fetchdf()

        print("\nOverall distribution:")
        for _, row in overall.iterrows():
            print(f"  - {row['quality_flag']}: {row['count']:,} ({row['percentage']}%)")

        # By source
        by_source = con.execute(
            """
            SELECT
                source,
                quality_flag,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY source), 2) as percentage
            FROM fact_measurement
            GROUP BY source, quality_flag
            ORDER BY source, count DESC
        """
        ).fetchdf()

        print("\nBy source:")
        for source in by_source["source"].unique():
            print(f"  {source}:")
            source_data = by_source[by_source["source"] == source]
            for _, row in source_data.iterrows():
                print(f"    - {row['quality_flag']}: {row['count']:,} ({row['percentage']}%)")

        # Identify low quality metrics (>50% NODATA)
        low_quality = con.execute(
            """
            SELECT
                metric,
                COUNT(*) FILTER (WHERE quality_flag = 'NODATA') as nodata,
                COUNT(*) as total,
                CASE
                    WHEN COUNT(*) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(*) FILTER (WHERE quality_flag = 'NODATA') * 100.0 / COUNT(*), 2
                    )
                END as nodata_pct
            FROM fact_measurement
            GROUP BY metric
            HAVING CASE
                    WHEN COUNT(*) = 0 THEN 0
                    ELSE COUNT(*) FILTER (WHERE quality_flag = 'NODATA') * 100.0 / COUNT(*)
                END > 50
            ORDER BY nodata_pct DESC
        """
        ).fetchdf()

        if not low_quality.empty:
            print("\n⚠️  LOW QUALITY METRICS (>50% NODATA):")
            for _, row in low_quality.iterrows():
                print(f"  - {row['metric']}: {row['nodata_pct']}% NODATA")
        else:
            print("\n✅ No metrics with excessive NODATA flags")

        return {
            "overall": overall.to_dict("records"),
            "by_source": by_source.to_dict("records"),
            "low_quality_metrics": low_quality.to_dict("records") if not low_quality.empty else [],
        }

    finally:
        con.close()


def run_all_dq_checks(duckdb_path: str) -> Dict[str, any]:
    """Run all data quality checks and return comprehensive report.

    Args:
        duckdb_path: Path to DuckDB database

    Returns:
        Dictionary with all DQ check results
    """
    print("\n" + "=" * 80)
    print("COMPREHENSIVE DATA QUALITY REPORT")
    print("=" * 80)

    results = {
        "summary": dq_summary(duckdb_path),
        "row_counts": check_row_counts(duckdb_path),
        "null_rates": check_null_rates(duckdb_path),
        "value_ranges": check_value_ranges(duckdb_path),
        "temporal_coverage": check_temporal_coverage(duckdb_path),
        "quality_flags": check_quality_flags(duckdb_path),
    }

    print("\n" + "=" * 80)
    print("DATA QUALITY REPORT COMPLETE")
    print("=" * 80)

    # Overall assessment
    issues = []
    if results["null_rates"]["overall_null_pct"] > 30:
        issues.append(f"High overall null rate: {results['null_rates']['overall_null_pct']}%")
    if results["null_rates"]["high_null_metrics"]:
        issues.append(f"High null metrics: {', '.join(results['null_rates']['high_null_metrics'])}")
    if results["value_ranges"]["outliers_exist"]:
        issues.append("Outlier values detected outside expected ranges")
    if results["quality_flags"]["low_quality_metrics"]:
        issues.append(
            f"Low quality metrics detected: {len(results['quality_flags']['low_quality_metrics'])}"
        )

    if issues:
        print("\n⚠️  DATA QUALITY ISSUES:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("\n✅ NO MAJOR DATA QUALITY ISSUES DETECTED")

    return results


if __name__ == "__main__":
    """Run DQ checks when executed directly."""
    import os
    import sys

    duckdb_path = os.environ.get("DUCKDB_PATH", "/opt/airflow/data/duckdb/cct_env.duckdb")

    print(f"🔍 Running data quality checks on: {duckdb_path}")

    try:
        results = run_all_dq_checks(duckdb_path)
        print("\n✅ Data quality checks complete")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error running DQ checks: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
