"""Normalisation utilities to transform staged sensor data into analytics-ready schema."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import yaml

_METRIC_UNITS: Dict[str, str] = {
    # Wind metrics
    "wind_speed": "m/s",
    "wind_direction": "degrees",
    # Air quality metrics
    "no2": "ug/m3(S)",
    "o3": "ug/m3(S)",
    "pm10": "ug/m3(S)",
    "pm25": "ug/m3(S)",
    "so2": "ug/m3(S)",
}


def _resolve_station_config_path() -> Path:
    """Return the path to the station mapping YAML regardless of runtime environment."""

    candidates: Iterable[Path] = (
        Path("/opt/airflow/src/configs/station_mapping.yaml"),
        Path(__file__).resolve().parent.parent / "configs" / "station_mapping.yaml",
    )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError("station_mapping.yaml not found in expected locations")


def _load_station_mapping_dataframe() -> pd.DataFrame:
    """Load station metadata as a DataFrame keyed by surrogate key."""

    with _resolve_station_config_path().open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    station_records: List[Dict[str, object]] = config["wind_stations"]["station_mappings"]
    station_df = pd.DataFrame(station_records)

    if station_df.empty:
        raise ValueError("No station mappings defined in configuration")

    station_df["station_pk"] = station_df["station_pk"].astype(int)
    return station_df


_COLUMN_PATTERN = re.compile(r"^station_(?P<station_pk>\d+)_(?P<metric>.+)$")


def _extract_year_from_path(path: Path) -> Optional[str]:
    match = re.search(r"(19|20)\d{2}", path.stem)
    return match.group(0) if match else None


def _build_station_dim(dest_dir: Path, station_df: pd.DataFrame) -> Path:
    """Persist the station dimension, merging with any existing file for idempotency."""

    dim_columns = [
        "station_pk",
        "station_code",
        "station_name",
        "location_type",
        "description",
    ]

    station_dim = (
        station_df[dim_columns].drop_duplicates(subset=["station_pk"]).sort_values("station_pk")
    )

    output_path = dest_dir / "station_dim.parquet"

    if output_path.exists():
        existing = pd.read_parquet(output_path)
        station_dim = (
            pd.concat([existing, station_dim], ignore_index=True)
            .drop_duplicates(subset=["station_pk"], keep="last")
            .sort_values("station_pk")
        )

    station_dim.to_parquet(output_path, index=False)
    return output_path


def _normalise_wind_dataframe(
    df: pd.DataFrame, station_df: pd.DataFrame, year: Optional[str]
) -> pd.DataFrame:
    """Convert wide wind dataframe into tall format with station metadata."""

    if df.empty:
        raise ValueError("Staged wind dataframe is empty")

    value_frames: List[pd.DataFrame] = []

    for column in df.columns:
        if column == "datetime":
            continue

        match = _COLUMN_PATTERN.match(column)
        if not match:
            # Skip any non-standard columns but keep the pipeline moving
            continue

        station_pk = int(match.group("station_pk"))
        metric = match.group("metric").lower()

        if metric not in _METRIC_UNITS:
            # Unknown metric naming – keep original string but log for debugging
            print(f"⚠️  Unknown metric column '{column}', keeping metric name '{metric}'.")

        metric_name = metric
        unit = _METRIC_UNITS.get(metric_name)

        values = (
            df[["datetime", column]]
            .rename(columns={column: "value"})
            .assign(station_pk=station_pk, metric=metric_name)
        )

        values["value"] = pd.to_numeric(values["value"], errors="coerce")

        if unit:
            values["unit"] = unit
        else:
            values["unit"] = pd.NA

        if year:
            values["year"] = year

        values["source"] = "wind"
        values["quality_flag"] = values["value"].apply(
            lambda val: "NODATA" if pd.isna(val) else "VALID"
        )

        value_frames.append(values)

    if not value_frames:
        raise ValueError("No station columns found to normalise")

    tall_df = pd.concat(value_frames, ignore_index=True)

    tall_df["datetime"] = pd.to_datetime(tall_df["datetime"], errors="coerce")
    tall_df = tall_df.dropna(subset=["datetime"]).reset_index(drop=True)

    tall_df = tall_df.merge(
        station_df[["station_pk", "station_code", "station_name", "location_type"]],
        on="station_pk",
        how="left",
        validate="m:1",
    )

    missing_metadata = tall_df["station_code"].isna().sum()
    if missing_metadata:
        print(f"⚠️  {missing_metadata} rows missing station metadata after merge.")

    column_order = [
        "datetime",
        "station_pk",
        "station_code",
        "station_name",
        "location_type",
        "metric",
        "unit",
        "value",
        "quality_flag",
        "source",
    ]

    if "year" in tall_df.columns:
        column_order.append("year")

    tall_df = (
        tall_df[column_order]
        .sort_values(["datetime", "station_pk", "metric"])
        .reset_index(drop=True)
    )
    return tall_df


def normalise_to_tall(parquet_path: str, dest_dir: str) -> Optional[str]:
    """Read staged wind Parquet and produce normalised tall-format Parquet.

    Args:
        parquet_path: Path to the staged wide-format Parquet file.
        dest_dir: Directory where the normalised outputs should be written.

    Returns:
        Path to the generated tall Parquet, or None if normalisation fails.
    """

    source_path = Path(parquet_path)
    if not source_path.exists():
        print(f"❌ Staged file not found: {parquet_path}")
        return None

    destination = Path(dest_dir)
    destination.mkdir(parents=True, exist_ok=True)

    try:
        staged_df = pd.read_parquet(source_path)
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"❌ Failed to read staged Parquet {parquet_path}: {exc}")
        return None

    year = _extract_year_from_path(source_path)
    station_df = _load_station_mapping_dataframe()

    try:
        tall_df = _normalise_wind_dataframe(staged_df, station_df, year)
    except Exception as exc:
        print(f"❌ Normalisation failed for {parquet_path}: {exc}")
        return None

    fact_filename = source_path.stem + "_normalised.parquet"
    fact_path = destination / fact_filename

    tall_df.to_parquet(fact_path, index=False)

    _build_station_dim(destination, station_df)

    print(f"✅ Normalised wind data written to {fact_path}")
    return str(fact_path)


def normalise_wind_year(
    year: str,
    staged_dir: str = "/opt/airflow/data/staged",
    normalised_dir: str = "/opt/airflow/data/normalised",
) -> Optional[str]:
    """Normalise a single year's staged wind data."""

    staged_path = Path(staged_dir) / f"wind_{year}.parquet"
    return normalise_to_tall(str(staged_path), normalised_dir)


def normalise_all_wind(
    staged_dir: str = "/opt/airflow/data/staged",
    normalised_dir: str = "/opt/airflow/data/normalised",
) -> List[str]:
    """Normalise every staged wind file found in the staging directory."""

    staged_directory = Path(staged_dir)
    if not staged_directory.exists():
        print(f"❌ Staged directory not found: {staged_dir}")
        return []

    normalised_paths: List[str] = []

    for staged_file in sorted(staged_directory.glob("wind_*.parquet")):
        result = normalise_to_tall(str(staged_file), normalised_dir)
        if result:
            normalised_paths.append(result)

    if normalised_paths:
        print(f"✅ Normalised {len(normalised_paths)} wind files into {normalised_dir}")
    else:
        print("⚠️  No wind files were normalised")

    return normalised_paths


def _normalise_air_quality_dataframe(
    df: pd.DataFrame, station_df: pd.DataFrame, year: Optional[str], pollutant: str
) -> pd.DataFrame:
    """Convert wide air quality dataframe into tall format with station metadata."""

    if df.empty:
        raise ValueError("Staged air quality dataframe is empty")

    value_frames: List[pd.DataFrame] = []

    for column in df.columns:
        if column == "datetime":
            continue

        match = _COLUMN_PATTERN.match(column)
        if not match:
            # Skip any non-standard columns but keep the pipeline moving
            continue

        station_pk = int(match.group("station_pk"))
        metric = match.group("metric").lower()

        # For air quality files, the metric should match the pollutant
        if metric != pollutant.lower():
            print(f"⚠️  Unexpected metric '{metric}' in {pollutant} file, keeping as '{metric}'.")

        metric_name = metric
        unit = _METRIC_UNITS.get(metric_name)

        values = (
            df[["datetime", column]]
            .rename(columns={column: "value"})
            .assign(station_pk=station_pk, metric=metric_name)
        )

        values["value"] = pd.to_numeric(values["value"], errors="coerce")

        if unit:
            values["unit"] = unit
        else:
            values["unit"] = pd.NA

        if year:
            values["year"] = year

        values["source"] = "air_quality"
        values["quality_flag"] = values["value"].apply(
            lambda val: "NODATA" if pd.isna(val) else "VALID"
        )

        value_frames.append(values)

    if not value_frames:
        raise ValueError("No station columns found to normalise")

    tall_df = pd.concat(value_frames, ignore_index=True)

    tall_df["datetime"] = pd.to_datetime(tall_df["datetime"], errors="coerce")
    tall_df = tall_df.dropna(subset=["datetime"]).reset_index(drop=True)

    tall_df = tall_df.merge(
        station_df[["station_pk", "station_code", "station_name", "location_type"]],
        on="station_pk",
        how="left",
        validate="m:1",
    )

    missing_metadata = tall_df["station_code"].isna().sum()
    if missing_metadata:
        print(f"⚠️  {missing_metadata} rows missing station metadata after merge.")

    column_order = [
        "datetime",
        "station_pk",
        "station_code",
        "station_name",
        "location_type",
        "metric",
        "unit",
        "value",
        "quality_flag",
        "source",
    ]

    if "year" in tall_df.columns:
        column_order.append("year")

    tall_df = (
        tall_df[column_order]
        .sort_values(["datetime", "station_pk", "metric"])
        .reset_index(drop=True)
    )
    return tall_df


def normalise_air_quality_to_tall(parquet_path: str, dest_dir: str) -> Optional[str]:
    """Read staged air quality Parquet and produce normalised tall-format Parquet.

    Args:
        parquet_path: Path to the staged wide-format air quality Parquet file.
        dest_dir: Directory where the normalised outputs should be written.

    Returns:
        Path to the generated tall Parquet, or None if normalisation fails.
    """

    source_path = Path(parquet_path)
    if not source_path.exists():
        print(f"❌ Staged file not found: {parquet_path}")
        return None

    destination = Path(dest_dir)
    destination.mkdir(parents=True, exist_ok=True)

    # Extract pollutant from filename (e.g., air_quality_no2_2020.parquet -> no2)
    filename_parts = source_path.stem.split("_")
    if len(filename_parts) < 3 or filename_parts[0] != "air" or filename_parts[1] != "quality":
        print(f"❌ Unexpected air quality filename format: {source_path.name}")
        return None

    pollutant = filename_parts[2]
    year = filename_parts[3] if len(filename_parts) >= 4 else None

    try:
        staged_df = pd.read_parquet(source_path)
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"❌ Failed to read staged Parquet {parquet_path}: {exc}")
        return None

    station_df = _load_station_mapping_dataframe()

    try:
        tall_df = _normalise_air_quality_dataframe(staged_df, station_df, year, pollutant)
    except Exception as exc:
        print(f"❌ Normalisation failed for {parquet_path}: {exc}")
        return None

    fact_filename = source_path.stem + "_normalised.parquet"
    fact_path = destination / fact_filename

    tall_df.to_parquet(fact_path, index=False)

    _build_station_dim(destination, station_df)

    print(f"✅ Normalised air quality data written to {fact_path}")
    return str(fact_path)


def normalise_air_quality_year(
    year: str,
    pollutant: str,
    staged_dir: str = "/opt/airflow/data/staged",
    normalised_dir: str = "/opt/airflow/data/normalised",
) -> Optional[str]:
    """Normalise a single year's staged air quality data for a specific pollutant."""

    staged_path = Path(staged_dir) / f"air_quality_{pollutant}_{year}.parquet"
    return normalise_air_quality_to_tall(str(staged_path), normalised_dir)


def normalise_all_air_quality(
    staged_dir: str = "/opt/airflow/data/staged",
    normalised_dir: str = "/opt/airflow/data/normalised",
) -> List[str]:
    """Normalise every staged air quality file found in the staging directory."""

    staged_directory = Path(staged_dir)
    if not staged_directory.exists():
        print(f"❌ Staged directory not found: {staged_dir}")
        return []

    normalised_paths: List[str] = []

    for staged_file in sorted(staged_directory.glob("air_quality_*.parquet")):
        result = normalise_air_quality_to_tall(str(staged_file), normalised_dir)
        if result:
            normalised_paths.append(result)

    if normalised_paths:
        print(f"✅ Normalised {len(normalised_paths)} air quality files into {normalised_dir}")
    else:
        print("⚠️  No air quality files were normalised")

    return normalised_paths
