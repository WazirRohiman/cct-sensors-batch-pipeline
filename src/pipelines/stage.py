"""Staging pipeline for City of Cape Town environmental sensor data.

This module converts raw Excel/ZIP files to clean, validated Parquet files with:
- Surrogate key mapping for stations
- Data quality validation
- Standardized wide-format output ready for normalisation

Key Features:
- Wind data: Multi-header Excel → Wide format Parquet
- Station mapping with surrogate keys (station_pk)
- Data quality flags and validation
- Optimized for downstream normalisation
"""

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml


@dataclass
class StationMapping:
    """Station metadata with surrogate key."""

    station_pk: int
    station_code: str
    station_name: str
    normalised_names: List[str]
    location_type: str
    description: str


def load_station_mappings(
    config_path: str = "/opt/airflow/src/configs/station_mapping.yaml",
) -> Dict[str, StationMapping]:
    """Load station mappings from YAML configuration."""

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Station mapping config not found: {config_path}")

    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in config file: {exc}") from exc

    if not config:
        raise ValueError("Station mapping configuration is empty")

    wind_section = config.get("wind_stations") if isinstance(config, dict) else None
    if not isinstance(wind_section, dict):
        raise ValueError("Missing required 'wind_stations' section in config")

    station_configs = wind_section.get("station_mappings")
    if not isinstance(station_configs, list) or not station_configs:
        raise ValueError("Missing required 'wind_stations.station_mappings' in config")

    mappings: Dict[str, StationMapping] = {}
    for station_config in station_configs:
        station = StationMapping(**station_config)

        # Map all normalised names to this station
        for name in station.normalised_names:
            mappings[name] = station

    return mappings


def normalise_station_name(raw_name: str) -> str:
    """Normalise station names to handle variations."""
    # Handle common variations
    normalised = raw_name.strip()

    # Somerset West variations
    if "Somerset" in normalised:
        if "Somerset-West" in normalised:
            normalised = normalised.replace("Somerset-West", "Somerset West")

    return normalised


def parse_wind_headers(df: pd.DataFrame) -> Tuple[List[str], List[str], List[str], int]:
    """Parse wind data multi-level headers.

    Returns:
        Tuple of (station_names, parameters, units, data_start_row)
    """
    # Wind data structure:
    # Row 0: Title with period info
    # Row 1: Empty
    # Row 2: Station names (some may be repeated)
    # Row 3: Parameter names (Wind Dir V, Wind Speed V)
    # Row 4: Units (Deg, m/s)
    # Row 5+: Data

    try:
        # Get raw header rows
        station_row = df.iloc[2].fillna("").astype(str).tolist()
        param_row = df.iloc[3].fillna("").astype(str).tolist()
        unit_row = df.iloc[4].fillna("").astype(str).tolist()

        # Parse station and parameter pairs
        stations = []
        parameters = []
        units = []

        current_station = None

        for i in range(1, len(station_row)):  # Skip column 0 (Date & Time)
            station_name = station_row[i].strip()
            param_name = param_row[i].strip() if i < len(param_row) else ""
            unit_name = unit_row[i].strip() if i < len(unit_row) else ""

            # Update current station if we have a new station name
            if station_name and station_name != "":
                current_station = normalise_station_name(station_name)

            # Add entry if we have parameter info
            if param_name and current_station:
                stations.append(current_station)
                parameters.append(param_name)
                units.append(unit_name)

        return stations, parameters, units, 5

    except Exception as e:
        print(f"Error parsing wind headers: {e}")
        return [], [], [], 5


def validate_wind_data(
    df: pd.DataFrame,
    station_mappings: Dict[str, StationMapping],
) -> pd.DataFrame:
    """Validate wind data and add quality flags."""
    # Create copy for modification
    validated_df = df.copy()

    # Validate timestamp
    validated_df["datetime"] = pd.to_datetime(validated_df["datetime"], errors="coerce")

    # Process each station column
    for col in validated_df.columns:
        if col == "datetime":
            continue

        # Extract station and metric from column name
        if "_wind_direction" in col:
            # Validate wind direction (0-360 degrees)
            validated_df[col] = pd.to_numeric(validated_df[col], errors="coerce")
            validated_df.loc[(validated_df[col] < 0) | (validated_df[col] > 360), col] = np.nan

        elif "_wind_speed" in col:
            # Validate wind speed (>= 0 m/s)
            validated_df[col] = pd.to_numeric(validated_df[col], errors="coerce")
            validated_df.loc[validated_df[col] < 0, col] = np.nan

    return validated_df


def process_wind_excel(file_path: str, year: str) -> Optional[pd.DataFrame]:
    """Process a wind Excel file into standardized wide format.

    Args:
        file_path: Path to wind Excel file
        year: Year string for validation

    Returns:
        Wide-format DataFrame with surrogate key columns or None on failure
    """
    try:
        print(f"Processing wind file: {file_path}")

        # Load station mappings
        station_mappings = load_station_mappings()

        # Read Excel file with all headers
        df = pd.read_excel(file_path, header=None, engine="openpyxl")

        # Parse headers
        stations, parameters, units, data_start = parse_wind_headers(df)

        if not stations:
            print(f"❌ No stations found in {file_path}")
            return None

        print(f"Found {len(stations)} station-parameter combinations")

        # Extract data rows
        data_df = df.iloc[data_start:].reset_index(drop=True)

        # Create column names for data
        data_columns = ["datetime"]
        station_pk_mapping = {}

        for i, (station, param, unit) in enumerate(zip(stations, parameters, units)):
            # Map station name to surrogate key
            if station in station_mappings:
                station_pk = station_mappings[station].station_pk
                station_code = station_mappings[station].station_code

                # Create standardized column name with surrogate key
                if "Dir" in param:
                    metric = "wind_direction"
                elif "Speed" in param:
                    metric = "wind_speed"
                else:
                    metric = param.lower().replace(" ", "_")

                col_name = f"station_{station_pk}_{metric}"
                data_columns.append(col_name)

                # Store mapping for metadata
                station_pk_mapping[col_name] = {
                    "station_pk": station_pk,
                    "station_code": station_code,
                    "station_name": station,
                    "metric": metric,
                    "unit": unit,
                    "parameter": param,
                }
            else:
                print(f"⚠️  Unknown station: {station}")
                data_columns.append(f"unknown_station_{i}")

        # Assign column names to data
        if len(data_columns) != len(data_df.columns):
            print(
                (
                    "⚠️  Column count mismatch: expected "
                    f"{len(data_columns)}, got {len(data_df.columns)}"
                )
            )
            # Adjust to match actual columns
            data_columns = data_columns[: len(data_df.columns)]

        data_df.columns = data_columns

        # Handle missing data indicators
        for col in data_df.columns:
            if col != "datetime":
                data_df[col] = data_df[col].replace(["NoData", "InVld", "", " "], np.nan)

        # Validate data
        validated_df = validate_wind_data(data_df, station_mappings)

        # Remove rows with invalid timestamps
        validated_df = validated_df.dropna(subset=["datetime"])

        print(f"✅ Processed {len(validated_df)} valid records")
        print(f"   Columns: {list(validated_df.columns)}")

        # Add metadata as attributes
        validated_df.attrs["station_mapping"] = station_pk_mapping
        validated_df.attrs["source_file"] = file_path
        validated_df.attrs["year"] = year
        validated_df.attrs["processed_at"] = datetime.now().isoformat()

        return validated_df

    except Exception as e:
        print(f"❌ Error processing wind file {file_path}: {e}")
        import traceback

        traceback.print_exc()
        return None


def stage_wind_year(
    year: str,
    raw_data_dir: str = "/opt/airflow/data/raw",
    staged_data_dir: str = "/opt/airflow/data/staged",
) -> Optional[str]:
    """Stage wind data for a specific year.

    Args:
        year: Year to process (e.g., "2020")
        raw_data_dir: Directory containing raw Excel files
        staged_data_dir: Directory for staged Parquet output

    Returns:
        Path to staged Parquet file or None on failure
    """
    try:
        # Input file path
        input_path = os.path.join(raw_data_dir, f"wind_{year}.xlsx")

        if not os.path.exists(input_path):
            print(f"❌ Input file not found: {input_path}")
            return None

        # Process the Excel file
        staged_df = process_wind_excel(input_path, year)

        if staged_df is None or staged_df.empty:
            print(f"❌ No data processed for wind {year}")
            return None

        # Create output directory
        os.makedirs(staged_data_dir, exist_ok=True)

        # Output file path
        output_path = os.path.join(staged_data_dir, f"wind_{year}.parquet")

        # Save as Parquet
        staged_df.to_parquet(output_path, engine="pyarrow", index=False)

        print(f"✅ Staged wind {year}: {output_path}")
        print(f"   Shape: {staged_df.shape}")
        print(f"   Date range: {staged_df['datetime'].min()} to {staged_df['datetime'].max()}")

        return output_path

    except Exception as e:
        print(f"❌ Error staging wind {year}: {e}")
        import traceback

        traceback.print_exc()
        return None


def stage_all_wind_data(
    raw_data_dir: str = "/opt/airflow/data/raw", staged_data_dir: str = "/opt/airflow/data/staged"
) -> List[str]:
    """Stage all available wind data files.

    Returns:
        List of staged Parquet file paths
    """
    staged_files = []

    # Find all wind files in raw directory
    if not os.path.exists(raw_data_dir):
        print(f"❌ Raw data directory not found: {raw_data_dir}")
        return staged_files

    for filename in os.listdir(raw_data_dir):
        if filename.startswith("wind_") and filename.endswith(".xlsx"):
            # Extract year from filename
            year = filename.replace("wind_", "").replace(".xlsx", "")

            print(f"\n🔄 Processing wind data for {year}")

            result = stage_wind_year(year, raw_data_dir, staged_data_dir)
            if result:
                staged_files.append(result)

    print(f"\n✅ Staging complete: {len(staged_files)} files processed")
    return staged_files


# Airflow task functions
def stage_wind_data_task(year: str, **context) -> str:
    """Airflow task to stage wind data for a specific year."""
    result = stage_wind_year(year)
    if result is None:
        raise ValueError(f"Failed to stage wind data for {year}")
    return result


def stage_all_wind_data_task(**context) -> List[str]:
    """Airflow task to stage all wind data."""
    return stage_all_wind_data()


if __name__ == "__main__":
    # Test processing
    import sys

    if len(sys.argv) > 1:
        year = sys.argv[1]
        result = stage_wind_year(year)
        if result:
            print(f"Success: {result}")
        else:
            print("Failed")
    else:
        # Process all files
        results = stage_all_wind_data()
        print(f"Processed {len(results)} files")
