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


# Air Quality Processing Functions


def process_air_quality_excel(file_path: str, pollutant: str, year: str) -> Optional[pd.DataFrame]:
    """Process a single air quality Excel file (NO2, O3, PM10, PM2.5, SO2).

    Uses the same header structure as wind data but for a single pollutant.

    Args:
        file_path: Path to Excel file
        pollutant: Pollutant type (no2, o3, pm10, pm25, so2)
        year: Year being processed

    Returns:
        DataFrame with datetime and station_{pk}_{pollutant} columns
    """
    try:
        # Load station mappings - air quality uses same stations as wind
        station_mappings = load_station_mappings()

        print(f"🔄 Processing {pollutant.upper()} data: {file_path}")

        # Read Excel file - same structure as wind data
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path, engine="openpyxl")
        else:
            df = pd.read_excel(file_path, engine="xlrd")

        # Parse headers - use air quality specific parser for air quality files
        print("   🔍 Using air quality specific header parsing...")
        stations, parameters, units, data_start_row = parse_air_quality_headers(df)

        print(f"   Found {len(stations)} stations: {stations}")
        print(f"   Parameters: {parameters}")
        print(f"   Units: {units}")

        # Extract datetime and data
        datetime_col = df.iloc[data_start_row:, 0].copy()
        data_df = df.iloc[data_start_row:, 1:].copy()

        # Parse datetime column
        datetime_col = pd.to_datetime(datetime_col, format="%d/%m/%Y %H:%M", errors="coerce")

        # Create final dataframe
        result_df = pd.DataFrame()
        result_df["datetime"] = datetime_col

        # Process station columns
        station_pk_mapping = {}

        for i, station in enumerate(stations):
            if i >= len(data_df.columns):
                break

            # Normalize station name
            normalized_station = normalise_station_name(station)

            if normalized_station in station_mappings:
                station_obj = station_mappings[normalized_station]
                station_pk = station_obj.station_pk

                # Create column name: station_{pk}_{pollutant}
                col_name = f"station_{station_pk}_{pollutant}"

                # Copy data
                result_df[col_name] = data_df.iloc[:, i].copy()

                # Store mapping for metadata
                station_pk_mapping[col_name] = {
                    "station_pk": station_pk,
                    "station_code": station_obj.station_code,
                    "station_name": station,
                    "metric": pollutant,
                    "unit": units[i] if i < len(units) else "ug/m3(S)",
                    "parameter": parameters[i] if i < len(parameters) else pollutant.upper(),
                }

                print(f"   ✅ Mapped {station} → station_{station_pk}_{pollutant}")
            else:
                print(f"   ⚠️  Unknown station: {station}")

        # Clean data - handle air quality specific flags
        for col in result_df.columns:
            if col != "datetime":
                result_df[col] = result_df[col].replace(
                    ["NoData", "InVld", "<Samp", "Down", "RS232", "Zero", "Calib", "", " ", "NaN"],
                    np.nan,
                )

        # Format data for staging (minimal transformation)
        formatted_df = format_air_quality_data(result_df, pollutant)

        # Remove rows with invalid timestamps only
        formatted_df = formatted_df.dropna(subset=["datetime"])

        print(f"   ✅ Staged {len(formatted_df)} records (raw data preserved)")
        print(f"   Columns: {list(formatted_df.columns)}")

        # Add metadata
        formatted_df.attrs["station_mapping"] = station_pk_mapping
        formatted_df.attrs["source_file"] = file_path
        formatted_df.attrs["year"] = year
        formatted_df.attrs["pollutant"] = pollutant
        formatted_df.attrs["processed_at"] = datetime.now().isoformat()

        return formatted_df

    except Exception as e:
        print(f"❌ Error processing {pollutant} file {file_path}: {e}")
        import traceback

        traceback.print_exc()
        return None


def parse_air_quality_headers(df: pd.DataFrame) -> Tuple[List[str], List[str], List[str], int]:
    """Parse air quality data headers with correct structure detection.

    Air quality files have specific structure:
    Row 0: Title (e.g., "MultiStation: Periodically...")
    Row 1: Empty
    Row 2: Station names (e.g., "Bellville South AQM Site", "Foreshore AQM Site")
    Row 3: Pollutant types (e.g., "NO2", "NO2", "NO2")
    Row 4: Units (e.g., "ug/m3", "ug/m3", "ug/m3")
    Row 5+: Data starts
    """

    print("   🔍 Analyzing air quality header structure...")

    # Print first 10 rows to understand structure
    print("   📋 First 6 rows of Excel file:")
    for i in range(min(6, len(df))):
        row_values = [
            str(val)[:30] if pd.notna(val) else "NaN" for val in df.iloc[i].head(8).values
        ]
        print(f"      Row {i}: {row_values}")

    # Air quality standard format: Row 2 = stations, Row 3 = pollutants, Row 4 = units
    if len(df) >= 5:
        stations_row = df.iloc[2].fillna("").astype(str).tolist()
        pollutants_row = df.iloc[3].fillna("").astype(str).tolist()
        units_row = df.iloc[4].fillna("").astype(str).tolist()

        print(f"   📍 Row 2 (potential stations): {stations_row}")
        print(f"   📍 Row 3 (potential pollutants): {pollutants_row}")
        print(f"   📍 Row 4 (potential units): {units_row}")

        # Check if row 2 contains station names (look for AQM indicators)
        station_indicators = [
            "AQM",
            "Site",
            "Station",
            "Atlantis",
            "Bellville",
            "Bothasig",
            "Goodwood",
            "Khayelitsha",
            "Somerset",
            "Tableview",
            "Foreshore",
            "Molteno",
            "Plattekloof",
            "Wallacedene",
        ]

        station_matches = 0
        for station in stations_row[1:]:  # Skip first column (Date & Time)
            if any(indicator in str(station) for indicator in station_indicators):
                station_matches += 1

        print(f"   📊 Found {station_matches} recognizable station names in row 2")

        # Check if row 3 contains pollutant names
        pollutant_indicators = ["NO2", "O3", "PM10", "PM2.5", "SO2"]
        pollutant_matches = 0
        for pollutant in pollutants_row[1:]:  # Skip first column
            if str(pollutant).upper() in pollutant_indicators:
                pollutant_matches += 1

        print(f"   📊 Found {pollutant_matches} recognizable pollutant names in row 3")

        # Verify this is correct air quality format
        if station_matches >= 2 and pollutant_matches >= 1:
            print(
                f"   ✅ Confirmed air quality format: {station_matches} stations, "
                f"{pollutant_matches} pollutants"
            )

            # Return station names from row 2, pollutant types from row 3, units from row 4
            # Skip first column which is "Date & Time"
            final_stations = stations_row[1:]
            final_pollutants = pollutants_row[1:]
            final_units = units_row[1:]

            print(f"   🎯 Extracted stations: {final_stations}")
            print(f"   🎯 Extracted pollutants: {final_pollutants}")
            print(f"   🎯 Extracted units: {final_units}")

            return (final_stations, final_pollutants, final_units, 5)

    # Fallback: search for station names in any row
    print("   ⚠️  Standard format not detected, searching for station indicators...")

    station_indicators = [
        "AQM",
        "Site",
        "Station",
        "Atlantis",
        "Bellville",
        "Bothasig",
        "Goodwood",
        "Khayelitsha",
        "Somerset",
        "Tableview",
        "Foreshore",
        "Molteno",
        "Plattekloof",
        "Wallacedene",
    ]

    best_row_idx = -1
    best_station_count = 0

    for row_idx in range(min(8, len(df))):
        row_data = df.iloc[row_idx].fillna("").astype(str).tolist()
        station_count = sum(
            1
            for cell in row_data
            if any(indicator in str(cell) for indicator in station_indicators)
        )

        if station_count > best_station_count:
            best_station_count = station_count
            best_row_idx = row_idx

    if best_row_idx >= 0 and best_station_count >= 2:
        print(f"   ✅ Found {best_station_count} stations in row {best_row_idx}")
        stations = df.iloc[best_row_idx].fillna("").astype(str).tolist()[1:]  # Skip first column
        params = ["Unknown"] * len(stations)
        units = ["Unknown"] * len(stations)
        return (stations, params, units, best_row_idx + 1)

    # Last resort fallback
    print("   ❌ Could not find station names, using column headers as fallback")
    stations = [f"Unknown_Station_{i}" for i in range(len(df.columns) - 1)]
    params = ["Unknown"] * len(stations)
    units = ["Unknown"] * len(stations)
    return (stations, params, units, 1)


def extract_pollutant_from_filename(filename: str) -> Optional[str]:
    """Extract pollutant type dynamically from filename.

    Handles standard CCT naming convention: YYYY_POLLUTANT_CCT.xls(x)
    Examples:
    - 2019_NO2_CCT.xls -> no2
    - 2020_PM2.5_CCT.xls -> pm25
    - 2022_CO_CCT.xlsx -> co (new pollutant)
    """

    filename_upper = filename.upper()

    # Remove common prefixes/suffixes to isolate pollutant
    # Pattern: YYYY_POLLUTANT_CCT.xls(x)
    import re

    # Match the standard CCT pattern
    match = re.match(r"^\d{4}_([^_]+)_CCT\.xlsx?$", filename_upper)
    if match:
        pollutant_raw = match.group(1)

        # Normalize pollutant name
        pollutant = pollutant_raw.lower()

        # Handle special cases for PM measurements
        if "PM2.5" in pollutant_raw or "PM25" in pollutant_raw:
            pollutant = "pm25"
        elif "PM10" in pollutant_raw:
            pollutant = "pm10"
        elif "PM1" in pollutant_raw:
            pollutant = "pm1"

        print(f"   📝 Dynamic mapping: {filename} -> pollutant '{pollutant}'")
        return pollutant

    # Fallback: try to extract any pollutant-like pattern
    # This handles non-standard naming
    common_pollutants = ["NO2", "NO", "O3", "SO2", "CO", "NH3", "H2S", "BENZENE"]
    for pollutant_pattern in common_pollutants:
        if pollutant_pattern in filename_upper:
            pollutant = pollutant_pattern.lower()
            print(f"   📝 Fallback mapping: {filename} -> pollutant '{pollutant}'")
            return pollutant

    # Check for PM patterns with fallback
    if "PM" in filename_upper:
        if "2.5" in filename_upper or "25" in filename_upper:
            return "pm25"
        elif "10" in filename_upper:
            return "pm10"
        elif "1" in filename_upper and "10" not in filename_upper:
            return "pm1"
        else:
            return "pm"  # Generic PM

    return None


def format_air_quality_data(df: pd.DataFrame, pollutant: str) -> pd.DataFrame:
    """Format air quality data for staging - preserve raw values, only convert data types.

    Staging should preserve raw data integrity with minimal transformation:
    - Convert data types for consistency
    - Handle quality flags to NaN for analysis compatibility
    - NO data cleaning, validation, or outlier removal
    """

    print(f"   📋 Formatting {pollutant} data for staging (preserving raw values)")

    # Apply minimal formatting to all data columns (exclude datetime)
    for col in df.columns:
        if col != "datetime" and col.startswith("station_"):
            # Convert to numeric, preserving original values
            # Only quality flags like 'NoData', 'InVld' become NaN
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def extract_air_quality_zip(zip_path: str, extract_dir: str) -> List[str]:
    """Extract air quality zip file and return list of Excel file paths."""
    import zipfile

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        # Find all Excel files in extracted directory
        excel_files = []
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.endswith((".xls", ".xlsx")) and any(
                    pollutant in file.upper() for pollutant in ["NO2", "O3", "PM10", "PM2.5", "SO2"]
                ):
                    excel_files.append(os.path.join(root, file))

        return excel_files

    except Exception as e:
        print(f"❌ Error extracting {zip_path}: {e}")
        return []


def stage_air_quality_year(
    year: str,
    raw_data_dir: str = "/opt/airflow/data/raw",
    staged_data_dir: str = "/opt/airflow/data/staged",
) -> List[str]:
    """Stage air quality data for a specific year.

    Args:
        year: Year to process (e.g., "2020")
        raw_data_dir: Directory containing raw zip files
        staged_data_dir: Directory for staged Parquet output

    Returns:
        List of staged Parquet file paths
    """
    import tempfile

    staged_files = []

    try:
        # Input zip file path
        zip_path = os.path.join(raw_data_dir, f"air_quality_{year}.zip")

        if not os.path.exists(zip_path):
            print(f"❌ Input file not found: {zip_path}")
            return staged_files

        print(f"🔄 Processing air quality {year}: {zip_path}")

        # Create temporary directory for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract zip file
            excel_files = extract_air_quality_zip(zip_path, temp_dir)

            if not excel_files:
                print(f"❌ No Excel files found in {zip_path}")
                return staged_files

            print(f"   Found {len(excel_files)} Excel files")

            # Create output directory
            os.makedirs(staged_data_dir, exist_ok=True)

            # Process each pollutant file
            for excel_path in excel_files:
                filename = os.path.basename(excel_path)

                # Extract pollutant type dynamically from filename
                pollutant = extract_pollutant_from_filename(filename)

                if not pollutant:
                    print(f"   ⚠️  Could not determine pollutant from filename: {filename}")
                    print("      File will be skipped.")
                    continue

                print(f"   🔍 Detected pollutant: {pollutant}")

                # Process the Excel file
                staged_df = process_air_quality_excel(excel_path, pollutant, year)

                if staged_df is None or staged_df.empty:
                    print(f"   ❌ No data processed for {pollutant} {year}")
                    continue

                # Output file path
                output_path = os.path.join(
                    staged_data_dir, f"air_quality_{pollutant}_{year}.parquet"
                )

                # Save as Parquet
                staged_df.to_parquet(output_path, engine="pyarrow", index=False)

                print(f"   ✅ Staged {pollutant} {year}: {output_path}")
                print(f"      Shape: {staged_df.shape}")
                print(
                    f"      Date range: {staged_df['datetime'].min()} to "
                    f"{staged_df['datetime'].max()}"
                )

                staged_files.append(output_path)

        print(f"✅ Staging complete for {year}: {len(staged_files)} files")
        return staged_files

    except Exception as e:
        print(f"❌ Error staging air quality {year}: {e}")
        import traceback

        traceback.print_exc()
        return staged_files


# Airflow task functions for air quality
def stage_air_quality_data_task(year: str, **context) -> List[str]:
    """Airflow task to stage air quality data for a specific year."""
    result = stage_air_quality_year(year)
    if not result:
        raise ValueError(f"Failed to stage air quality data for {year}")
    return result


if __name__ == "__main__":
    # Test processing
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "air":
            # Test air quality staging
            year = sys.argv[2] if len(sys.argv) > 2 else "2019"
            result = stage_air_quality_year(year)
            print(f"Air quality {year}: {len(result)} files processed")
        else:
            # Test wind staging
            year = sys.argv[1]
            result = stage_wind_year(year)
            if result:
                print(f"Wind success: {result}")
            else:
                print("Wind failed")
    else:
        # Process all wind files
        results = stage_all_wind_data()
        print(f"Processed {len(results)} wind files")
