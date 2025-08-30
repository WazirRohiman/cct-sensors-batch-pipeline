"""Validation stubs using Pandera or manual checks."""

from typing import Tuple

import pandas as pd


def validate_dataframe(df: pd.DataFrame, schema_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split an input DataFrame into validated rows and a DataFrame of validation errors for a named schema.
    
    Parameters:
        df (pd.DataFrame): Input data to validate.
        schema_name (str): Identifier of the validation schema to apply (implementation-dependent).
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple (valid_df, errors_df). In the current stub implementation
        `valid_df` is returned unchanged (same as `df`) and `errors_df` is an empty DataFrame whose columns
        match `df` plus an "__error__" column. Intended behavior is to apply the named schema and
        separate rows that pass validation (valid_df) from rows with validation failures (errors_df).
    """
    # TODO: implement schemas and apply them
    return df, pd.DataFrame(columns=list(df.columns) + ["__error__"])  # placeholder
