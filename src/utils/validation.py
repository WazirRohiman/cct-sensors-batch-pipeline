"""Validation stubs using Pandera or manual checks."""

from typing import Tuple

import pandas as pd


def validate_dataframe(df: pd.DataFrame, schema_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split into (valid_df, errors_df) according to a named schema.

    Implementation detail will follow using Pandera or custom checks.
    """
    # TODO: implement schemas and apply them
    return df, pd.DataFrame(columns=list(df.columns) + ["__error__"])  # placeholder
