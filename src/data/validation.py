"""Validates GHG emissions data structure and content."""

# src/data/validation.py
import logging
from typing import Dict, List

import pandas as pd


class GHGDataValidator:
    """Validates GHG emissions data structure and content."""

    def __init__(self):
        """Initialize validator."""
        self.logger = logging.getLogger(__name__)

        # Define expected columns and their types
        self.required_columns = {"date": "datetime64[ns]", "emissions": "float64"}

        # Define valid ranges for emissions data
        self.validation_rules = {
            "emissions": {
                "min": 0,  # Emissions can't be negative
                "max": 1e9,  # Reasonable upper bound for annual emissions
            }
        }

    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Validate a DataFrame against GHG data requirements.

        Args:
            df: DataFrame to validate

        Returns:
            Dictionary of validation issues found

        """
        issues = {
            "missing_columns": [],
            "type_errors": [],
            "value_errors": [],
            "date_errors": [],
        }

        # Check required columns
        for col in self.required_columns:
            if col not in df.columns:
                issues["missing_columns"].append(f"Missing required column: {col}")

        if not issues["missing_columns"]:
            # Check data types
            for col, expected_type in self.required_columns.items():
                if not pd.api.types.is_dtype_equal(df[col].dtype, expected_type):
                    try:
                        # Attempt to convert
                        df[col] = df[col].astype(expected_type)
                    except ValueError:
                        issues["type_errors"].append(
                            f"Column {col} could not be converted to {expected_type}"
                        )

            # Check value ranges
            for col, rules in self.validation_rules.items():
                if col in df.columns:
                    mask = (df[col] < rules["min"]) | (df[col] > rules["max"])
                    if mask.any():
                        issues["value_errors"].append(
                            f"Column {col} contains values outside valid range "
                            f"[{rules['min']}, {rules['max']}]"
                        )

            # Validate date format and range
            if "date" in df.columns:
                try:
                    df["date"] = pd.to_datetime(df["date"])
                    future_dates = df["date"] > pd.Timestamp.now()
                    if future_dates.any():
                        issues["date_errors"].append("Found dates in the future")
                except ValueError:
                    issues["date_errors"].append("Invalid date format")

        return issues
