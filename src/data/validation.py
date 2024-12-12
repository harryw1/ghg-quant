"""Validates GHG emissions data structure and content."""

# src/data/validation.py
import logging
from typing import Dict, List

import pandas as pd


class GHGDataValidator:
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
        """Validate a DataFrame against GHG data requirements."""
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
            # Handle date column separately
            if "date" in df.columns:
                try:
                    # Try to convert dates and catch any conversion errors
                    df["date"] = pd.to_datetime(df["date"])
                except (ValueError, TypeError):
                    issues["date_errors"].append("Invalid date format in date column")
                else:
                    # Check for future dates only if conversion succeeded
                    future_dates = df["date"] > pd.Timestamp.now()
                    if future_dates.any():
                        issues["date_errors"].append("Found dates in the future")

            # Handle emissions column
            if "emissions" in df.columns:
                try:
                    df["emissions"] = pd.to_numeric(df["emissions"], errors="raise")
                except (ValueError, TypeError):
                    issues["type_errors"].append(
                        "Column emissions contains non-numeric values"
                    )
                else:
                    # Check value ranges only if conversion succeeded
                    rules = self.validation_rules["emissions"]
                    mask = (df["emissions"] < rules["min"]) | (
                        df["emissions"] > rules["max"]
                    )
                    if mask.any():
                        issues["value_errors"].append(
                            f"Column emissions contains values outside valid range "
                            f"[{rules['min']}, {rules['max']}]"
                        )

        return issues
