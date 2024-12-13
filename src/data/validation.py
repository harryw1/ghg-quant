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
        self.required_columns = {
            "emissions": "float64",  # renamed from co2e_emission
            "facility_name": "object",
            "county": "object",
            "sector_name": "object",
        }

        # Define valid ranges for emissions data
        self.validation_rules = {
            "emissions": {
                "min": 0,  # Emissions can't be negative
                "max": 1e9,  # Reasonable upper bound for annual emissions
            }
        }

    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Validate a DataFrame against GHG data requirements."""
        self.logger.debug(f"Validating DataFrame with columns: {df.columns.tolist()}")

        issues = {
            "missing_columns": [],
            "type_errors": [],
            "value_errors": [],
            "date_errors": [],
        }

        # Check for empty DataFrame
        if df is None or df.empty:
            issues["missing_columns"].append("DataFrame is empty")
            return issues

        # Check required columns (allowing for both original and renamed columns)
        for col, expected_type in self.required_columns.items():
            original_col = "co2e_emission" if col == "emissions" else col
            if col not in df.columns and original_col not in df.columns:
                issues["missing_columns"].append(f"Missing required column: {col}")

        # If we have critical missing columns, return early
        if issues["missing_columns"]:
            return issues

        # Validate emissions data
        emissions_col = "emissions" if "emissions" in df.columns else "co2e_emission"
        if emissions_col in df.columns:
            try:
                df[emissions_col] = pd.to_numeric(df[emissions_col], errors="coerce")
                rules = self.validation_rules["emissions"]
                invalid_emissions = df[emissions_col].notna() & (
                    (df[emissions_col] < rules["min"])
                    | (df[emissions_col] > rules["max"])
                )
                if invalid_emissions.any():
                    issues["value_errors"].append(
                        f"Column {emissions_col} contains values outside valid range "
                        f"[{rules['min']}, {rules['max']}]"
                    )
            except (ValueError, TypeError):
                issues["type_errors"].append(
                    f"Column {emissions_col} contains non-numeric values"
                )

        return issues
