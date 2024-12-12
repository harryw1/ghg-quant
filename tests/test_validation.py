"""Test validation of GHG data."""
# tests/test_validation.py

import os
import sys

import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data.validation import GHGDataValidator


@pytest.fixture
def validator():
    """Return a validator instance."""
    return GHGDataValidator()


@pytest.fixture
def valid_data():
    """Return a valid GHG data frame."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=3),
            "emissions": [100.0, 200.0, 300.0],
        }
    )


def test_valid_data(validator, valid_data):
    """Test validation of correct data."""
    issues = validator.validate_dataframe(valid_data)
    assert not any(issues.values())


def test_missing_columns(validator):
    """Test validation of data with missing columns."""
    df = pd.DataFrame({"wrong_column": [1, 2, 3]})
    issues = validator.validate_dataframe(df)
    assert len(issues["missing_columns"]) > 0


def test_invalid_values(validator):
    """Test validation of data with invalid values."""
    df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=3),
            "emissions": [-100.0, 200.0, 300.0],  # Negative emissions
        }
    )
    issues = validator.validate_dataframe(df)
    assert len(issues["value_errors"]) > 0
