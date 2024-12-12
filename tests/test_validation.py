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
    return GHGDataValidator()


def test_validate_valid_data(validator):
    """Test validation of correctly formatted data."""
    valid_data = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", "2020-12-31", freq="ME"),
            "emissions": [100.0] * 12,
        }
    )

    issues = validator.validate_dataframe(valid_data)
    assert all(len(issues[key]) == 0 for key in issues)


def test_validate_invalid_dates(validator):
    """Test validation of data with invalid dates."""
    # Test case 1: Invalid date format
    invalid_data = pd.DataFrame(
        {"date": ["invalid_date"] * 3, "emissions": [100.0] * 3}
    )
    issues = validator.validate_dataframe(invalid_data)
    assert len(issues["date_errors"]) > 0
    assert "Invalid date format" in issues["date_errors"][0]

    # Test case 2: Future dates
    future_data = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", "2025-03-31", freq="ME"),
            "emissions": [100.0] * 3,
        }
    )
    future_issues = validator.validate_dataframe(future_data)
    assert len(future_issues["date_errors"]) > 0
    assert "Found dates in the future" in future_issues["date_errors"][0]


def test_validate_invalid_emissions(validator):
    """Test validation of data with invalid emissions values."""
    invalid_data = pd.DataFrame(
        {
            "date": pd.date_range(
                "2020-01-01", "2020-03-31", freq="ME"
            ),  # Note: Changed M to ME
            "emissions": [-1, 2e9, "invalid"],
        }
    )

    issues = validator.validate_dataframe(invalid_data)

    # Check that type errors are caught
    assert len(issues["type_errors"]) > 0
    assert "emissions" in " ".join(issues["type_errors"])


def test_validate_numeric_emissions(validator):
    """Test validation of numeric but invalid emissions values."""
    invalid_data = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", "2020-03-31", freq="ME"),
            "emissions": [-1, 2e9, 100],  # All numeric values
        }
    )

    issues = validator.validate_dataframe(invalid_data)

    # Check that value range errors are caught
    assert len(issues["value_errors"]) > 0
    assert "emissions" in " ".join(issues["value_errors"])
