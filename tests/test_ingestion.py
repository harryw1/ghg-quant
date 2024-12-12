# tests/test_ingestion.py
"""Test ingestion module."""

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data.ingestion import DataIngestion

# Set up the test data path
TEST_DATA_PATH = Path("src/data/raw")

def test_data_ingestion_valid_file():
    """Test reading a valid GHG emissions file."""
    # Initialize data ingestion with test data path
    ingestion = DataIngestion(TEST_DATA_PATH)

    # Read the valid file
    df = ingestion.read_file("valid_ghg_emissions.csv")

    # Basic assertions
    assert isinstance(df, pd.DataFrame)
    assert "date" in df.columns
    assert "emissions" in df.columns
    assert len(df) > 0
    assert df["emissions"].dtype == "float64"

def test_data_ingestion_invalid_file():
    """Test reading an invalid GHG emissions file."""
    ingestion = DataIngestion(TEST_DATA_PATH)

    # The invalid file should raise a ValueError due to validation issues
    with pytest.raises(ValueError):
        ingestion.read_file("invalid_ghg_emissions.csv")

def test_list_available_files():
    """Test listing available files in the data directory."""
    ingestion = DataIngestion(TEST_DATA_PATH)
    files = ingestion.list_available_files()

    assert "valid_ghg_emissions.csv" in files
    assert "invalid_ghg_emissions.csv" in files
