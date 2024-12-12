# tests/test_ingestion.py
"""Test ingestion module."""

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data.ingestion import DataIngestion


# Setup a test fixture for sample data
@pytest.fixture
def sample_data_dir(tmp_path):
    """Create a temporary directory with multiple test files."""
    # Create test CSV
    df1 = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "emissions": [100, 200]})
    csv_path = tmp_path / "test1.csv"
    df1.to_csv(csv_path, index=False)
    print(f"Created CSV file at: {csv_path}, exists: {csv_path.exists()}")

    # Create test Excel file
    df2 = pd.DataFrame({"date": ["2024-01-03", "2024-01-04"], "emissions": [300, 400]})
    excel_path = tmp_path / "test2.xlsx"
    df2.to_excel(
        excel_path, index=False, engine="openpyxl"
    )  # Explicitly specify engine
    print(f"Created Excel file at: {excel_path}, exists: {excel_path.exists()}")

    # Create an empty file
    empty_path = tmp_path / "empty.csv"
    empty_path.touch()
    print(f"Created empty file at: {empty_path}, exists: {empty_path.exists()}")

    # List all files in directory
    print("\nAll files in directory:")
    for file in tmp_path.iterdir():
        print(f"- {file.name}")

    return tmp_path


def test_list_available_files(sample_data_dir):
    """Test that we can list available data files."""
    ingestor = DataIngestion(sample_data_dir)
    files = ingestor.list_available_files()
    assert len(files) == 3
    assert "test1.csv" in files
    assert "test2.xlsx" in files
    assert "empty.csv" in files


def test_read_csv_file(sample_data_dir):
    """Test reading a CSV file."""
    ingestor = DataIngestion(sample_data_dir)
    df = ingestor.read_file("test1.csv")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
    assert list(df.columns) == ["date", "emissions"]


def test_initialization():
    """Test DataIngestion initialization."""
    ingestor = DataIngestion("data/raw")
    assert isinstance(ingestor.raw_data_path, Path)
    assert str(ingestor.raw_data_path) == "data/raw"


def test_read_excel_file(sample_data_dir):
    """Test reading an Excel file."""
    ingestor = DataIngestion(sample_data_dir)
    df = ingestor.read_file("test2.xlsx")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
    assert list(df.columns) == ["date", "emissions"]


def test_invalid_file_format(sample_data_dir):
    """Test handling of invalid file formats."""
    ingestor = DataIngestion(sample_data_dir)
    with pytest.raises(ValueError, match="Unsupported file format"):
        ingestor.read_file("nonexistent.txt")


def test_file_not_found(sample_data_dir):
    """Test handling of non-existent files."""
    ingestor = DataIngestion(sample_data_dir)
    with pytest.raises(FileNotFoundError):
        ingestor.read_file("nonexistent.csv")


def test_empty_file(sample_data_dir):
    """Test handling of empty files."""
    ingestor = DataIngestion(sample_data_dir)
    with pytest.raises(pd.errors.EmptyDataError):
        ingestor.read_file("empty.csv")
