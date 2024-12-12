import os
import sys

import matplotlib.pyplot as plt
import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.visualizations.plots import create_emissions_plots


@pytest.fixture
def sample_data():
    """Create sample emissions data for testing."""
    dates = pd.date_range(start="2020-01-01", end="2023-12-31", freq="ME")
    df = pd.DataFrame({"date": dates, "emissions": range(len(dates))})
    return df


@pytest.fixture
def output_dir(tmp_path):
    """Create temporary directory for test outputs."""
    return tmp_path


def test_create_emissions_plots_returns_figures(sample_data):
    """Test that create_emissions_plots returns dictionary of figures."""
    figures = create_emissions_plots(sample_data)

    assert isinstance(figures, dict)
    assert len(figures) == 4
    assert all(isinstance(fig, plt.Figure) for fig in figures.values())

    # Check all expected plots are present
    expected_plots = {"time_series", "monthly_dist", "annual_trend", "rolling_avg"}
    assert set(figures.keys()) == expected_plots


def test_create_emissions_plots_saves_files(sample_data, output_dir):
    """Test that create_emissions_plots saves files when output_dir is provided."""
    create_emissions_plots(sample_data, output_dir=output_dir)

    # Check that files were created
    expected_files = {
        "time_series.png",
        "monthly_dist.png",
        "annual_trend.png",
        "rolling_avg.png",
    }
    actual_files = set(os.listdir(output_dir))
    assert expected_files == actual_files


def test_create_emissions_plots_handles_empty_data():
    """Test that create_emissions_plots handles empty DataFrame appropriately."""
    empty_df = pd.DataFrame(columns=["date", "emissions"])
    with pytest.raises(ValueError, match="Cannot create plots from empty DataFrame"):
        create_emissions_plots(empty_df)


def test_create_emissions_plots_handles_missing_columns():
    """Test that function raises error when required columns are missing."""
    invalid_df = pd.DataFrame({"wrong_column": [1, 2, 3]})
    with pytest.raises(
        ValueError, match="DataFrame must contain 'date' and 'emissions' columns"
    ):
        create_emissions_plots(invalid_df)


def test_create_emissions_plots_handles_single_row():
    """Test that function raises error when DataFrame has insufficient data."""
    single_row_df = pd.DataFrame({"date": ["2023-01-01"], "emissions": [100]})
    with pytest.raises(
        ValueError, match="DataFrame must contain at least 2 rows of data"
    ):
        create_emissions_plots(single_row_df)


@pytest.fixture(autouse=True)
def close_figures():
    """Automatically close matplotlib figures after each test."""
    yield
    plt.close("all")
