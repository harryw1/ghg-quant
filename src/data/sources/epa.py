# src/data/sources/epa.py
import pandas as pd

from .base import DataSource


class EPADataSource(DataSource):
    """EPA FLIGHT data source."""

    def __init__(self, state_code: str = None):
        """Initialize EPA data source."""
        self.state_code = state_code
        self.base_url = "https://ghgdata.epa.gov/ghgp/service/facilityDownload"

    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from EPA FLIGHT."""
        # For testing purposes, return dummy data
        dates = pd.date_range(start="2020-01-01", end="2023-12-31", freq="M")
        df = pd.DataFrame({
            'date': dates,
            'emissions': range(len(dates)),
            'facility': [f'Facility_{i}' for i in range(len(dates))],
            'county': ['County_' + str(i % 5) for i in range(len(dates))],
            'state': self.state_code,
            'industry': ['Industry_' + str(i % 3) for i in range(len(dates))]
        })
        return df

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess EPA data into standard format."""
        return df
