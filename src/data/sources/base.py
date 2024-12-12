# src/data/sources/base.py
from abc import ABC, abstractmethod

import pandas as pd


class DataSource(ABC):
    """Abstract base class for data sources."""

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from source."""
        pass

    @abstractmethod
    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess data into standard format."""
        pass

    def get_data(self) -> pd.DataFrame:
        """Get and preprocess data."""
        df = self.fetch_data()
        return self.preprocess_data(df)
