"""Core package for ghg-quant - A sustainability science data analysis toolkit."""

# Import commonly used functions for easier access
from .data.ingestion import DataIngestion

# Define package version
__version__ = "0.1.0"

# List publicly available functions
__all__ = [
    "DataIngestion",
]
