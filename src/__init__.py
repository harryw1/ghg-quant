# src/__init__.py
"""Core package for ghg-quant - A sustainability science data analysis toolkit."""

from .config import EPA_CONFIG
from .data.ingestion import DataIngestion

__version__ = "0.1.0"

__all__ = [
    "DataIngestion",
    "EPA_CONFIG",
]
