# src/data/__init__.py
"""Data processing and cleaning functionality."""

from .ingestion import DataIngestion
from .validation import GHGDataValidator

__all__ = ["DataIngestion", "GHGDataValidator"]
