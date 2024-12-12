# src/data/ingestion.py
"""Module to ingest data from raw data files."""

import logging
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd

from .validation import GHGDataValidator


class DataIngestion:
    """Class to ingest data from raw data files."""

    def __init__(self, raw_data_path: Union[str, Path]):
        """Initialize data ingestion with path to raw data.

        Args:
            raw_data_path: Path to directory containing raw data files

        """
        self.raw_data_path = Path(raw_data_path)
        self.validator = GHGDataValidator()
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging for the ingestion process."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(__name__)

    def read_file(self, filename: str, validate: bool = True) -> pd.DataFrame:
        """Read a single data file and optionally validate it.

        Args:
            filename: Name of file to read
            validate: Whether to validate the data after reading

        Returns:
            DataFrame containing validated file contents

        Raises:
            ValueError: If validation fails and issues are found

        """
        file_path = self.raw_data_path / filename
        self.logger.info(f"Reading file: {file_path}")

        # Read the file
        if file_path.suffix == ".csv":
            df = pd.read_csv(file_path)
        elif file_path.suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

        # Validate the data if requested
        if validate:
            self.logger.info(f"Validating data from {filename}")
            validation_issues = self.validator.validate_dataframe(df)

            # Check if there are any validation issues
            has_issues = any(issues for issues in validation_issues.values())

            if has_issues:
                issue_details = "\n".join(
                    f"{category}: {', '.join(issues)}"
                    for category, issues in validation_issues.items()
                    if issues
                )
                self.logger.error(f"Validation failed for {filename}:\n{issue_details}")
                raise ValueError(f"Data validation failed for {filename}")

            self.logger.info(f"Validation successful for {filename}")

        return df

    def read_and_validate_all_files(self) -> Dict[str, pd.DataFrame]:
        """Read and validate all available files in the raw data directory.

        Returns:
            Dictionary mapping filenames to their validated DataFrames

        """
        valid_data = {}
        for filename in self.list_available_files():
            try:
                df = self.read_file(filename, validate=True)
                valid_data[filename] = df
            except ValueError as e:
                self.logger.error(f"Skipping {filename}: {str(e)}")
                continue

        return valid_data

    def list_available_files(self) -> List[str]:
        """List all available data files in the raw data directory."""
        return [
            f.name
            for f in self.raw_data_path.glob("*")
            if f.suffix in [".csv", ".xlsx", ".xls"]
        ]
