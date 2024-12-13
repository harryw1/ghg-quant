# src/data/ingestion.py
import logging
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd

from .sources.base import DataSource
from .sources.epa import EPADataSource
from .validation import GHGDataValidator


class DataIngestion:
    """Class to ingest data from various sources."""

    def __init__(
        self,
        source: Union[str, Path, DataSource],
        state_code: Optional[str] = None,
        year: Optional[int] = None,
    ):
        """Initialize data ingestion.

        Args:
            source: Data source or path to data file
            state_code: State code for EPA data (e.g., 'NJ')
            year: Year to fetch data for
        """
        self.validator = GHGDataValidator()
        self._setup_logging()

        if isinstance(source, str) and source.lower() == "epa":
            self.source = EPADataSource(state_code=state_code, year=year)
            self.raw_data_path = None
        elif isinstance(source, DataSource):
            self.source = source
            self.raw_data_path = None
        else:
            self.source = None
            self.raw_data_path = Path(source)

    def _setup_logging(self):
        """Configure logging for the ingestion process."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(__name__)

    def read_data(
        self,
        table: Optional[str] = None,
        filters: Optional[Dict] = None,
        validate: bool = True,
    ) -> pd.DataFrame:
        """Read data from the configured source."""
        if isinstance(self.source, EPADataSource):
            self.logger.info("Reading EPA data from GraphQL endpoint")
            df = self.source.get_data(table=table, filters=filters)
        elif self.source is not None:
            self.logger.info("Reading data from DataSource")
            df = self.source.get_data()
        else:
            self.logger.info("Reading data from files")
            return self.read_and_validate_all_files()

        if validate:
            self.logger.info("Validating data")
            validation_issues = self.validator.validate_dataframe(df)

            has_issues = any(issues for issues in validation_issues.values())
            if has_issues:
                issue_details = "\n".join(
                    f"{category}: {', '.join(issues)}"
                    for category, issues in validation_issues.items()
                    if issues
                )
                self.logger.error(f"Validation failed:\n{issue_details}")
                raise ValueError("Data validation failed")

        return df
