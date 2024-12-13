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
        """Initialize data ingestion."""
        self.validator = GHGDataValidator()
        self._setup_logging()

        if isinstance(source, str) and source.lower() == "epa":
            if not state_code:
                raise ValueError("state_code is required for EPA data source")
            self.source = EPADataSource(state_code=state_code, year=year)
            self.raw_data_path = None
        elif isinstance(source, DataSource):
            self.source = source
            self.raw_data_path = None
        else:
            self.source = None
            self.raw_data_path = Path(source)

    def read_data(
        self,
        filters: Optional[Dict] = None,
        validate: bool = True,
    ) -> pd.DataFrame:
        """Read data from the configured source."""
        if isinstance(self.source, EPADataSource):
            self.logger.info("Reading EPA data from efservice endpoint")
            df = self.source.fetch_data(filters=filters)

            if validate:
                self.logger.info("Validating data")
                validation_issues = self.validator.validate_dataframe(df)
                if any(issues for issues in validation_issues.values()):
                    issue_details = "\n".join(
                        f"{category}: {', '.join(issues)}"
                        for category, issues in validation_issues.items()
                        if issues
                    )
                    self.logger.warning(f"Validation issues found:\n{issue_details}")

            return df

    def _setup_logging(self):
        """Configure logging for the ingestion process."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(__name__)
