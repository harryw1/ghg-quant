import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from ...config import EPA_CONFIG
from .queries import EPA_GHG_QUERY


class EPADataSource:
    def __init__(
        self, state_code: str, year: Optional[int] = None, cleanup_batches: bool = True
    ):
        self.state_code = state_code
        self.year = year or datetime.now().year - 1
        self.graphql_url = "https://data.epa.gov/dmapservice/query/graphql"
        self.logger = logging.getLogger(__name__)
        self.cleanup_batches = cleanup_batches

    def _cleanup_batch_files(self, batch_files: List[Path]) -> None:
        """Clean up individual batch files after successful combination."""
        for batch_file in batch_files:
            try:
                if batch_file.exists():
                    batch_file.unlink()
                    self.logger.debug(f"Deleted batch file: {batch_file}")
            except Exception as e:
                self.logger.warning(f"Failed to delete batch file {batch_file}: {e}")

    def fetch_data(
        self,
        table: Optional[str] = None,
        filters: Optional[Dict] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Fetch data using GraphQL."""
        try:
            variables = {
                "offset": offset,
                "limit": limit,
                "orderBy": [{"tier1_co2_combustion_emissions": "desc"}],
            }

            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            payload = {"query": EPA_GHG_QUERY, "variables": variables}

            self.logger.debug(f"Making GraphQL request to {self.graphql_url}")
            self.logger.debug(f"Query payload: {payload}")

            response = requests.post(
                self.graphql_url,
                json=payload,
                headers=headers,
                timeout=EPA_CONFIG["timeout"],
            )

            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                self.logger.error(f"GraphQL errors: {data['errors']}")
                raise ValueError(f"GraphQL errors: {data['errors']}")

            records = data["data"]["ghg__c_fuel_level_information"]
            df = pd.DataFrame.from_records(records)

            if df.empty:
                self.logger.warning(
                    f"No data found for {self.state_code} in {self.year}"
                )
                return pd.DataFrame()

            batch_num = offset // limit
            data_dir = Path("data/raw")
            data_dir.mkdir(parents=True, exist_ok=True)

            batch_path = (
                data_dir / f"epa_{self.state_code}_{self.year}_batch_{batch_num}.csv"
            )
            df.to_csv(batch_path, index=False)

            self.logger.info(
                f"Successfully fetched {len(df)} records (batch {batch_num})"
            )
            return df

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching EPA data: {e}")
            raise ValueError(f"Failed to fetch EPA data: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise

    def get_data(
        self,
        table: Optional[str] = None,
        filters: Optional[Dict] = None,
        batch_size: int = 10,
    ) -> pd.DataFrame:
        """Get all data using pagination."""
        all_data = []
        offset = 0
        batch_files = []
        data_dir = Path("data/raw")
        data_dir.mkdir(parents=True, exist_ok=True)

        while True:
            df_batch = self.fetch_data(
                table=table, filters=filters, limit=batch_size, offset=offset
            )

            if df_batch.empty:
                break

            all_data.append(df_batch)

            batch_num = offset // batch_size
            batch_file = (
                data_dir / f"epa_{self.state_code}_{self.year}_batch_{batch_num}.csv"
            )
            batch_files.append(batch_file)

            offset += batch_size

            if len(df_batch) < batch_size:
                break

        if not all_data:
            self.logger.warning("No data collected from any batch")
            return pd.DataFrame()

        try:
            df_combined = pd.concat(all_data, ignore_index=True)

            combined_path = data_dir / f"epa_{self.state_code}_{self.year}_combined.csv"
            df_combined.to_csv(combined_path, index=False)

            self.logger.info(
                f"Combined {len(batch_files)} batches into {combined_path} "
                f"with {len(df_combined)} total records"
            )

            # Verify the combined file
            if not combined_path.exists():
                raise FileNotFoundError(
                    f"Failed to create combined file at {combined_path}"
                )

            df_verification = pd.read_csv(combined_path)
            if len(df_verification) != len(df_combined):
                raise ValueError(
                    f"Combined file verification failed: "
                    f"Expected {len(df_combined)} records but found {len(df_verification)}"
                )

            # Clean up batch files if requested
            if self.cleanup_batches:
                self.logger.info("Cleaning up batch files...")
                self._cleanup_batch_files(batch_files)

            return self.preprocess_data(df_combined)

        except Exception as e:
            self.logger.error(f"Error combining batch files: {e}")
            self.logger.info("Attempting to recover from batch files...")

            recovered_data = []
            for batch_file in batch_files:
                if batch_file.exists():
                    try:
                        df_batch = pd.read_csv(batch_file)
                        recovered_data.append(df_batch)
                    except Exception as batch_error:
                        self.logger.error(
                            f"Error reading batch file {batch_file}: {batch_error}"
                        )

            if recovered_data:
                df_recovered = pd.concat(recovered_data, ignore_index=True)
                self.logger.info(
                    f"Recovered {len(df_recovered)} records from batch files"
                )
                return self.preprocess_data(df_recovered)
            else:
                raise ValueError("Failed to recover data from batch files") from e

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the data."""
        # Add any preprocessing steps here
        return df
