# src/data/sources/epa.py
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from ...config import EPA_CONFIG, EPA_TABLES_QUERY
from .queries import EPA_GHG_QUERY, SCHEMA_QUERY


class EPADataSource:
    def __init__(
        self, state_code: str, year: Optional[int] = None, cleanup_batches: bool = True
    ):
        self.state_code = state_code
        self.year = year or datetime.now().year - 1
        self.graphql_url = "https://data.epa.gov/dmapservice/query/graphql"
        self.logger = logging.getLogger(__name__)
        self.cleanup_batches = cleanup_batches

    def discover_available_tables(self):
        """Discover available tables."""
        try:
            response = requests.post(
                self.graphql_url,
                json={"query": EPA_TABLES_QUERY},
                headers={"Content-Type": "application/json"},
                timeout=EPA_CONFIG["timeout"],
            )
            return response.json()
        except Exception as e:
            self.logger.error(f"Error discovering available tables: {e}")
            return None

    def check_schema(self):
        """Debug method to check available schema."""
        try:
            response = requests.post(
                self.graphql_url,
                json={"query": SCHEMA_QUERY},
                headers={"Content-Type": "application/json"},
                timeout=EPA_CONFIG["timeout"],
            )
            return response.json()
        except Exception as e:
            self.logger.error(f"Error checking schema: {e}")
            return None

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
        limit: int = 50000,  # Set a high limit to attempt to fetch all data
        offset: int = 0,
    ) -> pd.DataFrame:
        """Fetch data using GraphQL."""
        self.discover_available_tables()

        try:
            variables = {
                "offset": offset,
                "limit": limit,
                "state": self.state_code,
                "year": self.year,
            }

            self.logger.debug(f"Making GraphQL request with variables: {variables}")

            response = requests.post(
                self.graphql_url,
                json={"query": EPA_GHG_QUERY, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=EPA_CONFIG["timeout"],
            )

            self.logger.debug(f"Response status: {response.status_code}")
            self.logger.debug(f"Response content: {response.text}")

            if response.status_code != 200:
                self.logger.error(
                    f"API Error: {response.status_code} - {response.text}"
                )
                raise ValueError(
                    f"API request failed with status {response.status_code}"
                )

            data = response.json()

            if "errors" in data:
                self.logger.error(f"GraphQL errors: {data['errors']}")
                raise ValueError(f"GraphQL errors: {data['errors']}")

            records = data.get("data", {}).get("ghg__rlps_ghg_emitter_sector", [])

            if not records:
                self.logger.warning(
                    f"No data found for state {self.state_code} (offset: {offset})"
                )
                return pd.DataFrame()

            # Create DataFrame from records
            df = pd.DataFrame.from_records(records)

            self.logger.info(
                f"Successfully fetched {len(df)} records for {self.state_code}"
            )
            return df

        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
            raise

        try:
            variables = {
                "offset": offset,
                "limit": limit,
                "state_name": self.state_code,
            }

            self.logger.debug(f"Making GraphQL request with variables: {variables}")

            response = requests.post(
                self.graphql_url,
                json={"query": EPA_GHG_QUERY, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=EPA_CONFIG["timeout"],
            )

            self.logger.debug(f"Response status: {response.status_code}")
            self.logger.debug(f"Response content: {response.text}")

            if response.status_code != 200:
                self.logger.error(
                    f"API Error: {response.status_code} - {response.text}"
                )
                raise ValueError(
                    f"API request failed with status {response.status_code}"
                )

            data = response.json()

            if "errors" in data:
                self.logger.error(f"GraphQL errors: {data['errors']}")
                raise ValueError(f"GraphQL errors: {data['errors']}")

            records = data.get("data", {}).get("ghg__rlps_ghg_emitter_gas", [])

            if not records:
                self.logger.warning(
                    f"No data found for state {self.state_code} (offset: {offset})"
                )
                return pd.DataFrame()

            # Create DataFrame from records
            df = pd.DataFrame.from_records(records)

            # Do initial preprocessing here
            if not df.empty:
                # Rename columns
                df = df.rename(
                    columns={"co2e_emission": "emissions", "state_name": "state"}
                )

                # Filter by year if specified
                if self.year is not None:
                    df = df[df["year"] == self.year]

            self.logger.info(
                f"Successfully fetched {len(df)} records for {self.state_code} "
                f"(batch {offset//limit})"
            )
            return df

        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}", exc_info=True)
            raise

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the data into standard format."""
        if df is not None and not df.empty:
            # Rename columns
            column_mapping = {
                "co2e_emission": "emissions",
                "sector_name": "industry",  # Map sector to industry for consistency
                "facility_name": "facility",
            }
            # Apply mapping for columns that exist
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns and new_col not in df.columns:
                    df = df.rename(columns={old_col: new_col})

            # Convert numeric columns
            numeric_columns = {
                "year": "int64",
                "emissions": "float64",
                "latitude": "float64",
                "longitude": "float64",
            }

            for col, dtype in numeric_columns.items():
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    df[col] = df[col].astype(dtype)

            # Create combined sector field if needed
            if all(col in df.columns for col in ["sector_name", "subsector_name"]):
                df["sector_full"] = df["sector_name"] + " - " + df["subsector_name"]

            # Add date column based on year
            if "year" in df.columns and "date" not in df.columns:
                df["date"] = pd.to_datetime(df["year"].astype(str), format="%Y")

            # Remove any duplicate records
            df = df.drop_duplicates()

            # Sort by year and emissions
            if all(col in df.columns for col in ["year", "emissions"]):
                df = df.sort_values(["year", "emissions"], ascending=[False, False])

            self.logger.debug(f"Preprocessed DataFrame columns: {df.columns.tolist()}")

        return df
