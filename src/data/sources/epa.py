# src/data/sources/epa.py
import logging
from typing import Dict, Optional

import pandas as pd
import requests

from ...config import EPA_CONFIG


class EPADataSource:
    def __init__(self, state_code: str, year: Optional[int] = None):
        """Initialize EPA data source."""
        self.state_code = state_code
        self.year = year
        self.logger = logging.getLogger(__name__)

    def _build_url(self, table: str, filters: Dict = None, limit: int = None) -> str:
        """Build URL for EPA efservice API."""
        url_parts = [EPA_CONFIG["base_url"], table]

        if filters:
            for key, value in filters.items():
                url_parts.extend([key, "equals", str(value)])
                if key != list(filters.keys())[-1]:  # If not last filter
                    url_parts.append("and")

        if limit:
            url_parts.append(f"0:{limit}")

        url_parts.append("json")
        return "/".join(url_parts)

    def fetch_data(self, filters: Optional[Dict] = None) -> pd.DataFrame:
        """Fetch and join facility and emissions data."""
        try:
            # First get facilities data
            facility_filters = {"state": self.state_code}
            if self.year:
                facility_filters["year"] = self.year

            facilities_url = self._build_url(
                table=EPA_CONFIG["tables"]["facilities"],
                filters=facility_filters,
                limit=EPA_CONFIG["batch_size"],
            )

            self.logger.debug(f"Fetching facilities data from: {facilities_url}")
            facilities_response = requests.get(
                facilities_url, timeout=EPA_CONFIG["timeout"]
            )
            facilities_response.raise_for_status()
            facilities_df = pd.DataFrame(facilities_response.json())

            if facilities_df.empty:
                self.logger.warning(f"No facilities found for {self.state_code}")
                return pd.DataFrame()

            # Get facility IDs to fetch emissions data
            facility_ids = facilities_df["facility_id"].unique()

            # Fetch emissions data for all facilities
            emissions_df = pd.DataFrame()
            for facility_id in facility_ids:
                emissions_filters = {"facility_id": facility_id}
                if self.year:
                    emissions_filters["year"] = self.year

                emissions_url = self._build_url(
                    table=EPA_CONFIG["tables"]["emissions"],
                    filters=emissions_filters,
                    limit=EPA_CONFIG["batch_size"],
                )

                try:
                    emissions_response = requests.get(
                        emissions_url, timeout=EPA_CONFIG["timeout"]
                    )
                    emissions_response.raise_for_status()
                    facility_emissions = pd.DataFrame(emissions_response.json())
                    emissions_df = pd.concat(
                        [emissions_df, facility_emissions], ignore_index=True
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Error fetching emissions for facility {facility_id}: {e}"
                    )

            # Merge facility and emissions data
            if not emissions_df.empty:
                df = pd.merge(
                    facilities_df, emissions_df, on=["facility_id", "year"], how="left"
                )
            else:
                df = facilities_df

            return self.preprocess_data(df)

        except Exception as e:
            self.logger.error(f"Error fetching data: {e}", exc_info=True)
            raise

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the data into standard format."""
        if df is not None and not df.empty:
            # Rename columns to match expected format
            column_mapping = {
                "co2e_emission": "emissions",
                "facility_name": "facility",
                "reported_industry_types": "sector",
            }

            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})

            # Add date column from year
            if "year" in df.columns:
                df["date"] = pd.to_datetime(df["year"].astype(str), format="%Y")

            # Clean up county names
            if "county" in df.columns:
                df["county"] = (
                    df["county"].str.upper().str.replace(" COUNTY", "").str.strip()
                )

            # Convert numeric columns
            numeric_cols = {
                "year": "int64",
                "latitude": "float64",
                "longitude": "float64",
                "emissions": "float64",
            }

            for col, dtype in numeric_cols.items():
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Remove duplicates
            df = df.drop_duplicates()

            # Sort by year
            if "year" in df.columns:
                df = df.sort_values("year", ascending=False)

            self.logger.debug(f"Preprocessed DataFrame columns: {df.columns.tolist()}")

        return df
