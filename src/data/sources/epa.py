import logging
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import requests

from ...config import EPA_CONFIG
from .queries import EPA_GHG_QUERY


class EPADataSource:
    def __init__(self, state_code: str, year: Optional[int] = None):
        self.state_code = state_code
        self.year = year or datetime.now().year - 1
        self.graphql_url = (
            "https://data.epa.gov/dmapservice/query/graphql"  # Updated endpoint
        )
        self.logger = logging.getLogger(__name__)

    def fetch_data(
        self,
        table: Optional[str] = None,
        filters: Optional[Dict] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Fetch data using GraphQL."""
        try:
            # Construct GraphQL variables with orderBy
            variables = {
                "offset": offset,
                "limit": limit,
                "orderBy": [{"tier1_co2_combustion_emissions": "desc"}],
            }

            # Make GraphQL request
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

            # Check for errors in response
            if "errors" in data:
                self.logger.error(f"GraphQL errors: {data['errors']}")
                raise ValueError(f"GraphQL errors: {data['errors']}")

            # Extract records from response
            records = data["data"]["ghg__c_fuel_level_information"]

            # Convert to DataFrame
            df = pd.DataFrame.from_records(records)

            if df.empty:
                self.logger.warning(
                    f"No data found for {self.state_code} in {self.year}"
                )
                return pd.DataFrame()

            # Save raw data for debugging
            raw_data_path = f"data/raw/epa_{self.state_code}_{self.year}.csv"
            df.to_csv(raw_data_path, index=False)

            self.logger.info(f"Successfully fetched {len(df)} records")
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
        batch_size: int = 10,  # Updated batch size to match API limit
    ) -> pd.DataFrame:
        """Get all data using pagination."""
        all_data = []
        offset = 0

        while True:
            df_batch = self.fetch_data(
                table=table, filters=filters, limit=batch_size, offset=offset
            )

            if df_batch.empty:
                break

            all_data.append(df_batch)
            offset += batch_size

            # Break if we got fewer records than the batch size
            if len(df_batch) < batch_size:
                break

        if not all_data:
            return pd.DataFrame()

        # Combine all batches and preprocess
        df_combined = pd.concat(all_data, ignore_index=True)
        return self.preprocess_data(df_combined)
