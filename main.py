# main.py
import json
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from src.analysis.regional import RegionalAnalysis
from src.data.ingestion import DataIngestion


def analyze_state_emissions(
    state_code: str,
    year: Optional[int] = None,
    output_dir: Optional[str] = None,
    table: str = "ghg__rlps_ghg_emitter_sector",
) -> Tuple[Optional[pd.DataFrame], Optional[Dict]]:
    """Analyze emissions for a specific state."""
    logger = logging.getLogger(__name__)

    # Create state name mapping
    state_mapping = {
        "AL": "ALABAMA",
        "AK": "ALASKA",
        "AZ": "ARIZONA",
        "AR": "ARKANSAS",
        "CA": "CALIFORNIA",
        "CO": "COLORADO",
        "CT": "CONNECTICUT",
        "DE": "DELAWARE",
        "FL": "FLORIDA",
        "GA": "GEORGIA",
        "HI": "HAWAII",
        "ID": "IDAHO",
        "IL": "ILLINOIS",
        "IN": "INDIANA",
        "IA": "IOWA",
        "KS": "KANSAS",
        "KY": "KENTUCKY",
        "LA": "LOUISIANA",
        "ME": "MAINE",
        "MD": "MARYLAND",
        "MA": "MASSACHUSETTS",
        "MI": "MICHIGAN",
        "MN": "MINNESOTA",
        "MS": "MISSISSIPPI",
        "MO": "MISSOURI",
        "MT": "MONTANA",
        "NE": "NEBRASKA",
        "NV": "NEVADA",
        "NH": "NEW HAMPSHIRE",
        "NJ": "NEW JERSEY",
        "NM": "NEW MEXICO",
        "NY": "NEW YORK",
        "NC": "NORTH CAROLINA",
        "ND": "NORTH DAKOTA",
        "OH": "OHIO",
        "OK": "OKLAHOMA",
        "OR": "OREGON",
        "PA": "PENNSYLVANIA",
        "RI": "RHODE ISLAND",
        "SC": "SOUTH CAROLINA",
        "SD": "SOUTH DAKOTA",
        "TN": "TENNESSEE",
        "TX": "TEXAS",
        "UT": "UTAH",
        "VT": "VERMONT",
        "VA": "VIRGINIA",
        "WA": "WASHINGTON",
        "WV": "WEST VIRGINIA",
        "WI": "WISCONSIN",
        "WY": "WYOMING",
        "DC": "DISTRICT OF COLUMBIA",
        "PR": "PUERTO RICO",
        "VI": "VIRGIN ISLANDS",
        "GU": "GUAM",
        "AS": "AMERICAN SAMOA",
        "MP": "NORTHERN MARIANA ISLANDS",
    }

    # Convert state code to full name if available
    state_name = state_mapping.get(state_code.upper())
    if not state_name:
        logger.warning(f"Unknown state code: {state_code}")
        state_name = state_code.upper()

    logger.info(f"Starting analysis for {state_name}")

    try:
        # Initialize data ingestion with EPA source
        ingestion = DataIngestion(source="epa", state_code=state_code, year=year)

        # Check schema
        schema = ingestion.source.check_schema()
        print("Available schema:", json.dumps(schema, indent=2))

        # Create filters dict only with non-None values
        filters = {}
        if state_code:
            filters["state_code"] = state_code
        if year:
            filters["year"] = year

        # Read and validate data
        logger.info("Fetching and validating data...")
        df = ingestion.read_data(table=table, filters=filters)

        # Set up output directory
        output_dir = output_dir or f"output/{state_code.lower()}"
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        if df is not None:
            print("\nAvailable columns in the data:")
            print(df.columns.tolist())

        # Check if we got any data
        if df.empty:
            logger.warning(f"No data found for {state_name}")
            return df, {"county_stats": {}, "industry_stats": {}, "temporal_stats": {}}

        # Create regional analysis
        logger.info("Performing regional analysis...")
        analysis = RegionalAnalysis(df)

        # Generate statistics
        stats = {
            "county_stats": analysis.county_statistics(),
            "industry_stats": analysis.industry_analysis(),
            "temporal_stats": analysis.temporal_analysis(),
        }

        # Create visualizations
        logger.info("Generating visualizations...")
        analysis.create_visualization_suite(output_dir)

        # Save statistics to CSV
        for name, stat_df in stats.items():
            if isinstance(stat_df, (pd.DataFrame, pd.Series)):
                stat_df.to_csv(output_path / f"{name}.csv")

        logger.info(f"Analysis complete. Results saved to {output_path}")
        return df, stats

    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        return None, None

    # Check if we got any data
    if df.empty:
        logger.warning(f"No data found for {state_name}")
        return None, {
            "county_stats": {},
            "industry_stats": {},
            "temporal_stats": {},
        }


def main():
    """Main function to run the GHG emissions analysis."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("analysis.log"), logging.StreamHandler()],
    )

    print("\n=== GHG Emissions Analysis Tool ===\n")

    # Add input validation
    while True:
        state_code = input("Enter state code (e.g., NJ): ").strip().upper()
        if state_code and len(state_code) == 2:
            break
        print("Please enter a valid 2-letter state code")
    year = input("Enter year (press Enter for most recent): ")
    year = int(year) if year else None

    # Add table selection option
    table = input("Enter table name (press Enter for default GHG data): ").strip()
    table = table if table else "ghg__rlps_ghg_emitter_sector"

    df, stats = analyze_state_emissions(state_code, year, table=table)

    if df is None or df.empty:
        print("\nNo data available for analysis")
        return

    print("\nAnalysis Summary:")

    try:
        if "county_stats" in stats and "total_emissions" in stats["county_stats"]:
            print("\nTop 5 Counties by Emissions:")
            print(
                stats["county_stats"]["total_emissions"]
                .sort_values(ascending=False)
                .head()
            )
        else:
            print("\nNo county emissions data available")

        if "industry_stats" in stats and "by_sector" in stats["industry_stats"]:
            print("\nTop 5 Industries by Emissions:")
            print(
                stats["industry_stats"]["by_sector"]
                .sort_values("sum", ascending=False)
                .head()
            )
        else:
            print("\nNo industry emissions data available")

        print(f"\nDetailed results saved to output/{state_code.lower()}/")
    except Exception as e:
        print(f"\nError displaying results: {e}")


if __name__ == "__main__":
    main()
