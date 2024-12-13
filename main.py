# main.py
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
    table: str = "ghg.gh_ghgrp_data",  # Add default table
) -> Tuple[Optional[pd.DataFrame], Optional[Dict]]:
    """Analyze emissions for a specific state."""
    logger = logging.getLogger(__name__)
    logger.info(f"Starting analysis for {state_code}")

    try:
        # Initialize data ingestion with EPA source
        ingestion = DataIngestion(source="epa", state_code=state_code, year=year)

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


def main():
    """Main function to run the GHG emissions analysis."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("analysis.log"), logging.StreamHandler()],
    )

    print("\n=== GHG Emissions Analysis Tool ===\n")

    state_code = input("Enter state code (e.g., NJ): ").upper()
    year = input("Enter year (press Enter for most recent): ")
    year = int(year) if year else None

    # Add table selection option
    table = input("Enter table name (press Enter for default GHG data): ")
    table = table if table else "ghg.gh_ghgrp_data"

    df, stats = analyze_state_emissions(state_code, year, table=table)

    if stats:
        print("\nAnalysis Summary:")
        print("\nTop 5 Counties by Emissions:")
        print(
            stats["county_stats"]["total_emissions"].sort_values(ascending=False).head()
        )

        print("\nTop 5 Industries by Emissions:")
        print(
            stats["industry_stats"]["by_industry"]
            .sort_values("sum", ascending=False)
            .head()
        )

        print(f"\nDetailed results saved to output/{state_code.lower()}/")


if __name__ == "__main__":
    main()
