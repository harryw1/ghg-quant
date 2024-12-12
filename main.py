from pathlib import Path

from src.data.ingestion import DataIngestion
from src.data.sources.epa import EPADataSource
from src.visualizations.plots import create_emissions_plots


def analyze_state_emissions(state_code: str):
    """Analyze emissions for a specific state."""
    print(f"\nAnalyzing emissions for state: {state_code}")

    # Initialize EPA data source for state
    source = EPADataSource(state_code=state_code)

    # Initialize data ingestion with source
    ingestion = DataIngestion(source)

    try:
        # Read and validate data
        print("Fetching and validating data...")
        df = ingestion.read_data()

        # Set up output directory
        output_dir = Path("output") / state_code.lower()
        output_dir.mkdir(parents=True, exist_ok=True)

        print("\nData summary:")
        print(f"Total records: {len(df)}")
        print("\nFirst few rows:")
        print(df.head())

        # Create visualizations
        print("\nGenerating visualizations...")
        create_emissions_plots(df, output_dir=str(output_dir))

        print(f"\nOutputs saved to: {output_dir}")

        return df, None

    except Exception as e:
        print(f"\nError during analysis: {e}")
        return None, None

def main():
    """Main function to run the GHG emissions analysis."""
    print("\n=== GHG Emissions Analysis Tool ===")

    # Example: Analyze New Jersey emissions
    df, stats = analyze_state_emissions("NJ")

if __name__ == "__main__":
    main()
