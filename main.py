import os
from pathlib import Path

from src.data.ingestion import DataIngestion
from src.visualizations.plots import create_emissions_plots


def main():
    """Main function to run the GHG emissions analysis."""
    print("\n=== GHG Emissions Analysis Tool ===\n")

    # Set up paths
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    data_dir = current_dir / "data" / "raw"
    output_dir = current_dir / "output"
    output_dir.mkdir(exist_ok=True)

    print(f"Reading data from: {data_dir}")
    print(f"Saving outputs to: {output_dir}\n")

    # Initialize data ingestion
    ingestion = DataIngestion(data_dir)

    # List available files
    print("Available data files:")
    for file in ingestion.list_available_files():
        print(f"- {file}")
    print()

    try:
        # Read and validate the valid data file
        print("Processing valid_ghg_emissions.csv...")
        df = ingestion.read_file("valid_ghg_emissions.csv")
        print("Data validation successful!")
        print(f"Number of records: {len(df)}")
        print("\nFirst few rows of the data:")
        print(df.head())
        print("\nBasic statistics:")
        print(df.describe())

        # Create visualizations
        print("\nGenerating plots...")
        figures = create_emissions_plots(df, output_dir=str(output_dir))
        print(f"Plots saved to: {output_dir}")
        print("Generated plots:")
        for plot_name in figures.keys():
            print(f"- {plot_name}.png")

        # Try to process invalid data file to demonstrate validation
        print("\nAttempting to process invalid_ghg_emissions.csv...")
        ingestion.read_file("invalid_ghg_emissions.csv")

    except ValueError as e:
        print(f"\nValidation Error: {e}")
    except Exception as e:
        print(f"\nAn error occurred: {e}")

    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()
