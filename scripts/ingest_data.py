"""Script that pulls data in from files."""

from pathlib import Path

from src.data.ingestion import DataIngestion


def main():
    """Ingests data from files and stores calls processing.

    This script reads data from files in the data/raw directory
    and displays information about the available files and their contents.
    """
    # Initialize ingestion with path to raw data
    data_path = Path("data/raw")
    ingestor = DataIngestion(data_path)

    # List available files
    print("Available files:")
    for file in ingestor.list_available_files():
        print(f"  - {file}")

    # Read each file
    for filename in ingestor.list_available_files():
        df = ingestor.read_file(filename)
        print(f"\nRead {filename}:")
        print(f"Shape: {df.shape}")
        print("Columns:", df.columns.tolist())


if __name__ == "__main__":
    main()
