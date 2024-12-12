"""Module to run the pipeline."""

# src/pipeline.py
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data.ingestion import DataIngestion
from src.data.validation import GHGDataValidator


class DataPipeline:
    """Data pipeline."""

    def __init__(self, data):
        """Initialize the pipeline."""
        self.data = data
        self.ingestion = DataIngestion(data)
        self.validator = GHGDataValidator()

    def run(self):
        """Run the pipeline."""
        self.it(self.ingestion.read_and_validate_all_files())

    def it(self, data):
        """Iterate over the data."""
        for filename, df in data.items():
            print(filename)
            print(df.head())
            print(self.validator.validate_dataframe(df))


if __name__ == "__main__":
    pipeline = DataPipeline(data="./data/raw/ghg-emissions.csv")
    pipeline.run()
