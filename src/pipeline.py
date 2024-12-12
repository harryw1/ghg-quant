"""Module to run the pipeline."""
# src/pipeline.py

from src.analysis.calculations import calculate_metrics
from src.data.preprocess import clean_data, validate_data
from src.visualizations.plots import plot_data


class DataPipeline:
    """Data pipeline."""

    def __init__(self, data):
        """Initialize the pipeline."""
        self.data = "./data/input.csv"
        self.cleaned_data = None
        self.validated_data = None
        self.metrics_data = None
        self.plot_data = None

    def run(self):
        """Run the pipeline."""
        self.cleaned_data = clean_data(self.data)
        self.validated_data = validate_data(self.cleaned_data)
        self.metrics_data = calculate_metrics(self.validated_data)
        self.plot_data = plot_data(self.metrics_data)


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run()
