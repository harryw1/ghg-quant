# generate_test_data.py
from pathlib import Path

import numpy as np
import pandas as pd

# Create data directory if it doesn't exist
data_dir = Path("../data/raw")
data_dir.mkdir(parents=True, exist_ok=True)

# Generate valid data
np.random.seed(42)
dates = pd.date_range(start="2020-01-01", end="2023-12-31", freq="M")
valid_data = pd.DataFrame(
    {
        "date": dates,
        "emissions": np.random.uniform(
            1000, 100000, len(dates)
        ),  # Random emissions between 1000 and 100000
    }
)

# Save valid data
valid_data.to_csv(data_dir / "valid_ghg_emissions.csv", index=False)

# Generate invalid data (with various issues)
invalid_dates = pd.date_range(
    start="2020-01-01", end="2025-12-31", freq="M"
)  # Including future dates
invalid_data = pd.DataFrame(
    {
        "date": invalid_dates,
        "emissions": np.random.uniform(
            -1000, 2e9, len(invalid_dates)
        ),  # Including negative values and values above max
    }
)

# Add some invalid format dates
invalid_data.loc[0:5, "date"] = "invalid_date"

# Save invalid data
invalid_data.to_csv(data_dir / "invalid_ghg_emissions.csv", index=False)

print("Test data generated successfully!")
