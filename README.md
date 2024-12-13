# ghg-quant

A Python toolkit for analyzing and visualizing greenhouse gas (GHG) emissions data from EPA and state-level sources.

## To Do

This project is severly lacking in full functionality, so please take everything with a grain of salt as it gets built out.

## Overview

ghg-quant is a comprehensive data analysis toolkit designed to process, analyze, and visualize greenhouse gas emissions data. It provides functionality to:

- Fetch data from EPA's emissions database
- Process and validate emissions data
- Perform regional and temporal analysis
- Generate insightful visualizations
- Export analysis results

## Project Structure

```
ghg-quant/
├── data/
│   └── geo/                     # Geographic data files for mapping
├── src/
│   ├── analysis/               # Analysis modules
│   │   ├── calculations.py     # Core metric calculations
│   │   └── regional.py        # Regional analysis functionality
│   ├── data/                  # Data processing modules
│   │   ├── sources/          # Data source implementations
│   │   ├── ingestion.py      # Data ingestion functionality
│   │   └── validation.py     # Data validation utilities
│   ├── visualizations/       # Visualization modules
│   │   └── plots.py         # Plotting functions
│   ├── config.py           # Configuration settings
│   └── __init__.py        # Package initialization
└── main.py               # Command-line interface
```

## Features

- **Data Ingestion**: Flexible data ingestion from multiple sources, including EPA's efservice API
- **Regional Analysis**: County-level and state-level emissions analysis
- **Temporal Analysis**: Time series analysis of emissions trends
- **Industry Analysis**: Sector-based emissions analysis
- **Visualization Suite**: Comprehensive set of visualizations including:
  - County-level emissions maps
  - Time series plots
  - Industry breakdown charts
  - Facility distribution analysis

## Installation

```bash
git clone https://github.com/harryw1/ghg-quant.git
cd ghg-quant
pip install .
```

## Usage

Run the main analysis script:

```bash
python main.py
```

You'll be prompted to:
1. Enter a state code (e.g., "NJ" for New Jersey)
2. Enter a year (optional)
3. Select a data table (optional)

The script will:
- Fetch emissions data
- Perform analysis
- Generate visualizations
- Save results to the `output/{state}` directory

## Requirements

- Python 3.8+
- pandas
- geopandas
- matplotlib
- seaborn
- requests

## Data Sources

The toolkit primarily uses data from:
- EPA's Greenhouse Gas Reporting Program (GHGRP)
- State-level environmental agencies (where available)

## Output

Analysis results are saved in the following formats:
- CSV files containing statistical analysis
- PNG files with visualizations
- Log file with analysis details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
