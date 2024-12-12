"""Module to plot data."""

# src/visualizations/plots.py
# src/visualizations/plots.py
import warnings
from typing import Dict, Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore", category=UserWarning, message=".*color palette.*")


def create_emissions_plots(
    df: pd.DataFrame, output_dir: Optional[str] = None
) -> Dict[str, plt.Figure]:
    """Create standard visualization suite for GHG emissions data.

    Args:
        df: DataFrame with 'date' and 'emissions' columns
        output_dir: Optional directory to save plots to

    Returns:
        dict: Dictionary of matplotlib figures

    Raises:
        ValueError: If DataFrame is empty or missing required columns
    """
    # Input validation
    if df.empty:
        raise ValueError("Cannot create plots from empty DataFrame")

    if not all(col in df.columns for col in ["date", "emissions"]):
        raise ValueError("DataFrame must contain 'date' and 'emissions' columns")

    if len(df) < 2:
        raise ValueError("DataFrame must contain at least 2 rows of data")

    # Ensure date column is datetime
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Define month order
    month_order = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    figures = {}

    # 1. Time series plot
    fig_ts, ax_ts = plt.subplots(figsize=(12, 6))
    sns.lineplot(data=df, x="date", y="emissions", ax=ax_ts)
    ax_ts.set_title("GHG Emissions Over Time", pad=20)
    ax_ts.set_xlabel("Date")
    ax_ts.set_ylabel("Emissions")
    fig_ts.tight_layout()
    figures["time_series"] = fig_ts

    # 2. Monthly distribution
    df["month"] = pd.Categorical(
        df["date"].dt.strftime("%B"), categories=month_order, ordered=True
    )
    fig_box, ax_box = plt.subplots(figsize=(12, 6))
    sns.boxplot(data=df, x="month", y="emissions", ax=ax_box)
    ax_box.set_title("Monthly Emissions Distribution", pad=20)

    # Properly set ticks and labels
    num_months = len(month_order)
    ax_box.set_xticks(range(num_months))  # Set the tick positions
    ax_box.set_xticklabels(month_order, rotation=45, ha="right")  # Set the tick labels

    # Adjust layout to prevent label cutoff
    fig_box.tight_layout()
    figures["monthly_dist"] = fig_box

    # 3. Annual trend
    annual_emissions = df.groupby(df["date"].dt.year)["emissions"].mean()
    fig_annual, ax_annual = plt.subplots(figsize=(10, 6))
    annual_emissions.plot(kind="bar", ax=ax_annual)
    ax_annual.set_title("Annual Average Emissions", pad=20)
    ax_annual.set_xlabel("Year")
    ax_annual.set_ylabel("Average Emissions")
    fig_annual.tight_layout()
    figures["annual_trend"] = fig_annual

    # 4. Rolling average
    window_size = min(6, len(df))
    df["rolling_avg"] = df["emissions"].rolling(window=window_size, center=True).mean()
    fig_roll, ax_roll = plt.subplots(figsize=(12, 6))
    sns.lineplot(
        data=df, x="date", y="emissions", label="Actual", alpha=0.5, ax=ax_roll
    )
    sns.lineplot(
        data=df,
        x="date",
        y="rolling_avg",
        label=f"{window_size}-month Rolling Average",
        ax=ax_roll,
    )
    ax_roll.set_title("Emissions with Rolling Average", pad=20)
    ax_roll.set_xlabel("Date")
    ax_roll.set_ylabel("Emissions")
    ax_roll.legend()
    fig_roll.tight_layout()
    figures["rolling_avg"] = fig_roll

    # Save figures if output directory is provided
    if output_dir:
        for name, fig in figures.items():
            fig.savefig(f"{output_dir}/{name}.png", bbox_inches="tight", dpi=300)

    return figures
