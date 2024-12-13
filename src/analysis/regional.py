import logging
from pathlib import Path
from typing import Dict

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


class RegionalAnalysis:
    """Regional emissions analysis."""

    def __init__(self, df: pd.DataFrame):
        """Initialize regional analysis."""
        self.df = df
        self.logger = logging.getLogger(__name__)

    def county_statistics(self) -> Dict[str, pd.Series]:
        """Calculate county-level statistics."""
        stats = {
            "total_emissions": self.df.groupby("county")["emissions"].sum(),
            "avg_emissions": self.df.groupby("county")["emissions"].mean(),
            "facility_count": self.df.groupby("county")["facility"].nunique(),
            "industry_count": self.df.groupby("county")["industry"].nunique(),
            "emissions_per_facility": (
                self.df.groupby("county")["emissions"].sum()
                / self.df.groupby("county")["facility"].nunique()
            ),
        }
        return stats

    def industry_analysis(self) -> Dict[str, pd.DataFrame]:
        """Analyze emissions by industry."""
        return {
            "by_industry": self.df.groupby("industry")["emissions"].agg(
                ["sum", "mean", "count"]
            ),
            "industry_by_county": pd.pivot_table(
                self.df,
                values="emissions",
                index="county",
                columns="industry",
                aggfunc="sum",
                fill_value=0,
            ),
        }

    def temporal_analysis(self) -> Dict[str, pd.Series]:
        """Analyze temporal trends."""
        return {
            "monthly_trend": self.df.groupby(self.df["date"].dt.to_period("M"))[
                "emissions"
            ].sum(),
            "seasonal_pattern": self.df.groupby(self.df["date"].dt.month)[
                "emissions"
            ].mean(),
            "year_over_year": self.df.groupby(self.df["date"].dt.year)[
                "emissions"
            ].sum(),
        }

    def create_visualization_suite(self, output_dir: str) -> None:
        """Create a suite of visualizations."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 1. County emissions map
        self._create_county_map(output_path / "county_map.png")

        # 2. Industry breakdown
        self._create_industry_plot(output_path / "industry_breakdown.png")

        # 3. Temporal trends
        self._create_temporal_plots(output_path / "temporal_trends.png")

        # 4. Facility distribution
        self._create_facility_plots(output_path / "facility_distribution.png")

    def _create_county_map(self, output_path: Path) -> None:
        """Create county-level emissions map."""
        try:
            # Load county boundaries (you'll need to provide this data)
            counties_gdf = gpd.read_file(
                f"data/geo/{self.df['state'].iloc[0]}_counties.geojson"
            )

            # Merge with emissions data
            emissions_by_county = self.df.groupby("county")["emissions"].sum()
            counties_gdf["emissions"] = counties_gdf["COUNTY"].map(emissions_by_county)

            # Create map
            fig, ax = plt.subplots(figsize=(15, 10))
            counties_gdf.plot(
                column="emissions",
                ax=ax,
                legend=True,
                legend_kwds={"label": "Total Emissions (metric tons CO2e)"},
                missing_kwds={"color": "lightgrey"},
                cmap="YlOrRd",
            )

            plt.title(f"GHG Emissions by County - {self.df['state'].iloc[0]}")
            plt.savefig(output_path, bbox_inches="tight", dpi=300)
            plt.close()

        except Exception as e:
            self.logger.error(f"Error creating county map: {e}")

    def _create_industry_plot(self, output_path: Path) -> None:
        """Create industry analysis plots."""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))

        # Top industries by emissions
        industry_emissions = (
            self.df.groupby("industry")["emissions"].sum().sort_values(ascending=True)
        )
        industry_emissions.plot(kind="barh", ax=ax1)
        ax1.set_title("Total Emissions by Industry")

        # Industry composition by county
        industry_county = pd.crosstab(
            self.df["county"],
            self.df["industry"],
            values=self.df["emissions"],
            aggfunc="sum",
        )
        industry_county.plot(kind="bar", stacked=True, ax=ax2)
        ax2.set_title("Industry Composition by County")
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig(output_path, bbox_inches="tight", dpi=300)
        plt.close()

    def _create_temporal_plots(self, output_path: Path) -> None:
        """Create temporal analysis plots."""
        temporal = self.temporal_analysis()

        fig, axes = plt.subplots(3, 1, figsize=(15, 15))

        # Monthly trend
        temporal["monthly_trend"].plot(ax=axes[0])
        axes[0].set_title("Monthly Emissions Trend")

        # Seasonal pattern
        temporal["seasonal_pattern"].plot(kind="bar", ax=axes[1])
        axes[1].set_title("Average Emissions by Month")

        # Year over year
        temporal["year_over_year"].plot(kind="bar", ax=axes[2])
        axes[2].set_title("Annual Emissions")

        plt.tight_layout()
        plt.savefig(output_path, bbox_inches="tight", dpi=300)
        plt.close()

    def _create_facility_plots(self, output_path: Path) -> None:
        """Create facility-level analysis plots."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Facility count by county
        facility_counts = self.df.groupby("county")["facility"].nunique()
        facility_counts.plot(kind="bar", ax=ax1)
        ax1.set_title("Number of Facilities by County")

        # Emissions distribution
        sns.boxplot(data=self.df, x="county", y="emissions", ax=ax2)
        ax2.set_title("Emissions Distribution by County")
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig(output_path, bbox_inches="tight", dpi=300)
        plt.close()
