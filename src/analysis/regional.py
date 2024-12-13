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
        self.df = df
        self.logger = logging.getLogger(__name__)

        if self.df.empty:
            self.logger.warning("Initialized with empty DataFrame")

        # Log available columns for debugging
        self.logger.info(f"Available columns: {df.columns.tolist()}")

        # Update required columns mapping
        required_columns = {
            "emissions": ["emissions", "ghg_quantity", "co2e_emission"],
            "sector": ["sector", "reported_industry_types", "industry_type"],
            "county": ["county"],
            "facility": ["facility_name", "facility"],
            "date": ["date", "year"],  # Add year as alternative for date
        }

        # Make validation more flexible
        self.column_mapping = {}
        for key, alternatives in required_columns.items():
            found_col = next((col for col in alternatives if col in df.columns), None)
            if found_col:
                self.column_mapping[key] = found_col
            else:
                self.logger.warning(
                    f"No column found for {key}. Alternatives were: {alternatives}"
                )
        # Verify geo data availability
        geo_file = Path("data/geo/cb_2017_us_county_20m.geojson")
        if not geo_file.exists():
            self.logger.error(f"County boundaries file not found: {geo_file}")

    def county_statistics(self) -> Dict[str, pd.Series]:
        """Calculate county-level statistics."""
        if self.df.empty:
            self.logger.warning("No data available for county statistics")
            return {}

        # Check if required columns exist
        required_cols = ["county", "emissions", "facility_name", "sector_name"]
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            self.logger.error(f"Missing required columns: {missing_cols}")
            return {
                "total_emissions": pd.Series(dtype=float),
                "avg_emissions": pd.Series(dtype=float),
                "facility_count": pd.Series(dtype=int),
                "sector_count": pd.Series(dtype=int),
                "emissions_per_facility": pd.Series(dtype=float),
            }

        try:
            stats = {
                "total_emissions": self.df.groupby("county")["emissions"].sum(),
                "avg_emissions": self.df.groupby("county")["emissions"].mean(),
                "facility_count": self.df.groupby("county")["facility_name"].nunique(),
                "sector_count": self.df.groupby("county")["sector_name"].nunique(),
                "emissions_per_facility": (
                    self.df.groupby("county")["emissions"].sum()
                    / self.df.groupby("county")["facility_name"].nunique()
                ),
            }
            return stats
        except Exception as e:
            self.logger.error(f"Error calculating county statistics: {e}")
            return {
                "total_emissions": pd.Series(dtype=float),
                "avg_emissions": pd.Series(dtype=float),
                "facility_count": pd.Series(dtype=int),
                "sector_count": pd.Series(dtype=int),
                "emissions_per_facility": pd.Series(dtype=float),
            }

    def industry_analysis(self) -> Dict[str, pd.DataFrame]:
        """Analyze emissions by industry/sector."""
        if self.df.empty:
            self.logger.warning("No data available for industry analysis")
            return {
                "by_sector": pd.DataFrame(),
                "sector_by_county": pd.DataFrame(),
                "subsector_analysis": pd.DataFrame(),
            }

        try:
            return {
                "by_sector": self.df.groupby("sector_name")["emissions"].agg(
                    ["sum", "mean", "count"]
                )
                if "sector_name" in self.df.columns
                else pd.DataFrame(),
                "sector_by_county": pd.pivot_table(
                    self.df,
                    values="emissions",
                    index="county",
                    columns="sector_name",
                    aggfunc="sum",
                    fill_value=0,
                )
                if all(col in self.df.columns for col in ["county", "sector_name"])
                else pd.DataFrame(),
                "subsector_analysis": (
                    self.df.groupby(["sector_name", "subsector_name"])["emissions"]
                    .sum()
                    .unstack(fill_value=0)
                )
                if all(
                    col in self.df.columns for col in ["sector_name", "subsector_name"]
                )
                else pd.DataFrame(),
            }
        except Exception as e:
            self.logger.error(f"Error in industry analysis: {e}")
            return {
                "by_sector": pd.DataFrame(),
                "sector_by_county": pd.DataFrame(),
                "subsector_analysis": pd.DataFrame(),
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
            # Load GeoJSON for all US counties
            counties_gdf = gpd.read_file("data/geo/cb_2017_us_county_20m.geojson")

            # Create a state FIPS lookup for filtering
            state_fips = {
                "AL": "01",
                "AK": "02",
                "AZ": "04",
                "AR": "05",
                "CA": "06",
                "CO": "08",
                "CT": "09",
                "DE": "10",
                "FL": "12",
                "GA": "13",
                "HI": "15",
                "ID": "16",
                "IL": "17",
                "IN": "18",
                "IA": "19",
                "KS": "20",
                "KY": "21",
                "LA": "22",
                "ME": "23",
                "MD": "24",
                "MA": "25",
                "MI": "26",
                "MN": "27",
                "MS": "28",
                "MO": "29",
                "MT": "30",
                "NE": "31",
                "NV": "32",
                "NH": "33",
                "NJ": "34",
                "NM": "35",
                "NY": "36",
                "NC": "37",
                "ND": "38",
                "OH": "39",
                "OK": "40",
                "OR": "41",
                "PA": "42",
                "RI": "44",
                "SC": "45",
                "SD": "46",
                "TN": "47",
                "TX": "48",
                "UT": "49",
                "VT": "50",
                "VA": "51",
                "WA": "53",
                "WV": "54",
                "WI": "55",
                "WY": "56",
            }

            # Filter for the state we're analyzing
            state_code = self.df["state"].iloc[0] if not self.df.empty else None
            if state_code in state_fips:
                counties_gdf = counties_gdf[
                    counties_gdf["STATEFP"] == state_fips[state_code]
                ]

            # Create a mapping dictionary from county name to emissions
            emissions_by_county = self.df.groupby("county")["emissions"].sum()

            # Create a mapping between county names and GEOID
            county_mapping = {}
            for idx, row in counties_gdf.iterrows():
                county_name = row["NAME"].upper()
                county_mapping[county_name] = row["GEOID"]

            # Map emissions to GeoDataFrame
            counties_gdf["emissions"] = 0.0  # Initialize with zeros
            for county, emissions in emissions_by_county.items():
                county_name = county.upper().replace(" COUNTY", "").strip()
                matching_geoids = [
                    geoid
                    for name, geoid in county_mapping.items()
                    if county_name in name or name in county_name
                ]
                if matching_geoids:
                    counties_gdf.loc[
                        counties_gdf["GEOID"] == matching_geoids[0], "emissions"
                    ] = emissions

            # Create the map
            fig, ax = plt.subplots(figsize=(15, 10))

            # Plot counties with emissions data
            counties_gdf.plot(
                column="emissions",
                ax=ax,
                legend=True,
                legend_kwds={
                    "label": "Total Emissions (metric tons CO2e)",
                    "orientation": "horizontal",
                },
                missing_kwds={"color": "lightgrey"},
                cmap="YlOrRd",
            )

            # Add county boundaries
            counties_gdf.boundary.plot(ax=ax, color="black", linewidth=0.5)

            # Add county labels for counties with emissions
            for idx, row in counties_gdf.iterrows():
                if row["emissions"] > 0:
                    centroid = row["geometry"].centroid
                    ax.annotate(
                        row["NAME"],
                        xy=(centroid.x, centroid.y),
                        xytext=(3, 3),
                        textcoords="offset points",
                        fontsize=8,
                        ha="center",
                    )

            plt.title(f"GHG Emissions by County - {state_code}")
            plt.axis("off")
            plt.tight_layout()

            # Save the figure
            plt.savefig(output_path, bbox_inches="tight", dpi=300)
            plt.close()

        except Exception as e:
            self.logger.error(f"Error creating county map: {e}", exc_info=True)

    def _create_industry_plot(self, output_path: Path) -> None:
        """Create sector analysis plots."""
        try:
            # First, determine which column contains sector information
            sector_columns = ["sector_name", "industry", "sector", "sector_type"]
            sector_col = next(
                (col for col in sector_columns if col in self.df.columns), None
            )

            if sector_col is None:
                self.logger.error("No sector/industry column found in DataFrame")
                return

            # Check for emissions column
            emissions_col = (
                "emissions" if "emissions" in self.df.columns else "co2e_emission"
            )
            if emissions_col not in self.df.columns:
                self.logger.error("No emissions data column found")
                return

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))

            # Top sectors by emissions
            try:
                sector_emissions = (
                    self.df.groupby(sector_col)[emissions_col]
                    .sum()
                    .sort_values(ascending=True)
                )
                sector_emissions.plot(kind="barh", ax=ax1)
                ax1.set_title("Total Emissions by Sector")
                ax1.set_xlabel("Emissions")
                ax1.set_ylabel("Sector")
            except Exception as e:
                self.logger.error(f"Error creating sector emissions plot: {e}")
                ax1.text(
                    0.5, 0.5, "Error creating sector plot", ha="center", va="center"
                )

            # Sector composition by county
            try:
                if "county" in self.df.columns:
                    sector_county = pd.crosstab(
                        self.df["county"],
                        self.df[sector_col],
                        values=self.df[emissions_col],
                        aggfunc="sum",
                    )
                    sector_county.plot(kind="bar", stacked=True, ax=ax2)
                    ax2.set_title("Sector Composition by County")
                    ax2.set_xlabel("County")
                    ax2.set_ylabel("Emissions")
                    plt.xticks(rotation=45)
                else:
                    self.logger.warning(
                        "No county column found for sector composition plot"
                    )
                    ax2.text(
                        0.5, 0.5, "No county data available", ha="center", va="center"
                    )
            except Exception as e:
                self.logger.error(f"Error creating sector composition plot: {e}")
                ax2.text(
                    0.5,
                    0.5,
                    "Error creating composition plot",
                    ha="center",
                    va="center",
                )

            plt.tight_layout()
            plt.savefig(output_path, bbox_inches="tight", dpi=300)
            plt.close()

        except Exception as e:
            self.logger.error(f"Error in _create_industry_plot: {e}")

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
