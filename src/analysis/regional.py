from typing import Dict, Optional

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd


class RegionalAnalysis:
    """Regional emissions analysis."""

    def __init__(self, df: pd.DataFrame):
        """Initialize regional analysis.

        Args:
            df: DataFrame with emissions data
        """
        self.df = df

    def analyze_by_county(self) -> Dict[str, pd.DataFrame]:
        """Analyze emissions by county."""
        results = {
            'total_emissions': self.df.groupby('county')['emissions'].sum(),
            'avg_emissions': self.df.groupby('county')['emissions'].mean(),
            'facility_count': self.df.groupby('county')['facility'].nunique()
        }
        return results

    def create_choropleth(self,
                         geo_data: gpd.GeoDataFrame,
                         output_dir: Optional[str] = None) -> plt.Figure:
        """Create choropleth map of emissions."""
        # Merge emissions data with geographic boundaries
        merged = geo_data.merge(
            self.df.groupby('county')['emissions'].sum().reset_index(),
            how='left',
            left_on='COUNTY',
            right_on='county'
        )

        # Create map
        fig, ax = plt.subplots(1, 1, figsize=(15, 10))
        merged.plot(column='emissions',
                   ax=ax,
                   legend=True,
                   legend_kwds={'label': 'Total Emissions'},
                   missing_kwds={'color': 'lightgrey'},
                   cmap='YlOrRd')

        if output_dir:
            fig.savefig(f"{output_dir}/emissions_map.png",
                       bbox_inches='tight',
                       dpi=300)

        return fig
