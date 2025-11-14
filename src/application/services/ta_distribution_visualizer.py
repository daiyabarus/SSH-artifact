# src/application/services/ta_distribution_visualizer.py

"""
TA Distribution Visualizer Module untuk Non-Augmented Data.
Membuat visualisasi Plotly untuk analisis distribusi TA menggunakan data original (non-augmented).
"""

from typing import List, Optional
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from streamlit_extras.stylable_container import stylable_container


class TADistributionVisualizer:
    """Visualizer untuk TA Distribution charts menggunakan data Non-Augmented."""

    # Monokai Dark color scheme
    BAR_COLOR = "#66D9EF"  # Cyan for bars
    TA90_COLOR = "#ED0000"  # Red for TA90 line
    TEXT_COLOR = "#F8F8F2"  # Light gray for text and legend
    BACKGROUND_COLOR = "#272822"  # Monokai Dark background
    GRID_COLOR = "#75715E"  # Subtle gray for gridlines
    BORDER_COLOR = "#75715E"  # Subtle gray for container border

    # Band-specific colors untuk bars dan CDF
    BAND_COLORS = {
        850: "#3498DB",  # Blue
        1800: "#E74C3C",  # Red
        2100: "#2ECC71",  # Green
        2300: "#F39C12",  # Orange
        2600: "#9B59B6",  # Purple
    }

    # CDF line colors (darker shades of band colors)
    CDF_COLORS = {
        850: "#1F618D",  # Dark Blue
        1800: "#A93226",  # Dark Red
        2100: "#196F3D",  # Dark Green
        2300: "#B9770E",  # Dark Orange
        2600: "#6C3483",  # Dark Purple
    }

    # Priority patterns for manual sector sorting
    SECTOR_PRIORITIES = ["1", "2", "3", "4", "M1", "M2", "M3", "11", "12", "13"]

    def __init__(self):
        """Initialize dengan distance labels sesuai format CSV."""
        self.distance_labels = [
            "0 - 78 m",
            "78 - 234 m",
            "234 - 390 m",
            "390 - 546 m",
            "546 - 702 m",
            "702 - 858 m",
            "858 - 1014 m",
            "1014 - 1560 m",
            "1560 - 2106 m",
            "2106 - 2652 m",
            "2652 - 3120 m",
            "3120 - 3900 m",
            "3900 - 6318 m",
            "6318 - 10062 m",
            "10062 - 13962 m",
            "13962 - 20000 m",
        ]

    def _get_band_color(self, band: int) -> str:
        """Get color based on Band value."""
        return self.BAND_COLORS.get(band, "#95A5A6")

    def _get_cdf_color(self, band: int) -> str:
        """Get CDF color based on Band value."""
        return self.CDF_COLORS.get(band, "#7F8C8D")

    def _get_sector_priority(self, sector_name: str) -> int:
        """Get priority index for sector based on containing priority patterns."""
        for i, pattern in enumerate(self.SECTOR_PRIORITIES):
            if pattern in sector_name:
                return i
        return len(self.SECTOR_PRIORITIES)  # Default to end for non-matching

    def create_sector_chart(
        self, sector_data: pl.DataFrame, sector_name: str, tower_id: str
    ) -> go.Figure:
        """
        Create combined bar and CDF chart untuk single sector.

        Args:
            sector_data: DataFrame containing TA distribution data for one sector
            sector_name: Name of the sector
            tower_id: Tower ID for reference
        """
        if sector_data.is_empty():
            fig = go.Figure()
            fig.add_annotation(
                text=f"No data available for sector {sector_name}",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                xanchor="center",
                yanchor="middle",
                showarrow=False,
                font=dict(size=16, color=self.TEXT_COLOR),
            )
            fig.update_layout(
                plot_bgcolor=self.BACKGROUND_COLOR,
                paper_bgcolor=self.BACKGROUND_COLOR,
                font=dict(color=self.TEXT_COLOR),
                title=dict(
                    text=sector_name,
                    x=0.5,
                    xanchor="center",
                    font=dict(size=14, color=self.TEXT_COLOR),
                ),
            )
            return fig

        # Create subplot dengan secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Process each band in the sector
        bands_in_sector = []
        for row in sector_data.iter_rows(named=True):
            band = row.get("Band", "Unknown")
            bands_in_sector.append(band)

            # Extract distance values (raw counts)
            distance_values = [row.get(label, 0) for label in self.distance_labels]

            # Extract CDF percentage values
            cdf_values = [
                row.get("78", 0),
                row.get("234", 0),
                row.get("390", 0),
                row.get("546", 0),
                row.get("702", 0),
                row.get("858", 0),
                row.get("1014", 0),
                row.get("1560", 0),
                row.get("2106", 0),
                row.get("2652", 0),
                row.get("3120", 0),
                row.get("3900", 0),
                row.get("6318", 0),
                row.get("10062", 0),
                row.get("13962", 0),
                row.get("20000", 0),
            ]

            # Add bar chart for samples
            fig.add_trace(
                go.Bar(
                    name=f"L{band} Samples",
                    x=self.distance_labels,
                    y=distance_values,
                    marker_color=self._get_band_color(band),
                    opacity=0.8,
                    hovertemplate="<b>L%{data.name}</b><br>Distance: %{x}<br>Samples: %{y}<extra></extra>",
                ),
                secondary_y=False,
            )

            # Add CDF line dengan color sesuai band
            cdf_color = self._get_cdf_color(band)

            fig.add_trace(
                go.Scatter(
                    name=f"L{band} CDF",
                    x=self.distance_labels,
                    y=cdf_values,
                    mode="lines+markers+text",
                    line=dict(color=cdf_color, width=3),
                    marker=dict(size=6, color=cdf_color),
                    text=[f"{val:.1f}%" for val in cdf_values],
                    textposition="top center",
                    textfont=dict(size=9, color="#ffffff"),
                    hovertemplate="<b>L%{data.name}</b><br>Distance: %{x}<br>CDF: %{y:.1f}%<extra></extra>",
                    showlegend=True,
                ),
                secondary_y=True,
            )

        # Add TA90 reference line
        fig.add_trace(
            go.Scatter(
                name="TA90%",
                x=[self.distance_labels[0], self.distance_labels[-1]],
                y=[90, 90],
                mode="lines",
                line=dict(color=self.TA90_COLOR, width=2, dash="dashdot"),
                hovertemplate="<b>TA90 Reference</b><br>Value: 90%<extra></extra>",
                showlegend=True,
            ),
            secondary_y=True,
        )

        # Update layout
        fig.update_layout(
            title=dict(
                text=f"SECTOR {sector_name}",
                x=0.5,
                xanchor="center",
                font=dict(size=14, color=self.TEXT_COLOR),
            ),
            height=350,
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=9, color=self.TEXT_COLOR),
                bgcolor=self.BACKGROUND_COLOR,
                bordercolor=self.GRID_COLOR,
                borderwidth=1,
                itemwidth=30,
            ),
            margin=dict(r=30, l=50, t=50, b=120),
            plot_bgcolor=self.BACKGROUND_COLOR,
            paper_bgcolor=self.BACKGROUND_COLOR,
        )

        # Update axes
        fig.update_xaxes(
            title_text="",
            tickangle=45,
            tickfont=dict(size=9, color=self.TEXT_COLOR),
            gridcolor=self.GRID_COLOR,
        )

        fig.update_yaxes(
            title_text="Samples",
            secondary_y=False,
            title_font=dict(size=9, color=self.TEXT_COLOR),
            tickfont=dict(color=self.TEXT_COLOR),
            gridcolor=self.GRID_COLOR,
        )

        fig.update_yaxes(
            title_text="CDF (%)",
            secondary_y=True,
            range=[0, 105],
            title_font=dict(size=9, color=self.TEXT_COLOR),
            tickfont=dict(color=self.TEXT_COLOR),
            gridcolor=self.GRID_COLOR,
        )

        return fig

    def display_sector_charts_in_rows(self, df: pl.DataFrame, tower_id: str):
        """
        Display TA distribution charts in rows (one chart per row).
        """
        if df.is_empty():
            st.warning("No TA distribution data available.")
            return

        # Get unique sectors
        sector_column = "newta_sector_name"
        if sector_column not in df.columns:
            st.warning(f"Sector column '{sector_column}' not found.")
            return

        unique_sectors = df[sector_column].unique().to_list()

        if not unique_sectors:
            st.warning("No sector data found.")
            return

        # Custom sort by priority
        unique_sectors.sort(key=self._get_sector_priority)

        # Display summary info
        st.info(f"📋 Showing {len(unique_sectors)} sectors for tower **{tower_id}**")

        # Display each sector in its own row
        for idx, sector_name in enumerate(unique_sectors):
            with stylable_container(
                key=f"sector_chart_{tower_id}_{sector_name}_{idx}",
                css_styles=f"""
                {{
                    background-color: {self.BACKGROUND_COLOR};
                    border: 2px solid {self.BORDER_COLOR};
                    border-radius: 0.5rem;
                    padding: calc(1em - 1px);
                    margin-bottom: 1rem;    
                }}
                """,
            ):
                sector_data = df.filter(pl.col(sector_column) == sector_name)

                if not sector_data.is_empty():
                    # Tampilkan info sector
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        bands_in_sector = sector_data["newta_band"].unique().to_list()
                        st.write(f"**Bands:** {', '.join(map(str, bands_in_sector))}")
                    with col2:
                        avg_ta90 = sector_data.select(
                            pl.col("newta_ta90").mean()
                        ).item()
                        st.write(f"**Avg TA90:** {avg_ta90:.2f}m")
                    with col3:
                        total_samples = sector_data.select(
                            pl.col("newta_total").sum()
                        ).item()
                        st.write(f"**Samples:** {total_samples:,}")

                    fig = self.create_sector_chart(sector_data, sector_name, tower_id)
                    st.plotly_chart(
                        fig, width="stretch", config={"displayModeBar": True}
                    )
                else:
                    st.info(f"No data for sector {sector_name}")

            # Add small spacing between charts
            if idx < len(unique_sectors) - 1:
                st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
