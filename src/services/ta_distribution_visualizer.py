"""
# src/services/ta_distribution_visualizer.py
"""

from typing import List, Optional
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from streamlit_extras.stylable_container import stylable_container


class TADistributionVisualizer:
    """Visualizer untuk TA Distribution charts menggunakan data TA langsung."""

    # Monokai Dark color scheme
    BAR_COLOR = "#66D9EF"  # Cyan for bars
    TA90_COLOR = "#ED0000"  # Red for TA90 line
    TEXT_COLOR = "#F8F8F2"  # Light gray for text and legend
    BACKGROUND_COLOR = "#272822"  # Monokai Dark background
    GRID_COLOR = "#75715E"  # Subtle gray for gridlines
    BORDER_COLOR = "#75715E"  # Subtle gray for container border

    BAND_COLORS = {
        "850": "#52eb0c",  # Blue
        "1800": "#080cec",  # Red
        "2100": "#ef17e8",  # Green
        "2300": "#F39C12",  # Orange
        "2600": "#9B59B6",  # Purple
    }

    CDF_COLORS = {
        "850": "#52eb0c",  # Dark Blue
        "1800": "#080cec",  # Dark Red
        "2100": "#ef17e8",  # Dark Green
        "2300": "#B9770E",  # Dark Orange
        "2600": "#6C3483",  # Dark Purple
    }

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

    def _get_band_color(self, band: str) -> str:
        """Get color based on Band value."""
        return self.BAND_COLORS.get(str(band), "#95A5A6")

    def _get_cdf_color(self, band: str) -> str:
        """Get CDF color based on Band value."""
        return self.CDF_COLORS.get(str(band), "#7F8C8D")

    def _get_sector_priority(self, sector_name: str) -> int:
        """Get priority index for sector based on containing priority patterns."""
        for i, pattern in enumerate(self.SECTOR_PRIORITIES):
            if pattern in str(sector_name):
                return i
        return len(self.SECTOR_PRIORITIES)

    def _add_cdf_range_labels(
        self,
        fig: go.Figure,
        cdf_values: List[float],
        band: str,
        secondary_y: bool = True,
    ):
        """
        Add CDF range labels for specific value ranges with white text.

        Args:
            fig: Plotly figure object
            cdf_values: List of CDF percentage values
            band: Band identifier for the CDF line
            secondary_y: Whether the CDF is on secondary y-axis
        """
        ranges = [
            {"min": 45, "max": 55, "label": "40%", "color": "white"},
            {"min": 65, "max": 75, "label": "70%", "color": "white"},
            {"min": 85, "max": 95, "label": "90%", "color": "white"},
        ]

        for range_config in ranges:
            range_min = range_config["min"]
            range_max = range_config["max"]
            range_label = range_config["label"]
            text_color = range_config["color"]

            range_indices = []
            for i, value in enumerate(cdf_values):
                if range_min <= value <= range_max:
                    range_indices.append(i)

            if range_indices:
                middle_idx = range_indices[len(range_indices) // 2]

                # Add annotation
                fig.add_annotation(
                    x=self.distance_labels[middle_idx],
                    y=cdf_values[middle_idx],
                    text=range_label,
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1,
                    arrowwidth=2,
                    arrowcolor=text_color,
                    ax=0,
                    ay=30,
                    font=dict(size=12, color=text_color, family="Arial, sans-serif"),
                    bgcolor="rgba(0,0,0,0.5)",
                    bordercolor=text_color,
                    borderwidth=1,
                    borderpad=4,
                    opacity=0.9,
                    yref="y2" if secondary_y else "y",
                )

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

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        bands_in_sector = []
        for row in sector_data.iter_rows(named=True):
            band = str(row.get("newta_band", "Unknown"))
            bands_in_sector.append(band)

            distance_values = [
                row.get("newta_0_78_m", 0),
                row.get("newta_78_234_m", 0),
                row.get("newta_234_390_m", 0),
                row.get("newta_390_546_m", 0),
                row.get("newta_546_702_m", 0),
                row.get("newta_702_858_m", 0),
                row.get("newta_858_1014_m", 0),
                row.get("newta_1014_1560_m", 0),
                row.get("newta_1560_2106_m", 0),
                row.get("newta_2106_2652_m", 0),
                row.get("newta_2652_3120_m", 0),
                row.get("newta_3120_3900_m", 0),
                row.get("newta_3900_6318_m", 0),
                row.get("newta_6318_10062_m", 0),
                row.get("newta_10062_13962_m", 0),
                row.get("newta_13962_20000_m", 0),
            ]

            cdf_values = [
                row.get("newta_78", 0),
                row.get("newta_234", 0),
                row.get("newta_390", 0),
                row.get("newta_546", 0),
                row.get("newta_702", 0),
                row.get("newta_858", 0),
                row.get("newta_1014", 0),
                row.get("newta_1560", 0),
                row.get("newta_2106", 0),
                row.get("newta_2652", 0),
                row.get("newta_3120", 0),
                row.get("newta_3900", 0),
                row.get("newta_6318", 0),
                row.get("newta_10062", 0),
                row.get("newta_13962", 0),
                row.get("newta_20000", 0),
            ]

            fig.add_trace(
                go.Bar(
                    name=f"L{band} Samples",
                    x=self.distance_labels,
                    y=distance_values,
                    marker_color=self._get_band_color(band),
                    opacity=0.8,
                    hovertemplate=f"<b>L{band}</b><br>Distance: %{{x}}<br>Samples: %{{y}}<extra></extra>",
                ),
                secondary_y=False,
            )

            cdf_color = self._get_cdf_color(band)

            fig.add_trace(
                go.Scatter(
                    name=f"L{band} CDF",
                    x=self.distance_labels,
                    y=cdf_values,
                    mode="lines+markers",
                    line=dict(color=cdf_color, width=3),
                    marker=dict(size=6, color=cdf_color),
                    hovertemplate=f"<b>L{band}</b><br>Distance: %{{x}}<br>CDF: %{{y:.1f}}%<extra></extra>",
                    showlegend=True,
                ),
                secondary_y=True,
            )

            self._add_cdf_range_labels(fig, cdf_values, band, secondary_y=True)

        fig.add_trace(
            go.Scatter(
                name="TA90% Reference",
                x=[self.distance_labels[0], self.distance_labels[-1]],
                y=[90, 90],
                mode="lines",
                line=dict(color=self.TA90_COLOR, width=2, dash="dashdot"),
                hovertemplate="<b>TA90 Reference</b><br>Value: 90%<extra></extra>",
                showlegend=True,
            ),
            secondary_y=True,
        )

        fig.update_layout(
            title=dict(
                text=f"{tower_id} SECTOR {sector_name}",
                x=0.3,
                xanchor="center",
                font=dict(size=16, color=self.TEXT_COLOR),
            ),
            height=350,
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.5,
                xanchor="center",
                x=0.5,
                font=dict(size=11, color=self.TEXT_COLOR),
                bgcolor=self.BACKGROUND_COLOR,
                bordercolor=self.GRID_COLOR,
                borderwidth=1,
                itemwidth=30,
            ),
            margin=dict(l=20, r=20, t=40, b=20),
            plot_bgcolor=self.BACKGROUND_COLOR,
            paper_bgcolor=self.BACKGROUND_COLOR,
        )

        fig.update_xaxes(
            title_text="",
            tickangle=45,
            tickfont=dict(size=14, color=self.TEXT_COLOR),
            gridcolor=self.GRID_COLOR,
            showgrid=True,
        )

        fig.update_yaxes(
            title_text="Number of Samples",
            secondary_y=False,
            title_font=dict(size=12, color=self.TEXT_COLOR),
            tickfont=dict(size=14, color=self.TEXT_COLOR),
            gridcolor=self.GRID_COLOR,
            showgrid=True,
        )

        fig.update_yaxes(
            title_text="CDF (%)",
            secondary_y=True,
            range=[0, 105],
            title_font=dict(size=10, color=self.TEXT_COLOR),
            tickfont=dict(size=14, color=self.TEXT_COLOR),
            gridcolor=self.GRID_COLOR,
            showgrid=True,
        )

        return fig

    def display_sector_charts_in_rows(self, df: pl.DataFrame, tower_id: str):
        """
        Display TA distribution charts in rows (one chart per row).
        """
        if df.is_empty():
            st.warning("No TA distribution data available.")
            return

        sector_column = "newta_sector_name"
        if sector_column not in df.columns:
            st.warning(f"Sector column '{sector_column}' not found.")
            return

        unique_sectors = df[sector_column].unique().to_list()

        if not unique_sectors:
            st.warning("No sector data found.")
            return

        unique_sectors.sort(key=self._get_sector_priority)

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
                    fig = self.create_sector_chart(sector_data, sector_name, tower_id)
                    st.plotly_chart(
                        fig, width="stretch", config={"displayModeBar": True}
                    )
                else:
                    st.info(f"No data for sector {sector_name}")

            if idx < len(unique_sectors) - 1:
                st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

    def display_ta_distribution_simple(self, df: pl.DataFrame, tower_id: str):
        """
        Simple display untuk debugging - tampilkan data mentah dulu
        """
        if df.is_empty():
            st.warning("No TA data available.")
            return

        st.subheader("📊 TA Distribution Data Preview")

        important_cols = [
            "newta_sector_name",
            "newta_band",
            "newta_ta90",
            "newta_ta99",
            "newta_0_78_m",
            "newta_78_234_m",
            "newta_234_390_m",
            "newta_390_546_m",
            "newta_546_702_m",
            "newta_702_858_m",
            "newta_858_1014_m",
            "newta_1014_1560_m",
            "newta_1560_2106_m",
            "newta_2106_2652_m",
            "newta_2652_3120_m",
            "newta_3120_3900_m",
            "newta_3900_6318_m",
            "newta_6318_10062_m",
            "newta_10062_13962_m",
            "newta_13962_20000_m",
            "newta_78",
            "newta_234",
            "newta_390",
            "newta_546",
            "newta_702",
            "newta_858",
            "newta_1014",
            "newta_1560",
            "newta_2106",
            "newta_2652",
            "newta_3120",
            "newta_3900",
            "newta_6318",
            "newta_10062",
            "newta_13962",
            "newta_20000",
        ]

        existing_cols = [col for col in important_cols if col in df.columns]

        if existing_cols:
            preview_df = df.select(existing_cols).head(10)  # Limit to 10 rows
            st.dataframe(preview_df.to_pandas(), width="stretch")
        else:
            st.warning("Required TA distribution columns not found.")
            st.write("Available columns:", df.columns)

    def get_ta_data_summary(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Get summary statistics for TA data.
        """
        if df.is_empty():
            return pl.DataFrame()

        summary_cols = [
            "newta_average_ta_m",
            "newta_ta90",
            "newta_ta99",
            "newta_total",
            "newta_0_78_m",
            "newta_78_234_m",
            "newta_234_390_m",
            "newta_390_546_m",
            "newta_546_702_m",
            "newta_702_858_m",
            "newta_858_1014_m",
            "newta_1014_1560_m",
            "newta_1560_2106_m",
            "newta_2106_2652_m",
            "newta_2652_3120_m",
            "newta_3120_3900_m",
            "newta_3900_6318_m",
            "newta_6318_10062_m",
            "newta_10062_13962_m",
            "newta_13962_20000_m",
        ]

        available_cols = [col for col in summary_cols if col in df.columns]

        if not available_cols:
            return pl.DataFrame()

        summary = df.select(
            [pl.col(col).mean().alias(f"{col}_mean") for col in available_cols]
            + [pl.col(col).max().alias(f"{col}_max") for col in available_cols]
            + [pl.col(col).min().alias(f"{col}_min") for col in available_cols]
        )

        return summary
