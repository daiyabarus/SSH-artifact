"""
============================================================================
FILE: src/services/bh_ta_chart_visualizer.py
UPDATED: Added tower-based throughput charts (single chart, no sector separation)
============================================================================
"""

import streamlit as st
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Union
import logging
from streamlit_extras.stylable_container import stylable_container

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BHTAChartVisualizer:
    """
    Visualizes BH+TA joined data with sector-based charts
    Similar to WDTAChartVisualizer but for Busy Hour data
    """

    def __init__(self):
        self.color_palette = self._generate_color_palette()
        self.kpi_configs = self._define_kpi_configs()
        self.silver_light_bg = "rgba(245, 245, 245, 1)"
        self.container_bg = "rgba(245, 245, 245, 0.8)"
        self.border_color = "rgba(100, 100, 100, 1)"
        self.grid_color = "rgba(200, 200, 200, 0.8)"

    def _generate_color_palette(self) -> List[str]:
        """Generate distinct colors for different band+sector combinations"""
        return [
            "#080cec",
            "#ef17e8",
            "#52eb0c",
            "#f59e0b",
            "#10b981",
            "#06b6d4",
            "#ef4444",
            "#a855f7",
            "#14b8a6",
            "#f97316",
            "#84cc16",
            "#0ea5e9",
            "#f43f5e",
            "#8b5cf6",
            "#22d3ee",
        ]

    def _define_kpi_configs(self) -> Dict:
        """Define KPI calculation and display configurations for Busy Hour data"""
        return {
            "total_payload": {
                "col": "newbh_total_payload_gb_kpi",
                "label": "Total Payload (GB)",
                "format": ".2f",
                "chart_type": "area",
            },
            "dl_user_throughput": {
                "num": "newbh_cell_downlink_user_throughput_num",
                "den": "newbh_cell_downlink_user_throughput_den",
                "label": "DL User Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            "ul_user_throughput": {
                "num": "newbh_cell_uplink_user_throughput_num",
                "den": "newbh_cell_uplink_user_throughput_den",
                "label": "UL User Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            "pdcp_dl_throughput": {
                "num": "newbh_pdcp_cell_throughput_dl_num",
                "den": "newbh_pdcp_cell_throughput_dl_denom",
                "label": "DL Cell Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            "pdcp_ul_throughput": {
                "num": "newbh_pdcp_cell_throughput_ul_num",
                "den": "newbh_pdcp_cell_throughput_ul_den",
                "label": "UL Cell Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            "volte_dl_loss": {
                "num": "newbh_cell_volte_dl_packet_loss_ratio_num",
                "den": "newbh_cell_volte_dl_packet_loss_ratio_den",
                "label": "VoLTE DL Packet Loss (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            "volte_ul_loss": {
                "num": "newbh_cell_volte_ul_packet_loss_ratio_num",
                "den": "newbh_cell_volte_ul_packet_loss_ratio_den",
                "label": "VoLTE UL Packet Loss (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            "session_setup_sr": {
                "num": [
                    "newbh_cell_session_setup_success_rate_a_num",
                    "newbh_cell_session_setup_success_rate_b_num",
                    "newbh_cell_session_setup_success_rate_c_num",
                ],
                "den": [
                    "newbh_cell_session_setup_success_rate_a_den",
                    "newbh_cell_session_setup_success_rate_b_den",
                    "newbh_cell_session_setup_success_rate_c_den",
                ],
                "label": "SSSR (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "erab_drop_rate": {
                "num": "newbh_lerabdroprate_num",
                "den": "newbh_lerabdroprate_den",
                "label": "ERAB Drop Rate (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            "handover_sr": {
                "num": "newbh_cell_handover_success_rate_inter_and_intra_frequency_num",
                "den": "newbh_cell_handover_success_rate_inter_and_intra_frequency_den",
                "label": "Handover Success Rate (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "avg_cqi": {
                "num": "newbh_cell_average_cqi_num",
                "den": "newbh_cell_average_cqi_den",
                "label": "Average CQI",
                "format": ".2f",
                "chart_type": "line",
            },
            "spectral_efficiency": {
                "num": "newbh_spectral_efficiency_dl_num",
                "den": "newbh_spectral_efficiency_dl_den",
                "label": "Spectrum Efficiency",
                "format": ".4f",
                "chart_type": "line",
            },
            "qpsk": {
                "num": "newbh_cell_qpsk_rate_num",
                "den": "newbh_cell_qpsk_rate_den",
                "label": "QPSK Rate (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "packet_latency": {
                "num": "newbh_cell_packet_latency_num",
                "den": "newbh_cell_packet_latency_den",
                "label": "Packet Latency (ms)",
                "format": ".2f",
                "chart_type": "line",
            },
            "lasttti": {
                "num": "newbh_cell_last_tti_ratio_num",
                "den": "newbh_cell_last_tti_ratio_den",
                "label": "Last TTI Ratio (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "voltecssr": {
                "num": "newbh_cell_volte_cssr_num_4",
                "den": "newbh_cell_volte_cssr_denom_4",
                "label": "VoLTE CSSR (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "rank2": {
                "num": "newbh_cell_mimo_transmission_rank_eq_2_rate_num",
                "den": "newbh_cell_mimo_transmission_rank_eq_2_rate_den",
                "label": "Rank 2",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            }
        }

    def _clean_numeric_column(self, df: pl.DataFrame, col_name: str) -> pl.DataFrame:
        """Clean numeric column - replace empty strings with NULL, then cast to float"""
        if col_name not in df.columns:
            return df

        try:
            df = df.with_columns(
                pl.when(
                    (pl.col(col_name).cast(pl.Utf8).str.len_chars() == 0)
                    | (pl.col(col_name).cast(pl.Utf8) == "")
                    | (pl.col(col_name).is_null())
                )
                .then(None)
                .otherwise(pl.col(col_name))
                .alias(col_name)
            )

            df = df.with_columns(
                pl.col(col_name).cast(pl.Float64, strict=False).alias(col_name)
            )

            logger.debug(f"Successfully cleaned and cast column: {col_name}")

        except Exception as e:
            logger.warning(f"Could not clean column {col_name}: {e}")
            try:
                df = df.with_columns(
                    pl.col(col_name).cast(pl.Float64, strict=False).alias(col_name)
                )
            except:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col_name))

        return df

    def _calculate_kpi(
        self,
        df: pl.DataFrame,
        num_col: Union[str, List[str]],
        den_col: Union[str, List[str]],
        is_percent: bool = False,
    ) -> pl.DataFrame:
        """Calculate KPI with proper empty string handling"""
        is_list = isinstance(num_col, list)
        if is_list:
            if not isinstance(den_col, list) or len(num_col) != len(den_col):
                logger.warning("Num and den must both be lists of equal length")
                return df.with_columns(pl.lit(None).alias("kpi_value"))

            for nc in num_col:
                if nc in df.columns:
                    df = self._clean_numeric_column(df, nc)
            for dc in den_col:
                if dc in df.columns:
                    df = self._clean_numeric_column(df, dc)

            ratio_exprs = []
            for nc, dc in zip(num_col, den_col):
                if nc in df.columns and dc in df.columns:
                    ratio = (
                        pl.when((pl.col(dc).is_not_null()) & (pl.col(dc) != 0))
                        .then(pl.col(nc) / pl.col(dc))
                        .otherwise(None)
                    )
                    ratio_exprs.append(ratio)
                else:
                    ratio_exprs.append(pl.lit(None))

            if not ratio_exprs:
                product = pl.lit(None)
            else:
                product = ratio_exprs[0]
                for r in ratio_exprs[1:]:
                    product = product * r

            multiplier = 100 if is_percent else 1
            kpi_expr = product * multiplier

            df = df.with_columns(kpi_expr.alias("kpi_value"))
        else:
            if num_col not in df.columns or den_col not in df.columns:
                logger.warning(f"Missing columns: {num_col} or {den_col}")
                return df.with_columns(pl.lit(None).alias("kpi_value"))

            df = self._clean_numeric_column(df, num_col)
            df = self._clean_numeric_column(df, den_col)

            if is_percent:
                expr = (
                    pl.when((pl.col(den_col).is_not_null()) & (pl.col(den_col) != 0))
                    .then((pl.col(num_col) / pl.col(den_col)) * 100)
                    .otherwise(None)
                )
            else:
                expr = (
                    pl.when((pl.col(den_col).is_not_null()) & (pl.col(den_col) != 0))
                    .then(pl.col(num_col) / pl.col(den_col))
                    .otherwise(None)
                )

            df = df.with_columns(expr.alias("kpi_value"))

        return df

    def _create_band_sector_key(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create band+sector combined key for grouping"""
        df = df.with_columns(
            (
                "L"
                + pl.col("newbh_cell_fdd_band")
                .cast(pl.Float64)
                .cast(pl.Int64)
                .cast(pl.Utf8)
                + " SEC "
                + pl.col("newta_sector_name").cast(pl.Utf8)
            ).alias("band_sector_key")
        )
        return df

    def _parse_dates_safely(self, df: pl.DataFrame) -> pl.DataFrame:
        """Parse mixed date formats in the same column"""
        if "newbh_date" not in df.columns:
            logger.warning("Column 'newbh_date' not found")
            return df

        date_formats = ["%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%d/%m/%Y", "%Y/%m/%d"]

        parsed_columns = []

        for idx, date_format in enumerate(date_formats):
            try:
                parsed_col = (
                    pl.col("newbh_date")
                    .str.strptime(pl.Date, date_format, strict=False)
                    .alias(f"parsed_{idx}")
                )
                parsed_columns.append(parsed_col)
            except Exception as e:
                logger.debug(f"Format {date_format} setup failed: {e}")
                continue

        if not parsed_columns:
            logger.warning("No date formats could be set up")
            return df.with_columns(pl.col("newbh_date").alias("date_parsed"))

        df = df.with_columns(parsed_columns)
        coalesce_expr = pl.coalesce([f"parsed_{i}" for i in range(len(parsed_columns))])
        df = df.with_columns(coalesce_expr.alias("date_parsed"))
        df = df.drop([f"parsed_{i}" for i in range(len(parsed_columns))])

        success_count = df.filter(pl.col("date_parsed").is_not_null()).height
        total_count = df.height

        if success_count > 0:
            logger.info(
                f"Successfully parsed {success_count}/{total_count} dates with mixed formats"
            )
        else:
            logger.warning("Could not parse any dates, using original strings")
            df = df.with_columns(pl.col("newbh_date").alias("date_parsed"))

        return df

    def _prepare_chart_data(self, df: pl.DataFrame, kpi_name: str) -> pl.DataFrame:
        """Prepare data for specific KPI chart"""
        config = self.kpi_configs.get(kpi_name)
        if not config:
            logger.error(f"Unknown KPI: {kpi_name}")
            return pl.DataFrame()

        # Handle simple column KPIs (like total_payload)
        if "col" in config:
            df = self._clean_numeric_column(df, config["col"])
            df = df.with_columns(pl.col(config["col"]).alias("kpi_value"))
        else:
            # Handle calculated KPIs
            df = self._calculate_kpi(
                df, config["num"], config["den"], config.get("is_percent", False)
            )

        df = self._create_band_sector_key(df)
        df = self._parse_dates_safely(df)

        date_col = "date_parsed" if "date_parsed" in df.columns else "newbh_date"

        chart_data = (
            df.group_by(
                [
                    date_col,
                    "band_sector_key",
                    "newta_sector_name",
                    "newbh_enodeb_fdd_msc",
                ]
            )
            .agg(
                [
                    pl.col("kpi_value").mean().alias("avg_kpi"),
                    pl.col("newbh_cell_fdd_band").first().alias("band"),
                    pl.col("newbh_date").first().alias("newbh_date"),
                ]
            )
            .sort(date_col)
        )

        return chart_data

    def _prepare_tower_chart_data(self, df: pl.DataFrame, kpi_name: str) -> pl.DataFrame:
        """
        ✅ NEW: Prepare data for tower-based KPI chart (aggregated by tower+date)
        Used for throughput charts that show tower-level trends
        """
        config = self.kpi_configs.get(kpi_name)
        if not config:
            logger.error(f"Unknown KPI: {kpi_name}")
            return pl.DataFrame()

        # Clean numerator and denominator columns
        num_col = config["num"]
        den_col = config["den"]
        
        df = self._clean_numeric_column(df, num_col)
        df = self._clean_numeric_column(df, den_col)
        
        # Parse dates
        df = self._parse_dates_safely(df)
        date_col = "date_parsed" if "date_parsed" in df.columns else "newbh_date"

        # Aggregate by tower and date - sum numerators and denominators
        chart_data = (
            df.group_by([date_col, "newbh_enodeb_fdd_msc"])
            .agg([
                pl.col(num_col).sum().alias("total_num"),
                pl.col(den_col).sum().alias("total_den"),
                pl.col("newbh_date").first().alias("newbh_date"),
            ])
            .sort(date_col)
        )

        # Calculate KPI from aggregated values
        is_percent = config.get("is_percent", False)
        multiplier = 100 if is_percent else 1
        
        chart_data = chart_data.with_columns([
            pl.when((pl.col("total_den").is_not_null()) & (pl.col("total_den") != 0))
            .then((pl.col("total_num") / pl.col("total_den")) * multiplier)
            .otherwise(None)
            .alias("avg_kpi")
        ])

        return chart_data

    def _create_sector_chart(
        self, df: pl.DataFrame, sector_name: str, kpi_name: str
    ) -> go.Figure:
        """Create chart (line or stacked area) for a specific sector"""
        config = self.kpi_configs[kpi_name]
        sector_data = df.filter(pl.col("newta_sector_name") == sector_name)

        if sector_data.is_empty():
            return None

        chart_type = config.get("chart_type", "line")

        # Determine x-axis column
        if (
            "date_parsed" in sector_data.columns
            and not sector_data["date_parsed"].is_null().any()
        ):
            x_col = "date_parsed"
        else:
            x_col = "newbh_date"

        if chart_type == "area":
            # Convert to pandas for plotly express
            sector_df = sector_data.to_pandas()
            
            # Use plotly express for stacked area chart
            fig = px.area(
                sector_df,
                x=x_col,
                y="avg_kpi",
                color="band_sector_key",
                line_group="band_sector_key",
                color_discrete_sequence=self.color_palette,
                labels={
                    "avg_kpi": config["label"],
                    x_col: "",
                    "band_sector_key": ""
                },
            )
            
            # Update hover template for better formatting
            fig.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>"
                + "Date: %{x|%m/%d/%Y}<br>"
                + f"{config['label']}: %{{y:{config['format']}}}<br>"
                + "<extra></extra>"
            )
        else:
            # Use existing line chart logic
            fig = go.Figure()
            unique_keys = sector_data["band_sector_key"].unique().sort().to_list()

            for idx, band_sector_key in enumerate(unique_keys):
                line_data = sector_data.filter(pl.col("band_sector_key") == band_sector_key)

                if line_data.is_empty():
                    continue

                color = self.color_palette[idx % len(self.color_palette)]
                x_data = line_data[x_col].to_list()

                fig.add_trace(
                    go.Scatter(
                        x=x_data,
                        y=line_data["avg_kpi"].to_list(),
                        name=band_sector_key,
                        mode="lines+markers",
                        line=dict(color=color, width=3),
                        marker=dict(size=8, color=color),
                        hovertemplate="<b>%{fullData.name}</b><br>"
                        + "Date: %{x|%m/%d/%Y}<br>"
                        + f"{config['label']}: %{{y:{config['format']}}}<br>"
                        + "<extra></extra>",
                    )
                )

        fig.update_layout(
            title_text=f"SECTOR - {sector_name}",
            title_x=0.4,
            title_font=dict(size=20, color="#000000"),
            xaxis_title="",
            yaxis_title=config["label"],
            yaxis_title_font=dict(size=16, family="Arial, sans-serif"),
            template="plotly_white",
            font=dict(size=14, color="#000000"),
            hovermode="x unified",
            autosize=True,
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.45,
                xanchor="center",
                x=0.5,
                font=dict(size=14),
                bgcolor=self.container_bg,
                bordercolor=self.border_color,
                borderwidth=1,
            ),
            width=600,
            height=350,
            margin=dict(l=80, r=80, t=40, b=20),
            plot_bgcolor=self.silver_light_bg,
            paper_bgcolor=self.silver_light_bg,
        )

        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor=self.grid_color,
            tickformat="%m/%d/%Y",
            tickangle=-45,
            tickfont=dict(size=14),
            tickmode="auto",
            linecolor=self.border_color,
            linewidth=2,
            mirror=True,
            showline=True,
        )

        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor=self.grid_color,
            tickfont=dict(size=14),
            linecolor=self.border_color,
            linewidth=2,
            mirror=True,
            showline=True,
        )

        return fig

    def _create_tower_chart(self, df: pl.DataFrame, kpi_name: str) -> go.Figure:
        """
        ✅ NEW: Create single chart showing all towers (no sector separation)
        Used for DL/UL User Throughput visualization
        ✅ UPDATED: Added reference lines for throughput thresholds
        """
        config = self.kpi_configs[kpi_name]
        
        if df.is_empty():
            return None

        # Determine x-axis column
        if "date_parsed" in df.columns and not df["date_parsed"].is_null().any():
            x_col = "date_parsed"
        else:
            x_col = "newbh_date"

        fig = go.Figure()
        unique_towers = df["newbh_enodeb_fdd_msc"].unique().sort().to_list()

        for idx, tower_id in enumerate(unique_towers):
            tower_data = df.filter(pl.col("newbh_enodeb_fdd_msc") == tower_id)

            if tower_data.is_empty():
                continue

            color = self.color_palette[idx % len(self.color_palette)]
            x_data = tower_data[x_col].to_list()

            fig.add_trace(
                go.Scatter(
                    x=x_data,
                    y=tower_data["avg_kpi"].to_list(),
                    name=tower_id,
                    mode="lines+markers",
                    line=dict(color=color, width=3),
                    marker=dict(size=8, color=color),
                    hovertemplate="<b>%{fullData.name}</b><br>"
                    + "Date: %{x|%m/%d/%Y}<br>"
                    + f"{config['label']}: %{{y:{config['format']}}}<br>"
                    + "<extra></extra>",
                )
            )

        reference_values = {
            "dl_user_throughput": 3.0,
            "ul_user_throughput": 1.0,
        }
        
        if kpi_name in reference_values:
            threshold = reference_values[kpi_name]
            fig.add_hline(
                y=threshold,
                line_dash="dashdot",
                line_color="red",
                line_width=4,
            )

        tower_display = ", ".join(unique_towers) if len(unique_towers) <= 3 else f"{len(unique_towers)} Towers"

        fig.update_layout(
            title_text=f"{config['label']} - {tower_display}",
            title_x=0.4,
            title_font=dict(size=20, color="#000000"),
            xaxis_title="",
            yaxis_title=config["label"],
            yaxis_title_font=dict(size=16, family="Arial, sans-serif"),
            template="plotly_white",
            font=dict(size=14, color="#000000"),
            hovermode="x unified",
            autosize=True,
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.2,
                xanchor="center",
                x=0.5,
                font=dict(size=14),
                bgcolor=self.container_bg,
                bordercolor=self.border_color,
                borderwidth=1,
            ),
            height=500,
            margin=dict(l=80, r=80, t=60, b=100),
            plot_bgcolor=self.silver_light_bg,
            paper_bgcolor=self.silver_light_bg,
        )

        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor=self.grid_color,
            tickformat="%m/%d/%Y",
            tickangle=-45,
            tickfont=dict(size=14),
            tickmode="auto",
            linecolor=self.border_color,
            linewidth=2,
            mirror=True,
            showline=True,
        )

        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor=self.grid_color,
            tickfont=dict(size=14),
            linecolor=self.border_color,
            linewidth=2,
            mirror=True,
            showline=True,
        )

        return fig

    def render_kpi_charts_by_sector(self, df: pl.DataFrame, kpi_name: str):
        """Render charts separated by sector in 3-column grid layout using stylable_container"""
        if df.is_empty():
            st.warning(f"No data available for {kpi_name}")
            return

        chart_data = self._prepare_chart_data(df, kpi_name)

        if chart_data.is_empty():
            st.warning(f"No valid data after KPI calculation for {kpi_name}")
            return

        unique_sectors = chart_data["newta_sector_name"].unique().sort().to_list()
        unique_towerid = chart_data["newbh_enodeb_fdd_msc"].unique().sort().to_list()

        config = self.kpi_configs[kpi_name]

        if unique_towerid:
            tower_display = (
                f"- {', '.join(unique_towerid)}"
                if len(unique_towerid) > 1
                else f"- {unique_towerid[0]}"
            )
        else:
            tower_display = ""

        st.markdown(f"### 📊 {config['label']} Busy Hour {tower_display}")

        num_sectors = len(unique_sectors)
        num_rows = (num_sectors + 2) // 3

        for row in range(num_rows):
            start_idx = row * 3
            end_idx = min(start_idx + 3, num_sectors)
            sectors_in_row = unique_sectors[start_idx:end_idx]

            if sectors_in_row:
                cols = st.columns(3)

                for idx, sector in enumerate(sectors_in_row):
                    with cols[idx]:
                        tower_id = chart_data.filter(
                            pl.col("newta_sector_name") == sector
                        )["newbh_enodeb_fdd_msc"].first()

                        with stylable_container(
                            key=f"bh_sector_chart_{tower_id}_{sector}_{kpi_name}_{idx}",
                            css_styles=f"""
                            {{
                                background-color: {self.silver_light_bg};
                                border: 4px solid {self.border_color};
                                border-radius: 0.5rem;
                                padding: calc(1em - 1px);
                                margin-bottom: 1rem;    
                            }}
                            """,
                        ):
                            fig = self._create_sector_chart(
                                chart_data, sector, kpi_name
                            )
                            if fig:
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info(f"No data for sector {sector}")

    def render_tower_throughput_chart(self, df: pl.DataFrame, kpi_name: str):
        """
        ✅ NEW: Render single tower-based throughput chart
        Used for DL/UL User Throughput - shows all towers in one chart
        """
        if df.is_empty():
            st.warning(f"No data available for {kpi_name}")
            return

        chart_data = self._prepare_tower_chart_data(df, kpi_name)

        if chart_data.is_empty():
            st.warning(f"No valid data after KPI calculation for {kpi_name}")
            return

        config = self.kpi_configs[kpi_name]
        unique_towerid = chart_data["newbh_enodeb_fdd_msc"].unique().sort().to_list()

        tower_display = ", ".join(unique_towerid) if len(unique_towerid) <= 3 else f"{len(unique_towerid)} Towers"

        st.markdown(f"### 📊 {config['label']} Busy Hour - {tower_display}")

        with stylable_container(
            key=f"bh_tower_chart_{kpi_name}",
            css_styles=f"""
            {{
                background-color: {self.silver_light_bg};
                border: 4px solid {self.border_color};
                border-radius: 0.5rem;
                padding: calc(1em - 1px);
                margin-bottom: 1rem;    
            }}
            """,
        ):
            fig = self._create_tower_chart(chart_data, kpi_name)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"No data for {kpi_name}")

    def render_all_kpis(self, df: pl.DataFrame):
        """Render all KPI charts sequentially"""
        if df.is_empty():
            st.warning("No BH+TA data available for visualization")
            return

        self.render_tower_throughput_chart(df, "dl_user_throughput")
        self.render_tower_throughput_chart(df, "ul_user_throughput")
        
        all_kpis = [
            "dl_user_throughput",
            "ul_user_throughput",
            "avg_cqi",
            "qpsk",
            "lasttti",
            "spectral_efficiency",
            "voltecssr",
            "erab_drop_rate",
            "total_payload",
            "pdcp_dl_throughput",
            "pdcp_ul_throughput",
            "volte_dl_loss",
            "volte_ul_loss",
            "session_setup_sr",
            "handover_sr",
            "packet_latency",
            "rank2",
        ]

        for kpi in all_kpis:
            self.render_kpi_charts_by_sector(df, kpi)