"""
============================================================================
FILE: src/services/wd_ta_chart_visualizer.py
UPDATED: Added area chart support for total_payload KPI
============================================================================
"""

import streamlit as st
import polars as pl
import plotly.graph_objects as go
from typing import List, Dict, Union
import logging
from streamlit_extras.stylable_container import stylable_container
import plotly.express as px

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WDTAChartVisualizer:
    """
    Visualizes WD+TA joined data with sector-based charts
    UPDATED: Added area chart support for total_payload
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
        """Define KPI calculation and display configurations"""
        return {
            "total_payload": {
                "col": "newwd_total_payload_gb_kpi",
                "label": "Total Payload (GB)",
                "format": ".2f",
                "chart_type": "area",
            },
            "dl_user_throughput": {
                "num": "newwd_cell_downlink_user_throughput_num",
                "den": "newwd_cell_downlink_user_throughput_den",
                "label": "DL User Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            "ul_user_throughput": {
                "num": "newwd_cell_uplink_user_throughput_num",
                "den": "newwd_cell_uplink_user_throughput_den",
                "label": "UL User Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            "pdcp_dl_throughput": {
                "num": "newwd_pdcp_cell_throughput_dl_num",
                "den": "newwd_pdcp_cell_throughput_dl_denom",
                "label": "DL Cell Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            "pdcp_ul_throughput": {
                "num": "newwd_pdcp_cell_throughput_ul_num",
                "den": "newwd_pdcp_cell_throughput_ul_den",
                "label": "UL Cell Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            "volte_dl_loss": {
                "num": "newwd_cell_volte_dl_packet_loss_ratio_num",
                "den": "newwd_cell_volte_dl_packet_loss_ratio_den",
                "label": "VoLTE DL Packet Loss (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            "volte_ul_loss": {
                "num": "newwd_cell_volte_ul_packet_loss_ratio_num",
                "den": "newwd_cell_volte_ul_packet_loss_ratio_den",
                "label": "VoLTE UL Packet Loss (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            "session_setup_sr": {
                "num": [
                    "newwd_cell_session_setup_success_rate_a_num",
                    "newwd_cell_session_setup_success_rate_b_num",
                    "newwd_cell_session_setup_success_rate_c_num",
                ],
                "den": [
                    "newwd_cell_session_setup_success_rate_a_den",
                    "newwd_cell_session_setup_success_rate_b_den",
                    "newwd_cell_session_setup_success_rate_c_den",
                ],
                "label": "SSSR (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "erab_drop_rate": {
                "num": "newwd_lerabdroprate_num",
                "den": "newwd_lerabdroprate_den",
                "label": "ERAB Drop Rate (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            "handover_sr": {
                "num": "newwd_cell_handover_success_rate_inter_and_intra_frequency_num",
                "den": "newwd_cell_handover_success_rate_inter_and_intra_frequency_den",
                "label": "Handover Success Rate (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "avg_cqi": {
                "num": "newwd_cell_average_cqi_num",
                "den": "newwd_cell_average_cqi_den",
                "label": "Average CQI",
                "format": ".2f",
                "chart_type": "line",
            },
            "spectral_efficiency": {
                "num": "newwd_spectral_efficiency_dl_num",
                "den": "newwd_spectral_efficiency_dl_den",
                "label": "Spectrum Efficiency",
                "format": ".4f",
                "chart_type": "line",
            },
            "qpsk": {
                "num": "newwd_cell_qpsk_rate_num",
                "den": "newwd_cell_qpsk_rate_den",
                "label": "QPSK Rate (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "packet_latency": {
                "num": "newwd_cell_packet_latency_num",
                "den": "newwd_cell_packet_latency_den",
                "label": "Packet Latency (ms)",
                "format": ".2f",
                "chart_type": "line",
            },
            "lasttti": {
                "num": "newwd_cell_last_tti_ratio_num",
                "den": "newwd_cell_last_tti_ratio_den",
                "label": "Last TTI Ratio (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "voltecssr": {
                "num": "newwd_cell_volte_cssr_num_4",
                "den": "newwd_cell_volte_cssr_denom_4",
                "label": "VoLTE CSSR (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
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
                + pl.col("newwd_cell_fdd_band")
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
        if "newwd_date" not in df.columns:
            logger.warning("Column 'newwd_date' not found")
            return df

        date_formats = ["%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%d/%m/%Y", "%Y/%m/%d"]

        parsed_columns = []

        for idx, date_format in enumerate(date_formats):
            try:
                parsed_col = (
                    pl.col("newwd_date")
                    .str.strptime(pl.Date, date_format, strict=False)
                    .alias(f"parsed_{idx}")
                )
                parsed_columns.append(parsed_col)
            except Exception as e:
                logger.debug(f"Format {date_format} setup failed: {e}")
                continue

        if not parsed_columns:
            logger.warning("No date formats could be set up")
            return df.with_columns(pl.col("newwd_date").alias("date_parsed"))

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
            df = df.with_columns(pl.col("newwd_date").alias("date_parsed"))

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

        date_col = "date_parsed" if "date_parsed" in df.columns else "newwd_date"

        chart_data = (
            df.group_by(
                [
                    date_col,
                    "band_sector_key",
                    "newta_sector_name",
                    "newwd_enodeb_fdd_msc",
                ]
            )
            .agg(
                [
                    pl.col("kpi_value").mean().alias("avg_kpi"),
                    pl.col("newwd_cell_fdd_band").first().alias("band"),
                    pl.col("newwd_date").first().alias("newwd_date"),
                ]
            )
            .sort(date_col)
        )

        return chart_data

    # def _create_sector_chart(
    #     self, df: pl.DataFrame, sector_name: str, kpi_name: str
    # ) -> go.Figure:
    #     """Create chart (line or area) for a specific sector"""
    #     config = self.kpi_configs[kpi_name]
    #     sector_data = df.filter(pl.col("newta_sector_name") == sector_name)

    #     if sector_data.is_empty():
    #         return None

    #     fig = go.Figure()
    #     unique_keys = sector_data["band_sector_key"].unique().sort().to_list()
    #     chart_type = config.get("chart_type", "line")

    #     for idx, band_sector_key in enumerate(unique_keys):
    #         line_data = sector_data.filter(pl.col("band_sector_key") == band_sector_key)

    #         if line_data.is_empty():
    #             continue

    #         color = self.color_palette[idx % len(self.color_palette)]

    #         if (
    #             "date_parsed" in line_data.columns
    #             and not line_data["date_parsed"].is_null().any()
    #         ):
    #             x_data = line_data["date_parsed"].to_list()
    #             hover_date_format = "%m/%d/%Y"
    #         else:
    #             x_data = line_data["newwd_date"].to_list()
    #             hover_date_format = "%s"

    #         if chart_type == "area":
    #             fig.add_trace(
    #                 go.Scatter(
    #                     x=x_data,
    #                     y=line_data["avg_kpi"].to_list(),
    #                     name=band_sector_key,
    #                     mode="lines",
    #                     fill="tonexty",
    #                     line=dict(color=color, width=2),
    #                     fillcolor=color.replace(")", ", 0.3)").replace("rgb", "rgba"),
    #                     hovertemplate="<b>%{fullData.name}</b><br>"
    #                     + f"Date: %{{x|{hover_date_format}}}<br>"
    #                     + f"{config['label']}: %{{y:{config['format']}}}<br>"
    #                     + "<extra></extra>",
    #                 )
    #             )
    #         else:
    #             fig.add_trace(
    #                 go.Scatter(
    #                     x=x_data,
    #                     y=line_data["avg_kpi"].to_list(),
    #                     name=band_sector_key,
    #                     mode="lines+markers",
    #                     line=dict(color=color, width=4),
    #                     marker=dict(size=8, color=color),
    #                     hovertemplate="<b>%{fullData.name}</b><br>"
    #                     + f"Date: %{{x|{hover_date_format}}}<br>"
    #                     + f"{config['label']}: %{{y:{config['format']}}}<br>"
    #                     + "<extra></extra>",
    #                 )
    #             )

    #     fig.update_layout(
    #         title_text=f"SECTOR - {sector_name}",
    #         title_x=0.4,
    #         title_font=dict(size=20, color="#000000"),
    #         xaxis_title="",
    #         yaxis_title=config["label"],
    #         yaxis_title_font=dict(size=16, family="Arial, sans-serif"),
    #         template="plotly_white",
    #         font=dict(size=14, color="#000000"),
    #         hovermode="x unified",
    #         autosize=True,
    #         legend=dict(
    #             orientation="h",
    #             yanchor="top",
    #             y=-0.45,
    #             xanchor="center",
    #             x=0.5,
    #             font=dict(size=12),
    #             bgcolor=self.container_bg,
    #             bordercolor=self.border_color,
    #             borderwidth=1,
    #         ),
    #         width=600,
    #         height=350,
    #         margin=dict(l=80, r=80, t=40, b=40),
    #         plot_bgcolor=self.silver_light_bg,
    #         paper_bgcolor=self.silver_light_bg,
    #     )

    #     fig.update_xaxes(
    #         showgrid=True,
    #         gridwidth=1,
    #         gridcolor=self.grid_color,
    #         tickformat="%m/%d/%Y",
    #         tickangle=-45,
    #         tickfont=dict(size=14),
    #         tickmode="auto",
    #         linecolor=self.border_color,
    #         linewidth=2,
    #         mirror=True,
    #         showline=True,
    #     )

    #     fig.update_yaxes(
    #         showgrid=True,
    #         gridwidth=1,
    #         gridcolor=self.grid_color,
    #         tickfont=dict(size=14),
    #         linecolor=self.border_color,
    #         linewidth=2,
    #         mirror=True,
    #         showline=True,
    #     )

    #     return fig

    def _create_sector_chart(
        self, df: pl.DataFrame, sector_name: str, kpi_name: str
    ) -> go.Figure:
        """Create chart (line or area) for a specific sector"""
        config = self.kpi_configs[kpi_name]
        sector_data = df.filter(pl.col("newta_sector_name") == sector_name)

        if sector_data.is_empty():
            return None

        chart_type = config.get("chart_type", "line")
        
        # Convert to pandas for plotly express
        sector_df = sector_data.to_pandas()
        
        # Determine x-axis column
        if (
            "date_parsed" in sector_df.columns
            and sector_df["date_parsed"].notna().any()
        ):
            x_col = "date_parsed"
        else:
            x_col = "newwd_date"

        if chart_type == "area":
            # Use plotly express for stacked area chart
            fig = px.area(
                sector_df,
                x=x_col,
                y="avg_kpi",
                color="band_sector_key",  # This creates the line_group
                line_group="band_sector_key",
                color_discrete_sequence=self.color_palette,
                labels={
                    "avg_kpi": config["label"],
                    x_col: "",
                    "band_sector_key": ""
                },
                hover_data={
                    x_col: "|%m/%d/%Y",
                    "avg_kpi": f":{config['format']}",
                    "band_sector_key": True
                }
            )
            
            # Update hover template for better formatting
            fig.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>"
                + "Date: %{x|%m/%d/%Y}<br>"
                + f"{config['label']}: %{{y:{config['format']}}}<br>"
                + "<extra></extra>"
            )
            
        else:
            # Keep your existing line chart logic
            fig = go.Figure()
            unique_keys = sector_data["band_sector_key"].unique().sort().to_list()
            
            for idx, band_sector_key in enumerate(unique_keys):
                line_data = sector_data.filter(pl.col("band_sector_key") == band_sector_key)

                if line_data.is_empty():
                    continue

                color = self.color_palette[idx % len(self.color_palette)]

                if (
                    "date_parsed" in line_data.columns
                    and not line_data["date_parsed"].is_null().any()
                ):
                    x_data = line_data["date_parsed"].to_list()
                    hover_date_format = "%m/%d/%Y"
                else:
                    x_data = line_data["newwd_date"].to_list()
                    hover_date_format = "%s"

                fig.add_trace(
                    go.Scatter(
                        x=x_data,
                        y=line_data["avg_kpi"].to_list(),
                        name=band_sector_key,
                        mode="lines+markers",
                        line=dict(color=color, width=4),
                        marker=dict(size=8, color=color),
                        hovertemplate="<b>%{fullData.name}</b><br>"
                        + f"Date: %{{x|{hover_date_format}}}<br>"
                        + f"{config['label']}: %{{y:{config['format']}}}<br>"
                        + "<extra></extra>",
                    )
                )

        # Apply common layout settings
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
                font=dict(size=12),
                bgcolor=self.container_bg,
                bordercolor=self.border_color,
                borderwidth=1,
            ),
            width=600,
            height=350,
            margin=dict(l=80, r=80, t=40, b=40),
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
        unique_towerid = chart_data["newwd_enodeb_fdd_msc"].unique().sort().to_list()

        config = self.kpi_configs[kpi_name]

        if unique_towerid:
            tower_display = (
                f"- {', '.join(unique_towerid)}"
                if len(unique_towerid) > 1
                else f"- {unique_towerid[0]}"
            )
        else:
            tower_display = ""

        st.markdown(f"### 📊 {config['label']} Daily {tower_display}")

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
                        )["newwd_enodeb_fdd_msc"].first()

                        with stylable_container(
                            key=f"sector_chart_{tower_id}_{sector}_{kpi_name}_{idx}",
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
                                st.plotly_chart(fig, width="stretch")
                            else:
                                st.info(f"No data for sector {sector}")

    def render_all_kpis(self, df: pl.DataFrame):
        """Render all KPI charts sequentially"""
        if df.is_empty():
            st.warning("No WD+TA data available for visualization")
            return

        all_kpis = [
            "avg_cqi",
            "spectral_efficiency",
            "qpsk",
            "total_payload",
            "dl_user_throughput",
            "ul_user_throughput",
            "pdcp_dl_throughput",
            "pdcp_ul_throughput",
            "lasttti",
            "voltecssr",
            "volte_dl_loss",
            "volte_ul_loss",
            "session_setup_sr",
            "erab_drop_rate",
            "handover_sr",
            "packet_latency",
        ]

        for kpi in all_kpis:
            self.render_kpi_charts_by_sector(df, kpi)