"""
============================================================================
FILE: src/services/lte_hourly_visualizer.py
UPDATED: Removed KPI selection - show all KPIs automatically
============================================================================
"""

import streamlit as st
import polars as pl
import plotly.graph_objects as go
from typing import List, Dict, Union
import logging
from streamlit_extras.stylable_container import stylable_container

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LTEHourlyVisualizer:
    """Visualizes LTE Hourly data with sector-based charts and KPI calculations"""

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
            "#ec4899",
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
        """✅ KPI configurations using ACTUAL column names from your data"""
        return {
            # Total Traffic (GB) - Simple column
            "total_traffic": {
                "col": "lte_hour_total_traffic_gb",
                "label": "Total Traffic (GB)",
                "format": ".2f",
                "chart_type": "area",
            },
            # User DL Throughput (Mbps)
            "dl_user_throughput": {
                "num": "lte_hour_user_dl_thp_num",
                "den": "lte_hour_user_dl_thp_den",
                "label": "User DL Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            # User UL Throughput (Mbps)
            "ul_user_throughput": {
                "num": "lte_hour_user_ul_thp_num",
                "den": "lte_hour_user_ul_thp_den",
                "label": "User UL Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            # Cell DL Throughput (Mbps)
            "cell_dl_throughput": {
                "num": "lte_hour_cell_dl_thp_num",
                "den": "lte_hour_cell_dl_thp_den",
                "label": "Cell DL Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            # Cell UL Throughput (Mbps)
            "cell_ul_throughput": {
                "num": "lte_hour_cell_ul_thp_num",
                "den": "lte_hour_cell_ul_thp_den",
                "label": "Cell UL Throughput (Mbps)",
                "format": ".2f",
                "chart_type": "line",
            },
            # VoLTE DL Packet Loss Rate (%)
            "volte_dl_loss": {
                "num": "lte_hour_volte_dl_plr_num",
                "den": "lte_hour_volte_dl_plr_den",
                "label": "VoLTE DL Packet Loss (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            # VoLTE UL Packet Loss Rate (%)
            "volte_ul_loss": {
                "num": "lte_hour_volte_ul_plr_num",
                "den": "lte_hour_volte_ul_plr_den",
                "label": "VoLTE UL Packet Loss (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            # Session Setup Success Rate - SSSR (%)
            "sssr": {
                "num": [
                    "lte_hour_sssr_rrc_num_a",
                    "lte_hour_sssr_erab_num_b",
                    "lte_hour_sssr_s1_num_c",
                ],
                "den": [
                    "lte_hour_sssr_rrc_den_a",
                    "lte_hour_sssr_erab_den_b",
                    "lte_hour_sssr_s1_den_c",
                ],
                "label": "SSSR (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            # VoLTE Call Setup Success Rate - CSSR (%)
            "volte_cssr": {
                "num": [
                    "lte_hour_volte_cssr_rrc_num_a",
                    "lte_hour_volte_cssr_erab_num_b",
                    "lte_hour_volte_cssr_s1_num_c",
                    "lte_hour_volte_cssr_volte_num_d",
                ],
                "den": [
                    "lte_hour_volte_cssr_rrc_den_a",
                    "lte_hour_volte_cssr_erab_den_b",
                    "lte_hour_volte_cssr_s1_den_c",
                    "lte_hour_volte_cssr_volte_den_d",
                ],
                "label": "VoLTE CSSR (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            # ERAB Drop Rate (%)
            "erab_drop_rate": {
                "num": "lte_hour_erab_drop_num",
                "den": "lte_hour_erab_drop_den",
                "label": "ERAB Drop Rate (%)",
                "format": ".4f",
                "is_percent": True,
                "chart_type": "line",
            },
            # Handover Success Rate (%)
            "handover_sr": {
                "num": "lte_hour_intra_inter_hosr_num",
                "den": "lte_hour_intra_inter_hosr_den",
                "label": "Handover Success Rate (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            # Average CQI
            "avg_cqi": {
                "num": "lte_hour_cqi_num",
                "den": "lte_hour_cqi_den",
                "label": "Average CQI",
                "format": ".2f",
                "chart_type": "line",
            },
            # DL QPSK Rate (%)
            "qpsk_rate": {
                "num": "lte_hour_dl_qpsk_num",
                "den": "lte_hour_dl_qpsk_den",
                "label": "DL QPSK Rate (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            # DL Spectral Efficiency
            "spectral_efficiency": {
                "num": "lte_hour_dl_se_num",
                "den": "lte_hour_dl_se_den",
                "label": "DL Spectral Efficiency",
                "format": ".4f",
                "chart_type": "line",
            },
            "ran_latency": {
                "num": "lte_hour_ran_latency_num",
                "den": "lte_hour_ran_latency_den",
                "label": "RAN Latency (ms)",
                "format": ".2f",
                "chart_type": "line",
            },
            "dl_prb_util": {
                "num": "lte_hour_dl_prb_util_num",
                "den": "lte_hour_dl_prb_util_den",
                "label": "DL PRB Utilization (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            "ul_prb_util": {
                "num": "lte_hour_ul_prb_util_num",
                "den": "lte_hour_ul_prb_util_den",
                "label": "UL PRB Utilization (%)",
                "format": ".2f",
                "is_percent": True,
                "chart_type": "line",
            },
            # VoLTE Traffic (Erlang)
            "volte_traffic": {
                "col": "lte_hour_volte_traffic_erl",
                "label": "VoLTE Traffic (Erlang)",
                "format": ".4f",
                "chart_type": "area",
            },
            # Cell Availability (%)
            "cell_availability": {
                "col": "lte_hour_cell_avail_ssh1",
                "label": "Cell Availability (%)",
                "format": ".2f",
                "chart_type": "line",
            },
            # MAX RRC Users
            "maxrrc": {
                "col": "lte_hour_conn_user_max",
                "label": "MAX RRC Users",
                "format": ".2f",
                "chart_type": "line",
            },
        }

    def _create_band_sector_key(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create band+sector combined key for grouping"""
        if "band" not in df.columns or "sector" not in df.columns:
            logger.warning("❌ Missing band or sector columns for grouping")
            return df

        df = df.with_columns(
            (
                "L"
                + pl.col("band").cast(pl.Utf8)
                + " SEC "
                + pl.col("sector").cast(pl.Utf8)
            ).alias("band_sector_key")
        )
        return df

    def _create_sector_chart(
        self, df: pl.DataFrame, sector_name: str, kpi_name: str
    ) -> go.Figure:
        """Create chart (line or area) for a specific sector"""
        config = self.kpi_configs[kpi_name]
        sector_data = df.filter(pl.col("sector") == sector_name)

        if sector_data.is_empty():
            logger.warning(f"⚠️ No data for sector {sector_name}")
            return None

        fig = go.Figure()
        unique_keys = sector_data["band_sector_key"].unique().sort().to_list()
        chart_type = config.get("chart_type", "line")

        for idx, band_sector_key in enumerate(unique_keys):
            line_data = sector_data.filter(pl.col("band_sector_key") == band_sector_key)

            if line_data.is_empty():
                continue

            color = self.color_palette[idx % len(self.color_palette)]
            x_data = line_data["lte_hour_begin_time"].to_list()
            y_data = line_data["avg_kpi"].to_list()

            # Area chart or Line chart
            if chart_type == "area":
                fig.add_trace(
                    go.Scatter(
                        x=x_data,
                        y=y_data,
                        name=band_sector_key,
                        mode="lines",
                        fill="tozeroy",
                        line=dict(color=color, width=2),
                        fillcolor=color.replace(")", ", 0.3)").replace("rgb", "rgba"),
                        hovertemplate="<b>%{fullData.name}</b><br>"
                        + "Time: %{x|%Y-%m-%d %H:%M}<br>"
                        + f"{config['label']}: %{{y:{config['format']}}}<br>"
                        + "<extra></extra>",
                    )
                )
            else:
                fig.add_trace(
                    go.Scatter(
                        x=x_data,
                        y=y_data,
                        name=band_sector_key,
                        mode="lines",
                        line=dict(color=color, width=3),
                        marker=dict(size=8, color=color),
                        hovertemplate="<b>%{fullData.name}</b><br>"
                        + "Time: %{x|%Y-%m-%d %H:%M}<br>"
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
                y=-0.5,
                xanchor="center",
                x=0.5,
                font=dict(size=14),
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
            tickformat="%m/%d %H:%M",
            tickangle=-45,
            tickfont=dict(size=14),
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
        """Render KPI charts separated by sector in 3-column grid layout"""
        if df.is_empty():
            st.warning(f"❌ No data available for {kpi_name}")
            return

        with st.spinner(f"Generating {kpi_name} charts..."):
            chart_data = self._prepare_chart_data(df, kpi_name)

        if chart_data.is_empty():
            st.warning(f"⚠️ No valid data after KPI calculation for {kpi_name}")
            return

        unique_sectors = chart_data["sector"].unique().sort().to_list()
        tower_name = (
            chart_data["tower_name"].first()
            if "tower_name" in chart_data.columns
            else "Unknown"
        )
        config = self.kpi_configs[kpi_name]

        st.markdown(f"### 📊 {config['label']} Hourly - {tower_name}")

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
                        with stylable_container(
                            key=f"lte_sector_chart_{tower_name}_{sector}_{kpi_name}_{idx}",
                            css_styles=f"""
                            {{
                                background-color: {self.silver_light_bg};
                                border: 2px solid {self.border_color};
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
                                st.info(f"📭 No data for sector {sector}")

    def render_all_kpis(self, df: pl.DataFrame):
        """
        ✅ UPDATED: Render all available KPIs automatically without selection
        """
        if df.is_empty():
            st.warning("❌ No LTE hourly data available for visualization")
            return

        priority_kpis = [
            "avg_cqi",
            "spectral_efficiency",
            "qpsk_rate",
            "dl_user_throughput",
            "ul_user_throughput",
            "cell_dl_throughput",
            "cell_ul_throughput",
            "cell_availability",
            "total_traffic",
            "maxrrc",
            "dl_prb_util",
            "sssr",
            "volte_cssr",
            "erab_drop_rate",
            "handover_sr",
            "ul_prb_util",
            "ran_latency",
            "volte_traffic",
            "volte_dl_loss",
            "volte_ul_loss",
        ]

        # Check which KPIs are available
        available_kpis = []
        for kpi in priority_kpis:
            config = self.kpi_configs[kpi]
            if "col" in config:
                if config["col"] in df.columns:
                    available_kpis.append(kpi)
            else:
                # Check if all required num/den columns exist
                num_cols = config["num"] if isinstance(config["num"], list) else [config["num"]]
                den_cols = config["den"] if isinstance(config["den"], list) else [config["den"]]
                
                if all(col in df.columns for col in num_cols + den_cols):
                    available_kpis.append(kpi)

        if not available_kpis:
            st.error("❌ No KPIs can be calculated with available data")
            st.write(
                "Available numeric columns:",
                [col for col in df.columns if df[col].dtype in [pl.Int64, pl.Float64]],
            )
            return

        for kpi in available_kpis:
            self.render_kpi_charts_by_sector(df, kpi)

    def _clean_numeric_column(self, df: pl.DataFrame, col_name: str) -> pl.DataFrame:
        """
        ✅ Clean numeric column - handle all data types properly
        """
        if col_name not in df.columns:
            logger.warning(f"❌ Column {col_name} not found in DataFrame")
            return df

        try:
            col_dtype = df[col_name].dtype
            
            # Already numeric - just ensure Float64
            if col_dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
                df = df.with_columns(
                    pl.col(col_name).cast(pl.Float64, strict=False).alias(col_name)
                )
                logger.debug(f"✅ Column {col_name} already numeric, cast to Float64")
                return df
            
            # String type - needs cleaning
            if col_dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col(col_name)
                    .str.replace_all(",", "")
                    .str.replace_all('"', "")
                    .str.replace_all("%", "")
                    .str.strip_chars()
                    .cast(pl.Float64, strict=False)
                    .alias(col_name)
                )
                logger.debug(f"✅ Cleaned string column {col_name}")
                return df
            
            # Null type - cast to Float64
            if col_dtype == pl.Null:
                df = df.with_columns(
                    pl.lit(None).cast(pl.Float64).alias(col_name)
                )
                logger.debug(f"⚠️ Column {col_name} is all nulls")
                return df
            
            # Other types - try casting
            df = df.with_columns(
                pl.col(col_name).cast(pl.Float64, strict=False).alias(col_name)
            )
            logger.debug(f"✅ Cast column {col_name} from {col_dtype} to Float64")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not clean column {col_name}: {e}")
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col_name))
        
        return df

    def _calculate_kpi(
        self,
        df: pl.DataFrame,
        num_col: Union[str, List[str]],
        den_col: Union[str, List[str]],
        is_percent: bool = False,
    ) -> pl.DataFrame:
        """
        ✅ Calculate KPI with proper null handling and debugging
        """
        is_list = isinstance(num_col, list)

        if is_list:
            # Multiple component calculation (SSSR, CSSR)
            if not isinstance(den_col, list) or len(num_col) != len(den_col):
                logger.warning("❌ Num and den must both be lists of equal length")
                return df.with_columns(pl.lit(None).alias("kpi_value"))

            # Clean all columns
            for nc in num_col:
                if nc not in df.columns:
                    logger.error(f"❌ Missing numerator column: {nc}")
                    return df.with_columns(pl.lit(None).alias("kpi_value"))
                df = self._clean_numeric_column(df, nc)
                
            for dc in den_col:
                if dc not in df.columns:
                    logger.error(f"❌ Missing denominator column: {dc}")
                    return df.with_columns(pl.lit(None).alias("kpi_value"))
                df = self._clean_numeric_column(df, dc)

            # Calculate product of ratios
            ratio_product = pl.lit(1.0)

            for nc, dc in zip(num_col, den_col):
                ratio = (
                    pl.when((pl.col(dc).is_not_null()) & (pl.col(dc) != 0))
                    .then(pl.col(nc) / pl.col(dc))
                    .otherwise(pl.lit(1.0))
                )
                ratio_product = ratio_product * ratio

            multiplier = 100.0 if is_percent else 1.0
            kpi_expr = ratio_product * multiplier
            df = df.with_columns(kpi_expr.alias("kpi_value"))

        else:
            if num_col not in df.columns:
                logger.error(f"❌ Missing numerator column: {num_col}")
                return df.with_columns(pl.lit(None).alias("kpi_value"))
                
            if den_col not in df.columns:
                logger.error(f"❌ Missing denominator column: {den_col}")
                return df.with_columns(pl.lit(None).alias("kpi_value"))

            df = self._clean_numeric_column(df, num_col)
            df = self._clean_numeric_column(df, den_col)
            
            num_nulls = df.select(pl.col(num_col).null_count()).item()
            den_nulls = df.select(pl.col(den_col).null_count()).item()
            logger.debug(f"📊 {num_col}: {num_nulls} nulls, {den_col}: {den_nulls} nulls")

            # Calculate KPI
            if is_percent:
                expr = (
                    pl.when((pl.col(den_col).is_not_null()) & (pl.col(den_col) != 0))
                    .then((pl.col(num_col) / pl.col(den_col)) * 100.0)
                    .otherwise(None)
                )
            else:
                expr = (
                    pl.when((pl.col(den_col).is_not_null()) & (pl.col(den_col) != 0))
                    .then(pl.col(num_col) / pl.col(den_col))
                    .otherwise(None)
                )

            df = df.with_columns(expr.alias("kpi_value"))
            
            # DEBUG: Check KPI values
            kpi_nulls = df.select(pl.col("kpi_value").null_count()).item()
            logger.debug(f"📊 KPI calculated: {kpi_nulls} nulls out of {len(df)} rows")

        return df

    def _prepare_chart_data(self, df: pl.DataFrame, kpi_name: str) -> pl.DataFrame:
        """
        ✅ Prepare data for specific KPI chart with better debugging
        """
        config = self.kpi_configs.get(kpi_name)
        if not config:
            logger.error(f"❌ Unknown KPI: {kpi_name}")
            return pl.DataFrame()

        logger.info(f"📊 Preparing chart data for KPI: {kpi_name}")
        
        # Verify required columns exist
        if "col" in config:
            if config["col"] not in df.columns:
                logger.error(f"❌ Column {config['col']} not found for KPI {kpi_name}")
                return pl.DataFrame()
        else:
            # Check num/den columns
            num_cols = config["num"] if isinstance(config["num"], list) else [config["num"]]
            den_cols = config["den"] if isinstance(config["den"], list) else [config["den"]]
            
            missing_cols = []
            for col in num_cols + den_cols:
                if col not in df.columns:
                    missing_cols.append(col)
            
            if missing_cols:
                logger.error(f"❌ Missing columns for {kpi_name}: {missing_cols}")
                return pl.DataFrame()

        # Create working copy
        chart_df = df.clone()

        # Calculate or extract KPI value
        if "col" in config:
            chart_df = self._clean_numeric_column(chart_df, config["col"])
            chart_df = chart_df.with_columns(pl.col(config["col"]).alias("kpi_value"))
        else:
            chart_df = self._calculate_kpi(
                chart_df, config["num"], config["den"], config.get("is_percent", False)
            )

        # Filter out null values
        before_filter = len(chart_df)
        chart_df = chart_df.filter(pl.col("kpi_value").is_not_null())
        after_filter = len(chart_df)
        
        logger.info(f"📊 KPI filtering: {before_filter} → {after_filter} rows (removed {before_filter - after_filter} nulls)")

        if chart_df.is_empty():
            logger.warning(f"⚠️ No valid data for KPI {kpi_name} after filtering")
            return pl.DataFrame()

        # Verify sector and band columns exist
        if "sector" not in chart_df.columns or "band" not in chart_df.columns:
            logger.error(f"❌ Missing sector/band columns for grouping")
            return pl.DataFrame()

        # Create band+sector key
        chart_df = self._create_band_sector_key(chart_df)

        # Verify datetime column
        if "lte_hour_begin_time" not in chart_df.columns:
            logger.error(f"❌ Missing datetime column: lte_hour_begin_time")
            return pl.DataFrame()

        # Aggregate by hour and band_sector_key
        group_columns = ["lte_hour_begin_time", "band_sector_key", "sector", "band"]
        agg_columns = [pl.col("kpi_value").mean().alias("avg_kpi")]

        # Add tower name if available
        if "clean_tower_id" in chart_df.columns:
            agg_columns.append(pl.col("clean_tower_id").first().alias("tower_name"))
        elif "lte_hour_me_name" in chart_df.columns:
            agg_columns.append(pl.col("lte_hour_me_name").first().alias("tower_name"))

        try:
            chart_data = (
                chart_df.group_by(group_columns)
                .agg(agg_columns)
                .sort("lte_hour_begin_time")
            )
            
            logger.info(f"✅ Chart data prepared: {len(chart_data)} rows for {kpi_name}")
            
            return chart_data
            
        except Exception as e:
            logger.error(f"❌ Error aggregating data for {kpi_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pl.DataFrame()