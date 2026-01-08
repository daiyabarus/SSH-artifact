"""
============================================================================
FILE: src/ui/dashboard.py
Refactored with Clean Architecture, SOLID principles, and streamlit-extras
============================================================================
"""

import streamlit as st
from typing import List, Optional
from datetime import datetime
import polars as pl
from abc import ABC, abstractmethod

# Services
from src.services.data_service import DataService
from src.services.coverage_map_service import render_coverage_map
from src.services.ta_distribution_visualizer import TADistributionVisualizer
from src.services.wd_ta_chart_visualizer import WDTAChartVisualizer
from src.services.bh_ta_chart_visualizer import BHTAChartVisualizer
from src.services.lte_hourly_visualizer import LTEHourlyVisualizer

# Utilities
from src.utils.style.global_css import inject_global_css
from src.utils.style.containers import info_box

# Streamlit Extras
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.colored_header import colored_header
from streamlit_extras.dataframe_explorer import dataframe_explorer


class TabRenderer(ABC):
    """
    Abstract base class for tab rendering
    Implements Open/Closed Principle - open for extension, closed for modification
    """

    def __init__(self, data_service: DataService):
        self.data_service = data_service

    @abstractmethod
    def render(self, tower_ids: List[str], start_date: datetime, end_date: datetime):
        """Render the tab content"""
        pass

    def _show_empty_state(self, message: str):
        """Show consistent empty state UI"""
        st.warning(f"⚠️ {message}")


class MetricsRenderer:
    """
    Single Responsibility: Handle metrics display
    """

    @staticmethod
    def render_metric_card(label: str, value: str, delta: Optional[str] = None):
        """Render a single metric card"""
        st.metric(label=label, value=value, delta=delta)

    @staticmethod
    def render_metric_grid(metrics: List[dict], columns: int = 4):
        """
        Render multiple metrics in a grid layout

        Args:
            metrics: List of dicts with 'label', 'value', 'delta' keys
            columns: Number of columns in the grid
        """
        cols = st.columns(columns)
        for idx, metric in enumerate(metrics):
            with cols[idx % columns]:
                st.metric(
                    label=metric.get("label", ""),
                    value=metric.get("value", "N/A"),
                    delta=metric.get("delta"),
                )
        style_metric_cards()


class DataValidator:
    """
    Single Responsibility: Validate dashboard inputs
    """

    @staticmethod
    def validate_tower_selection(tower_ids: List[str]) -> bool:
        """Validate tower ID selection"""
        if not tower_ids:
            st.error("❌ Please select at least one tower ID")
            return False
        return True

    @staticmethod
    def validate_date_range(start_date: datetime, end_date: datetime) -> bool:
        """Validate date range"""
        if start_date > end_date:
            st.error("❌ Start date must be before end date")
            return False
        return True

    @staticmethod
    def validate_dataframe(df: pl.DataFrame, name: str = "Data") -> bool:
        """Validate dataframe is not empty"""
        if df.is_empty():
            st.warning(f"⚠️ No {name} available")
            return False
        return True


class CoverageTabRenderer(TabRenderer):
    """Render Coverage Map & TA Distribution tab"""

    def render(self, tower_ids: List[str], start_date: datetime, end_date: datetime):
        """Render coverage map and TA distribution"""
        df_gcell_scot_ta = self.data_service.get_joined_gcell_scot_ta(tower_ids)

        if not DataValidator.validate_dataframe(df_gcell_scot_ta, "configuration data"):
            return

        col1, col2 = st.columns([2, 1])

        with col1:
            with st.container(border=True):
                colored_header(
                    label="🗺️ Map Overview",
                    description="Site coverage visualization",
                    color_name="blue-70",
                )
                render_coverage_map(df_gcell_scot_ta)

        with col2:
            with st.container(border=True):
                colored_header(
                    label="📍 TA Distribution",
                    description="LTE Timing Advance distribution by sector",
                    color_name="green-70",
                )
                self._render_ta_distribution(tower_ids)

    def _render_ta_distribution(self, tower_ids: List[str]):
        """Render TA Distribution visualizer"""
        df_ta_distribution = self.data_service.get_ta_distribution_data(tower_ids)

        if not DataValidator.validate_dataframe(df_ta_distribution, "TA distribution"):
            return

        visualizer = TADistributionVisualizer()
        selected_tower = tower_ids[0] if tower_ids else "Unknown"

        with st.spinner("📊 Generating TA distribution charts..."):
            visualizer.display_sector_charts_in_rows(df_ta_distribution, selected_tower)


class DailyStatisticsTabRenderer(TabRenderer):
    """Render Daily Statistics (WD+TA) tab"""

    def __init__(self, data_service: DataService):
        super().__init__(data_service)
        self.visualizer = WDTAChartVisualizer()

    def render(self, tower_ids: List[str], start_date: datetime, end_date: datetime):
        """Render WD+TA analytics with KPI charts"""
        with st.spinner("🔄 Loading daily statistics..."):
            df = self.data_service.get_joined_wd_ta(tower_ids, start_date, end_date)

        if not DataValidator.validate_dataframe(df, "daily statistics"):
            return

        # Summary metrics
        self._render_summary_metrics(df)

        # KPI Charts
        with st.spinner("📊 Generating KPI charts..."):
            self.visualizer.render_all_kpis(df)

    def _render_summary_metrics(self, df: pl.DataFrame):
        """Render summary metrics for WD+TA data"""
        metrics = [
            {
                "label": "Total Records",
                "value": f"{len(df):,}",
            },
            {
                "label": "TA Joins",
                "value": f"{df.select(pl.col('newta_sector').is_not_null().sum()).item():,}",
            },
            {
                "label": "Unique Dates",
                "value": f"{df.select(pl.col('newwd_date').n_unique()).item():,}",
            },
            {
                "label": "Unique Sectors",
                "value": f"{df.select(pl.col('newta_sector').n_unique()).item():,}",
            },
        ]

        MetricsRenderer.render_metric_grid(metrics, columns=4)
        st.markdown("---")


class BusyHourTabRenderer(TabRenderer):
    """Render Busy Hour Statistics (BH+TA) tab"""

    def __init__(self, data_service: DataService):
        super().__init__(data_service)
        self.visualizer = BHTAChartVisualizer()

    def render(self, tower_ids: List[str], start_date: datetime, end_date: datetime):
        """Render BH+TA analytics with KPI charts"""
        with st.spinner("🔄 Loading busy hour statistics..."):
            df = self.data_service.get_joined_bh_ta(tower_ids, start_date, end_date)

        if not DataValidator.validate_dataframe(df, "busy hour statistics"):
            return

        # Summary metrics
        self._render_summary_metrics(df)

        # KPI Charts
        with st.spinner("📊 Generating busy hour KPI charts..."):
            self.visualizer.render_all_kpis(df)

    def _render_summary_metrics(self, df: pl.DataFrame):
        """Render summary metrics for BH+TA data"""
        metrics = [
            {
                "label": "Total Records",
                "value": f"{len(df):,}",
            },
            {
                "label": "TA Joins",
                "value": f"{df.select(pl.col('newta_sector').is_not_null().sum()).item():,}",
            },
            {
                "label": "Unique Dates",
                "value": f"{df.select(pl.col('newbh_date').n_unique()).item():,}",
            },
            {
                "label": "Unique Sectors",
                "value": f"{df.select(pl.col('newta_sector').n_unique()).item():,}",
            },
        ]

        MetricsRenderer.render_metric_grid(metrics, columns=4)
        st.markdown("---")


class HourlyStatisticsTabRenderer(TabRenderer):
    """Render Hourly Statistics (LTE Hourly) tab"""

    def __init__(self, data_service: DataService):
        super().__init__(data_service)
        self.visualizer = LTEHourlyVisualizer()

    def render(self, tower_ids: List[str], start_date: datetime, end_date: datetime):
        """Render LTE Hourly analytics with KPI charts"""
        with st.spinner("🔄 Loading hourly statistics..."):
            df = self.data_service.get_lte_hourly_data(tower_ids, start_date, end_date)

        if not DataValidator.validate_dataframe(df, "hourly statistics"):
            return

        # Summary metrics
        self._render_summary_metrics(df)

        # KPI Charts
        with st.spinner("📊 Generating hourly KPI charts..."):
            self.visualizer.render_all_kpis(df)

    def _render_summary_metrics(self, df: pl.DataFrame):
        """Render summary metrics for LTE Hourly data"""
        total_traffic = None
        if "lte_hour_total_traffic_gb" in df.columns:
            total_traffic = df.select(pl.col("lte_hour_total_traffic_gb").sum()).item()

        metrics = [
            {
                "label": "Total Records",
                "value": f"{len(df):,}",
            },
            {
                "label": "Unique Sectors",
                "value": f"{df.select(pl.col('sector').n_unique()).item()}",
            },
            {
                "label": "Unique Bands",
                "value": f"{df.select(pl.col('band').n_unique()).item()}",
            },
            {
                "label": "Total Traffic",
                "value": f"{total_traffic:.2f} GB" if total_traffic else "N/A",
            },
        ]

        MetricsRenderer.render_metric_grid(metrics, columns=4)

        # Time range information
        if "lte_hour_begin_time" in df.columns:
            time_info = df.select(
                [
                    pl.col("lte_hour_begin_time").min().alias("start"),
                    pl.col("lte_hour_begin_time").max().alias("end"),
                ]
            ).to_dicts()[0]

            if time_info["start"] and time_info["end"]:
                time_range = time_info["end"] - time_info["start"]
                total_hours = (time_range.days * 24) + (time_range.seconds // 3600)
                st.info(
                    f"📅 **Time Range**: {time_info['start'].strftime('%Y-%m-%d %H:%M')} "
                    f"to {time_info['end'].strftime('%Y-%m-%d %H:%M')} "
                    f"({time_range.days} days, {total_hours} hours)"
                )

        st.markdown("---")


class DataTablesTabRenderer(TabRenderer):
    """Render Data Tables & Configuration tab"""

    def render(self, tower_ids: List[str], start_date: datetime, end_date: datetime):
        """Render all data tables in sub-tabs"""
        colored_header(
            label="📋 Data Tables & Configuration",
            description="Raw data and configuration views",
            color_name="violet-70",
        )

        subtabs = st.tabs(
            ["📊 WD+TA Data", "⏰ BH+TA Data", "⚙️ Configuration", "📞 2G Network"]
        )

        with subtabs[0]:
            self._render_wd_ta_table(tower_ids, start_date, end_date)

        with subtabs[1]:
            self._render_bh_ta_table(tower_ids, start_date, end_date)

        with subtabs[2]:
            self._render_configuration_tables(tower_ids)

        with subtabs[3]:
            self._render_2g_table(tower_ids, start_date, end_date)

    def _render_wd_ta_table(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render Joined WD+TA data table"""
        st.subheader("🔗 Joined Weekday + TA Data")

        df = self.data_service.get_joined_wd_ta(tower_ids, start_date, end_date)

        if not DataValidator.validate_dataframe(df, "WD+TA data"):
            return

        metrics = [
            {"label": "Total Records", "value": f"{len(df):,}"},
            {
                "label": "TA Joins",
                "value": f"{df.select(pl.col('newta_sector').is_not_null().sum()).item():,}",
            },
        ]

        MetricsRenderer.render_metric_grid(metrics, columns=2)

        st.info(
            "👇 **Table**: WD data joined with TA sector info (joined on moentity=eutrancell)"
        )

        # Using dataframe_explorer for better UX
        filtered_df = dataframe_explorer(df.to_pandas(), case=False)
        st.dataframe(filtered_df, width="stretch", height=500)

    def _render_bh_ta_table(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render Joined BH+TA data table"""
        st.subheader("🔗 Joined Busy Hour + TA Data")

        df = self.data_service.get_joined_bh_ta(tower_ids, start_date, end_date)

        if not DataValidator.validate_dataframe(df, "BH+TA data"):
            return

        st.metric("Total Records", f"{len(df):,}")
        st.info(
            "👇 **Table**: BH data joined with TA sector info (joined on moentity=eutrancell)"
        )

        filtered_df = dataframe_explorer(df.to_pandas(), case=False)
        st.dataframe(filtered_df, width="stretch", height=500)

    def _render_configuration_tables(self, tower_ids: List[str]):
        """Render Configuration tables (SCOT, GCELL)"""
        st.subheader("⚙️ Site Configuration")

        df_scot, df_gcell = self.data_service.get_configuration_data(tower_ids)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 📍 SCOT Data")
            if not df_scot.is_empty():
                st.success(f"✅ {len(df_scot)} records")
                st.dataframe(df_scot.to_pandas(), width="stretch", height=400)
            else:
                st.info("No SCOT data available")

        with col2:
            st.markdown("#### 📡 GCELL Data")
            if not df_gcell.is_empty():
                st.success(f"✅ {len(df_gcell)} records")
                st.dataframe(df_gcell.to_pandas(), width="stretch", height=400)
            else:
                st.info("No GCELL data available")

        st.markdown("---")
        st.markdown("#### 🔗 Joined GCELL + SCOT + TA")

        df_joined = self.data_service.get_joined_gcell_scot_ta(tower_ids)

        if not df_joined.is_empty():
            st.success(f"✅ {len(df_joined)} joined records")
            st.info(
                "👇 **Table**: GCELL + SCOT + TA (joined on moentity=eutrancell=cell)"
            )
            filtered_df = dataframe_explorer(df_joined.to_pandas(), case=False)
            st.dataframe(filtered_df, width="stretch", height=500)
        else:
            st.warning("⚠️ No joined configuration data available")

    def _render_2g_table(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render 2G Network data table"""
        st.subheader("📞 2G Network Data")

        df = self.data_service.get_twog_data_with_kpi(tower_ids, start_date, end_date)

        if not DataValidator.validate_dataframe(df, "2G data"):
            return

        st.success(f"✅ Loaded {len(df):,} records")
        filtered_df = dataframe_explorer(df.to_pandas(), case=False)
        st.dataframe(filtered_df, width="stretch", height=500)


class Dashboard:
    """
    Main dashboard component with tabbed interface
    Uses Dependency Injection and Composition (SOLID principles)
    """

    def __init__(self, data_service: DataService):
        """
        Initialize dashboard with required services

        Args:
            data_service: Service for fetching all data (daily, hourly, config)
        """
        self.data_service = data_service

        self.coverage_tab = CoverageTabRenderer(data_service)
        self.daily_stats_tab = DailyStatisticsTabRenderer(data_service)
        self.busy_hour_tab = BusyHourTabRenderer(data_service)
        self.hourly_stats_tab = HourlyStatisticsTabRenderer(data_service)
        self.data_tables_tab = DataTablesTabRenderer(data_service)

    def render(self, tower_ids: List[str], start_date: datetime, end_date: datetime):
        """
        Render complete dashboard with tabbed interface

        Args:
            tower_ids: List of tower IDs to analyze
            start_date: Start date for data filtering
            end_date: End date for data filtering
        """
        self._inject_styles()

        if not self._validate_inputs(tower_ids, start_date, end_date):
            return

        tabs = st.tabs(
            [
                "🗺️ Map",
                "📊 Daily Statistic",
                "⏰ Busy Hour Statistic",
                "⏱️ Hourly Statistic",
                "📋 Data Tables",
            ]
        )

        with tabs[0]:
            self.coverage_tab.render(tower_ids, start_date, end_date)

        with tabs[1]:
            self.daily_stats_tab.render(tower_ids, start_date, end_date)

        with tabs[2]:
            self.busy_hour_tab.render(tower_ids, start_date, end_date)

        with tabs[3]:
            self.hourly_stats_tab.render(tower_ids, start_date, end_date)

        with tabs[4]:
            self.data_tables_tab.render(tower_ids, start_date, end_date)

    def _inject_styles(self):
        """Inject global CSS styles"""
        css, _ = inject_global_css()
        st.markdown(css, unsafe_allow_html=True)

    def _validate_inputs(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> bool:
        """
        Validate dashboard inputs

        Returns:
            bool: True if inputs are valid, False otherwise
        """
        return DataValidator.validate_tower_selection(
            tower_ids
        ) and DataValidator.validate_date_range(start_date, end_date)
