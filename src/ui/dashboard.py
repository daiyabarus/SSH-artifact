"""
============================================================================
FILE: src/ui/dashboard.py
============================================================================
"""

import streamlit as st
from typing import List, Optional
from datetime import datetime
import polars as pl
from src.services.data_service import DataService
from src.services.coverage_map_service import render_coverage_map
from src.services.ta_distribution_visualizer import TADistributionVisualizer
from src.services.wd_ta_chart_visualizer import WDTAChartVisualizer
from src.services.lte_hourly_visualizer import LTEHourlyVisualizer
from src.utils.style.global_css import inject_global_css
from src.utils.style.containers import info_box


class Dashboard:
    """
    Main dashboard component with tabbed interface for better organization
    """

    def __init__(self, data_service: DataService):
        """
        Initialize dashboard with required services

        Args:
            data_service: Service for fetching all data (daily, hourly, config)
        """
        self._data_service = data_service
        self._wd_ta_visualizer = WDTAChartVisualizer()
        self._lte_hourly_visualizer = LTEHourlyVisualizer()

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

        tab1, tab2, tab3, tab4 = st.tabs(
            ["🗺️ Map", "📊 Daily Statistic", "⏱️ Hourly Statistic", "📋 Data Tables"]
        )

        with tab1:
            self._render_coverage_tab(tower_ids)

        with tab2:
            self._render_wd_ta_tab(tower_ids, start_date, end_date)

        with tab3:
            self._render_lte_hourly_tab(tower_ids, start_date, end_date)

        with tab4:
            self._render_data_tables_tab(tower_ids, start_date, end_date)

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
        if not tower_ids:
            st.error("❌ Please select at least one tower ID")
            return False

        if start_date > end_date:
            st.error("❌ Start date must be before end date")
            return False

        return True

    def _show_data_status(self, label: str, df: pl.DataFrame, icon: str = "📊") -> None:
        """
        Show data availability status with consistent formatting

        Args:
            label: Label for the data type
            df: DataFrame to check
            icon: Emoji icon to display
        """
        status = "✅" if not df.is_empty() else "❌"
        count = len(df) if not df.is_empty() else 0
        st.write(f"{status} **{icon} {label}**: {count:,} records")

    def _render_empty_state(self, message: str, tab_name: str = ""):
        """Render consistent empty state UI"""
        warning_html, _ = info_box(
            f"No data available for {tab_name}. {message}",
            box_type="warning",
            icon="⚠️",
        )
        st.markdown(warning_html, unsafe_allow_html=True)

    def _render_data_availability_summary(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render data availability check for all data sources"""
        with st.spinner("Checking data availability..."):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown("**📊 Daily Data**")
                df_wd = self._data_service.get_joined_wd_ta(
                    tower_ids, start_date, end_date
                )
                self._show_data_status("WD+TA", df_wd)

                df_bh = self._data_service.get_joined_bh_ta(
                    tower_ids, start_date, end_date
                )
                self._show_data_status("BH+TA", df_bh)

            with col2:
                st.markdown("**📡 Configuration**")
                df_gcell = self._data_service.get_joined_gcell_scot_ta(tower_ids)
                self._show_data_status("GCELL+SCOT+TA", df_gcell)

                df_ta_dist = self._data_service.get_ta_distribution_data(tower_ids)
                self._show_data_status("TA Distribution", df_ta_dist)

            with col3:
                st.markdown("**⏱️ Hourly Data**")
                df_hourly = self._data_service.get_lte_hourly_data(tower_ids)
                self._show_data_status("LTE Hourly", df_hourly)

                df_2g = self._data_service.get_twog_data_with_kpi(
                    tower_ids, start_date, end_date
                )
                self._show_data_status("2G Network", df_2g)

    def _render_coverage_tab(self, tower_ids: List[str]):
        """Render Coverage Map and TA Distribution tab"""
        # st.header("🗺️ Coverage Map & TA Distribution")

        df_gcell_scot_ta = self._data_service.get_joined_gcell_scot_ta(tower_ids)

        if df_gcell_scot_ta.is_empty():
            self._render_empty_state(
                "No configuration data found. Check tower IDs.", "Coverage Map"
            )
            return

        # Layout: Map on left, TA Distribution on right
        col1, col2 = st.columns([2, 1])

        with col1:
            with st.container(border=True):
                st.markdown("### 🗺️ Map Overview")
                render_coverage_map(df_gcell_scot_ta)

                with st.expander("ℹ️ Actions & Notes", expanded=False):
                    st.text_area("")

        with col2:
            with st.container(border=True):
                st.markdown("### 📍 TA Distributions")
                self._render_ta_distribution(tower_ids)

    def _render_ta_distribution(self, tower_ids: List[str]):
        """Render TA Distribution visualizer"""
        df_ta_distribution = self._data_service.get_ta_distribution_data(tower_ids)

        if df_ta_distribution.is_empty():
            st.info("No TA distribution data available")
            return

        visualizer = TADistributionVisualizer()
        selected_tower = tower_ids[0] if tower_ids else "Unknown"

        with st.spinner("Generating TA distribution charts..."):
            visualizer.display_sector_charts_in_rows(df_ta_distribution, selected_tower)

    def _render_wd_ta_tab(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render WD+TA Analytics tab with KPI charts"""
        df = self._data_service.get_joined_wd_ta(tower_ids, start_date, end_date)
        with st.spinner("Loading KPI charts..."):
            self._wd_ta_visualizer.render_all_kpis(df)

    def _render_wd_ta_summary(self, df: pl.DataFrame):
        """Render WD+TA summary metrics"""
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_records = len(df)
            st.metric("Total Records", f"{total_records:,}")

        with col2:
            ta_joins = df.select(pl.col("newta_sector").is_not_null().sum()).item()
            st.metric("TA Joins", f"{ta_joins:,}")

        with col3:
            unique_dates = df.select(pl.col("newwd_date").n_unique()).item()
            st.metric("Unique Dates", f"{unique_dates:,}")

        with col4:
            unique_sectors = df.select(pl.col("newta_sector").n_unique()).item()
            st.metric("Unique Sectors", f"{unique_sectors:,}")

    def _render_lte_hourly_tab(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """✅ SIMPLIFIED: Render LTE Hourly Analytics tab - langsung show semua KPI charts"""
        
        # Fetch data dengan date filter
        with st.spinner("🔄 Loading LTE hourly data..."):
            df = self._data_service.get_lte_hourly_data(tower_ids, start_date, end_date)
        
        if df.is_empty():
            st.error("❌ No LTE Hourly data found!")
            return
        
        # KPI Charts - langsung render semua
        # st.markdown("### 📈 Hourly KPI Trend Analysis")
        # st.info("💡 Charts show hourly granularity organized by sector and band")
        
        with st.spinner("📊 Generating hourly KPI charts..."):
            self._lte_hourly_visualizer.render_all_kpis(df)

    def _test_lte_hourly_connection(self, tower_ids: List[str]):
        """Test LTE Hourly database connection and table structure"""
        try:
            import sqlite3
            from src.config.settings import Settings

            settings = Settings()

            st.write("### 🧪 Connection Test Results")

            # Test database connection
            try:
                conn = sqlite3.connect(settings.DB_PATH)
                cursor = conn.cursor()
                st.success(f"✅ Database connection successful: {settings.DB_PATH}")

                # Check if table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='tbl_newltehourly'"
                )
                table_exists = cursor.fetchone()

                if table_exists:
                    st.success("✅ Table 'tbl_newltehourly' exists")

                    # Get column info
                    cursor.execute("PRAGMA table_info(tbl_newltehourly)")
                    columns = cursor.fetchall()

                    st.write("### 📋 Table Columns:")
                    for col in columns:
                        st.write(f"- `{col[1]}` ({col[2]})")

                    # Check sample data
                    cursor.execute("SELECT COUNT(*) FROM tbl_newltehourly")
                    total_rows = cursor.fetchone()[0]
                    st.write(f"### 📊 Total Rows: {total_rows:,}")

                    if total_rows > 0:
                        # Check tower IDs in data
                        cursor.execute(
                            "SELECT DISTINCT lte_hour_me_name FROM tbl_newltehourly LIMIT 10"
                        )
                        towers_in_data = [row[0] for row in cursor.fetchall()]
                        st.write(f"### 🏗️ Sample Tower IDs in data: {towers_in_data}")

                        # Check date range
                        cursor.execute(
                            "SELECT MIN(lte_hour_begin_time), MAX(lte_hour_begin_time) FROM tbl_newltehourly"
                        )
                        min_date, max_date = cursor.fetchone()
                        st.write(f"### 📅 Date Range: {min_date} to {max_date}")

                else:
                    st.error("❌ Table 'tbl_newltehourly' does not exist")

                conn.close()

            except Exception as e:
                st.error(f"❌ Database error: {e}")

        except Exception as e:
            st.error(f"❌ Test failed: {e}")

    def _render_lte_hourly_summary(self, df: pl.DataFrame):
        """Render LTE Hourly summary metrics"""
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_records = len(df)
            st.metric("Total Records", f"{total_records:,}")

        with col2:
            unique_sectors = df.select(pl.col("sector").n_unique()).item()
            st.metric("Unique Sectors", unique_sectors)

        with col3:
            unique_bands = df.select(pl.col("band").n_unique()).item()
            st.metric("Unique Bands", unique_bands)

        with col4:
            if "lte_hour_total_traffic_gb" in df.columns:
                total_traffic = df.select(
                    pl.col("lte_hour_total_traffic_gb").sum()
                ).item()
                if total_traffic:
                    st.metric("Total Traffic", f"{total_traffic:.2f} GB")
                else:
                    st.metric("Total Traffic", "N/A")
            else:
                st.metric("Total Traffic", "N/A")

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
                    f"📅 Time Range: {time_info['start'].strftime('%Y-%m-%d %H:%M')} "
                    f"to {time_info['end'].strftime('%Y-%m-%d %H:%M')} "
                    f"({time_range.days} days, {total_hours} hours)"
                )

        # Sector and Band breakdown
        with st.expander("📊 Sector & Band Breakdown", expanded=False):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Sectors:**")
                sector_counts = (
                    df.group_by("sector").agg(pl.count().alias("count")).sort("sector")
                )
                st.dataframe(
                    sector_counts.to_pandas(), width="stretch", hide_index=True
                )

            with col2:
                st.markdown("**Bands:**")
                band_counts = (
                    df.group_by("band").agg(pl.count().alias("count")).sort("band")
                )
                st.dataframe(band_counts.to_pandas(), width="stretch", hide_index=True)

    def _render_data_tables_tab(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render Data Tables and Configuration tab"""
        st.header("📋 Data Tables & Configuration")

        # Create sub-tabs for different data types
        subtab1, subtab2, subtab3, subtab4 = st.tabs(
            ["WD+TA Data", "BH+TA Data", "Configuration", "2G Network"]
        )

        with subtab1:
            self._render_wd_ta_table(tower_ids, start_date, end_date)

        with subtab2:
            self._render_bh_ta_table(tower_ids, start_date, end_date)

        with subtab3:
            self._render_configuration_tables(tower_ids)

        with subtab4:
            self._render_2g_table(tower_ids, start_date, end_date)

    def _render_wd_ta_table(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render Joined WD+TA data table"""
        st.subheader("🔗 Joined Weekday + TA Data")

        df = self._data_service.get_joined_wd_ta(tower_ids, start_date, end_date)

        if df.is_empty():
            st.warning("⚠️ No joined WD+TA data available")
            return

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Records", f"{len(df):,}")
        with col2:
            ta_joins = df.select(pl.col("newta_sector").is_not_null().sum()).item()
            st.metric("TA Joins", f"{ta_joins:,}")

        st.info(
            "👇 Table: WD data joined with TA sector info (joined on moentity=eutrancell)"
        )
        st.dataframe(df.to_pandas(), width="stretch", height=500)

    def _render_bh_ta_table(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render Joined BH+TA data table"""
        st.subheader("🔗 Joined Busy Hour + TA Data")

        df = self._data_service.get_joined_bh_ta(tower_ids, start_date, end_date)

        if df.is_empty():
            st.warning("⚠️ No joined BH+TA data available")
            return

        st.metric("Total Records", f"{len(df):,}")
        st.info(
            "👇 Table: BH data joined with TA sector info (joined on moentity=eutrancell)"
        )
        st.dataframe(df.to_pandas(), width="stretch", height=500)

    def _render_configuration_tables(self, tower_ids: List[str]):
        """Render Configuration tables (SCOT, GCELL)"""
        st.subheader("⚙️ Site Configuration")

        df_scot, df_gcell = self._data_service.get_configuration_data(tower_ids)

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

        # Combined GCELL+SCOT+TA view
        st.markdown("---")
        st.markdown("#### 🔗 Joined GCELL + SCOT + TA")

        df_joined = self._data_service.get_joined_gcell_scot_ta(tower_ids)

        if not df_joined.is_empty():
            st.success(f"✅ {len(df_joined)} joined records")
            st.info("👇 Table: GCELL + SCOT + TA (joined on moentity=eutrancell=cell)")
            st.dataframe(df_joined.to_pandas(), width="stretch", height=500)
        else:
            st.warning("⚠️ No joined configuration data available")

    def _render_2g_table(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render 2G Network data table"""
        st.subheader("📞 2G Network Data")

        df = self._data_service.get_twog_data_with_kpi(tower_ids, start_date, end_date)

        if df.is_empty():
            st.warning("⚠️ No 2G data available")
            return

        st.success(f"✅ Loaded {len(df):,} records")
        st.dataframe(df.to_pandas(), width="stretch", height=500)
