"""
Dashboard page - Data visualization and analytics
Clean and effective version with Date Range filter
"""

import streamlit as st
import polars as pl
import pandas as pd
from datetime import datetime, timedelta
from src.infrastructure.database.repository import DatabaseRepository
from src.application.services.dashboard_service import DashboardService
from src.application.services.coverage_map_service import render_coverage_map
from src.application.services.ta_distribution_visualizer import TADistributionVisualizer


def render():
    """Render the dashboard page"""
    repository = DatabaseRepository()
    dashboard_service = DashboardService(repository)

    st.sidebar.header("🔍 Filters")
    try:
        managed_elements = dashboard_service.get_managed_elements()
    except Exception as e:
        st.error(f"Error loading filters: {str(e)}")
        return

    if not managed_elements:
        st.warning(
            "⚠️ No data available in Timing Advance table. Please import data first."
        )
        return

    # Filter 1: Select TOWERID
    selected_element = st.sidebar.selectbox(
        "TOWERID",
        options=["All"] + managed_elements,
        index=0,
        help="Filter data by Managed Element",
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("📅 Date Range for Hourly Data")

    col1, col2 = st.sidebar.columns(2)

    with col1:
        # Default: 7 days ago
        default_start = datetime.now() - timedelta(days=7)
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            help="Start date for LTE and 2G Hourly data",
        )

    with col2:
        # Default: today
        default_end = datetime.now()
        end_date = st.date_input(
            "End Date", value=default_end, help="End date for LTE and 2G Hourly data"
        )

    # Validate date range
    if start_date > end_date:
        st.sidebar.error("❌ Start date must be before end date")
        return

    # Convert date to datetime
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())

    # Show selected date range
    date_range_days = (end_date - start_date).days + 1
    st.sidebar.info(f"📊 Selected range: **{date_range_days} days**")

    run_query = st.sidebar.button("▶️ Run Query", type="primary")

    if run_query and selected_element != "All":
        with st.spinner("Running queries..."):
            results = dashboard_service.execute_all_queries(
                selected_element, start_date=start_datetime, end_date=end_datetime
            )
            _render_query_results(
                results, selected_element, start_datetime, end_datetime
            )
    elif run_query:
        st.warning("⚠️ Please select a specific Managed Element to run queries")
    else:
        st.info(
            "ℹ️ Select a Managed Element and date range, then click 'Run Query' to start analysis"
        )


def _get_scot_non_augmented(results: dict, managed_element: str):
    """
    Filter SCOT data untuk mendapatkan hanya data NON-AUGMENTED
    (hanya data yang SiteID = managed_element)
    """
    df_scot = results.get("scot")

    if df_scot is None or df_scot.is_empty():
        return None

    df_non_augmented = df_scot.filter(pl.col("SiteID") == managed_element)

    if df_non_augmented.is_empty():
        return None

    target_columns = [
        "SiteID",
        "Sectorid_v2",
        "TAPC90",
        "Cell_PI-1",
        "NCELL SiteID",
        "Min of S2S Distance",
        "FINAL Remark COSTv2.0T",
        "Additional Tilt Recommendation",
    ]

    available_columns = [
        col for col in target_columns if col in df_non_augmented.columns
    ]

    if not available_columns:
        return None

    if "Sectorid_v2" in available_columns:
        df_sorted = df_non_augmented.select(available_columns).sort(
            ["Sectorid_v2", "Cell_PI-1"]
        )
    else:
        df_sorted = df_non_augmented.select(available_columns)

    return df_sorted


def _render_ta_distribution_section(results: dict, managed_element: str):
    """Render TA Distribution visualization section"""
    df_timingadvance = results.get("timingadvance_original")

    if df_timingadvance is None or df_timingadvance.is_empty():
        st.warning("⚠️ No Timing Advance data available for TA Distribution analysis")
        return

    distance_cols = [
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

    cdf_cols = [
        "78",
        "234",
        "390",
        "546",
        "702",
        "858",
        "1014",
        "1560",
        "2106",
        "2652",
        "3120",
        "3900",
        "6318",
        "10062",
        "13962",
        "20000",
    ]

    required_cols = ["Sector_Name", "Band"] + distance_cols + cdf_cols
    missing_cols = [col for col in required_cols if col not in df_timingadvance.columns]

    if missing_cols:
        st.error(f"❌ Missing required columns: {missing_cols[:5]}...")
        return

    visualizer = TADistributionVisualizer()
    visualizer.display_sector_charts_in_rows(df_timingadvance, managed_element)


def _styled_dataframe(df, height: int = 400):
    """Render dataframe dengan styling yang konsisten"""
    st.markdown(
        """
        <style>
        .stDataFrame table {
            font-size: 12px !important;
        }
        .stDataFrame thead th {
            background-color: #1f77b4 !important;
            color: white !important;
            font-weight: bold !important;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )

    st.dataframe(df, height=height, width="stretch", hide_index=True)


def _render_data_section(
    title: str,
    results_key: str,
    results: dict,
    expanded: bool = False,
    height: int = 400,
    show_metrics: bool = False,
    start_date: datetime = None,
    end_date: datetime = None,
):
    """Render a standardized data section"""
    df = results.get(results_key)

    if df is None or df.is_empty():
        st.info(f"No data found in {results_key}")
        return

    # Show date range info for hourly data
    if results_key in ["ltehourly", "twoghourly"] and start_date and end_date:
        st.success(
            f"✅ Found {len(df):,} records from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )
    else:
        st.success(f"✅ Found {len(df):,} records")

    # Show eNodeBId info for Timing Advance
    if results_key == "timingadvance" and "eNodeBId" in df.columns:
        enodeb_ids = df["eNodeBId"].unique().to_list()
        st.info(
            f"**eNodeBId for LTE Hourly mapping:** {', '.join(map(str, enodeb_ids[:5]))}{'...' if len(enodeb_ids) > 5 else ''}"
        )

    with st.expander(f"View {title}", expanded=expanded):
        _styled_dataframe(df.to_pandas(), height)


def _render_metrics_section(results: dict):
    """Render key metrics for data sections"""
    df_timingadvance = results.get("timingadvance")
    df_coverage = results.get("gcell_coverage")

    if df_timingadvance is not None and not df_timingadvance.is_empty():
        if "TA90" in df_timingadvance.columns:
            col1, col2 = st.columns(2)
            with col1:
                unique_ta = df_timingadvance["TA90"].n_unique()
                st.metric("Unique TA90 Values", unique_ta)
            with col2:
                avg_ta = df_timingadvance["TA90"].mean()
                st.metric(
                    "Avg TA90 Value", f"{avg_ta:.2f}" if avg_ta is not None else "N/A"
                )

    if df_coverage is not None and not df_coverage.is_empty():
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if "TA90" in df_coverage.columns:
                avg_ta90 = df_coverage["TA90"].mean()
                st.metric(
                    "Avg TA90", f"{avg_ta90:.2f}" if avg_ta90 is not None else "N/A"
                )
        with col2:
            st.markdown("")
            # if "Min of S2S Distance" in df_coverage.columns:
            # avg_distance = df_coverage["Min of S2S Distance"].mean()
            # st.metric("Avg S2S Distance", f"{avg_distance:.2f}")
        with col3:
            st.metric("Total Columns", len(df_coverage.columns))
        with col4:
            unique_cells = (
                df_coverage["CellName"].n_unique()
                if "CellName" in df_coverage.columns
                else 0
            )
            st.metric("Unique Cells", unique_cells)


def _styled_table(df):
    """Render table dengan styling yang konsisten untuk SCOT data"""
    st.markdown(
        """
        <style>
        .small-table table {
            font-size: 12px !important;
            width: 100% !important;
        }
        .small-table thead th {
            background-color: #1f77b4 !important;
            color: white !important;
            font-weight: bold !important;
            font-size: 11px !important;
            padding: 8px 4px !important;
        }
        .small-table tbody td {
            padding: 6px 4px !important;
            font-size: 11px !important;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )

    pandas_df = df.to_pandas()

    st.dataframe(
        pandas_df,
        width="stretch",
        hide_index=True,
        height=min(400, 35 * len(pandas_df) + 38),
    )


def _render_summary_section(results: dict, start_date: datetime, end_date: datetime):
    """Render summary section dengan styling"""
    tables = {
        "LTE Timing Advance (Augmented)": "timingadvance",
        "LTE Timing Advance (Original)": "timingadvance_original",
        "GCell": "gcell",
        "SCOT": "scot",
        "Mapping": "mapping",
        "LTE Hourly": "ltehourly",
        "LTE Hourly Combined": "ltehourly_combined",
        "2G Hourly": "twoghourly",
        "GCell Coverage": "gcell_coverage",
    }

    summary_data = []
    for table_name, results_key in tables.items():
        df = results.get(results_key)
        record_count = len(df) if df is not None and not df.is_empty() else 0
        status = "✅ Found" if record_count > 0 else "❌ Empty"

        # Add date range info for hourly data
        if (
            results_key in ["ltehourly", "ltehourly_combined", "twoghourly"]
            and record_count > 0
        ):
            date_info = f" ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})"
            table_name += date_info

        summary_data.append(
            {"Table": table_name, "Records Found": record_count, "Status": status}
        )

    summary_df = pd.DataFrame(summary_data)
    st.markdown(
        """
        <style>
        .summary-table table {
            font-size: 8px !important;
        }
        .summary-table thead th {
            background-color: #2E86AB !important;
            color: white !important;
            font-weight: bold !important;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )

    st.dataframe(summary_df, width="stretch", hide_index=True)


def _render_query_results(
    results: dict, managed_element: str, start_date: datetime, end_date: datetime
):
    """Render all query results in organized sections"""
    st.markdown(
        """
        <style>
        /* Global table styling */
        .stDataFrame table {
            font-size: 8px !important;
        }
        .stDataFrame thead th {
            background-color: #1f77b4 !important;
            color: white !important;
            font-weight: bold !important;
            font-size: 8px !important;
        }
        .stDataFrame tbody td {
            font-size: 8px !important;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("### 🌎 Map Overview")
        render_coverage_map(results)
        st.markdown("##### 📋 SCOT Data")
        df_scot_non_augmented = _get_scot_non_augmented(results, managed_element)
        if df_scot_non_augmented is not None:
            _styled_table(df_scot_non_augmented)
        else:
            st.info("OPEN AREA")
        st.markdown("")
        st.markdown("### Actions Taken:")
        (col1,) = st.columns(1)  # Tuple unpacking untuk mendapatkan satu kolom

        with col1:
            container = st.container(border=True)
            with container:
                st.markdown("")
                st.markdown("")
                st.markdown("")
                st.markdown("")
                st.markdown("")
                st.markdown("")
                st.markdown("")
                st.markdown("")

    with col2:
        st.markdown("### 📍 TA Distributions")
        _render_ta_distribution_section(results, managed_element)

    # Data sections
    sections = [
        ("🔄 LTE Timing Advance Data (Augmented)", "timingadvance", False),
        ("🔄 LTE Timing Advance Data (Non-Augmented)", "timingadvance_original", False),
        ("1️⃣ GCell Data", "gcell", False),
        ("2️⃣ SCOT Data (Full)", "scot", False),  # Full SCOT data (augmented)
        ("3️⃣ Mapping Data", "mapping", False),
        ("4️⃣ LTE Hourly Data", "ltehourly", True),
        ("4️⃣b LTE Hourly Combined (with TA)", "ltehourly_combined", True),
        ("5️⃣ 2G Hourly Data", "twoghourly", False),
        ("6️⃣ GCell Coverage Data", "gcell_coverage", True),
    ]

    for title, key, expanded in sections:
        st.markdown("---")
        st.subheader(title)
        _render_data_section(
            title, key, results, expanded, start_date=start_date, end_date=end_date
        )

    # Metrics section
    st.markdown("---")
    st.subheader("📊 Key Metrics")
    _render_metrics_section(results)

    # Summary section
    st.markdown("---")
    st.subheader("📈 Summary")
    _render_summary_section(results, start_date, end_date)
