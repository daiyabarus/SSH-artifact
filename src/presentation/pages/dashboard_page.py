"""
Dashboard page - Data visualization and analytics
"""

import streamlit as st
import polars as pl
from src.infrastructure.database.repository import DatabaseRepository
from src.application.services.dashboard_service import DashboardService
from src.application.services.coverage_map_service import render_coverage_map


def render():
    """Render the dashboard page"""
    st.title("📊 Dashboard")
    st.markdown("---")

    # Initialize services
    repository = DatabaseRepository()
    dashboard_service = DashboardService(repository)

    # Filters section
    st.sidebar.header("🔍 Filters")

    # Get unique Managed Element values
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

    # Managed Element filter dropdown
    selected_element = st.sidebar.selectbox(
        "Managed Element",
        options=["All"] + managed_elements,
        index=0,
        help="Filter data by Managed Element",
    )

    # Run Query button
    run_query = st.sidebar.button("▶️ Run Query", type="primary", width="stretch")

    # Display selected filter
    # st.subheader("📋 Current Selection")
    # st.metric("Managed Element", selected_element)

    st.markdown("---")

    # Execute queries when button clicked
    if run_query and selected_element != "All":
        with st.spinner("Running queries..."):
            results = dashboard_service.execute_all_queries(selected_element)
            _render_query_results(results, selected_element)
    elif run_query and selected_element == "All":
        st.warning("⚠️ Please select a specific Managed Element to run queries")
    else:
        st.info("ℹ️ Select a Managed Element and click 'Run Query' to start analysis")


def _render_query_results(results: dict, managed_element: str):
    """Render all query results in organized sections"""

    # Coverage Map Visualization - Pindah ke atas
    render_coverage_map(results)

    # Query: LTE Timing Advance Data (Augmented)
    st.subheader("🔄 LTE Timing Advance Data (Augmented)")
    df_timingadvance = results.get("timingadvance")
    if df_timingadvance is not None and not df_timingadvance.is_empty():
        st.success(f"✅ Found {len(df_timingadvance):,} records")

        # Show summary stats if relevant columns exist
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
        elif "Timing Advance" in df_timingadvance.columns or any(
            "TA" in col for col in df_timingadvance.columns
        ):
            col1, col2 = st.columns(2)
            with col1:
                ta_col = next(
                    (col for col in df_timingadvance.columns if "TA" in col), None
                )
                unique_ta = df_timingadvance[ta_col].n_unique() if ta_col else "N/A"
                st.metric("Unique TA Values", unique_ta)
            with col2:
                avg_ta = df_timingadvance[ta_col].mean() if ta_col else "N/A"
                st.metric("Avg TA Value", f"{avg_ta:.2f}" if avg_ta != "N/A" else "N/A")

        with st.expander("View LTE Timing Advance Data (Augmented)", expanded=True):
            st.dataframe(df_timingadvance.to_pandas(), width="stretch", height=400)
    else:
        st.info("No data found in tbl_timingadvance (Augmented)")

    st.markdown("---")

    # Query: LTE Timing Advance Data (Non-Augmented / Original)
    st.subheader("🔄 LTE Timing Advance Data (Non-Augmented)")
    df_timingadvance_original = results.get("timingadvance_original")
    if df_timingadvance_original is not None and not df_timingadvance_original.is_empty():
        st.success(f"✅ Found {len(df_timingadvance_original):,} records")

        # Show summary stats if relevant columns exist
        if "TA90" in df_timingadvance_original.columns:
            col1, col2 = st.columns(2)
            with col1:
                unique_ta = df_timingadvance_original["TA90"].n_unique()
                st.metric("Unique TA90 Values", unique_ta)
            with col2:
                avg_ta = df_timingadvance_original["TA90"].mean()
                st.metric(
                    "Avg TA90 Value", f"{avg_ta:.2f}" if avg_ta is not None else "N/A"
                )
        elif "Timing Advance" in df_timingadvance_original.columns or any(
            "TA" in col for col in df_timingadvance_original.columns
        ):
            col1, col2 = st.columns(2)
            with col1:
                ta_col = next(
                    (col for col in df_timingadvance_original.columns if "TA" in col), None
                )
                unique_ta = df_timingadvance_original[ta_col].n_unique() if ta_col else "N/A"
                st.metric("Unique TA Values", unique_ta)
            with col2:
                avg_ta = df_timingadvance_original[ta_col].mean() if ta_col else "N/A"
                st.metric("Avg TA Value", f"{avg_ta:.2f}" if avg_ta != "N/A" else "N/A")

        with st.expander("View LTE Timing Advance Data (Non-Augmented)", expanded=True):
            st.dataframe(df_timingadvance_original.to_pandas(), width="stretch", height=400)
    else:
        st.info("No data found in tbl_timingadvance (Non-Augmented)")

    st.markdown("---")

    # Query 1: GCell Data
    st.subheader("1️⃣ GCell Data")
    df_gcell = results.get("gcell")
    if df_gcell is not None and not df_gcell.is_empty():
        st.success(f"✅ Found {len(df_gcell):,} records")

        # Tampilkan eNodeBId untuk debugging mapping
        if "eNodeBId" in df_gcell.columns:
            enodeb_ids = df_gcell["eNodeBId"].unique().to_list()
            st.info(f"**eNodeBId for mapping:** {', '.join(map(str, enodeb_ids))}")

        with st.expander("View GCell Data"):
            st.dataframe(df_gcell.to_pandas(), width="stretch")
    else:
        st.info("No data found in tbl_gcell")

    st.markdown("---")

    # Query 2: SCOT Data
    st.subheader("2️⃣ SCOT Data")
    df_scot = results.get("scot")
    if df_scot is not None and not df_scot.is_empty():
        st.success(f"✅ Found {len(df_scot):,} records")
        with st.expander("View SCOT Data"):
            st.dataframe(df_scot.to_pandas(), width="stretch")
    else:
        st.info("No data found in tbl_scot")

    st.markdown("---")

    # Query 3: Mapping Data
    st.subheader("3️⃣ Mapping Data")
    df_mapping = results.get("mapping")
    if df_mapping is not None and not df_mapping.is_empty():
        st.success(f"✅ Found {len(df_mapping):,} records")
        with st.expander("View Mapping Data"):
            st.dataframe(df_mapping.to_pandas(), width="stretch")
    else:
        st.info("No data found in tbl_mapping")

    st.markdown("---")

    # Query 4: LTE Hourly Data (NEW eNodeBId MAPPING LOGIC)
    st.subheader("4️⃣ LTE Hourly Data (via GCell eNodeBId Mapping)")
    df_lte = results.get("ltehourly")
    if df_lte is not None and not df_lte.is_empty():
        st.success(f"✅ Found {len(df_lte):,} records")

        # Show summary stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", f"{len(df_lte):,}")
        with col2:
            st.metric("Total Columns", len(df_lte.columns))
        with col3:
            # Show unique eNodeBId jika ada
            if "eNodeBId" in df_lte.columns:
                unique_enodeb = df_lte["eNodeBId"].n_unique()
                st.metric("Unique eNodeBId", unique_enodeb)

        with st.expander("View LTE Hourly Data", expanded=True):
            st.dataframe(df_lte.to_pandas(), width="stretch", height=400)
    else:
        st.warning(f"⚠️ No data found for '{managed_element}' in tbl_ltehourly")
        # Berikan info tentang mapping
        if (
            df_gcell is not None
            and not df_gcell.is_empty()
            and "eNodeBId" in df_gcell.columns
        ):
            enodeb_ids = df_gcell["eNodeBId"].unique().to_list()
            st.info(
                f"💡 GCell has eNodeBId: {enodeb_ids}, but no matching LTE Hourly data found"
            )

    st.markdown("---")

    # Query 5: 2G Hourly Data
    st.subheader("5️⃣ 2G Hourly Data")
    df_2g = results.get("twoghourly")
    if df_2g is not None and not df_2g.is_empty():
        st.success(f"✅ Found {len(df_2g):,} records")
        with st.expander("View 2G Hourly Data"):
            st.dataframe(df_2g.to_pandas(), width="stretch", height=300)
    else:
        if df_mapping is None or df_mapping.is_empty():
            st.warning("⚠️ No mapping found, skipping 2G Hourly query")
        else:
            st.info("No data found in tbl_twoghourly")

    st.markdown("---")

    # Query 6: Merged GCell Coverage Data
    st.subheader("6️⃣ GCell Coverage (Merged Data)")
    df_coverage = results.get("gcell_coverage")
    if df_coverage is not None and not df_coverage.is_empty():
        st.success(f"✅ Found {len(df_coverage):,} merged records")

        # Show key merged columns summary
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if "TA90" in df_coverage.columns:
                avg_ta90 = df_coverage["TA90"].mean()
                st.metric(
                    "Avg TA90", f"{avg_ta90:.2f}" if avg_ta90 is not None else "N/A"
                )
        with col2:
            if "Min of S2S Distance" in df_coverage.columns:
                avg_distance = df_coverage["Min of S2S Distance"].mean()
                st.metric("Avg S2S Distance", f"{avg_distance:.2f}")
        with col3:
            st.metric("Total Columns", len(df_coverage.columns))
        with col4:
            st.metric(
                "Unique Cells",
                df_coverage["CellName"].n_unique()
                if "CellName" in df_coverage.columns
                else 0,
            )

        with st.expander("View GCell Coverage Data", expanded=True):
            st.dataframe(df_coverage.to_pandas(), width="stretch", height=400)
    else:
        st.warning(
            "⚠️ No merged GCell Coverage data available (requires GCell, Timing Advance, and SCOT data)"
        )

    # Summary section
    st.markdown("---")
    st.subheader("📈 Summary")

    summary_data = {
        "Table": [
            "LTE Timing Advance (Augmented)",
            "LTE Timing Advance (Original)",
            "GCell",
            "SCOT",
            "Mapping",
            "LTE Hourly",
            "2G Hourly",
            "GCell Coverage",
        ],
        "Records Found": [
            len(df_timingadvance)
            if df_timingadvance is not None and not df_timingadvance.is_empty()
            else 0,
            len(df_timingadvance_original)
            if df_timingadvance_original is not None and not df_timingadvance_original.is_empty()
            else 0,
            len(df_gcell) if df_gcell is not None and not df_gcell.is_empty() else 0,
            len(df_scot) if df_scot is not None and not df_scot.is_empty() else 0,
            len(df_mapping)
            if df_mapping is not None and not df_mapping.is_empty()
            else 0,
            len(df_lte) if df_lte is not None and not df_lte.is_empty() else 0,
            len(df_2g) if df_2g is not None and not df_2g.is_empty() else 0,
            len(df_coverage)
            if df_coverage is not None and not df_coverage.is_empty()
            else 0,
        ],
        "Status": [
            "✅ Found"
            if df_timingadvance is not None and not df_timingadvance.is_empty()
            else "❌ Empty",
            "✅ Found"
            if df_timingadvance_original is not None and not df_timingadvance_original.is_empty()
            else "❌ Empty",
            "✅ Found"
            if df_gcell is not None and not df_gcell.is_empty()
            else "❌ Empty",
            "✅ Found"
            if df_scot is not None and not df_scot.is_empty()
            else "❌ Empty",
            "✅ Found"
            if df_mapping is not None and not df_mapping.is_empty()
            else "❌ Empty",
            "✅ Found" if df_lte is not None and not df_lte.is_empty() else "❌ Empty",
            "✅ Found" if df_2g is not None and not df_2g.is_empty() else "❌ Empty",
            "✅ Found"
            if df_coverage is not None and not df_coverage.is_empty()
            else "❌ Empty",
        ],
    }

    import pandas as pd

    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, width="stretch", hide_index=True)