"""
Component to view and compare table schemas
"""

import streamlit as st
from src.infrastructure.database.repository import DatabaseRepository


def render_schema_comparison(repository: DatabaseRepository):
    """
    Display table schema information and column comparison
    """
    st.subheader("📋 Schema Viewer")

    tables = repository.get_all_tables()

    if not tables:
        st.info("No tables found. Import data first.")
        return

    # Table selector
    selected_table = st.selectbox(
        "Select Table to View Schema",
        options=tables,
        format_func=lambda x: x.replace("tbl_", "").replace("_", " ").title(),
    )

    if selected_table:
        # Get table info
        table_info = repository.get_table_info(selected_table)

        if table_info:
            col1, col2 = st.columns([3, 1])

            with col1:
                st.markdown(f"**Table:** `{selected_table}`")
                st.markdown(f"**Total Columns:** {len(table_info['columns'])}")

            with col2:
                st.metric("Rows", f"{table_info['row_count']:,}")

            # Display columns in a nice table
            st.markdown("#### Columns")

            # Get column details
            import sqlite3

            with sqlite3.connect(repository.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({selected_table})")
                columns_info = cursor.fetchall()

            # Create dataframe for display
            import pandas as pd

            schema_df = pd.DataFrame(
                columns_info,
                columns=["Index", "Column Name", "Type", "NotNull", "Default", "PK"],
            )

            # Format for display
            schema_df = schema_df[["Index", "Column Name", "Type"]]

            # Show in expandable sections by type
            text_cols = schema_df[schema_df["Type"] == "TEXT"]
            int_cols = schema_df[schema_df["Type"] == "INTEGER"]
            real_cols = schema_df[schema_df["Type"] == "REAL"]

            tab1, tab2, tab3, tab4 = st.tabs(
                [
                    f"All ({len(schema_df)})",
                    f"TEXT ({len(text_cols)})",
                    f"INTEGER ({len(int_cols)})",
                    f"REAL ({len(real_cols)})",
                ]
            )

            with tab1:
                st.dataframe(schema_df, width="stretch", hide_index=True)

            with tab2:
                if len(text_cols) > 0:
                    st.dataframe(text_cols, width="stretch", hide_index=True)
                else:
                    st.info("No TEXT columns")

            with tab3:
                if len(int_cols) > 0:
                    st.dataframe(int_cols, width="stretch", hide_index=True)
                else:
                    st.info("No INTEGER columns")

            with tab4:
                if len(real_cols) > 0:
                    st.dataframe(real_cols, width="stretch", hide_index=True)
                else:
                    st.info("No REAL columns")

            # Sample data preview
            st.markdown("#### Sample Data (First 5 Rows)")
            sample_query = f"SELECT * FROM {selected_table} LIMIT 5"
            sample_df = repository.query(sample_query)

            if sample_df is not None and not sample_df.is_empty():
                st.dataframe(sample_df.to_pandas(), width="stretch", height=200)
            else:
                st.info("No data available")


def render_column_search(repository: DatabaseRepository):
    """
    Search for columns across all tables
    """
    st.subheader("🔍 Column Search")

    search_term = st.text_input(
        "Search for column name", placeholder="e.g., Cell ID, UTRAN, etc."
    )

    if search_term:
        results = []
        tables = repository.get_all_tables()

        for table in tables:
            info = repository.get_table_info(table)
            if info:
                matching_cols = [
                    col for col in info["columns"] if search_term.lower() in col.lower()
                ]

                if matching_cols:
                    results.append({"table": table, "columns": matching_cols})

        if results:
            st.success(f"Found in {len(results)} table(s)")

            for result in results:
                with st.expander(f"📊 {result['table']}"):
                    for col in result["columns"]:
                        st.markdown(f"- `{col}`")
        else:
            st.warning(f"No columns found matching '{search_term}'")
