"""
Schema Viewer - Minor cleanup, no major changes needed.
"""

import streamlit as st
import sqlite3
import pandas as pd
from src.infrastructure.database.repository import DatabaseRepository


def render_schema_comparison(repo: DatabaseRepository):
    """Display schema (unchanged layout)."""
    st.subheader("📋 Schema Viewer")
    tables = repo.get_all_tables()
    if not tables:
        st.info("No tables found. Import data first.")
        return
    selected = st.selectbox(
        "Select Table",
        options=tables,
        format_func=lambda x: x.replace("tbl_", "").replace("_", " ").title(),
    )
    if selected:
        info = repo.get_table_info(selected)
        if info:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**Table:** `{selected}`")
                st.markdown(f"**Total Columns:** {len(info['columns'])}")
            with col2:
                st.metric("Rows", f"{info['row_count']:,}")
            st.markdown("#### Columns")
            with sqlite3.connect(repo.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({selected})")
                cols_info = cursor.fetchall()
            schema_df = pd.DataFrame(
                cols_info,
                columns=["Index", "Column Name", "Type", "NotNull", "Default", "PK"],
            )
            schema_df = schema_df[["Index", "Column Name", "Type"]]
            text, inte, real = (
                schema_df[schema_df["Type"] == "TEXT"],
                schema_df[schema_df["Type"] == "INTEGER"],
                schema_df[schema_df["Type"] == "REAL"],
            )
            tab1, tab2, tab3, tab4 = st.tabs(
                [
                    f"All ({len(schema_df)})",
                    f"TEXT ({len(text)})",
                    f"INTEGER ({len(inte)})",
                    f"REAL ({len(real)})",
                ]
            )
            with tab1:
                st.dataframe(schema_df, hide_index=True)
            with tab2:
                st.dataframe(text, hide_index=True) if len(text) > 0 else st.info(
                    "No TEXT columns"
                )
            with tab3:
                st.dataframe(inte, hide_index=True) if len(inte) > 0 else st.info(
                    "No INTEGER columns"
                )
            with tab4:
                st.dataframe(real, hide_index=True) if len(real) > 0 else st.info(
                    "No REAL columns"
                )
            st.markdown("#### Sample Data (First 5 Rows)")
            sample = repo.query(f"SELECT * FROM {selected} LIMIT 5")
            if sample is not None and not sample.is_empty():
                st.dataframe(sample.to_pandas(), height=200)
            else:
                st.info("No data available")


def render_column_search(repo: DatabaseRepository):
    """Column search (unchanged)."""
    st.subheader("🔍 Column Search")
    term = st.text_input(
        "Search for column name", placeholder="e.g., Cell ID, UTRAN, etc."
    )
    if term:
        results = []
        for table in repo.get_all_tables():
            info = repo.get_table_info(table)
            if info:
                matching = [
                    col for col in info["columns"] if term.lower() in col.lower()
                ]
                if matching:
                    results.append({"table": table, "columns": matching})
        if results:
            st.success(f"Found in {len(results)} table(s)")
            for res in results:
                with st.expander(f"📊 {res['table']}"):
                    for col in res["columns"]:
                        st.markdown(f"- `{col}`")
        else:
            st.warning(f"No columns found matching '{term}'")
