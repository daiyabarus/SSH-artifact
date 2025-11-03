"""
Configuration page for CSV import
"""

import streamlit as st
from pathlib import Path
from src.infrastructure.database.repository import DatabaseRepository
from src.application.use_cases.import_csv_use_case import ImportCSVUseCase
from src.presentation.components.schema_viewer import (
    render_schema_comparison,
    render_column_search,
)


def render():
    """Render the configuration page"""
    st.title("📁 Data Import Configuration")
    st.markdown("---")

    # Initialize use case (Dependency Injection)
    repository = DatabaseRepository()
    import_use_case = ImportCSVUseCase(repository)

    # Get all table configurations
    table_configs = import_use_case.get_all_table_configs()

    # Display import interface
    st.subheader("Import CSV Files")

    # Create two columns for better layout
    col1, col2 = st.columns([2, 1])

    with col1:
        # Table selection
        table_options = {
            config["display_name"]: table_name
            for table_name, config in table_configs.items()
        }

        selected_display_name = st.selectbox(
            "Select Table",
            options=list(table_options.keys()),
            help="Choose the target table for import",
        )

        selected_table = table_options[selected_display_name]
        table_config = import_use_case.get_table_config(selected_table)

        # Show import type info
        import_type = table_config.get("import_type", "append")
        use_header = table_config.get("use_header", True)

        col_info1, col_info2 = st.columns(2)

        with col_info1:
            if import_type == "append":
                st.info(
                    "📝 **Import Type:** Append - New data will be added to existing records"
                )
            else:
                st.warning(
                    "🔄 **Import Type:** Replace - Existing data will be cleared before import"
                )

        with col_info2:
            if use_header:
                st.success("📋 **Header Mode:** Using column names from CSV header")
            else:
                st.warning(
                    "🔢 **Position Mode:** Ignoring headers, using column positions (from row 2)"
                )

        # File uploader
        uploaded_file = st.file_uploader(
            "Choose CSV file", type=["csv"], help="Upload a CSV file to import"
        )

        # Preview uploaded file
        if uploaded_file is not None:
            st.markdown("#### 📋 Data Preview")

            # Save to temp file for preview
            temp_preview_path = f"preview_{uploaded_file.name}"
            with open(temp_preview_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            try:
                # Read and preview with Polars
                import polars as pl

                df_preview = pl.read_csv(
                    temp_preview_path,
                    infer_schema_length=0,
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                    n_rows=5,  # Only first 5 rows for preview
                )

                # Clean for preview
                for col in df_preview.columns:
                    if col.lower() not in ["id", "imported_at"]:
                        try:
                            df_preview = df_preview.with_columns(
                                [
                                    pl.col(col)
                                    .str.replace_all(",", "")
                                    .str.strip_chars()
                                    .cast(pl.Float64, strict=False)
                                    .alias(col)
                                ]
                            )
                        except:
                            pass

                st.dataframe(df_preview.to_pandas(), width="stretch")
                st.caption(
                    f"Showing first 5 rows. Total rows in file: {len(pl.read_csv(temp_preview_path, infer_schema_length=0))}"
                )

            except Exception as e:
                st.warning(f"Could not preview file: {str(e)}")
            finally:
                # Clean up preview file
                Path(temp_preview_path).unlink(missing_ok=True)

        # Import button
        if st.button("🚀 Import Data", type="primary", width="stretch"):
            if uploaded_file is not None:
                with st.spinner("Importing data..."):
                    # Save uploaded file temporarily
                    temp_path = f"temp_{uploaded_file.name}"
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                    # Execute import
                    success, message = import_use_case.execute(
                        csv_path=temp_path, table_name=selected_table
                    )

                    # Clean up temp file
                    Path(temp_path).unlink(missing_ok=True)

                    # Show result
                    if success:
                        st.success(f"✅ {message}")
                    else:
                        st.error(f"❌ {message}")
            else:
                st.warning("⚠️ Please upload a CSV file first")

    with col2:
        # Display table information
        st.subheader("Table Info")
        table_info = repository.get_table_info(selected_table)

        if table_info:
            st.metric("Total Rows", table_info["row_count"])

            with st.expander("View Columns"):
                for col in table_info["columns"]:
                    if col not in ["id", "imported_at"]:
                        st.text(f"• {col}")
        else:
            st.info("No data yet")

    # Show all tables status
    st.markdown("---")
    st.subheader("📊 Database Status")

    # Create metrics for each table
    cols = st.columns(5)
    for idx, (table_name, config) in enumerate(table_configs.items()):
        with cols[idx]:
            info = repository.get_table_info(table_name)
            row_count = info["row_count"] if info else 0

            # Color coding based on data availability
            if row_count > 0:
                st.metric(
                    label=config["display_name"],
                    value=f"{row_count:,} rows",
                    delta="Active" if row_count > 0 else None,
                )
            else:
                st.metric(label=config["display_name"], value="No data")

    # Additional info section
    st.markdown("---")
    with st.expander("ℹ️ Import Guidelines & Schema Flexibility"):
        st.markdown("""
        **Data Format Notes:**
        - ✅ Thousand separator: Comma (,) - will be automatically removed
        - ✅ Decimal separator: Dot (.) - supported
        - ✅ Example: `1,660.50` will be converted to `1660.50`
        
        **Import Types:**
        - 📝 **Append**: New data added to existing records (2G Hourly, LTE Hourly)
          - Missing columns in CSV → filled with NULL
          - New columns in CSV → automatically added to table
        - 🔄 **Replace**: All existing data cleared before import (SCOT, GCell, Timing Advance)
          - Table schema recreated from CSV structure
        
        **Header Modes:**
        - 📋 **Header Mode** (SCOT, GCell, Timing Advance):
          - Uses column names from CSV header for mapping
          - Flexible: handles different column names
          - Good for reference tables with stable structure
        
        - 🔢 **Position Mode** (2G Hourly, LTE Hourly):
          - **Ignores CSV header row completely**
          - **Imports data starting from row 2**
          - Uses column position (1st column → 1st DB column, etc.)
          - Consistent even if header names change
          - Perfect for daily KPI reports with varying headers
        
        **Dynamic Schema Support:**
        - 🔀 Handles different column names (e.g., "E-UTRAN Cell ID" vs "E-UTRAN FDD Cell ID")
        - 🆕 Auto-adds new columns when appending
        - 📊 Flexible schema for FDD/TDD data variations
        
        **Tips:**
        - Check data preview before importing
        - Large files may take a few moments to process
        - Ensure CSV encoding is UTF-8 for best results
        - Column order doesn't matter - matched by name
        """)

    # Schema viewer section
    st.markdown("---")

    # Use tabs for better organization
    tab1, tab2 = st.tabs(["📋 Schema Viewer", "🔍 Column Search"])

    with tab1:
        render_schema_comparison(repository)

    with tab2:
        render_column_search(repository)
