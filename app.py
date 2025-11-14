"""
============================================================================
FILE: app.py
============================================================================
"""

import streamlit as st
from datetime import datetime, timedelta
from src.config.settings import Settings
from src.services.tower_service import TowerService
from src.services.data_service import DataService
from src.ui.sidebar import Sidebar
from src.ui.dashboard import Dashboard

# Page Configuration
st.set_page_config(
    page_title="Dashboard",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """Main application entry point"""

    # Initialize settings
    settings = Settings()

    # Initialize services
    tower_service = TowerService(settings.DB_PATH)
    data_service = DataService(settings.DB_PATH)

    # Title
    # st.title("📡 Tower Analytics Dashboard")
    # st.markdown("---")

    # Sidebar
    sidebar = Sidebar(tower_service)
    filters = sidebar.render()

    # Check if tower IDs selected
    if not filters["tower_ids"]:
        st.info(
            "👈 Please select at least one Tower ID from the sidebar and click RUN QUERY"
        )
        return

    # Check if run query button clicked
    if not filters["run_query"]:
        st.info(
            "👈 Configure your filters and click the **RUN QUERY** button to load data"
        )

        # Show current selections
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Selected Towers", len(filters["tower_ids"]))
        with col2:
            st.metric("Start Date", filters["start_date"].strftime("%Y-%m-%d"))
        with col3:
            st.metric("End Date", filters["end_date"].strftime("%Y-%m-%d"))

        return

    # Main Dashboard - Only loads when button clicked
    with st.spinner("⏳ Loading and processing data..."):
        dashboard = Dashboard(data_service)
        dashboard.render(
            tower_ids=filters["tower_ids"],
            start_date=filters["start_date"],
            end_date=filters["end_date"],
        )


if __name__ == "__main__":
    main()
