"""
Main entry point for Streamlit application
"""

import streamlit as st
from src.presentation.pages import config_page, dashboard_page

# Page configuration
st.set_page_config(
    page_title="Data Management Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """Main application function"""
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select Page", ["Config", "Dashboard"], index=0)

    # Route to selected page
    if page == "Config":
        config_page.render()
    elif page == "Dashboard":
        dashboard_page.render()


if __name__ == "__main__":
    main()
