"""
============================================================================
FILE: app.py
Refactored with Clean Architecture, Error Handling, and streamlit-extras
============================================================================
"""

import streamlit as st
from datetime import datetime
from typing import Dict, Any, Optional
import logging
from contextlib import contextmanager

# Services
from src.config.settings import Settings
from src.services.tower_service import TowerService
from src.services.data_service import DataService

# UI Components
from src.ui.sidebar import Sidebar
from src.ui.dashboard import Dashboard

# Streamlit Extras
from streamlit_extras.app_logo import add_logo
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.colored_header import colored_header
from streamlit_extras.let_it_rain import rain

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION & INITIALIZATION
# ============================================================================


def set_page_width(width: int):
    """Set the page width for a Streamlit app with custom CSS.

    Args:
        width (int): The maximum width in pixels for the content area.
    """
    style = f"""
    <style>
    .main .block-container {{
        max-width: {width}px;
        padding-left: 1rem;
        padding-right: 1rem;
    }}
    </style>
    """
    st.markdown(style, unsafe_allow_html=True)


class AppConfig:
    """Application configuration - Single Responsibility"""

    PAGE_TITLE = "Tower Analytics Dashboard"
    PAGE_ICON = "📡"
    LAYOUT = "wide"
    SIDEBAR_STATE = "expanded"
    MAX_WIDTH = 1400

    @staticmethod
    def configure_page():
        """Configure Streamlit page settings"""
        st.set_page_config(
            page_title=AppConfig.PAGE_TITLE,
            page_icon=AppConfig.PAGE_ICON,
            layout=AppConfig.LAYOUT,
            initial_sidebar_state=AppConfig.SIDEBAR_STATE,
        )
        # Apply custom page width
        set_page_width(AppConfig.MAX_WIDTH)


class ServiceContainer:
    """
    Service Container - Dependency Injection Container
    Implements Dependency Inversion Principle
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._tower_service: Optional[TowerService] = None
        self._data_service: Optional[DataService] = None

    @property
    def tower_service(self) -> TowerService:
        """Lazy initialization of TowerService"""
        if self._tower_service is None:
            self._tower_service = TowerService(self.settings.DB_PATH)
            logger.info("✅ TowerService initialized")
        return self._tower_service

    @property
    def data_service(self) -> DataService:
        """Lazy initialization of DataService"""
        if self._data_service is None:
            self._data_service = DataService(self.settings.DB_PATH)
            logger.info("✅ DataService initialized")
        return self._data_service

    def health_check(self) -> bool:
        """Check if all services are healthy"""
        try:
            # Test database connection
            _ = self.tower_service
            _ = self.data_service
            return True
        except Exception as e:
            logger.error(f"❌ Service health check failed: {e}")
            return False


# ============================================================================
# UI COMPONENTS
# ============================================================================


class WelcomeScreen:
    """Welcome screen component - Single Responsibility"""

    @staticmethod
    def render():
        """Render welcome/empty state screen"""
        colored_header(
            label="Welcome to Tower Analytics Dashboard",
            description="Configure your analysis parameters to get started",
            color_name="blue-70",
        )

        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            st.markdown(
                """
                ### 🚀 Getting Started
                
                1. **Select Tower IDs** from the sidebar
                2. **Choose Date Range** for your analysis
                3. **Click RUN QUERY** to load data
                
                ### 📊 Available Features
                
                - 🗺️ **Coverage Map** - Visualize site locations and sectors
                - 📈 **Daily Statistics** - Weekday performance metrics
                - ⏰ **Busy Hour Stats** - Peak hour analysis
                - ⏱️ **Hourly Trends** - Granular time-series data
                - 📋 **Data Tables** - Raw data exploration
                
                ### 💡 Tips
                
                - Select multiple towers for comparison
                - Use date filters to focus on specific periods
                - Export charts and tables for reporting
                """
            )

            st.info("👈 **Start by configuring filters in the sidebar**")


class FilterSummary:
    """Filter summary display component"""

    @staticmethod
    def render(filters: Dict[str, Any]):
        """
        Render filter summary cards

        Args:
            filters: Dictionary containing filter values
        """
        colored_header(
            label="Current Selection",
            description="Review your filter configuration",
            color_name="green-70",
        )

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                label="🏗️ Selected Towers",
                value=len(filters["tower_ids"]),
                help="Number of tower IDs selected for analysis",
            )

        with col2:
            st.metric(
                label="📅 Start Date",
                value=filters["start_date"].strftime("%Y-%m-%d"),
                help="Analysis period start date",
            )

        with col3:
            st.metric(
                label="📅 End Date",
                value=filters["end_date"].strftime("%Y-%m-%d"),
                help="Analysis period end date",
            )

        style_metric_cards(
            background_color="#f0f2f6",
            border_left_color="#1f77b4",
            border_size_px=3,
        )

        # Show tower ID details in expander
        with st.expander("📋 View Selected Tower IDs", expanded=False):
            tower_list = ", ".join(filters["tower_ids"])
            st.code(tower_list, language=None)

        st.info(
            "✨ **Ready to analyze!** Click the **RUN QUERY** button in the sidebar to load data."
        )


class ErrorHandler:
    """Error handling component - Single Responsibility"""

    @staticmethod
    def show_error(title: str, message: str, exception: Optional[Exception] = None):
        """Show formatted error message"""
        st.error(f"❌ **{title}**")
        st.markdown(f"```\n{message}\n```")

        if exception:
            with st.expander("🔍 Technical Details", expanded=False):
                st.code(str(exception))

        st.info(
            "💡 **Troubleshooting**: Check database connection and ensure data is available."
        )

    @staticmethod
    @contextmanager
    def error_boundary(operation: str):
        """Context manager for error handling"""
        try:
            yield
        except Exception as e:
            logger.error(f"Error in {operation}: {e}", exc_info=True)
            ErrorHandler.show_error(
                title=f"Error in {operation}", message=str(e), exception=e
            )


# ============================================================================
# MAIN APPLICATION
# ============================================================================


class TowerAnalyticsApp:
    """
    Main application class
    Orchestrates all components using Dependency Injection
    """

    def __init__(self):
        """Initialize application"""
        self.settings = Settings()
        self.services = ServiceContainer(self.settings)

    def run(self):
        """Run the application"""
        # Configure page
        AppConfig.configure_page()

        # Check service health
        if not self.services.health_check():
            ErrorHandler.show_error(
                title="Service Initialization Failed",
                message="Could not connect to database or initialize services.",
            )
            return

        # Render sidebar and get filters
        with ErrorHandler.error_boundary("Sidebar Rendering"):
            sidebar = Sidebar(self.services.tower_service)
            filters = sidebar.render()

        # Handle different application states
        if not filters["tower_ids"]:
            self._render_no_selection_state()
        elif not filters["run_query"]:
            self._render_pre_query_state(filters)
        else:
            self._render_dashboard_state(filters)

    def _render_no_selection_state(self):
        """Render state when no towers are selected"""
        WelcomeScreen.render()

    def _render_pre_query_state(self, filters: Dict[str, Any]):
        """Render state before query execution"""
        FilterSummary.render(filters)

    def _render_dashboard_state(self, filters: Dict[str, Any]):
        """Render main dashboard with data"""
        with ErrorHandler.error_boundary("Dashboard Rendering"):
            with st.spinner("⏳ Loading and processing data..."):
                # Log query execution
                logger.info(
                    f"Executing query: Towers={len(filters['tower_ids'])}, "
                    f"Date Range={filters['start_date']} to {filters['end_date']}"
                )

                # Initialize and render dashboard
                dashboard = Dashboard(self.services.data_service)
                dashboard.render(
                    tower_ids=filters["tower_ids"],
                    start_date=filters["start_date"],
                    end_date=filters["end_date"],
                )

                # Success celebration
                self._show_success_celebration()

    def _show_success_celebration(self):
        """Show success indicator after dashboard loads"""
        # Optional: Add celebration effect
        # rain(emoji="📊", font_size=20, falling_speed=3, animation_length=1)
        pass


# ============================================================================
# APPLICATION ENTRY POINT
# ============================================================================


def main():
    """Application entry point"""
    try:
        app = TowerAnalyticsApp()
        app.run()
    except Exception as e:
        logger.critical(f"Critical application error: {e}", exc_info=True)
        st.error("💥 **Critical Application Error**")
        st.markdown(
            """
            The application encountered a critical error and cannot continue.
            
            **Possible causes:**
            - Database connection failure
            - Missing configuration files
            - Corrupted application state
            
            **Recommended actions:**
            1. Refresh the page
            2. Check database connection
            3. Contact system administrator
            """
        )

        with st.expander("🔍 Error Details", expanded=False):
            st.code(str(e))


if __name__ == "__main__":
    main()
