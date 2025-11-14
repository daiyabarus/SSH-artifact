"""
============================================================================
FILE: src/ui/sidebar.py
Sidebar UI Component
Single Responsibility: User input handling
============================================================================
"""

import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List
from src.services.tower_service import TowerService


class Sidebar:
    """
    Sidebar component for filters
    Follows Single Responsibility Principle
    """

    def __init__(self, tower_service: TowerService):
        """
        Initialize Sidebar

        Args:
            tower_service: Tower service instance
        """
        self._tower_service = tower_service

    def render(self) -> Dict:
        """
        Render sidebar and return selected filters

        Returns:
            Dict with filters: {tower_ids, start_date, end_date, run_query}
        """
        with st.sidebar:
            st.header("🔧 Filters")
            st.markdown("---")

            # Tower ID Multiselect
            tower_ids = self._render_tower_selector()

            st.markdown("---")

            # Date Range
            start_date, end_date = self._render_date_range()

            st.markdown("---")

            # Info
            # self._render_info(tower_ids, start_date, end_date)

            # st.markdown("---")

            # Run Query Button
            run_query = self._render_run_button(tower_ids)

        return {
            "tower_ids": tower_ids,
            "start_date": start_date,
            "end_date": end_date,
            "run_query": run_query,
        }

    def _render_tower_selector(self) -> List[str]:
        """Render tower ID multiselect"""
        st.subheader("📡 Tower Selection")

        # Get unique tower IDs
        with st.spinner("Loading tower IDs..."):
            tower_ids = self._tower_service.get_unique_tower_ids()

        if not tower_ids:
            st.error("❌ No tower IDs found in database")
            return []

        # Multiselect
        selected = st.multiselect(
            label="Select Tower IDs",
            options=tower_ids,
            default=None,
            help="Select one or more tower IDs to analyze",
            placeholder="Choose tower IDs...",
        )

        # Show selection count
        if selected:
            st.success(f"✅ {len(selected)} tower(s) selected")
        else:
            st.info("ℹ️ No towers selected")

        return selected

    def _render_date_range(self) -> tuple:
        """Render date range selector"""
        st.subheader("📅 Date Range")

        # Calculate defaults
        today = datetime.now().date()
        default_start = today - timedelta(days=15)
        default_end = today - timedelta(days=1)

        col1, col2 = st.columns(2)

        with col1:
            start_date = st.date_input(
                label="Start Date",
                value=default_start,
                max_value=today,
                help="Select start date for data range",
            )

        with col2:
            end_date = st.date_input(
                label="End Date",
                value=default_end,
                max_value=today,
                help="Select end date for data range",
            )

        # Validation
        if start_date > end_date:
            st.error("❌ Start date must be before end date")
            return default_start, default_end

        # Convert to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())

        return start_datetime, end_datetime

    def _render_info(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ):
        """Render information section"""
        st.subheader("ℹ️ Selection Info")

        # Calculate days
        days = (end_date - start_date).days + 1

        info_data = {
            "Towers": len(tower_ids) if tower_ids else 0,
            "Start Date": start_date.strftime("%Y-%m-%d"),
            "End Date": end_date.strftime("%Y-%m-%d"),
            "Days": days,
        }

        for key, value in info_data.items():
            st.metric(label=key, value=value)

    def _render_run_button(self, tower_ids: List[str]) -> bool:
        """
        Render RUN QUERY button

        Args:
            tower_ids: Selected tower IDs

        Returns:
            True if button clicked and validation passed
        """
        st.subheader("🚀 Execute Query")

        # Validation check
        can_run = len(tower_ids) > 0

        if not can_run:
            st.warning("⚠️ Please select at least one tower ID")

        # Button with styling
        button_clicked = st.button(
            label="▶️ RUN QUERY",
            type="primary",
            disabled=not can_run,
            width="stretch",
            help="Click to load and process data for selected filters",
        )

        if button_clicked:
            st.success("✅ Query executed!")
            # Clear any existing cache if needed
            st.cache_data.clear()

        # Show instruction when not clicked
        if not button_clicked and can_run:
            st.info("💡 Click button to start data processing")

        return button_clicked
