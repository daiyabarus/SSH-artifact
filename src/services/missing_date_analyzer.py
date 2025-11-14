"""
============================================================================
FILE: src/services/missing_date_analyzer.py
Missing Date Analyzer - Shows which dates are missing from expected range
============================================================================
"""

import streamlit as st
import polars as pl
from datetime import datetime, timedelta
from typing import List, Set
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MissingDateAnalyzer:
    """
    Analyzes and displays missing dates in data tables
    """

    def __init__(self):
        self.date_formats = ["%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"]

    def _parse_date_safe(self, date_str: str) -> datetime:
        """Safely parse date string with multiple format attempts"""
        if not date_str or date_str == "":
            return None

        for fmt in self.date_formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except:
                continue

        logger.warning(f"Could not parse date: {date_str}")
        return None

    def _get_expected_dates(
        self, start_date: datetime, end_date: datetime
    ) -> Set[datetime]:
        """Generate set of all expected dates in range (inclusive)"""
        expected = set()
        current = start_date

        while current <= end_date:
            expected.add(current)
            current += timedelta(days=1)

        return expected

    def _get_actual_dates(self, df: pl.DataFrame, date_col: str) -> Set[datetime]:
        """Extract actual dates from dataframe"""
        if df.is_empty() or date_col not in df.columns:
            return set()

        actual = set()

        # Get unique dates
        unique_dates = df.select(date_col).unique()[date_col].to_list()

        for date_str in unique_dates:
            parsed = self._parse_date_safe(date_str)
            if parsed:
                actual.add(parsed)

        return actual

    def _format_date_list(self, dates: List[datetime]) -> str:
        """Format list of dates as readable string"""
        if not dates:
            return "None"

        sorted_dates = sorted(dates)
        formatted = [d.strftime("%m/%d/%Y") for d in sorted_dates]

        # Group consecutive dates
        if len(formatted) > 10:
            return f"{', '.join(formatted[:5])} ... {', '.join(formatted[-5:])} ({len(formatted)} total)"
        else:
            return ", ".join(formatted)

    def analyze_missing_dates(
        self,
        df: pl.DataFrame,
        date_col: str,
        table_name: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict:
        """
        Analyze missing dates for a specific table

        Returns:
            dict with analysis results
        """
        expected_dates = self._get_expected_dates(start_date, end_date)
        actual_dates = self._get_actual_dates(df, date_col)
        missing_dates = expected_dates - actual_dates

        total_expected = len(expected_dates)
        total_actual = len(actual_dates)
        total_missing = len(missing_dates)

        coverage_pct = (
            (total_actual / total_expected * 100) if total_expected > 0 else 0
        )

        return {
            "table_name": table_name,
            "date_col": date_col,
            "total_expected": total_expected,
            "total_actual": total_actual,
            "total_missing": total_missing,
            "coverage_pct": coverage_pct,
            "missing_dates": sorted(list(missing_dates)),
            "actual_dates": sorted(list(actual_dates)),
        }

    def render_missing_dates_summary(
        self,
        df_wd: pl.DataFrame,
        df_bh: pl.DataFrame,
        df_twog: pl.DataFrame,
        start_date: datetime,
        end_date: datetime,
    ):
        """
        Render comprehensive missing dates analysis for all tables
        """
        st.markdown("---")
        st.header("📅 Date Coverage Analysis")

        st.info(
            f"**Expected Date Range:** {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')} "
            f"({(end_date - start_date).days + 1} days)"
        )

        # Analyze each table
        analyses = []

        # Weekday (WD)
        if not df_wd.is_empty():
            wd_analysis = self.analyze_missing_dates(
                df_wd, "newwd_date", "tbl_newwd (Weekday)", start_date, end_date
            )
            analyses.append(wd_analysis)

        # Busy Hour (BH)
        if not df_bh.is_empty():
            bh_analysis = self.analyze_missing_dates(
                df_bh, "newbh_date", "tbl_newbh (Busy Hour)", start_date, end_date
            )
            analyses.append(bh_analysis)

        # 2G (TWOG)
        if not df_twog.is_empty():
            twog_analysis = self.analyze_missing_dates(
                df_twog, "newtwog_date", "tbl_newtwog (2G)", start_date, end_date
            )
            analyses.append(twog_analysis)

        if not analyses:
            st.warning("No data available for analysis")
            return

        # Display summary cards
        st.subheader("📊 Coverage Summary")
        cols = st.columns(len(analyses))

        for idx, analysis in enumerate(analyses):
            with cols[idx]:
                # Determine color based on coverage
                if analysis["coverage_pct"] >= 90:
                    color = "#10b981"  # Green
                    icon = "✅"
                elif analysis["coverage_pct"] >= 70:
                    color = "#f59e0b"  # Yellow
                    icon = "⚠️"
                else:
                    color = "#ef4444"  # Red
                    icon = "❌"

                st.markdown(
                    f"""
                <div style="
                    background: white;
                    padding: 20px;
                    border-radius: 12px;
                    border-left: 4px solid {color};
                    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                ">
                    <div style="font-size: 24px; margin-bottom: 8px;">{icon}</div>
                    <div style="font-size: 14px; color: #6b7280; font-weight: 600; margin-bottom: 8px;">
                        {analysis["table_name"]}
                    </div>
                    <div style="font-size: 32px; font-weight: 700; color: {color}; margin-bottom: 8px;">
                        {analysis["coverage_pct"]:.1f}%
                    </div>
                    <div style="font-size: 12px; color: #9ca3af;">
                        {analysis["total_actual"]} of {analysis["total_expected"]} days
                    </div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

        # Detailed missing dates breakdown
        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("🔍 Missing Dates Details")

        for analysis in analyses:
            with st.expander(
                f"**{analysis['table_name']}** - {analysis['total_missing']} missing dates",
                expanded=(analysis["total_missing"] > 0),
            ):
                if analysis["total_missing"] == 0:
                    st.success("✅ All dates present in range!")
                else:
                    # Show missing dates
                    st.warning(f"⚠️ **Missing {analysis['total_missing']} dates:**")
                    missing_str = self._format_date_list(analysis["missing_dates"])
                    st.code(missing_str, language=None)

                    # Show statistics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Expected Days", analysis["total_expected"])
                    with col2:
                        st.metric("Actual Days", analysis["total_actual"])
                    with col3:
                        st.metric("Missing Days", analysis["total_missing"])

                    # Show actual dates present
                    st.info(f"**Dates Present ({analysis['total_actual']}):**")
                    actual_str = self._format_date_list(analysis["actual_dates"])
                    st.code(actual_str, language=None)

        # Data quality recommendation
        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("💡 Recommendations")

        for analysis in analyses:
            if analysis["total_missing"] > 0:
                missing_pct = (
                    analysis["total_missing"] / analysis["total_expected"] * 100
                )

                if missing_pct > 30:
                    st.error(
                        f"🚨 **{analysis['table_name']}**: High data gap ({missing_pct:.1f}% missing). "
                        f"Check data collection or ETL process."
                    )
                elif missing_pct > 10:
                    st.warning(
                        f"⚠️ **{analysis['table_name']}**: Moderate data gap ({missing_pct:.1f}% missing). "
                        f"Consider investigating missing dates."
                    )
                else:
                    st.info(
                        f"ℹ️ **{analysis['table_name']}**: Minor data gap ({missing_pct:.1f}% missing). "
                        f"Acceptable for analysis."
                    )

        # Check if all tables have same missing dates (systematic issue)
        if len(analyses) > 1:
            all_missing = [
                set(a["missing_dates"]) for a in analyses if a["total_missing"] > 0
            ]
            if all_missing:
                common_missing = (
                    set.intersection(*all_missing)
                    if len(all_missing) > 1
                    else all_missing[0]
                )
                if common_missing:
                    st.warning(
                        f"⚠️ **Common Missing Dates Across Tables**: "
                        f"{len(common_missing)} dates are missing from ALL tables. "
                        f"This suggests a systematic data collection issue."
                    )
                    common_str = self._format_date_list(sorted(list(common_missing)))
                    st.code(common_str, language=None)


# Convenience function for quick usage
def render_date_coverage_analysis(
    df_wd: pl.DataFrame,
    df_bh: pl.DataFrame,
    df_twog: pl.DataFrame,
    start_date: datetime,
    end_date: datetime,
):
    """Quick render function"""
    analyzer = MissingDateAnalyzer()
    analyzer.render_missing_dates_summary(df_wd, df_bh, df_twog, start_date, end_date)
