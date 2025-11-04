"""
Example usage untuk LTE Hourly Combined data
Contoh-contoh analisa yang bisa dilakukan dengan data combined
"""

import polars as pl
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional

class LTEHourlyCombinedAnalyzer:
    """
    Analyzer class untuk data LTE Hourly Combined
    Provides various analysis methods
    """
    
    def __init__(self, df_combined: pl.DataFrame):
        """
        Initialize analyzer dengan combined dataframe
        
        Args:
            df_combined: DataFrame hasil dari get_combined_ltehourly()
        """
        self.df = df_combined
    
    def get_sector_statistics(self) -> pl.DataFrame:
        """
        Get aggregate statistics per sector
        
        Returns:
            DataFrame dengan metrics per sector
        """
        if "Sector_Name" not in self.df.columns:
            print("ERROR: Sector_Name column not found")
            return None
        
        stats = self.df.groupby("Sector_Name").agg([
            # Traffic metrics
            pl.col("ZTE-SQM_Total_Traffic_GB").sum().alias("Total_Traffic_GB"),
            pl.col("ZTE-SQM_Total_Traffic_GB").mean().alias("Avg_Traffic_GB"),
            
            # VoLTE metrics
            pl.col("ZTE-SQM_Volte_Traffic_Erl").sum().alias("Total_VoLTE_Erl"),
            pl.col("ZTE-SQM_Volte_Traffic_Erl").mean().alias("Avg_VoLTE_Erl"),
            
            # Throughput metrics
            pl.col("ZTE-SQM_User_DL_Thp_Mbps_Num").mean().alias("Avg_User_DL_Throughput"),
            pl.col("ZTE-SQM_Cell_DL_Thp_Mbps_Num").mean().alias("Avg_Cell_DL_Throughput"),
            
            # Count
            pl.count().alias("Record_Count"),
            pl.col("E-UTRAN Cell Name").n_unique().alias("Unique_Cells")
        ]).sort("Total_Traffic_GB", descending=True)
        
        return stats
    
    def get_frequency_band_statistics(self) -> pl.DataFrame:
        """
        Get aggregate statistics per frequency band
        
        Returns:
            DataFrame dengan metrics per frequency band
        """
        if "FrequencyBand" not in self.df.columns:
            print("ERROR: FrequencyBand column not found")
            return None
        
        stats = self.df.groupby("FrequencyBand").agg([
            # Traffic metrics
            pl.col("ZTE-SQM_Total_Traffic_GB").sum().alias("Total_Traffic_GB"),
            pl.col("ZTE-SQM_Total_Traffic_GB").mean().alias("Avg_Traffic_GB"),
            
            # Throughput metrics
            pl.col("ZTE-SQM_User_DL_Thp_Mbps_Num").mean().alias("Avg_User_DL_Throughput"),
            pl.col("ZTE-SQM_Cell_DL_Thp_Mbps_Num").mean().alias("Avg_Cell_DL_Throughput"),
            
            # Count
            pl.count().alias("Record_Count"),
            pl.col("E-UTRAN Cell Name").n_unique().alias("Unique_Cells"),
            pl.col("Sector_Name").n_unique().alias("Unique_Sectors")
        ]).sort("Total_Traffic_GB", descending=True)
        
        return stats
    
    def get_sector_band_matrix(self) -> pl.DataFrame:
        """
        Get traffic distribution matrix: Sector x Band
        
        Returns:
            DataFrame dengan traffic per sector per band
        """
        if "Sector_Name" not in self.df.columns or "FrequencyBand" not in self.df.columns:
            print("ERROR: Required columns not found")
            return None
        
        matrix = self.df.groupby(["Sector_Name", "FrequencyBand"]).agg([
            pl.col("ZTE-SQM_Total_Traffic_GB").sum().alias("Total_Traffic_GB"),
            pl.count().alias("Record_Count")
        ]).sort(["Sector_Name", "FrequencyBand"])
        
        return matrix
    
    def get_time_series_by_sector(self, sector_name: str = None) -> pl.DataFrame:
        """
        Get time series data untuk specific sector atau all sectors
        
        Args:
            sector_name: Specific sector name (None untuk semua)
            
        Returns:
            DataFrame dengan time series data
        """
        if "Begin Time" not in self.df.columns:
            print("ERROR: Begin Time column not found")
            return None
        
        df_filtered = self.df
        
        if sector_name:
            if "Sector_Name" not in self.df.columns:
                print("ERROR: Sector_Name column not found")
                return None
            df_filtered = self.df.filter(pl.col("Sector_Name") == sector_name)
        
        # Group by time
        time_series = df_filtered.groupby("Begin Time").agg([
            pl.col("ZTE-SQM_Total_Traffic_GB").sum().alias("Total_Traffic_GB"),
            pl.col("ZTE-SQM_User_DL_Thp_Mbps_Num").mean().alias("Avg_DL_Throughput"),
            pl.col("Sector_Name").n_unique().alias("Active_Sectors") if "Sector_Name" in df_filtered.columns else pl.lit(0).alias("Active_Sectors")
        ]).sort("Begin Time")
        
        return time_series
    
    def get_top_cells_by_traffic(self, top_n: int = 10) -> pl.DataFrame:
        """
        Get top N cells by total traffic
        
        Args:
            top_n: Number of top cells to return
            
        Returns:
            DataFrame dengan top cells
        """
        top_cells = self.df.groupby("E-UTRAN Cell Name").agg([
            pl.col("ZTE-SQM_Total_Traffic_GB").sum().alias("Total_Traffic_GB"),
            pl.col("Sector_Name").first().alias("Sector_Name") if "Sector_Name" in self.df.columns else pl.lit(None).alias("Sector_Name"),
            pl.col("FrequencyBand").first().alias("FrequencyBand") if "FrequencyBand" in self.df.columns else pl.lit(None).alias("FrequencyBand"),
            pl.col("Managed Element").first().alias("Managed_Element") if "Managed Element" in self.df.columns else pl.lit(None).alias("Managed_Element"),
            pl.count().alias("Record_Count")
        ]).sort("Total_Traffic_GB", descending=True).head(top_n)
        
        return top_cells
    
    def get_data_quality_report(self) -> dict:
        """
        Get data quality report untuk combined data
        
        Returns:
            Dictionary dengan quality metrics
        """
        report = {
            "total_records": len(self.df),
            "date_range": {
                "start": self.df["Begin Time"].min() if "Begin Time" in self.df.columns else None,
                "end": self.df["Begin Time"].max() if "Begin Time" in self.df.columns else None
            },
            "unique_cells": self.df["E-UTRAN Cell Name"].n_unique() if "E-UTRAN Cell Name" in self.df.columns else 0,
            "join_quality": {}
        }
        
        # Check join quality
        if "Sector_Name" in self.df.columns:
            matched = self.df.filter(pl.col("Sector_Name").is_not_null()).height
            report["join_quality"]["sector_match_rate"] = f"{(matched / len(self.df)) * 100:.1f}%"
            report["join_quality"]["sector_nulls"] = len(self.df) - matched
        
        if "FrequencyBand" in self.df.columns:
            matched = self.df.filter(pl.col("FrequencyBand").is_not_null()).height
            report["join_quality"]["band_match_rate"] = f"{(matched / len(self.df)) * 100:.1f}%"
            report["join_quality"]["band_nulls"] = len(self.df) - matched
        
        if "Managed Element" in self.df.columns:
            matched = self.df.filter(pl.col("Managed Element").is_not_null()).height
            report["join_quality"]["managed_element_match_rate"] = f"{(matched / len(self.df)) * 100:.1f}%"
            report["join_quality"]["managed_element_nulls"] = len(self.df) - matched
        
        return report
    
    def get_unmatched_cells(self) -> pl.DataFrame:
        """
        Get cells yang tidak ter-match dengan Timing Advance
        
        Returns:
            DataFrame dengan unmatched cells
        """
        if "Sector_Name" not in self.df.columns:
            print("ERROR: Sector_Name column not found")
            return None
        
        unmatched = self.df.filter(
            pl.col("Sector_Name").is_null()
        ).select([
            "E-UTRAN Cell Name",
            "eNodeBId",
            "Begin Time"
        ]).unique(subset=["E-UTRAN Cell Name"])
        
        return unmatched
    
    def plot_sector_traffic_pie(self) -> go.Figure:
        """
        Create pie chart untuk traffic distribution per sector
        
        Returns:
            Plotly figure
        """
        if "Sector_Name" not in self.df.columns:
            print("ERROR: Sector_Name column not found")
            return None
        
        sector_stats = self.get_sector_statistics()
        
        # Filter out nulls
        sector_stats = sector_stats.filter(pl.col("Sector_Name").is_not_null())
        
        fig = px.pie(
            sector_stats.to_pandas(),
            values="Total_Traffic_GB",
            names="Sector_Name",
            title="Traffic Distribution by Sector"
        )
        
        return fig
    
    def plot_band_traffic_bar(self) -> go.Figure:
        """
        Create bar chart untuk traffic per frequency band
        
        Returns:
            Plotly figure
        """
        if "FrequencyBand" not in self.df.columns:
            print("ERROR: FrequencyBand column not found")
            return None
        
        band_stats = self.get_frequency_band_statistics()
        
        # Filter out nulls
        band_stats = band_stats.filter(pl.col("FrequencyBand").is_not_null())
        
        fig = px.bar(
            band_stats.to_pandas(),
            x="FrequencyBand",
            y="Total_Traffic_GB",
            title="Traffic by Frequency Band",
            labels={"Total_Traffic_GB": "Total Traffic (GB)", "FrequencyBand": "Frequency Band"}
        )
        
        return fig
    
    def plot_time_series_traffic(self, by_sector: bool = False) -> go.Figure:
        """
        Create time series plot untuk traffic
        
        Args:
            by_sector: Group by sector atau aggregate semua
            
        Returns:
            Plotly figure
        """
        if "Begin Time" not in self.df.columns:
            print("ERROR: Begin Time column not found")
            return None
        
        if by_sector and "Sector_Name" in self.df.columns:
            # Group by time and sector
            ts_data = self.df.groupby(["Begin Time", "Sector_Name"]).agg([
                pl.col("ZTE-SQM_Total_Traffic_GB").sum().alias("Total_Traffic_GB")
            ]).sort("Begin Time")
            
            fig = px.line(
                ts_data.to_pandas(),
                x="Begin Time",
                y="Total_Traffic_GB",
                color="Sector_Name",
                title="Traffic Time Series by Sector"
            )
        else:
            # Aggregate all
            ts_data = self.df.groupby("Begin Time").agg([
                pl.col("ZTE-SQM_Total_Traffic_GB").sum().alias("Total_Traffic_GB")
            ]).sort("Begin Time")
            
            fig = px.line(
                ts_data.to_pandas(),
                x="Begin Time",
                y="Total_Traffic_GB",
                title="Traffic Time Series (All Sectors)"
            )
        
        return fig

    def get_combined_ltehourly(
        self,
        df_ltehourly: Optional[pl.DataFrame],
        df_timingadvance: Optional[pl.DataFrame]
    ) -> Optional[pl.DataFrame]:
        """
        Combine LTE Hourly dengan Timing Advance data
        
        Join Criteria:
        - tbl_ltehourly."E-UTRAN Cell Name" == tbl_timingadvance."Eutrancell"
        
        Additional Columns from Timing Advance:
        - Sector_Name
        - FrequencyBand (atau Band)
        - Managed Element
        """
        try:
            # Validasi input
            if df_ltehourly is None or df_ltehourly.is_empty():
                print("DEBUG: LTE Hourly data is empty, cannot combine")
                return None
            
            if df_timingadvance is None or df_timingadvance.is_empty():
                print("DEBUG: Timing Advance data is empty, returning original")
                return df_ltehourly
            
            # Check required columns
            if "E-UTRAN Cell Name" not in df_ltehourly.columns:
                print("ERROR: 'E-UTRAN Cell Name' column not found in LTE Hourly")
                return df_ltehourly
            
            if "Eutrancell" not in df_timingadvance.columns:
                print("ERROR: 'Eutrancell' column not found in Timing Advance")
                return df_ltehourly
            
            # Prepare columns from Timing Advance
            ta_columns = ["Eutrancell"]
            
            if "Sector_Name" in df_timingadvance.columns:
                ta_columns.append("Sector_Name")
            
            if "FrequencyBand" in df_timingadvance.columns:
                ta_columns.append("FrequencyBand")
            elif "Band" in df_timingadvance.columns:
                ta_columns.append("Band")
            
            if "Managed Element" in df_timingadvance.columns:
                ta_columns.append("Managed Element")
            
            print(f"DEBUG: Joining with columns: {ta_columns}")
            
            # Prepare join DataFrame
            df_ta_join = df_timingadvance.select(ta_columns).with_columns(
                pl.col("Eutrancell").cast(pl.Utf8)
            )
            
            # Rename Band to FrequencyBand if needed
            if "Band" in df_ta_join.columns and "FrequencyBand" not in df_ta_join.columns:
                df_ta_join = df_ta_join.rename({"Band": "FrequencyBand"})
            
            # Remove duplicates
            df_ta_join = df_ta_join.unique(subset=["Eutrancell"], keep="first")
            
            print(f"DEBUG: TA join data has {len(df_ta_join)} unique cells")
            
            # Cast cell name to string
            df_ltehourly_clean = df_ltehourly.with_columns(
                pl.col("E-UTRAN Cell Name").cast(pl.Utf8)
            )
            
            # Perform left join
            df_combined = df_ltehourly_clean.join(
                df_ta_join,
                left_on="E-UTRAN Cell Name",
                right_on="Eutrancell",
                how="left"
            )
            
            # Count matches
            if "Sector_Name" in df_combined.columns:
                matched = df_combined.filter(pl.col("Sector_Name").is_not_null()).height
                print(f"DEBUG: Matched {matched}/{len(df_combined)} records")
            
            print(f"DEBUG: Combined result has {len(df_combined)} records")
            
            return df_combined
            
        except Exception as e:
            print(f"ERROR: Failed to combine: {str(e)}")
            import traceback
            traceback.print_exc()
            return df_ltehourly
# ===========================================
# EXAMPLE USAGE IN STREAMLIT
# ===========================================

def render_combined_analysis_section(df_combined: pl.DataFrame):
    """
    Example function untuk render analysis section di Streamlit
    
    Args:
        df_combined: Combined LTE Hourly DataFrame
    """
    import streamlit as st
    
    if df_combined is None or df_combined.is_empty():
        st.warning("No combined data available")
        return
    
    analyzer = LTEHourlyCombinedAnalyzer(df_combined)
    
    st.markdown("### 📊 Combined LTE Hourly Analysis")
    
    # Data Quality Report
    with st.expander("📋 Data Quality Report", expanded=True):
        quality_report = analyzer.get_data_quality_report()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", quality_report["total_records"])
        with col2:
            st.metric("Unique Cells", quality_report["unique_cells"])
        with col3:
            if "sector_match_rate" in quality_report["join_quality"]:
                st.metric("Sector Match Rate", quality_report["join_quality"]["sector_match_rate"])
        
        st.json(quality_report)
    
    # Sector Statistics
    with st.expander("📍 Sector Statistics"):
        sector_stats = analyzer.get_sector_statistics()
        if sector_stats is not None:
            st.dataframe(sector_stats.to_pandas(), use_container_width=True)
            
            # Pie chart
            fig_pie = analyzer.plot_sector_traffic_pie()
            if fig_pie:
                st.plotly_chart(fig_pie, use_container_width=True)
    
    # Frequency Band Statistics
    with st.expander("📡 Frequency Band Statistics"):
        band_stats = analyzer.get_frequency_band_statistics()
        if band_stats is not None:
            st.dataframe(band_stats.to_pandas(), use_container_width=True)
            
            # Bar chart
            fig_bar = analyzer.plot_band_traffic_bar()
            if fig_bar:
                st.plotly_chart(fig_bar, use_container_width=True)
    
    # Top Cells
    with st.expander("🏆 Top 10 Cells by Traffic"):
        top_cells = analyzer.get_top_cells_by_traffic(10)
        if top_cells is not None:
            st.dataframe(top_cells.to_pandas(), use_container_width=True)
    
    # Time Series
    with st.expander("📈 Traffic Time Series"):
        by_sector = st.checkbox("Group by Sector", value=False)
        fig_ts = analyzer.plot_time_series_traffic(by_sector=by_sector)
        if fig_ts:
            st.plotly_chart(fig_ts, use_container_width=True)
    
    # Unmatched Cells
    unmatched = analyzer.get_unmatched_cells()
    if unmatched is not None and not unmatched.is_empty():
        with st.expander(f"⚠️ Unmatched Cells ({len(unmatched)})"):
            st.dataframe(unmatched.to_pandas(), use_container_width=True)


# ===========================================
# STANDALONE USAGE EXAMPLE
# ===========================================

def example_standalone_analysis(df_combined: pl.DataFrame):
    """
    Example untuk standalone analysis (non-Streamlit)
    """
    
    print("=" * 60)
    print("LTE HOURLY COMBINED ANALYSIS")
    print("=" * 60)
    
    analyzer = LTEHourlyCombinedAnalyzer(df_combined)
    
    # 1. Data Quality Report
    print("\n1. DATA QUALITY REPORT")
    print("-" * 60)
    quality = analyzer.get_data_quality_report()
    for key, value in quality.items():
        print(f"{key}: {value}")
    
    # 2. Sector Statistics
    print("\n2. SECTOR STATISTICS")
    print("-" * 60)
    sector_stats = analyzer.get_sector_statistics()
    if sector_stats is not None:
        print(sector_stats)
    
    # 3. Frequency Band Statistics
    print("\n3. FREQUENCY BAND STATISTICS")
    print("-" * 60)
    band_stats = analyzer.get_frequency_band_statistics()
    if band_stats is not None:
        print(band_stats)
    
    # 4. Top Cells
    print("\n4. TOP 10 CELLS BY TRAFFIC")
    print("-" * 60)
    top_cells = analyzer.get_top_cells_by_traffic(10)
    if top_cells is not None:
        print(top_cells)
    
    # 5. Unmatched Cells
    print("\n5. UNMATCHED CELLS")
    print("-" * 60)
    unmatched = analyzer.get_unmatched_cells()
    if unmatched is not None:
        print(f"Total unmatched cells: {len(unmatched)}")
        if not unmatched.is_empty():
            print(unmatched.head(5))
    
    print("\n" + "=" * 60)