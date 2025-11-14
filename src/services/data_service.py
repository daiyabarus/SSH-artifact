"""
============================================================================
FILE: src/services/data_service.py
============================================================================
"""

from typing import List
from datetime import datetime
import polars as pl
from src.repositories.data_repository import DataRepository
from src.domain.aggregators.kpi_aggregator import KPIAggregator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataService:
    """
    Service layer for data operations
    Follows Single Responsibility Principle
    """

    def __init__(self, db_path: str):
        """
        Initialize DataService

        Args:
            db_path: Path to database
        """
        self._repository = DataRepository(db_path)
        self._kpi_aggregator = KPIAggregator()

    def get_ta_data(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get TA (Timing Advance) data - ALL DATA, no date filter"""
        return self._repository.fetch_ta_data_all(tower_ids)

    def get_kqi_data(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get KQI data - ALL DATA, no date filter"""
        return self._repository.fetch_kqi_data_all(tower_ids)

    def get_bh_data(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get Busy Hour data - WITH date filter"""
        return self._repository.fetch_bh_data(tower_ids, start_date, end_date)

    def get_wd_data(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get Weekday data - WITH date filter"""
        return self._repository.fetch_wd_data(tower_ids, start_date, end_date)

    def get_joined_ta_wd(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """FIXED: Get joined WD + TA data (WD as anchor)"""
        return self._repository.fetch_joined_ta_wd(tower_ids, start_date, end_date)

    def get_joined_ta_bh(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """FIXED: Get joined BH + TA data (BH as anchor)"""
        return self._repository.fetch_joined_ta_bh(tower_ids, start_date, end_date)

    def get_twog_data(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get 2G data - WITH date filter"""
        return self._repository.fetch_twog_data(tower_ids, start_date, end_date)

    def get_bh_data_with_kpi(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get Busy Hour data WITH KPI calculation"""
        df = self._repository.fetch_bh_data(tower_ids, start_date, end_date)
        if df.is_empty():
            return df
        try:
            return self._kpi_aggregator.calculate_bh_kpis(df)
        except Exception as e:
            print(f"KPI calculation error: {e}")
            return df

    def get_wd_data_with_kpi(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get Weekday data WITH KPI calculation"""
        df = self._repository.fetch_wd_data(tower_ids, start_date, end_date)
        if df.is_empty():
            return df
        try:
            return self._kpi_aggregator.calculate_wd_kpis(df)
        except Exception as e:
            print(f"KPI calculation error: {e}")
            return df

    def get_twog_data_with_kpi(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get 2G data WITH KPI calculation"""
        df = self._repository.fetch_twog_data(tower_ids, start_date, end_date)
        if df.is_empty():
            return df
        try:
            return self._kpi_aggregator.calculate_twog_kpis(df)
        except Exception as e:
            print(f"KPI calculation error: {e}")
            return df

    def get_scot_data(self, tower_ids: List[str]) -> pl.DataFrame:
        """Get SCOT (Site Configuration) data"""
        return self._repository.fetch_scot_data(tower_ids)

    def get_gcell_data(self, tower_ids: List[str]) -> pl.DataFrame:
        """Get GCELL data"""
        return self._repository.fetch_gcell_data(tower_ids)

    def get_configuration_data(
        self, tower_ids: List[str]
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Get SCOT + GCELL with SCOT reference for tower_ids"""
        df_scot = self.get_scot_data(tower_ids)
        if df_scot.is_empty():
            return df_scot, pl.DataFrame()

        scot_tower_cols = ["newscot_site", "newscot_target_site"]
        unique_towers = set()
        for col in scot_tower_cols:
            if col in df_scot.columns:
                unique_towers.update(
                    df_scot.select(pl.col(col)).unique().to_series().to_list()
                )

        scot_tower_ids = list(unique_towers)

        df_gcell = self.get_gcell_data(scot_tower_ids)

        return df_scot, df_gcell

    def get_joined_gcell_scot_ta(self, tower_ids: List[str]) -> pl.DataFrame:
        """
        Get joined GCELL + SCOT + TA data dengan 1st tier towers
        """

        df_scot = self.get_scot_data(tower_ids)
        if df_scot.is_empty():
            logger.warning("No SCOT data found for tower_ids")
            return pl.DataFrame()

        all_tower_ids = set(tower_ids)

        if "newscot_target_site" in df_scot.columns:
            target_sites = (
                df_scot.select(pl.col("newscot_target_site"))
                .filter(pl.col("newscot_target_site").is_not_null())
                .unique()
                .to_series()
                .to_list()
            )
            all_tower_ids.update(target_sites)
            logger.info(f"✅ Added {len(target_sites)} target sites (1st tier)")

        if "newscot_site" in df_scot.columns:
            source_sites = (
                df_scot.select(pl.col("newscot_site"))
                .filter(pl.col("newscot_site").is_not_null())
                .unique()
                .to_series()
                .to_list()
            )
            all_tower_ids.update(source_sites)

        expanded_tower_ids = list(all_tower_ids)
        logger.info(f"📡 Main towers: {len(tower_ids)}")
        logger.info(f"🔗 Total with 1st tier: {len(expanded_tower_ids)}")

        return self._repository.fetch_joined_gcell_scot_ta(expanded_tower_ids)

    def get_ta_distribution_data(self, tower_ids: List[str]) -> pl.DataFrame:
        """Get TA distribution data khusus untuk visualizer"""
        return self._repository.fetch_ta_distribution_data(tower_ids)

    def get_joined_wd_ta(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Get joined WD + TA data dengan debugging"""

        df_wd_separate, df_ta_separate = self.get_wd_ta_separate(
            tower_ids, start_date, end_date
        )

        logger.info(f"DEBUG - WD Separate: {len(df_wd_separate)} rows")
        logger.info(f"DEBUG - TA Separate: {len(df_ta_separate)} rows")

        if not df_wd_separate.is_empty():
            sample_dates = df_wd_separate["newwd_date"].unique().head(5).to_list()
            sample_towers = df_wd_separate["newwd_enodeb_fdd_msc"].unique().to_list()
            logger.info(f"DEBUG - WD Sample Dates: {sample_dates}")
            logger.info(f"DEBUG - WD Sample Towers: {sample_towers}")

        if not df_ta_separate.is_empty():
            sample_sectors = (
                df_ta_separate["newta_sector_name"].unique().head(5).to_list()
            )
            logger.info(f"DEBUG - TA Sample Sectors: {sample_sectors}")

        df_joined = self._repository.fetch_joined_ta_wd(tower_ids, start_date, end_date)

        logger.info(f"DEBUG - Joined Rows: {len(df_joined)}")
        if not df_joined.is_empty():
            join_success = df_joined.filter(
                pl.col("newta_sector_name").is_not_null()
            ).height
            logger.info(f"DEBUG - Successful Joins: {join_success}")

            unique_sectors = df_joined["newta_sector_name"].unique().to_list()
            unique_dates = df_joined["newwd_date"].unique().to_list()
            logger.info(f"DEBUG - Joined Unique Sectors: {len(unique_sectors)}")
            logger.info(f"DEBUG - Joined Unique Dates: {len(unique_dates)}")

        return df_joined

    def get_joined_bh_ta(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """FIXED: Get joined BH + TA data (BH as anchor)"""
        return self._repository.fetch_joined_ta_bh(tower_ids, start_date, end_date)

    def get_wd_ta_separate(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Get WD and TA data separately for debugging"""
        return self._repository.fetch_wd_ta_separate(tower_ids, start_date, end_date)

    def get_bh_ta_separate(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Get BH and TA data separately for debugging"""
        return self._repository.fetch_bh_ta_separate(tower_ids, start_date, end_date)

    def get_lte_hourly_data(
        self,
        tower_ids: List[str],
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> pl.DataFrame:
        """
        ✅ FIXED: Get LTE Hourly data with proper data cleansing
        """
        return self._repository.fetch_lte_hourly_data(tower_ids, start_date, end_date)

    def get_lte_hourly_summary(
        self,
        tower_ids: List[str],
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> dict:
        """
        Get summary statistics for LTE Hourly data

        Returns:
            dict: Summary metrics including record count, sectors, bands, etc.
        """
        df = self.get_lte_hourly_data(tower_ids, start_date, end_date)

        if df.is_empty():
            return {
                "total_records": 0,
                "unique_sectors": 0,
                "unique_bands": 0,
                "total_traffic_gb": 0,
                "time_range_days": 0,
                "start_time": None,
                "end_time": None,
            }

        summary = {
            "total_records": len(df),
            "unique_sectors": df.select(pl.col("sector").n_unique()).item(),
            "unique_bands": df.select(pl.col("band").n_unique()).item(),
        }

        if "lte_hour_total_traffic_gb" in df.columns:
            summary["total_traffic_gb"] = df.select(
                pl.col("lte_hour_total_traffic_gb").sum()
            ).item()
        else:
            summary["total_traffic_gb"] = 0

        if "lte_hour_begin_time" in df.columns:
            time_info = df.select(
                [
                    pl.col("lte_hour_begin_time").min().alias("start"),
                    pl.col("lte_hour_begin_time").max().alias("end"),
                ]
            ).to_dicts()[0]

            summary["start_time"] = time_info["start"]
            summary["end_time"] = time_info["end"]

            if time_info["start"] and time_info["end"]:
                time_range = time_info["end"] - time_info["start"]
                summary["time_range_days"] = time_range.days
            else:
                summary["time_range_days"] = 0
        else:
            summary["start_time"] = None
            summary["end_time"] = None
            summary["time_range_days"] = 0

        return summary
