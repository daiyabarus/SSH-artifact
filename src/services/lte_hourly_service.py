"""
============================================================================
FILE: src/services/lte_hourly_service.py
LTE Hourly Data Service - Data Fetching, Cleansing & Transformation
============================================================================
"""

import polars as pl
import sqlite3
import logging
from typing import List
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LTEHourlyService:
    """Service for fetching and cleansing LTE hourly data from SQLite"""

    CELL_ID_MAPPING = {
        131: ("1", "850"),
        132: ("2", "850"),
        133: ("3", "850"),
        134: ("4", "850"),
        4: ("1", "1800"),
        5: ("2", "1800"),
        6: ("3", "1800"),
        24: ("4", "1800"),
        51: ("11", "1800"),
        52: ("12", "1800"),
        53: ("13", "1800"),
        54: ("14", "1800"),
        55: ("15", "1800"),
        56: ("16", "1800"),
        14: ("M1", "1800"),
        15: ("M2", "1800"),
        16: ("M3", "1800"),
        64: ("M4", "1800"),
        1: ("1", "2100"),
        2: ("2", "2100"),
        3: ("3", "2100"),
        7: ("1", "2100"),
        8: ("2", "2100"),
        9: ("3", "2100"),
        97: ("11", "2100"),
        27: ("4", "2100"),
        91: ("11", "2100"),
        92: ("12", "2100"),
        93: ("13", "2100"),
        94: ("14", "2100"),
        95: ("15", "2100"),
        96: ("16", "2100"),
        17: ("M1", "2100"),
        18: ("M2", "2100"),
        19: ("M3", "2100"),
        67: ("M4", "2100"),
        111: ("1", "2300F1"),
        112: ("2", "2300F1"),
        113: ("3", "2300F1"),
        114: ("4", "2300F1"),
        141: ("11", "2300F1"),
        142: ("12", "2300F1"),
        143: ("13", "2300F1"),
        144: ("14", "2300F1"),
        145: ("15", "2300F1"),
        146: ("16", "2300F1"),
        121: ("1", "2300F2"),
        122: ("2", "2300F2"),
        123: ("3", "2300F2"),
        124: ("4", "2300F2"),
        151: ("11", "2300F2"),
        152: ("12", "2300F2"),
        153: ("13", "2300F2"),
        154: ("14", "2300F2"),
        155: ("15", "2300F2"),
        156: ("16", "2300F2"),
    }

    def __init__(self, db_path: str):
        """Initialize with SQLite database path"""
        self.db_path = db_path

    def lte_hourly(self, tower_ids: List[str]) -> pl.DataFrame:
        """
        Fetch and cleanse LTE hourly data for given tower IDs

        Args:
            tower_ids: List of tower ID patterns (e.g., ['SUM-AC-STR-0013'])

        Returns:
            Polars DataFrame with cleansed data
        """
        if not tower_ids:
            logger.warning("No tower IDs provided")
            return pl.DataFrame()

        try:
            where_conditions = " OR ".join(
                [f"lte_hour_me_name LIKE '%{tid}%'" for tid in tower_ids]
            )

            query = f"""
            SELECT * FROM tbl_newltehourly 
            WHERE {where_conditions}
            ORDER BY lte_hour_begin_time, lte_hour_cell_id
            """

            conn = sqlite3.connect(self.db_path)
            df = pl.read_database(query, conn)
            conn.close()

            logger.info(f"Fetched {len(df)} records from database")

            if df.is_empty():
                logger.warning("No data found for specified tower IDs")
                return df

            df = self._cleanse_data(df)

            df = self._add_sector_band_columns(df)

            logger.info(f"Data cleansing completed. Final records: {len(df)}")

            return df

        except Exception as e:
            logger.error(f"Error fetching LTE hourly data: {e}")
            return pl.DataFrame()

    def _cleanse_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Cleanse the data:
        1. Convert datetime columns
        2. Remove commas from numeric columns
        """

        datetime_cols = ["lte_hour_begin_time", "lte_hour_end_time"]
        for col in datetime_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                    .alias(col)
                )

        exclude_cols = datetime_cols + [
            "lte_hour_granularity",
            "lte_hour_subnet_name",
            "lte_hour_me_name",
            "lte_hour_enodeb_cu_name",
            "lte_hour_lte_name",
            "lte_hour_eutran_cell_name",
        ]

        numeric_cols = [col for col in df.columns if col not in exclude_cols]

        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(col).dtype == pl.Utf8)
                    .then(
                        pl.col(col)
                        .str.replace_all(",", "")
                        .cast(pl.Float64, strict=False)
                    )
                    .otherwise(pl.col(col).cast(pl.Float64, strict=False))
                    .alias(col)
                )

        return df

    def _add_sector_band_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add Sector and Band columns based on lte_hour_cell_id mapping
        """
        if "lte_hour_cell_id" not in df.columns:
            logger.warning("lte_hour_cell_id column not found")
            return df

        sector_mapping = pl.when(pl.col("lte_hour_cell_id").is_null()).then(None)
        band_mapping = pl.when(pl.col("lte_hour_cell_id").is_null()).then(None)

        for cell_id, (sector, band) in self.CELL_ID_MAPPING.items():
            sector_mapping = sector_mapping.when(
                pl.col("lte_hour_cell_id") == cell_id
            ).then(pl.lit(sector))

            band_mapping = band_mapping.when(
                pl.col("lte_hour_cell_id") == cell_id
            ).then(pl.lit(band))

        sector_mapping = sector_mapping.otherwise(pl.lit("Unknown"))
        band_mapping = band_mapping.otherwise(pl.lit("Unknown"))

        df = df.with_columns(
            [sector_mapping.alias("sector"), band_mapping.alias("band")]
        )

        return df
