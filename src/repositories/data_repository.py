"""
============================================================================
FILE: src/repositories/data_repository.py
============================================================================
"""

from typing import List
from datetime import datetime
import polars as pl
import logging
from src.config.settings import Settings
from src.utils.process.query_builder import QueryBuilder
from src.utils.process.date_normalizer import DateNormalizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataRepository:
    """Repository for analytics data access with date normalization"""

    def __init__(self, db_path: str):
        self._query_builder = QueryBuilder(db_path)
        self._settings = Settings()
        self._date_normalizer = DateNormalizer()

    def _format_date(self, date: datetime) -> str:
        """Format datetime to string for SQL"""
        return date.strftime("%m/%d/%Y")

    def _build_tower_filter(self, tower_ids: List[str], column: str) -> str:
        """Build SQL filter for tower IDs"""
        if not tower_ids:
            return "1=0"

        ids_str_list = [str(tower_id) for tower_id in tower_ids]
        ids_str = "', '".join(ids_str_list)
        return f"{column} IN ('{ids_str}')"

    def _build_date_filter_flexible(
        self, start_date: datetime, end_date: datetime, column: str
    ) -> str:
        """
        Build flexible date filter that handles multiple formats
        """
        start_mdy = self._format_date(start_date)
        end_mdy = self._format_date(end_date)

        start_ymd = start_date.strftime("%Y-%m-%d")
        end_ymd = end_date.strftime("%Y-%m-%d")

        filter_str = (
            f"({column} BETWEEN '{start_mdy}' AND '{end_mdy}' OR "
            f"{column} BETWEEN '{start_ymd}' AND '{end_ymd}')"
        )

        logger.info(f"Flexible date filter: {filter_str}")
        return filter_str

    def _normalize_dates_in_df(self, df: pl.DataFrame, date_col: str) -> pl.DataFrame:
        """
        Normalize mixed date formats in DataFrame after fetch
        """
        if df.is_empty() or date_col not in df.columns:
            return df

        df = self._date_normalizer.normalize_date_column(
            df, date_col, output_col=f"{date_col}_normalized"
        )

        if f"{date_col}_normalized" in df.columns:
            df = df.with_columns(
                pl.col(f"{date_col}_normalized")
                .dt.strftime("%m/%d/%Y")
                .alias(f"{date_col}_clean")
            )

        return df

    def fetch_wd_data(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Fetch Weekday data with date normalization"""
        table = self._settings.TABLE_WD
        tower_col = self._settings.TOWERID_COLUMNS[table]
        date_col = self._settings.DATE_COLUMNS[table]

        where = f"{self._build_tower_filter(tower_ids, tower_col)} AND {self._build_date_filter_flexible(start_date, end_date, date_col)}"
        logger.info(f"WD Query WHERE: {where}")

        query = self._query_builder.select(
            table=table, where=where, order_by=f"{date_col}, {tower_col}"
        )

        df = self._query_builder.to_dataframe(query, engine="polars")
        logger.info(f"WD Data fetched (before date normalization): {len(df)} rows")

        if not df.is_empty():
            df = self._normalize_dates_in_df(df, date_col)
            logger.info(f"WD Data after normalization: {len(df)} rows")

        return df

    def fetch_bh_data(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Fetch Busy Hour data with date normalization"""
        table = self._settings.TABLE_BH
        tower_col = self._settings.TOWERID_COLUMNS[table]
        date_col = self._settings.DATE_COLUMNS[table]

        where = f"{self._build_tower_filter(tower_ids, tower_col)} AND {self._build_date_filter_flexible(start_date, end_date, date_col)}"
        logger.info(f"BH Query WHERE: {where}")

        query = self._query_builder.select(
            table=table, where=where, order_by=f"{date_col}, {tower_col}"
        )

        df = self._query_builder.to_dataframe(query, engine="polars")
        logger.info(f"BH Data fetched: {len(df)} rows")

        if not df.is_empty():
            df = self._normalize_dates_in_df(df, date_col)

        return df

    def fetch_twog_data(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Fetch 2G data with date normalization"""
        table = self._settings.TABLE_TWOG
        tower_col = self._settings.TOWERID_COLUMNS[table]
        date_col = self._settings.DATE_COLUMNS[table]

        where = f"{self._build_tower_filter(tower_ids, tower_col)} AND {self._build_date_filter_flexible(start_date, end_date, date_col)}"
        logger.info(f"2G Query WHERE: {where}")

        query = self._query_builder.select(
            table=table, where=where, order_by=f"{date_col}, {tower_col}"
        )

        df = self._query_builder.to_dataframe(query, engine="polars")
        logger.info(f"2G Data fetched: {len(df)} rows")

        if not df.is_empty():
            df = self._normalize_dates_in_df(df, date_col)
            logger.info(f"2G Columns: {df.columns}")

        return df

    def fetch_joined_ta_wd(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Fetch joined WD + TA with date normalization"""
        ta_table = self._settings.TABLE_TA
        wd_table = self._settings.TABLE_WD

        wd_tower_col = self._settings.TOWERID_COLUMNS[wd_table]
        wd_date_col = self._settings.DATE_COLUMNS[wd_table]

        wd_join_col = "newwd_moentity"
        ta_join_col = "newta_eutrancell"

        tower_filter = self._build_tower_filter(tower_ids, wd_tower_col)
        date_filter = self._build_date_filter_flexible(
            start_date, end_date, wd_date_col
        )

        where = f"{tower_filter} AND {date_filter}"

        join_query = f"""
        SELECT 
            w.*,
            t.newta_sector,
            t.newta_sector_name, 
            t.newta_enodebid,
            t.newta_cellid
        FROM {wd_table} w
        LEFT JOIN {ta_table} t ON w.{wd_join_col} = t.{ta_join_col}
        WHERE {where}
        ORDER BY w.{wd_date_col}, w.{wd_tower_col}
        """

        logger.info(f"Joined WD+TA Query: {join_query}")

        df = self._query_builder.to_dataframe(join_query, engine="polars")
        logger.info(f"Joined WD+TA Data fetched: {len(df)} rows")

        if not df.is_empty():
            df = self._normalize_dates_in_df(df, wd_date_col)

        return df

    def fetch_joined_ta_bh(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> pl.DataFrame:
        """Fetch joined BH + TA with date normalization"""
        ta_table = self._settings.TABLE_TA
        bh_table = self._settings.TABLE_BH

        bh_tower_col = self._settings.TOWERID_COLUMNS[bh_table]
        bh_date_col = self._settings.DATE_COLUMNS[bh_table]

        bh_join_col = "newbh_moentity"
        ta_join_col = "newta_eutrancell"

        tower_filter = self._build_tower_filter(tower_ids, bh_tower_col)
        date_filter = self._build_date_filter_flexible(
            start_date, end_date, bh_date_col
        )

        where = f"{tower_filter} AND {date_filter}"

        join_query = f"""
        SELECT 
            b.*,
            t.newta_sector,
            t.newta_sector_name, 
            t.newta_enodebid,
            t.newta_cellid
        FROM {bh_table} b
        LEFT JOIN {ta_table} t ON b.{bh_join_col} = t.{ta_join_col}
        WHERE {where}
        ORDER BY b.{bh_date_col}, b.{bh_tower_col}
        """

        logger.info(f"Joined BH+TA Query: {join_query}")

        df = self._query_builder.to_dataframe(join_query, engine="polars")
        logger.info(f"Joined BH+TA Data fetched: {len(df)} rows")

        if not df.is_empty():
            df = self._normalize_dates_in_df(df, bh_date_col)

        return df

    def fetch_ta_data_all(self, tower_ids: List[str]) -> pl.DataFrame:
        """Fetch ALL TA data - NO date filter"""
        table = self._settings.TABLE_TA
        tower_col = self._settings.TOWERID_COLUMNS[table]

        where = self._build_tower_filter(tower_ids, tower_col)
        logger.info(f"TA ALL Query WHERE: {where}")

        query = self._query_builder.select(
            table=table,
            where=where,
            order_by=f"{self._settings.DATE_COLUMNS[table]}, {tower_col}",
        )

        df = self._query_builder.to_dataframe(query, engine="polars")
        logger.info(f"TA ALL Data fetched: {len(df)} rows")
        return df

    def fetch_kqi_data_all(self, tower_ids: List[str]) -> pl.DataFrame:
        """Fetch ALL KQI data - NO date filter"""
        table = self._settings.TABLE_KQI
        tower_col = self._settings.TOWERID_COLUMNS[table]

        where = self._build_tower_filter(tower_ids, tower_col)
        logger.info(f"KQI ALL Query WHERE: {where}")

        query = self._query_builder.select(
            table=table,
            where=where,
            order_by=f"{self._settings.DATE_COLUMNS[table]}, {tower_col}",
        )

        df = self._query_builder.to_dataframe(query, engine="polars")
        logger.info(f"KQI ALL Data fetched: {len(df)} rows")
        return df

    def fetch_scot_data(self, tower_ids: List[str]) -> pl.DataFrame:
        """Fetch SCOT data (no date filter)"""
        table = self._settings.TABLE_SCOT
        tower_cols = self._settings.TOWERID_COLUMNS[table]

        where = f"({self._build_tower_filter(tower_ids, tower_cols[0])} OR {self._build_tower_filter(tower_ids, tower_cols[1])})"
        logger.info(f"SCOT Query WHERE: {where}")

        query = self._query_builder.select(table=table, where=where)

        df = self._query_builder.to_dataframe(query, engine="polars")
        logger.info(f"SCOT Data fetched: {len(df)} rows")
        return df

    def fetch_gcell_data(self, tower_ids: List[str]) -> pl.DataFrame:
        """Fetch GCELL data (no date filter)"""
        table = self._settings.TABLE_GCELL
        tower_col = self._settings.TOWERID_COLUMNS[table]

        where = self._build_tower_filter(tower_ids, tower_col)
        logger.info(f"GCELL Query WHERE: {where}")

        query = self._query_builder.select(table=table, where=where)

        df = self._query_builder.to_dataframe(query, engine="polars")
        logger.info(f"GCELL Data fetched: {len(df)} rows")
        return df

    def fetch_joined_gcell_scot_ta(self, tower_ids: List[str]) -> pl.DataFrame:
        """
        Fetch joined GCELL + SCOT + TA data
        """
        gcell_table = self._settings.TABLE_GCELL
        scot_table = self._settings.TABLE_SCOT
        ta_table = self._settings.TABLE_TA

        gcell_tower_col = self._settings.TOWERID_COLUMNS[gcell_table]
        tower_filter = self._build_tower_filter(tower_ids, gcell_tower_col)

        join_query = f"""
        SELECT 
            g.moentity,
            g.new_tower_id,
            g.band,
            g.longitude AS longitude,
            g.latitude AS latitude,
            g.dir,
            g.ant_type,
            g.ant_size,
            g.beam,
            g.avg_ta,
            g.ta90,
            g.ta99,
            g.ta99_2_5,
            s.newscot_isd,
            s.newscot_target_site,
            t.newta_sector,
            t.newta_sector_name,
            t.newta_ta90
        FROM {gcell_table} g
        LEFT JOIN {scot_table} s ON g.moentity = s.newscot_cell
        LEFT JOIN {ta_table} t ON g.moentity = t.newta_eutrancell
        WHERE {tower_filter}
        ORDER BY g.{gcell_tower_col}, g.moentity
        """

        logger.info(f"Joined GCELL+SCOT+TA Query")

        df = self._query_builder.to_dataframe(join_query, engine="polars")
        logger.info(f"Joined GCELL+SCOT+TA Data fetched: {len(df)} rows")

        if not df.is_empty():
            sample = df.head(3).select(
                ["moentity", "new_tower_id", "latitude", "longitude"]
            )
            logger.info(f"Sample GCELL coordinates:\n{sample}")

        return df

    def fetch_ta_distribution_data(self, tower_ids: List[str]) -> pl.DataFrame:
        """Fetch TA distribution data"""
        table = self._settings.TABLE_TA
        tower_col = self._settings.TOWERID_COLUMNS[table]

        where = self._build_tower_filter(tower_ids, tower_col)
        logger.info(f"TA Distribution Query: {where}")

        query = f"""
        SELECT 
            newta_towerid_sector,
            newta_sector_name,
            newta_band,
            newta_ta90,
            newta_ta99,
            newta_total,
            newta_0_78_m, newta_78_234_m, newta_234_390_m, newta_390_546_m,
            newta_546_702_m, newta_702_858_m, newta_858_1014_m, newta_1014_1560_m,
            newta_1560_2106_m, newta_2106_2652_m, newta_2652_3120_m, newta_3120_3900_m,
            newta_3900_6318_m, newta_6318_10062_m, newta_10062_13962_m, newta_13962_20000_m,
            newta_78, newta_234, newta_390, newta_546, newta_702, newta_858,
            newta_1014, newta_1560, newta_2106, newta_2652, newta_3120, newta_3900,
            newta_6318, newta_10062, newta_13962, newta_20000
        FROM {table}
        WHERE {where}
        ORDER BY newta_sector_name, newta_band
        """

        df = self._query_builder.to_dataframe(query, engine="polars")
        logger.info(f"TA Distribution Data fetched: {len(df)} rows")
        return df

    def fetch_wd_ta_separate(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Fetch WD and TA data separately for debugging"""
        wd_table = self._settings.TABLE_WD
        ta_table = self._settings.TABLE_TA

        wd_tower_col = self._settings.TOWERID_COLUMNS[wd_table]
        wd_date_col = self._settings.DATE_COLUMNS[wd_table]
        ta_tower_col = self._settings.TOWERID_COLUMNS[ta_table]

        wd_tower_filter = self._build_tower_filter(tower_ids, wd_tower_col)
        wd_date_filter = self._build_date_filter_flexible(
            start_date, end_date, wd_date_col
        )
        ta_tower_filter = self._build_tower_filter(tower_ids, ta_tower_col)

        wd_query = f"""
        SELECT * FROM {wd_table}
        WHERE {wd_tower_filter} AND {wd_date_filter}
        ORDER BY {wd_date_col}, {wd_tower_col}
        """

        ta_query = f"""
        SELECT * FROM {ta_table}
        WHERE {ta_tower_filter}
        """

        logger.info(f"WD Separate Query: {wd_query}")
        logger.info(f"TA Separate Query: {ta_query}")

        df_wd = self._query_builder.to_dataframe(wd_query, engine="polars")
        df_ta = self._query_builder.to_dataframe(ta_query, engine="polars")

        logger.info(f"WD Separate Data fetched: {len(df_wd)} rows")
        logger.info(f"TA Separate Data fetched: {len(df_ta)} rows")

        if not df_wd.is_empty():
            df_wd = self._normalize_dates_in_df(df_wd, wd_date_col)

        return df_wd, df_ta

    def fetch_bh_ta_separate(
        self, tower_ids: List[str], start_date: datetime, end_date: datetime
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Fetch BH and TA data separately for debugging"""
        bh_table = self._settings.TABLE_BH
        ta_table = self._settings.TABLE_TA

        bh_tower_col = self._settings.TOWERID_COLUMNS[bh_table]
        bh_date_col = self._settings.DATE_COLUMNS[bh_table]
        ta_tower_col = self._settings.TOWERID_COLUMNS[ta_table]

        bh_tower_filter = self._build_tower_filter(tower_ids, bh_tower_col)
        bh_date_filter = self._build_date_filter_flexible(
            start_date, end_date, bh_date_col
        )
        ta_tower_filter = self._build_tower_filter(tower_ids, ta_tower_col)

        bh_query = f"""
        SELECT * FROM {bh_table}
        WHERE {bh_tower_filter} AND {bh_date_filter}
        ORDER BY {bh_date_col}, {bh_tower_col}
        """

        ta_query = f"""
        SELECT * FROM {ta_table}
        WHERE {ta_tower_filter}
        """

        logger.info(f"BH Separate Query: {bh_query}")
        logger.info(f"TA Separate Query: {ta_query}")

        df_bh = self._query_builder.to_dataframe(bh_query, engine="polars")
        df_ta = self._query_builder.to_dataframe(ta_query, engine="polars")

        logger.info(f"BH Separate Data fetched: {len(df_bh)} rows")
        logger.info(f"TA Separate Data fetched: {len(df_ta)} rows")

        if not df_bh.is_empty():
            df_bh = self._normalize_dates_in_df(df_bh, bh_date_col)

        return df_bh, df_ta

    def _extract_clean_tower_id(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Extract clean tower ID from complex lte_hour_me_name
        Example: "XNZ_4210040E_LTE_LAMPAHAN_2#SUM-AC-STR-0013#MC(810040)" -> "SUM-AC-STR-0013"
        """
        if "lte_hour_me_name" not in df.columns:
            return df

        df = df.with_columns(
            [
                pl.col("lte_hour_me_name")
                .str.extract(r"#([^#]+)#")
                .alias("clean_tower_id")
            ]
        )

        df = df.with_columns(
            [
                pl.when(pl.col("clean_tower_id").is_null())
                .then(pl.col("lte_hour_me_name"))
                .otherwise(pl.col("clean_tower_id"))
                .alias("clean_tower_id")
            ]
        )

        logger.info(f"🏗️ Clean tower IDs: {df['clean_tower_id'].unique().to_list()}")
        return df

    def fetch_lte_hourly_data(
        self,
        tower_ids: List[str],
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> pl.DataFrame:
        """
        ✅ FIXED v2: Fetch LTE Hourly data - DIRECT Polars read (bypass pandas)
        Since all DB columns are properly typed, read directly with Polars
        """
        table = self._settings.TABLE_LTE_HOURLY
        tower_col = self._settings.TOWERID_COLUMNS[table]
        date_col = self._settings.DATE_COLUMNS[table]

        logger.info(f"🔄 Fetching LTE Hourly data for towers: {tower_ids}")

        if not tower_ids:
            logger.warning("❌ No tower IDs provided")
            return pl.DataFrame()

        # Build WHERE clause
        tower_conditions = []
        for tid in tower_ids:
            tower_conditions.append(f"{tower_col} LIKE '%#{tid}#%'")
            tower_conditions.append(f"{tower_col} LIKE '%{tid}%'")

        where = f"({' OR '.join(tower_conditions)})"

        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%d 00:00:00")
            end_str = end_date.strftime("%Y-%m-%d 23:59:59")
            date_filter = f"{date_col} BETWEEN '{start_str}' AND '{end_str}'"
            where = f"{where} AND {date_filter}"

        logger.info(f"🔍 WHERE clause: {where}")

        query = f"SELECT * FROM {table} WHERE {where} ORDER BY {date_col}, lte_hour_cell_id"

        try:
            import sqlite3

            # ===== METHOD 1: Direct Polars Read (Preferred) =====
            # This bypasses pandas entirely and reads directly with Polars
            conn_uri = f"sqlite:///{self._query_builder.db_path}"
            
            try:
                logger.info("🚀 Attempting direct Polars read_database...")
                df = pl.read_database(query, connection=conn_uri)
                logger.info(f"✅ Direct Polars read successful: {len(df)} rows")
                
            except Exception as e:
                logger.warning(f"⚠️ Direct Polars read failed: {e}")
                logger.info("🔄 Falling back to SQLite connector...")
                
                # ===== METHOD 2: Using SQLite Connector =====
                conn = sqlite3.connect(self._query_builder.db_path)
                df = pl.read_database(query, connection=conn)
                conn.close()
                logger.info(f"✅ SQLite connector read successful: {len(df)} rows")

            if df.is_empty():
                logger.warning("⚠️ No data returned from query")
                return pl.DataFrame()

            # Log sample data
            if len(df) > 0:
                sample_cols = [tower_col, 'lte_hour_cell_id'] if tower_col in df.columns else df.columns[:3]
                logger.info(f"📝 Sample data:\n{df.select(sample_cols).head(3)}")

            # Process the data
            df = self._cleanse_lte_hourly_data(df)
            df = self._add_sector_band_mapping(df)
            df = self._extract_clean_tower_id(df)

            logger.info(f"🎯 Final data: {len(df)} rows, columns: {len(df.columns)}")
            return df

        except Exception as e:
            logger.error(f"❌ Error fetching LTE Hourly data: {e}")
            import traceback
            logger.error(f"🔍 Stack trace:\n{traceback.format_exc()}")
            return pl.DataFrame()


    def _cleanse_lte_hourly_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        ✅ FIXED: Cleanse data while preserving Integer columns like cell_id
        Keep cell_id and enodeb_id as Int64, convert metrics to Float64
        """
        if df.is_empty():
            return df

        logger.info("🧹 Starting data cleansing...")

        # ===== 1. Parse Datetime Columns =====
        datetime_cols = ["lte_hour_begin_time", "lte_hour_end_time"]
        for col in datetime_cols:
            if col not in df.columns:
                continue
                
            try:
                if df[col].dtype == pl.Datetime:
                    logger.debug(f"✅ {col} already datetime")
                    continue
                    
                df = df.with_columns(
                    pl.col(col).str.strptime(
                        pl.Datetime, 
                        "%Y-%m-%d %H:%M:%S", 
                        strict=False
                    ).alias(col)
                )
                logger.debug(f"✅ Parsed {col} to datetime")
                
            except Exception as e:
                logger.warning(f"⚠️ Could not parse {col}: {e}")

        # ===== 2. Define Column Categories =====
        # Text columns - keep as string
        text_columns = [
            'lte_hour_granularity',
            'lte_hour_subnet_name',
            'lte_hour_me_name',
            'lte_hour_enodeb_cu_name',
            'lte_hour_enodeb_cu_id',
            'lte_hour_lte_name',
            'lte_hour_eutran_cell_name',
            'lte_hour_dummy_hw_port'
        ]
        
        # Integer ID columns - keep as Int64 (CRITICAL for mapping!)
        integer_id_columns = [
            'lte_hour_cell_id',
            'lte_hour_eutran_cell_id',
            'lte_hour_enodeb_id',
            'lte_hour_subnet_id',
            'lte_hour_me_id',
            'lte_hour_lte_id'
        ]
        
        exclude_from_conversion = datetime_cols + text_columns + integer_id_columns
        
        # ===== 3. Handle Integer ID Columns - Keep as Int64 =====
        logger.info(f"🔑 Processing {len(integer_id_columns)} ID columns as Int64...")
        for col in integer_id_columns:
            if col not in df.columns:
                continue
                
            try:
                if df[col].dtype in [pl.Int64, pl.Int32]:
                    # Already integer, keep it
                    df = df.with_columns(
                        pl.col(col).cast(pl.Int64, strict=False).alias(col)
                    )
                    logger.debug(f"✅ {col} kept as Int64")
                
                elif df[col].dtype == pl.Float64:
                    # Convert float to int (handles 131.0 → 131)
                    df = df.with_columns(
                        pl.col(col).cast(pl.Int64, strict=False).alias(col)
                    )
                    logger.debug(f"✅ {col} converted Float64 → Int64")
                
                elif df[col].dtype == pl.Utf8:
                    # String to int
                    df = df.with_columns(
                        pl.col(col)
                        .str.strip_chars()
                        .cast(pl.Int64, strict=False)
                        .alias(col)
                    )
                    logger.debug(f"✅ {col} converted String → Int64")
                    
            except Exception as e:
                logger.warning(f"⚠️ Could not process ID column {col}: {e}")

        # ===== 4. Convert Metric Columns to Float64 =====
        metric_cols = [
            col for col in df.columns 
            if col not in exclude_from_conversion
        ]
        
        logger.info(f"🔢 Converting {len(metric_cols)} metric columns to Float64...")
        
        for col in metric_cols:
            try:
                if df[col].dtype in [pl.Int64, pl.Float64]:
                    df = df.with_columns(
                        pl.col(col).cast(pl.Float64, strict=False).alias(col)
                    )
                
                elif df[col].dtype == pl.Utf8:
                    # Clean string metrics (remove commas, quotes, etc.)
                    df = df.with_columns(
                        pl.col(col)
                        .str.replace_all(",", "")
                        .str.replace_all('"', "")
                        .str.replace_all("%", "")
                        .str.strip_chars()
                        .cast(pl.Float64, strict=False)
                        .alias(col)
                    )
                
            except Exception as e:
                logger.debug(f"⚠️ Could not convert {col}: {e}")
                df = df.with_columns(
                    pl.lit(None).cast(pl.Float64).alias(col)
                )

        # ===== 5. Verify cell_id is Int64 =====
        if 'lte_hour_cell_id' in df.columns:
            cell_dtype = df['lte_hour_cell_id'].dtype
            sample_cells = df.select('lte_hour_cell_id').unique().sort('lte_hour_cell_id').head(10)
            logger.info(f"✅ lte_hour_cell_id dtype: {cell_dtype}")
            logger.info(f"✅ Sample cell_ids: {sample_cells['lte_hour_cell_id'].to_list()}")

        logger.info("✅ Data cleansing complete")
        return df


    def _add_sector_band_mapping(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        ✅ FIXED: Add Sector and Band columns based on lte_hour_cell_id
        Works directly with Int64 cell_id (no conversion needed)
        """
        if "lte_hour_cell_id" not in df.columns:
            logger.warning("❌ Cannot add sector/band mapping - missing cell_id")
            return df

        logger.info("🗺️ Adding sector and band mapping...")
        
        # Verify cell_id is Int64
        cell_dtype = df['lte_hour_cell_id'].dtype
        logger.info(f"🔍 Cell_id dtype: {cell_dtype}")
        
        if cell_dtype != pl.Int64:
            logger.warning(f"⚠️ Expected Int64, got {cell_dtype}. Attempting conversion...")
            df = df.with_columns(
                pl.col("lte_hour_cell_id").cast(pl.Int64, strict=False).alias("lte_hour_cell_id")
            )
        
        # Sample values for debugging
        sample_cells = df.select("lte_hour_cell_id").unique().sort("lte_hour_cell_id").head(10)
        logger.info(f"🔍 Sample cell_id values: {sample_cells['lte_hour_cell_id'].to_list()}")

        # Define the mapping
        CELL_ID_MAPPING = {
            # 850 MHz
            131: ("1", "850"), 132: ("2", "850"), 133: ("3", "850"), 134: ("4", "850"),
            # 1800 MHz
            4: ("1", "1800"), 5: ("2", "1800"), 6: ("3", "1800"), 24: ("4", "1800"),
            51: ("11", "1800"), 52: ("12", "1800"), 53: ("13", "1800"), 
            54: ("14", "1800"), 55: ("15", "1800"), 56: ("16", "1800"),
            14: ("M1", "1800"), 15: ("M2", "1800"), 16: ("M3", "1800"), 64: ("M4", "1800"),
            # 2100 MHz
            1: ("1", "2100"), 2: ("2", "2100"), 3: ("3", "2100"), 7: ("1", "2100"),
            8: ("2", "2100"), 9: ("3", "2100"), 27: ("4", "2100"),
            91: ("11", "2100"), 92: ("12", "2100"), 93: ("13", "2100"), 
            94: ("14", "2100"), 95: ("15", "2100"), 96: ("16", "2100"), 97: ("11", "2100"),
            17: ("M1", "2100"), 18: ("M2", "2100"), 19: ("M3", "2100"), 67: ("M4", "2100"),
            # 2300 F1
            111: ("1", "2300F1"), 112: ("2", "2300F1"), 113: ("3", "2300F1"), 114: ("4", "2300F1"),
            141: ("11", "2300F1"), 142: ("12", "2300F1"), 143: ("13", "2300F1"),
            144: ("14", "2300F1"), 145: ("15", "2300F1"), 146: ("16", "2300F1"),
            # 2300 F2
            121: ("1", "2300F2"), 122: ("2", "2300F2"), 123: ("3", "2300F2"), 124: ("4", "2300F2"),
            151: ("11", "2300F2"), 152: ("12", "2300F2"), 153: ("13", "2300F2"),
            154: ("14", "2300F2"), 155: ("15", "2300F2"), 156: ("16", "2300F2"),
        }

        # Build mapping expressions - direct comparison with Int64
        sector_expr = pl.lit("Unknown")
        band_expr = pl.lit("Unknown")

        for cell_id, (sector, band) in CELL_ID_MAPPING.items():
            sector_expr = (
                pl.when(pl.col("lte_hour_cell_id") == cell_id)
                .then(pl.lit(sector))
                .otherwise(sector_expr)
            )
            band_expr = (
                pl.when(pl.col("lte_hour_cell_id") == cell_id)
                .then(pl.lit(band))
                .otherwise(band_expr)
            )

        # Apply mapping
        df = df.with_columns([
            sector_expr.alias("sector"),
            band_expr.alias("band")
        ])

        # Log results with detail
        sector_counts = df.group_by("sector").agg(pl.count()).sort("sector")
        band_counts = df.group_by("band").agg(pl.count()).sort("band")
        
        logger.info(f"📊 Sector mapping: {sector_counts.to_dicts()}")
        logger.info(f"📡 Band mapping: {band_counts.to_dicts()}")
        
        # Warn if all Unknown
        unknown_count = df.filter(pl.col("sector") == "Unknown").height
        if unknown_count == len(df):
            logger.error(f"❌ ALL {len(df)} records mapped to Unknown!")
            unique_cells = df.select("lte_hour_cell_id").unique().sort("lte_hour_cell_id")
            logger.error(f"❌ Unique cell_ids in data: {unique_cells['lte_hour_cell_id'].to_list()}")
            logger.error(f"❌ Expected cell_ids: {list(CELL_ID_MAPPING.keys())[:20]}...")
        elif unknown_count > 0:
            logger.warning(f"⚠️ {unknown_count} records have Unknown sector/band")
            unknown_cells = (
                df.filter(pl.col("sector") == "Unknown")
                .select("lte_hour_cell_id")
                .unique()
                .sort("lte_hour_cell_id")
            )
            logger.warning(f"⚠️ Unmapped cell_ids: {unknown_cells['lte_hour_cell_id'].to_list()}")
        else:
            logger.info(f"✅ All {len(df)} records successfully mapped!")

        return df