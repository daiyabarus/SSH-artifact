"""
Hourly Data Service - Dedicated service for LTE and 2G Hourly data queries with date range
"""

from typing import Optional, List
from datetime import datetime
import polars as pl
from src.infrastructure.database.repository import DatabaseRepository


class HourlyDataService:
    """Service untuk query LTE Hourly dan 2G Hourly dengan date range filter"""

    def __init__(self, repository: DatabaseRepository):
        self._repository = repository

    def get_enodeb_ids_from_timingadvance(self, managed_element: str) -> List[str]:
        """
        Get eNodeBId list from tbl_timingadvance
        
        Args:
            managed_element: The Managed Element to filter by
            
        Returns:
            List of eNodeBId values
        """
        try:
            query = f"""
            SELECT DISTINCT "eNodeBId" 
            FROM tbl_timingadvance 
            WHERE "Managed Element" = '{managed_element}'
            AND "eNodeBId" IS NOT NULL
            """
            
            result = self._repository.query(query)
            
            if result is not None and not result.is_empty():
                enodeb_ids = result["eNodeBId"].unique().to_list()
                # Clean the values
                clean_ids = []
                for eid in enodeb_ids:
                    if eid is None:
                        continue
                    eid_str = str(eid).strip()
                    if not eid_str or eid_str.lower() == "nan":
                        continue
                    # Remove .0 suffix if exists
                    if eid_str.endswith(".0"):
                        eid_str = eid_str[:-2]
                    clean_ids.append(eid_str)
                
                print(f"DEBUG: Found {len(clean_ids)} eNodeBId from Timing Advance")
                return clean_ids
            
            print(f"DEBUG: No eNodeBId found in Timing Advance for {managed_element}")
            return []
            
        except Exception as e:
            print(f"Error getting eNodeBId from Timing Advance: {str(e)}")
            return []

    def query_ltehourly_by_daterange(
        self,
        enodeb_ids: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> Optional[pl.DataFrame]:
        """
        Query tbl_ltehourly berdasarkan eNodeBId dan date range
        
        Args:
            enodeb_ids: List of eNodeBId from tbl_timingadvance
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Polars DataFrame or None
        """
        try:
            if not enodeb_ids:
                print("DEBUG: No eNodeBId provided for LTE Hourly query")
                return None

            # Clean and validate eNodeBId values
            valid_enodeb_ids = []
            for enodeb_id in enodeb_ids:
                if not enodeb_id:
                    continue
                eid_str = str(enodeb_id).strip()
                if not eid_str or eid_str.lower() == "nan":
                    continue
                if eid_str.endswith(".0"):
                    eid_str = eid_str[:-2]
                valid_enodeb_ids.append(eid_str)

            if not valid_enodeb_ids:
                print("DEBUG: No valid eNodeBId values for LTE Hourly query")
                return None

            # Format dates for SQL query
            start_date_str = start_date.strftime("%Y-%m-%d 00:00:00")
            end_date_str = end_date.strftime("%Y-%m-%d 23:59:59")

            # Build IN clause
            enodeb_in_clause = ",".join([f"'{eid}'" for eid in valid_enodeb_ids])
            
            # Query dengan filter date range
            query = f"""
            SELECT * FROM tbl_ltehourly 
            WHERE "eNodeBId" IN ({enodeb_in_clause})
            AND "Begin Time" >= '{start_date_str}'
            AND "Begin Time" <= '{end_date_str}'
            ORDER BY "Begin Time" DESC
            """

            print(f"DEBUG: LTE Hourly query - Date range: {start_date_str} to {end_date_str}")
            
            result = self._repository.query(query)

            if result is not None and not result.is_empty():
                print(f"DEBUG: SUCCESS - Found {len(result)} LTE Hourly records")
            else:
                print(f"DEBUG: No LTE Hourly records found")

            return result

        except Exception as e:
            print(f"Error querying LTE Hourly: {str(e)}")
            return None

    def query_twoghourly_by_daterange(
        self,
        site_names: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> Optional[pl.DataFrame]:
        """
        Query tbl_twoghourly berdasarkan SITE Name dan date range
        
        Args:
            site_names: List of site names from tbl_mapping
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Polars DataFrame or None
        """
        try:
            if not site_names:
                print("DEBUG: No site names provided for 2G Hourly query")
                return None

            # Format dates for SQL query
            start_date_str = start_date.strftime("%Y-%m-%d 00:00:00")
            end_date_str = end_date.strftime("%Y-%m-%d 23:59:59")

            # Create IN clause
            site_names_clean = [str(name).replace("'", "''") for name in site_names]
            site_in_clause = ",".join([f"'{name}'" for name in site_names_clean])
            
            query = f"""
            SELECT * FROM tbl_twoghourly 
            WHERE "SITE Name" IN ({site_in_clause})
            AND "Begin Time" >= '{start_date_str}'
            AND "Begin Time" <= '{end_date_str}'
            ORDER BY "Begin Time" DESC
            """

            print(f"DEBUG: 2G Hourly query - Date range: {start_date_str} to {end_date_str}")
            
            result = self._repository.query(query)

            if result is not None and not result.is_empty():
                print(f"DEBUG: SUCCESS - Found {len(result)} 2G Hourly records")
            else:
                print(f"DEBUG: No 2G Hourly records found")

            return result

        except Exception as e:
            print(f"Error querying 2G Hourly: {str(e)}")
            return None

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
        
        Args:
            df_ltehourly: DataFrame dari tbl_ltehourly
            df_timingadvance: DataFrame dari tbl_timingadvance
            
        Returns:
            Combined Polars DataFrame or None
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
                print("ERROR: 'E-UTRAN Cell Name' not found in LTE Hourly")
                return df_ltehourly
            
            if "Eutrancell" not in df_timingadvance.columns:
                print("ERROR: 'Eutrancell' not found in Timing Advance")
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
            
            print(f"DEBUG: Combining with columns: {ta_columns}")
            
            # Select and prepare TA data
            df_ta_join = df_timingadvance.select(ta_columns).with_columns(
                pl.col("Eutrancell").cast(pl.Utf8)
            )
            
            # Rename Band to FrequencyBand if needed
            if "Band" in df_ta_join.columns and "FrequencyBand" not in df_ta_join.columns:
                df_ta_join = df_ta_join.rename({"Band": "FrequencyBand"})
            
            # Remove duplicates
            df_ta_join = df_ta_join.unique(subset=["Eutrancell"], keep="first")
            
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
            
            print(f"DEBUG: Combined LTE Hourly has {len(df_combined)} records")
            
            return df_combined
            
        except Exception as e:
            print(f"ERROR: Failed to combine: {str(e)}")
            return df_ltehourly