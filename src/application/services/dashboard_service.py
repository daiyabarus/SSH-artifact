"""
Dashboard service - Business logic for dashboard queries
"""

from typing import Optional, List, Dict
from datetime import datetime
import polars as pl
from src.infrastructure.database.repository import DatabaseRepository
from src.application.services.hourly_data_service import HourlyDataService


class DashboardService:
    """Service layer for dashboard data queries"""

    def __init__(self, repository: DatabaseRepository):
        self._repository = repository
        self._hourly_service = HourlyDataService(repository)

    def get_managed_elements(self) -> List[str]:
        """Get unique Managed Element values from tbl_timingadvance"""
        try:
            query = 'SELECT DISTINCT "Managed Element" FROM tbl_timingadvance WHERE "Managed Element" IS NOT NULL ORDER BY "Managed Element"'
            result = self._repository.query(query)

            if result is not None and not result.is_empty():
                return result["Managed Element"].to_list()
            return []
        except Exception as e:
            raise Exception(f"Error fetching Managed Elements: {str(e)}")

    def execute_all_queries(
        self, 
        managed_element: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Optional[pl.DataFrame]]:
        """
        Execute all queries for a given Managed Element
        Returns dictionary with query results
        
        Args:
            managed_element: The Managed Element to query
            start_date: Start date for hourly data (optional)
            end_date: End date for hourly data (optional)
        """
        results = {}

        # Query: SCOT (combined SiteID and NCELL SiteID)
        results["scot"] = self._query_scot_combined(managed_element)

        # Query: LTE Timing Advance - augmented
        results["timingadvance"] = self._query_timingadvance_augmented(
            managed_element, results["scot"]
        )

        # Query: LTE Timing Advance - original
        results["timingadvance_original"] = self._query_timingadvance_original(managed_element)

        # Query: GCell - augmented
        results["gcell"] = self._query_gcell_augmented(managed_element, results["scot"])

        # Query: Mapping
        results["mapping"] = self._query_mapping(managed_element)

        # Query: LTE Hourly
        if start_date and end_date:
            enodeb_ids = self._hourly_service.get_enodeb_ids_from_timingadvance(managed_element)
            results["ltehourly"] = self._hourly_service.query_ltehourly_by_daterange(
                enodeb_ids, start_date, end_date
            )
        else:
            enodeb_ids = self._hourly_service.get_enodeb_ids_from_timingadvance(managed_element)
            results["ltehourly"] = self._query_ltehourly_fallback(enodeb_ids)

        # Query: LTE Hourly Combined
        results["ltehourly_combined"] = self._hourly_service.get_combined_ltehourly(
            results.get("ltehourly"),
            results.get("timingadvance")
        )

        # Query: 2G Hourly
        if results["mapping"] is not None and not results["mapping"].is_empty():
            site_names = (
                results["mapping"]["new Site NAME"].to_list()
                if "new Site NAME" in results["mapping"].columns
                else []
            )
            if start_date and end_date:
                results["twoghourly"] = self._hourly_service.query_twoghourly_by_daterange(
                    site_names, start_date, end_date
                )
            else:
                results["twoghourly"] = self._query_twoghourly_fallback(site_names)
        else:
            results["twoghourly"] = None

        # Query: Merged GCell Coverage
        results["gcell_coverage"] = self._merge_gcell_coverage(results)

        return results

    def _query_timingadvance_augmented(
        self, managed_element: str, scot_data: Optional[pl.DataFrame]
    ) -> Optional[pl.DataFrame]:
        """Query tbl_timingadvance with SCOT augmentation"""
        try:
            managed_values = [managed_element]

            if scot_data is not None and not scot_data.is_empty():
                if "NCELL SiteID" in scot_data.columns:
                    ncell_site_ids = scot_data["NCELL SiteID"].unique().to_list()
                    valid_ncell_ids = [
                        str(nid).strip()
                        for nid in ncell_site_ids
                        if nid is not None
                        and str(nid).strip()
                        and str(nid).strip() != managed_element
                        and str(nid).lower() != "nan"
                    ]
                    managed_values.extend(valid_ncell_ids)

            if len(managed_values) == 1:
                query = f"SELECT * FROM tbl_timingadvance WHERE \"Managed Element\" = '{managed_element}'"
            else:
                managed_str = "','".join(managed_values)
                query = f"SELECT * FROM tbl_timingadvance WHERE \"Managed Element\" IN ('{managed_str}')"

            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying augmented Timing Advance: {str(e)}")
            return self._query_timingadvance_original(managed_element)

    def _query_timingadvance_original(self, managed_element: str) -> Optional[pl.DataFrame]:
        """Original Timing Advance query"""
        try:
            query = f"SELECT * FROM tbl_timingadvance WHERE \"Managed Element\" = '{managed_element}'"
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying original Timing Advance: {str(e)}")
            return None

    def _query_gcell_augmented(
        self, managed_element: str, scot_data: Optional[pl.DataFrame]
    ) -> Optional[pl.DataFrame]:
        """Query tbl_gcell with SCOT augmentation"""
        try:
            msc_values = [managed_element]

            if scot_data is not None and not scot_data.is_empty():
                if "NCELL SiteID" in scot_data.columns:
                    ncell_site_ids = scot_data["NCELL SiteID"].unique().to_list()
                    valid_ncell_ids = [
                        str(nid).strip()
                        for nid in ncell_site_ids
                        if nid is not None
                        and str(nid).strip()
                        and str(nid).strip() != managed_element
                        and str(nid).lower() != "nan"
                    ]
                    msc_values.extend(valid_ncell_ids)

            if len(msc_values) == 1:
                query = f"SELECT * FROM tbl_gcell WHERE \"MSC\" = '{managed_element}'"
            else:
                msc_str = "','".join(msc_values)
                query = f"SELECT * FROM tbl_gcell WHERE \"MSC\" IN ('{msc_str}')"

            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying augmented GCell: {str(e)}")
            return None

    def _query_scot_combined(self, managed_element: str) -> Optional[pl.DataFrame]:
        """Query tbl_scot"""
        try:
            query = f"""
            SELECT * FROM tbl_scot 
            WHERE "SiteID" = '{managed_element}' 
            OR "NCELL SiteID" = '{managed_element}'
            """
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying SCOT: {str(e)}")
            return None

    def _query_mapping(self, managed_element: str) -> Optional[pl.DataFrame]:
        """Query tbl_mapping"""
        try:
            query = f"SELECT * FROM tbl_mapping WHERE \"New Tower ID\" = '{managed_element}'"
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying Mapping: {str(e)}")
            return None

    def _query_ltehourly_fallback(self, enodeb_ids: List[str]) -> Optional[pl.DataFrame]:
        """Fallback LTE Hourly query without date range"""
        try:
            if not enodeb_ids:
                return None
            
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
                return None

            enodeb_in_clause = ",".join([f"'{eid}'" for eid in valid_enodeb_ids])
            query = f"""
            SELECT * FROM tbl_ltehourly 
            WHERE "eNodeBId" IN ({enodeb_in_clause})
            ORDER BY "Begin Time" DESC
            LIMIT 100
            """

            return self._repository.query(query)
        except Exception as e:
            print(f"Error in LTE Hourly fallback: {str(e)}")
            return None

    def _query_twoghourly_fallback(self, site_names: List[str]) -> Optional[pl.DataFrame]:
        """Fallback 2G Hourly query without date range"""
        try:
            if not site_names:
                return None

            site_names_clean = [str(name).replace("'", "''") for name in site_names]
            site_in_clause = ",".join([f"'{name}'" for name in site_names_clean])
            query = f"""
            SELECT * FROM tbl_twoghourly 
            WHERE "SITE Name" IN ({site_in_clause}) 
            ORDER BY "Begin Time" DESC 
            LIMIT 100
            """
            return self._repository.query(query)
        except Exception as e:
            print(f"Error in 2G Hourly fallback: {str(e)}")
            return None

    def _merge_gcell_coverage(
        self, results: Dict[str, Optional[pl.DataFrame]]
    ) -> Optional[pl.DataFrame]:
        """Merge GCell with Timing Advance and SCOT data"""
        try:
            df_gcell = results.get("gcell")
            df_timing = results.get("timingadvance")
            df_scot = results.get("scot")

            if df_gcell is None or df_gcell.is_empty():
                return None

            df_coverage = df_gcell.clone().with_columns(
                pl.col("CellName").cast(pl.Utf8)
            )

            # Join with Timing Advance
            if df_timing is not None and not df_timing.is_empty():
                if "Eutrancell" in df_timing.columns and "TA90" in df_timing.columns:
                    timing_join = df_timing.select([
                        pl.col("Eutrancell").cast(pl.Utf8).alias("CellName"),
                        pl.col("TA90").cast(pl.Float64, strict=False).alias("TA90"),
                    ])
                    df_coverage = df_coverage.join(timing_join, on="CellName", how="left")

            # Join with SCOT
            if df_scot is not None and not df_scot.is_empty():
                if "Cell_PI-1" in df_scot.columns and "Min of S2S Distance" in df_scot.columns:
                    df_scot_join = df_scot.select([
                        pl.col("Cell_PI-1").cast(pl.Utf8).alias("CellName"),
                        pl.col("Min of S2S Distance").cast(pl.Utf8),
                        pl.col("FINAL Remark COSTv2.0T").alias("SCOT Remark"),
                        pl.col("NCELL SiteID").alias("1st Tier"),
                    ])
                    df_coverage = df_coverage.join(df_scot_join, on="CellName", how="left")
                    
                    if "Min of S2S Distance" in df_coverage.columns:
                        df_coverage = df_coverage.with_columns(
                            pl.coalesce([pl.col("Min of S2S Distance"), pl.lit(0.0)])
                            .alias("Min of S2S Distance")
                        )

            return df_coverage

        except Exception as e:
            print(f"Error merging gcell_coverage: {str(e)}")
            return None