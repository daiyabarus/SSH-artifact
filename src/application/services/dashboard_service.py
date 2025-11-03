"""
Dashboard service - Business logic for dashboard queries
"""

from typing import Optional, List, Dict
import polars as pl
from src.infrastructure.database.repository import DatabaseRepository


class DashboardService:
    """Service layer for dashboard data queries"""

    def __init__(self, repository: DatabaseRepository):
        self._repository = repository

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
        self, managed_element: str
    ) -> Dict[str, Optional[pl.DataFrame]]:
        """
        Execute all queries for a given Managed Element
        Returns dictionary with query results
        """
        results = {}

        # Query 1: GCell
        results["gcell"] = self._query_gcell(managed_element)

        # Query 2: SCOT (combined SiteID and NCELL SiteID)
        results["scot"] = self._query_scot_combined(managed_element)

        # Query 3: Mapping
        results["mapping"] = self._query_mapping(managed_element)

        # Query 4: LTE Hourly (FIXED eNodeBId mapping)
        results["ltehourly"] = self._query_ltehourly(managed_element)

        # Query 5: 2G Hourly (using mapped Site Names)
        if results["mapping"] is not None and not results["mapping"].is_empty():
            site_names = (
                results["mapping"]["new Site NAME"].to_list()
                if "new Site NAME" in results["mapping"].columns
                else []
            )
            results["twoghourly"] = self._query_twoghourly(site_names)
        else:
            results["twoghourly"] = None

        return results

    def _query_gcell(self, managed_element: str) -> Optional[pl.DataFrame]:
        """Query tbl_gcell where MSC = Managed Element"""
        try:
            query = f"SELECT * FROM tbl_gcell WHERE \"MSC\" = '{managed_element}'"
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying GCell: {str(e)}")
            return None

    def _query_scot_combined(self, managed_element: str) -> Optional[pl.DataFrame]:
        """Query tbl_scot where SiteID or NCELL SiteID = Managed Element"""
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
        """Query tbl_mapping where New Tower ID = Managed Element"""
        try:
            query = f"SELECT * FROM tbl_mapping WHERE \"New Tower ID\" = '{managed_element}'"
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying Mapping: {str(e)}")
            return None

    def _query_ltehourly(self, managed_element: str) -> Optional[pl.DataFrame]:
        """
        Query tbl_ltehourly dengan filter data yang valid (tidak semua 0)
        """
        try:
            # Step 1: Dapatkan eNodeBId dari GCell
            gcell_query = f"SELECT * FROM tbl_gcell WHERE \"MSC\" = '{managed_element}'"
            gcell_data = self._repository.query(gcell_query)

            if gcell_data is None or gcell_data.is_empty():
                print(f"DEBUG: No GCell data found for MSC: {managed_element}")
                return None

            if "eNodeBId" not in gcell_data.columns:
                print(f"DEBUG: eNodeBId column not found in GCell data")
                return None

            # Ambil semua eNodeBId yang unik dari GCell
            enodeb_ids = gcell_data["eNodeBId"].unique().to_list()
            print(f"DEBUG: Found eNodeBId from GCell: {enodeb_ids}")

            if not enodeb_ids:
                print(f"DEBUG: No eNodeBId values found in GCell")
                return None

            # Step 2: Query LTE Hourly dengan filter data yang valid
            valid_enodeb_ids = []
            for enodeb_id in enodeb_ids:
                if (
                    enodeb_id is not None
                    and str(enodeb_id).strip()
                    and str(enodeb_id).lower() != "nan"
                ):
                    clean_id = str(enodeb_id).strip()
                    if clean_id.endswith(".0"):
                        clean_id = clean_id[:-2]
                    valid_enodeb_ids.append(f"'{clean_id}'")

            if not valid_enodeb_ids:
                print(f"DEBUG: No valid eNodeBId values for query")
                return None

            print(f"DEBUG: Querying LTE Hourly with eNodeBId: {valid_enodeb_ids}")

            # BUAT QUERY YANG LEBIH SELEKTIF - filter data yang tidak semua 0
            enodeb_ids_str = ",".join(valid_enodeb_ids)
            query = f"""
            SELECT * FROM tbl_ltehourly 
            WHERE "eNodeBId" IN ({enodeb_ids_str})
            AND (
                "ZTE-SQM_Total_Traffic_GB" > 0 
                OR "ZTE-SQM_Volte_Traffic_Erl" > 0
                OR "ZTE-SQM_User_DL_Thp_Mbps_Num" > 0
                OR "ZTE-SQM_Cell_DL_Thp_Mbps_Num" > 0
            )
            ORDER BY "Begin Time" DESC
            LIMIT 100
            """

            print(f"DEBUG: LTE Hourly query with data filter: {query}")
            result = self._repository.query(query)

            if result is not None and not result.is_empty():
                print(f"DEBUG: SUCCESS - Found {len(result)} VALID LTE Hourly records")
                if "eNodeBId" in result.columns:
                    found_enodeb = result["eNodeBId"].unique().to_list()
                    print(f"DEBUG: Matching eNodeBId with valid data: {found_enodeb}")

                # Tampilkan sample data untuk konfirmasi
                if (
                    "Begin Time" in result.columns
                    and "ZTE-SQM_Total_Traffic_GB" in result.columns
                ):
                    sample = result.select(
                        ["Begin Time", "ZTE-SQM_Total_Traffic_GB"]
                    ).head(3)
                    print(f"DEBUG: Sample data - {sample}")
            else:
                print(f"DEBUG: No VALID LTE Hourly records found (all zeros or empty)")

                # Fallback: tampilkan data meskipun 0, tapi dengan limit
                fallback_query = f"""
                SELECT * FROM tbl_ltehourly 
                WHERE "eNodeBId" IN ({enodeb_ids_str})
                ORDER BY "Begin Time" DESC 
                LIMIT 10
                """
                fallback_result = self._repository.query(fallback_query)
                if fallback_result is not None and not fallback_result.is_empty():
                    print(
                        f"DEBUG: Fallback - found {len(fallback_result)} records (including zeros)"
                    )
                    return fallback_result

            return result

        except Exception as e:
            print(f"Error querying LTE Hourly: {str(e)}")
            return None

    def _query_ltehourly_by_enodebid(
        self, managed_element: str
    ) -> Optional[pl.DataFrame]:
        """Strategy 1: Direct eNodeBId mapping"""
        try:
            # Step 1: Dapatkan eNodeBId dari GCell
            gcell_query = f"SELECT * FROM tbl_gcell WHERE \"MSC\" = '{managed_element}'"
            gcell_data = self._repository.query(gcell_query)

            if gcell_data is None or gcell_data.is_empty():
                print(f"DEBUG: No GCell data found for MSC: {managed_element}")
                return None

            if "eNodeBId" not in gcell_data.columns:
                print(f"DEBUG: eNodeBId column not found in GCell data")
                return None

            # Ambil semua eNodeBId yang unik dari GCell
            enodeb_ids = gcell_data["eNodeBId"].unique().to_list()
            print(f"DEBUG: Found eNodeBId from GCell: {enodeb_ids}")

            if not enodeb_ids:
                print(f"DEBUG: No eNodeBId values found in GCell")
                return None

            # Step 2: Query LTE Hourly berdasarkan eNodeBId
            # Filter hanya eNodeBId yang valid
            valid_enodeb_ids = []
            for enodeb_id in enodeb_ids:
                if enodeb_id is None:
                    continue
                try:
                    # Convert ke int untuk menghilangkan .0
                    enodeb_int = int(float(enodeb_id))
                    valid_enodeb_ids.append(str(enodeb_int))
                except (ValueError, TypeError):
                    continue

            if not valid_enodeb_ids:
                print(f"DEBUG: No valid eNodeBId values for query")
                return None

            print(f"DEBUG: Querying LTE Hourly with eNodeBId: {valid_enodeb_ids}")

            # Buat kondisi IN untuk setiap eNodeBId
            enodeb_ids_str = ",".join(valid_enodeb_ids)
            query = (
                f'SELECT * FROM tbl_ltehourly WHERE "eNodeBId" IN ({enodeb_ids_str})'
            )

            print(f"DEBUG: LTE Hourly eNodeBId query: {query}")
            result = self._repository.query(query)

            if result is not None and not result.is_empty():
                print(
                    f"DEBUG: Strategy 1 SUCCESS - Found {len(result)} LTE Hourly records"
                )
                # Debug: tampilkan eNodeBId yang ditemukan
                if "eNodeBId" in result.columns:
                    found_enodeb = result["eNodeBId"].unique().to_list()
                    print(f"DEBUG: Found eNodeBId in LTE Hourly: {found_enodeb}")
            else:
                print(f"DEBUG: Strategy 1 FAILED - No LTE Hourly records found")

                # Debug: cek apakah ada data di LTE Hourly
                check_query = 'SELECT "eNodeBId" FROM tbl_ltehourly LIMIT 5'
                check_result = self._repository.query(check_query)
                if check_result is not None and not check_result.is_empty():
                    sample_enodeb = check_result["eNodeBId"].unique().to_list()
                    print(f"DEBUG: Sample eNodeBId in LTE Hourly: {sample_enodeb}")

            return result

        except Exception as e:
            print(f"Error in eNodeBId mapping strategy: {str(e)}")
            return None

    def _query_ltehourly_fallback(self, managed_element: str) -> Optional[pl.DataFrame]:
        """Strategy 2: Fallback mapping melalui Managed Element pattern"""
        try:
            # Coba extract Site ID dari Managed Element
            # Format: "SUM-AC-STR-0013" atau "290532"
            import re

            # Pattern untuk SUM-xxx-xxx-xxxx
            sum_pattern = r"SUM-[A-Z]+-[A-Z]+-\d+"
            sum_match = re.search(sum_pattern, managed_element)

            if sum_match:
                site_id = sum_match.group(0)
                print(f"DEBUG: Fallback - Extracted Site ID: {site_id}")

                # Cari di LTE Hourly dimana Managed Element mengandung Site ID
                query = f"SELECT * FROM tbl_ltehourly WHERE \"Managed Element\" LIKE '%{site_id}%'"
                print(f"DEBUG: Fallback query: {query}")

                result = self._repository.query(query)
                if result is not None and not result.is_empty():
                    print(f"DEBUG: Fallback SUCCESS - Found {len(result)} records")
                    return result

            # Coba numeric ID
            numeric_pattern = r"\d+"
            numeric_match = re.search(numeric_pattern, managed_element)
            if numeric_match:
                numeric_id = numeric_match.group(0)
                print(f"DEBUG: Fallback - Extracted Numeric ID: {numeric_id}")

                # Cari di berbagai kolom
                queries = [
                    f"SELECT * FROM tbl_ltehourly WHERE \"Managed Element\" LIKE '%{numeric_id}%'",
                    f'SELECT * FROM tbl_ltehourly WHERE "eNodeBId" = {numeric_id}',
                    f'SELECT * FROM tbl_ltehourly WHERE "ManagedElement ID" = {numeric_id}',
                ]

                for q in queries:
                    print(f"DEBUG: Trying fallback query: {q}")
                    result = self._repository.query(q)
                    if result is not None and not result.is_empty():
                        print(f"DEBUG: Fallback query SUCCESS: {q}")
                        return result

            print("DEBUG: All fallback strategies failed")
            return None

        except Exception as e:
            print(f"Error in fallback strategy: {str(e)}")
            return None

    def _query_twoghourly(self, site_names: List[str]) -> Optional[pl.DataFrame]:
        """Query tbl_twoghourly where SITE Name matches mapped Site Names"""
        try:
            if not site_names:
                return None

            # Create IN clause for multiple site names
            site_names_str = "','".join([str(name) for name in site_names])
            query = f"SELECT * FROM tbl_twoghourly WHERE \"SITE Name\" IN ('{site_names_str}')"
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying 2G Hourly: {str(e)}")
            return None
