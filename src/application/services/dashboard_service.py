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

        # Query: SCOT (combined SiteID and NCELL SiteID) - query first to use for Timing Advance and GCell
        results["scot"] = self._query_scot_combined(managed_element)

        # Query: LTE Timing Advance - now augmented with NCELL SiteID from SCOT
        results["timingadvance"] = self._query_timingadvance_augmented(
            managed_element, results["scot"]
        )

        # Query: LTE Timing Advance - non-augmented (original)
        results["timingadvance_original"] = self._query_timingadvance_original(managed_element)

        # Query 1: GCell - now augmented with NCELL SiteID from SCOT
        results["gcell"] = self._query_gcell_augmented(managed_element, results["scot"])

        # Query 2: Mapping
        results["mapping"] = self._query_mapping(managed_element)

        # Query 3: LTE Hourly (FIXED eNodeBId mapping)
        results["ltehourly"] = self._query_ltehourly(managed_element)

        # Query 4: 2G Hourly (using mapped Site Names)
        if results["mapping"] is not None and not results["mapping"].is_empty():
            site_names = (
                results["mapping"]["new Site NAME"].to_list()
                if "new Site NAME" in results["mapping"].columns
                else []
            )
            results["twoghourly"] = self._query_twoghourly(site_names)
        else:
            results["twoghourly"] = None

        # Query 6: Merged GCell Coverage
        results["gcell_coverage"] = self._merge_gcell_coverage(results)

        return results

    def _query_timingadvance_augmented(
        self, managed_element: str, scot_data: Optional[pl.DataFrame]
    ) -> Optional[pl.DataFrame]:
        """
        Query tbl_timingadvance where Managed Element = managed_element or Managed Element from NCELL SiteID in SCOT results.
        If SCOT data is None or empty, falls back to original Managed Element-only query.
        """
        try:
            managed_values = [managed_element]

            if scot_data is not None and not scot_data.is_empty():
                if "NCELL SiteID" in scot_data.columns:
                    ncell_site_ids = scot_data["NCELL SiteID"].unique().to_list()
                    # Filter valid NCELL SiteID (not null, not empty, and not equal to managed_element to avoid duplicates)
                    valid_ncell_ids = [
                        str(nid).strip()
                        for nid in ncell_site_ids
                        if nid is not None
                        and str(nid).strip()
                        and str(nid).strip() != managed_element
                        and str(nid).lower() != "nan"
                    ]
                    managed_values.extend(valid_ncell_ids)
                    print(
                        f"DEBUG: Augmented Timing Advance with {len(valid_ncell_ids)} NCELL SiteIDs: {valid_ncell_ids}"
                    )

            if len(managed_values) == 1:
                # Fallback to original query if no NCELL SiteIDs
                query = f"SELECT * FROM tbl_timingadvance WHERE \"Managed Element\" = '{managed_element}'"
            else:
                # Build IN clause for multiple Managed Element values
                managed_str = "','".join(managed_values)
                query = f"SELECT * FROM tbl_timingadvance WHERE \"Managed Element\" IN ('{managed_str}')"

            print(f"DEBUG: Augmented Timing Advance query: {query}")
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying augmented Timing Advance: {str(e)}")
            # Fallback to original query on error
            return self._query_timingadvance_original(managed_element)

    def _query_timingadvance_original(
        self, managed_element: str
    ) -> Optional[pl.DataFrame]:
        """Original Timing Advance query (fallback)"""
        try:
            query = f"SELECT * FROM tbl_timingadvance WHERE \"Managed Element\" = '{managed_element}'"
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying original Timing Advance: {str(e)}")
            return None

    def _query_gcell_augmented(
        self, managed_element: str, scot_data: Optional[pl.DataFrame]
    ) -> Optional[pl.DataFrame]:
        """
        Query tbl_gcell where MSC = Managed Element or MSC from NCELL SiteID in SCOT results.
        If SCOT data is None or empty, falls back to original MSC-only query.
        """
        try:
            msc_values = [managed_element]

            if scot_data is not None and not scot_data.is_empty():
                if "NCELL SiteID" in scot_data.columns:
                    ncell_site_ids = scot_data["NCELL SiteID"].unique().to_list()
                    # Filter valid NCELL SiteID (not null, not empty, and not equal to managed_element to avoid duplicates)
                    valid_ncell_ids = [
                        str(nid).strip()
                        for nid in ncell_site_ids
                        if nid is not None
                        and str(nid).strip()
                        and str(nid).strip() != managed_element
                        and str(nid).lower() != "nan"
                    ]
                    msc_values.extend(valid_ncell_ids)
                    print(
                        f"DEBUG: Augmented GCell with {len(valid_ncell_ids)} NCELL SiteIDs: {valid_ncell_ids}"
                    )

            if len(msc_values) == 1:
                # Fallback to original query if no NCELL SiteIDs
                query = f"SELECT * FROM tbl_gcell WHERE \"MSC\" = '{managed_element}'"
            else:
                # Build IN clause for multiple MSC values
                msc_str = "','".join(msc_values)
                query = f"SELECT * FROM tbl_gcell WHERE \"MSC\" IN ('{msc_str}')"

            print(f"DEBUG: Augmented GCell query: {query}")
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying augmented GCell: {str(e)}")
            # Fallback to original query on error
            return self._query_gcell_original(managed_element)

    def _query_gcell_original(self, managed_element: str) -> Optional[pl.DataFrame]:
        """Original GCell query (fallback)"""
        try:
            query = f"SELECT * FROM tbl_gcell WHERE \"MSC\" = '{managed_element}'"
            return self._repository.query(query)
        except Exception as e:
            print(f"Error querying original GCell: {str(e)}")
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
                print("DEBUG: eNodeBId column not found in GCell data")
                return None

            # Ambil semua eNodeBId yang unik dari GCell
            enodeb_ids = gcell_data["eNodeBId"].unique().to_list()
            print(f"DEBUG: Found eNodeBId from GCell: {enodeb_ids}")

            if not enodeb_ids:
                print("DEBUG: No eNodeBId values found in GCell")
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

    def _merge_gcell_coverage(
        self, results: Dict[str, Optional[pl.DataFrame]]
    ) -> Optional[pl.DataFrame]:
        """
        Merge GCell (anchor) with Timing Advance, SCOT, and Mapping data based on specified criteria.
        Results in a combined table named 'gcell_coverage'.

        Joins:
        - GCell.CellName == TimingAdvance.Eutrancell → Add TimingAdvance.TA90
        - GCell.CellName == SCOT."Cell_PI-1" → Add SCOT."Min of S2S Distance" (coalesce to 0 if null/blank)
        - Mapping is included but join criteria not specified; assuming left join on a common key if available
          (e.g., GCell.MSC == Mapping."New Tower ID" – adjust as needed)

        Returns: Polars DataFrame with merged data
        """
        try:
            df_gcell = results.get("gcell")
            df_timing = results.get("timingadvance")
            df_scot = results.get("scot")
            # df_mapping = results.get("mapping")

            if df_gcell is None or df_gcell.is_empty():
                print("DEBUG: No GCell data available for merge")
                return None

            # Start with GCell as anchor, ensure CellName is string
            df_coverage = df_gcell.clone().with_columns(
                pl.col("CellName").cast(pl.Utf8)
            )

            # Join with Timing Advance: GCell.CellName == TimingAdvance.Eutrancell, add TA90 (cast to float)
            if df_timing is not None and not df_timing.is_empty():
                if (
                    "CellName" in df_coverage.columns
                    and "Eutrancell" in df_timing.columns
                    and "TA90" in df_timing.columns
                ):
                    timing_join = df_timing.select(
                        [
                            pl.col("Eutrancell").cast(pl.Utf8).alias("CellName"),
                            pl.col("TA90").cast(pl.Float64, strict=False).alias("TA90"),
                        ]
                    )
                    df_coverage = df_coverage.join(
                        timing_join, on="CellName", how="left"
                    )
                    print(f"DEBUG: Merged {len(df_timing)} Timing Advance records")
                else:
                    print("DEBUG: Required columns missing for Timing Advance join")

            # Join with SCOT: GCell.CellName == SCOT."Cell_PI-1", add "Min of S2S Distance" (coalesce to 0)
            if df_scot is not None and not df_scot.is_empty():
                if (
                    "CellName" in df_coverage.columns
                    and "Cell_PI-1" in df_scot.columns
                    and "Min of S2S Distance" in df_scot.columns
                ):
                    # Rename join key for consistency, cast types
                    df_scot_join = df_scot.select(
                        [
                            pl.col("Cell_PI-1").cast(pl.Utf8).alias("CellName"),
                            pl.col("Min of S2S Distance").cast(pl.Float64),
                            pl.col("New FINAL Remark COSTv3.0T").alias("SCOT Remark"),
                            pl.col("NCELL SiteID").alias("1st Tier"),
                        ]
                    )
                    df_coverage = df_coverage.join(
                        df_scot_join, on="CellName", how="left"
                    )
                    # Coalesce "Min of S2S Distance" to 0 if null
                    if "Min of S2S Distance" in df_coverage.columns:
                        df_coverage = df_coverage.with_columns(
                            pl.coalesce(
                                [pl.col("Min of S2S Distance"), pl.lit(0.0)]
                            ).alias("Min of S2S Distance")
                        )
                    print(f"DEBUG: Merged {len(df_scot)} SCOT records")
                else:
                    print("DEBUG: Required columns missing for SCOT join")

            print(
                f"DEBUG: Final gcell_coverage has {len(df_coverage)} records and {len(df_coverage.columns)} columns"
            )
            return df_coverage

        except Exception as e:
            print(f"Error merging gcell_coverage: {str(e)}")
            return None