"""
============================================================================
FILE: src/domain/aggregators/kpi_aggregator.py
KPI Aggregator - Handles KPI calculations
Single Responsibility: KPI computation logic
Open/Closed: Open for extension via new calculation methods
============================================================================
"""

import polars as pl
import logging
from src.utils.process.data_processing import DataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KPIAggregator:
    """
    Aggregates and calculates KPIs from raw data
    Follows Single Responsibility and Open/Closed Principles
    """

    def __init__(self):
        self._processor = DataProcessor()

    def _safe_calculate(
        self,
        df: pl.DataFrame,
        num_col: str,
        den_col: str,
        result_col: str,
        is_pct: bool = True,
    ):
        """Helper: Calculate ratio/percentage safely (check columns)"""
        if num_col not in df.columns or den_col not in df.columns:
            logger.warning(f"Missing columns {num_col}/{den_col} for {result_col}")
            return df.with_columns(pl.lit(0.0).alias(result_col))

        if is_pct:
            return self._processor.calculate_percentage(
                df, num_col, den_col, result_col
            )
        else:
            return self._processor.calculate_ratio(
                df, num_col, den_col, result_col, multiply_by=1.0
            )

    def calculate_bh_kpis(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate Busy Hour KPIs

        Args:
            df: Raw BH data

        Returns:
            DataFrame with calculated KPIs
        """
        if df.is_empty():
            return df

        logger.info(f"Calculating BH KPIs for {len(df)} rows")

        # Downlink User Throughput
        df = self._safe_calculate(
            df,
            "newbh_cell_downlink_user_throughput_num",
            "newbh_cell_downlink_user_throughput_den",
            "dl_user_throughput_kbps",
            is_pct=False,
        )

        # Uplink User Throughput
        df = self._safe_calculate(
            df,
            "newbh_cell_uplink_user_throughput_num",
            "newbh_cell_uplink_user_throughput_den",
            "ul_user_throughput_kbps",
            is_pct=False,
        )

        # PDCP Cell Throughput DL
        df = self._safe_calculate(
            df,
            "newbh_pdcp_cell_throughput_dl_num",
            "newbh_pdcp_cell_throughput_dl_denom",
            "pdcp_cell_throughput_dl_kbps",
            is_pct=False,
        )

        # PDCP Cell Throughput UL
        df = self._safe_calculate(
            df,
            "newbh_pdcp_cell_throughput_ul_num",
            "newbh_pdcp_cell_throughput_ul_den",
            "pdcp_cell_throughput_ul_kbps",
            is_pct=False,
        )

        # VoLTE DL Packet Loss Ratio
        df = self._safe_calculate(
            df,
            "newbh_cell_volte_dl_packet_loss_ratio_num",
            "newbh_cell_volte_dl_packet_loss_ratio_den",
            "volte_dl_packet_loss_pct",
        )

        # VoLTE UL Packet Loss Ratio
        df = self._safe_calculate(
            df,
            "newbh_cell_volte_ul_packet_loss_ratio_num",
            "newbh_cell_volte_ul_packet_loss_ratio_den",
            "volte_ul_packet_loss_pct",
        )

        # Session Setup Success Rate
        df = self._safe_calculate(
            df,
            "newbh_cell_session_setup_success_rate_a_num",
            "newbh_cell_session_setup_success_rate_a_den",
            "session_setup_sr_pct",
        )

        # ERAB Drop Rate
        df = self._safe_calculate(
            df,
            "newbh_lerabdroprate_num",
            "newbh_lerabdroprate_den",
            "erab_drop_rate_pct",
        )

        # Handover Success Rate
        df = self._safe_calculate(
            df,
            "newbh_cell_handover_success_rate_inter_and_intra_frequency_num",
            "newbh_cell_handover_success_rate_inter_and_intra_frequency_den",
            "handover_sr_pct",
        )

        # Average CQI
        df = self._safe_calculate(
            df,
            "newbh_cell_average_cqi_num",
            "newbh_cell_average_cqi_den",
            "avg_cqi",
            is_pct=False,
        )

        # QPSK Rate
        df = self._safe_calculate(
            df, "newbh_cell_qpsk_rate_num", "newbh_cell_qpsk_rate_den", "qpsk_rate_pct"
        )

        # Spectral Efficiency DL
        df = self._safe_calculate(
            df,
            "newbh_spectral_efficiency_dl_num",
            "newbh_spectral_efficiency_dl_den",
            "spectral_efficiency_dl",
            is_pct=False,
        )

        # Packet Latency
        df = self._safe_calculate(
            df,
            "newbh_cell_packet_latency_num",
            "newbh_cell_packet_latency_den",
            "packet_latency_ms",
            is_pct=False,
        )

        # Average TA
        df = self._safe_calculate(
            df,
            "newbh_average_ta_num_mpi",
            "newbh_average_ta_den_mpi",
            "avg_ta_m",
            is_pct=False,
        )

        return df

    def calculate_wd_kpis(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate Weekday KPIs (same as BH structure)

        Args:
            df: Raw WD data

        Returns:
            DataFrame with calculated KPIs
        """
        if df.is_empty():
            return df

        logger.info(f"Calculating WD KPIs for {len(df)} rows")

        # Downlink User Throughput
        df = self._safe_calculate(
            df,
            "newwd_cell_downlink_user_throughput_num",
            "newwd_cell_downlink_user_throughput_den",
            "dl_user_throughput_kbps",
            is_pct=False,
        )

        # Uplink User Throughput
        df = self._safe_calculate(
            df,
            "newwd_cell_uplink_user_throughput_num",
            "newwd_cell_uplink_user_throughput_den",
            "ul_user_throughput_kbps",
            is_pct=False,
        )

        # ERAB Drop Rate
        df = self._safe_calculate(
            df,
            "newwd_lerabdroprate_num",
            "newwd_lerabdroprate_den",
            "erab_drop_rate_pct",
        )

        # Handover Success Rate
        df = self._safe_calculate(
            df,
            "newwd_cell_handover_success_rate_inter_and_intra_frequency_num",
            "newwd_cell_handover_success_rate_inter_and_intra_frequency_den",
            "handover_sr_pct",
        )

        return df

    def calculate_twog_kpis(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate 2G KPIs

        Args:
            df: Raw 2G data

        Returns:
            DataFrame with calculated KPIs
        """
        if df.is_empty():
            return df

        logger.info(f"Calculating 2G KPIs for {len(df)} rows")

        # Call Setup Success Rate
        df = self._safe_calculate(
            df,
            "newtwog_cell_call_setup_success_rate_num",
            "newtwog_cell_call_setup_success_rate_den",
            "call_setup_sr_pct",
        )

        # SDCCH Success Rate
        df = self._safe_calculate(
            df,
            "newtwog_cell_sdcch_success_rate_num",
            "newtwog_cell_sdcch_success_rate_den",
            "sdcch_sr_pct",
        )

        # Perceive Drop Rate
        df = self._safe_calculate(
            df,
            "newtwog_cell_perceive_drop_rate_num",
            "newtwog_cell_perceive_drop_rate_den",
            "perceive_drop_rate_pct",
        )

        # Average Voice Traffic Volume (direct avg, no ratio)
        if "newtwog_voice_traffic_volume_kpi" in df.columns:
            df = df.with_columns(
                pl.col("newtwog_voice_traffic_volume_kpi")
                .round(4)
                .alias("avg_voice_traffic_kpi")
            )

        # Average TCH Traffic (direct avg)
        if "newtwog_tch_traffic_kpi" in df.columns:
            df = df.with_columns(
                pl.col("newtwog_tch_traffic_kpi").round(2).alias("avg_tch_traffic_kpi")
            )

        return df
