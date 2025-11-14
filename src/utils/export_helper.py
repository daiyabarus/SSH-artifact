"""
============================================================================
FILE: src/utils/export_helper.py
Export Helper - Save raw data for next processing
============================================================================
"""

import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Union, Literal


class ExportHelper:
    """Helper class for exporting data"""

    @staticmethod
    def export_to_csv(
        df: Union[pl.DataFrame, pd.DataFrame],
        filename: str,
        output_dir: str = "exports",
    ) -> str:
        """
        Export DataFrame to CSV

        Args:
            df: DataFrame to export
            filename: Output filename (without extension)
            output_dir: Output directory

        Returns:
            Full path to exported file
        """
        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)

        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.csv"
        filepath = Path(output_dir) / full_filename

        # Export based on type
        if isinstance(df, pl.DataFrame):
            df.write_csv(filepath)
        else:
            df.to_csv(filepath, index=False)

        return str(filepath)

    @staticmethod
    def export_to_excel(
        df: Union[pl.DataFrame, pd.DataFrame],
        filename: str,
        output_dir: str = "exports",
        sheet_name: str = "Data",
    ) -> str:
        """
        Export DataFrame to Excel

        Args:
            df: DataFrame to export
            filename: Output filename (without extension)
            output_dir: Output directory
            sheet_name: Excel sheet name

        Returns:
            Full path to exported file
        """
        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)

        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.xlsx"
        filepath = Path(output_dir) / full_filename

        # Convert to pandas if needed
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()

        # Export to Excel
        df.to_excel(filepath, sheet_name=sheet_name, index=False)

        return str(filepath)

    @staticmethod
    def export_to_parquet(
        df: Union[pl.DataFrame, pd.DataFrame],
        filename: str,
        output_dir: str = "exports",
    ) -> str:
        """
        Export DataFrame to Parquet (efficient for large data)

        Args:
            df: DataFrame to export
            filename: Output filename (without extension)
            output_dir: Output directory

        Returns:
            Full path to exported file
        """
        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)

        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.parquet"
        filepath = Path(output_dir) / full_filename

        # Export based on type
        if isinstance(df, pl.DataFrame):
            df.write_parquet(filepath)
        else:
            df.to_parquet(filepath, index=False)

        return str(filepath)

    @staticmethod
    def get_dataframe_info(df: Union[pl.DataFrame, pd.DataFrame]) -> dict:
        """
        Get DataFrame information

        Args:
            df: DataFrame

        Returns:
            Dict with info: rows, columns, memory usage
        """
        if isinstance(df, pl.DataFrame):
            return {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": df.columns,
                "dtypes": {
                    col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)
                },
                "memory_mb": df.estimated_size() / (1024 * 1024),
            }
        else:
            return {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "dtypes": df.dtypes.to_dict(),
                "memory_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
            }
