"""
============================================================================
FILE: src/utils/process/date_normalizer.py
Date Normalizer - Handles mixed date formats from SQLite
============================================================================
"""

import polars as pl
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DateNormalizer:
    """
    Normalizes mixed date formats in SQLite columns
    Handles: 11/8/2025, 2025-11-03, 11/08/2025, etc.
    """

    SUPPORTED_FORMATS = [
        "%m/%d/%Y",  # 11/8/2025, 11/08/2025
        "%Y-%m-%d",  # 2025-11-03
        "%m/%d/%y",  # 11/08/25
        "%d/%m/%Y",  # 08/11/2025 (European)
        "%Y/%m/%d",  # 2025/11/08
        "%d-%m-%Y",  # 08-11-2025
        "%Y%m%d",  # 20251103
    ]

    @staticmethod
    def normalize_date_column(
        df: pl.DataFrame, date_col: str, output_col: str = None
    ) -> pl.DataFrame:
        """
        Normalize a date column with mixed formats

        Args:
            df: Input DataFrame
            date_col: Name of date column to normalize
            output_col: Name of output column (default: date_col + '_normalized')

        Returns:
            DataFrame with normalized date column
        """
        if date_col not in df.columns:
            logger.error(f"Column '{date_col}' not found in DataFrame")
            return df

        if output_col is None:
            output_col = f"{date_col}_normalized"

        # Create list of parsed columns for each format
        parsed_expressions = []

        for idx, date_format in enumerate(DateNormalizer.SUPPORTED_FORMATS):
            try:
                # Parse with strict=False to handle mismatches gracefully
                parsed_expr = (
                    pl.col(date_col)
                    .str.strptime(pl.Date, date_format, strict=False)
                    .alias(f"_temp_parsed_{idx}")
                )
                parsed_expressions.append(parsed_expr)
            except Exception as e:
                logger.debug(f"Could not set up format {date_format}: {e}")
                continue

        if not parsed_expressions:
            logger.warning(f"No valid date formats found for column '{date_col}'")
            return df.with_columns(pl.col(date_col).alias(output_col))

        # Add all parsed columns to DataFrame
        df = df.with_columns(parsed_expressions)

        # Coalesce: take first non-null value
        temp_cols = [f"_temp_parsed_{i}" for i in range(len(parsed_expressions))]
        coalesce_expr = pl.coalesce(temp_cols).alias(output_col)
        df = df.with_columns(coalesce_expr)

        # Drop temporary columns
        df = df.drop(temp_cols)

        # Log statistics
        total_rows = df.height
        parsed_rows = df.filter(pl.col(output_col).is_not_null()).height

        logger.info(
            f"Date normalization: {parsed_rows}/{total_rows} rows parsed successfully"
        )

        if parsed_rows < total_rows:
            # Log some unparsed examples
            unparsed = df.filter(pl.col(output_col).is_null()).select(date_col).head(5)
            logger.warning(
                f"Failed to parse {total_rows - parsed_rows} dates. Examples: {unparsed[date_col].to_list()}"
            )

        return df

    @staticmethod
    def normalize_multiple_columns(
        df: pl.DataFrame, date_columns: List[str]
    ) -> pl.DataFrame:
        """
        Normalize multiple date columns at once

        Args:
            df: Input DataFrame
            date_columns: List of date column names

        Returns:
            DataFrame with all date columns normalized
        """
        for col in date_columns:
            if col in df.columns:
                df = DateNormalizer.normalize_date_column(
                    df, col, output_col=f"{col}_normalized"
                )
            else:
                logger.warning(f"Column '{col}' not found, skipping normalization")

        return df

    @staticmethod
    def get_date_format_distribution(df: pl.DataFrame, date_col: str) -> dict:
        """
        Analyze which date formats are present in the column
        Useful for debugging

        Args:
            df: Input DataFrame
            date_col: Name of date column

        Returns:
            Dictionary with format names and count of matching rows
        """
        if date_col not in df.columns:
            logger.error(f"Column '{date_col}' not found")
            return {}

        distribution = {}

        for date_format in DateNormalizer.SUPPORTED_FORMATS:
            try:
                # Try parsing with this format
                test_df = df.with_columns(
                    pl.col(date_col)
                    .str.strptime(pl.Date, date_format, strict=False)
                    .alias("_test_parse")
                )

                # Count successful parses
                count = test_df.filter(pl.col("_test_parse").is_not_null()).height

                if count > 0:
                    distribution[date_format] = count

            except Exception:
                continue

        return distribution


# Example usage function for testing
def example_usage():
    """Example of how to use DateNormalizer"""

    # Sample data with mixed formats
    df = pl.DataFrame(
        {
            "date_col": [
                "11/8/2025",
                "2025-11-03",
                "11/05/2025",
                "2025-11-10",
                "invalid_date",
            ],
            "value": [1, 2, 3, 4, 5],
        }
    )

    print("Original DataFrame:")
    print(df)

    # Normalize dates
    df = DateNormalizer.normalize_date_column(df, "date_col", "parsed_date")

    print("\nAfter normalization:")
    print(df)

    # Check format distribution
    distribution = DateNormalizer.get_date_format_distribution(df, "date_col")
    print("\nDate format distribution:")
    for fmt, count in distribution.items():
        print(f"  {fmt}: {count} rows")


if __name__ == "__main__":
    example_usage()
