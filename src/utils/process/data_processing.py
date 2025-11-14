# ============================================================================
# FILE: src/utils/process/data_processing.py
# ============================================================================

import polars as pl
import pandas as pd
from typing import Union, List, Optional, Literal
from functools import reduce


class DataProcessor:
    """Utility class for data processing operations"""

    @staticmethod
    def merge_columns(
        df: Union[pl.DataFrame, pd.DataFrame],
        columns: List[str],
        new_column: str,
        separator: str = " ",
        drop_original: bool = False,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Merge multiple columns into one

        Args:
            df: DataFrame (Polars or Pandas)
            columns: List of column names to merge
            new_column: Name of the new merged column
            separator: Separator between values
            drop_original: Whether to drop original columns

        Returns:
            DataFrame with merged column
        """
        if isinstance(df, pl.DataFrame):
            # Polars implementation
            df = df.with_columns(
                pl.concat_str(
                    [pl.col(c).cast(pl.Utf8) for c in columns], separator=separator
                ).alias(new_column)
            )
        else:
            # Pandas implementation
            df = df.copy()
            df[new_column] = df[columns].astype(str).agg(separator.join, axis=1)

        if drop_original:
            df = (
                df.drop(columns=columns)
                if isinstance(df, pd.DataFrame)
                else df.drop(columns)
            )

        return df

    @staticmethod
    def combine_columns_conditional(
        df: Union[pl.DataFrame, pd.DataFrame],
        columns: List[str],
        new_column: str,
        method: Literal[
            "coalesce", "sum", "mean", "max", "min", "first_valid"
        ] = "coalesce",
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Combine columns based on condition/method

        Args:
            df: DataFrame
            columns: List of columns to combine
            new_column: Name of new column
            method: Combination method
                - coalesce: First non-null value
                - sum: Sum of all values
                - mean: Average of all values
                - max/min: Max/min value
                - first_valid: First non-null non-zero value

        Returns:
            DataFrame with combined column
        """
        if isinstance(df, pl.DataFrame):
            if method == "coalesce":
                df = df.with_columns(
                    pl.coalesce([pl.col(c) for c in columns]).alias(new_column)
                )
            elif method == "sum":
                df = df.with_columns(
                    pl.sum_horizontal([pl.col(c) for c in columns]).alias(new_column)
                )
            elif method == "mean":
                df = df.with_columns(
                    pl.mean_horizontal([pl.col(c) for c in columns]).alias(new_column)
                )
            elif method == "max":
                df = df.with_columns(
                    pl.max_horizontal([pl.col(c) for c in columns]).alias(new_column)
                )
            elif method == "min":
                df = df.with_columns(
                    pl.min_horizontal([pl.col(c) for c in columns]).alias(new_column)
                )
            elif method == "first_valid":
                # First non-null and non-zero
                expr = pl.coalesce(
                    [
                        pl.when(pl.col(c).is_not_null() & (pl.col(c) != 0)).then(
                            pl.col(c)
                        )
                        for c in columns
                    ]
                )
                df = df.with_columns(expr.alias(new_column))
        else:
            # Pandas implementation
            df = df.copy()
            if method == "coalesce":
                df[new_column] = df[columns].bfill(axis=1).iloc[:, 0]
            elif method == "sum":
                df[new_column] = df[columns].sum(axis=1)
            elif method == "mean":
                df[new_column] = df[columns].mean(axis=1)
            elif method == "max":
                df[new_column] = df[columns].max(axis=1)
            elif method == "min":
                df[new_column] = df[columns].min(axis=1)
            elif method == "first_valid":
                df[new_column] = df[columns].replace(0, pd.NA).bfill(axis=1).iloc[:, 0]

        return df

    @staticmethod
    def calculate_ratio(
        df: Union[pl.DataFrame, pd.DataFrame],
        numerator: str,
        denominator: str,
        new_column: str,
        default_value: float = 0.0,
        multiply_by: float = 1.0,
        round_digits: Optional[int] = None,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Calculate ratio between two columns (numerator/denominator)

        Args:
            df: DataFrame
            numerator: Numerator column name
            denominator: Denominator column name
            new_column: Name of result column
            default_value: Value when denominator is 0
            multiply_by: Multiplier (e.g., 100 for percentage)
            round_digits: Number of decimal places

        Returns:
            DataFrame with calculated ratio
        """
        if isinstance(df, pl.DataFrame):
            expr = (
                pl.when(pl.col(denominator) != 0)
                .then((pl.col(numerator) / pl.col(denominator)) * multiply_by)
                .otherwise(default_value)
            )

            if round_digits is not None:
                expr = expr.round(round_digits)

            df = df.with_columns(expr.alias(new_column))
        else:
            df = df.copy()
            df[new_column] = df.apply(
                lambda row: (row[numerator] / row[denominator] * multiply_by)
                if row[denominator] != 0
                else default_value,
                axis=1,
            )

            if round_digits is not None:
                df[new_column] = df[new_column].round(round_digits)

        return df

    @staticmethod
    def calculate_percentage(
        df: Union[pl.DataFrame, pd.DataFrame],
        numerator: str,
        denominator: str,
        new_column: str,
        round_digits: int = 2,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Calculate percentage (numerator/denominator * 100)

        Args:
            df: DataFrame
            numerator: Numerator column
            denominator: Denominator column
            new_column: Result column name
            round_digits: Decimal places

        Returns:
            DataFrame with percentage column
        """
        return DataProcessor.calculate_ratio(
            df,
            numerator,
            denominator,
            new_column,
            default_value=0.0,
            multiply_by=100.0,
            round_digits=round_digits,
        )

    @staticmethod
    def calculate_growth_rate(
        df: Union[pl.DataFrame, pd.DataFrame],
        current_col: str,
        previous_col: str,
        new_column: str,
        as_percentage: bool = True,
        round_digits: int = 2,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Calculate growth rate: (current - previous) / previous

        Args:
            df: DataFrame
            current_col: Current value column
            previous_col: Previous value column
            new_column: Result column name
            as_percentage: Return as percentage (multiply by 100)
            round_digits: Decimal places

        Returns:
            DataFrame with growth rate
        """
        multiplier = 100.0 if as_percentage else 1.0

        if isinstance(df, pl.DataFrame):
            expr = (
                pl.when(pl.col(previous_col) != 0)
                .then(
                    (
                        (pl.col(current_col) - pl.col(previous_col))
                        / pl.col(previous_col)
                    )
                    * multiplier
                )
                .otherwise(0.0)
            )

            if round_digits is not None:
                expr = expr.round(round_digits)

            df = df.with_columns(expr.alias(new_column))
        else:
            df = df.copy()
            df[new_column] = df.apply(
                lambda row: (
                    (row[current_col] - row[previous_col])
                    / row[previous_col]
                    * multiplier
                )
                if row[previous_col] != 0
                else 0.0,
                axis=1,
            )

            if round_digits is not None:
                df[new_column] = df[new_column].round(round_digits)

        return df

    @staticmethod
    def weighted_average(
        df: Union[pl.DataFrame, pd.DataFrame],
        value_col: str,
        weight_col: str,
        new_column: str,
        group_by: Optional[List[str]] = None,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Calculate weighted average

        Args:
            df: DataFrame
            value_col: Column with values
            weight_col: Column with weights
            new_column: Result column name
            group_by: Columns to group by (optional)

        Returns:
            DataFrame with weighted average
        """
        if isinstance(df, pl.DataFrame):
            expr = (pl.col(value_col) * pl.col(weight_col)).sum() / pl.col(
                weight_col
            ).sum()

            if group_by:
                result = df.group_by(group_by).agg(expr.alias(new_column))
                df = df.join(result, on=group_by, how="left")
            else:
                weighted_avg = df.select(expr).to_numpy()[0, 0]
                df = df.with_columns(pl.lit(weighted_avg).alias(new_column))
        else:
            df = df.copy()
            if group_by:
                df[new_column] = (
                    df.groupby(group_by)
                    .apply(
                        lambda x: (x[value_col] * x[weight_col]).sum()
                        / x[weight_col].sum()
                    )
                    .reset_index(name=new_column)[new_column]
                )
            else:
                weighted_avg = (df[value_col] * df[weight_col]).sum() / df[
                    weight_col
                ].sum()
                df[new_column] = weighted_avg

        return df

    @staticmethod
    def cumulative_sum(
        df: Union[pl.DataFrame, pd.DataFrame],
        column: str,
        new_column: str,
        group_by: Optional[List[str]] = None,
        order_by: Optional[str] = None,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Calculate cumulative sum

        Args:
            df: DataFrame
            column: Column to sum
            new_column: Result column name
            group_by: Columns to group by
            order_by: Column to order by

        Returns:
            DataFrame with cumulative sum
        """
        if isinstance(df, pl.DataFrame):
            if order_by:
                df = df.sort(order_by)

            if group_by:
                df = df.with_columns(
                    pl.col(column).cum_sum().over(group_by).alias(new_column)
                )
            else:
                df = df.with_columns(pl.col(column).cum_sum().alias(new_column))
        else:
            df = df.copy()
            if order_by:
                df = df.sort_values(order_by)

            if group_by:
                df[new_column] = df.groupby(group_by)[column].cumsum()
            else:
                df[new_column] = df[column].cumsum()

        return df

    @staticmethod
    def running_average(
        df: Union[pl.DataFrame, pd.DataFrame],
        column: str,
        new_column: str,
        window_size: int = 3,
        group_by: Optional[List[str]] = None,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Calculate running/moving average

        Args:
            df: DataFrame
            column: Column to average
            new_column: Result column name
            window_size: Window size for moving average
            group_by: Columns to group by

        Returns:
            DataFrame with running average
        """
        if isinstance(df, pl.DataFrame):
            if group_by:
                df = df.with_columns(
                    pl.col(column)
                    .rolling_mean(window_size=window_size)
                    .over(group_by)
                    .alias(new_column)
                )
            else:
                df = df.with_columns(
                    pl.col(column)
                    .rolling_mean(window_size=window_size)
                    .alias(new_column)
                )
        else:
            df = df.copy()
            if group_by:
                df[new_column] = df.groupby(group_by)[column].transform(
                    lambda x: x.rolling(window=window_size, min_periods=1).mean()
                )
            else:
                df[new_column] = (
                    df[column].rolling(window=window_size, min_periods=1).mean()
                )

        return df
