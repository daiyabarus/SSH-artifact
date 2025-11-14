# ============================================================================
# FILE: src/utils/process/data_helpers.py
# ============================================================================

from typing import List, Literal, Dict, Union, Optional
import polars as pl
import pandas as pd
from src.utils.process.data_processing import DataProcessor


class DataHelpers:
    """Common data helper functions"""

    @staticmethod
    def convert_types(
        df: Union[pl.DataFrame, pd.DataFrame], type_map: Dict[str, str]
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Convert column types

        Args:
            df: DataFrame
            type_map: Dict of {column: dtype}

        Returns:
            DataFrame with converted types
        """
        if isinstance(df, pl.DataFrame):
            for col, dtype in type_map.items():
                if dtype == "int":
                    df = df.with_columns(pl.col(col).cast(pl.Int64))
                elif dtype == "float":
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
                elif dtype == "str":
                    df = df.with_columns(pl.col(col).cast(pl.Utf8))
                elif dtype == "date":
                    df = df.with_columns(pl.col(col).cast(pl.Date))
        else:
            df = df.copy()
            for col, dtype in type_map.items():
                if dtype == "int":
                    df[col] = df[col].astype("int64")
                elif dtype == "float":
                    df[col] = df[col].astype("float64")
                elif dtype == "str":
                    df[col] = df[col].astype("str")
                elif dtype == "date":
                    df[col] = pd.to_datetime(df[col])

        return df

    @staticmethod
    def fill_nulls(
        df: Union[pl.DataFrame, pd.DataFrame],
        strategy: Literal["zero", "mean", "median", "forward", "backward"] = "zero",
        columns: Optional[List[str]] = None,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Fill null values

        Args:
            df: DataFrame
            strategy: Fill strategy
            columns: Columns to fill (None = all)

        Returns:
            DataFrame with filled values
        """
        cols = columns or df.columns

        if isinstance(df, pl.DataFrame):
            for col in cols:
                if strategy == "zero":
                    df = df.with_columns(pl.col(col).fill_null(0))
                elif strategy == "mean":
                    df = df.with_columns(pl.col(col).fill_null(pl.col(col).mean()))
                elif strategy == "median":
                    df = df.with_columns(pl.col(col).fill_null(pl.col(col).median()))
                elif strategy == "forward":
                    df = df.with_columns(pl.col(col).fill_null(strategy="forward"))
                elif strategy == "backward":
                    df = df.with_columns(pl.col(col).fill_null(strategy="backward"))
        else:
            df = df.copy()
            for col in cols:
                if strategy == "zero":
                    df[col] = df[col].fillna(0)
                elif strategy == "mean":
                    df[col] = df[col].fillna(df[col].mean())
                elif strategy == "median":
                    df[col] = df[col].fillna(df[col].median())
                elif strategy == "forward":
                    df[col] = df[col].fillna(method="ffill")
                elif strategy == "backward":
                    df[col] = df[col].fillna(method="bfill")

        return df

    @staticmethod
    def pivot_table(
        df: Union[pl.DataFrame, pd.DataFrame],
        index: Union[str, List[str]],
        columns: str,
        values: str,
        aggfunc: str = "sum",
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Create pivot table

        Args:
            df: DataFrame
            index: Row index column(s)
            columns: Column to pivot
            values: Values to aggregate
            aggfunc: Aggregation function

        Returns:
            Pivoted DataFrame
        """
        if isinstance(df, pl.DataFrame):
            return df.pivot(
                index=index if isinstance(index, list) else [index],
                columns=columns,
                values=values,
                aggregate_function=aggfunc,
            )
        else:
            return df.pivot_table(
                index=index, columns=columns, values=values, aggfunc=aggfunc
            )


# Quick helper functions
def merge_cols(
    df, columns: List[str], new_col: str, sep: str = " "
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Quick merge columns"""
    return DataProcessor.merge_columns(df, columns, new_col, sep)


def calc_ratio(
    df, num: str, denom: str, result: str, multiply: float = 1.0
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Quick calculate ratio"""
    return DataProcessor.calculate_ratio(df, num, denom, result, multiply_by=multiply)


def calc_pct(
    df, num: str, denom: str, result: str
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Quick calculate percentage"""
    return DataProcessor.calculate_percentage(df, num, denom, result)
