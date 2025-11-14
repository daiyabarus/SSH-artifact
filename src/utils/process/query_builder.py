# ============================================================================
# FILE: src/utils/process/query_builder.py
# ============================================================================

import sqlite3
from contextlib import contextmanager
from typing import List, Tuple, Literal, Dict, Union, Optional
import polars as pl
import pandas as pd


class QueryBuilder:
    """SQL Query Builder for SQLite operations"""

    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize QueryBuilder

        Args:
            db_path: Path to SQLite database (default: in-memory)
        """
        self.db_path = db_path

    @contextmanager
    def get_connection(self):
        """Context manager for database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, query: str, params: Tuple = None) -> List[sqlite3.Row]:
        """
        Execute SQL query

        Args:
            query: SQL query string
            params: Query parameters (for parameterized queries)

        Returns:
            List of rows
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            conn.commit()
            return cursor.fetchall()

    def execute_many(self, query: str, data: List[Tuple]) -> None:
        """
        Execute query with multiple parameter sets

        Args:
            query: SQL query string
            data: List of parameter tuples
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, data)
            conn.commit()

    def to_dataframe(
        self,
        query: str,
        params: Tuple = None,
        engine: Literal["pandas", "polars"] = "polars",
        infer_schema_length: Optional[int] = 10000,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Execute query and return as DataFrame

        Args:
            query: SQL query
            params: Query parameters
            engine: Return as Polars or Pandas DataFrame

        Returns:
            DataFrame with query results
        """
        with self.get_connection() as conn:
            if engine == "polars":
                return pl.read_database(
                    query, conn, infer_schema_length=infer_schema_length
                )
            else:
                return pd.read_sql_query(query, conn, params=params)

    def from_dataframe(
        self,
        df: Union[pl.DataFrame, pd.DataFrame],
        table_name: str,
        if_exists: Literal["fail", "replace", "append"] = "replace",
    ) -> None:
        """
        Write DataFrame to SQLite table

        Args:
            df: DataFrame to write
            table_name: Target table name
            if_exists: Behavior if table exists
        """
        with self.get_connection() as conn:
            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()

            df.to_sql(table_name, conn, if_exists=if_exists, index=False)

    @staticmethod
    def select(
        table: str,
        columns: List[str] = None,
        where: str = None,
        order_by: str = None,
        limit: int = None,
        distinct: bool = False,
    ) -> str:
        """
        Build SELECT query

        Args:
            table: Table name
            columns: List of columns (None = all)
            where: WHERE clause
            order_by: ORDER BY clause
            limit: LIMIT value
            distinct: Use DISTINCT

        Returns:
            SQL query string
        """
        cols = ", ".join(columns) if columns else "*"
        distinct_str = "DISTINCT " if distinct else ""

        query = f"SELECT {distinct_str}{cols} FROM {table}"

        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"

        return query

    @staticmethod
    def insert(table: str, columns: List[str]) -> str:
        """
        Build INSERT query

        Args:
            table: Table name
            columns: List of column names

        Returns:
            SQL query string with placeholders
        """
        cols = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        return f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    @staticmethod
    def update(table: str, columns: List[str], where: str) -> str:
        """
        Build UPDATE query

        Args:
            table: Table name
            columns: Columns to update
            where: WHERE clause

        Returns:
            SQL query string with placeholders
        """
        set_clause = ", ".join([f"{col} = ?" for col in columns])
        return f"UPDATE {table} SET {set_clause} WHERE {where}"

    @staticmethod
    def delete(table: str, where: str) -> str:
        """
        Build DELETE query

        Args:
            table: Table name
            where: WHERE clause

        Returns:
            SQL query string
        """
        return f"DELETE FROM {table} WHERE {where}"

    @staticmethod
    def create_table(
        table: str,
        columns: Dict[str, str],
        primary_key: Optional[str] = None,
        if_not_exists: bool = True,
    ) -> str:
        """
        Build CREATE TABLE query

        Args:
            table: Table name
            columns: Dict of column_name: data_type
            primary_key: Primary key column
            if_not_exists: Add IF NOT EXISTS

        Returns:
            SQL query string
        """
        exists_str = "IF NOT EXISTS " if if_not_exists else ""

        col_defs = []
        for col, dtype in columns.items():
            definition = f"{col} {dtype}"
            if col == primary_key:
                definition += " PRIMARY KEY"
            col_defs.append(definition)

        cols_str = ", ".join(col_defs)
        return f"CREATE TABLE {exists_str}{table} ({cols_str})"

    @staticmethod
    def aggregate(
        table: str,
        aggregations: Dict[str, str],
        group_by: List[str] = None,
        where: str = None,
        having: str = None,
    ) -> str:
        """
        Build aggregation query

        Args:
            table: Table name
            aggregations: Dict of {result_name: "AGG_FUNC(column)"}
            group_by: Columns to group by
            where: WHERE clause
            having: HAVING clause

        Returns:
            SQL query string

        Example:
            aggregations = {
                "total_sales": "SUM(sales)",
                "avg_price": "AVG(price)",
                "count": "COUNT(*)"
            }
        """
        select_parts = []

        if group_by:
            select_parts.extend(group_by)

        for name, expr in aggregations.items():
            select_parts.append(f"{expr} as {name}")

        query = f"SELECT {', '.join(select_parts)} FROM {table}"

        if where:
            query += f" WHERE {where}"
        if group_by:
            query += f" GROUP BY {', '.join(group_by)}"
        if having:
            query += f" HAVING {having}"

        return query

    @staticmethod
    def join(
        left_table: str,
        right_table: str,
        on: str,
        join_type: Literal["INNER", "LEFT", "RIGHT", "FULL"] = "INNER",
        columns: List[str] = None,
    ) -> str:
        """
        Build JOIN query

        Args:
            left_table: Left table name
            right_table: Right table name
            on: JOIN condition
            join_type: Type of join
            columns: Columns to select

        Returns:
            SQL query string
        """
        cols = ", ".join(columns) if columns else "*"
        return f"SELECT {cols} FROM {left_table} {join_type} JOIN {right_table} ON {on}"
