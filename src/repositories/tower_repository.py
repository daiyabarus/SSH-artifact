"""
============================================================================
FILE: src/repositories/tower_repository.py
Tower Repository - Data Access Layer
Single Responsibility: Database queries for tower data
============================================================================
"""

import polars as pl
from src.config.settings import Settings
from src.utils.process.query_builder import QueryBuilder


class TowerRepository:
    """
    Repository for Tower data access
    Follows Single Responsibility and Dependency Inversion Principles
    """

    def __init__(self, db_path: str):
        """
        Initialize repository

        Args:
            db_path: Path to SQLite database
        """
        self._query_builder = QueryBuilder(db_path)
        self._settings = Settings()

    def fetch_tower_ids(self) -> pl.DataFrame:
        """
        Fetch unique tower IDs from database

        Returns:
            Polars DataFrame with TOWERID column
        """
        table = self._settings.TABLE_TA
        column = self._settings.TOWERID_COLUMNS[table]

        query = self._query_builder.select(table=table, columns=[column], distinct=True)

        df = self._query_builder.to_dataframe(query, engine="polars")

        if not df.is_empty():
            df = df.rename({column: "TOWERID"})

            df = df.filter(
                (pl.col("TOWERID").is_not_null()) & (pl.col("TOWERID") != "")
            )

        return df
