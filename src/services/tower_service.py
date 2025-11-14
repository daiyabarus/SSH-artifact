"""
============================================================================
FILE: src/services/tower_service.py
Tower Service - Handles tower-related business logic
Single Responsibility: Tower ID management
============================================================================
"""

from typing import List
import polars as pl
from src.repositories.tower_repository import TowerRepository


class TowerService:
    """
    Service layer for Tower operations
    Follows Single Responsibility Principle
    """

    def __init__(self, db_path: str):
        """
        Initialize TowerService

        Args:
            db_path: Path to database
        """
        self._repository = TowerRepository(db_path)

    def get_unique_tower_ids(self) -> List[str]:
        """
        Get list of unique tower IDs

        Returns:
            List of unique tower IDs sorted alphabetically
        """
        df = self._repository.fetch_tower_ids()

        if df.is_empty():
            return []

        # Get unique values and sort
        tower_ids = df["TOWERID"].unique().sort().to_list()

        return tower_ids

    def validate_tower_ids(self, tower_ids: List[str]) -> bool:
        """
        Validate if tower IDs exist in database

        Args:
            tower_ids: List of tower IDs to validate

        Returns:
            True if all IDs are valid
        """
        if not tower_ids:
            return False

        valid_ids = set(self.get_unique_tower_ids())
        return all(tid in valid_ids for tid in tower_ids)
