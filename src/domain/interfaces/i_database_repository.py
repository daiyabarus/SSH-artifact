"""
Interface for database repository (Dependency Inversion Principle)
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Tuple
import polars as pl


class IDatabaseRepository(ABC):
    """Abstract interface for database operations"""

    @abstractmethod
    def import_csv_to_table(
        self, csv_path: str, table_name: str, import_type: str, use_header: bool = True
    ) -> Tuple[bool, str]:
        """Import CSV file to database table"""
        pass

    @abstractmethod
    def get_table_info(self, table_name: str) -> Optional[dict]:
        """Get information about a table"""
        pass

    @abstractmethod
    def get_all_tables(self) -> List[str]:
        """Get list of all tables"""
        pass

    @abstractmethod
    def query(self, sql: str) -> Optional[pl.DataFrame]:
        """Execute SQL query"""
        pass
