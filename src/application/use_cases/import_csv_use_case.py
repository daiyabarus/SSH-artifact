"""
Use case for CSV import operations
Following Single Responsibility Principle
"""

from typing import Tuple
from src.domain.interfaces.i_database_repository import IDatabaseRepository


class ImportCSVUseCase:
    """Handles business logic for CSV import operations"""

    # Define import configurations
    TABLE_CONFIGS = {
        "tbl_twoghourly": {
            "import_type": "append",
            "display_name": "2G Hourly",
            "use_header": False,  # Import by column position, not header name
        },
        "tbl_ltehourly": {
            "import_type": "append",
            "display_name": "LTE Hourly",
            "use_header": False,  # Import by column position, not header name
        },
        "tbl_scot": {
            "import_type": "replace",
            "display_name": "SCOT",
            "use_header": True,  # Use header names
        },
        "tbl_gcell": {
            "import_type": "replace",
            "display_name": "GCell",
            "use_header": True,  # Use header names
        },
        "tbl_timingadvance": {
            "import_type": "replace",
            "display_name": "Timing Advance",
            "use_header": True,  # Use header names
        },
    }

    def __init__(self, repository: IDatabaseRepository):
        self._repository = repository

    def execute(self, csv_path: str, table_name: str) -> Tuple[bool, str]:
        """
        Execute CSV import operation

        Args:
            csv_path: Path to CSV file
            table_name: Target table name

        Returns:
            Tuple of (success: bool, message: str)
        """
        # Validate table name
        if table_name not in self.TABLE_CONFIGS:
            return False, f"Invalid table name: {table_name}"

        # Get configuration
        config = self.TABLE_CONFIGS[table_name]
        import_type = config["import_type"]
        use_header = config["use_header"]

        # Delegate to repository
        return self._repository.import_csv_to_table(
            csv_path=csv_path,
            table_name=table_name,
            import_type=import_type,
            use_header=use_header,
        )

    def get_table_config(self, table_name: str) -> dict:
        """Get configuration for a specific table"""
        return self.TABLE_CONFIGS.get(table_name, {})

    def get_all_table_configs(self) -> dict:
        """Get all table configurations"""
        return self.TABLE_CONFIGS
