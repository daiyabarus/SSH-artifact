"""
============================================================================
FILE: src/config/settings.py
============================================================================
"""

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class Settings:
    """Application settings (Immutable)"""

    # Database
    DB_PATH: str = "newdatabase.db"

    # Date defaults
    DEFAULT_START_OFFSET: int = 15
    DEFAULT_END_OFFSET: int = 1

    # Query timeout
    QUERY_TIMEOUT: int = 30

    # Cache settings
    CACHE_TTL: int = 3600  # 1 hour

    # Table names
    TABLE_TA: str = "tbl_newta"
    TABLE_KQI: str = "tbl_newkqi"
    TABLE_BH: str = "tbl_newbh"
    TABLE_WD: str = "tbl_newwd"
    TABLE_TWOG: str = "tbl_newtwog"
    TABLE_SCOT: str = "tbl_newscot"
    TABLE_GCELL: str = "tbl_newgcell"
    TABLE_TWOG_HOURLY: str = "tbl_newtwoghourly"
    TABLE_LTE_HOURLY: str = "tbl_newltehourly"

    # Column mappings for TOWERID
    TOWERID_COLUMNS: dict = None

    # Date columns for filtering
    DATE_COLUMNS: dict = None

    def __post_init__(self):
        """Initialize computed attributes"""
        # TOWERID column mappings
        object.__setattr__(
            self,
            "TOWERID_COLUMNS",
            {
                self.TABLE_TA: "newta_managed_element",
                self.TABLE_KQI: "newkqi_swe_l6",
                self.TABLE_BH: "newbh_enodeb_fdd_msc",
                self.TABLE_WD: "newwd_enodeb_fdd_msc",
                self.TABLE_TWOG: "newtwog_towerid",
                self.TABLE_SCOT: ["newscot_site", "newscot_target_site"],
                self.TABLE_GCELL: "new_tower_id",
                self.TABLE_TWOG_HOURLY: "twog_hour_towerid",
                self.TABLE_LTE_HOURLY: "lte_hour_me_name",
            },
        )

        # Date column mappings
        object.__setattr__(
            self,
            "DATE_COLUMNS",
            {
                self.TABLE_TA: "newta_date",
                self.TABLE_KQI: "newkqi_date",
                self.TABLE_BH: "newbh_date",
                self.TABLE_WD: "newwd_date",
                self.TABLE_TWOG: "newtwog_date",
                self.TABLE_TWOG_HOURLY: "twog_hour_date",
                self.TABLE_LTE_HOURLY: "lte_hour_begin_time",
            },
        )
