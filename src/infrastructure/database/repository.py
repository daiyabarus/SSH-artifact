"""
Database repository - handles all database operations
Following Repository Pattern for data access abstraction
"""

import sqlite3
from typing import Optional, List, Tuple
from pathlib import Path
import polars as pl
from src.domain.interfaces.i_database_repository import IDatabaseRepository


class DatabaseRepository(IDatabaseRepository):
    """SQLite database repository implementation"""

    def __init__(self, db_path: str = "mydatabase.db"):
        self.db_path = db_path
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Create database and tables if they don't exist"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Note: Tables are created dynamically on first import
            # This allows flexible schema based on CSV structure

            conn.commit()

    def import_csv_to_table(
        self,
        csv_path: str,
        table_name: str,
        import_type: str = "append",
        use_header: bool = True,
    ) -> Tuple[bool, str]:
        """
        Import CSV file to database table with dynamic schema handling

        Args:
            csv_path: Path to CSV file
            table_name: Target table name
            import_type: "append" or "replace"
            use_header: If False, ignore CSV headers and use column positions

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            if use_header:
                # Original behavior: Use header names
                return self._import_with_header(csv_path, table_name, import_type)
            else:
                # New behavior: Ignore headers, use column positions (start from row 2)
                return self._import_without_header(csv_path, table_name, import_type)

        except FileNotFoundError:
            return False, f"File not found: {csv_path}"
        except sqlite3.Error as e:
            return False, f"Database error: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"

    def _import_with_header(
        self, csv_path: str, table_name: str, import_type: str
    ) -> Tuple[bool, str]:
        """Import CSV using header names for column mapping"""
        # Read CSV with Polars - read all as string first to handle formatting
        df = pl.read_csv(
            csv_path,
            infer_schema_length=0,  # Read all as string
            ignore_errors=True,
            truncate_ragged_lines=True,
            null_values=["", "NULL", "null", "N/A", "n/a", "-"],
        )

        if df.is_empty():
            return False, "CSV file is empty"

        # Store original row count
        original_rows = len(df)

        # Clean numeric columns
        df = self._clean_numeric_columns(df)

        # Remove duplicate rows
        df = df.unique()
        final_rows = len(df)

        # Convert to Pandas
        df_pandas = df.to_pandas()

        with sqlite3.connect(self.db_path) as conn:
            if import_type == "replace":
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()

            if_exists_mode = "replace" if import_type == "replace" else "append"

            if import_type == "append":
                table_exists = self._table_exists(conn, table_name)

                if table_exists:
                    existing_cols = self._get_table_columns(conn, table_name)
                    csv_cols = list(df_pandas.columns)

                    # Add missing columns
                    new_cols = set(csv_cols) - set(existing_cols)
                    if new_cols:
                        cursor = conn.cursor()
                        for col in new_cols:
                            col_type = self._infer_column_type(df_pandas[col])
                            try:
                                cursor.execute(
                                    f'ALTER TABLE {table_name} ADD COLUMN "{col}" {col_type}'
                                )
                            except sqlite3.OperationalError:
                                pass
                        conn.commit()

                    # Fill missing columns with None
                    missing_in_csv = set(existing_cols) - set(csv_cols)
                    for col in missing_in_csv:
                        df_pandas[col] = None

                    # Reorder columns
                    all_cols = existing_cols + [
                        c for c in csv_cols if c not in existing_cols
                    ]
                    df_pandas = df_pandas[all_cols]

            # Insert data
            df_pandas.to_sql(
                table_name,
                conn,
                if_exists=if_exists_mode if import_type == "replace" else "append",
                index=False,
            )

        # Success message
        message = f"Successfully imported {final_rows:,} rows"
        if original_rows != final_rows:
            removed = original_rows - final_rows
            message += f" ({removed:,} duplicate rows removed)"

        return True, message

    def _import_without_header(
        self, csv_path: str, table_name: str, import_type: str
    ) -> Tuple[bool, str]:
        """
        Import CSV ignoring headers, using column positions
        Starts reading from row 2 (skips header row)
        """
        # Read CSV starting from row 2 (skip_rows=1)
        df = pl.read_csv(
            csv_path,
            skip_rows=1,  # Skip header row
            has_header=False,  # Don't use first row as header
            infer_schema_length=0,  # Read all as string
            ignore_errors=True,
            truncate_ragged_lines=True,
            null_values=["", "NULL", "null", "N/A", "n/a", "-"],
        )

        if df.is_empty():
            return False, "CSV file is empty (no data rows after header)"

        # Store original row count
        original_rows = len(df)

        # Get or create column names based on table structure
        with sqlite3.connect(self.db_path) as conn:
            table_exists = self._table_exists(conn, table_name)

            if table_exists and import_type == "append":
                # Use existing table column names
                existing_cols = self._get_table_columns(conn, table_name)

                # Adjust dataframe to match existing columns
                num_csv_cols = len(df.columns)
                num_table_cols = len(existing_cols)

                if num_csv_cols < num_table_cols:
                    # CSV has fewer columns, add None columns
                    for i in range(num_csv_cols, num_table_cols):
                        df = df.with_columns(pl.lit(None).alias(f"column_{i + 1}"))
                elif num_csv_cols > num_table_cols:
                    # CSV has more columns, need to add them to table
                    # Use generic names for new columns
                    extra_cols = num_csv_cols - num_table_cols
                    cursor = conn.cursor()
                    for i in range(extra_cols):
                        new_col_name = f"column_{num_table_cols + i + 1}"
                        try:
                            cursor.execute(
                                f'ALTER TABLE {table_name} ADD COLUMN "{new_col_name}" TEXT'
                            )
                        except sqlite3.OperationalError:
                            pass
                    conn.commit()
                    existing_cols = self._get_table_columns(conn, table_name)

                # Rename dataframe columns to match table
                df.columns = existing_cols[: len(df.columns)]

            else:
                # New table or replace mode - generate column names
                num_cols = len(df.columns)

                # Try to read first row of original CSV to get header names
                try:
                    header_df = pl.read_csv(csv_path, n_rows=0)
                    col_names = list(header_df.columns)

                    # Ensure we have enough column names
                    if len(col_names) < num_cols:
                        col_names += [
                            f"column_{i + 1}" for i in range(len(col_names), num_cols)
                        ]

                    df.columns = col_names[:num_cols]
                except:
                    # Fallback to generic names
                    df.columns = [f"column_{i + 1}" for i in range(num_cols)]

        # Clean numeric columns
        df = self._clean_numeric_columns(df)

        # Remove duplicates
        df = df.unique()
        final_rows = len(df)

        # Convert to Pandas
        df_pandas = df.to_pandas()

        # Import to database
        with sqlite3.connect(self.db_path) as conn:
            if import_type == "replace":
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()

            if_exists_mode = "replace" if import_type == "replace" else "append"

            df_pandas.to_sql(table_name, conn, if_exists=if_exists_mode, index=False)

        # Success message
        message = f"Successfully imported {final_rows:,} rows (header ignored, position-based)"
        if original_rows != final_rows:
            removed = original_rows - final_rows
            message += f" ({removed:,} duplicate rows removed)"

        return True, message

    def get_table_info(self, table_name: str) -> Optional[dict]:
        """Get information about a table"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]

                # Get column info
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]

                return {
                    "table_name": table_name,
                    "row_count": row_count,
                    "columns": columns,
                }
        except sqlite3.Error:
            return None

    def get_all_tables(self) -> List[str]:
        """Get list of all tables in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """)
                return [row[0] for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def query(self, sql: str) -> Optional[pl.DataFrame]:
        """Execute SQL query and return results as Polars DataFrame"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pl.read_database(sql, conn)
        except Exception:
            return None

    def _clean_numeric_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean numeric columns by removing thousand separators (comma)
        and converting to proper numeric types
        """
        for col in df.columns:
            # Skip ID and timestamp columns
            if col.lower() in ["id", "imported_at"]:
                continue

            try:
                # Get the column data
                col_data = df[col]

                # Check if column contains comma-separated numbers
                sample_values = [
                    str(val) for val in col_data.head(100) if val is not None
                ]
                has_comma = any("," in val for val in sample_values)

                # Check if values contain letters (alphanumeric like "2300F1", "M2", "M3")
                has_letters = any(
                    any(c.isalpha() for c in str(val)) for val in sample_values if val
                )

                # Check if column appears to be categorical (like Sector: 1,2,3,M2,M3)
                # If unique values are small compared to total, likely categorical
                unique_ratio = len(set(sample_values)) / max(len(sample_values), 1)
                is_likely_categorical = (
                    unique_ratio < 0.1 and len(set(sample_values)) < 20
                )

                # Only convert to numeric if:
                # - No letters present
                # - Not likely categorical
                # - Has comma separators or looks like numbers
                if (
                    not has_letters
                    and not is_likely_categorical
                    and (
                        has_comma
                        or any(
                            self._looks_like_number(val) for val in sample_values[:3]
                        )
                    )
                ):
                    # Clean and convert to numeric
                    df = df.with_columns(
                        [
                            pl.col(col)
                            .str.replace_all(",", "")  # Remove thousand separator
                            .str.replace_all(r"^\s+|\s+$", "")  # Trim whitespace
                            .str.replace("", None)  # Empty string to null
                            .cast(pl.Float64, strict=False)  # Convert to float
                            .alias(col)
                        ]
                    )
            except Exception as e:
                # If conversion fails, keep as string
                print(f"Warning: Could not convert column {col}: {str(e)}")
                continue

        return df

    def _looks_like_number(self, value: str) -> bool:
        """Check if string looks like a number"""
        if not value:
            return False

        # Remove commas and whitespace
        cleaned = value.replace(",", "").strip()

        # Check if it's a number (int or float)
        try:
            float(cleaned)
            return True
        except ValueError:
            return False

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        """Check if table exists in database"""
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """,
            (table_name,),
        )
        return cursor.fetchone() is not None

    def _get_table_columns(
        self, conn: sqlite3.Connection, table_name: str
    ) -> List[str]:
        """Get list of columns in a table"""
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cursor.fetchall()]

    def _infer_column_type(self, series) -> str:
        """Infer SQLite column type from pandas series"""
        import pandas as pd

        # Check if column contains mixed alphanumeric (like "2300F1", "2300F2")
        # This should be treated as TEXT even if it starts with numbers
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            # Check sample values for mixed content
            sample_values = series.dropna().head(100).astype(str)
            if len(sample_values) > 0:
                # If any value contains letters, treat as TEXT
                has_letters = sample_values.str.contains("[a-zA-Z]", regex=True).any()
                if has_letters:
                    return "TEXT"

        # Check data type
        if pd.api.types.is_integer_dtype(series):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(series):
            return "REAL"
        elif pd.api.types.is_bool_dtype(series):
            return "INTEGER"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "TEXT"
        else:
            return "TEXT"
