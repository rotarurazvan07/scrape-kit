import glob
import json
import logging
import os
import sqlite3
import threading
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

import pandas as pd

from errors import StorageError

# Configure structured logging
logger = logging.getLogger("scrape_kit.storage")


class BaseStorageManager:
    """Core Generic Storage Orchestrator using SQLite in WAL mode with fast bulk inserts."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.row_factory = sqlite3.Row
        self.db_lock = threading.Lock()

        # Optionally track file modifications (good for hot-swaps)
        self._file_mtime = (
            os.path.getmtime(self.db_path) if os.path.exists(self.db_path) else 0
        )
        self._create_tables()

    # ── Serialization ──────────────────────────────────────────────────────────

    def _serialize_json(self, obj: Any) -> Optional[str]:
        """Generic JSON serializer for complex rows."""
        if obj is None:
            return None
        try:
            if hasattr(obj, "__dict__"):
                return json.dumps(obj.__dict__)
            return json.dumps(obj)
        except (TypeError, ValueError) as e:
            raise StorageError(
                f"Serialization failed for {type(obj).__name__}: {e}"
            ) from e

    def _deserialize_json(self, json_str: Optional[str]) -> Any:
        """Generic JSON deserializer for complex rows."""
        if not json_str:
            return None
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"[BaseDatabaseManager] Deserialization error: {e}")
            return None

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        """Convert a sqlite3.Row object to a standard dictionary."""
        return dict(row)

    # ── Data Fetching ──────────────────────────────────────────────────────────

    def fetch_rows(
        self, query: str, params: Optional[Sequence[Any]] = None
    ) -> List[sqlite3.Row]:
        """Execute a query and return all results as sqlite3.Row objects."""
        params = params or ()
        self.reopen_if_changed()
        with self.db_lock:
            try:
                cursor = self.conn.execute(query, params)
                return cursor.fetchall()
            except sqlite3.Error as e:
                raise StorageError(f"Query [{query}] failed: {e}") from e

    def fetch_dataframe(
        self, query: str, params: Optional[Sequence[Any]] = None
    ) -> pd.DataFrame:
        """Execute a query and return results directly as a pandas DataFrame."""
        params = params or ()
        self.reopen_if_changed()
        with self.db_lock:
            try:
                return pd.read_sql_query(query, self.conn, params=params)
            except Exception as e:
                raise StorageError(f"DataFrame fetch failed for [{query}]: {e}") from e

    def fetch_objs(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        mapper: Optional[Callable[[sqlite3.Row], Any]] = None,
    ) -> List[Any]:
        """Fetch rows and automatically map them to objects using a provided callback."""
        rows = self.fetch_rows(query, params)
        if mapper:
            return [mapper(row) for row in rows]
        return [self._row_to_dict(row) for row in rows]

    # ── Writing & Indexing ──────────────────────────────────────────────────────

    def execute_batch(
        self, query: str, params_list: Sequence[Union[Sequence[Any], Mapping[str, Any]]]
    ):
        """Execute multiple inserts/updates in a single transaction for performance."""
        if not params_list:
            return
        with self.db_lock:
            try:
                self.conn.executemany(query, params_list)
                self.conn.commit()
            except sqlite3.Error as e:
                self.conn.rollback()
                raise StorageError(f"Batch execution failed: {e}") from e

    def create_index(self, table_name: str, columns: List[str], unique: bool = False):
        """Helper to safely create indexes on tables."""
        idx_name = f"idx_{table_name}_{'_'.join(columns)}"
        unique_str = "UNIQUE" if unique else ""
        query = f"CREATE {unique_str} INDEX IF NOT EXISTS {idx_name} ON {table_name}({', '.join(columns)})"
        with self.db_lock:
            try:
                self.conn.execute(query)
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Index creation failed on {table_name}: {e}") from e

    def exists(self, table_name: str, column: str, value: Any) -> bool:
        """Check if a value exists in a specific column of a table."""
        safe_table = f'"{table_name.replace('"', '""')}"'
        safe_column = f'"{column.replace('"', '""')}"'
        query = f"SELECT 1 FROM {safe_table} WHERE {safe_column} = ? LIMIT 1"  # nosec B608
        rows = self.fetch_rows(query, (value,))
        return len(rows) > 0

    def insert(self, table_name: str, data: Mapping[str, Any]):
        """Insert a single dictionary as a row into the specified table."""
        safe_table = f'"{table_name.replace('"', '""')}"'
        columns = list(data.keys())
        placeholders = ["?" for _ in columns]
        safe_cols = [f'"{c.replace('"', '""')}"' for c in columns]

        query = f"INSERT INTO {safe_table} ({', '.join(safe_cols)}) VALUES ({', '.join(placeholders)})"  # nosec B608
        with self.db_lock:
            try:
                self.conn.execute(query, list(data.values()))
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Insert failed on {table_name}: {e}") from e

    # ── Internal Orchestration & Merging ────────────────────────────────────────

    def merge_databases(self, input_dir: str, table_name: str):
        """
        Merge multiple .db chunks into the current database using SQL-based ATTACH strategy.
        Significantly faster than row-by-row merging for bulk data.
        """
        db_files: List[str] = self._get_chunk_files(input_dir, skip_file=self.db_path)
        if not db_files:
            return

        # Escaping identifiers for security
        safe_table: str = f'"{table_name.replace('"', '""')}"'
        staging: str = f"staging_{table_name}"
        safe_staging: str = f'"{staging.replace('"', '""')}"'

        with self.db_lock:
            try:
                # 1. Create staging table from master schema
                self.conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {safe_staging} AS SELECT * FROM {safe_table} WHERE 0"
                )  # nosec B608

                for db_file in db_files:
                    try:
                        self.conn.execute("ATTACH DATABASE ? AS chunk", (db_file,))
                        self.conn.execute(
                            f"INSERT INTO {safe_staging} SELECT * FROM chunk.{safe_table}"
                        )  # nosec B608
                        self.conn.commit()
                        self.conn.execute("DETACH DATABASE chunk")
                    except sqlite3.Error as e:
                        logger.error(f"Skip {db_file}: {e}")
                        try:
                            self.conn.execute("DETACH DATABASE chunk")
                        except sqlite3.Error as detach_e:
                            logger.error(
                                f"Error detaching database after merge failure: {detach_e}"
                            )

                print(
                    f"[BaseStorageManager] Merged {len(db_files)} chunks into {staging}"
                )
            except sqlite3.Error as e:
                raise StorageError(f"Merge failed: {e}") from e

    def merge_row_by_row(
        self,
        input_dir: str,
        table_name: str,
        row_callback: Callable[[sqlite3.Row], None],
        flush_callback: Optional[Callable[[], None]] = None,
    ):
        """
        Merging strategy for logic-heavy tables (like matches needing similarity checks).
        Iterates over rows of every chunk and passes them to the provided callback.
        """
        db_files: List[str] = self._get_chunk_files(input_dir, skip_file=self.db_path)
        if not db_files:
            return

        for db_file in db_files:
            if not (os.path.exists(db_file) and os.path.getsize(db_file) > 100):
                continue

            try:
                # Use a separate temporary connection to avoid locking the main one
                # Use safe table naming
                safe_table = f'"{table_name.replace('"', '""')}"'
                temp_conn = sqlite3.connect(db_file)
                temp_conn.row_factory = sqlite3.Row
                cursor = temp_conn.execute(f"SELECT * FROM {safe_table}")  # nosec B608
                chunk_rows = cursor.fetchall()
                temp_conn.close()

                for row in chunk_rows:
                    row_callback(row)

                # Periodic flush to disk to keep memory stable
                if flush_callback:
                    flush_callback()

            except sqlite3.Error as e:
                logger.error(f"Skipping chunk {db_file} due to error: {e}")
                continue

    # ── Internal Orchestration ──────────────────────────────────────────────────

    def _create_tables(self):
        """Override this method to create application-specific tables"""

    def reopen_if_changed(self):
        """Reopen the database connection if the underlying DB file changed timestamp on disk."""
        try:
            current_mtime = os.path.getmtime(self.db_path)
        except OSError:
            return

        if current_mtime == self._file_mtime:
            return

        with self.db_lock:
            try:
                self.conn.close()
            except Exception as e:
                logger.warning(f"Cleanup error: {e}")

            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.row_factory = sqlite3.Row
            self._file_mtime = current_mtime

    @staticmethod
    def _get_chunk_files(input_dir: str, skip_file: str = None) -> List[str]:
        search_path = os.path.join(input_dir, "*.db")
        candidates = [os.path.abspath(f) for f in glob.glob(search_path)]

        if skip_file:
            skip_abs = os.path.abspath(skip_file)
            return [f for f in candidates if f != skip_abs]

        return candidates

    def flush_and_close(self):
        """Force flush WAL to the main file and safely shut down the connection"""
        try:
            self.conn.commit()
            with self.db_lock:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                self.conn.execute("PRAGMA journal_mode=DELETE;")
                self.conn.close()
        except sqlite3.Error as e:
            raise StorageError(f"Fatal error during database shutdown / cleanup: {e}")

    def clear_database(self, table_name: str):
        """Clear all records from a specified database table generically."""
        safe_table = f'"{table_name.replace('"', '""')}"'
        with self.db_lock:
            try:
                self.conn.execute(f"DELETE FROM {safe_table}")  # nosec B608
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Clearing failed on {table_name}: {e}") from e


class BufferedStorageManager(BaseStorageManager):
    """
    Advanced Storage Manager with an in-memory pandas buffer.
    Perfect for high-speed lookups and batch-flushing on large scrapers.
    """

    def __init__(self, db_path: str, table_name: str):
        self._table_name = table_name
        self._buffer: Optional[pd.DataFrame] = None
        self._dirty: bool = False
        super().__init__(db_path)

    def _ensure_buffer(self) -> pd.DataFrame:
        """Lazy-loads the entire table into a DataFrame if not already present."""
        if self._buffer is not None:
            return self._buffer

        safe_table = f'"{self._table_name.replace('"', '""')}"'
        self._buffer = self.fetch_dataframe(f"SELECT * FROM {safe_table}")  # nosec B608
        return self._buffer

    def flush(self):
        """Writes the current buffer state back to the SQLite table."""
        if not self._dirty or self._buffer is None:
            return

        with self.db_lock:
            try:
                self._buffer.to_sql(
                    self._table_name, self.conn, if_exists="replace", index=False
                )
                self.conn.commit()
                self._dirty = False
            except Exception as e:
                raise StorageError(f"Buffer flush failed: {e}") from e

    def exists(self, column: str, value: Any) -> bool:
        """High-performance in-memory check if a value exists in the buffered table."""
        df = self._ensure_buffer()
        if df.empty:
            return False
        return value in df[column].values

    def insert(self, data: Mapping[str, Any]):
        """Append a record to the in-memory buffer. Mark dirty for later flushing."""
        df = self._ensure_buffer()
        new_row = pd.DataFrame([data])
        self._buffer = pd.concat([df, new_row], ignore_index=True)
        self._dirty = True

    def clear_database(self, table_name: str):
        """Standard clear + buffer reset."""
        super().clear_database(table_name)
        if table_name == self._table_name:
            self._buffer = None
            self._dirty = False

    def reopen_if_changed(self):
        """Standard reopen + buffer clear to force reload of fresh data."""
        prev_mtime = self._file_mtime
        super().reopen_if_changed()
        if self._file_mtime != prev_mtime:
            self._buffer = None
            self._dirty = False

    def close(self):
        self.flush()
        self.flush_and_close()
