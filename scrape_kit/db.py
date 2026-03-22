import os
import glob
import json
import sqlite3
import threading
import pandas as pd
from typing import List, Any, Optional, Dict, Union, Callable
from .exceptions import DatabaseError

class BaseDatabaseManager:
    """Core Generic Database Orchestrator using SQLite in WAL mode with fast bulk inserts."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.row_factory = sqlite3.Row
        self.db_lock = threading.Lock()

        # Optionally track file modifications (good for hot-swaps)
        self._file_mtime = os.path.getmtime(self.db_path) if os.path.exists(self.db_path) else 0
        self._create_tables()

    # ── Serialization ──────────────────────────────────────────────────────────

    def _serialize_json(self, obj: Any) -> Optional[str]:
        """Generic JSON serializer for complex rows."""
        if obj is None:
            return None
        try:
            if hasattr(obj, '__dict__'):
                return json.dumps(obj.__dict__)
            return json.dumps(obj)
        except (TypeError, ValueError) as e:
            raise DatabaseError(f"Serialization failed for {type(obj).__name__}: {e}")

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

    def fetch_rows(self, query: str, params: Optional[Union[list, tuple]] = None) -> List[sqlite3.Row]:
        """Execute a query and return all results as sqlite3.Row objects."""
        params = params or ()
        self.reopen_if_changed()
        with self.db_lock:
            try:
                cursor = self.conn.execute(query, params)
                return cursor.fetchall()
            except sqlite3.Error as e:
                raise DatabaseError(f"Query [{query}] failed: {e}")

    def fetch_dataframe(self, query: str, params: Optional[Union[list, tuple]] = None) -> pd.DataFrame:
        """Execute a query and return results directly as a pandas DataFrame."""
        params = params or ()
        self.reopen_if_changed()
        with self.db_lock:
            try:
                return pd.read_sql_query(query, self.conn, params=params)
            except Exception as e:
                raise DatabaseError(f"DataFrame fetch failed for [{query}]: {e}")

    def fetch_objs(self, query: str, params: Optional[Union[list, tuple]] = None,
                  mapper: Optional[Callable[[sqlite3.Row], Any]] = None) -> List[Any]:
        """Fetch rows and automatically map them to objects using a provided callback."""
        rows = self.fetch_rows(query, params)
        if mapper:
            return [mapper(row) for row in rows]
        return [self._row_to_dict(row) for row in rows]

    # ── Writing & Indexing ──────────────────────────────────────────────────────

    def execute_batch(self, query: str, params_list: List[Union[list, tuple]]):
        """Execute multiple inserts/updates in a single transaction for performance."""
        if not params_list:
            return
        with self.db_lock:
            try:
                self.conn.executemany(query, params_list)
                self.conn.commit()
            except sqlite3.Error as e:
                self.conn.rollback()
                raise DatabaseError(f"Batch execution failed: {e}")

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
                raise DatabaseError(f"Index creation failed on {table_name}: {e}")

    # ── Internal Orchestration & Merging ────────────────────────────────────────

    def merge_databases(self, input_dir: str, table_name: str, staging_table: Optional[str] = None):
        """
        Generic high-speed merge of multiple .db files using SQL ATTACH.
        This follows the 'staging -> deduplicate -> final' pattern.
        """
        db_files = self._get_chunk_files(input_dir, skip_file=self.db_path)
        if not db_files:
            return

        staging = staging_table or f"staging_{table_name}"

        with self.db_lock:
            try:
                # 1. Create staging table from master schema
                self.conn.execute(f"CREATE TABLE IF NOT EXISTS {staging} AS SELECT * FROM {table_name} WHERE 0")

                # 2. Attach and dump all chunks
                for db_file in db_files:
                    if os.path.exists(db_file) and os.path.getsize(db_file) > 100:
                        self.conn.execute("ATTACH DATABASE ? AS chunk", (db_file,))
                        self.conn.execute(f"INSERT INTO {staging} SELECT * FROM chunk.{table_name}")
                        self.conn.commit()
                        self.conn.execute("DETACH DATABASE chunk")

                print(f"[BaseDatabaseManager] Merged {len(db_files)} chunks into {staging}")
            except sqlite3.Error as e:
                raise DatabaseError(f"Merge failed: {e}")

    def _create_tables(self):
        """Override this method to create application-specific tables"""
        pass

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
            except Exception:
                pass

            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.execute('PRAGMA journal_mode=WAL')
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
            raise DatabaseError(f"Fatal error during database shutdown / cleanup: {e}")

    def clear_database(self, table_name: str):
        """Clear all records from a specified database table generically."""
        with self.db_lock:
            try:
                self.conn.execute(f'DELETE FROM {table_name}')
                self.conn.commit()
            except sqlite3.Error as e:
                raise DatabaseError(f"Clearing failed on {table_name}: {e}")


class BufferedDatabaseManager(BaseDatabaseManager):
    """
    Advanced Database Manager with an in-memory pandas buffer.
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

        self._buffer = self.fetch_dataframe(f"SELECT * FROM {self._table_name}")
        return self._buffer

    def flush(self):
        """Writes the current buffer state back to the SQLite table."""
        if not self._dirty or self._buffer is None:
            return

        with self.db_lock:
            try:
                self._buffer.to_sql(self._table_name, self.conn, if_exists='replace', index=False)
                self.conn.commit()
                self._dirty = False
            except Exception as e:
                raise DatabaseError(f"Buffer flush failed: {e}")

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
