"""Base SQLite storage orchestrator: connection lifecycle, CRUD, serialization, and reopen detection."""

import json
import os
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pandas as pd

from ..errors import StorageError
from ..logger import get_logger
from .merge import ChunkMergeMixin, _qi

logger = get_logger(__name__)


class BaseStorageManager(ChunkMergeMixin):
    """Core Generic Storage Orchestrator using SQLite."""

    def __init__(self, db_path: str) -> None:
        """Open the SQLite database, create tables, and record the file mtime for reload detection."""
        self.db_path = db_path
        try:
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
        except sqlite3.Error as e:
            raise StorageError(f"Failed to open {db_path}: {e}") from e
        self.conn.row_factory = sqlite3.Row
        self.db_lock = threading.RLock()
        logger.info("Initialized StorageManager for %s", db_path)
        self._create_tables()

        # Record mtime AFTER creation tables (initialization-time writes) to avoid immediate reload
        self._file_mtime = os.path.getmtime(self.db_path) if os.path.exists(self.db_path) else 0

    # ── Serialization ─────────────────────────────────────────────────────────

    def serialize_json(self, obj: Any) -> str | None:
        """Serialize an object to a JSON string.

        Args:
            obj: The object to serialize (JSON-serializable, or has a __dict__).

        Returns:
            The JSON string, or None if obj is None.

        Raises:
            StorageError: If serialization fails.
        """
        if obj is None:
            return None
        try:
            if hasattr(obj, "__dict__"):
                return json.dumps(obj.__dict__)
            return json.dumps(obj)
        except (TypeError, ValueError) as e:
            raise StorageError(f"Serialization failed for {type(obj).__name__}: {e}") from e

    def deserialize_json(self, json_str: str | float | None) -> Any:
        """Deserialize a JSON string to a Python object.

        None, empty strings, and any float (incl. NaN from pandas) map to None.

        Args:
            json_str: The JSON string to parse, or a None-mapping input.

        Returns:
            The parsed object; None for invalid JSON (logged as a warning).
        """
        if json_str is None or isinstance(json_str, float) or json_str == "":
            return None
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("Deserialization error: %s", e)
            return None

    def row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        """Convert a sqlite3.Row to a plain dictionary.

        Args:
            row: The sqlite3.Row to convert.

        Returns:
            A dictionary mapping column names to values.
        """
        return dict(row)

    # ── Data Fetching ─────────────────────────────────────────────────────────

    def fetch_rows(
        self,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> list[sqlite3.Row]:
        """Execute a query and return all results as sqlite3.Row objects."""
        params = params or ()
        self.reopen_if_changed()
        with self.db_lock:
            try:
                cursor = self.conn.execute(query, params)
                res = cursor.fetchall()
                logger.debug("Query: %s | Params: %s | Rows: %d", query, params, len(res))
                return res
            except sqlite3.Error as e:
                logger.error("Query failed: %s | Error: %s", query, e)
                raise StorageError(f"Query [{query}] failed: {e}") from e

    def fetch_dataframe(
        self,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a query and return results directly as a pandas DataFrame."""
        params = params or ()
        self.reopen_if_changed()
        with self.db_lock:
            try:
                return pd.read_sql_query(query, self.conn, params=params)
            except Exception as e:
                raise StorageError(f"DataFrame fetch failed for [{query}]: {e}") from e

    def fetch_objects(
        self,
        query: str,
        params: Sequence[Any] | None = None,
        mapper: Callable[[sqlite3.Row], Any] | None = None,
    ) -> list[Any]:
        """Fetch rows and automatically map them to objects using a provided callback."""
        rows = self.fetch_rows(query, params)
        if mapper:
            return [mapper(row) for row in rows]
        return [self.row_to_dict(row) for row in rows]

    # ── Writing & Indexing ────────────────────────────────────────────────────

    def execute_batch(
        self,
        query: str,
        params_list: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> None:
        """Execute multiple inserts/updates in a single transaction for performance.

        Mirrors the read paths by reopening first when the file changed
        externally, so the batch lands on the current file.

        Args:
            query: The SQL statement with placeholders.
            params_list: Sequence of parameter tuples/mappings, one per row.

        Raises:
            StorageError: If the batch fails; the transaction is rolled back.
        """
        if not params_list:
            return
        self.reopen_if_changed()
        with self.db_lock:
            try:
                logger.debug("Batch execution: %s (elements: %d)", query, len(params_list))
                self.conn.executemany(query, params_list)
                self.conn.commit()
            except sqlite3.Error as e:
                logger.error("Batch execution failed: %s", e)
                self.conn.rollback()
                raise StorageError(f"Batch execution failed: {e}") from e

    def create_index(
        self,
        table_name: str,
        columns: list[str],
        unique: bool = False,
    ) -> None:
        """Helper to safely create indexes on tables."""
        idx_name = f"idx_{table_name}_{'_'.join(columns)}"
        unique_str = "UNIQUE" if unique else ""
        index_cols = ", ".join(_qi(c) for c in columns)
        query = f"CREATE {unique_str} INDEX IF NOT EXISTS {_qi(idx_name)} ON {_qi(table_name)}({index_cols})"
        with self.db_lock:
            try:
                self.conn.execute(query)
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Index creation failed on {table_name}: {e}") from e

    def exists(self, table_name: str, column: str, value: Any) -> bool:
        """Check if a value exists in a specific column of a table.

        Error contract: missing table raises StorageError; a missing column
        returns False (SQLite reads unknown quoted identifiers as string
        literals). SQL ``=`` never matches NULL — ``value=None`` returns
        False; the buffered overlay handles NULL lookups.

        Args:
            table_name: Name of the table to search.
            column: Column to compare against.
            value: Value to look for.

        Returns:
            True if at least one row holds the value in the column.

        Raises:
            StorageError: If the table does not exist.
        """
        query = f"SELECT 1 FROM {_qi(table_name)} WHERE {_qi(column)} = ? LIMIT 1"  # nosec B608 — identifiers quoted via _qi, value bound
        rows = self.fetch_rows(query, (value,))
        return len(rows) > 0

    def insert(self, table_name: str, data: Mapping[str, Any]) -> None:
        """Insert a single dictionary as a row into the specified table.

        Mirrors the read paths by reopening first when the file changed
        externally, so the write lands on the current file.

        Args:
            table_name: Name of the table to insert into.
            data: Mapping of column names to values.

        Raises:
            StorageError: If the insert fails (e.g. missing table/column, constraint violation).
        """
        self.reopen_if_changed()
        columns = list(data.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_list = ", ".join(_qi(c) for c in columns)
        query = f"INSERT INTO {_qi(table_name)} ({col_list}) VALUES ({placeholders})"  # nosec B608 — ids quoted via _qi, values bound
        with self.db_lock:
            try:
                logger.debug("Insert into %s: %s", table_name, data)
                self.conn.execute(query, list(data.values()))
                self.conn.commit()
            except sqlite3.Error as e:
                logger.error("Insert failed into %s: %s", table_name, e)
                raise StorageError(f"Insert failed on {table_name}: {e}") from e

    # ── Internals ─────────────────────────────────────────────────────────────

    def _create_tables(self) -> None:
        """Override to create application-specific tables."""

    def reopen_if_changed(self) -> None:
        """Reopen the connection if the underlying file was modified externally.

        A failed close of the stale connection is logged and ignored by
        contract: the connection is replaced regardless, and surfacing the
        cleanup failure would leave the manager bound to a stale file.
        """
        try:
            current_mtime = os.path.getmtime(self.db_path)
        except OSError:
            return

        if current_mtime == self._file_mtime:
            return

        with self.db_lock:
            try:
                self.conn.close()
            except sqlite3.Error as e:
                logger.warning("Cleanup error on reopen: %s", e)

            logger.info("Database file changed externally, reopening %s", self.db_path)
            try:
                self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            except sqlite3.Error as e:
                raise StorageError(f"Failed to open {self.db_path}: {e}") from e
            self.conn.row_factory = sqlite3.Row
            self._file_mtime = current_mtime

    def flush_and_close(self) -> None:
        """Shut down the connection cleanly.

        The final commit and close both run under db_lock, and a failed
        commit still closes the connection, so shutdown cannot leak it.

        Raises:
            StorageError: If the commit or close fails.
        """
        try:
            with self.db_lock:
                self.conn.commit()
                self.conn.close()
        except sqlite3.Error as e:
            try:
                with self.db_lock:
                    self.conn.close()
            except sqlite3.Error as close_e:
                logger.warning("Cleanup error on shutdown: %s", close_e)
            raise StorageError(f"Fatal error during shutdown: {e}") from e

    def close(self) -> None:
        """Alias for :meth:`flush_and_close` so both storage types share one shutdown name."""
        self.flush_and_close()

    def clear_table(self, table_name: str) -> None:
        """Delete all rows from a table without dropping it.

        Args:
            table_name: Name of the table to clear.

        Raises:
            StorageError: If the DELETE fails (e.g. the table does not exist).
        """
        with self.db_lock:
            try:
                self.conn.execute(f"DELETE FROM {_qi(table_name)}")  # nosec B608 — identifier quoted via _qi
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Clearing failed on {table_name}: {e}") from e
