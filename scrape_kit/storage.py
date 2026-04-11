import glob
import json
import os
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from .errors import StorageError
from .logger import get_logger

logger = get_logger(__name__)


@dataclass
class MergeReport:
    """Report from a database merge operation."""

    processed_chunks: int = 0
    skipped_chunks: int = 0
    processed_rows: int = 0
    errors: list[str] = field(default_factory=list)


def _qi(name: str) -> str:
    """Quote a SQLite identifier safely (works on all Python versions).

    Replaces every double-quote in `name` with two double-quotes and wraps
    the result in double-quotes, which is the SQL standard for identifier
    quoting.  Using a helper instead of an inline f-string avoids the
    nested-same-quote syntax that is only valid in Python 3.12+.
    """
    return '"' + name.replace('"', '""') + '"'


class BaseStorageManager:
    """Core Generic Storage Orchestrator using SQLite."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
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
            obj: The object to serialize. Can be any JSON-serializable object
                 or an object with a __dict__ attribute.

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

    def deserialize_json(self, json_str: str | None) -> Any:
        """Deserialize a JSON string to a Python object.

        Args:
            json_str: The JSON string to parse, or None.

        Returns:
            The parsed Python object, or None if input is None/empty/invalid.
        """
        if json_str is None or json_str == "" or (isinstance(json_str, float) and json_str != json_str):
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
        """Execute multiple inserts/updates in a single transaction for performance."""
        if not params_list:
            return
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
        query = (
            f"CREATE {unique_str} INDEX IF NOT EXISTS {idx_name} ON {_qi(table_name)}({', '.join(_qi(c) for c in columns)})"
        )
        with self.db_lock:
            try:
                self.conn.execute(query)
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Index creation failed on {table_name}: {e}") from e

    def exists(self, table_name: str, column: str, value: Any) -> bool:
        """Check if a value exists in a specific column of a table."""
        query = f"SELECT 1 FROM {_qi(table_name)} WHERE {_qi(column)} = ? LIMIT 1"  # nosec B608
        rows = self.fetch_rows(query, (value,))
        return len(rows) > 0

    def insert(self, table_name: str, data: Mapping[str, Any]) -> None:
        """Insert a single dictionary as a row into the specified table."""
        columns = list(data.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_list = ", ".join(_qi(c) for c in columns)
        query = f"INSERT INTO {_qi(table_name)} ({col_list}) VALUES ({placeholders})"  # nosec B608
        with self.db_lock:
            try:
                logger.debug("Insert into %s: %s", table_name, data)
                self.conn.execute(query, list(data.values()))
                self.conn.commit()
            except sqlite3.Error as e:
                logger.error("Insert failed into %s: %s", table_name, e)
                raise StorageError(f"Insert failed on {table_name}: {e}") from e

    # ── Merging ───────────────────────────────────────────────────────────────

    def create_staging_table(self, source_table: str, staging_name: str) -> None:
        """Create a temporary-like staging table with the same schema as source."""
        with self.db_lock:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {_qi(staging_name)}")  # nosec B608
                self.conn.execute(
                    f"CREATE TABLE {_qi(staging_name)}"  # nosec B608
                    f" AS SELECT * FROM {_qi(source_table)} WHERE 0"
                )
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Staging table creation failed: {e}") from e

    def merge_databases(
        self,
        input_dir: str,
        table_name: str,
        end_process_query: str | None = None,
    ) -> MergeReport:
        """Merge all .db chunks from input_dir into the main database.

        Bulk-fetches data using SQLite ATTACH to a staging table.
        If end_process_query is provided, it must describe an INSERT INTO table_name SELECT ...
        pattern to move data from the staging table to the final destination.
        """
        db_files = self.get_chunk_files(input_dir, skip_file=self.db_path)
        if not db_files:
            return MergeReport()

        staging = f"staging_{table_name}"
        report = MergeReport()

        with self.db_lock:
            try:
                # 1. Prepare Staging
                self.create_staging_table(table_name, staging)

                # 2. Bulk Attach and Insert
                for db_file in db_files:
                    try:  # nosec PERF203
                        logger.debug("Staging merge from %s...", os.path.basename(db_file))
                        self.conn.execute("ATTACH DATABASE ? AS chunk", (db_file,))
                        self.conn.execute(
                            f"INSERT INTO {_qi(staging)}"  # nosec B608
                            f" SELECT * FROM chunk.{_qi(table_name)}"
                        )
                        self.conn.commit()
                        self.conn.execute("DETACH DATABASE chunk")
                        report.processed_chunks += 1
                    except sqlite3.Error as e:
                        logger.error("Skip %s: %s", db_file, e)
                        report.skipped_chunks += 1
                        report.errors.append(f"{db_file}: {e}")
                        try:
                            self.conn.execute("DETACH DATABASE chunk")
                        except sqlite3.Error as detach_e:
                            logger.error("Error detaching after merge failure: %s", detach_e)
                            report.errors.append(f"{db_file} detach: {detach_e}")

                logger.info("Merged %d chunks into %s", report.processed_chunks, staging)

                # 3. Optional Deduplication/Finalization step
                if end_process_query:
                    logger.info("Running end process query...")
                    self.conn.execute(end_process_query)
                    self.conn.commit()
                    self.conn.execute(f"DROP TABLE IF EXISTS {_qi(staging)}")  # nosec B608
                    self.conn.commit()

            except sqlite3.Error as e:
                raise StorageError(f"Merge failed: {e}") from e

        return report

    def merge_row_by_row(
        self,
        input_dir: str,
        table_name: str,
        row_callback: Callable[[sqlite3.Row], None],
        flush_callback: Callable[[], None] | None = None,
        read_batch_size: int = 1000,
        flush_every_rows: int | None = None,
    ) -> MergeReport:
        """Merge chunk databases row by row with callback processing.

        Args:
            input_dir: Directory containing chunk .db files.
            table_name: Name of the table to merge.
            row_callback: Function to call for each row (allows custom processing).
            flush_callback: Optional function to call at flush intervals.
            read_batch_size: Number of rows to fetch at a time from each chunk.
            flush_every_rows: How many rows to process before calling flush_callback.

        Returns:
            A MergeReport summarizing the operation.
        """
        report = MergeReport()
        rows_since_flush = 0
        for db_file in self.get_chunk_files(input_dir, skip_file=self.db_path):
            if not self._is_valid_chunk(db_file):
                report.skipped_chunks += 1
                continue
            try:
                rows_processed, rows_since_flush = self._process_chunk(
                    db_file,
                    table_name,
                    row_callback,
                    flush_callback,
                    read_batch_size,
                    flush_every_rows,
                    rows_since_flush,
                    report,
                )
                report.processed_chunks += 1
            except sqlite3.Error as e:
                self._handle_chunk_error(db_file, e, report)

        return report

    def _is_valid_chunk(self, db_file: str) -> bool:
        """Check if a chunk file is valid for merging.

        Args:
            db_file: Path to the chunk database file.

        Returns:
            True if the file exists and is larger than 100 bytes.
        """
        return os.path.exists(db_file) and os.path.getsize(db_file) > 100

    def _process_chunk(
        self,
        db_file: str,
        table_name: str,
        row_callback: Callable[[sqlite3.Row], None],
        flush_callback: Callable[[], None] | None,
        read_batch_size: int,
        flush_every_rows: int | None,
        rows_since_flush: int,
        report: MergeReport,
    ) -> tuple[int, int]:
        """Process a single chunk file row by row.

        Args:
            db_file: Path to the chunk database.
            table_name: Name of the table to read from.
            row_callback: Function to call for each row.
            flush_callback: Optional function to call at flush intervals.
            read_batch_size: Number of rows to fetch per batch.
            flush_every_rows: Row threshold to trigger flush_callback.
            rows_since_flush: Current count of rows since last flush.
            report: The MergeReport to update.

        Returns:
            A tuple of (total_processed_rows, rows_since_flush).
        """
        logger.info("Merging chunk %s...", os.path.basename(db_file))
        temp_conn: sqlite3.Connection | None = None
        try:
            temp_conn = sqlite3.connect(db_file)
            temp_conn.row_factory = sqlite3.Row
            cursor = temp_conn.execute(f"SELECT * FROM {_qi(table_name)}")  # nosec B608
            while True:
                chunk_rows = cursor.fetchmany(read_batch_size)
                if not chunk_rows:
                    break
                for row in chunk_rows:
                    row_callback(row)
                    report.processed_rows += 1
                    rows_since_flush += 1
                    rows_since_flush = self._maybe_flush(flush_callback, flush_every_rows, rows_since_flush)
            if flush_callback and not flush_every_rows:
                flush_callback()
            return report.processed_rows, rows_since_flush
        finally:
            if temp_conn is not None:
                temp_conn.close()

    def _maybe_flush(
        self,
        flush_callback: Callable[[], None] | None,
        flush_every_rows: int | None,
        rows_since_flush: int,
    ) -> int:
        """Flush the buffer if the row threshold is reached.

        Args:
            flush_callback: Function to call when flushing.
            flush_every_rows: The row threshold.
            rows_since_flush: Current count of rows since last flush.

        Returns:
            Updated rows_since_flush (0 if flushed, otherwise unchanged).
        """
        if flush_callback and flush_every_rows and rows_since_flush >= flush_every_rows:
            flush_callback()
            return 0
        return rows_since_flush

    def _handle_chunk_error(
        self,
        db_file: str,
        error: sqlite3.Error,
        report: MergeReport,
    ) -> None:
        """Handle an error during chunk processing.

        Args:
            db_file: The chunk file that caused the error.
            error: The exception that occurred.
            report: The MergeReport to update.
        """
        logger.error("Skipping chunk %s: %s", db_file, error)
        report.skipped_chunks += 1
        report.errors.append(f"{db_file}: {error}")

    # ── Internals ─────────────────────────────────────────────────────────────

    def _create_tables(self) -> None:
        """Override to create application-specific tables."""

    def reopen_if_changed(self) -> None:
        """Reopen the connection if the underlying file was modified externally."""
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
                logger.warning("Cleanup error on reopen: %s", e)

            logger.info("Database file changed externally, reopening %s", self.db_path)
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self._file_mtime = current_mtime

    @staticmethod
    def get_chunk_files(input_dir: str, skip_file: str | None = None) -> list[str]:
        """Get all .db files in a directory, optionally excluding one.

        Args:
            input_dir: Directory to search for .db files.
            skip_file: Optional file path to exclude from results.

        Returns:
            A list of absolute paths to .db files.
        """
        candidates = [os.path.abspath(f) for f in glob.glob(os.path.join(input_dir, "*.db"))]
        if skip_file:
            skip_abs = os.path.abspath(skip_file)
            return [f for f in candidates if f != skip_abs]
        return candidates

    def flush_and_close(self) -> None:
        """Shut down the connection cleanly."""
        try:
            self.conn.commit()
            with self.db_lock:
                self.conn.close()
        except sqlite3.Error as e:
            raise StorageError(f"Fatal error during shutdown: {e}") from e

    def clear_database(self, table_name: str) -> None:
        """Delete all rows from a table without dropping it."""
        with self.db_lock:
            try:
                self.conn.execute(f"DELETE FROM {_qi(table_name)}")  # nosec B608
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Clearing failed on {table_name}: {e}") from e


class BufferedStorageManager(BaseStorageManager):
    """Storage manager with an in-memory pandas buffer for high-speed lookups."""

    def __init__(self, db_path: str, table_name: str, preserve_schema: bool = True) -> None:
        self._table_name = table_name
        self._buffer: pd.DataFrame | None = None
        self._dirty: bool = False
        self._pending_rows: list[dict[str, Any]] = []
        self._preserve_schema = preserve_schema
        super().__init__(db_path)

    def _materialize_pending_rows(self) -> None:
        """Merge pending rows into the buffer DataFrame."""
        if not self._pending_rows:
            return

        pending_df = pd.DataFrame(self._pending_rows)
        self._pending_rows.clear()

        if self._buffer is None or self._buffer.empty:
            self._buffer = pending_df.reset_index(drop=True)
            return

        pending_df = pending_df.dropna(axis=1, how="all")
        self._buffer = pd.concat([self._buffer, pending_df], ignore_index=True)

    def ensure_buffer(self) -> pd.DataFrame:
        """Lazy-load the entire table into a DataFrame if not already cached."""
        if self._buffer is None:
            self._buffer = self.fetch_dataframe(
                f"SELECT * FROM {_qi(self._table_name)}"  # nosec B608
            )
        self._materialize_pending_rows()
        return self._buffer

    def flush(self) -> None:
        """Write the buffer back to SQLite."""
        if not self._dirty:
            return
        df = self.ensure_buffer()
        with self.db_lock:
            try:
                if self._preserve_schema:
                    # DELETE + append preserves custom schema/indexes/triggers
                    self.conn.execute(f"DELETE FROM {_qi(self._table_name)}")  # nosec B608
                    if not df.empty:
                        df.to_sql(self._table_name, self.conn, if_exists="append", index=False)
                else:
                    # replace drops and recreates the table (standard pandas behavior)
                    df.to_sql(self._table_name, self.conn, if_exists="replace", index=False)
                self.conn.commit()
                self._dirty = False
            except Exception as e:
                raise StorageError(f"Buffer flush failed for {self._table_name}: {e}") from e

    def exists(
        self,
        table_name: str,
        column: str | None = None,
        value: Any | None = None,
    ) -> bool:
        if column is None:
            raise StorageError("exists requires either (table_name, column, value) or legacy (column, value)")
        if value is None:
            # Legacy buffered call shape: exists(column, value)
            column_name = table_name
            target_value = column
            table = self._table_name
        else:
            table = table_name
            column_name = column
            target_value = value

        if table != self._table_name:
            raise StorageError(f"BufferedStorageManager is bound to table '{self._table_name}', got '{table}'")

        for row in self._pending_rows:
            if row.get(column_name) == target_value:
                return True

        df = self.ensure_buffer()
        if df.empty:
            return False
        return target_value in df[column_name].values

    def insert(self, table_name: str | Mapping[str, Any], data: Mapping[str, Any] | None = None) -> None:
        """Insert a row into the buffer.

        Supports two call signatures:
          - insert(data) - legacy buffered style, uses the manager's bound table.
          - insert(table_name, data) - explicit table name.

        Args:
            table_name: Table name (or data dict in legacy mode).
            data: Row data dictionary (or None in legacy mode).

        Raises:
            StorageError: If the table name doesn't match the bound table.
        """
        if data is None:
            # Legacy buffered call shape: insert(data)
            table = self._table_name
            payload = table_name
            if not isinstance(payload, Mapping):
                raise StorageError("insert requires mapping payload")
        else:
            table = str(table_name)
            payload = data

        if table != self._table_name:
            raise StorageError(f"BufferedStorageManager is bound to table '{self._table_name}', got '{table}'")

        self._pending_rows.append(dict(payload))
        self._dirty = True

    def clear_database(self, table_name: str) -> None:
        """Clear SQL table and reset the buffer if it matches."""
        super().clear_database(table_name)
        if table_name == self._table_name:
            self._buffer = None
            self._pending_rows = []
            self._dirty = False

    def reopen_if_changed(self) -> None:
        """Reopen + invalidate buffer so fresh data is loaded on next access."""
        prev_mtime = self._file_mtime
        super().reopen_if_changed()
        if self._file_mtime != prev_mtime:
            self._buffer = None
            self._pending_rows = []
            self._dirty = False

    def close(self) -> None:
        """Close the storage manager, flushing any pending changes."""
        self.flush()
        self.flush_and_close()
