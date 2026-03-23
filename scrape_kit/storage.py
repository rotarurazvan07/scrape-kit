import glob
import json
import logging
import os
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from .errors import StorageError

logger = logging.getLogger("scrape_kit.storage")


@dataclass
class MergeReport:
    processed_chunks: int = 0
    skipped_chunks: int = 0
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
    """Core Generic Storage Orchestrator using SQLite in WAL mode."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.row_factory = sqlite3.Row
        self.db_lock = threading.Lock()
        self._file_mtime = os.path.getmtime(self.db_path) if os.path.exists(self.db_path) else 0
        self._create_tables()

    # ── Serialization ─────────────────────────────────────────────────────────

    def serialize_json(self, obj: Any) -> str | None:
        if obj is None:
            return None
        try:
            if hasattr(obj, "__dict__"):
                return json.dumps(obj.__dict__)
            return json.dumps(obj)
        except (TypeError, ValueError) as e:
            raise StorageError(f"Serialization failed for {type(obj).__name__}: {e}") from e

    def deserialize_json(self, json_str: str | None) -> Any:
        if json_str is None or json_str == "" or (isinstance(json_str, float) and json_str != json_str):
            return None
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("Deserialization error: %s", e)
            return None

    def row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
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
                return cursor.fetchall()
            except sqlite3.Error as e:
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
                self.conn.executemany(query, params_list)
                self.conn.commit()
            except sqlite3.Error as e:
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
                self.conn.execute(query, list(data.values()))
                self.conn.commit()
            except sqlite3.Error as e:
                raise StorageError(f"Insert failed on {table_name}: {e}") from e

    # ── Merging ───────────────────────────────────────────────────────────────

    def merge_databases(self, input_dir: str, table_name: str) -> MergeReport:
        db_files = self.get_chunk_files(input_dir, skip_file=self.db_path)
        if not db_files:
            return MergeReport()

        staging = f"staging_{table_name}"
        report = MergeReport()

        with self.db_lock:
            try:
                self.conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {_qi(staging)}"  # nosec B608
                    f" AS SELECT * FROM {_qi(table_name)} WHERE 0"
                )

                for db_file in db_files:
                    try:
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
            except sqlite3.Error as e:
                raise StorageError(f"Merge failed: {e}") from e

        return report

    def merge_row_by_row(
        self,
        input_dir: str,
        table_name: str,
        row_callback: Callable[[sqlite3.Row], None],
        flush_callback: Callable[[], None] | None = None,
    ) -> MergeReport:
        report = MergeReport()
        for db_file in self.get_chunk_files(input_dir, skip_file=self.db_path):
            if not (os.path.exists(db_file) and os.path.getsize(db_file) > 100):
                report.skipped_chunks += 1
                continue
            try:
                temp_conn = sqlite3.connect(db_file)
                temp_conn.row_factory = sqlite3.Row
                cursor = temp_conn.execute(f"SELECT * FROM {_qi(table_name)}")  # nosec B608
                chunk_rows = cursor.fetchall()
                temp_conn.close()

                for row in chunk_rows:
                    row_callback(row)

                if flush_callback:
                    flush_callback()
                report.processed_chunks += 1

            except sqlite3.Error as e:
                logger.error("Skipping chunk %s: %s", db_file, e)
                report.skipped_chunks += 1
                report.errors.append(f"{db_file}: {e}")
                continue

        return report

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

            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.row_factory = sqlite3.Row
            self._file_mtime = current_mtime

    @staticmethod
    def get_chunk_files(input_dir: str, skip_file: str | None = None) -> list[str]:
        candidates = [os.path.abspath(f) for f in glob.glob(os.path.join(input_dir, "*.db"))]
        if skip_file:
            skip_abs = os.path.abspath(skip_file)
            return [f for f in candidates if f != skip_abs]
        return candidates

    def flush_and_close(self) -> None:
        """Force-flush WAL and shut down the connection cleanly."""
        try:
            self.conn.commit()
            with self.db_lock:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                self.conn.execute("PRAGMA journal_mode=DELETE;")
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

    def __init__(self, db_path: str, table_name: str) -> None:
        self._table_name = table_name
        self._buffer: pd.DataFrame | None = None
        self._dirty: bool = False
        super().__init__(db_path)

    def ensure_buffer(self) -> pd.DataFrame:
        """Lazy-load the entire table into a DataFrame if not already cached."""
        if self._buffer is not None:
            return self._buffer
        self._buffer = self.fetch_dataframe(
            f"SELECT * FROM {_qi(self._table_name)}"  # nosec B608
        )
        return self._buffer

    def flush(self) -> None:
        """Write the buffer back to SQLite."""
        if not self._dirty or self._buffer is None:
            return
        with self.db_lock:
            try:
                self._buffer.to_sql(self._table_name, self.conn, if_exists="replace", index=False)
                self.conn.commit()
                self._dirty = False
            except Exception as e:
                raise StorageError(f"Buffer flush failed: {e}") from e

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
            raise StorageError(
                f"BufferedStorageManager is bound to table '{self._table_name}', got '{table}'"
            )

        df = self.ensure_buffer()
        if df.empty:
            return False
        return target_value in df[column_name].values

    def insert(self, table_name: str | Mapping[str, Any], data: Mapping[str, Any] | None = None) -> None:
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
            raise StorageError(
                f"BufferedStorageManager is bound to table '{self._table_name}', got '{table}'"
            )

        df = self.ensure_buffer()
        self._buffer = pd.concat([df, pd.DataFrame([payload])], ignore_index=True)
        self._dirty = True

    def clear_database(self, table_name: str) -> None:
        """Clear SQL table and reset the buffer if it matches."""
        super().clear_database(table_name)
        if table_name == self._table_name:
            self._buffer = None
            self._dirty = False

    def reopen_if_changed(self) -> None:
        """Reopen + invalidate buffer so fresh data is loaded on next access."""
        prev_mtime = self._file_mtime
        super().reopen_if_changed()
        if self._file_mtime != prev_mtime:
            self._buffer = None
            self._dirty = False

    def close(self) -> None:
        self.flush()
        self.flush_and_close()
