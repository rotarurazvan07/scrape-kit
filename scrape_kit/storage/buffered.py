"""Pandas-buffered storage manager for high-speed deduplication lookups on one bound table."""

from collections.abc import Mapping
from typing import Any

import pandas as pd

from ..errors import StorageError
from .base import BaseStorageManager
from .merge import _qi


class BufferedStorageManager(BaseStorageManager):
    """Storage manager with an in-memory pandas buffer for high-speed lookups."""

    def __init__(self, db_path: str, table_name: str, preserve_schema: bool = True) -> None:
        """Bind to one table with a lazily-built pandas buffer and a pending-row queue."""
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
                f"SELECT * FROM {_qi(self._table_name)}"  # nosec B608 — identifier quoted via _qi
            )
        self._materialize_pending_rows()
        return self._buffer

    def flush(self) -> None:
        """Write the buffer and pending rows back to SQLite.
        Runs entirely under db_lock (reentrant, so ensure_buffer's fetch
        re-acquires it), making the flush atomic against insert()/exists().

        Raises:
            StorageError: If the write fails; the dirty flag stays set so a
                retry re-attempts the full buffer.
        """
        if not self._dirty:
            return
        with self.db_lock:
            try:
                df = self.ensure_buffer()
                if self._preserve_schema:
                    # DELETE + append preserves custom schema/indexes/triggers
                    self.conn.execute(f"DELETE FROM {_qi(self._table_name)}")  # nosec B608 — identifier quoted via _qi
                    if not df.empty:
                        df.to_sql(self._table_name, self.conn, if_exists="append", index=False)
                else:
                    # replace drops and recreates the table (standard pandas behavior)
                    df.to_sql(self._table_name, self.conn, if_exists="replace", index=False)
                self.conn.commit()
                self._dirty = False
            except Exception as e:
                raise StorageError(f"Buffer flush failed for {self._table_name}: {e}") from e

    def exists(self, table_name: str, column: str, value: Any) -> bool:
        """Check if a value exists in a column of the bound table.

        Pending (unflushed) rows are checked first, then the pandas buffer.
        None is a legitimate lookup value in this overlay: it matches rows
        whose column is None (a plain SQL ``= NULL`` would never match).

        Args:
            table_name: Must match the manager's bound table.
            column: Column name to inspect.
            value: Value to look for; None matches NULL/absent columns.

        Returns:
            True if any pending or buffered row holds the value in the column.

        Raises:
            StorageError: If table_name does not match the bound table.
        """
        if table_name != self._table_name:
            raise StorageError(f"BufferedStorageManager is bound to table '{self._table_name}', got '{table_name}'")

        with self.db_lock:
            for row in self._pending_rows:
                if row.get(column) == value:
                    return True

            df = self.ensure_buffer()
            if df.empty:
                return False
            if column not in df.columns:
                return False
            if value is None:
                return bool(df[column].isna().any())
            return bool(value in df[column].values)

    def insert(self, table_name: str, data: Mapping[str, Any]) -> None:
        """Queue a single row for the bound table's buffer.

        The row is persisted on the next flush(), not immediately.

        Args:
            table_name: Must match the manager's bound table.
            data: Mapping of column names to values.

        Raises:
            StorageError: If table_name does not match the bound table.
            ValueError: If data is not a mapping (call-time misuse).
        """
        if table_name != self._table_name:
            raise StorageError(f"BufferedStorageManager is bound to table '{self._table_name}', got '{table_name}'")
        if not isinstance(data, Mapping):
            raise ValueError("insert requires a mapping payload")
        with self.db_lock:
            self._pending_rows.append(dict(data))
            self._dirty = True

    def clear_table(self, table_name: str) -> None:
        """Clear the SQL table and reset the buffer if it matches the bound table.

        Args:
            table_name: Name of the table to clear.

        Raises:
            StorageError: If the DELETE fails (e.g. the table does not exist).
        """
        with self.db_lock:
            super().clear_table(table_name)
            if table_name == self._table_name:
                self._buffer = None
                self._pending_rows = []
                self._dirty = False

    def reopen_if_changed(self) -> None:
        """Invalidate only the disk-backed buffer on external file changes.

        Pending rows and the dirty flag are carried across the reopen: they
        are not backed by the old file, so dropping them would lose data.
        They are persisted by the next flush().
        """
        prev_mtime = self._file_mtime
        with self.db_lock:
            super().reopen_if_changed()
            if self._file_mtime != prev_mtime:
                self._buffer = None

    def flush_and_close(self) -> None:
        """Drain pending rows, then commit and close the connection.

        Raises:
            StorageError: If the buffer flush or the shutdown fails.
        """
        self.flush()
        super().flush_and_close()

    def close(self) -> None:
        """Close the storage manager, persisting any pending changes first."""
        self.flush_and_close()
