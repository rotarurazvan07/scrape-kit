"""Chunk-database merging: staging-table bulk merge, row-by-row streaming, and chunk discovery."""

import glob
import os
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from ..errors import StorageError
from ..logger import get_logger

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


class ChunkMergeMixin:
    """Chunk-merge operations mixed into storage managers.

    Host classes must provide ``conn`` (sqlite3.Connection), ``db_lock``
    (threading.RLock), and ``db_path`` (str).
    """

    conn: sqlite3.Connection
    db_lock: threading.RLock
    db_path: str

    def create_staging_table(self, source_table: str, staging_name: str) -> None:
        """Create a temporary-like staging table with the same schema as source."""
        with self.db_lock:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {_qi(staging_name)}")  # nosec B608 — identifiers quoted via _qi
                self.conn.execute(
                    f"CREATE TABLE {_qi(staging_name)}"  # nosec B608 — identifiers quoted via _qi
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
        """ "Land all .db chunks from input_dir in a staging_<table> table.

        Bulk-fetches data using SQLite ATTACH into ``staging_<table_name>``;
        the destination table itself is untouched unless an explicit
        end_process_query moves rows onward. If end_process_query is
        provided, it must describe an INSERT INTO table_name SELECT ...
        pattern to move data from the staging table to the final destination.

        Args:
            input_dir: Directory containing chunk .db files.
            table_name: Name of the table chunks are read from and staged for.
            end_process_query: Optional SQL moving staged rows into the
                destination table; when it runs, the staging table is dropped.

        Returns:
            A MergeReport summarizing processed and skipped chunks.

        Raises:
            StorageError: If staging creation or the end process query fails.
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
                    try:  # nosec PERF203 — per-file skip is the documented contract
                        logger.debug("Staging merge from %s...", os.path.basename(db_file))
                        self.conn.execute("ATTACH DATABASE ? AS chunk", (db_file,))
                        self.conn.execute(
                            f"INSERT INTO {_qi(staging)}"  # nosec B608 — identifiers quoted via _qi, paths bound
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
                    self.conn.execute(f"DROP TABLE IF EXISTS {_qi(staging)}")  # nosec B608 — identifiers quoted via _qi
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
                rows_since_flush = self._process_chunk(
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
    ) -> int:
        """Process a single chunk file row by row.

        Args:
            db_file: Path to the chunk database.
            table_name: Name of the table to read from.
            row_callback: Function to call for each row.
            flush_callback: Optional function to call at flush intervals.
            read_batch_size: Number of rows to fetch per batch.
            flush_every_rows: Row threshold to trigger flush_callback.
            rows_since_flush: Current count of rows since last flush.
            report: The MergeReport to update (owns the processed-row count).

        Returns:
            The updated count of rows since the last flush.
        """
        logger.info("Merging chunk %s...", os.path.basename(db_file))
        temp_conn: sqlite3.Connection | None = None
        try:
            temp_conn = sqlite3.connect(db_file)
            temp_conn.row_factory = sqlite3.Row
            cursor = temp_conn.execute(f"SELECT * FROM {_qi(table_name)}")  # nosec B608 — identifier quoted via _qi
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
            return rows_since_flush
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
