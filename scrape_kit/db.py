import os
import glob
import sqlite3
import threading
from typing import List, Any
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
        self._file_mtime = os.path.exists(db_path) and os.path.getmtime(self.db_path) or 0
        self._create_tables()

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
