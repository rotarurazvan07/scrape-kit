"""Buffered quote persistence (TR §4.1: BufferedStorageManager roundtrip).

Satisfies: TR §4.1 - uses the public BufferedStorageManager facade,
queueing rows through insert() and draining them with flush().
"""
from __future__ import annotations

import csv
from typing import Any

from scrape_kit import BufferedStorageManager

TABLE = 'quotes'


class QuoteStore:
    """Thin SQLite-backed store for parsed quotes.

    Satisfies: TR §4.1 storage layer; rows are buffered then flushed once at
    the end of a run so the example demonstrates the real API surface.
    """

    def __init__(self, db_path: str = 'quotes.sqlite', table_name: str = TABLE) -> None:
        self._mgr = BufferedStorageManager(db_path, table_name)
        self._table_name = table_name
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the backing table if absent (TR 4.1 storage layer)."""
        self._mgr.conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{self._table_name}" '
            f'(text TEXT NOT NULL, author TEXT, tags TEXT)'
        )

    def add(self, quotes: list[dict[str, Any]]) -> None:
        """Queue parsed rows for the bound table (TR §4.1 insert pattern)."""
        for quote in quotes:
            self._mgr.insert(self._table_name, quote)

    def flush(self) -> None:
        """Persist queued rows to SQLite."""
        self._mgr.flush()

    def to_csv(self, csv_path: str) -> int:
        """Export the persisted table to CSV; returns the row count written."""
        df = self._mgr.fetch_dataframe(f'SELECT * FROM {self._table_name}')
        df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
        return len(df)

    def close(self) -> None:
        """Drain pending rows and close the connection."""
        self._mgr.close()
