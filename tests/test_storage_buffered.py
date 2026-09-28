"""BufferedStorageManager (issue #10 split)."""

import os
import sqlite3
from unittest.mock import MagicMock

import pytest
from conftest import BUFFERED_DB_NAME, ITEMS_TABLE, create_items_schema

from scrape_kit.errors import StorageError
from scrape_kit.storage import BufferedStorageManager

pytestmark = pytest.mark.p0


# ── BufferedStorageManager — exists ──────────────────────────────────────────


class TestBufferedExists:
    """BufferedStorageManager.exists resolves through the pandas buffer."""

    def test_normal_found_in_buffer(self, buffered_db):
        assert buffered_db.exists("name", "alpha") is True

    def test_normal_not_found_returns_false(self, buffered_db):
        assert buffered_db.exists("name", "omega") is False

    def test_edge_empty_buffer_returns_false(self, tmp_path):
        conn = sqlite3.connect(str(tmp_path / "empty.db"))
        create_items_schema(conn)
        conn.commit()
        conn.close()
        manager = BufferedStorageManager(str(tmp_path / "empty.db"), ITEMS_TABLE)
        assert manager.exists("name", "anything") is False
        manager.close()

    def test_normal_exists_after_insert_without_flush(self, buffered_db):
        buffered_db.insert({"id": 99, "name": "in_memory", "value": "yes"})
        assert buffered_db.exists("name", "in_memory") is True


# ── BufferedStorageManager — insert ──────────────────────────────────────────


class TestBufferedInsert:
    """BufferedStorageManager.insert appends to the in-memory buffer."""

    def test_normal_insert_grows_buffer(self, buffered_db):
        before = len(buffered_db.ensure_buffer())
        buffered_db.insert({"id": 3, "name": "gamma", "value": "g"})
        assert len(buffered_db.ensure_buffer()) == before + 1
        assert buffered_db._pending_rows == []

    def test_normal_insert_marks_buffer_dirty(self, buffered_db):
        assert buffered_db._dirty is False
        buffered_db.insert({"id": 3, "name": "new", "value": "n"})
        assert buffered_db._dirty is True

    def test_edge_multiple_inserts_all_in_buffer(self, buffered_db):
        for i in range(10):
            buffered_db.insert({"id": 100 + i, "name": f"item_{i}", "value": str(i)})
        assert len(buffered_db.ensure_buffer()) == 12  # 2 pre-existing + 10

    def test_normal_flush_writes_inserted_rows_to_disk(self, buffered_db):
        buffered_db.insert({"id": 3, "name": "flushed", "value": "f"})
        buffered_db.flush()
        rows = buffered_db.fetch_rows("SELECT * FROM items WHERE name = ?", ("flushed",))
        assert len(rows) == 1


# ── BufferedStorageManager — flush ────────────────────────────────────────────


class TestBufferedFlush:
    """flush persists dirty buffers and clears the dirty flag."""

    def test_normal_dirty_buffer_written_to_db(self, buffered_db):
        buffered_db.insert({"id": 99, "name": "write_me", "value": "v"})
        buffered_db.flush()
        df = buffered_db.fetch_dataframe("SELECT * FROM items")
        assert any(df["name"] == "write_me")

    def test_edge_flush_when_not_dirty_does_not_overwrite(self, buffered_db):
        buffered_db._dirty = False
        buffered_db.flush()  # should be a no-op
        rows = buffered_db.fetch_rows("SELECT * FROM items")
        assert len(rows) == 2  # original rows untouched

    def test_edge_flush_when_buffer_none_persists_pending(self, buffered_db):
        buffered_db._buffer = None
        buffered_db._pending_rows = [{"id": 55, "name": "late", "value": "z"}]
        buffered_db._dirty = True
        buffered_db.flush()
        rows = buffered_db.fetch_rows("SELECT * FROM items WHERE name = ?", ("late",))
        assert len(rows) == 1

    def test_normal_flush_clears_dirty_flag(self, buffered_db):
        buffered_db.insert({"id": 3, "name": "x", "value": "y"})
        assert buffered_db._dirty is True
        buffered_db.flush()
        assert buffered_db._dirty is False


# ── BufferedStorageManager — clear_database ───────────────────────────────────


class TestBufferedClearDatabase:
    """clear_database resets SQL and the bound-table buffer."""

    def test_normal_clears_sql_and_resets_buffer(self, buffered_db):
        buffered_db.clear_database(ITEMS_TABLE)
        assert buffered_db._buffer is None
        assert buffered_db._dirty is False
        rows = buffered_db.fetch_rows("SELECT * FROM items")
        assert rows == []

    def test_edge_clearing_different_table_keeps_buffer_intact(self, buffered_db):
        # Create a second table
        buffered_db.conn.execute("CREATE TABLE other (x INTEGER)")
        buffered_db.conn.commit()
        _ = buffered_db.ensure_buffer()
        buffered_db.clear_database("other")
        # Buffer for 'items' must be untouched
        assert buffered_db._buffer is not None


# ── BufferedStorageManager — reopen_if_changed ───────────────────────────────


class TestBufferedReopenIfChanged:
    """mtime changes invalidate the buffer on reopen."""

    def test_normal_mtime_change_clears_buffer(self, buffered_db):
        _ = buffered_db.ensure_buffer()
        assert buffered_db._buffer is not None
        st = os.stat(buffered_db.db_path)
        os.utime(buffered_db.db_path, times=(st.st_atime + 5, st.st_mtime + 5))
        buffered_db.reopen_if_changed()
        assert buffered_db._buffer is None

    def test_edge_unchanged_file_keeps_buffer(self, buffered_db):
        _ = buffered_db.ensure_buffer()
        before = id(buffered_db._buffer)
        buffered_db.reopen_if_changed()
        assert id(buffered_db._buffer) == before


class TestBufferedStorageEdgeCases:
    """flush/exists/insert failure and validation edges on the buffered manager."""

    def test_edge_flush_replace_mode(self, tmp_path):
        """flush() on an empty table persists buffered rows via replace mode."""
        conn = sqlite3.connect(str(tmp_path / BUFFERED_DB_NAME))
        create_items_schema(conn)
        conn.commit()
        conn.close()

        manager = BufferedStorageManager(str(tmp_path / BUFFERED_DB_NAME), ITEMS_TABLE)
        manager.insert({"id": 1, "name": "item_1", "value": "val_1"})

        # Flush with no existing data - should use replace mode
        manager.flush()

        # Verify data was written
        rows = manager.fetch_rows("SELECT * FROM items")
        assert len(rows) == 1
        manager.close()

    def test_error_flush_fails_raises(self, tmp_path):
        """flush() wraps commit failures in StorageError."""
        conn = sqlite3.connect(str(tmp_path / BUFFERED_DB_NAME))
        create_items_schema(conn)
        conn.commit()
        conn.close()

        manager = BufferedStorageManager(str(tmp_path / BUFFERED_DB_NAME), ITEMS_TABLE)
        manager.insert({"id": 1, "name": "item_1", "value": "val_1"})

        # Create a mock connection that raises error on commit
        mock_conn = MagicMock()
        mock_conn.commit.side_effect = Exception("Commit failed")

        # Replace the connection temporarily
        original_conn = manager.conn
        manager.conn = mock_conn
        try:
            with pytest.raises(StorageError, match="Buffer flush failed"):
                manager.flush()
        finally:
            manager.conn = original_conn
            manager.close()

    def test_error_exists_without_column_raises(self, tmp_path):
        """exists() without a column argument raises StorageError."""
        conn = sqlite3.connect(str(tmp_path / BUFFERED_DB_NAME))
        create_items_schema(conn)
        conn.commit()
        conn.close()

        manager = BufferedStorageManager(str(tmp_path / BUFFERED_DB_NAME), ITEMS_TABLE)
        with pytest.raises(StorageError, match="exists requires either"):
            manager.exists(ITEMS_TABLE)
        manager.close()

    def test_error_exists_wrong_table_raises(self, tmp_path):
        """exists() rejects a table other than the bound table."""
        conn = sqlite3.connect(str(tmp_path / BUFFERED_DB_NAME))
        create_items_schema(conn)
        conn.commit()
        conn.close()

        manager = BufferedStorageManager(str(tmp_path / BUFFERED_DB_NAME), ITEMS_TABLE)
        with pytest.raises(StorageError, match="BufferedStorageManager is bound to table 'items'"):
            manager.exists("wrong_table", "name", "value")
        manager.close()

    def test_error_insert_wrong_table_raises(self, tmp_path):
        """insert() rejects a table other than the bound table."""
        conn = sqlite3.connect(str(tmp_path / BUFFERED_DB_NAME))
        create_items_schema(conn)
        conn.commit()
        conn.close()

        manager = BufferedStorageManager(str(tmp_path / BUFFERED_DB_NAME), ITEMS_TABLE)
        with pytest.raises(StorageError, match="BufferedStorageManager is bound to table 'items'"):
            manager.insert("wrong_table", {"id": 1, "name": "test"})
        manager.close()

    def test_error_insert_non_mapping_raises(self, tmp_path):
        """insert() requires a mapping payload."""
        conn = sqlite3.connect(str(tmp_path / BUFFERED_DB_NAME))
        create_items_schema(conn)
        conn.commit()
        conn.close()

        manager = BufferedStorageManager(str(tmp_path / BUFFERED_DB_NAME), ITEMS_TABLE)
        with pytest.raises(StorageError, match="insert requires mapping payload"):
            manager.insert("not_a_mapping")
        manager.close()
