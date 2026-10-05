"""BufferedStorageManager (issue #10 split) — converged explicit-table API."""

import os
import sqlite3
import threading
from unittest.mock import MagicMock

import pytest
from conftest import BUFFERED_DB_NAME, ITEMS_TABLE, create_items_schema

from scrape_kit.errors import StorageError
from scrape_kit.storage import BufferedStorageManager

pytestmark = pytest.mark.p0


def make_buffered_manager(tmp_path, db_name: str = BUFFERED_DB_NAME) -> BufferedStorageManager:
    """Fresh BufferedStorageManager over an empty items-schema database."""
    path = str(tmp_path / db_name)
    conn = sqlite3.connect(path)
    create_items_schema(conn)
    conn.commit()
    conn.close()
    return BufferedStorageManager(path, ITEMS_TABLE)


# ── BufferedStorageManager — exists ──────────────────────────────────────────


class TestBufferedExists:
    """exists resolves pending rows first, then the pandas buffer."""

    def test_normal_found_in_buffer(self, buffered_db):
        assert buffered_db.exists(ITEMS_TABLE, "name", "alpha") is True

    def test_normal_not_found_returns_false(self, buffered_db):
        assert buffered_db.exists(ITEMS_TABLE, "name", "omega") is False

    def test_edge_empty_buffer_returns_false(self, tmp_path):
        manager = make_buffered_manager(tmp_path)
        assert manager.exists(ITEMS_TABLE, "name", "anything") is False
        manager.close()

    def test_normal_exists_after_insert_without_flush(self, buffered_db):
        buffered_db.insert(ITEMS_TABLE, {"id": 99, "name": "in_memory", "value": "yes"})
        assert buffered_db.exists(ITEMS_TABLE, "name", "in_memory") is True

    def test_normal_none_value_matches_pending_null(self, buffered_db):
        """exists(..., None) is a real NULL lookup, not a legacy-shape trigger."""
        buffered_db.insert(ITEMS_TABLE, {"id": 98, "name": "nully", "value": None})
        assert buffered_db.exists(ITEMS_TABLE, "value", None) is True

    def test_normal_none_value_matches_buffered_null(self, buffered_db):
        buffered_db.insert(ITEMS_TABLE, {"id": 97, "name": "bufnull", "value": None})
        buffered_db.flush()  # NULL now lives in the disk-backed buffer
        assert buffered_db.exists(ITEMS_TABLE, "value", None) is True

    def test_edge_none_value_absent_returns_false(self, buffered_db):
        assert buffered_db.exists(ITEMS_TABLE, "value", None) is False

    def test_normal_pending_checked_before_disk(self, tmp_path):
        """Pending rows must answer exists() before any disk access."""
        manager = make_buffered_manager(tmp_path)
        original_conn = manager.conn
        poisoned = MagicMock()
        poisoned.execute.side_effect = sqlite3.OperationalError("disk unreachable")
        manager.conn = poisoned
        try:
            manager.insert(ITEMS_TABLE, {"id": 1, "name": "pend_only", "value": "p"})
            assert manager.exists(ITEMS_TABLE, "name", "pend_only") is True
        finally:
            manager.conn = original_conn
            manager.close()

    def test_error_wrong_table_raises(self, tmp_path):
        manager = make_buffered_manager(tmp_path)
        with pytest.raises(StorageError, match="BufferedStorageManager is bound to table 'items'"):
            manager.exists("wrong_table", "name", "value")
        manager.close()

    def test_edge_ghost_column_returns_false(self, buffered_db):
        # Mirrors BaseStorageManager: unknown columns can never match.
        assert buffered_db.exists(ITEMS_TABLE, "no_such_column", "val") is False


# ── BufferedStorageManager — insert ──────────────────────────────────────────


class TestBufferedInsert:
    """insert queues rows for the bound table's buffer."""

    def test_normal_insert_grows_buffer(self, buffered_db):
        before = len(buffered_db.ensure_buffer())
        buffered_db.insert(ITEMS_TABLE, {"id": 3, "name": "gamma", "value": "g"})
        assert len(buffered_db.ensure_buffer()) == before + 1
        assert buffered_db._pending_rows == []

    def test_normal_insert_marks_buffer_dirty(self, buffered_db):
        assert buffered_db._dirty is False
        buffered_db.insert(ITEMS_TABLE, {"id": 3, "name": "new", "value": "n"})
        assert buffered_db._dirty is True

    def test_edge_multiple_inserts_all_in_buffer(self, buffered_db):
        for i in range(10):
            buffered_db.insert(ITEMS_TABLE, {"id": 100 + i, "name": f"item_{i}", "value": str(i)})
        assert len(buffered_db.ensure_buffer()) == 12  # 2 pre-existing + 10

    def test_normal_flush_writes_inserted_rows_to_disk(self, buffered_db):
        buffered_db.insert(ITEMS_TABLE, {"id": 3, "name": "flushed", "value": "f"})
        buffered_db.flush()
        rows = buffered_db.fetch_rows("SELECT * FROM items WHERE name = ?", ("flushed",))
        assert len(rows) == 1

    def test_edge_insert_allows_none_values(self, buffered_db):
        buffered_db.insert(ITEMS_TABLE, {"id": 5, "name": "nullrow", "value": None})
        buffered_db.flush()
        rows = buffered_db.fetch_rows("SELECT * FROM items WHERE value IS NULL")
        assert len(rows) == 1

    def test_error_insert_wrong_table_raises(self, tmp_path):
        manager = make_buffered_manager(tmp_path)
        with pytest.raises(StorageError, match="BufferedStorageManager is bound to table 'items'"):
            manager.insert("wrong_table", {"id": 1, "name": "test"})
        manager.close()

    def test_error_insert_non_mapping_raises(self, tmp_path):
        manager = make_buffered_manager(tmp_path)
        with pytest.raises(ValueError, match="mapping payload"):
            manager.insert(ITEMS_TABLE, "not_a_mapping")
        manager.close()


# ── BufferedStorageManager — flush ────────────────────────────────────────────


class TestBufferedFlush:
    """flush persists dirty buffers and clears the dirty flag."""

    def test_normal_dirty_buffer_written_to_db(self, buffered_db):
        buffered_db.insert(ITEMS_TABLE, {"id": 99, "name": "write_me", "value": "v"})
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
        buffered_db.insert(ITEMS_TABLE, {"id": 3, "name": "x", "value": "y"})
        assert buffered_db._dirty is True
        buffered_db.flush()
        assert buffered_db._dirty is False


# ── BufferedStorageManager — clear_table ───────────────────────────────────────


class TestBufferedClearTable:
    """clear_table resets SQL and the bound-table buffer."""

    def test_normal_clears_sql_and_resets_buffer(self, buffered_db):
        buffered_db.clear_table(ITEMS_TABLE)
        assert buffered_db._buffer is None
        assert buffered_db._dirty is False
        rows = buffered_db.fetch_rows("SELECT * FROM items")
        assert rows == []

    def test_edge_clearing_different_table_keeps_buffer_intact(self, buffered_db):
        # Create a second table
        buffered_db.conn.execute("CREATE TABLE other (x INTEGER)")
        buffered_db.conn.commit()
        _ = buffered_db.ensure_buffer()
        buffered_db.clear_table("other")
        # Buffer for 'items' must be untouched
        assert buffered_db._buffer is not None


# ── BufferedStorageManager — reopen_if_changed ───────────────────────────────


class TestBufferedReopenIfChanged:
    """mtime changes invalidate the disk-backed buffer but never pending rows."""

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

    def test_normal_reopen_preserves_pending_rows(self, tmp_path):
        """User-inserted pending rows must survive an external-change reopen."""
        manager = make_buffered_manager(tmp_path)
        manager.insert(ITEMS_TABLE, {"id": 42, "name": "survivor", "value": "s"})
        st = os.stat(manager.db_path)
        os.utime(manager.db_path, times=(st.st_atime + 5, st.st_mtime + 5))
        manager.reopen_if_changed()
        assert {"id": 42, "name": "survivor", "value": "s"} in manager._pending_rows
        assert manager._dirty is True
        manager.flush()
        rows = manager.fetch_rows("SELECT * FROM items WHERE name = 'survivor'")
        assert len(rows) == 1
        manager.close()


# ── BufferedStorageManager — teardown ──────────────────────────────────────────


class TestBufferedTeardown:
    """Both shutdown paths must persist queued rows before closing."""

    def test_normal_flush_and_close_persists_pending_rows(self, tmp_path):
        manager = make_buffered_manager(tmp_path, "teardown.db")
        manager.insert(ITEMS_TABLE, {"id": 7, "name": "drain_me", "value": "d"})
        manager.flush_and_close()
        conn = sqlite3.connect(manager.db_path)
        count = conn.execute("SELECT COUNT(*) FROM items WHERE name = 'drain_me'").fetchone()[0]
        conn.close()
        assert count == 1

    def test_normal_close_persists_pending_rows(self, tmp_path):
        manager = make_buffered_manager(tmp_path, "teardown2.db")
        manager.insert(ITEMS_TABLE, {"id": 8, "name": "drain_too", "value": "d"})
        manager.close()
        conn = sqlite3.connect(manager.db_path)
        count = conn.execute("SELECT COUNT(*) FROM items WHERE name = 'drain_too'").fetchone()[0]
        conn.close()
        assert count == 1


# ── BufferedStorageManager — failure and validation edges ────────────────────


class TestBufferedStorageEdgeCases:
    """flush/exists/insert failure and validation edges on the buffered manager."""

    def test_edge_flush_replace_mode(self, tmp_path):
        """flush() on an empty table persists buffered rows via replace mode."""
        manager = make_buffered_manager(tmp_path)
        manager.insert(ITEMS_TABLE, {"id": 1, "name": "item_1", "value": "val_1"})

        # Flush with no existing data - should use replace mode
        manager.flush()

        # Verify data was written
        rows = manager.fetch_rows("SELECT * FROM items")
        assert len(rows) == 1
        manager.close()

    def test_error_flush_fails_raises(self, tmp_path):
        """flush() wraps any write failure in StorageError and keeps the buffer dirty."""
        manager = make_buffered_manager(tmp_path)
        manager.insert(ITEMS_TABLE, {"id": 1, "name": "item_1", "value": "val_1"})

        # Create a mock connection that fails every disk interaction
        mock_conn = MagicMock()
        mock_conn.commit.side_effect = Exception("Commit failed")

        # Replace the connection temporarily
        original_conn = manager.conn
        manager.conn = mock_conn
        try:
            with pytest.raises(StorageError, match="Buffer flush failed"):
                manager.flush()
            assert manager._dirty is True  # failed flush must stay retryable
        finally:
            manager.conn = original_conn
            manager.close()


# ── BufferedStorageManager — concurrency ────────────────────────────────────────


def _run_threads(*targets) -> None:
    """Start all target threads, join them, and assert that none hung."""
    threads = [threading.Thread(target=t) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
        assert not t.is_alive(), "worker thread hung — possible deadlock under contention"


class TestBufferedConcurrency:
    """Cross-thread insert/exists/flush must not lose or corrupt queued rows."""

    def test_scenario_concurrent_insert_exists_flush(self, tmp_path):
        manager = make_buffered_manager(tmp_path, "conc.db")
        errors: list[str] = []
        barrier = threading.Barrier(3)

        def writer():
            try:
                barrier.wait(timeout=10)
                for i in range(100):
                    manager.insert(ITEMS_TABLE, {"id": i, "name": f"n{i}", "value": str(i)})
            except Exception as e:
                errors.append(f"writer: {e}")

        def prober():
            try:
                barrier.wait(timeout=10)
                for i in range(100):
                    manager.exists(ITEMS_TABLE, "name", f"n{i}")
            except Exception as e:
                errors.append(f"prober: {e}")

        def flusher():
            try:
                barrier.wait(timeout=10)
                for _ in range(5):
                    manager.flush()
            except Exception as e:
                errors.append(f"flusher: {e}")

        _run_threads(writer, prober, flusher)
        assert errors == [], f"Thread errors: {errors}"

        manager.flush()
        manager.close()
        conn = sqlite3.connect(manager.db_path)
        count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        conn.close()
        assert count == 100  # every queued row must land on disk exactly once
