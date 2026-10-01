"""BaseStorageManager connection lifecycle (issue #10 split)."""

import os
import sqlite3
from unittest.mock import MagicMock

import pytest
from conftest import MockDB, create_items_schema

from scrape_kit.errors import StorageError
from scrape_kit.storage import BaseStorageManager

pytestmark = pytest.mark.p0


# ── reopen_if_changed ─────────────────────────────────────────────────────────


class TestReopenIfChanged:
    """reopen_if_changed swaps connections on mtime change."""

    def test_normal_unchanged_file_keeps_same_connection(self, db):
        original_id = id(db.conn)
        db.reopen_if_changed()
        assert id(db.conn) == original_id

    def test_edge_modified_mtime_triggers_reopen(self, db):
        original_mtime = db._file_mtime
        st = os.stat(db.db_path)
        os.utime(db.db_path, times=(st.st_atime + 5, st.st_mtime + 5))
        db.reopen_if_changed()
        assert db._file_mtime > original_mtime

    def test_edge_data_readable_after_reopen(self, db):
        db.insert("items", {"name": "before_reopen", "value": "x"})
        db.conn.commit()
        st = os.stat(db.db_path)
        os.utime(db.db_path, times=(st.st_atime + 5, st.st_mtime + 5))
        db.reopen_if_changed()
        rows = db.fetch_rows("SELECT * FROM items WHERE name = ?", ("before_reopen",))
        assert len(rows) == 1

    def test_error_missing_file_does_not_raise(self, tmp_path):
        path = str(tmp_path / "ephemeral.db")
        manager = MockDB(path)
        manager.flush_and_close()  # release Windows file lock before removing
        os.remove(path)
        manager.reopen_if_changed()  # should not raise
        assert manager.conn is not None  # connection survives a missing backing file


# ── flush_and_close ───────────────────────────────────────────────────────────


class TestFlushAndClose:
    """flush_and_close drains WAL and closes the connection."""

    def test_normal_connection_unusable_after_close(self, tmp_path):
        manager = MockDB(str(tmp_path / "close_test.db"))
        manager.flush_and_close()
        with pytest.raises((sqlite3.ProgrammingError, sqlite3.OperationalError)):
            manager.conn.execute("SELECT 1")

    def test_normal_data_persists_after_close_and_reopen(self, tmp_path):
        path = str(tmp_path / "persist.db")
        manager = MockDB(path)
        manager.conn.execute("INSERT INTO items (name) VALUES ('persisted')")
        manager.conn.commit()
        manager.flush_and_close()

        reopened = MockDB(path)
        rows = reopened.fetch_rows("SELECT * FROM items")
        assert rows[0]["name"] == "persisted"
        reopened.flush_and_close()

    def test_edge_wal_checkpoint_clears_wal_file(self, tmp_path):
        path = str(tmp_path / "wal_test.db")
        manager = MockDB(path)
        manager.conn.execute("INSERT INTO items (name) VALUES ('wal_row')")
        manager.conn.commit()
        manager.flush_and_close()
        # After TRUNCATE checkpoint, WAL should be empty/absent
        wal_path = path + "-wal"
        assert not os.path.exists(wal_path) or os.path.getsize(wal_path) == 0


# ── Serialization and shutdown edge cases ───────────────────────────────────────


class TestSerializationEdgeCases:
    """serialize_json/deserialize_json edge behaviour: None, objects, NaN, invalid JSON."""

    def test_edge_serialize_none_returns_none(self, db):
        """serialize_json returns None for None input."""
        result = db.serialize_json(None)
        assert result is None

    def test_edge_serialize_object_with_dict(self, db):
        """serialize_json serialises plain objects via their __dict__."""

        class TestObj:
            """Row-to-object serialization edge behavior."""

            def __init__(self):
                self.name = "test"
                self.value = 42

        obj = TestObj()
        result = db.serialize_json(obj)
        assert result == '{"name": "test", "value": 42}'

    def test_error_serialize_unserializable_raises(self, db):
        """serialize_json raises StorageError for unserialisable objects."""

        class Unserializable:
            def __init__(self):
                self.func = lambda: None  # Functions can't be serialized

        obj = Unserializable()
        with pytest.raises(StorageError, match="Serialization failed"):
            db.serialize_json(obj)

    def test_edge_deserialize_nan_returns_none(self, db):
        """serialize_json maps NaN payloads to None."""
        nan_value = float("nan")
        result = db.deserialize_json(nan_value)
        assert result is None

    def test_edge_deserialize_invalid_json_returns_none(self, db):
        """deserialize_json returns None for invalid JSON input."""
        result = db.deserialize_json("not valid json")
        assert result is None


class TestReopenEdgeCases:
    """reopen_if_changed and chunk-file discovery edge cases."""

    def test_edge_reopen_cleanup_error_ignored(self, tmp_path):
        """reopen_if_changed keeps a usable connection after external file changes."""
        # Note: Can't mock sqlite3.Connection.close as it's read-only
        # This test verifies normal reopen behavior works
        db = MockDB(str(tmp_path / "test.db"))
        db.execute_batch("INSERT INTO items (name, value) VALUES (?, ?)", [("test", "value")])

        # Modify the file externally — explicit future mtime, no sleeps
        st = os.stat(tmp_path / "test.db")
        os.utime(tmp_path / "test.db", times=(st.st_atime + 5, st.st_mtime + 5))

        # Should not raise, just log warning and reopen
        db.reopen_if_changed()

        # Connection should still be usable after reopen
        assert db.conn is not None
        db.flush_and_close()

    def test_edge_get_chunk_files_with_skip(self, tmp_path):
        """get_chunk_files excludes the explicitly skipped file."""
        # Create some chunk files
        for i in range(3):
            chunk_db = sqlite3.connect(str(tmp_path / f"chunk_{i:03d}.db"))
            create_items_schema(chunk_db)
            chunk_db.commit()
            chunk_db.close()

        skip_file = str(tmp_path / "chunk_001.db")
        result = BaseStorageManager.get_chunk_files(str(tmp_path), skip_file=skip_file)

        # Should have 2 files (skipping chunk_001.db)
        assert len(result) == 2
        assert skip_file not in result


class TestFlushAndCloseEdgeCases:
    """flush_and_close failure behaviour."""

    def test_error_flush_and_close_fails_raises(self, tmp_path):
        """flush_and_close wraps commit failures in StorageError."""
        db = MockDB(str(tmp_path / "test.db"))
        db.execute_batch("INSERT INTO items (name, value) VALUES (?, ?)", [("test", "value")])

        # Create a mock connection that raises error on commit
        mock_conn = MagicMock()
        mock_conn.commit.side_effect = sqlite3.Error("Commit failed")
        mock_conn.close = MagicMock()

        # Replace the connection temporarily
        original_conn = db.conn
        db.conn = mock_conn
        try:
            with pytest.raises(StorageError, match="Fatal error during shutdown"):
                db.flush_and_close()
        finally:
            db.conn = original_conn
            db.flush_and_close()
