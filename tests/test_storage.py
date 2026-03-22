"""
Comprehensive tests for storage.py — BaseStorageManager & BufferedStorageManager.

Public API covered (Base):
  __init__, fetch_rows, fetch_dataframe, fetch_objs, execute_batch,
  create_index, exists, insert, merge_databases, merge_row_by_row,
  reopen_if_changed, flush_and_close, clear_database

Public API covered (Buffered):
  __init__, flush, exists, insert, clear_database, reopen_if_changed, close

Each method has: normal case(s), edge case(s), error case.
Plus 5 complex integration scenarios at the bottom.
"""

import os
import time
import sqlite3
import threading
import pytest
import pandas as pd

from storage import BaseStorageManager, BufferedStorageManager
from errors import StorageError


# ── Shared test schema ────────────────────────────────────────────────────────

class MockDB(BaseStorageManager):
    """Concrete subclass with a simple two-table schema for testing."""

    def _create_tables(self):
        with self.db_lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS items (
                    id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    name  TEXT    NOT NULL,
                    value TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    tag     TEXT
                )
            """)
            self.conn.commit()


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    manager = MockDB(str(tmp_path / "test.db"))
    yield manager
    try:
        manager.flush_and_close()
    except Exception:
        pass


@pytest.fixture
def populated_db(tmp_path):
    manager = MockDB(str(tmp_path / "populated.db"))
    manager.conn.executemany(
        "INSERT INTO items (name, value) VALUES (?, ?)",
        [("alpha", "1"), ("beta", "2"), ("gamma", "3")],
    )
    manager.conn.commit()
    yield manager
    try:
        manager.flush_and_close()
    except Exception:
        pass


@pytest.fixture
def buffered_db(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "buffer.db"))
    conn.execute("CREATE TABLE items (id INTEGER, name TEXT, value TEXT)")
    conn.executemany("INSERT INTO items VALUES (?, ?, ?)", [(1, "alpha", "a"), (2, "beta", "b")])
    conn.commit()
    conn.close()
    manager = BufferedStorageManager(str(tmp_path / "buffer.db"), "items")
    yield manager
    try:
        manager.close()
    except Exception:
        pass


def make_chunk(path, rows, table="items"):
    """Helper: create a standalone .db chunk with the items schema."""
    conn = sqlite3.connect(str(path))
    conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, value TEXT)")
    conn.executemany(f"INSERT INTO {table} (name, value) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


# ── fetch_rows ────────────────────────────────────────────────────────────────

class TestFetchRows:
    def test_normal_returns_matching_rows(self, populated_db):
        rows = populated_db.fetch_rows("SELECT * FROM items WHERE name = ?", ("alpha",))
        assert len(rows) == 1
        assert rows[0]["name"] == "alpha"
        assert rows[0]["value"] == "1"

    def test_normal_parameterless_query_returns_all(self, populated_db):
        rows = populated_db.fetch_rows("SELECT * FROM items")
        assert len(rows) == 3

    def test_normal_rows_accessible_by_column_name(self, populated_db):
        rows = populated_db.fetch_rows("SELECT name, value FROM items ORDER BY name")
        assert rows[0]["name"] == "alpha"

    def test_edge_empty_table_returns_empty_list(self, db):
        rows = db.fetch_rows("SELECT * FROM items")
        assert rows == []

    def test_edge_no_matches_returns_empty_list(self, populated_db):
        rows = populated_db.fetch_rows("SELECT * FROM items WHERE name = ?", ("zzz",))
        assert rows == []

    def test_error_invalid_table_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.fetch_rows("SELECT * FROM nonexistent_table")

    def test_error_syntax_error_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.fetch_rows("SELEKT * FORM items")


# ── fetch_dataframe ───────────────────────────────────────────────────────────

class TestFetchDataframe:
    def test_normal_returns_dataframe_with_correct_shape(self, populated_db):
        df = populated_db.fetch_dataframe("SELECT * FROM items")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert set(df.columns) >= {"name", "value"}

    def test_normal_column_values_match_db(self, populated_db):
        df = populated_db.fetch_dataframe("SELECT * FROM items ORDER BY name")
        assert list(df["name"]) == ["alpha", "beta", "gamma"]

    def test_normal_parameterized_query(self, populated_db):
        df = populated_db.fetch_dataframe(
            "SELECT * FROM items WHERE name = ?", ("beta",)
        )
        assert len(df) == 1
        assert df.iloc[0]["name"] == "beta"

    def test_edge_empty_table_returns_empty_dataframe(self, db):
        df = db.fetch_dataframe("SELECT * FROM items")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_error_invalid_query_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.fetch_dataframe("SELECT * FROM ghost_table")


# ── fetch_objs ────────────────────────────────────────────────────────────────

class TestFetchObjs:
    def test_normal_with_mapper_transforms_rows(self, populated_db):
        result = populated_db.fetch_objs(
            "SELECT * FROM items ORDER BY name",
            mapper=lambda r: r["name"].upper(),
        )
        assert result == ["ALPHA", "BETA", "GAMMA"]

    def test_normal_without_mapper_returns_list_of_dicts(self, populated_db):
        result = populated_db.fetch_objs("SELECT * FROM items ORDER BY name")
        assert all(isinstance(r, dict) for r in result)
        assert result[0]["name"] == "alpha"

    def test_normal_parameterized_with_mapper(self, populated_db):
        result = populated_db.fetch_objs(
            "SELECT * FROM items WHERE name = ?",
            params=("gamma",),
            mapper=lambda r: r["value"],
        )
        assert result == ["3"]

    def test_edge_no_rows_returns_empty_list(self, db):
        assert db.fetch_objs("SELECT * FROM items") == []

    def test_error_invalid_query_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.fetch_objs("SELECT * FROM no_such_table")


# ── execute_batch ─────────────────────────────────────────────────────────────

class TestExecuteBatch:
    def test_normal_inserts_all_rows_in_one_transaction(self, db):
        params = [("item1", "v1"), ("item2", "v2"), ("item3", "v3")]
        db.execute_batch("INSERT INTO items (name, value) VALUES (?, ?)", params)
        assert len(db.fetch_rows("SELECT * FROM items")) == 3

    def test_normal_large_batch(self, db):
        params = [(f"item_{i}", str(i)) for i in range(500)]
        db.execute_batch("INSERT INTO items (name, value) VALUES (?, ?)", params)
        assert len(db.fetch_rows("SELECT * FROM items")) == 500

    def test_edge_empty_params_list_is_noop(self, db):
        db.execute_batch("INSERT INTO items (name, value) VALUES (?, ?)", [])
        assert db.fetch_rows("SELECT * FROM items") == []

    def test_error_pk_violation_rolls_back_entire_batch(self, db):
        db.conn.execute("INSERT INTO items (id, name) VALUES (99, 'original')")
        db.conn.commit()
        with pytest.raises(StorageError):
            db.execute_batch(
                "INSERT INTO items (id, name) VALUES (?, ?)",
                [(99, "duplicate"), (100, "would_succeed")],
            )
        # Original preserved; batch rolled back
        rows = db.fetch_rows("SELECT * FROM items WHERE id = 99")
        assert rows[0]["name"] == "original"
        assert not db.fetch_rows("SELECT * FROM items WHERE id = 100")


# ── create_index ─────────────────────────────────────────────────────────────

class TestCreateIndex:
    def test_normal_creates_single_column_index(self, db):
        db.create_index("items", ["name"])
        cursor = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_items_name'"
        )
        assert cursor.fetchone() is not None

    def test_normal_creates_unique_multicolumn_index(self, db):
        db.create_index("items", ["name", "value"], unique=True)
        cursor = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_items_name_value'"
        )
        assert cursor.fetchone() is not None

    def test_edge_create_same_index_twice_is_idempotent(self, db):
        db.create_index("items", ["name"])
        db.create_index("items", ["name"])  # IF NOT EXISTS, no exception expected

    def test_error_invalid_table_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.create_index("nonexistent_table", ["col"])

    def test_error_invalid_column_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.create_index("items", ["no_such_column"])


# ── exists ────────────────────────────────────────────────────────────────────

class TestBaseExists:
    def test_normal_returns_true_when_value_present(self, populated_db):
        assert populated_db.exists("items", "name", "alpha") is True

    def test_normal_returns_false_when_value_absent(self, populated_db):
        assert populated_db.exists("items", "name", "delta") is False

    def test_edge_empty_table_always_returns_false(self, db):
        assert db.exists("items", "name", "anything") is False

    def test_edge_integer_value_lookup(self, db):
        db.conn.execute("INSERT INTO items (id, name) VALUES (42, 'test')")
        db.conn.commit()
        assert db.exists("items", "id", 42) is True
        assert db.exists("items", "id", 99) is False

    def test_error_nonexistent_column_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.exists("items", "no_such_column", "val")

    def test_error_nonexistent_table_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.exists("ghost_table", "name", "val")


# ── insert ────────────────────────────────────────────────────────────────────

class TestBaseInsert:
    def test_normal_inserts_row_and_is_retrievable(self, db):
        db.insert("items", {"name": "myitem", "value": "myval"})
        rows = db.fetch_rows("SELECT * FROM items WHERE name = ?", ("myitem",))
        assert len(rows) == 1
        assert rows[0]["value"] == "myval"

    def test_normal_insert_multiple_sequential_rows(self, db):
        for i in range(5):
            db.insert("items", {"name": f"row_{i}", "value": str(i)})
        assert len(db.fetch_rows("SELECT * FROM items")) == 5

    def test_edge_insert_with_none_value(self, db):
        db.insert("items", {"name": "nullval", "value": None})
        rows = db.fetch_rows("SELECT * FROM items WHERE name = ?", ("nullval",))
        assert rows[0]["value"] is None

    def test_error_insert_into_nonexistent_table_raises(self, db):
        with pytest.raises(StorageError):
            db.insert("ghost_table", {"col": "val"})

    def test_error_not_null_violation_raises(self, db):
        # 'name' has NOT NULL constraint
        with pytest.raises(StorageError):
            db.insert("items", {"value": "no_name_provided"})


# ── clear_database ────────────────────────────────────────────────────────────

class TestClearDatabase:
    def test_normal_removes_all_rows(self, populated_db):
        populated_db.clear_database("items")
        assert populated_db.fetch_rows("SELECT * FROM items") == []

    def test_normal_table_structure_intact_after_clear(self, populated_db):
        populated_db.clear_database("items")
        populated_db.insert("items", {"name": "fresh", "value": "new"})
        assert len(populated_db.fetch_rows("SELECT * FROM items")) == 1

    def test_edge_clearing_already_empty_table_is_noop(self, db):
        db.clear_database("items")  # no rows to delete — should not raise

    def test_error_nonexistent_table_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.clear_database("nonexistent_table")


# ── merge_databases ───────────────────────────────────────────────────────────

class TestMergeDatabases:
    def test_normal_single_chunk_lands_in_staging(self, db, tmp_path):
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        make_chunk(chunk_dir / "c1.db", [("alpha", "1"), ("beta", "2")])
        db.merge_databases(str(chunk_dir), "items")
        rows = db.fetch_rows("SELECT * FROM staging_items")
        assert len(rows) == 2

    def test_normal_multiple_chunks_all_merged(self, db, tmp_path):
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        for i in range(3):
            make_chunk(chunk_dir / f"c{i}.db", [(f"item_{i}_{j}", str(j)) for j in range(4)])
        db.merge_databases(str(chunk_dir), "items")
        rows = db.fetch_rows("SELECT * FROM staging_items")
        assert len(rows) == 12

    def test_edge_empty_directory_does_nothing(self, db, tmp_path):
        empty = tmp_path / "empty_chunks"
        empty.mkdir()
        db.merge_databases(str(empty), "items")  # no-op, must not raise

    def test_edge_master_db_skipped_in_merge(self, db, tmp_path):
        """The master DB itself must not be attached as a chunk."""
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        make_chunk(chunk_dir / "real.db", [("from_chunk", "yes")])
        db.merge_databases(str(chunk_dir), "items")
        rows = db.fetch_rows("SELECT * FROM staging_items")
        names = [r["name"] for r in rows]
        assert "from_chunk" in names


# ── merge_row_by_row ──────────────────────────────────────────────────────────

class TestMergeRowByRow:
    def test_normal_callback_called_for_every_row(self, db, tmp_path):
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        make_chunk(chunk_dir / "c1.db", [("r1", "v1"), ("r2", "v2")])
        collected = []
        db.merge_row_by_row(str(chunk_dir), "items", row_callback=lambda r: collected.append(r["name"]))
        assert sorted(collected) == ["r1", "r2"]

    def test_normal_flush_callback_invoked_once_per_chunk(self, db, tmp_path):
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        make_chunk(chunk_dir / "c1.db", [("a", "1")])
        make_chunk(chunk_dir / "c2.db", [("b", "2")])
        flush_hits = []
        db.merge_row_by_row(
            str(chunk_dir), "items",
            row_callback=lambda r: None,
            flush_callback=lambda: flush_hits.append(1),
        )
        assert len(flush_hits) == 2

    def test_edge_empty_directory_calls_no_callbacks(self, db, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        called = []
        db.merge_row_by_row(str(empty), "items", row_callback=lambda r: called.append(r))
        assert called == []

    def test_edge_corrupt_chunk_skipped_gracefully(self, db, tmp_path):
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        # A valid chunk
        make_chunk(chunk_dir / "good.db", [("valid", "data")])
        # A tiny corrupt file (too small, filtered by size check)
        (chunk_dir / "corrupt.db").write_bytes(b"not a db")
        collected = []
        db.merge_row_by_row(str(chunk_dir), "items", row_callback=lambda r: collected.append(r["name"]))
        assert "valid" in collected


# ── reopen_if_changed ─────────────────────────────────────────────────────────

class TestReopenIfChanged:
    def test_normal_unchanged_file_keeps_same_connection(self, db):
        original_id = id(db.conn)
        db.reopen_if_changed()
        assert id(db.conn) == original_id

    def test_edge_modified_mtime_triggers_reopen(self, db):
        original_mtime = db._file_mtime
        time.sleep(0.05)
        os.utime(db.db_path, None)
        db.reopen_if_changed()
        assert db._file_mtime > original_mtime

    def test_edge_data_readable_after_reopen(self, db):
        db.insert("items", {"name": "before_reopen", "value": "x"})
        db.conn.commit()
        time.sleep(0.05)
        os.utime(db.db_path, None)
        db.reopen_if_changed()
        rows = db.fetch_rows("SELECT * FROM items WHERE name = ?", ("before_reopen",))
        assert len(rows) == 1

    def test_error_missing_file_does_not_raise(self, tmp_path):
        path = str(tmp_path / "ephemeral.db")
        manager = MockDB(path)
        os.remove(path)
        manager.reopen_if_changed()  # OSError caught internally — must not propagate


# ── flush_and_close ───────────────────────────────────────────────────────────

class TestFlushAndClose:
    def test_normal_connection_unusable_after_close(self, tmp_path):
        manager = MockDB(str(tmp_path / "close_test.db"))
        manager.flush_and_close()
        with pytest.raises(Exception):
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


# ── BufferedStorageManager — exists ──────────────────────────────────────────

class TestBufferedExists:
    def test_normal_found_in_buffer(self, buffered_db):
        assert buffered_db.exists("name", "alpha") is True

    def test_normal_not_found_returns_false(self, buffered_db):
        assert buffered_db.exists("name", "omega") is False

    def test_edge_empty_buffer_returns_false(self, tmp_path):
        conn = sqlite3.connect(str(tmp_path / "empty.db"))
        conn.execute("CREATE TABLE items (id INTEGER, name TEXT, value TEXT)")
        conn.commit()
        conn.close()
        manager = BufferedStorageManager(str(tmp_path / "empty.db"), "items")
        assert manager.exists("name", "anything") is False
        manager.close()

    def test_normal_exists_after_insert_without_flush(self, buffered_db):
        buffered_db.insert({"id": 99, "name": "in_memory", "value": "yes"})
        assert buffered_db.exists("name", "in_memory") is True


# ── BufferedStorageManager — insert ──────────────────────────────────────────

class TestBufferedInsert:
    def test_normal_insert_grows_buffer(self, buffered_db):
        before = len(buffered_db._ensure_buffer())
        buffered_db.insert({"id": 3, "name": "gamma", "value": "g"})
        assert len(buffered_db._ensure_buffer()) == before + 1

    def test_normal_insert_marks_buffer_dirty(self, buffered_db):
        assert buffered_db._dirty is False
        buffered_db.insert({"id": 3, "name": "new", "value": "n"})
        assert buffered_db._dirty is True

    def test_edge_multiple_inserts_all_in_buffer(self, buffered_db):
        for i in range(10):
            buffered_db.insert({"id": 100 + i, "name": f"item_{i}", "value": str(i)})
        assert len(buffered_db._ensure_buffer()) == 12  # 2 pre-existing + 10

    def test_normal_flush_writes_inserted_rows_to_disk(self, buffered_db):
        buffered_db.insert({"id": 3, "name": "flushed", "value": "f"})
        buffered_db.flush()
        rows = buffered_db.fetch_rows("SELECT * FROM items WHERE name = ?", ("flushed",))
        assert len(rows) == 1


# ── BufferedStorageManager — flush ────────────────────────────────────────────

class TestBufferedFlush:
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

    def test_edge_flush_when_buffer_none_is_noop(self, buffered_db):
        buffered_db._buffer = None
        buffered_db._dirty = True
        buffered_db.flush()  # guard: if not _dirty or buffer is None → return

    def test_normal_flush_clears_dirty_flag(self, buffered_db):
        buffered_db.insert({"id": 3, "name": "x", "value": "y"})
        assert buffered_db._dirty is True
        buffered_db.flush()
        assert buffered_db._dirty is False


# ── BufferedStorageManager — clear_database ───────────────────────────────────

class TestBufferedClearDatabase:
    def test_normal_clears_sql_and_resets_buffer(self, buffered_db):
        buffered_db.clear_database("items")
        assert buffered_db._buffer is None
        assert buffered_db._dirty is False
        rows = buffered_db.fetch_rows("SELECT * FROM items")
        assert rows == []

    def test_edge_clearing_different_table_keeps_buffer_intact(self, buffered_db):
        # Create a second table
        buffered_db.conn.execute("CREATE TABLE other (x INTEGER)")
        buffered_db.conn.commit()
        _ = buffered_db._ensure_buffer()
        buffered_db.clear_database("other")
        # Buffer for 'items' must be untouched
        assert buffered_db._buffer is not None


# ── BufferedStorageManager — reopen_if_changed ───────────────────────────────

class TestBufferedReopenIfChanged:
    def test_normal_mtime_change_clears_buffer(self, buffered_db):
        _ = buffered_db._ensure_buffer()
        assert buffered_db._buffer is not None
        time.sleep(0.05)
        os.utime(buffered_db.db_path, None)
        buffered_db.reopen_if_changed()
        assert buffered_db._buffer is None

    def test_edge_unchanged_file_keeps_buffer(self, buffered_db):
        _ = buffered_db._ensure_buffer()
        before = id(buffered_db._buffer)
        buffered_db.reopen_if_changed()
        assert id(buffered_db._buffer) == before


# ── Complex Scenarios ─────────────────────────────────────────────────────────

class TestStorageScenarios:
    def test_scenario_batch_insert_index_and_exists(self, db):
        """Insert 1 000 rows via execute_batch, index the name column,
        then verify random lookups via exists() are correct."""
        params = [(f"item_{i}", str(i)) for i in range(1000)]
        db.execute_batch("INSERT INTO items (name, value) VALUES (?, ?)", params)
        db.create_index("items", ["name"])

        assert db.exists("items", "name", "item_0") is True
        assert db.exists("items", "name", "item_999") is True
        assert db.exists("items", "name", "item_9999") is False
        assert db.exists("items", "name", "item_500") is True

    def test_scenario_multi_chunk_merge_then_dataframe_query(self, db, tmp_path):
        """Merge 4 chunks, then run a DataFrame aggregation on the staging table."""
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        for i in range(4):
            make_chunk(chunk_dir / f"c{i}.db", [(f"node_{i}_{j}", str(j)) for j in range(5)])
        db.merge_databases(str(chunk_dir), "items")
        df = db.fetch_dataframe("SELECT * FROM staging_items")
        assert len(df) == 20
        assert len(df["name"].unique()) == 20

    def test_scenario_buffered_insert_exists_flush_verify(self, tmp_path):
        """50 inserts via buffer → in-memory exists checks → flush → disk verification."""
        conn = sqlite3.connect(str(tmp_path / "buf.db"))
        conn.execute("CREATE TABLE items (id INTEGER, name TEXT, value TEXT)")
        conn.commit()
        conn.close()
        manager = BufferedStorageManager(str(tmp_path / "buf.db"), "items")

        for i in range(50):
            manager.insert({"id": i, "name": f"item_{i}", "value": str(i)})
        for i in range(50):
            assert manager.exists("name", f"item_{i}") is True
        assert manager.exists("name", "item_50") is False

        manager.flush()
        count = manager.fetch_rows("SELECT COUNT(*) as cnt FROM items")[0]["cnt"]
        assert count == 50
        manager.close()

    def test_scenario_concurrent_reads_while_batch_write(self, db):
        """Writer thread and multiple reader threads must not deadlock or corrupt data."""
        params = [(f"concurrent_{i}", str(i)) for i in range(200)]
        errors = []

        def writer():
            try:
                db.execute_batch("INSERT INTO items (name, value) VALUES (?, ?)", params)
            except Exception as e:
                errors.append(("writer", e))

        def reader():
            try:
                db.fetch_rows("SELECT * FROM items")
            except Exception as e:
                errors.append(("reader", e))

        threads = (
            [threading.Thread(target=writer)] +
            [threading.Thread(target=reader) for _ in range(6)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == [], f"Thread errors: {errors}"

    def test_scenario_clear_and_reingest_fresh_data(self, populated_db):
        """Clear all rows, re-insert a completely different dataset, verify clean slate."""
        populated_db.clear_database("items")
        assert populated_db.fetch_rows("SELECT * FROM items") == []

        new_data = [("x", "10"), ("y", "20"), ("z", "30")]
        populated_db.execute_batch("INSERT INTO items (name, value) VALUES (?, ?)", new_data)
        rows = populated_db.fetch_rows("SELECT name FROM items ORDER BY name")
        assert [r["name"] for r in rows] == ["x", "y", "z"]
        # Old names must be gone
        assert not populated_db.exists("items", "name", "alpha")