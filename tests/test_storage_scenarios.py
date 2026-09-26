"""Cross-manager integration scenarios (issue #10 split)."""

"""
Comprehensive tests for storage.py — BaseStorageManager & BufferedStorageManager.

Public API covered (Base):
  __init__, fetch_rows, fetch_dataframe, fetch_objects, execute_batch,
  create_index, exists, insert, merge_databases, merge_row_by_row,
  reopen_if_changed, flush_and_close, clear_database

Public API covered (Buffered):
  __init__, flush, exists, insert, clear_database, reopen_if_changed, close

Each method has: normal case(s), edge case(s), error case.
Plus 5 complex integration scenarios at the bottom.
"""

import sqlite3
import threading


from scrape_kit.storage import BufferedStorageManager

from conftest import create_items_schema, make_chunk

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
        create_items_schema(conn)
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

        threads = [threading.Thread(target=writer)] + [threading.Thread(target=reader) for _ in range(6)]
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



