import pytest
import os
import sqlite3
from scrape_kit.db import BaseDatabaseManager, BufferedDatabaseManager
import time

class MockDB(BaseDatabaseManager):
    """Subclass of BaseDatabaseManager to test schema and inserts."""
    def _create_tables(self):
        with self.db_lock:
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )
            ''')
            self.conn.commit()

def test_db_manager_creation(tmp_path):
    db_file = tmp_path / "test.db"
    manager = MockDB(str(db_file))

    assert db_file.exists()

    # Check if table was created
    cursor = manager.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
    assert cursor.fetchone()[0] == 'items'

    manager.flush_and_close()

def test_db_serialization(tmp_path):
    db_file = tmp_path / "test_json.db"
    manager = MockDB(str(db_file))

    complex_data = {"scores": [1, 2, 3], "nested": {"a": 1}}
    serialized = manager._serialize_json(complex_data)
    assert '"scores": [1, 2, 3]' in serialized

    deserialized = manager._deserialize_json(serialized)
    assert deserialized == complex_data

    manager.flush_and_close()

def test_db_fetch_methods(tmp_path):
    db_file = tmp_path / "test_fetch.db"
    manager = MockDB(str(db_file))

    with manager.db_lock:
        manager.conn.execute("INSERT INTO items (data) VALUES (?)", ("data1",))
        manager.conn.execute("INSERT INTO items (data) VALUES (?)", ("data2",))
        manager.conn.commit()

    # 1. fetch_rows
    rows = manager.fetch_rows("SELECT * FROM items")
    assert len(rows) == 2
    assert rows[0]['data'] == "data1"

    # 2. fetch_dataframe
    df = manager.fetch_dataframe("SELECT * FROM items")
    assert len(df) == 2
    assert df['data'].iloc[0] == "data1"

    # 3. fetch_objs (with mapper)
    objs = manager.fetch_objs("SELECT * FROM items", mapper=lambda r: r['data'].upper())
    assert objs == ["DATA1", "DATA2"]

    # 4. fetch_objs (default to dict)
    dicts = manager.fetch_objs("SELECT * FROM items")
    assert isinstance(dicts[0], dict)
    assert dicts[0]['data'] == "data1"

    manager.flush_and_close()

def test_db_manager_clear(tmp_path):
    db_file = tmp_path / "test_clear.db"
    manager = MockDB(str(db_file))

    with manager.db_lock:
        manager.conn.execute("INSERT INTO items (data) VALUES ('test')")
        manager.conn.commit()

    # Verify insert
    cursor = manager.conn.execute("SELECT COUNT(*) FROM items")
    assert cursor.fetchone()[0] == 1

    # Clear
    manager.clear_database("items")

    cursor = manager.conn.execute("SELECT COUNT(*) FROM items")
    assert cursor.fetchone()[0] == 0

    manager.flush_and_close()

def test_db_manager_reopen(tmp_path):
    db_file = tmp_path / "test_reopen.db"
    manager = MockDB(str(db_file))

    mtime = manager._file_mtime

    # Manually modify mtime to simulate external change
    time.sleep(1)
    os.utime(str(db_file), None)

    # Trigger reopen check
    manager.reopen_if_changed()

    assert manager._file_mtime > mtime

    manager.flush_and_close()

def test_buffered_db_manager(tmp_path):
    db_file = tmp_path / "buffer.db"
    manager = BufferedDatabaseManager(str(db_file), "items")
    # Setup table since super init calls it
    with manager.db_lock:
        manager.conn.execute("CREATE TABLE items (id INTEGER, data TEXT)")
        manager.conn.execute("INSERT INTO items VALUES (1, 'initial')")
        manager.conn.commit()

    # Lazy load
    df = manager._ensure_buffer()
    assert len(df) == 1
    assert df['data'].iloc[0] == 'initial'

    # Update buffer
    import pandas as pd
    new_row = pd.DataFrame([{'id': 2, 'data': 'new'}])
    manager._buffer = pd.concat([manager._buffer, new_row], ignore_index=True)
    manager._dirty = True

    # Flush
    manager.flush()

    # Verify in SQL
    rows = manager.fetch_rows("SELECT * FROM items")
    assert len(rows) == 2

    manager.close()

def test_db_merge_databases(tmp_path):
    master_path = tmp_path / "master.db"
    chunk_dir = tmp_path / "chunks"
    chunk_dir.mkdir()

    # Setup master
    master = MockDB(str(master_path))

    # Setup chunk
    chunk_path = chunk_dir / "chunk1.db"
    c_conn = sqlite3.connect(chunk_path)
    c_conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")
    c_conn.execute("INSERT INTO items (data) VALUES ('chunk_data')")
    c_conn.commit()
    c_conn.close()

    # Merge
    master.merge_databases(str(chunk_dir), "items")

    # Check staging
    rows = master.fetch_rows("SELECT * FROM staging_items")
    assert len(rows) == 1
    assert rows[0]['data'] == 'chunk_data'

    master.flush_and_close()

def test_get_chunk_files(tmp_path):
    # Setup some .db files
    (tmp_path / "chunk1.db").touch()
    (tmp_path / "chunk2.db").touch()
    (tmp_path / "master.db").touch()
    (tmp_path / "not_db.txt").touch()

    manager = MockDB(str(tmp_path / "master.db"))

    chunks = manager._get_chunk_files(str(tmp_path), skip_file=str(tmp_path / "master.db"))

    assert len(chunks) == 2
    assert all(c.endswith(".db") for c in chunks)
    assert str(tmp_path / "master.db") not in chunks

    manager.flush_and_close()
