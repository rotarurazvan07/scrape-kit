import os
import sqlite3
from storage import BaseStorageManager, BufferedStorageManager
import time

class MockDB(BaseStorageManager):
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
    manager = BufferedStorageManager(str(db_file), "items")
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

def test_db_merge_row_by_row(tmp_path):
    master_path = tmp_path / "master_row.db"
    chunk_dir = tmp_path / "chunks"
    chunk_dir.mkdir(exist_ok=True)

    # 1. Setup Chunk
    chunk_path = chunk_dir / "chunk2.db"
    c_conn = sqlite3.connect(chunk_path)
    c_conn.execute("CREATE TABLE items (id INTEGER, data TEXT)")
    c_conn.execute("INSERT INTO items VALUES (1, 'row1'), (2, 'row2')")
    c_conn.commit()
    c_conn.close()

    # 2. Master with callback logic
    master = MockDB(str(master_path))
    processed_data = []

    def my_callback(row):
        processed_data.append(row['data'])

    # 3. Merge
    master.merge_row_by_row(str(chunk_dir), "items", row_callback=my_callback)

    assert len(processed_data) == 2
    assert "row1" in processed_data
    assert "row2" in processed_data

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

def test_storage_speed_comparison(tmp_path):
    # Benchmarking SQL-based deduplication vs Buffered deduplication
    db_file = tmp_path / "benchmark.db"
    count = 10000

    # 1. Setup a pre-populated DB
    manager = MockDB(str(db_file))
    with manager.db_lock:
        data_list = [(i, f"item_{i}") for i in range(count)]
        manager.conn.executemany("INSERT INTO items (id, data) VALUES (?, ?)", data_list)
        manager.conn.commit()

    try:
        # 2. Benchmark Case A: Individual SQL SELECTs
        start_sql = time.perf_counter()
        for i in range(count):
            cursor = manager.conn.execute("SELECT 1 FROM items WHERE id = ?", (i,))
            cursor.fetchone()
        end_sql = time.perf_counter()
        duration_sql = end_sql - start_sql

        # 3. Close the seeder to allow fresh access for buffered
        manager.flush_and_close()

        # 4. Benchmark Case B: Buffered Lookup
        buffered = BufferedStorageManager(str(db_file), "items")
        start_buf = time.perf_counter()
        df = buffered._ensure_buffer()
        df = df.set_index('id')
        for i in range(count):
            _ = i in df.index
        end_buf = time.perf_counter()
        duration_buf = end_buf - start_buf

        print(f"\n🚀 STORAGE BENCHMARK (N={count})")
        print(f"   SQL Individual SELECTs: {duration_sql:.4f}s")
        print(f"   Buffered In-Memory:    {duration_buf:.4f}s")

        # At N=10,000, Buffered is typically 2-3x faster due to avoids SQL/IPC overhead
        assert duration_buf < duration_sql

    finally:
        try:
            buffered.close()
        except Exception:
            pass
