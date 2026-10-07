# Storage

Use the storage API to persist rows in SQLite, buffer high-volume inserts in pandas, and merge chunk databases.

```python
from scrape_kit import BaseStorageManager, BufferedStorageManager, MergeReport, StorageError
```

## Subclass BaseStorageManager

`BaseStorageManager(db_path)` opens SQLite, then calls `_create_tables()`. Override that hook to create your schema. Guard SQLite work with `self.db_lock`.

```python
from scrape_kit import BaseStorageManager, StorageError

class ItemsDb(BaseStorageManager):
    def _create_tables(self) -> None:
        with self.db_lock:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, title TEXT NOT NULL)"
            )
            self.conn.commit()

db = ItemsDb("items.db")
try:
    if not db.exists("items", "id", "1"):
        db.insert("items", {"id": "1", "title": "Example"})
    rows = db.fetch_rows("SELECT id, title FROM items WHERE id = ?", ("1",))
finally:
    db.close()
```

## Read and write rows

| Method | What it does |
| --- | --- |
| `insert(table_name, data)` | Insert one mapping as a row. |
| `exists(table_name, column, value)` | Return whether a value is present. A missing table raises `StorageError`. A missing column returns `False`. `value=None` never matches in SQL. |
| `fetch_rows(query, params=None)` | Return `sqlite3.Row` objects. |
| `fetch_dataframe(query, params=None)` | Return a pandas `DataFrame`. |
| `fetch_objects(query, params=None, mapper=None)` | Map rows with `mapper`, or return dicts. |
| `execute_batch(query, params_list)` | Run many parameterized statements in one transaction. |
| `create_index(table_name, columns, unique=False)` | Create an index if it does not exist. |
| `clear_table(table_name)` | Delete all rows without dropping the table. |

Reads and writes reopen the connection when the file changes on disk (`reopen_if_changed()`).

## JSON helpers

```python
payload = db.serialize_json({"ok": True})
parsed = db.deserialize_json(payload)
row_dict = db.row_to_dict(rows[0])
```

`serialize_json(None)` returns `None`. Invalid JSON in `deserialize_json()` returns `None` after a warning.

## Shut down

`flush_and_close()` commits and closes. `close()` is an alias. Both raise `StorageError` if shutdown fails.

## Buffer one table

`BufferedStorageManager(db_path, table_name, preserve_schema=True)` binds to a single table. `exists()` and `insert()` hit the pandas buffer; `flush()` persists.

`table_name` is required. A mismatched table name on `exists()` / `insert()` raises `StorageError`. `insert()` also raises `ValueError` when `data` is not a mapping.

Create the table first (a `BaseStorageManager` subclass is enough), then open a buffer on the same file:

```python
from scrape_kit import BaseStorageManager, BufferedStorageManager

class EventsDb(BaseStorageManager):
    def _create_tables(self) -> None:
        with self.db_lock:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, name TEXT)"
            )
            self.conn.commit()

schema = EventsDb("events.db")
schema.close()

buf = BufferedStorageManager("events.db", "events")
buf.insert("events", {"id": "a", "name": "open"})
assert buf.exists("events", "id", "a")
buf.flush()
buf.close()
```

Pending rows survive an external file change; only the disk-backed buffer is dropped. `close()` flushes first.

`preserve_schema=True` (default) deletes and re-appends so indexes stay. `False` replaces the table with pandas' default schema.

Buffered `exists()` treats `None` as a real lookup and matches NULL cells. Base SQL `exists()` does not.

## Merge chunk databases

`merge_databases(input_dir, table_name, end_process_query=None)` attaches every `*.db` in `input_dir` (except the destination file) into `staging_<table_name>`. Pass an `INSERT INTO table_name SELECT ...` query when you want rows moved out of staging.

`merge_row_by_row(input_dir, table_name, row_callback, flush_callback=None, read_batch_size=1000, flush_every_rows=None)` streams each row through your callback.

Both return a `MergeReport`:

| Field | Meaning |
| --- | --- |
| `processed_chunks` | Chunk files merged |
| `skipped_chunks` | Chunk files skipped |
| `processed_rows` | Rows seen by `merge_row_by_row` |
| `errors` | Per-file error strings |

```python
from scrape_kit import MergeReport

report: MergeReport = db.merge_databases(
    "chunks",
    "items",
    end_process_query="INSERT OR IGNORE INTO items SELECT * FROM staging_items",
)
print(report.processed_chunks, report.skipped_chunks, report.errors)
```

## Related guides

- [Errors](errors.md) for `StorageError`.
