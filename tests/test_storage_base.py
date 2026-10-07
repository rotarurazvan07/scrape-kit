"""BaseStorageManager queries + CRUD (issue #10 split)."""

import pandas as pd
import pytest

from scrape_kit.errors import StorageError

pytestmark = pytest.mark.p0


# ── fetch_rows ────────────────────────────────────────────────────────────────


class TestFetchRows:
    """fetch_rows returns column-addressable dict rows."""

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
    """fetch_dataframe materializes query results as a DataFrame."""

    def test_normal_returns_dataframe_with_correct_shape(self, populated_db):
        df = populated_db.fetch_dataframe("SELECT * FROM items")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert set(df.columns) >= {"name", "value"}

    def test_normal_column_values_match_db(self, populated_db):
        df = populated_db.fetch_dataframe("SELECT * FROM items ORDER BY name")
        assert list(df["name"]) == ["alpha", "beta", "gamma"]

    def test_normal_parameterized_query(self, populated_db):
        df = populated_db.fetch_dataframe("SELECT * FROM items WHERE name = ?", ("beta",))
        assert len(df) == 1
        assert df.iloc[0]["name"] == "beta"

    def test_edge_empty_table_returns_empty_dataframe(self, db):
        df = db.fetch_dataframe("SELECT * FROM items")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_error_invalid_query_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.fetch_dataframe("SELECT * FROM ghost_table")


# ── fetch_objects ────────────────────────────────────────────────────────────────


class TestFetchObjs:
    """fetch_objects maps rows through an optional mapper."""

    def test_normal_with_mapper_transforms_rows(self, populated_db):
        result = populated_db.fetch_objects(
            "SELECT * FROM items ORDER BY name",
            mapper=lambda r: r["name"].upper(),
        )
        assert result == ["ALPHA", "BETA", "GAMMA"]

    def test_normal_without_mapper_returns_list_of_dicts(self, populated_db):
        result = populated_db.fetch_objects("SELECT * FROM items ORDER BY name")
        assert all(isinstance(r, dict) for r in result)
        assert result[0]["name"] == "alpha"

    def test_normal_parameterized_with_mapper(self, populated_db):
        result = populated_db.fetch_objects(
            "SELECT * FROM items WHERE name = ?",
            params=("gamma",),
            mapper=lambda r: r["value"],
        )
        assert result == ["3"]

    def test_edge_no_rows_returns_empty_list(self, db):
        assert db.fetch_objects("SELECT * FROM items") == []

    def test_error_invalid_query_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.fetch_objects("SELECT * FROM no_such_table")


# ── execute_batch ─────────────────────────────────────────────────────────────


class TestExecuteBatch:
    """execute_batch commits all rows or rolls back entirely."""

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
    """create_index builds single/unique composite indexes idempotently."""

    def test_normal_creates_single_column_index(self, db):
        db.create_index("items", ["name"])
        cursor = db.conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_items_name'")
        assert cursor.fetchone() is not None

    def test_normal_creates_unique_multicolumn_index(self, db):
        db.create_index("items", ["name", "value"], unique=True)
        cursor = db.conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_items_name_value'")
        assert cursor.fetchone() is not None

    def test_edge_create_same_index_twice_is_idempotent(self, db):
        db.create_index("items", ["name"])
        db.create_index("items", ["name"])
        rows = db.fetch_rows("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_items_name'")
        assert len(rows) == 1  # IF NOT EXISTS — exactly one index after double create

    def test_error_invalid_table_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.create_index("nonexistent_table", ["col"])

    def test_edge_invalid_column_creates_index_silently(self, db):
        db.create_index("items", ["no_such_column"])
        rows = db.fetch_rows("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_items_no_such_column'")
        assert len(rows) == 1  # SQLite does not validate columns at index creation


# ── exists ────────────────────────────────────────────────────────────────────


class TestBaseExists:
    """exists checks value presence across types and missing columns."""

    @pytest.mark.smoke
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

    def test_edge_nonexistent_column_returns_false(self, db):
        assert db.exists("items", "no_such_column", "val") is False

    def test_error_nonexistent_table_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.exists("ghost_table", "name", "val")


# ── insert ────────────────────────────────────────────────────────────────────


class TestBaseInsert:
    """insert persists rows incl. NULL handling and constraint errors."""

    @pytest.mark.smoke
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


class TestClearTable:
    """clear_table drops all rows but keeps the schema."""

    def test_normal_removes_all_rows(self, populated_db):
        populated_db.clear_table("items")
        assert populated_db.fetch_rows("SELECT * FROM items") == []

    def test_normal_table_structure_intact_after_clear(self, populated_db):
        populated_db.clear_table("items")
        populated_db.insert("items", {"name": "fresh", "value": "new"})
        assert len(populated_db.fetch_rows("SELECT * FROM items")) == 1

    def test_edge_clearing_already_empty_table_is_noop(self, db):
        db.clear_table("items")  # no rows to delete — should not raise
        assert db.fetch_rows("SELECT * FROM items") == []

    def test_error_nonexistent_table_raises_storage_error(self, db):
        with pytest.raises(StorageError):
            db.clear_table("nonexistent_table")
