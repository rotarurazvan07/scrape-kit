"""Shared fixture layer for the scrape-kit test suite (issue #9).

Promotes the per-file helpers into one source of truth: factories
(make_page, make_interactive_session, make_fetcher_config, make_matching_cfg,
make_cfg, create_items_schema, make_chunk), fixtures (reset_shared, engine, db,
populated_db, buffered_db) and the named constants from issue #14. Test
modules import helpers directly from this conftest (same-directory import
under pytest's default import mode).
"""

import contextlib
import copy
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

import scrape_kit.fetcher as fetcher_module
from scrape_kit.matching import SimilarityEngine
from scrape_kit.storage import BaseStorageManager, BufferedStorageManager

# ── Named constants (issue #14 — magic literals → names) ──────────────────────

CLICK_IDLE_MS = 2000
CLICK_HARD_CAP_MS = 15000
CLICK_IDLE_ALT_MS = 5000
HARD_CAP_MULTIPLIER = 6
CLICK_HARD_CAP_DEFAULT_MS = HARD_CAP_MULTIPLIER * CLICK_IDLE_ALT_MS
WAIT_SELECTOR_MS = 5000
WAIT_FUNCTION_MS = 10000
WAIT_TIMEOUT_MS = 2000
SESSION_FETCH_TIMEOUT_MS = 30000
ESCALATE_TIMEOUT_MS = 120000

THRESHOLD_LENIENT = 40
THRESHOLD_DEFAULT = 65
THRESHOLD_MODERATE = 70
THRESHOLD_EXACT = 95
THRESHOLD_HIGH = 80

ITEMS_TABLE = "items"
BUFFERED_DB_NAME = "buf.db"


def pytest_configure(config):
    """Register priority markers (issue #13) — conftest-only, no pytest.ini."""
    config.addinivalue_line("markers", "smoke: core public-flow journeys — the <1 min pre-commit subset")
    config.addinivalue_line("markers", "p0: highest-priority fast paths (core contract)")
    config.addinivalue_line("markers", "p1: integration scenarios and concurrency behaviour")


# ── Fetcher factories ──────────────────────────────────────────────────────────


def make_page(html: str = "<html>OK</html>", status: int = 200) -> MagicMock:
    """Build a mock scrapling page with html content and status."""
    page = MagicMock()
    page.html_content = html
    page.status = status
    page.status_code = status
    return page


def make_interactive_session(html: str = "<html>page</html>", eval_return=None):
    """Build a (session, page) mock pair wired for InteractiveSession tests."""
    mock_session = MagicMock()
    mock_page = MagicMock()
    mock_session.context.new_page.return_value = mock_page
    mock_page.content.return_value = html
    mock_page.evaluate.return_value = eval_return
    return mock_session, mock_page


def make_fetcher_config(tmp_path, retry=(), block=(), name="scraper_config.yaml", dirname="config") -> Path:
    """Write a scraper-config YAML and return its directory path (issue #9)."""
    cfg_dir = tmp_path / dirname
    cfg_dir.mkdir(parents=True, exist_ok=True)
    payload = {"retry_indicators": list(retry), "block_indicators": list(block)}
    (cfg_dir / name).write_text(yaml.safe_dump(payload), encoding="utf-8")
    return cfg_dir


@pytest.fixture(autouse=True)
def reset_shared():
    """Reset the fetcher module-global shared instance around every test."""
    old = fetcher_module._shared
    fetcher_module._shared = None
    yield
    fetcher_module._shared = old


# ── Matching factory ───────────────────────────────────────────────────────────

RICH_CONFIG = {
    "threshold": 65,
    "acronyms": {
        "fc": "football club",
        "utd": "united",
        "afc": "athletic football club",
    },
    "synonyms": {
        "man city": "manchester city",
        "barca": "fc barcelona",
    },
    "weights": {
        "token": 0.5,
        "substr": 0.1,
        "phonetic": 0.1,
        "ratio": 0.3,
    },
}


def make_matching_cfg(**overrides) -> dict:
    """Fresh deep copy of RICH_CONFIG with overrides applied (issue #9)."""
    cfg = copy.deepcopy(RICH_CONFIG)
    cfg.update(overrides)
    return cfg


@pytest.fixture
def engine():
    """Default SimilarityEngine pre-loaded with the rich config."""
    return SimilarityEngine(copy.deepcopy(RICH_CONFIG))


# ── Settings factory ───────────────────────────────────────────────────────────


def make_cfg(tmp_path, structure: dict) -> Path:
    """Recursively write {relative_path: yaml_content_str} into tmp_path/config."""
    cfg = tmp_path / "config"
    for rel, content in structure.items():
        target = cfg / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return cfg


# ── Storage factories ──────────────────────────────────────────────────────────


def create_items_schema(conn, table: str = ITEMS_TABLE) -> None:
    """Create the canonical items table (issue #14 — one DDL, not four variants)."""
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, value TEXT)")


class MockDB(BaseStorageManager):
    """Concrete subclass with a simple two-table schema for testing."""

    def _create_tables(self):
        with self.db_lock:
            create_items_schema(self.conn)
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tags (
                    id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    tag     TEXT
                )
                """
            )
            self.conn.commit()


def make_chunk(path, rows, table=ITEMS_TABLE):
    """Create a standalone .db chunk with the canonical items schema."""
    conn = sqlite3.connect(str(path))
    create_items_schema(conn, table)
    conn.executemany(f"INSERT INTO {table} (name, value) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


@pytest.fixture
def db(tmp_path):
    """Fresh two-table MockDB under tmp_path, closed afterwards."""
    manager = MockDB(str(tmp_path / "test.db"))
    yield manager
    with contextlib.suppress(Exception):
        manager.flush_and_close()


@pytest.fixture
def populated_db(tmp_path):
    """MockDB pre-seeded with three items rows, closed afterwards."""
    manager = MockDB(str(tmp_path / "populated.db"))
    manager.conn.executemany(
        "INSERT INTO items (name, value) VALUES (?, ?)",
        [("alpha", "1"), ("beta", "2"), ("gamma", "3")],
    )
    manager.conn.commit()
    yield manager
    with contextlib.suppress(Exception):
        manager.flush_and_close()


@pytest.fixture
def buffered_db(tmp_path):
    """BufferedStorageManager bound to a seeded items table, closed afterwards."""
    conn = sqlite3.connect(str(tmp_path / "buffer.db"))
    create_items_schema(conn)
    conn.executemany("INSERT INTO items VALUES (?, ?, ?)", [(1, "alpha", "a"), (2, "beta", "b")])
    conn.commit()
    conn.close()
    manager = BufferedStorageManager(str(tmp_path / "buffer.db"), ITEMS_TABLE)
    yield manager
    with contextlib.suppress(Exception):
        manager.close()
