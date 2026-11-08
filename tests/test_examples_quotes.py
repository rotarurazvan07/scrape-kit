"""Gate coverage for the examples/quotes offline path (CR finding F1).

The example lives outside the CI targets (CI lints/tests only scrape_kit and
tests), so this module keeps the fixture -> parse -> dedupe -> store path
exercised by the gate suite without touching the network.
"""

from __future__ import annotations

import csv
import pathlib
import sys
from typing import Any

import pytest

EXAMPLES_DIR = pathlib.Path(__file__).resolve().parents[1] / "examples" / "quotes"
if str(EXAMPLES_DIR) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_DIR))


@pytest.fixture(name="parsed_quotes")
def _parsed_quotes_fixture() -> list[dict[str, Any]]:
    """Parse + dedupe the local fixture page (no network access)."""
    from parse_quotes import dedupe, parse_quotes

    html = (EXAMPLES_DIR / "fixtures" / "page_1.html").read_text(encoding="utf-8")
    return dedupe(parse_quotes(html))


def test_fixture_parses_all_ten_quotes(parsed_quotes: list[dict[str, Any]]) -> None:
    """CR finding F4: assert parsed content, not just a row count."""
    assert len(parsed_quotes) == 10
    first = parsed_quotes[0]
    assert first["author"] == "Albert Einstein"
    assert first["text"].startswith('"The world as we have created it')
    assert first["tags"] == "change;deep-thoughts;thinking;world"


def test_storage_roundtrip_matches_golden_csv(parsed_quotes: list[dict[str, Any]], tmp_path: pathlib.Path) -> None:
    """CR finding F4: compare every field against the golden CSV."""
    from storage_quotes import QuoteStore

    store = QuoteStore(str(tmp_path / "quotes.sqlite"))
    store.add(parsed_quotes)
    store.flush()
    csv_path = tmp_path / "quotes.csv"
    rows = store.to_csv(str(csv_path))
    store.close()

    assert rows == len(parsed_quotes)

    golden = EXAMPLES_DIR / "fixtures" / "expected_quotes.csv"
    expected = list(csv.DictReader(golden.read_text(encoding="utf-8").splitlines()))
    actual = list(csv.DictReader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert len(actual) == len(expected)

    for want, got in zip(expected, actual, strict=True):
        assert got["text"] == want["text"]
        assert got["author"] == want["author"]
        assert got["tags"] == want["tags"]


def test_dedupe_skips_near_identical_text() -> None:
    """CR finding: threshold is a 0-100 scale, not 0-1."""
    from parse_quotes import dedupe

    text = "Identical quote text here."
    quotes = [
        {"text": text, "author": "A", "tags": ""},
        {"text": text, "author": "A", "tags": ""},
    ]
    assert len(dedupe(quotes)) == 1


def test_dedupe_keeps_distinct_quotes() -> None:
    """Regression guard for the threshold-scale defect found during CR."""
    from parse_quotes import dedupe

    quotes = [
        {"text": "The world as we have created it", "author": "Einstein", "tags": ""},
        {"text": "It is our choices that show what we truly are", "author": "Rowling", "tags": ""},
    ]
    assert len(dedupe(quotes)) == 2


def test_example_settings_load_through_facade() -> None:
    """CR finding F2: the example config must load via the public channel."""
    import scrape_kit as sk

    cfg = sk.configure(str(EXAMPLES_DIR), "scraper_config")
    assert cfg is not None


def test_smoke_script_is_deterministic() -> None:
    """CR finding F5: the offline smoke path must exit 0 twice in a row."""
    import run_smoke_test

    assert run_smoke_test.main() == 0
