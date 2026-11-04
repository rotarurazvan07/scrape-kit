"""Parse package facade: public exports, three-way sync."""

import pytest

from scrape_kit.parse import (
    Selector,
    ParseError,
)
from scrape_kit import parse as parse_module
from scrape_kit.errors import FetcherError

pytestmark = pytest.mark.p0


class TestPublicExports:
    """__all__ contains exactly the documented names."""

    def test_normal_all_contains_expected(self):
        expected = {
            "Selector",
            "Selectors",
            "adaptive_select",
            "relocate",
            "follow_links",
            "extract_links",
            "ParseError",
        }
        assert set(parse_module.__all__) == expected

    def test_normal_parse_function_exists(self):
        assert callable(parse_module.parse)

    def test_normal_ParseError_is_FetcherError(self):
        assert ParseError is FetcherError


class TestParseFunction:
    """Module-level parse() entry point."""

    def test_normal_returns_selector(self):
        sel = parse_module.parse("<html></html>", url="https://example.com")
        from scrape_kit.parse import Selector

        assert isinstance(sel, Selector)

    def test_normal_passes_adaptive_flag(self):
        sel = parse_module.parse("<html></html>", adaptive=True)
        assert isinstance(sel, Selector)

    def test_normal_passes_adaptive_domain(self):
        sel = parse_module.parse("<html></html>", adaptive_domain="example.com")
        assert isinstance(sel, Selector)

    def test_normal_passes_encoding(self):
        sel = parse_module.parse(b"<html></html>", encoding="utf-8")
        assert isinstance(sel, Selector)


class TestThreeWaySync:
    """scrape_kit/__init__.py exports parse names; docs/index.md updated."""

    def test_normal_top_level_init_exports_parse_names(self):
        import scrape_kit

        for name in [
            "Selector",
            "Selectors",
            "adaptive_select",
            "relocate",
            "follow_links",
            "extract_links",
            "ParseError",
        ]:
            assert hasattr(scrape_kit, name), f"missing {name} in top-level exports"

    def test_normal_parse_in_lazy_imports(self):
        import scrape_kit

        assert hasattr(scrape_kit, "parse")
