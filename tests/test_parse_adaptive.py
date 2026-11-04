"""Adaptive element relocation helpers."""

import pytest

from scrape_kit.parse import Selectors, adaptive_select, relocate

pytestmark = pytest.mark.p0

HTML_V1 = """
<html><body><div class="product"><h1>Widget A</h1><span class="price">$19.99</span></div></body></html>
"""

HTML_V2 = """
<html><body><section class="item"><h2>Widget A</h2><p class="cost">$19.99</p></section></body></html>
"""


class TestAdaptiveSelect:
    """adaptive_select helper."""

    def test_normal_returns_list(self):
        items = adaptive_select(HTML_V1, "https://example.com", ".product", adaptive_domain="example.com")
        assert isinstance(items, Selectors)
        assert len(items) == 1

    def test_normal_auto_save_accepted(self):
        items = adaptive_select(HTML_V1, "https://example.com", ".product", auto_save=True, adaptive_domain="example.com")
        assert isinstance(items, Selectors)

    def test_normal_empty_selector_returns_empty(self):
        assert len(adaptive_select(HTML_V1, "https://example.com", ".nonexistent", adaptive_domain="example.com")) == 0

    def test_normal_domain_isolation(self):
        a = adaptive_select(HTML_V1, "https://a.com", ".product", adaptive_domain="a.com")
        b = adaptive_select(HTML_V1, "https://b.com", ".product", adaptive_domain="b.com")
        assert isinstance(a, Selectors)
        assert isinstance(b, Selectors)


class TestRelocate:
    """relocate helper."""

    def test_normal_unknown_identifier_returns_empty(self):
        items = relocate(HTML_V2, "https://example.com", "no-such-id", adaptive_domain="example.com")
        assert isinstance(items, Selectors)
        assert len(items) == 0

    def test_normal_percentage_parameter(self):
        items = relocate(HTML_V2, "https://example.com", "no-such-id", adaptive_domain="example.com", percentage=80)
        assert isinstance(items, Selectors)


class TestErrorBoundary:
    """Invalid input still returns a wrapped list."""

    def test_normal_invalid_html_still_returns_list(self):
        items = adaptive_select("not html", "https://example.com", ".x", adaptive_domain="example.com")
        assert isinstance(items, Selectors)


class TestInputGuards:
    """Validation guards for empty inputs and percentage bounds."""

    def test_error_empty_url_raises(self):
        with pytest.raises(ValueError, match="url must not be empty"):
            adaptive_select(HTML_V1, "", ".product", adaptive_domain="example.com")

    def test_error_empty_selector_raises(self):
        with pytest.raises(ValueError, match="selector must not be empty"):
            adaptive_select(HTML_V1, "https://example.com", "", adaptive_domain="example.com")

    def test_error_relocate_empty_url_raises(self):
        with pytest.raises(ValueError, match="url must not be empty"):
            relocate(HTML_V2, "", "some-id", adaptive_domain="example.com")

    def test_error_relocate_empty_identifier_raises(self):
        with pytest.raises(ValueError, match="identifier must not be empty"):
            relocate(HTML_V2, "https://example.com", "", adaptive_domain="example.com")

    def test_error_relocate_negative_percentage_raises(self):
        with pytest.raises(ValueError, match="percentage must be between 0 and 100"):
            relocate(HTML_V2, "https://example.com", "some-id", percentage=-5)

    def test_error_relocate_over_hundred_percentage_raises(self):
        with pytest.raises(ValueError, match="percentage must be between 0 and 100"):
            relocate(HTML_V2, "https://example.com", "some-id", percentage=150)
