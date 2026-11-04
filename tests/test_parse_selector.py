"""Selector core: construction, selection, extraction, properties."""

import pytest

from scrape_kit.parse import Selector, Selectors

pytestmark = pytest.mark.p0

HTML = """
<html>
<body>
<div class="product">
    <h1 class="title">Widget A</h1>
    <span class="price">$19.99</span>
</div>
<div class="product">
    <h1 class="title">Widget B</h1>
    <span class="price">$29.99</span>
</div>
<a href="/page/1">Link 1</a>
<a href="/page/2">Link 2</a>
</body>
</html>
"""


class TestInit:
    """Selector construction."""

    def test_normal_html_string_parses(self):
        sel = Selector(HTML, url="https://example.com")
        assert sel.url == "https://example.com"

    def test_normal_bytes_parses(self):
        assert isinstance(Selector(HTML.encode(), url="https://example.com"), Selector)

    def test_normal_adaptive_flag_accepted(self):
        assert isinstance(Selector(HTML, url="https://example.com", adaptive=True), Selector)

    def test_normal_invalid_html_still_constructs(self):
        assert isinstance(Selector("not html", url="https://example.com"), Selector)

    def test_error_empty_html_raises(self):
        with pytest.raises(ValueError, match="html must not be empty"):
            Selector("", url="https://example.com")

    def test_normal_repr(self):
        assert "Selector" in repr(Selector(HTML, url="https://example.com"))


class TestSelection:
    """CSS, XPath, find_by_text, find_similar, find_by_regex."""

    def setup_method(self):
        self.sel = Selector(HTML, url="https://example.com")

    def test_normal_css_returns_list_type(self):
        items = self.sel.css(".product")
        assert isinstance(items, Selectors)
        assert len(items) == 2
        assert all(isinstance(i, Selector) for i in items)

    def test_normal_xpath_returns_list_type(self):
        assert len(self.sel.xpath("//div[@class='product']")) == 2

    def test_normal_find_by_text_first(self):
        assert isinstance(self.sel.find_by_text("Widget A", first_match=True), Selector)

    def test_normal_find_by_text_all(self):
        assert isinstance(self.sel.find_by_text("Widget A", first_match=False), Selectors)

    def test_normal_find_by_text_partial(self):
        assert isinstance(self.sel.find_by_text("Widg", partial=True), Selector)

    def test_normal_find_by_regex_first(self):
        assert isinstance(self.sel.find_by_regex(r"\$\d+\.\d+", first_match=True), Selector)

    def test_normal_find_by_regex_all(self):
        assert isinstance(self.sel.find_by_regex(r"\$\d+\.\d+", first_match=False), Selectors)

    def test_normal_find_similar(self):
        base = self.sel.css(".product")[0]
        assert isinstance(base.find_similar(similarity_threshold=0.1), Selectors)

    def test_normal_empty_selector_returns_empty_list(self):
        assert len(self.sel.css(".nonexistent")) == 0


class TestExtraction:
    """get, getall, text, attrib, html_content."""

    def setup_method(self):
        self.sel = Selector(HTML, url="https://example.com")

    def test_normal_css_getall_strings(self):
        prices = self.sel.css(".price").getall()
        assert len(prices) == 2
        assert all("$" in p for p in prices)

    def test_normal_css_get_first_string(self):
        price = self.sel.css(".price").get()
        assert isinstance(price, str)
        assert "$" in price

    def test_normal_title_text(self):
        assert "Widget A" in self.sel.css(".title").first().text

    def test_normal_attrib_property(self):
        assert self.sel.css("a").first().attrib.get("href") == "/page/1"

    def test_normal_html_content(self):
        assert "Widget" in self.sel.css(".product").first().html_content


class TestListHelpers:
    """Selectors: len, iteration, indexing, first/last, get/getall."""

    def setup_method(self):
        self.sel = Selector(HTML, url="https://example.com")
        self.items = self.sel.css(".product")

    def test_normal_len(self):
        assert len(self.items) == 2

    def test_normal_iteration(self):
        assert sum(1 for _ in self.items) == 2

    def test_normal_indexing(self):
        assert isinstance(self.items[0], Selector)

    def test_normal_slice_returns_list_type(self):
        assert isinstance(self.items[0:1], Selectors)

    def test_normal_first_and_last(self):
        assert self.items.first().css(".title").first().text.strip() == "Widget A"
        assert self.items.last().css(".title").first().text.strip() == "Widget B"

    def test_normal_getall(self):
        assert len(self.items.getall()) == 2

    def test_normal_get_returns_string(self):
        assert isinstance(self.items.get(), str)

    def test_normal_get_default_on_empty(self):
        assert self.sel.css(".nonexistent").get("fallback") == "fallback"

    def test_normal_repr(self):
        assert "Selectors" in repr(self.items)


class TestAdaptiveMethods:
    """save, retrieve, relocate on Selector."""

    def test_normal_save_retrieve_roundtrip(self):
        sel = Selector(HTML, url="https://example.com", adaptive=True, adaptive_domain="example.com")
        price = sel.css(".price")[0]
        sel.save(price, "price")
        stored = sel.retrieve("price")
        assert stored is None or isinstance(stored, dict)

    def test_normal_relocate_unknown_returns_empty(self):
        sel = Selector(HTML, url="https://example.com", adaptive=True, adaptive_domain="example.com")
        assert len(sel.relocate("nonexistent-identifier")) == 0


class TestGuards:
    """Validation guards for empty selectors and percentage bounds."""

    def setup_method(self):
        self.sel = Selector(HTML, url="https://example.com")

    def test_error_empty_css_selector_raises(self):
        with pytest.raises(ValueError, match="selector must not be empty"):
            self.sel.css("")

    def test_error_empty_xpath_selector_raises(self):
        with pytest.raises(ValueError, match="selector must not be empty"):
            self.sel.xpath("")

    def test_error_css_percentage_out_of_range_raises(self):
        with pytest.raises(ValueError, match="percentage must be between 0 and 100"):
            self.sel.css(".price", percentage=-1)

    def test_error_xpath_percentage_out_of_range_raises(self):
        with pytest.raises(ValueError, match="percentage must be between 0 and 100"):
            self.sel.xpath("//a", percentage=101)

    def test_normal_css_accepts_adaptive_flag(self):
        assert isinstance(self.sel.css(".price", adaptive=True), Selectors)

    def test_normal_xpath_accepts_auto_save(self):
        assert isinstance(self.sel.xpath("//a", auto_save=True), Selectors)

    def test_error_relocate_percentage_out_of_range_raises(self):
        with pytest.raises(ValueError, match="percentage must be between 0 and 100"):
            self.sel.relocate("some-id", percentage=200)

    def test_normal_first_property_exists(self):
        assert isinstance(Selector.first, property)

    def test_normal_last_property_exists(self):
        assert isinstance(Selector.last, property)

    def test_normal_markdown_without_rag_extra_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="scrapling\[rag\] extra"):
            Selector(HTML, url="https://example.com").markdown()
