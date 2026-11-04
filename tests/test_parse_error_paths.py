"""Error-path coverage for the parse package.

Every public method must convert an underlying failure into FetcherError
with the source URL attached. These tests drive each exception branch with a
stub that raises on any attribute access.
"""

import pytest

from scrape_kit.errors import FetcherError
from scrape_kit.parse import Selector, Selectors

pytestmark = pytest.mark.p0

HTML = '<html><body><div class="p"><span class="x">A</span></div></body></html>'
URL = "https://example.com"


class BrokenInner:
    """Stub whose every attribute/property raises, simulating a parser failure."""

    def __getattr__(self, name: str):
        def _raise(*args: object, **kwargs: object) -> None:
            raise RuntimeError(f"boom: {name}")

        return _raise

    @property
    def text(self):
        raise RuntimeError("boom: text")

    @property
    def attrib(self):
        raise RuntimeError("boom: attrib")

    @property
    def html_content(self):
        raise RuntimeError("boom: html_content")


class TestSelectorErrorBoundary:
    """Each Selector method wraps failures in FetcherError."""

    def _broken(self) -> Selector:
        sel = Selector(HTML, url=URL)
        sel._selector = BrokenInner()
        return sel

    def test_error_find_by_text(self):
        with pytest.raises(FetcherError, match="find_by_text failed"):
            self._broken().find_by_text("A")

    def test_error_find_by_regex(self):
        with pytest.raises(FetcherError, match="find_by_regex failed"):
            self._broken().find_by_regex(r"\d+")

    def test_error_find_similar(self):
        with pytest.raises(FetcherError, match="find_similar failed"):
            self._broken().find_similar()

    def test_error_get(self):
        with pytest.raises(FetcherError, match="get failed"):
            self._broken().get()

    def test_error_getall(self):
        with pytest.raises(FetcherError, match="getall failed"):
            self._broken().getall()

    def test_error_re(self):
        with pytest.raises(FetcherError, match="re failed"):
            self._broken().re(r"\d+")

    def test_error_re_first(self):
        with pytest.raises(FetcherError, match="re_first failed"):
            self._broken().re_first(r"\d+")

    def test_error_save(self):
        with pytest.raises(FetcherError, match="adaptive save failed"):
            self._broken().save({"a": 1}, "id")

    def test_error_retrieve(self):
        with pytest.raises(FetcherError, match="adaptive retrieve failed"):
            self._broken().retrieve("id")

    def test_error_relocate(self):
        with pytest.raises(FetcherError, match="adaptive relocate failed"):
            self._broken().relocate({"a": 1})

    def test_error_text_property(self):
        with pytest.raises(FetcherError, match="text property failed"):
            _ = self._broken().text

    def test_error_attrib_property(self):
        with pytest.raises(FetcherError, match="attrib property failed"):
            _ = self._broken().attrib

    def test_error_html_content_property(self):
        with pytest.raises(FetcherError, match="html_content property failed"):
            _ = self._broken().html_content


class TestSelectorsErrorBoundary:
    """Each Selectors method wraps failures in FetcherError."""

    def _broken(self) -> Selectors:
        items = Selector(HTML, url=URL).css(".p")
        items._selectors = BrokenInner()
        return items

    def test_error_list_get(self):
        with pytest.raises(FetcherError, match="list get failed"):
            self._broken().get()

    def test_error_list_getall(self):
        with pytest.raises(FetcherError, match="list getall failed"):
            self._broken().getall()

    def test_error_list_re(self):
        with pytest.raises(FetcherError, match="list re failed"):
            self._broken().re(r"\d+")

    def test_error_list_re_first(self):
        with pytest.raises(FetcherError, match="list re_first failed"):
            self._broken().re_first(r"\d+")

    def test_error_list_extract(self):
        with pytest.raises(FetcherError, match="list extract failed"):
            self._broken().extract()

    def test_error_list_extract_first(self):
        with pytest.raises(FetcherError, match="list extract_first failed"):
            self._broken().extract_first()

    def test_error_list_css(self):
        with pytest.raises(FetcherError, match="list css failed"):
            self._broken().css(".x")

    def test_error_list_xpath(self):
        with pytest.raises(FetcherError, match="list xpath failed"):
            self._broken().xpath("//span")

    def test_error_list_text_property(self):
        with pytest.raises(FetcherError, match="list text property failed"):
            _ = self._broken().text

    def test_error_list_attrib_property(self):
        with pytest.raises(FetcherError, match="list attrib property failed"):
            _ = self._broken().attrib

    def test_error_list_html_content_property(self):
        with pytest.raises(FetcherError, match="list html_content property failed"):
            _ = self._broken().html_content
