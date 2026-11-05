"""Link following and extraction helpers."""

import pytest

from scrape_kit.errors import FetcherError
from scrape_kit.parse import extract_links, follow_links

pytestmark = pytest.mark.p0

HTML = """
<html>
<body>
<a href="/page/1">Link 1</a>
<a href="/page/2">Link 2</a>
<a href="https://external.com/other">External</a>
</body>
</html>
"""


class TestFollowLinks:
    """follow_links helper."""

    def test_normal_html_string_returns_absolute_urls(self):
        urls = follow_links(HTML, "a", base_url="https://example.com")
        assert len(urls) == 3
        assert all(u.startswith("https://") for u in urls)
        assert "https://example.com/page/1" in urls
        assert "https://example.com/page/2" in urls
        assert "https://external.com/other" in urls

    def test_normal_bytes_html(self):
        urls = follow_links(HTML.encode(), "a", base_url="https://example.com")
        assert len(urls) == 3

    def test_normal_xpath_selector(self):
        urls = follow_links(HTML, "//a", base_url="https://example.com")
        assert len(urls) == 3

    def test_normal_relative_urls_resolved(self):
        # hrefs are root-relative, so they resolve against the domain root
        urls = follow_links(HTML, "a", base_url="https://example.com/base/")
        assert "https://example.com/page/1" in urls
        assert all(u.startswith("https://") for u in urls)

    def test_normal_external_url_preserved(self):
        urls = follow_links(HTML, "a", base_url="https://example.com")
        assert "https://external.com/other" in urls

    def test_normal_no_matches_returns_empty(self):
        urls = follow_links(HTML, ".nonexistent", base_url="https://example.com")
        assert urls == []

    def test_error_missing_base_url_raises_fetcher_error(self):
        with pytest.raises(FetcherError, match="base_url required"):
            follow_links(HTML, "a")


class TestExtractLinks:
    """extract_links helper with metadata."""

    def test_normal_returns_list_of_dicts(self):
        links = extract_links(HTML, "a", base_url="https://example.com")
        assert len(links) == 3
        for link in links:
            assert "url" in link
            assert "text" in link
            assert "attributes" in link
            assert isinstance(link["attributes"], dict)
            assert link["url"].startswith("https://")

    def test_normal_text_and_attributes(self):
        links = extract_links(HTML, "a", base_url="https://example.com")
        texts = [link["text"] for link in links]
        assert "Link 1" in texts
        assert "Link 2" in texts
        assert "External" in texts
        assert all("href" in link["attributes"] for link in links)

    def test_normal_xpath_selector(self):
        links = extract_links(HTML, "//a", base_url="https://example.com")
        assert len(links) == 3

    def test_error_missing_base_url_raises_fetcher_error(self):
        with pytest.raises(FetcherError, match="base_url required"):
            extract_links(HTML, "a")


class TestFollowLinksWithResponse:
    """follow_links/extract_links with scrapling Response objects."""

    def test_normal_with_response_object(self):
        from scrapling.engines.toolbelt.custom import Response as ScraplingResponse

        response = ScraplingResponse(
            url="https://example.com",
            content=HTML,
            status=200,
            reason="OK",
            cookies={},
            headers={},
            request_headers={},
            encoding="utf-8",
            method="GET",
        )
        urls = follow_links(response, "a")
        assert len(urls) == 3
        assert "https://example.com/page/1" in urls

    def test_normal_extract_with_response(self):
        from scrapling.engines.toolbelt.custom import Response as ScraplingResponse

        response = ScraplingResponse(
            url="https://example.com",
            content=HTML,
            status=200,
            reason="OK",
            cookies={},
            headers={},
            request_headers={},
            encoding="utf-8",
            method="GET",
        )
        links = extract_links(response, "a")
        assert len(links) == 3


class TestErrorBoundary:
    """FetcherError wrapping."""

    def test_error_invalid_html_returns_empty(self):
        urls = follow_links("not html", "a", base_url="https://example.com")
        assert urls == []


class TestSelectorDispatch:
    """selector_type parameter and empty-selector guards."""

    def test_normal_explicit_css_selector_type(self):
        urls = follow_links(HTML, "a", base_url="https://example.com", selector_type="css")
        assert len(urls) == 3

    def test_normal_explicit_xpath_selector_type(self):
        urls = follow_links(HTML, "//a", base_url="https://example.com", selector_type="xpath")
        assert len(urls) == 3

    def test_normal_extract_explicit_css_selector_type(self):
        links = extract_links(HTML, "a", base_url="https://example.com", selector_type="css")
        assert len(links) == 3

    def test_normal_extract_explicit_xpath_selector_type(self):
        links = extract_links(HTML, "//a", base_url="https://example.com", selector_type="xpath")
        assert len(links) == 3

    def test_error_invalid_selector_type_raises(self):
        with pytest.raises(ValueError, match="selector_type must be"):
            follow_links(HTML, "a", base_url="https://example.com", selector_type="bogus")

    def test_error_extract_invalid_selector_type_raises(self):
        with pytest.raises(ValueError, match="selector_type must be"):
            extract_links(HTML, "a", base_url="https://example.com", selector_type="bogus")

    def test_error_empty_selector_raises(self):
        with pytest.raises(ValueError, match="selector must not be empty"):
            follow_links(HTML, "", base_url="https://example.com")

    def test_error_extract_empty_selector_raises(self):
        with pytest.raises(ValueError, match="selector must not be empty"):
            extract_links(HTML, "", base_url="https://example.com")
