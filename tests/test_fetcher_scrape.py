"""Batch scrape: mode dispatch, FAST fetch policy, and STEALTH engine passes."""

from unittest.mock import MagicMock, patch

import pytest

from scrape_kit.errors import FetcherError
from scrape_kit.fetcher.batch_types import ScrapeMode
from scrape_kit.fetcher.engine_adapter import ScraplingEngineAdapter
from scrape_kit.fetcher.web_fetcher import WebFetcher

pytestmark = pytest.mark.p0

A = "http://a.com"
B = "http://b.com"
C = "http://c.com"
BAD = "http://bad.com"


def _page(status=200, html="<html>stealthed</html>"):
    """Build a page mock exposing status and html_content."""
    page = MagicMock()
    page.status = status
    page.html_content = html
    return page


class _FakeSpider:
    """Spider stand-in replaying start_urls through the callback per pass.

    Records failed URLs so the adapter can defer them to the next pass and
    raises an engine failure when a pass ends with failures.
    """

    def __init__(self, pages=None, errors=()):
        """Initialize the spider with an optional page map and failure set."""
        self.start_urls = ()
        self.started = 0
        self._callback = lambda url, html, final=False: None
        self._errors_urls: list[str] = []
        self._pages = pages or {}
        self._errors = set(errors)

    def start(self):
        """Deliver one pass of pages, recording transport failures only."""
        self.started += 1
        self._errors_urls = []
        for url in list(self.start_urls):
            if url in self._errors:
                self._errors_urls.append(url)
                continue
            html = self._pages.get(url, _page().html_content)
            self._callback(url, html, False)
        if self._errors_urls:
            raise RuntimeError(", ".join(self._errors_urls))


def _wire(spider):
    """Return a _build_spider replacement configuring ``spider`` per pass."""

    def _build(*args):
        spider.start_urls = tuple(args[0])
        spider._callback = args[1]
        return spider

    return _build


class TestScrapeDispatch:
    """scrape() routing: modes, empty input, argument guards."""

    def test_edge_empty_urls_returns_without_calling_anything(self):
        """Empty input performs no work and never invokes the callback."""
        fetcher = WebFetcher()
        called = []
        fetcher.scrape([], callback=lambda u, h: called.append(u))
        assert called == []

    @patch.object(WebFetcher, "fetch", return_value="<html>ok</html>")
    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_normal_fast_mode_reaches_the_engine(self, mock_build, mock_fetch):
        """FAST mode dispatches exactly one engine pass through fetch()."""
        mock_build.side_effect = _wire(_FakeSpider())
        fetcher = WebFetcher()
        fetcher.scrape([A], callback=MagicMock(), mode=ScrapeMode.FAST)
        assert mock_build.call_count == 1
        assert mock_fetch.call_count == 1

    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_normal_stealth_mode_reaches_the_engine(self, mock_build):
        """STEALTH mode dispatches exactly one engine pass."""
        mock_build.side_effect = _wire(_FakeSpider())
        fetcher = WebFetcher()
        fetcher.scrape([A], callback=MagicMock(), mode=ScrapeMode.STEALTH)
        assert mock_build.call_count == 1

    @patch.object(WebFetcher, "fetch")
    def test_normal_fast_scrape_invokes_callback_for_each_url(self, mock_fetch):
        """Every URL is fetched and delivered exactly once in FAST mode."""
        mock_fetch.side_effect = lambda url, **kw: f"<html>{url}</html>"
        fetcher = WebFetcher()
        results = []
        fetcher.scrape([A, B, C], callback=lambda url, html: results.append(url), max_concurrency=2)
        assert sorted(results) == [A, B, C]

    @patch.object(WebFetcher, "fetch")
    def test_normal_stealth_mode_delivers_callback_for_each_url(self, mock_fetch):
        """STEALTH mode delivers pages from the engine session."""
        pages = {A: "<html>AAA</html>", B: "<html>BBB</html>"}
        fetcher = WebFetcher()
        got = []
        with patch.object(ScraplingEngineAdapter, "_build_spider", side_effect=_wire(_FakeSpider(pages=pages))):
            fetcher.scrape([A, B], callback=lambda u, h: got.append(u), mode=ScrapeMode.STEALTH)
        assert sorted(got) == [A, B]
        assert mock_fetch.call_count == 0

    def test_normal_scrape_stealth_empty_urls(self):
        """Empty STEALTH input performs no work and never calls the callback."""
        fetcher = WebFetcher()
        callback = MagicMock()
        fetcher.scrape([], callback, mode=ScrapeMode.STEALTH)
        assert callback.call_count == 0

    def test_error_scrape_unsupported_mode(self):
        """An unsupported mode is rejected before any work happens."""
        fetcher = WebFetcher()
        with pytest.raises(ValueError, match="Unsupported scrape mode"):
            fetcher.scrape([A], MagicMock(), mode="invalid_mode")

    def test_error_zero_concurrency_raises_value_error(self):
        """A non-positive concurrency is rejected before any fetch."""
        fetcher = WebFetcher()
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            fetcher.scrape([A], callback=lambda u, h: None, max_concurrency=0)


class TestFastBatchSemantics:
    """FAST batches deliver every URL and surface failures per URL."""

    @patch.object(WebFetcher, "fetch")
    def test_error_fast_failure_raises_with_url(self, mock_fetch):
        """A transport failure raises FetcherError naming the offending URL."""
        mock_fetch.side_effect = Exception("Network error")
        fetcher = WebFetcher()
        with pytest.raises(FetcherError, match=A):
            fetcher.scrape([A], callback=lambda u, h: None, mode=ScrapeMode.FAST)

    @patch.object(WebFetcher, "fetch")
    def test_edge_blocked_html_skips_callback(self, mock_fetch):
        """A blocked response is a fetch failure and never reaches the callback."""
        fetcher = WebFetcher(block_indicators=["blocked"])
        mock_fetch.side_effect = FetcherError("blocked")
        called = []
        with pytest.raises(FetcherError):
            fetcher.scrape([A], callback=lambda u, h: called.append(u), mode=ScrapeMode.FAST)
        assert called == []

    @patch.object(WebFetcher, "fetch")
    def test_error_fast_mode_callback_bug_propagates(self, mock_fetch):
        """A user-callback exception propagates immediately and is never retried."""
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()

        def bad_callback(url: str, html: str) -> None:
            raise ValueError("callback bug")

        with pytest.raises(ValueError, match="callback bug"):
            fetcher.scrape([A], callback=bad_callback, mode=ScrapeMode.FAST)
        assert mock_fetch.call_count == 1


class TestStealthBatchSemantics:
    """STEALTH batches deliver pages and summarize per-URL failures."""

    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_normal_stealth_batch_delivers_all_urls(self, mock_build):
        """Every URL is delivered once from the engine pass."""
        pages = {A: "<html>AAA</html>", B: "<html>BBB</html>"}
        spider = _FakeSpider(pages=pages)
        mock_build.side_effect = _wire(spider)
        fetcher = WebFetcher()
        got = []
        fetcher.scrape([A, B], callback=lambda u, h: got.append(u), mode=ScrapeMode.STEALTH, max_concurrency=2)
        assert sorted(got) == [A, B]
        assert spider.started == 1

    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_error_stealth_persistent_block_raises_with_summary(self, mock_build):
        """A permanently failing URL raises a summary naming that URL."""
        mock_build.side_effect = _wire(_FakeSpider(errors=[A]))
        fetcher = WebFetcher()
        with pytest.raises(FetcherError, match=A):
            fetcher.scrape([A], callback=lambda u, h: None, mode=ScrapeMode.STEALTH)

    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_error_stealth_partial_failure_summarises_and_keeps_good_result(self, mock_build):
        """Healthy URLs deliver before the failure summary is raised."""
        pages = {A: "<html>good</html>"}
        mock_build.side_effect = _wire(_FakeSpider(pages=pages, errors=[BAD]))
        fetcher = WebFetcher()
        got = []
        with pytest.raises(FetcherError, match=BAD):
            fetcher.scrape([A, BAD], callback=lambda u, h: got.append(u), mode=ScrapeMode.STEALTH, max_concurrency=2)
        assert got == [A]

    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_error_stealth_mode_callback_bug_propagates(self, mock_build):
        """A user-callback exception in STEALTH mode surfaces unwrapped."""
        mock_build.side_effect = _wire(_FakeSpider())
        fetcher = WebFetcher()

        def bad_callback(url: str, html: str) -> None:
            raise ValueError("callback bug")

        with pytest.raises(ValueError, match="callback bug"):
            fetcher.scrape([A], callback=bad_callback, mode=ScrapeMode.STEALTH)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
