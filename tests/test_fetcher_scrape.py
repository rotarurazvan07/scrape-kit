"""Batch scrape modes: scrape/_fetch_one_fast/_scrape_stealth."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scrape_kit.errors import FetcherError
from scrape_kit.fetcher import (
    ScrapeMode,
    WebFetcher,
)

pytestmark = pytest.mark.p0


# ── WebFetcher.scrape ─────────────────────────────────────────────────────────


class TestScrape:
    """Batch scrape dispatch: FAST/STEALTH modes and invalid-mode rejection."""

    def test_edge_empty_urls_returns_without_calling_anything(self):
        fetcher = WebFetcher()
        called = []
        fetcher.scrape([], callback=lambda u, h: called.append(u))
        assert called == []

    @patch.object(WebFetcher, "_scrape_fast")
    def test_normal_fast_mode_delegates_to_scrape_fast(self, mock_fast):
        fetcher = WebFetcher()
        fetcher.scrape(["http://a.com"], callback=MagicMock(), mode=ScrapeMode.FAST)
        mock_fast.assert_called_once()

    @patch.object(WebFetcher, "_scrape_stealth")
    def test_normal_stealth_mode_delegates_to_scrape_stealth(self, mock_stealth):
        fetcher = WebFetcher()
        fetcher.scrape(["http://a.com"], callback=MagicMock(), mode=ScrapeMode.STEALTH)
        mock_stealth.assert_called_once()

    @patch.object(WebFetcher, "fetch")
    def test_normal_fast_scrape_invokes_callback_for_each_url(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()
        results = []
        fetcher.scrape(
            ["http://a.com", "http://b.com", "http://c.com"],
            callback=lambda url, html: results.append(url),
            mode=ScrapeMode.FAST,
            max_concurrency=2,
        )
        assert sorted(results) == ["http://a.com", "http://b.com", "http://c.com"]

    @patch.object(WebFetcher, "fetch")
    def test_edge_blocked_html_skips_callback(self, mock_fetch):
        fetcher = WebFetcher(block_indicators=["blocked"])
        mock_fetch.return_value = "<html>blocked</html>"
        called = []
        with pytest.raises(FetcherError):
            fetcher.scrape(["http://example.com"], callback=lambda u, h: called.append(u), mode=ScrapeMode.FAST)
        assert called == []

    def test_error_invalid_mode_raises_value_error(self):
        fetcher = WebFetcher()
        with pytest.raises(ValueError, match="Unsupported scrape mode"):
            fetcher.scrape(["http://a.com"], callback=MagicMock(), mode="invalid")


class TestFetchOneFast:
    """_fetch_one_fast retry exhaustion and blocked-content failure."""

    def test_error_fetch_one_fast_all_attempts_fail(self):
        """_fetch_one_fast wraps exhausted fetch exceptions in FetcherError."""
        fetcher = WebFetcher(block_indicators=[])

        # Make fetch always raise an exception
        with (
            patch.object(fetcher, "fetch", side_effect=Exception("Network error")),
            pytest.raises(FetcherError, match="Fast scrape failed for http://test.com"),
        ):
            fetcher._fetch_one_fast("http://test.com", MagicMock())

    def test_error_fetch_one_fast_blocked_all_attempts(self):
        """_fetch_one_fast raises FetcherError when every attempt stays blocked."""
        fetcher = WebFetcher(block_indicators=["blocked"])

        # Make fetch always return blocked content
        with (
            patch.object(fetcher, "fetch", return_value="<html>blocked</html>"),
            pytest.raises(FetcherError, match="Fast scrape remained blocked for http://test.com"),
        ):
            fetcher._fetch_one_fast("http://test.com", MagicMock())


class TestScrapeStealth:
    """Scrape dispatch: empty input short-circuits, unknown modes raise."""

    def test_normal_scrape_stealth_empty_urls(self):
        """scrape() with no URLs performs no work and never calls the callback."""
        fetcher = WebFetcher()
        callback = MagicMock()
        # Should not raise any error
        fetcher.scrape([], callback, mode="stealth")
        assert callback.call_count == 0  # empty input — no work, no callback

    def test_error_scrape_unsupported_mode(self):
        """scrape() rejects an unsupported mode with ValueError."""
        fetcher = WebFetcher()
        callback = MagicMock()

        with pytest.raises(ValueError, match="Unsupported scrape mode"):
            fetcher.scrape(["http://test.com"], callback, mode="invalid_mode")


class TestScrapeStealthPipeline:
    """Stealth batch pipeline: async queue workers, per-URL fetch, retry escalation, callback delivery (#7)."""

    @staticmethod
    def _page(status=200, html="<html>stealthed</html>"):
        page = MagicMock()
        page.status = status
        page.html_content = html
        return page

    @staticmethod
    def _stealth_cm(fetch_impl):
        """AsyncStealthySession context-manager mock whose inner .fetch uses fetch_impl."""
        cm = AsyncMock()
        inner = cm.__aenter__.return_value
        inner.fetch = AsyncMock(side_effect=fetch_impl)
        return cm

    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("scrape_kit.fetcher.AsyncStealthySession")
    def test_normal_stealth_batch_delivers_callbacks(self, MockSession, mock_sleep):
        pages = {"http://a.com": self._page(html="<html>AAA</html>"), "http://b.com": self._page(html="<html>BBB</html>")}

        async def fetch_impl(url, **kwargs):
            return pages[url]

        MockSession.return_value = self._stealth_cm(fetch_impl)
        fetcher = WebFetcher()
        got = []
        fetcher._scrape_stealth(list(pages), lambda u, h: got.append((u, h)), max_concurrency=2)
        assert sorted(u for u, _ in got) == ["http://a.com", "http://b.com"]  # every URL delivered
        assert dict(got)["http://a.com"] == "<html>AAA</html>"
        # session entered exactly once, with capped concurrency and cloudflare solving
        MockSession.assert_called_once_with(max_pages=2, headless=True, solve_cloudflare=True)
        mock_sleep.assert_not_awaited()  # happy path: no retry backoff

    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("scrape_kit.fetcher.AsyncStealthySession")
    def test_normal_stealth_retries_429_then_succeeds(self, MockSession, mock_sleep):
        ok = self._page()
        cm = self._stealth_cm([self._page(status=429), self._page(status=429), ok])
        MockSession.return_value = cm
        fetcher = WebFetcher()
        got = []
        fetcher._scrape_stealth(["http://a.com"], lambda u, h: got.append(u), max_concurrency=1)
        assert got == ["http://a.com"]  # delivered after backoff
        delays = [c.args[0] for c in mock_sleep.await_args_list]
        assert delays == [30, 60]  # 30 * attempt anti-ban backoff
        cm.__aenter__.return_value.fetch.assert_called_with(
            "http://a.com", disable_resources=False, network_idle=True, timeout=90000
        )

    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("scrape_kit.fetcher.AsyncStealthySession")
    def test_error_stealth_persistently_blocked_raises(self, MockSession, mock_sleep):
        MockSession.return_value = self._stealth_cm([self._page(status=503)] * 4)
        fetcher = WebFetcher()
        with pytest.raises(FetcherError, match="Stealth scrape had 1 failures"):
            fetcher._scrape_stealth(["http://a.com"], lambda u, h: None, max_concurrency=1)
        # three backoffs (attempts 1-3), attempt 4 raises the per-URL failure
        assert len(mock_sleep.await_args_list) == 3

    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("scrape_kit.fetcher.AsyncStealthySession")
    def test_error_stealth_fetch_exception_exhausts_retries(self, MockSession, mock_sleep):
        async def fetch_impl(url, **kwargs):
            raise RuntimeError("page crashed")

        MockSession.return_value = self._stealth_cm(fetch_impl)
        fetcher = WebFetcher()
        with pytest.raises(FetcherError, match="Stealth scrape had 1 failures"):
            fetcher._scrape_stealth(["http://a.com"], lambda u, h: None, max_concurrency=1)
        assert len(mock_sleep.await_args_list) == 3  # 15 * attempt backoffs for attempts 1-3

    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("scrape_kit.fetcher.AsyncStealthySession")
    def test_error_stealth_partial_failure_summarises_and_keeps_good_result(self, MockSession, mock_sleep):
        async def fetch_impl(url, **kwargs):
            if "bad" in url:
                raise RuntimeError("boom")
            return self._page()

        MockSession.return_value = self._stealth_cm(fetch_impl)
        fetcher = WebFetcher()
        got = []
        with pytest.raises(FetcherError, match="Stealth scrape had 1 failures. Sample: http://bad.com"):
            fetcher._scrape_stealth(["http://good.com", "http://bad.com"], lambda u, h: got.append(u), max_concurrency=2)
        assert got == ["http://good.com"]  # the healthy URL still delivered before the summary raise
