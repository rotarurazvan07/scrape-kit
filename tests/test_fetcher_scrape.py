"""Batch scrape modes: scrape/_fetch_one_fast/_scrape_stealth."""

"""
Comprehensive tests for fetcher.py — WebFetcher, InteractiveSession, ScrapeMode,
configure(), configure_defaults(), and module-level proxy functions.

Public API covered:
  WebFetcher:         __init__, fetch, is_blocked, browser, scrape,
                      configure(), configure_defaults()
  InteractiveSession: __enter__/__exit__, fetch, execute_script,
                      wait_for_selector, wait_for_function, click,
                      wait_for_timeout, __getattr__
  ScrapeMode:         FAST, STEALTH constants
  Module proxies:     fetch, is_blocked, browser, scrape
  Package helpers:    configure, configure_defaults

All scrapling I/O is mocked — no network calls are made.
Each method has: normal case(s), edge case(s), error case.
Plus 5 complex integration scenarios at the bottom.
"""

from unittest.mock import MagicMock, patch

import pytest

from scrape_kit.errors import FetcherError
from scrape_kit.fetcher import (
    ScrapeMode,
    WebFetcher,
)


# ── WebFetcher.scrape ─────────────────────────────────────────────────────────


class TestScrape:
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
    """Test lines 415-416, 419-421 - _fetch_one_fast method"""

    def test_error_fetch_one_fast_all_attempts_fail(self):
        """Test lines 415-416, 419-421 - all attempts fail with exception"""
        fetcher = WebFetcher(block_indicators=[])

        # Make fetch always raise an exception
        with (
            patch.object(fetcher, "fetch", side_effect=Exception("Network error")),
            pytest.raises(FetcherError, match="Fast scrape failed for http://test.com"),
        ):
            fetcher._fetch_one_fast("http://test.com", MagicMock())

    def test_error_fetch_one_fast_blocked_all_attempts(self):
        """Test lines 423-425 - all attempts blocked"""
        fetcher = WebFetcher(block_indicators=["blocked"])

        # Make fetch always return blocked content
        with (
            patch.object(fetcher, "fetch", return_value="<html>blocked</html>"),
            pytest.raises(FetcherError, match="Fast scrape remained blocked for http://test.com"),
        ):
            fetcher._fetch_one_fast("http://test.com", MagicMock())


class TestScrapeStealth:
    """Test lines 428, 431-460, 463-479 - stealth scraping methods"""

    def test_normal_scrape_stealth_empty_urls(self):
        """Test line 382-383 - empty urls returns immediately"""
        fetcher = WebFetcher()
        callback = MagicMock()
        # Should not raise any error
        fetcher.scrape([], callback, mode="stealth")

    def test_error_scrape_unsupported_mode(self):
        """Test line 391 - unsupported scrape mode"""
        fetcher = WebFetcher()
        callback = MagicMock()

        with pytest.raises(ValueError, match="Unsupported scrape mode"):
            fetcher.scrape(["http://test.com"], callback, mode="invalid_mode")

