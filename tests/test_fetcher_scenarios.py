"""Fetcher integration scenarios (4th-tier test_scenario_)."""

from unittest.mock import patch

import pytest
from conftest import (
    SESSION_FETCH_TIMEOUT_MS,
    make_fetcher_config,
    make_interactive_session,
    make_page,
)

import scrape_kit.fetcher as fetcher_module
from scrape_kit.fetcher import (
    InteractiveSession,
    ScrapeMode,
    WebFetcher,
)
from scrape_kit.fetcher import is_blocked as module_is_blocked

pytestmark = pytest.mark.p1


# ── Complex Scenarios ─────────────────────────────────────────────────────────


class TestFetcherScenarios:
    """End-to-end fetcher journeys (4th-tier test_scenario_ integration)."""

    @patch("scrape_kit.fetcher.Fetcher")
    def test_scenario_two_failures_then_success_on_third(self, MockFetcher):
        MockFetcher.get.side_effect = [
            make_page(status=503),
            make_page(status=503),
            make_page("<html>Finally</html>"),
        ]
        fetcher = WebFetcher()
        result = fetcher.fetch("http://example.com", retries=3, backoff=0)
        assert "Finally" in result
        assert MockFetcher.get.call_count == 3

    @patch("scrape_kit.fetcher.Fetcher")
    @patch.object(WebFetcher, "_escalate_to_browser")
    def test_scenario_retry_indicator_exhausts_and_escalates(self, mock_escalate, MockFetcher):
        mock_escalate.return_value = "<html>Solved via browser</html>"
        MockFetcher.get.return_value = make_page("<html>just a moment</html>")
        fetcher = WebFetcher(retry_indicators=["just a moment"])
        result = fetcher.fetch("http://example.com", retries=1, backoff=0)
        mock_escalate.assert_called_once_with("http://example.com", "just a moment")
        assert result == "<html>Solved via browser</html>"

    @patch.object(WebFetcher, "fetch")
    def test_scenario_fast_scrape_with_concurrency_all_urls_processed(self, mock_fetch):
        mock_fetch.return_value = "<html>data</html>"
        fetcher = WebFetcher()
        results = []
        urls = [f"http://site{i}.com" for i in range(6)]
        fetcher.scrape(urls, callback=lambda u, h: results.append(u), mode=ScrapeMode.FAST, max_concurrency=3)
        assert sorted(results) == sorted(urls)

    @patch("scrape_kit.fetcher.Fetcher")
    def test_scenario_multiple_block_indicators_individually_detected(self, MockFetcher):
        fetcher = WebFetcher(block_indicators=["rate limited", "access denied", "captcha required"])
        assert fetcher.is_blocked("Sorry, rate limited right now") is True
        assert fetcher.is_blocked("<h1>Access Denied</h1>") is True
        assert fetcher.is_blocked("Please complete the captcha required") is True
        assert fetcher.is_blocked("<html>Welcome to our store</html>") is False

    def test_scenario_interactive_session_full_lifecycle(self):
        mock_session, mock_page = make_interactive_session(
            html="<html><title>Scraped</title></html>",
            eval_return="Scraped",
        )
        with InteractiveSession(mock_session) as session:
            resp = session.fetch("http://test.com", timeout=SESSION_FETCH_TIMEOUT_MS, wait_until="load")
            title = session.execute_script("return document.title")

        assert resp.html_content == "<html><title>Scraped</title></html>"
        assert title == "Scraped"
        mock_page.goto.assert_called_once_with("http://test.com", wait_until="load", timeout=SESSION_FETCH_TIMEOUT_MS)
        mock_page.close.assert_called_once()
        mock_session.close.assert_called_once()

    def test_scenario_configure_yaml_then_module_proxy_full_flow(self, tmp_path):
        """configure() from YAML → module proxies use the right indicators end-to-end."""
        cfg_dir = make_fetcher_config(tmp_path, block=["e2e_blocked"])
        WebFetcher.configure(str(cfg_dir))
        assert module_is_blocked("page contains e2e_blocked text") is True
        assert module_is_blocked("normal page") is False

    def test_scenario_multiple_configure_calls_last_one_wins(self, tmp_path):
        """Calling configure() twice replaces the shared instance."""
        cfg_a = make_fetcher_config(tmp_path, retry=["first"], dirname="cfg_a")
        cfg_b = make_fetcher_config(tmp_path, retry=["second"], dirname="cfg_b")
        WebFetcher.configure(str(cfg_a))
        first = fetcher_module._shared
        WebFetcher.configure(str(cfg_b))
        second = fetcher_module._shared
        assert first is not second
        assert second.retry_indicators == ["second"]
