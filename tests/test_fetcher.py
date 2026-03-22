"""
Comprehensive tests for fetcher.py — WebFetcher, InteractiveSession, ScrapeMode.

Public API covered:
  WebFetcher:         __init__, fetch, is_blocked, browser, scrape
  InteractiveSession: __enter__/__exit__, fetch, execute_script,
                      wait_for_selector, wait_for_function, click,
                      wait_for_timeout, __getattr__
  ScrapeMode:         FAST, STEALTH constants

All scrapling I/O is mocked — no network calls are made.
Each method has: normal case(s), edge case(s), error case.
Plus 5 complex integration scenarios at the bottom.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

from fetcher import WebFetcher, InteractiveSession, ScrapeMode
from errors import FetcherError


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_page(html: str = "<html>OK</html>", status: int = 200) -> MagicMock:
    page = MagicMock()
    page.html_content = html
    page.status = status
    page.status_code = status
    return page


def make_interactive_session(html: str = "<html>page</html>", eval_return=None):
    """Return (mock_scrapling_session, mock_page) with sensible defaults."""
    mock_session = MagicMock()
    mock_page = MagicMock()
    mock_session.context.new_page.return_value = mock_page
    mock_page.content.return_value = html
    mock_page.evaluate.return_value = eval_return
    return mock_session, mock_page


# ── ScrapeMode ────────────────────────────────────────────────────────────────

class TestScrapeMode:
    def test_normal_fast_constant(self):
        assert ScrapeMode.FAST == "fast"

    def test_normal_stealth_constant(self):
        assert ScrapeMode.STEALTH == "stealth"

    def test_edge_constants_are_strings(self):
        assert isinstance(ScrapeMode.FAST, str)
        assert isinstance(ScrapeMode.STEALTH, str)


# ── WebFetcher.__init__ ───────────────────────────────────────────────────────

class TestWebFetcherInit:
    def test_normal_custom_indicators_stored(self):
        fetcher = WebFetcher(retry_indicators=["retry_me"], block_indicators=["blocked"])
        assert fetcher.retry_indicators == ["retry_me"]
        assert fetcher.block_indicators == ["blocked"]

    def test_edge_defaults_are_empty_lists(self):
        fetcher = WebFetcher()
        assert fetcher.retry_indicators == []
        assert fetcher.block_indicators == []

    def test_edge_none_args_coerced_to_empty_lists(self):
        fetcher = WebFetcher(retry_indicators=None, block_indicators=None)
        assert fetcher.retry_indicators == []
        assert fetcher.block_indicators == []

    def test_normal_multiple_indicators_each(self):
        fetcher = WebFetcher(
            retry_indicators=["a", "b", "c"],
            block_indicators=["x", "y"],
        )
        assert len(fetcher.retry_indicators) == 3
        assert len(fetcher.block_indicators) == 2


# ── WebFetcher.is_blocked ─────────────────────────────────────────────────────

class TestIsBlocked:
    def test_normal_html_without_indicator_returns_false(self):
        fetcher = WebFetcher(block_indicators=["Access Denied"])
        assert fetcher.is_blocked("<html>Welcome!</html>") is False

    def test_normal_html_with_indicator_returns_true(self):
        fetcher = WebFetcher(block_indicators=["Access Denied"])
        assert fetcher.is_blocked("<html>Access Denied</html>") is True

    def test_normal_any_indicator_triggers_block(self):
        fetcher = WebFetcher(block_indicators=["rate limited", "cloudflare", "forbidden"])
        assert fetcher.is_blocked("page: Cloudflare Ray ID: 123") is True
        assert fetcher.is_blocked("Error: Rate Limited") is True
        assert fetcher.is_blocked("Just a normal page") is False

    def test_edge_empty_html_always_blocked(self):
        fetcher = WebFetcher(block_indicators=["anything"])
        assert fetcher.is_blocked("") is True

    def test_edge_no_indicators_non_empty_html_not_blocked(self):
        fetcher = WebFetcher()
        assert fetcher.is_blocked("<html>content</html>") is False

    def test_edge_no_indicators_empty_html_still_blocked(self):
        fetcher = WebFetcher()
        assert fetcher.is_blocked("") is True

    def test_edge_case_insensitive_matching(self):
        fetcher = WebFetcher(block_indicators=["access denied"])
        assert fetcher.is_blocked("<html>ACCESS DENIED</html>") is True
        assert fetcher.is_blocked("<html>Access Denied</html>") is True

    def test_error_none_html_raises(self):
        fetcher = WebFetcher(block_indicators=["x"])
        with pytest.raises((TypeError, AttributeError)):
            fetcher.is_blocked(None)


# ── WebFetcher.fetch ──────────────────────────────────────────────────────────

class TestFetch:
    @patch("fetcher.Fetcher")
    def test_normal_successful_first_attempt(self, MockFetcher):
        MockFetcher.get.return_value = make_page("<html>Hello</html>")
        fetcher = WebFetcher()
        assert fetcher.fetch("http://example.com") == "<html>Hello</html>"
        MockFetcher.get.assert_called_once()

    @patch("fetcher.Fetcher")
    def test_normal_no_retry_indicator_returns_immediately(self, MockFetcher):
        MockFetcher.get.return_value = make_page("<html>Clean</html>")
        fetcher = WebFetcher(retry_indicators=["wait"])
        result = fetcher.fetch("http://example.com", retries=3, backoff=0)
        assert result == "<html>Clean</html>"
        assert MockFetcher.get.call_count == 1  # no retry needed

    @patch("fetcher.Fetcher")
    def test_normal_retry_indicator_on_first_attempt_then_success(self, MockFetcher):
        blocked = make_page("<html>please wait...</html>")
        clean = make_page("<html>Welcome</html>")
        MockFetcher.get.side_effect = [blocked, clean]
        fetcher = WebFetcher(retry_indicators=["please wait"])
        result = fetcher.fetch("http://example.com", retries=2, backoff=0)
        assert "Welcome" in result
        assert MockFetcher.get.call_count == 2

    @patch("fetcher.Fetcher")
    def test_edge_status_503_retries_then_returns_empty(self, MockFetcher):
        MockFetcher.get.return_value = make_page(status=503)
        fetcher = WebFetcher()
        result = fetcher.fetch("http://example.com", retries=2, backoff=0)
        assert result == ""

    @patch("fetcher.Fetcher")
    def test_edge_status_429_treated_like_503(self, MockFetcher):
        MockFetcher.get.return_value = make_page(status=429)
        fetcher = WebFetcher()
        result = fetcher.fetch("http://example.com", retries=1, backoff=0)
        assert result == ""

    @patch("fetcher.Fetcher")
    def test_error_all_retries_exhaust_raises_fetcher_error(self, MockFetcher):
        MockFetcher.get.side_effect = ConnectionError("network down")
        fetcher = WebFetcher()
        with pytest.raises(FetcherError):
            fetcher.fetch("http://unreachable.example.com", retries=2, backoff=0)

    @patch("fetcher.Fetcher")
    @patch.object(WebFetcher, "_escalate_to_browser")
    def test_normal_escalates_when_indicator_persists_all_retries(self, mock_escalate, MockFetcher):
        mock_escalate.return_value = "<html>Bypassed</html>"
        MockFetcher.get.return_value = make_page("<html>just a moment</html>")
        fetcher = WebFetcher(retry_indicators=["just a moment"])
        result = fetcher.fetch("http://example.com", retries=1, backoff=0)
        mock_escalate.assert_called_once_with("http://example.com", "just a moment")
        assert result == "<html>Bypassed</html>"

    @patch("fetcher.Fetcher")
    def test_edge_retry_indicator_check_is_case_insensitive(self, MockFetcher):
        # indicator lower-cased in check
        blocked = make_page("<html>CLOUDFLARE CHECKING</html>")
        clean = make_page("<html>OK</html>")
        MockFetcher.get.side_effect = [blocked, clean]
        fetcher = WebFetcher(retry_indicators=["cloudflare checking"])
        result = fetcher.fetch("http://example.com", retries=2, backoff=0)
        assert "OK" in result


# ── WebFetcher.browser ────────────────────────────────────────────────────────

class TestBrowser:
    @patch("fetcher.DynamicSession")
    def test_normal_returns_dynamic_session_by_default(self, MockDynamic):
        fetcher = WebFetcher()
        session = fetcher.browser()
        assert session is MockDynamic.return_value

    @patch("fetcher.StealthySession")
    def test_normal_solve_cloudflare_uses_stealthy_session(self, MockStealthy):
        fetcher = WebFetcher()
        session = fetcher.browser(solve_cloudflare=True)
        assert session is MockStealthy.return_value
        MockStealthy.assert_called_once()
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("solve_cloudflare") is True

    @patch("fetcher.DynamicSession")
    def test_edge_interactive_flag_wraps_in_interactive_session(self, MockDynamic):
        fetcher = WebFetcher()
        session = fetcher.browser(interactive=True)
        assert isinstance(session, InteractiveSession)

    @patch("fetcher.StealthySession")
    def test_edge_interactive_plus_cloudflare_uses_stealthy_wrapped(self, MockStealthy):
        fetcher = WebFetcher()
        session = fetcher.browser(solve_cloudflare=True, interactive=True)
        assert isinstance(session, InteractiveSession)

    @patch("fetcher.DynamicSession")
    def test_normal_headless_flag_forwarded(self, MockDynamic):
        fetcher = WebFetcher()
        fetcher.browser(headless=False)
        call_kwargs = MockDynamic.call_args[1]
        assert call_kwargs.get("headless") is False

    @patch("fetcher.DynamicSession")
    def test_edge_extra_kwargs_forwarded_to_session(self, MockDynamic):
        fetcher = WebFetcher()
        fetcher.browser(custom_flag=True)
        call_kwargs = MockDynamic.call_args[1]
        assert call_kwargs.get("custom_flag") is True


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
        fetcher.scrape(["http://example.com"], callback=lambda u, h: called.append(u), mode=ScrapeMode.FAST)
        assert called == []


# ── InteractiveSession ────────────────────────────────────────────────────────

class TestInteractiveSessionContextManager:
    def test_normal_enter_starts_session_and_creates_page(self):
        mock_session, mock_page = make_interactive_session()
        with InteractiveSession(mock_session) as session:
            mock_session.start.assert_called_once()
            assert session.page is mock_page

    def test_normal_exit_closes_page_and_session(self):
        mock_session, mock_page = make_interactive_session()
        with InteractiveSession(mock_session):
            pass
        mock_page.close.assert_called_once()
        mock_session.close.assert_called_once()

    def test_edge_page_close_exception_handled_and_reraises(self):
        mock_session, mock_page = make_interactive_session()
        mock_page.close.side_effect = RuntimeError("browser crash")
        with pytest.raises(FetcherError):
            with InteractiveSession(mock_session):
                pass


class TestInteractiveSessionFetch:
    def test_normal_fetch_returns_namespace_with_html(self):
        mock_session, mock_page = make_interactive_session("<html>loaded</html>")
        session = InteractiveSession(mock_session)
        session.__enter__()
        result = session.fetch("http://example.com")
        assert hasattr(result, "html_content")
        assert result.html_content == "<html>loaded</html>"
        mock_page.goto.assert_called_once()
        mock_page.wait_for_timeout.assert_called_once_with(2000)

    def test_edge_fetch_without_enter_raises_runtime_error(self):
        mock_session = MagicMock()
        session = InteractiveSession(mock_session)
        with pytest.raises(RuntimeError, match="Session not started"):
            session.fetch("http://example.com")

    def test_normal_fetch_passes_timeout_and_wait_until(self):
        mock_session, mock_page = make_interactive_session()
        session = InteractiveSession(mock_session)
        session.__enter__()
        session.fetch("http://example.com", timeout=5000, wait_until="networkidle")
        mock_page.goto.assert_called_once_with(
            "http://example.com", wait_until="networkidle", timeout=5000
        )


class TestInteractiveSessionExecuteScript:
    def test_normal_plain_script_evaluates_directly(self):
        mock_session, mock_page = make_interactive_session(eval_return="test-title")
        session = InteractiveSession(mock_session)
        session.__enter__()
        result = session.execute_script("document.title")
        mock_page.evaluate.assert_called_once_with("document.title")
        assert result == "test-title"

    def test_normal_return_prefix_wrapped_in_arrow_function(self):
        mock_session, mock_page = make_interactive_session(eval_return=42)
        session = InteractiveSession(mock_session)
        session.__enter__()
        result = session.execute_script("return document.title.length")
        mock_page.evaluate.assert_called_once_with("() => { return document.title.length }")
        assert result == 42

    def test_edge_execute_without_enter_raises(self):
        mock_session = MagicMock()
        session = InteractiveSession(mock_session)
        with pytest.raises(RuntimeError, match="Call fetch"):
            session.execute_script("1 + 1")

    def test_error_js_error_propagates(self):
        mock_session, mock_page = make_interactive_session()
        mock_page.evaluate.side_effect = Exception("ReferenceError: x is not defined")
        session = InteractiveSession(mock_session)
        session.__enter__()
        with pytest.raises(Exception, match="ReferenceError"):
            session.execute_script("undeclared_var()")


class TestInteractiveSessionHelpers:
    def _started(self, mock_session, mock_page):
        session = InteractiveSession(mock_session)
        session.__enter__()
        return session

    def test_normal_wait_for_selector_delegates(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        session.wait_for_selector("#submit", timeout=5000)
        mock_page.wait_for_selector.assert_called_once_with("#submit", timeout=5000)

    def test_normal_wait_for_function_delegates(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        session.wait_for_function("() => window.ready", timeout=10000)
        mock_page.wait_for_function.assert_called_once_with("() => window.ready", timeout=10000)

    def test_normal_click_delegates(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        session.click(".btn", timeout=3000)
        mock_page.click.assert_called_once_with(".btn", timeout=3000)

    def test_normal_wait_for_timeout_delegates(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        session.wait_for_timeout(2000)
        mock_page.wait_for_timeout.assert_called_with(2000)

    def test_edge_getattr_delegates_to_underlying_session(self):
        mock_session = MagicMock()
        mock_session.cookies = {"session": "abc"}
        session = InteractiveSession(mock_session)
        assert session.cookies == {"session": "abc"}

    def test_edge_helpers_without_enter_raise_runtime(self):
        mock_session = MagicMock()
        session = InteractiveSession(mock_session)
        for method, args in [
            ("wait_for_selector", ("#x",)),
            ("wait_for_function", ("() => true",)),
            ("click", (".btn",)),
            ("wait_for_timeout", (1000,)),
        ]:
            with pytest.raises(RuntimeError):
                getattr(session, method)(*args)


# ── Complex Scenarios ─────────────────────────────────────────────────────────

class TestFetcherScenarios:
    @patch("fetcher.Fetcher")
    def test_scenario_two_failures_then_success_on_third(self, MockFetcher):
        """503 twice, then clean HTML on the third attempt."""
        MockFetcher.get.side_effect = [
            make_page(status=503),
            make_page(status=503),
            make_page("<html>Finally</html>"),
        ]
        fetcher = WebFetcher()
        result = fetcher.fetch("http://example.com", retries=3, backoff=0)
        assert "Finally" in result
        assert MockFetcher.get.call_count == 3

    @patch("fetcher.Fetcher")
    @patch.object(WebFetcher, "_escalate_to_browser")
    def test_scenario_retry_indicator_exhausts_and_escalates(self, mock_escalate, MockFetcher):
        """Retry indicator on every attempt → escalation called exactly once."""
        mock_escalate.return_value = "<html>Solved via browser</html>"
        MockFetcher.get.return_value = make_page("<html>just a moment</html>")
        fetcher = WebFetcher(retry_indicators=["just a moment"])
        result = fetcher.fetch("http://example.com", retries=1, backoff=0)
        mock_escalate.assert_called_once_with("http://example.com", "just a moment")
        assert result == "<html>Solved via browser</html>"

    @patch.object(WebFetcher, "fetch")
    def test_scenario_fast_scrape_with_concurrency_all_urls_processed(self, mock_fetch):
        """Batch of 6 URLs with concurrency 3 — all callbacks fire, order may differ."""
        mock_fetch.return_value = "<html>data</html>"
        fetcher = WebFetcher()
        results = []
        urls = [f"http://site{i}.com" for i in range(6)]
        fetcher.scrape(urls, callback=lambda u, h: results.append(u), mode=ScrapeMode.FAST, max_concurrency=3)
        assert sorted(results) == sorted(urls)

    @patch("fetcher.Fetcher")
    def test_scenario_multiple_block_indicators_individually_detected(self, MockFetcher):
        """Each distinct block indicator triggers is_blocked independently."""
        fetcher = WebFetcher(block_indicators=["rate limited", "access denied", "captcha required"])
        assert fetcher.is_blocked("Sorry, rate limited right now") is True
        assert fetcher.is_blocked("<h1>Access Denied</h1>") is True
        assert fetcher.is_blocked("Please complete the captcha required") is True
        assert fetcher.is_blocked("<html>Welcome to our store</html>") is False

    def test_scenario_interactive_session_full_lifecycle(self):
        """Full context manager lifecycle: enter, fetch, execute_script, exit."""
        mock_session, mock_page = make_interactive_session(
            html="<html><title>Scraped</title></html>",
            eval_return="Scraped",
        )
        with InteractiveSession(mock_session) as session:
            resp = session.fetch("http://test.com", timeout=30000, wait_until="load")
            title = session.execute_script("return document.title")

        assert resp.html_content == "<html><title>Scraped</title></html>"
        assert title == "Scraped"
        mock_page.goto.assert_called_once_with("http://test.com", wait_until="load", timeout=30000)
        mock_page.close.assert_called_once()
        mock_session.close.assert_called_once()