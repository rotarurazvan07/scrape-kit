"""WebFetcher core: mode/init/is_blocked/fetch/browser/escalate."""

from unittest.mock import MagicMock, patch

import pytest
from conftest import (
    ESCALATE_TIMEOUT_MS,
    make_fetcher_config,
    make_page,
)

from scrape_kit.errors import FetcherError
from scrape_kit.fetcher import (
    InteractiveSession,
    ScrapeMode,
    WebFetcher,
)

pytestmark = pytest.mark.p0


# ── ScrapeMode ────────────────────────────────────────────────────────────────


class TestScrapeMode:
    """ScrapeMode exposes the FAST/STEALTH string constants."""

    def test_normal_fast_constant(self):
        assert ScrapeMode.FAST == "fast"

    def test_normal_stealth_constant(self):
        assert ScrapeMode.STEALTH == "stealth"

    def test_edge_constants_are_strings(self):
        assert isinstance(ScrapeMode.FAST, str)
        assert isinstance(ScrapeMode.STEALTH, str)


# ── WebFetcher.__init__ ───────────────────────────────────────────────────────


class TestWebFetcherInit:
    """WebFetcher.__init__ stores and normalizes indicator lists."""

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
        fetcher = WebFetcher(retry_indicators=["a", "b", "c"], block_indicators=["x", "y"])
        assert len(fetcher.retry_indicators) == 3
        assert len(fetcher.block_indicators) == 2


# ── WebFetcher.is_blocked ─────────────────────────────────────────────────────


class TestIsBlocked:
    """is_blocked matches block indicators case-insensitively."""

    def test_normal_html_without_indicator_returns_false(self):
        fetcher = WebFetcher(block_indicators=["Access Denied"])
        assert fetcher.is_blocked("<html>Welcome!</html>") is False

    @pytest.mark.smoke
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

    def test_edge_none_html_treated_as_blocked(self):
        fetcher = WebFetcher(block_indicators=["x"])
        assert fetcher.is_blocked(None) is True


# ── WebFetcher.fetch ──────────────────────────────────────────────────────────


class TestFetch:
    """WebFetcher.fetch retry, block-escalation and error paths."""

    @patch("scrape_kit.fetcher.Fetcher")
    @pytest.mark.smoke
    def test_normal_successful_first_attempt(self, MockFetcher):
        MockFetcher.get.return_value = make_page("<html>Hello</html>")
        fetcher = WebFetcher()
        assert fetcher.fetch("http://example.com") == "<html>Hello</html>"
        MockFetcher.get.assert_called_once()

    @patch("scrape_kit.fetcher.Fetcher")
    def test_normal_no_retry_indicator_returns_immediately(self, MockFetcher):
        MockFetcher.get.return_value = make_page("<html>Clean</html>")
        fetcher = WebFetcher(retry_indicators=["wait"])
        result = fetcher.fetch("http://example.com", retries=3, backoff=0)
        assert result == "<html>Clean</html>"
        assert MockFetcher.get.call_count == 1

    @patch("scrape_kit.fetcher.Fetcher")
    def test_normal_retry_indicator_on_first_attempt_then_success(self, MockFetcher):
        blocked = make_page("<html>please wait...</html>")
        clean = make_page("<html>Welcome</html>")
        MockFetcher.get.side_effect = [blocked, clean]
        fetcher = WebFetcher(retry_indicators=["please wait"])
        result = fetcher.fetch("http://example.com", retries=2, backoff=0)
        assert "Welcome" in result
        assert MockFetcher.get.call_count == 2

    @patch("scrape_kit.fetcher.Fetcher")
    def test_edge_status_503_retries_then_raises(self, MockFetcher):
        MockFetcher.get.return_value = make_page(status=503)
        fetcher = WebFetcher()
        with pytest.raises(FetcherError):
            fetcher.fetch("http://example.com", retries=2, backoff=0)

    @patch("scrape_kit.fetcher.Fetcher")
    def test_edge_status_429_treated_like_503(self, MockFetcher):
        MockFetcher.get.return_value = make_page(status=429)
        fetcher = WebFetcher()
        with pytest.raises(FetcherError):
            fetcher.fetch("http://example.com", retries=1, backoff=0)

    @patch("scrape_kit.fetcher.Fetcher")
    def test_error_all_retries_exhaust_raises_fetcher_error(self, MockFetcher):
        MockFetcher.get.side_effect = ConnectionError("network down")
        fetcher = WebFetcher()
        with pytest.raises(FetcherError):
            fetcher.fetch("http://unreachable.example.com", retries=2, backoff=0)

    @patch("scrape_kit.fetcher.Fetcher")
    @patch.object(WebFetcher, "_escalate_to_browser")
    def test_normal_escalates_when_indicator_persists_all_retries(self, mock_escalate, MockFetcher):
        mock_escalate.return_value = "<html>Bypassed</html>"
        MockFetcher.get.return_value = make_page("<html>just a moment</html>")
        fetcher = WebFetcher(retry_indicators=["just a moment"])
        result = fetcher.fetch("http://example.com", retries=1, backoff=0)
        mock_escalate.assert_called_once_with("http://example.com", "just a moment")
        assert result == "<html>Bypassed</html>"

    @patch("scrape_kit.fetcher.Fetcher")
    def test_edge_retry_indicator_check_is_case_insensitive(self, MockFetcher):
        blocked = make_page("<html>CLOUDFLARE CHECKING</html>")
        clean = make_page("<html>OK</html>")
        MockFetcher.get.side_effect = [blocked, clean]
        fetcher = WebFetcher(retry_indicators=["cloudflare checking"])
        result = fetcher.fetch("http://example.com", retries=2, backoff=0)
        assert "OK" in result

    def test_error_retries_less_than_one_raises_value_error(self):
        fetcher = WebFetcher()
        with pytest.raises(ValueError, match="retries must be >= 1"):
            fetcher.fetch("http://example.com", retries=0)

    @patch("scrape_kit.fetcher.Fetcher")
    def test_normal_configured_instance_uses_yaml_indicators(self, MockFetcher, tmp_path):
        """configure() → instance respects loaded indicators on fetch()."""
        cfg_dir = make_fetcher_config(tmp_path, retry=["block_me"])
        fetcher = WebFetcher.configure(str(cfg_dir), set_shared=False)
        # First call blocked, second clean
        MockFetcher.get.side_effect = [
            make_page("<html>block_me</html>"),
            make_page("<html>OK</html>"),
        ]
        result = fetcher.fetch("http://example.com", retries=2, backoff=0)
        assert "OK" in result


# ── WebFetcher.browser ────────────────────────────────────────────────────────


class TestBrowser:
    """WebFetcher.browser returns the right session type per stealth flag."""

    @patch("scrape_kit.fetcher.DynamicSession")
    def test_normal_returns_dynamic_session_by_default(self, MockDynamic):
        fetcher = WebFetcher()
        session = fetcher.browser()
        assert isinstance(session, InteractiveSession)
        assert session.session is MockDynamic.return_value

    @patch("scrape_kit.fetcher.StealthySession")
    def test_normal_solve_cloudflare_uses_stealthy_session(self, MockStealthy):
        fetcher = WebFetcher()
        session = fetcher.browser(solve_cloudflare=True)
        assert isinstance(session, InteractiveSession)
        assert session.session is MockStealthy.return_value
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("solve_cloudflare") is True

    @patch("scrape_kit.fetcher.DynamicSession")
    def test_normal_headless_flag_forwarded(self, MockDynamic):
        fetcher = WebFetcher()
        fetcher.browser(headless=False)
        call_kwargs = MockDynamic.call_args[1]
        assert call_kwargs.get("headless") is False

    @patch("scrape_kit.fetcher.DynamicSession")
    def test_edge_extra_kwargs_forwarded_to_session(self, MockDynamic):
        fetcher = WebFetcher()
        fetcher.browser(custom_flag=True)
        call_kwargs = MockDynamic.call_args[1]
        assert call_kwargs.get("custom_flag") is True


# ── Additional tests for uncovered lines ───────────────────────────────────────


class TestEscalateToBrowser:
    """Test lines 332-342 - _escalate_to_browser method"""

    def test_normal_escalate_to_browser_success(self):
        """Test successful browser escalation"""
        fetcher = WebFetcher()
        mock_browser_session = MagicMock()
        mock_response = MagicMock()
        mock_response.html_content = "<html>Escalated content</html>"
        mock_browser_session.fetch.return_value = mock_response
        mock_browser_session.__enter__ = MagicMock(return_value=mock_browser_session)
        mock_browser_session.__exit__ = MagicMock(return_value=False)

        with patch.object(fetcher, "browser", return_value=mock_browser_session):
            result = fetcher._escalate_to_browser("http://test.com", "blocked")

        assert result == "<html>Escalated content</html>"
        mock_browser_session.fetch.assert_called_once_with("http://test.com", timeout=ESCALATE_TIMEOUT_MS)

    def test_edge_escalate_to_browser_no_html_content(self):
        """Test line 342 - browser returns no content"""
        fetcher = WebFetcher()
        mock_browser_session = MagicMock()
        mock_response = MagicMock()
        del mock_response.html_content  # No html_content attribute
        mock_browser_session.fetch.return_value = mock_response
        mock_browser_session.__enter__ = MagicMock(return_value=mock_browser_session)
        mock_browser_session.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(fetcher, "browser", return_value=mock_browser_session),
            pytest.raises(FetcherError, match="Escalation returned no content"),
        ):
            fetcher._escalate_to_browser("http://test.com", "blocked")

    def test_error_escalate_to_browser_failure(self):
        """Test lines 339-341 - browser escalation fails"""
        fetcher = WebFetcher()
        mock_browser_session = MagicMock()
        mock_browser_session.fetch.side_effect = Exception("Browser error")
        mock_browser_session.__enter__ = MagicMock(return_value=mock_browser_session)
        mock_browser_session.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(fetcher, "browser", return_value=mock_browser_session),
            pytest.raises(FetcherError, match="Escalation failed"),
        ):
            fetcher._escalate_to_browser("http://test.com", "blocked")
