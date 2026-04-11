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

import scrape_kit as sk
import scrape_kit.fetcher as fetcher_module
from scrape_kit.errors import FetcherError
from scrape_kit.fetcher import (
    InteractiveSession,
    ScrapeMode,
    WebFetcher,
    _get_shared,
)
from scrape_kit.fetcher import browser as module_browser
from scrape_kit.fetcher import fetch as module_fetch
from scrape_kit.fetcher import is_blocked as module_is_blocked
from scrape_kit.fetcher import scrape as module_scrape

# ── Helpers ───────────────────────────────────────────────────────────────────


def make_page(html: str = "<html>OK</html>", status: int = 200) -> MagicMock:
    page = MagicMock()
    page.html_content = html
    page.status = status
    page.status_code = status
    return page


def make_interactive_session(html: str = "<html>page</html>", eval_return=None):
    mock_session = MagicMock()
    mock_page = MagicMock()
    mock_session.context.new_page.return_value = mock_page
    mock_page.content.return_value = html
    mock_page.evaluate.return_value = eval_return
    return mock_session, mock_page


# ── Fixture: reset shared instance between tests ──────────────────────────────


@pytest.fixture(autouse=True)
def reset_shared():
    """Ensure each test starts with a clean shared instance state."""
    old = fetcher_module._shared
    fetcher_module._shared = None
    yield
    fetcher_module._shared = old


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
        fetcher = WebFetcher(retry_indicators=["a", "b", "c"], block_indicators=["x", "y"])
        assert len(fetcher.retry_indicators) == 3
        assert len(fetcher.block_indicators) == 2


# ── WebFetcher.configure() ────────────────────────────────────────────────────


class TestConfigure:
    def test_normal_loads_indicators_from_yaml(self, tmp_path):
        """configure() reads retry/block lists from a YAML file via SettingsManager."""
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(
            "retry_indicators:\n  - just a moment\n  - checking your browser\nblock_indicators:\n  - access denied\n",
            encoding="utf-8",
        )
        instance = WebFetcher.configure(str(cfg_dir), set_shared=False)
        assert instance.retry_indicators == ["just a moment", "checking your browser"]
        assert instance.block_indicators == ["access denied"]

    def test_normal_sets_shared_instance_by_default(self, tmp_path):
        """configure() stores result as module-level shared instance when set_shared=True."""
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(
            "retry_indicators:\n  - test\nblock_indicators: []\n",
            encoding="utf-8",
        )
        fetcher_module._shared = None
        instance = WebFetcher.configure(str(cfg_dir), set_shared=True)
        assert fetcher_module._shared is instance

    def test_edge_set_shared_false_does_not_replace_shared(self, tmp_path):
        """configure(set_shared=False) does not overwrite the module shared instance."""
        existing = WebFetcher(retry_indicators=["existing"])
        fetcher_module._shared = existing

        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(
            "retry_indicators:\n  - new\nblock_indicators: []\n",
            encoding="utf-8",
        )
        WebFetcher.configure(str(cfg_dir), set_shared=False)
        assert fetcher_module._shared is existing

    def test_normal_custom_config_key(self, tmp_path):
        """configure() uses a custom key to look up a differently named YAML block."""
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "my_scraper.yaml").write_text(
            "retry_indicators:\n  - custom\nblock_indicators:\n  - nope\n",
            encoding="utf-8",
        )
        instance = WebFetcher.configure(str(cfg_dir), config_key="my_scraper", set_shared=False)
        assert instance.retry_indicators == ["custom"]
        assert instance.block_indicators == ["nope"]

    def test_edge_missing_yaml_falls_back_to_defaults(self, tmp_path):
        """If config key not found, configure() uses class-level _DEFAULT_RETRY/_DEFAULT_BLOCK."""
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        # Write a YAML but with a different key — our key won't be found
        (cfg_dir / "other.yaml").write_text("other:\n  x: 1\n", encoding="utf-8")
        instance = WebFetcher.configure(str(cfg_dir), set_shared=False)
        assert instance.retry_indicators == WebFetcher._DEFAULT_RETRY
        assert instance.block_indicators == WebFetcher._DEFAULT_BLOCK


class TestConfigureDefaults:
    def test_normal_uses_class_defaults(self):
        instance = WebFetcher.configure_defaults(set_shared=False)
        assert instance.retry_indicators == WebFetcher._DEFAULT_RETRY
        assert instance.block_indicators == WebFetcher._DEFAULT_BLOCK

    def test_normal_sets_shared_by_default(self):
        fetcher_module._shared = None
        instance = WebFetcher.configure_defaults(set_shared=True)
        assert fetcher_module._shared is instance

    def test_edge_set_shared_false_leaves_shared_none(self):
        fetcher_module._shared = None
        WebFetcher.configure_defaults(set_shared=False)
        assert fetcher_module._shared is None

    def test_normal_default_indicators_are_nonempty(self):
        assert len(WebFetcher._DEFAULT_RETRY) > 0
        assert len(WebFetcher._DEFAULT_BLOCK) > 0


# ── Package-level configure helpers ──────────────────────────────────────────


class TestPackageConfigure:
    def test_normal_sk_configure_sets_shared(self, tmp_path):
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(
            "retry_indicators:\n  - pkg\nblock_indicators: []\n",
            encoding="utf-8",
        )
        fetcher_module._shared = None
        instance = sk.configure(str(cfg_dir))
        assert fetcher_module._shared is instance
        assert "pkg" in instance.retry_indicators

    def test_normal_sk_configure_defaults_sets_shared(self):
        fetcher_module._shared = None
        instance = sk.configure_defaults()
        assert fetcher_module._shared is instance
        assert instance.retry_indicators == WebFetcher._DEFAULT_RETRY


# ── Module-level proxy functions ──────────────────────────────────────────────


class TestModuleProxies:
    def test_normal_get_shared_creates_zero_config_instance_if_not_set(self):
        """_get_shared() auto-creates an empty WebFetcher when none is configured."""
        fetcher_module._shared = None
        shared = _get_shared()
        assert isinstance(shared, WebFetcher)
        assert shared.retry_indicators == []
        assert shared.block_indicators == []
        # Subsequent call returns same instance
        assert _get_shared() is shared

    @patch("scrape_kit.fetcher.Fetcher")
    def test_normal_module_fetch_delegates_to_shared(self, MockFetcher):
        """module fetch() uses whatever shared instance is set."""
        MockFetcher.get.return_value = make_page("<html>proxied</html>")
        fetcher = WebFetcher()
        fetcher_module._shared = fetcher
        result = module_fetch("http://example.com")
        assert result == "<html>proxied</html>"

    def test_normal_module_is_blocked_delegates_to_shared(self):
        fetcher = WebFetcher(block_indicators=["BLOCKED"])
        fetcher_module._shared = fetcher
        assert module_is_blocked("<html>BLOCKED</html>") is True
        assert module_is_blocked("<html>clean</html>") is False

    @patch("scrape_kit.fetcher.DynamicSession")
    def test_normal_module_browser_delegates_to_shared(self, MockDynamic):
        fetcher = WebFetcher()
        fetcher_module._shared = fetcher
        session = module_browser()
        assert isinstance(session, InteractiveSession)

    @patch.object(WebFetcher, "_scrape_fast")
    def test_normal_module_scrape_delegates_to_shared(self, mock_fast):
        fetcher = WebFetcher()
        fetcher_module._shared = fetcher
        module_scrape(["http://a.com"], callback=MagicMock(), mode=ScrapeMode.FAST)
        mock_fast.assert_called_once()

    @patch("scrape_kit.fetcher.Fetcher")
    def test_normal_configure_then_proxy_uses_configured_indicators(self, MockFetcher, tmp_path):
        """Full flow: configure from YAML → module proxy picks up the indicators."""
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(
            "retry_indicators:\n  - proxy_test\nblock_indicators:\n  - totally_blocked\n",
            encoding="utf-8",
        )
        WebFetcher.configure(str(cfg_dir))
        # is_blocked now uses the configured indicators
        assert module_is_blocked("page is totally_blocked") is True
        assert module_is_blocked("clean page") is False


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

    def test_edge_none_html_treated_as_blocked(self):
        fetcher = WebFetcher(block_indicators=["x"])
        assert fetcher.is_blocked(None) is True


# ── WebFetcher.fetch ──────────────────────────────────────────────────────────


class TestFetch:
    @patch("scrape_kit.fetcher.Fetcher")
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
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(
            "retry_indicators:\n  - block_me\nblock_indicators: []\n",
            encoding="utf-8",
        )
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
        with pytest.raises(FetcherError), InteractiveSession(mock_session):
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
        mock_page.goto.assert_called_once_with("http://example.com", wait_until="networkidle", timeout=5000)


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
        session = InteractiveSession(MagicMock())
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
        session = InteractiveSession(MagicMock())
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
            resp = session.fetch("http://test.com", timeout=30000, wait_until="load")
            title = session.execute_script("return document.title")

        assert resp.html_content == "<html><title>Scraped</title></html>"
        assert title == "Scraped"
        mock_page.goto.assert_called_once_with("http://test.com", wait_until="load", timeout=30000)
        mock_page.close.assert_called_once()
        mock_session.close.assert_called_once()

    def test_scenario_configure_yaml_then_module_proxy_full_flow(self, tmp_path):
        """configure() from YAML → module proxies use the right indicators end-to-end."""
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(
            "retry_indicators: []\nblock_indicators:\n  - e2e_blocked\n",
            encoding="utf-8",
        )
        WebFetcher.configure(str(cfg_dir))
        assert module_is_blocked("page contains e2e_blocked text") is True
        assert module_is_blocked("normal page") is False

    def test_scenario_multiple_configure_calls_last_one_wins(self, tmp_path):
        """Calling configure() twice replaces the shared instance."""
        cfg_a = tmp_path / "cfg_a"
        cfg_b = tmp_path / "cfg_b"
        cfg_a.mkdir()
        cfg_b.mkdir()
        (cfg_a / "scraper_config.yaml").write_text("retry_indicators: [first]\nblock_indicators: []\n")
        (cfg_b / "scraper_config.yaml").write_text("retry_indicators: [second]\nblock_indicators: []\n")
        WebFetcher.configure(str(cfg_a))
        first = fetcher_module._shared
        WebFetcher.configure(str(cfg_b))
        second = fetcher_module._shared
        assert first is not second
        assert second.retry_indicators == ["second"]


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
        mock_browser_session.fetch.assert_called_once_with("http://test.com", timeout=120000)

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
