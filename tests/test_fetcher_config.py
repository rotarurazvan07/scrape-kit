"""WebFetcher.configure family + module proxies."""

from unittest.mock import MagicMock, patch

import pytest
from conftest import (
    make_fetcher_config,
    make_page,
)

import scrape_kit as sk
import scrape_kit.fetcher as fetcher_module
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

pytestmark = pytest.mark.p0


# ── WebFetcher.configure() ────────────────────────────────────────────────────


class TestConfigure:
    """configure() loads retry/block indicators from YAML via SettingsManager."""

    def test_normal_loads_indicators_from_yaml(self, tmp_path):
        """configure() reads retry/block lists from a YAML file via SettingsManager."""
        cfg_dir = make_fetcher_config(tmp_path, retry=["just a moment", "checking your browser"], block=["access denied"])
        instance = WebFetcher.configure(str(cfg_dir), set_shared=False)
        assert instance.retry_indicators == ["just a moment", "checking your browser"]
        assert instance.block_indicators == ["access denied"]

    def test_normal_sets_shared_instance_by_default(self, tmp_path):
        """configure() stores result as module-level shared instance when set_shared=True."""
        cfg_dir = make_fetcher_config(tmp_path, retry=["test"])
        fetcher_module._shared = None
        instance = WebFetcher.configure(str(cfg_dir), set_shared=True)
        assert fetcher_module._shared is instance

    def test_edge_set_shared_false_does_not_replace_shared(self, tmp_path):
        """configure(set_shared=False) does not overwrite the module shared instance."""
        existing = WebFetcher(retry_indicators=["existing"])
        fetcher_module._shared = existing

        cfg_dir = make_fetcher_config(tmp_path, retry=["new"])
        WebFetcher.configure(str(cfg_dir), set_shared=False)
        assert fetcher_module._shared is existing

    def test_normal_custom_config_key(self, tmp_path):
        """configure() uses a custom key to look up a differently named YAML block."""
        cfg_dir = make_fetcher_config(tmp_path, retry=["custom"], block=["nope"], name="my_scraper.yaml")
        instance = WebFetcher.configure(str(cfg_dir), config_key="my_scraper", set_shared=False)
        assert instance.retry_indicators == ["custom"]
        assert instance.block_indicators == ["nope"]

    def test_edge_missing_yaml_falls_back_to_defaults(self, tmp_path):
        """If config key not found, configure() uses class-level _DEFAULT_RETRY/_DEFAULT_BLOCK."""
        cfg_dir = make_fetcher_config(tmp_path, name="other.yaml")  # different stem, key not found
        instance = WebFetcher.configure(str(cfg_dir), set_shared=False)
        assert instance.retry_indicators == WebFetcher._DEFAULT_RETRY
        assert instance.block_indicators == WebFetcher._DEFAULT_BLOCK


class TestConfigureDefaults:
    """configure_defaults() builds an instance from class-level defaults."""

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
    """Package-level configure/configure_defaults set the module shared instance."""

    def test_normal_sk_configure_sets_shared(self, tmp_path):
        cfg_dir = make_fetcher_config(tmp_path, retry=["pkg"])
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
    """Module-level fetch/is_blocked/browser/scrape delegate to the shared instance."""

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
        cfg_dir = make_fetcher_config(tmp_path, retry=["proxy_test"], block=["totally_blocked"])
        WebFetcher.configure(str(cfg_dir))
        # is_blocked now uses the configured indicators
        assert module_is_blocked("page is totally_blocked") is True
        assert module_is_blocked("clean page") is False
