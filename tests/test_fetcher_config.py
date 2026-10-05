"""WebFetcher.configure family + module proxies."""

from unittest.mock import MagicMock, patch

import pytest
from conftest import (
    make_fetcher_config,
    make_page,
)

import scrape_kit as sk
import scrape_kit.fetcher as fetcher_module
import scrape_kit.fetcher._state
from scrape_kit.fetcher import _get_shared
from scrape_kit.fetcher import browser as module_browser
from scrape_kit.fetcher import fetch as module_fetch
from scrape_kit.fetcher import is_blocked as module_is_blocked
from scrape_kit.fetcher import scrape as module_scrape
from scrape_kit.fetcher.batch import ScrapeMode
from scrape_kit.fetcher.session import InteractiveSession
from scrape_kit.fetcher.web_fetcher import WebFetcher

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
        fetcher_module.reset_shared()
        instance = WebFetcher.configure(str(cfg_dir), set_shared=True)
        assert fetcher_module._shared is instance

    def test_edge_set_shared_false_does_not_replace_shared(self, tmp_path):
        """configure(set_shared=False) does not overwrite the module shared instance."""
        existing = WebFetcher(retry_indicators=["existing"])
        fetcher_module._set_shared(existing)

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
        assert instance.retry_indicators == list(WebFetcher._DEFAULT_RETRY)
        assert instance.block_indicators == list(WebFetcher._DEFAULT_BLOCK)


class TestConfigureDefaults:
    """configure_defaults() builds an instance from class-level defaults."""

    def test_normal_uses_class_defaults(self):
        instance = WebFetcher.configure_defaults(set_shared=False)
        assert instance.retry_indicators == list(WebFetcher._DEFAULT_RETRY)
        assert instance.block_indicators == list(WebFetcher._DEFAULT_BLOCK)

    def test_normal_sets_shared_by_default(self):
        fetcher_module.reset_shared()
        instance = WebFetcher.configure_defaults(set_shared=True)
        assert fetcher_module._shared is instance

    def test_edge_set_shared_false_leaves_shared_none(self):
        fetcher_module.reset_shared()
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
        fetcher_module.reset_shared()
        instance = sk.configure(str(cfg_dir))
        assert fetcher_module._shared is instance
        assert "pkg" in instance.retry_indicators

    def test_normal_sk_configure_defaults_sets_shared(self):
        fetcher_module.reset_shared()
        instance = sk.configure_defaults()
        assert fetcher_module._shared is instance
        assert instance.retry_indicators == list(WebFetcher._DEFAULT_RETRY)


# ── Module-level proxy functions ──────────────────────────────────────────────


class TestModuleProxies:
    """Module-level fetch/is_blocked/browser/scrape delegate to the shared instance."""

    def test_normal_get_shared_creates_default_configured_instance_if_not_set(self):
        """_get_shared() auto-creates a default-configured WebFetcher (configure_defaults semantics)."""
        fetcher_module.reset_shared()
        shared = _get_shared()
        assert isinstance(shared, WebFetcher)
        # Zero-config fallback matches configure_defaults() exactly.
        assert shared.retry_indicators == list(WebFetcher._DEFAULT_RETRY)
        assert shared.block_indicators == list(WebFetcher._DEFAULT_BLOCK)
        assert shared.retry_indicators is not WebFetcher._DEFAULT_RETRY  # copy, not alias
        # Subsequent call returns same instance
        assert _get_shared() is shared

    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_module_fetch_delegates_to_shared(self, MockFetcher):
        """module fetch() uses whatever shared instance is set."""
        MockFetcher.get.return_value = make_page("<html>proxied</html>")
        fetcher = WebFetcher()
        fetcher_module._set_shared(fetcher)
        result = module_fetch("http://example.com")
        assert result == "<html>proxied</html>"

    def test_normal_module_is_blocked_delegates_to_shared(self):
        fetcher = WebFetcher(block_indicators=["BLOCKED"])
        fetcher_module._set_shared(fetcher)
        assert module_is_blocked("<html>BLOCKED</html>") is True
        assert module_is_blocked("<html>clean</html>") is False

    @patch("scrape_kit.fetcher.web_fetcher.DynamicSession")
    def test_normal_module_browser_delegates_to_shared(self, MockDynamic):
        fetcher = WebFetcher()
        fetcher_module._set_shared(fetcher)
        session = module_browser()
        assert isinstance(session, InteractiveSession)

    @patch.object(WebFetcher, "_scrape_fast")
    def test_normal_module_scrape_delegates_to_shared(self, mock_fast):
        fetcher = WebFetcher()
        fetcher_module._set_shared(fetcher)
        module_scrape(["http://a.com"], callback=MagicMock(), mode=ScrapeMode.FAST)
        mock_fast.assert_called_once()

    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_configure_then_proxy_uses_configured_indicators(self, MockFetcher, tmp_path):
        """Full flow: configure from YAML → module proxy picks up the indicators."""
        cfg_dir = make_fetcher_config(tmp_path, retry=["proxy_test"], block=["totally_blocked"])
        WebFetcher.configure(str(cfg_dir))
        # is_blocked now uses the configured indicators
        assert module_is_blocked("page is totally_blocked") is True
        assert module_is_blocked("clean page") is False


# ── Shared-state module contract ─────────────────────────────────────────────


class TestSharedStateContract:
    """Shared-instance state lives in the ._state leaf; aliases stay in sync."""

    def test_normal_state_leaf_is_single_source_of_truth(self):
        """Package reset_shared/_set_shared resolve to the ._state leaf's functions."""
        state = fetcher_module._state
        assert sk.reset_shared is state.reset_shared
        assert fetcher_module.reset_shared is state.reset_shared
        assert fetcher_module._set_shared is state._set_shared

    def test_normal_legacy_shared_alias_reads_state(self):
        """Legacy _shared/_set_shared package aliases reflect the ._state instance."""
        state = fetcher_module._state
        instance = WebFetcher(retry_indicators=["legacy"])
        fetcher_module._set_shared(instance)
        assert fetcher_module._shared is instance
        assert state._peek_shared() is instance
        fetcher_module.reset_shared()
        assert fetcher_module._shared is None
        assert state._peek_shared() is None

    def test_normal_get_shared_autoconfigures_state(self):
        """_get_shared() stores the auto-created default instance in ._state."""
        state = fetcher_module._state
        fetcher_module.reset_shared()
        shared = _get_shared()
        assert state._peek_shared() is shared
        assert fetcher_module._shared is shared
        assert shared.retry_indicators == list(WebFetcher._DEFAULT_RETRY)
