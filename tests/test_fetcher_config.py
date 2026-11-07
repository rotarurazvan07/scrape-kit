"""WebFetcher.configure family + module proxies."""

from unittest.mock import MagicMock, patch

import pytest
import yaml
from conftest import (
    make_fetcher_config,
    make_page,
)

import scrape_kit as sk
import scrape_kit.fetcher as fetcher_module
import scrape_kit.fetcher._state
from scrape_kit.errors import SettingsError
from scrape_kit.fetcher import _get_shared
from scrape_kit.fetcher import browser as module_browser
from scrape_kit.fetcher import fetch as module_fetch
from scrape_kit.fetcher import is_blocked as module_is_blocked
from scrape_kit.fetcher import scrape as module_scrape
from scrape_kit.fetcher.batch_types import ScrapeMode
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
        assert fetcher_module._state._peek_shared() is instance

    def test_edge_set_shared_false_does_not_replace_shared(self, tmp_path):
        """configure(set_shared=False) does not overwrite the module shared instance."""
        existing = WebFetcher(retry_indicators=["existing"])
        fetcher_module._state._set_shared(existing)

        cfg_dir = make_fetcher_config(tmp_path, retry=["new"])
        WebFetcher.configure(str(cfg_dir), set_shared=False)
        assert fetcher_module._state._peek_shared() is existing

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
        assert fetcher_module._state._peek_shared() is instance

    def test_edge_set_shared_false_leaves_shared_none(self):
        fetcher_module.reset_shared()
        WebFetcher.configure_defaults(set_shared=False)
        assert fetcher_module._state._peek_shared() is None

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
        assert fetcher_module._state._peek_shared() is instance
        assert "pkg" in instance.retry_indicators

    def test_normal_sk_configure_defaults_sets_shared(self):
        fetcher_module.reset_shared()
        instance = sk.configure_defaults()
        assert fetcher_module._state._peek_shared() is instance
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
        fetcher_module._state._set_shared(fetcher)
        result = module_fetch("http://example.com")
        assert result == "<html>proxied</html>"

    def test_normal_module_is_blocked_delegates_to_shared(self):
        fetcher = WebFetcher(block_indicators=["BLOCKED"])
        fetcher_module._state._set_shared(fetcher)
        assert module_is_blocked("<html>BLOCKED</html>") is True
        assert module_is_blocked("<html>clean</html>") is False

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_module_browser_delegates_to_shared(self, MockStealthy):
        fetcher = WebFetcher()
        fetcher_module._state._set_shared(fetcher)
        session = module_browser()
        assert isinstance(session, InteractiveSession)

    @patch.object(WebFetcher, "scrape", autospec=True)
    def test_normal_module_scrape_delegates_to_shared(self, mock_scrape):
        fetcher = WebFetcher()
        fetcher_module._state._set_shared(fetcher)
        module_scrape(["http://a.com"], callback=MagicMock(), mode=ScrapeMode.FAST)
        mock_scrape.assert_called_once()

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
    """Shared-instance state lives in the ._state leaf."""

    def test_normal_state_leaf_is_single_source_of_truth(self):
        """Package reset_shared resolves to the ._state leaf function."""
        state = fetcher_module._state
        assert sk.reset_shared is state.reset_shared
        assert fetcher_module.reset_shared is state.reset_shared

    def test_normal_get_shared_autoconfigures_state(self):
        """_get_shared() stores the auto-created default instance in ._state."""
        state = fetcher_module._state
        fetcher_module.reset_shared()
        shared = _get_shared()
        assert state._peek_shared() is shared
        assert shared.retry_indicators == list(WebFetcher._DEFAULT_RETRY)


# ── Helper for full config ───────────────────────────────────────────────────


def _write_full_scraper_config(tmp_path, config: dict, name="scraper_config.yaml", dirname="config") -> str:
    """Write a complete scraper-config YAML with any top-level keys and return the dir."""
    cfg_dir = tmp_path / dirname
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / name).write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(cfg_dir)


class TestParserConfig:
    """configure() accepts parser config section and makes it available for future use."""

    def test_normal_parser_section_accepted(self, tmp_path):
        """parser section with adaptive, adaptive_domain, markdown.main_content_only is accepted."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "parser": {
                "adaptive": True,
                "adaptive_domain": "example.com",
                "markdown": {"main_content_only": False},
            },
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        # Parser config is stored for future use by parse module
        assert instance._parser_config == config["parser"]

    def test_normal_parser_section_optional_keys(self, tmp_path):
        """parser section works with only some keys present."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "parser": {"adaptive": True},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._parser_config == {"adaptive": True}

    def test_normal_no_parser_section_defaults_to_empty(self, tmp_path):
        """Missing parser section yields empty config dict."""
        config = {"retry_indicators": ["test"], "block_indicators": ["blocked"]}
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._parser_config == {}


class TestBrowserConfig:
    """configure() maps browser config section to browser() kwargs."""

    def test_normal_browser_section_all_keys(self, tmp_path):
        """browser section with all keys maps to browser() kwargs."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {
                "block_ads": True,
                "dns_over_https": True,
                "blocked_domains": ["ads.example.com", "tracker.example.com"],
                "pierce_shadow": True,
                "record_requests": True,
                "max_recorded_requests": 500,
                "user_data_dir": "/tmp/browser-profile",
                "save_state_path": "/tmp/state.json",
                "load_state_path": "/tmp/state.json",
                "extra_flags": ["--custom-flag"],
            },
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        browser_cfg = instance._browser_config
        assert browser_cfg["block_ads"] is True
        assert browser_cfg["dns_over_https"] is True
        assert browser_cfg["blocked_domains"] == {"ads.example.com", "tracker.example.com"}
        assert browser_cfg["pierce_shadow"] is True
        assert browser_cfg["record_requests"] is True
        assert browser_cfg["max_recorded_requests"] == 500
        assert browser_cfg["user_data_dir"] == "/tmp/browser-profile"
        assert browser_cfg["save_state_path"] == "/tmp/state.json"
        assert browser_cfg["load_state_path"] == "/tmp/state.json"
        assert browser_cfg["extra_flags"] == ["--custom-flag"]

    def test_normal_browser_section_partial_keys(self, tmp_path):
        """browser section works with only some keys present."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"block_ads": True, "pierce_shadow": False},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        browser_cfg = instance._browser_config
        assert browser_cfg["block_ads"] is True
        assert browser_cfg["pierce_shadow"] is False
        # Other keys not present
        assert "dns_over_https" not in browser_cfg
        assert "blocked_domains" not in browser_cfg

    def test_normal_browser_blocked_domains_list_converted_to_set(self, tmp_path):
        """blocked_domains from YAML list becomes a set for Scrapling."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"blocked_domains": ["a.com", "b.com"]},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert isinstance(instance._browser_config["blocked_domains"], set)
        assert instance._browser_config["blocked_domains"] == {"a.com", "b.com"}

    def test_normal_browser_user_data_dir_empty_string_omitted(self, tmp_path):
        """Empty user_data_dir string is omitted (never pass None to Scrapling)."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"user_data_dir": ""},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert "user_data_dir" not in instance._browser_config

    def test_normal_no_browser_section_defaults_to_empty(self, tmp_path):
        """Missing browser section yields empty config dict."""
        config = {"retry_indicators": ["test"], "block_indicators": ["blocked"]}
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._browser_config == {}


class TestSpiderConfig:
    """configure() accepts spider config section and makes it available for future use."""

    def test_normal_spider_section_all_keys(self, tmp_path):
        """spider section with all keys is accepted."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "spider": {
                "concurrent_requests": 16,
                "concurrent_requests_per_domain": 8,
                "download_delay": 0.25,
                "autothrottle_enabled": True,
                "autothrottle_start_delay": 5.0,
                "autothrottle_max_delay": 60.0,
                "autothrottle_target_concurrency": 1.0,
                "robots_txt_obey": True,
                "crawl_strategy": "breadth",
                "crawldir": "/tmp/crawl",
                "development_mode": False,
            },
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._spider_config == config["spider"]

    def test_normal_spider_section_partial_keys(self, tmp_path):
        """spider section works with only some keys present."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "spider": {"concurrent_requests": 8, "download_delay": 0.5},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._spider_config == {"concurrent_requests": 8, "download_delay": 0.5}

    def test_normal_no_spider_section_defaults_to_empty(self, tmp_path):
        """Missing spider section yields empty config dict."""
        config = {"retry_indicators": ["test"], "block_indicators": ["blocked"]}
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._spider_config == {}


class TestDepthFirstFallback:
    """Depth-first fallback: call kwarg > configured > Scrapling default."""

    def test_browser_kwargs_override_configured_values(self, tmp_path):
        """Explicit browser() kwargs override configured values."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"block_ads": True, "pierce_shadow": False},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        # Call-time kwarg should override configured value
        kwargs = {}
        instance._merge_browser_kwargs(kwargs, block_ads=False, pierce_shadow=True)
        assert kwargs["block_ads"] is False  # Call kwarg wins
        assert kwargs["pierce_shadow"] is True  # Call kwarg wins

    def test_configured_values_used_when_kwarg_none(self, tmp_path):
        """Configured values are used when call-time kwarg is None."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"block_ads": True, "pierce_shadow": False},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        kwargs = {}
        instance._merge_browser_kwargs(kwargs, block_ads=None, pierce_shadow=None)
        assert kwargs["block_ads"] is True  # Configured value used
        assert kwargs["pierce_shadow"] is False  # Configured value used

    def test_scrapling_default_when_neither_configured_nor_kwarg(self, tmp_path):
        """Scrapling default applies when neither configured nor kwarg provided."""
        config = {"retry_indicators": ["test"], "block_indicators": ["blocked"]}
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        kwargs = {}
        instance._merge_browser_kwargs(kwargs, block_ads=None, pierce_shadow=None)
        # Neither configured nor kwarg - key not added to kwargs (Scrapling default)
        assert "block_ads" not in kwargs
        assert "pierce_shadow" not in kwargs


class TestSaveLoadStateNotConstructorKwargs:
    """save_state_path/load_state_path are NOT passed as StealthySession constructor kwargs."""

    def test_save_state_path_not_in_session_kwargs(self, tmp_path):
        """save_state_path is stored but not passed to StealthySession ctor."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"save_state_path": "/tmp/state.json"},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        browser_cfg = instance._browser_config
        assert browser_cfg["save_state_path"] == "/tmp/state.json"
        # When creating session, save_state_path should NOT be in kwargs passed to StealthySession
        kwargs = {}
        instance._merge_browser_kwargs(kwargs, **browser_cfg)
        assert "save_state_path" not in kwargs
        assert "load_state_path" not in kwargs

    def test_load_state_path_not_in_session_kwargs(self, tmp_path):
        """load_state_path is stored but not passed to StealthySession ctor."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"load_state_path": "/tmp/state.json"},
        }
        cfg_dir = _write_full_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        browser_cfg = instance._browser_config
        assert browser_cfg["load_state_path"] == "/tmp/state.json"
        kwargs = {}
        instance._merge_browser_kwargs(kwargs, **browser_cfg)
        assert "save_state_path" not in kwargs
        assert "load_state_path" not in kwargs


# ── status_retries configuration ────────────────────────────────────────────────


def _write_scraper_config(tmp_path, *, retry=(), block=(), status_retries=None, name="scraper_config.yaml", dirname="config"):
    """Write a scraper-config YAML with an optional status_retries map and return the dir."""
    cfg_dir = tmp_path / dirname
    cfg_dir.mkdir(parents=True, exist_ok=True)
    payload: dict = {"retry_indicators": list(retry), "block_indicators": list(block)}
    if status_retries is not None:
        payload["status_retries"] = status_retries
    (cfg_dir / name).write_text(yaml.safe_dump(payload), encoding="utf-8")
    return str(cfg_dir)


class TestStatusRetriesConfig:
    """configure() parses, merges, and validates the status_retries map."""

    def test_normal_absent_key_uses_class_defaults(self, tmp_path):
        """Without the key the resolved map is the three-code default."""
        cfg_dir = _write_scraper_config(tmp_path)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance.status_retries == {403: None, 429: None, 503: None}

    def test_normal_merges_over_defaults(self, tmp_path):
        """A listed code merges over the defaults, others keep None."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={429: 5})
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance.status_retries == {403: None, 429: 5, 503: None}

    def test_normal_explicit_zero_keeps_other_entries(self, tmp_path):
        """An explicit 0 is opt-out for one code only."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={503: 0, 429: 2})
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance.status_retries == {403: None, 429: 2, 503: 0}

    def test_normal_new_code_added(self, tmp_path):
        """A new code is added alongside the defaults."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={500: 2})
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance.status_retries == {403: None, 429: None, 503: None, 500: 2}

    def test_normal_string_key_parsed_to_int(self, tmp_path):
        """A numeric string key parses to its integer status."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={"429": 3})
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance.status_retries[429] == 3

    def test_normal_explicit_null_keeps_default_entry(self, tmp_path):
        """Listing a code with null resolves to the default entry."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={"429": None})
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance.status_retries == {403: None, 429: None, 503: None}

    def test_error_negative_value_raises_settings_error(self, tmp_path):
        """A negative budget is rejected at configure() time."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={429: -1})
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_non_int_value_raises_settings_error(self, tmp_path):
        """A non-int budget is rejected at configure() time."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={429: "many"})
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_numeric_string_value_raises_settings_error(self, tmp_path):
        """Values must be ints or null, not numeric strings."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={429: "5"})
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_non_int_key_raises_settings_error(self, tmp_path):
        """A non-integer status key is rejected at configure() time."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={"soon": 3})
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_non_mapping_value_raises_settings_error(self, tmp_path):
        """A list value is rejected at configure() time."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries=[429, 503])
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_normal_configure_defaults_passes_default_map(self):
        """The zero-config path resolves to the same default map."""
        instance = WebFetcher.configure_defaults(set_shared=False)
        assert instance.status_retries == {403: None, 429: None, 503: None}

    def test_normal_absent_key_keeps_log_lines_unchanged(self, tmp_path):
        """The two configure() log lines stay byte-identical without the key."""
        cfg_dir = _write_scraper_config(tmp_path, retry=["r"], block=["b"])
        with patch("scrape_kit.fetcher.web_fetcher.logger") as mock_log:
            WebFetcher.configure(cfg_dir, set_shared=False)
        assert mock_log.info.call_count == 1
        args = mock_log.info.call_args.args
        assert args[0] == "WebFetcher configured from '%s' (%d retry / %d block indicators)"

    def test_normal_present_key_adds_one_log_line(self, tmp_path):
        """A configured map adds exactly one log line naming the count."""
        cfg_dir = _write_scraper_config(tmp_path, retry=["r"], block=["b"], status_retries={429: 5, 500: 2})
        with patch("scrape_kit.fetcher.web_fetcher.logger") as mock_log:
            WebFetcher.configure(cfg_dir, set_shared=False)
        assert mock_log.info.call_count == 2
        first = mock_log.info.call_args_list[0].args
        second = mock_log.info.call_args_list[1].args
        assert first[0] == "WebFetcher configured from '%s' (%d retry / %d block indicators)"
        assert "%d" in second[0]
        assert 2 in [arg for arg in second[1:] if isinstance(arg, int)]

    def test_error_float_status_key_raises_settings_error(self, tmp_path):
        """A non-integer status key type is rejected at configure() time."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={429.5: 2})
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_out_of_range_status_key_raises_settings_error(self):
        """A key that parses as an int but is not an HTTP status is rejected."""
        for status in (-1, 0, 42, 999999):
            with pytest.raises(SettingsError):
                WebFetcher(status_retries={status: 5})

    def test_error_out_of_range_status_key_raises_at_configure(self, tmp_path):
        """The configure() path rejects non-HTTP keys too."""
        cfg_dir = _write_scraper_config(tmp_path, status_retries={-1: 5})
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_edge_valid_boundary_statuses_accepted(self):
        """100 and 599 are real HTTP statuses and stay accepted."""
        fetcher = WebFetcher(status_retries={100: 1, 599: 0})
        assert fetcher.status_retries[100] == 1
        assert fetcher.status_retries[599] == 0
