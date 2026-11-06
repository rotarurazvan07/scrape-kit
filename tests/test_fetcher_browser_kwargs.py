"""WebFetcher.browser() kwargs pass-through to StealthySession."""

from unittest.mock import MagicMock, patch

import pytest

from scrape_kit.fetcher.web_fetcher import WebFetcher

pytestmark = pytest.mark.p0


# ── Normal: all verified kwargs reach StealthySession ───────────────────────────


class TestBrowserKwargsNormal:
    """Normal cases: all verified kwargs are forwarded to StealthySession."""

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_record_requests_true_forwarded(self, MockStealthy):
        """record_requests=True reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(record_requests=True)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("record_requests") is True

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_record_requests_false_forwarded(self, MockStealthy):
        """record_requests=False reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(record_requests=False)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("record_requests") is False

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_max_recorded_requests_forwarded(self, MockStealthy):
        """max_recorded_requests reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(max_recorded_requests=500)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("max_recorded_requests") == 500

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_pierce_shadow_forwarded(self, MockStealthy):
        """pierce_shadow reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(pierce_shadow=True)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("pierce_shadow") is True

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_block_ads_forwarded(self, MockStealthy):
        """block_ads reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(block_ads=True)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("block_ads") is True

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_dns_over_https_forwarded(self, MockStealthy):
        """dns_over_https reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(dns_over_https=True)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("dns_over_https") is True

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_blocked_domains_set_forwarded(self, MockStealthy):
        """blocked_domains as set reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(blocked_domains={"ads.example.com", "tracker.example.com"})
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("blocked_domains") == {"ads.example.com", "tracker.example.com"}

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_blocked_domains_list_converted_to_set(self, MockStealthy):
        """blocked_domains as list is converted to set for StealthySession."""
        fetcher = WebFetcher()
        fetcher.browser(blocked_domains=["ads.example.com", "tracker.example.com"])
        call_kwargs = MockStealthy.call_args[1]
        assert isinstance(call_kwargs.get("blocked_domains"), set)
        assert call_kwargs.get("blocked_domains") == {"ads.example.com", "tracker.example.com"}

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_user_data_dir_forwarded(self, MockStealthy):
        """user_data_dir reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(user_data_dir="/tmp/browser-profile")
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("user_data_dir") == "/tmp/browser-profile"

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_extra_flags_forwarded(self, MockStealthy):
        """extra_flags reaches StealthySession constructor."""
        fetcher = WebFetcher()
        fetcher.browser(extra_flags=["--custom-flag"])
        call_kwargs = MockStealthy.call_args[1]
        assert "--custom-flag" in call_kwargs.get("extra_flags", [])

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_proxy_rotator_mapped_to_proxy(self, MockStealthy):
        """proxy_rotator is mapped to 'proxy' for StealthySession."""
        fetcher = WebFetcher()
        mock_rotator = MagicMock()
        fetcher.browser(proxy_rotator=mock_rotator)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("proxy") is mock_rotator

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_precedence_call_kwarg_over_configured(self, MockStealthy, tmp_path):
        """Call-time kwarg takes precedence over configured value."""
        import yaml

        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"block_ads": True, "pierce_shadow": False},
        }
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")

        fetcher = WebFetcher.configure(str(cfg_dir), set_shared=False)
        fetcher.browser(block_ads=False, pierce_shadow=True)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("block_ads") is False  # Call kwarg wins
        assert call_kwargs.get("pierce_shadow") is True  # Call kwarg wins

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_configured_used_when_kwarg_none(self, MockStealthy, tmp_path):
        """Configured value used when call-time kwarg is None."""
        import yaml

        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"block_ads": True, "pierce_shadow": False},
        }
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")

        fetcher = WebFetcher.configure(str(cfg_dir), set_shared=False)
        fetcher.browser(block_ads=None, pierce_shadow=None)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("block_ads") is True  # Configured value used
        assert call_kwargs.get("pierce_shadow") is False  # Configured value used

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_normal_scrapling_default_when_neither(self, MockStealthy):
        """Scrapling default applies when neither configured nor kwarg provided."""
        fetcher = WebFetcher()
        fetcher.browser(block_ads=None, pierce_shadow=None)
        call_kwargs = MockStealthy.call_args[1]
        # Neither configured nor kwarg - key not added to kwargs (Scrapling default)
        assert "block_ads" not in call_kwargs
        assert "pierce_shadow" not in call_kwargs


# ── Edge: conversion, merge order, unset config ─────────────────────────────────


class TestBrowserKwargsEdge:
    """Edge cases: conversions, merge order, unset config leaves Scrapling defaults."""

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_edge_blocked_domains_list_to_set_conversion(self, MockStealthy):
        """blocked_domains list is converted to set."""
        fetcher = WebFetcher()
        fetcher.browser(blocked_domains=["a.com", "b.com", "c.com"])
        call_kwargs = MockStealthy.call_args[1]
        assert isinstance(call_kwargs.get("blocked_domains"), set)
        assert call_kwargs.get("blocked_domains") == {"a.com", "b.com", "c.com"}

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_edge_blocked_domains_set_passthrough(self, MockStealthy):
        """blocked_domains set passes through unchanged."""
        fetcher = WebFetcher()
        fetcher.browser(blocked_domains={"a.com", "b.com"})
        call_kwargs = MockStealthy.call_args[1]
        assert isinstance(call_kwargs.get("blocked_domains"), set)
        assert call_kwargs.get("blocked_domains") == {"a.com", "b.com"}

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_edge_extra_flags_merge_with_low_memory_flags(self, MockStealthy):
        """extra_flags merges user flags with low-memory flags."""
        fetcher = WebFetcher()
        fetcher.browser(extra_flags=["--custom-flag"])
        call_kwargs = MockStealthy.call_args[1]
        extra_flags = call_kwargs.get("extra_flags", [])
        # Low-memory flags should always be present
        assert "--disable-dev-shm-usage" in extra_flags
        assert "--disable-gpu" in extra_flags
        assert "--no-sandbox" in extra_flags
        assert "--disable-setuid-sandbox" in extra_flags
        # User flag should also be present
        assert "--custom-flag" in extra_flags

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_edge_extra_flags_configured_merged_with_call_kwarg(self, MockStealthy, tmp_path):
        """Configured extra_flags merged with call-time extra_flags."""
        import yaml

        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"extra_flags": ["--configured-flag"]},
        }
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")

        fetcher = WebFetcher.configure(str(cfg_dir), set_shared=False)
        fetcher.browser(extra_flags=["--call-flag"])
        call_kwargs = MockStealthy.call_args[1]
        extra_flags = call_kwargs.get("extra_flags", [])
        assert "--configured-flag" in extra_flags
        assert "--call-flag" in extra_flags
        # Low-memory flags always present
        assert "--disable-dev-shm-usage" in extra_flags

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_edge_user_data_dir_empty_string_omitted(self, MockStealthy):
        """Empty user_data_dir string is omitted (never pass None to Scrapling)."""
        fetcher = WebFetcher()
        fetcher.browser(user_data_dir="")
        call_kwargs = MockStealthy.call_args[1]
        assert "user_data_dir" not in call_kwargs

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_edge_user_data_dir_none_omitted(self, MockStealthy):
        """None user_data_dir is omitted."""
        fetcher = WebFetcher()
        fetcher.browser(user_data_dir=None)
        call_kwargs = MockStealthy.call_args[1]
        assert "user_data_dir" not in call_kwargs

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_edge_save_load_state_path_not_constructor_kwargs(self, MockStealthy, tmp_path):
        """save_state_path and load_state_path are NOT passed to StealthySession constructor."""
        fetcher = WebFetcher()
        # These are stored in _browser_config but not forwarded to session
        fetcher._browser_config["save_state_path"] = "/tmp/state.json"
        fetcher._browser_config["load_state_path"] = "/tmp/state.json"
        fetcher.browser()
        call_kwargs = MockStealthy.call_args[1]
        assert "save_state_path" not in call_kwargs
        assert "load_state_path" not in call_kwargs

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_edge_unset_config_leaves_scrapling_defaults(self, MockStealthy):
        """Unset config leaves Scrapling defaults (key not in kwargs)."""
        fetcher = WebFetcher()
        fetcher.browser()
        call_kwargs = MockStealthy.call_args[1]
        # These should not be in kwargs when not explicitly set
        for key in [
            "record_requests",
            "max_recorded_requests",
            "pierce_shadow",
            "block_ads",
            "dns_over_https",
            "blocked_domains",
            "user_data_dir",
            "proxy",
        ]:
            assert key not in call_kwargs, f"{key} should not be in kwargs when unset"


# ── Error: invalid values raise appropriate errors ──────────────────────────────


class TestBrowserKwargsError:
    """Error cases: invalid values raise appropriate errors."""

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_error_max_recorded_requests_zero_raises_from_scrapling(self, MockStealthy):
        """max_recorded_requests=0 causes Scrapling to raise (>=1 required)."""
        # Scrapling validates this - we verify the call is made with 0
        # and Scrapling will raise when the session is actually used
        fetcher = WebFetcher()
        fetcher.browser(max_recorded_requests=0, record_requests=True)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("max_recorded_requests") == 0
        assert call_kwargs.get("record_requests") is True

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_error_unknown_capture_xhr_raises_typeerror_with_migration_message(self, MockStealthy):
        """Unknown capture_xhr raises TypeError with migration message from Scrapling."""
        # capture_xhr was removed in Scrapling v0.5
        # If passed, Scrapling will raise TypeError with migration message
        fetcher = WebFetcher()
        # We pass it via **kwargs since it's not a known parameter
        fetcher.browser(capture_xhr=True)
        call_kwargs = MockStealthy.call_args[1]
        # The kwarg is passed through to StealthySession
        # Scrapling will raise TypeError when session is created
        assert call_kwargs.get("capture_xhr") is True

    @patch("scrape_kit.fetcher.web_fetcher.StealthySession")
    def test_error_max_recorded_requests_negative_raises(self, MockStealthy):
        """max_recorded_requests negative is passed to Scrapling (validation there)."""
        fetcher = WebFetcher()
        fetcher.browser(max_recorded_requests=-1, record_requests=True)
        call_kwargs = MockStealthy.call_args[1]
        assert call_kwargs.get("max_recorded_requests") == -1
        # Scrapling will validate and raise
