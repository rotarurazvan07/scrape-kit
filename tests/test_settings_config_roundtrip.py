"""SettingsManager YAML round-trip: configure() loads, merges, and validates config sections."""

import yaml
import pytest

from scrape_kit.errors import SettingsError
from scrape_kit.fetcher.web_fetcher import WebFetcher

pytestmark = pytest.mark.p1


def _write_scraper_config(tmp_path, config: dict, name: str = "scraper_config.yaml") -> str:
    """Write a scraper-config YAML and return the directory path."""
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / name).write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(cfg_dir)


def _write_settings_file(tmp_path, structure: dict) -> str:
    """Write multiple YAML files into tmp_path/config and return the directory path."""
    cfg_dir = tmp_path / "config"
    for rel, content in structure.items():
        target = cfg_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return str(cfg_dir)


# ── Normal: full schema YAML → configure() → attrs ─────────────────────────────


class TestConfigRoundtripNormal:
    """Normal cases: full schema YAML round-trips through configure()."""

    def test_normal_full_schema_all_sections(self, tmp_path):
        """Full schema with parser, browser, spider sections round-trips."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "parser": {
                "adaptive": True,
                "adaptive_domain": "example.com",
                "markdown": {"main_content_only": False},
            },
            "browser": {
                "block_ads": True,
                "dns_over_https": True,
                "blocked_domains": ["ads.example.com", "tracker.example.com"],
                "pierce_shadow": True,
                "record_requests": True,
                "max_recorded_requests": 500,
                "user_data_dir": "/tmp/browser-profile",
                "extra_flags": ["--custom-flag"],
            },
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
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)

        assert instance.retry_indicators == ["test"]
        assert instance.block_indicators == ["blocked"]
        assert instance._parser_config == config["parser"]
        assert instance._browser_config["block_ads"] is True
        assert instance._browser_config["dns_over_https"] is True
        assert instance._browser_config["blocked_domains"] == {"ads.example.com", "tracker.example.com"}
        assert instance._browser_config["pierce_shadow"] is True
        assert instance._browser_config["record_requests"] is True
        assert instance._browser_config["max_recorded_requests"] == 500
        assert instance._browser_config["user_data_dir"] == "/tmp/browser-profile"
        assert instance._browser_config["extra_flags"] == ["--custom-flag"]
        assert instance._spider_config == config["spider"]

    def test_normal_proxies_list_in_config(self, tmp_path):
        """proxies list in config is available for ProxyRotator instantiation."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "proxies": ["http://proxy1:8080", "http://proxy2:8080"],
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        # Proxies are stored at top level for ProxyRotator use
        assert instance._proxy_config == ["http://proxy1:8080", "http://proxy2:8080"]

    def test_normal_configure_defaults_roundtrip(self):
        """configure_defaults() produces instance with class defaults."""
        instance = WebFetcher.configure_defaults(set_shared=False)
        assert instance.retry_indicators == list(WebFetcher._DEFAULT_RETRY)
        assert instance.block_indicators == list(WebFetcher._DEFAULT_BLOCK)
        assert instance._parser_config == {}
        assert instance._browser_config == {}
        assert instance._spider_config == {}
        assert instance.status_retries == {403: None, 429: None, 503: None}

    def test_normal_status_retries_merges_over_defaults(self, tmp_path):
        """status_retries merges over class defaults."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "status_retries": {429: 5, 500: 2},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance.status_retries == {403: None, 429: 5, 503: None, 500: 2}


# ── Edge: partial sections, missing keys, null values, unknown keys ────────────


class TestConfigRoundtripEdge:
    """Edge cases: partial sections, missing keys, null values, unknown keys."""

    def test_edge_partial_parser_section(self, tmp_path):
        """Parser section works with only some keys present."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "parser": {"adaptive": True},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._parser_config == {"adaptive": True}

    def test_edge_partial_browser_section(self, tmp_path):
        """Browser section works with only some keys present."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"block_ads": True, "pierce_shadow": False},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        browser_cfg = instance._browser_config
        assert browser_cfg["block_ads"] is True
        assert browser_cfg["pierce_shadow"] is False
        assert "dns_over_https" not in browser_cfg
        assert "blocked_domains" not in browser_cfg

    def test_edge_partial_spider_section(self, tmp_path):
        """Spider section works with only some keys present."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "spider": {"concurrent_requests": 8, "download_delay": 0.5},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._spider_config == {"concurrent_requests": 8, "download_delay": 0.5}

    def test_edge_missing_scraper_config_falls_back_to_defaults(self, tmp_path):
        """Missing scraper_config.yaml falls back to class defaults."""
        config = {"retry_indicators": ["test"], "block_indicators": ["blocked"]}
        cfg_dir = _write_scraper_config(tmp_path, config, name="other.yaml")
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance.retry_indicators == list(WebFetcher._DEFAULT_RETRY)
        assert instance.block_indicators == list(WebFetcher._DEFAULT_BLOCK)
        assert instance._parser_config == {}
        assert instance._browser_config == {}
        assert instance._spider_config == {}

    def test_edge_null_values_in_sections(self, tmp_path):
        """Null values in sections are preserved as None."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "parser": {"adaptive": None},
            "browser": {"block_ads": None},
            "spider": {"concurrent_requests": None},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._parser_config["adaptive"] is None
        assert instance._browser_config["block_ads"] is None
        assert instance._spider_config["concurrent_requests"] is None

    def test_edge_unknown_keys_in_sections_ignored(self, tmp_path):
        """Unknown keys in sections are stored but not validated."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "parser": {"unknown_key": "value"},
            "browser": {"unknown_flag": True},
            "spider": {"unknown_setting": 42},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert instance._parser_config["unknown_key"] == "value"
        assert instance._browser_config["unknown_flag"] is True
        assert instance._spider_config["unknown_setting"] == 42

    def test_edge_unknown_top_level_keys_ignored(self, tmp_path):
        """Unknown top-level keys are ignored; known sections still load."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "custom_section": {"key": "value"},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        # configure() only reads the keys it knows; unknown ones neither
        # raise nor leak into the instance.
        assert instance.retry_indicators == ["test"]
        assert instance.block_indicators == ["blocked"]
        assert not hasattr(instance, "custom_section")

    def test_edge_depth_first_fallback_leaf_lookup(self, tmp_path):
        """Depth-first fallback finds leaf keys in nested config files."""
        structure = {
            "a/config.yaml": "timeout: 10\n",
            "a/b/config.yaml": "timeout: 20\n",
            "a/b/c/config.yaml": "timeout: 30\n",
        }
        cfg_dir = _write_settings_file(tmp_path, structure)
        from scrape_kit.settings import SettingsManager

        settings_mgr = SettingsManager(cfg_dir)
        # Full path access distinguishes them
        assert settings_mgr.get("config", "a", "config", "timeout") == 10
        assert settings_mgr.get("config", "a", "b", "config", "timeout") == 20
        assert settings_mgr.get("config", "a", "b", "c", "config", "timeout") == 30

    def test_edge_empty_string_user_data_dir_omitted(self, tmp_path):
        """Empty user_data_dir string is omitted from browser config."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"user_data_dir": ""},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert "user_data_dir" not in instance._browser_config

    def test_edge_blocked_domains_yaml_list_converted_to_set(self, tmp_path):
        """blocked_domains from YAML list becomes a set for Scrapling."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "browser": {"blocked_domains": ["a.com", "b.com"]},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        instance = WebFetcher.configure(cfg_dir, set_shared=False)
        assert isinstance(instance._browser_config["blocked_domains"], set)
        assert instance._browser_config["blocked_domains"] == {"a.com", "b.com"}


# ── Error: malformed YAML, bad keys ────────────────────────────────────────────


class TestConfigRoundtripError:
    """Error cases: malformed YAML, bad status_retries keys."""

    def test_error_malformed_yaml_raises_settings_error(self, tmp_path):
        """Malformed YAML raises SettingsError."""
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "scraper_config.yaml").write_text("invalid: [unclosed", encoding="utf-8")
        with pytest.raises(SettingsError, match="Failed to load settings file"):
            WebFetcher.configure(str(cfg_dir), set_shared=False)

    def test_error_status_retries_negative_value_raises(self, tmp_path):
        """Negative status_retries budget raises SettingsError."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "status_retries": {429: -1},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_status_retries_non_int_value_raises(self, tmp_path):
        """Non-int status_retries value raises SettingsError."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "status_retries": {429: "many"},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_status_retries_numeric_string_value_raises(self, tmp_path):
        """Numeric string status_retries value raises SettingsError."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "status_retries": {429: "5"},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_status_retries_non_int_key_raises(self, tmp_path):
        """Non-integer status_retries key raises SettingsError."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "status_retries": {"soon": 3},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_status_retries_non_mapping_value_raises(self, tmp_path):
        """List value for status_retries raises SettingsError."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "status_retries": [429, 503],
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_float_status_key_raises(self, tmp_path):
        """Float status key raises SettingsError."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "status_retries": {429.5: 2},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_out_of_range_status_key_raises(self):
        """Out-of-range status key raises SettingsError at instance creation."""
        for status in (-1, 0, 42, 999999):
            with pytest.raises(SettingsError):
                WebFetcher(status_retries={status: 5})

    def test_error_out_of_range_status_key_raises_at_configure(self, tmp_path):
        """Out-of-range status key raises SettingsError at configure()."""
        config = {
            "retry_indicators": ["test"],
            "block_indicators": ["blocked"],
            "status_retries": {-1: 5},
        }
        cfg_dir = _write_scraper_config(tmp_path, config)
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg_dir, set_shared=False)

    def test_error_unreadable_yaml_file_raises_settings_error(self, tmp_path):
        """Unreadable YAML file raises SettingsError (skipped when running as root)."""
        import os

        if os.geteuid() == 0:
            pytest.skip("Root user ignores file permissions")
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        config_file = cfg_dir / "scraper_config.yaml"
        config_file.write_text("valid: yaml", encoding="utf-8")
        # Make file unreadable
        config_file.chmod(0o000)
        try:
            with pytest.raises(SettingsError, match="File system access failed"):
                WebFetcher.configure(str(cfg_dir), set_shared=False)
        finally:
            config_file.chmod(0o644)

    def test_error_ambiguous_config_layout_raises(self, tmp_path):
        """YAML stem colliding with sibling directory raises SettingsError."""
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "db.yaml").write_text("host: a")
        (cfg_dir / "db").mkdir()
        (cfg_dir / "db" / "cache.yaml").write_text("ttl: 300")
        with pytest.raises(SettingsError, match="Ambiguous config layout"):
            WebFetcher.configure(str(cfg_dir), set_shared=False)
