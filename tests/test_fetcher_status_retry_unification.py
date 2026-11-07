"""Status-retry unification: one canonical blocked-code default across fetch paths."""

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml

from scrape_kit.errors import SettingsError
from scrape_kit.fetcher import engine_adapter as engine_adapter_module
from scrape_kit.fetcher.engine_adapter import ScraplingEngineAdapter
from scrape_kit.fetcher.web_fetcher import WebFetcher
from scrape_kit.fetcher.web_fetcher import _DEFAULT_ESCALATION_BLOCKED, _DEFAULT_STATUS_RETRIES

pytestmark = pytest.mark.p0

A = "http://a.com"
CANON = set(engine_adapter_module._BLOCKED_CODES)


def _write_config(tmp_path, status_retries=None, spider=None):
    """Write a scraper-config YAML and return the config directory."""
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    payload: dict = {"retry_indicators": ["r"], "block_indicators": ["b"]}
    if status_retries is not None:
        payload["status_retries"] = status_retries
    if spider is not None:
        payload["spider"] = spider
    (cfg_dir / "scraper_config.yaml").write_text(yaml.safe_dump(payload), encoding="utf-8")
    return str(cfg_dir)


def _engine_gate(fetcher):
    """Return the blocked set and budget the adapter hands to the spider engine."""
    config = fetcher._engine_adapter._config
    return set(config["custom_blocked_codes"]), config["max_blocked_retries"]


class TestCanonicalDefault:
    """The 9-code set is the single canonical default for every path."""

    def test_normal_every_path_uses_the_same_set(self):
        """FAST map, browser gate, and engine adapter agree on one identical set."""
        fast = set(_DEFAULT_STATUS_RETRIES)
        browser = set(_DEFAULT_ESCALATION_BLOCKED)
        assert fast == browser == CANON

    def test_normal_default_set_is_exactly_nine_codes(self):
        """The canonical default is the 9-code set, asserted as a whole."""
        expected = {401, 403, 407, 429, 444, 500, 502, 503, 504}
        assert set(_DEFAULT_STATUS_RETRIES) == expected
        assert set(_DEFAULT_ESCALATION_BLOCKED) == expected
        assert expected == CANON

    def test_normal_default_budgets_are_all_none(self):
        """Every default FAST entry inherits the call-time retries argument."""
        assert set(_DEFAULT_STATUS_RETRIES.values()) == {None}


class TestUnconfiguredFetcherBlocksExactlyNineCodes:
    """An unconfigured fetcher blocks exactly the 9-code set on all paths."""

    PROBED = (200, 301, 401, 403, 407, 429, 444, 500, 502, 503, 504, 418)

    def test_normal_fast_path_set_equality(self):
        """FAST retries exactly the 9 codes and nothing else."""
        fetcher = WebFetcher()
        retried = {s for s in self.PROBED if fetcher._is_retryable_status(s)}
        assert retried == CANON

    def test_normal_browser_path_set_equality(self):
        """The escalation gate blocks exactly the 9 codes and nothing else."""
        fetcher = WebFetcher()
        blocked = {s for s in self.PROBED if fetcher._escalation_blocked(s)}
        assert blocked == CANON

    def test_normal_stealth_path_set_equality(self):
        """The engine adapter blocks exactly the 9 codes when nothing is configured."""
        fetcher = WebFetcher()
        blocked, budget = _engine_gate(fetcher)
        assert blocked == CANON
        assert budget == 3


class TestConfiguredMapReachesEngine:
    """A configured budget reaches the STEALTH spider through the adapter wiring."""

    def test_normal_adapter_config_carries_translated_map(self, tmp_path):
        """The adapter built by WebFetcher receives the translated merged map."""
        cfg = _write_config(tmp_path, status_retries={429: 5})
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        blocked, budget = _engine_gate(fetcher)
        assert budget == 5
        assert 429 in blocked
        assert 503 in blocked

    def test_normal_unconfigured_merged_map_reaches_engine(self, tmp_path):
        """With no YAML entry the merged map still populates the engine defaults."""
        cfg = _write_config(tmp_path)
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        blocked, budget = _engine_gate(fetcher)
        assert blocked == CANON
        assert budget == 3

    def test_normal_spider_gate_blocks_configured_code(self, tmp_path):
        """The spider's gate rejects a configured retryable status, accepts a clean one."""
        cfg = _write_config(tmp_path, status_retries={500: 3})
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        spider_cls = engine_adapter_module.build_spider_class({**fetcher._engine_adapter._config, "mode": "stealth"})
        assert asyncio.run(spider_cls.is_blocked(SimpleNamespace(), SimpleNamespace(status=500))) is True
        assert asyncio.run(spider_cls.is_blocked(SimpleNamespace(), SimpleNamespace(status=200))) is False


class _FakeSpider:
    """Spider stand-in replaying one pass with engine blocked-retry semantics."""

    def __init__(self, pages=None, blocked=(), budget=3):
        """Initialize with a page map, blocked statuses, and retry budget."""
        self.start_urls = ()
        self.started = 0
        self.blocked_attempts: list[int] = []
        self.delivered: list[str] = []
        self.retry_counts: dict[str, int] = {}
        self._callback = lambda url, html, final=False: None
        self._errors_urls: list[str] = []
        self._pages = pages or {}
        self._blocked = set(blocked)
        self._budget = budget

    def start(self):
        """Run one pass, cycling each URL through its blocked-retry budget."""
        self.started += 1
        self._errors_urls = []
        for url in list(self.start_urls):
            status = self._pages.get(url, 200)
            attempts = 0
            while status in self._blocked and attempts < self._budget:
                self.blocked_attempts.append(status)
                attempts += 1
                self.retry_counts[url] = attempts
            self.delivered.append(url)
            self._callback(url, f"<html>{url}</html>", False)
        if self._errors_urls:
            raise RuntimeError(", ".join(self._errors_urls))


def _wire(spider):
    """Return a _build_spider replacement configuring ``spider`` per pass."""

    def _build(_self, urls, callback, mode, max_concurrency):
        spider.start_urls = tuple(urls)
        spider._callback = callback
        return spider

    return _build


def _spider_for(fetcher, pages):
    """Build a fake spider wired to the fetcher's current engine gate."""
    blocked, budget = _engine_gate(fetcher)
    return _FakeSpider(pages=pages, blocked=blocked, budget=budget)


class TestStealthEndToEnd:
    """End-to-end stealth passes honor the configured map."""

    def test_normal_configured_budget_gives_five_retries(self, tmp_path):
        """A 429 with budget 5 is blocked five times before delivery."""
        cfg = _write_config(tmp_path, status_retries={429: 5})
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        spider = _spider_for(fetcher, pages={A: 429})
        with patch.object(ScraplingEngineAdapter, "_build_spider", _wire(spider)):
            delivered = []
            fetcher.scrape([A], lambda url, html: delivered.append(url), mode="stealth")
        assert spider.blocked_attempts.count(429) == 5
        assert spider.retry_counts[A] == 5
        assert delivered == [A]

    def test_normal_explicit_zero_never_blocks(self, tmp_path):
        """A 503 with budget 0 is never blocked and is delivered."""
        cfg = _write_config(tmp_path, status_retries={503: 0})
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        spider = _spider_for(fetcher, pages={A: 503})
        with patch.object(ScraplingEngineAdapter, "_build_spider", _wire(spider)):
            delivered = []
            fetcher.scrape([A], lambda url, html: delivered.append(url), mode="stealth")
        assert spider.blocked_attempts == []
        assert delivered == [A]

    def test_normal_unconfigured_blocks_the_nine_code_set(self, tmp_path):
        """With no configuration a blocked status retries up to the default budget."""
        cfg = _write_config(tmp_path)
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        blocked, budget = _engine_gate(fetcher)
        assert blocked == CANON
        assert budget == 3
        spider = _spider_for(fetcher, pages={A: 500})
        with patch.object(ScraplingEngineAdapter, "_build_spider", _wire(spider)):
            fetcher.scrape([A], lambda url, html: None, mode="stealth")
        assert spider.blocked_attempts == [500, 500, 500]
        assert spider.delivered == [A]

    def test_normal_opt_out_removes_code_from_gate(self, tmp_path):
        """An opted-out code is absent from the engine's blocked gate."""
        cfg = _write_config(tmp_path, status_retries={503: 0})
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        blocked, _budget = _engine_gate(fetcher)
        assert 503 not in blocked
        assert blocked == CANON - {503}

    def test_error_invalid_status_still_rejected(self, tmp_path):
        """An out-of-range configured status is still rejected at configure time."""
        cfg = _write_config(tmp_path, status_retries={999999: 1})
        with pytest.raises(SettingsError):
            WebFetcher.configure(cfg, set_shared=False)


class TestFastRegression:
    """Configured FAST/browser behavior is unchanged by the unification."""

    def test_normal_budget_overrides_call_time(self):
        """A configured budget still wins over the call-time retries argument."""
        fetcher = WebFetcher(status_retries={429: 5})
        assert fetcher._status_budget(429, 3) == 6

    def test_normal_explicit_zero_opts_out(self):
        """An explicit 0 still delivers the body on the FAST path."""
        fetcher = WebFetcher(status_retries={503: 0})
        assert fetcher._is_retryable_status(503) is False

    def test_normal_unlisted_code_inherits_defaults(self):
        """An unlisted code keeps its default entry in the merged map."""
        fetcher = WebFetcher(status_retries={429: 5})
        assert set(fetcher.status_retries) == CANON
        assert fetcher._is_retryable_status(500) is True

    def test_normal_none_entry_keeps_default_membership(self):
        """Naming a code with null cannot un-gate it."""
        fetcher = WebFetcher(status_retries={"429": None})
        assert fetcher._escalation_blocked(429) is True

    def test_normal_explicit_zero_delivers_on_browser_path(self):
        """An explicit 0 still delivers that status on the browser path."""
        fetcher = WebFetcher(status_retries={429: 0})
        assert fetcher._escalation_blocked(429) is False
        assert fetcher._escalation_blocked(503) is True


class TestScrapeDispatch:
    """scrape() dispatches through the constructor-built adapter, no fallback."""

    def test_normal_scrape_uses_the_bound_adapter(self, tmp_path):
        """scrape() routes through the adapter built in __init__."""
        cfg = _write_config(tmp_path)
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        adapter = fetcher._engine_adapter
        seen: dict = {}

        def spy(adapter_self, urls, callback, **kwargs):
            seen["adapter"] = adapter_self
            seen["mode"] = kwargs.get("mode")

        with patch.object(ScraplingEngineAdapter, "scrape", spy):
            fetcher.scrape([A], lambda url, html: None, mode="stealth")
        assert seen["adapter"] is adapter
        assert seen["mode"] == "stealth"

    def test_edge_translated_keys_merge_with_spider_config(self):
        """The translated keys merge cleanly alongside spider_config keys."""
        fetcher = WebFetcher(spider_config={"concurrent_requests": 8})
        config = fetcher._engine_adapter._config
        assert config["concurrent_requests"] == 8
        assert set(config["custom_blocked_codes"]) == CANON
        assert set(config["opt_out_codes"]) == set()
        assert config["max_blocked_retries"] == 3

    def test_normal_configured_yaml_spider_knob_survives(self, tmp_path):
        """A spider_config knob still reaches the adapter alongside the translation."""
        cfg = _write_config(tmp_path, status_retries={429: 5}, spider={"concurrent_requests": 16, "crawl_strategy": "dfs"})
        fetcher = WebFetcher.configure(cfg, set_shared=False)
        config = fetcher._engine_adapter._config
        assert config["concurrent_requests"] == 16
        assert config["crawl_strategy"] == "dfs"
        assert config["max_blocked_retries"] == 5
