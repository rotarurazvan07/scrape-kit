"""Tests for the single-engine Scrapling adapter."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from scrape_kit.errors import FetcherError
from scrape_kit.fetcher import WebFetcher
from scrape_kit.fetcher import engine_adapter as engine_adapter_module
from scrape_kit.fetcher.batch_types import ScrapeMode
from scrape_kit.fetcher.engine_adapter import ScraplingEngineAdapter

A = "http://a.com"
B = "http://b.com"


class TestTranslateStatusRetries:
    """status_retries maps to spider blocked-retry config."""

    def test_empty_returns_defaults(self):
        result = engine_adapter_module.translate_status_retries({})
        assert result["max_blocked_retries"] == 3
        assert result["custom_blocked_codes"] == set()
        assert result["opt_out_codes"] == set()

    def test_positive_budgets_include_codes(self):
        result = engine_adapter_module.translate_status_retries({403: 3, 429: 2})
        assert result["max_blocked_retries"] == 3
        assert result["custom_blocked_codes"] == {403, 429}
        assert result["opt_out_codes"] == set()

    def test_zero_budget_opts_out(self):
        result = engine_adapter_module.translate_status_retries({503: 0})
        assert result["max_blocked_retries"] == 3
        assert result["opt_out_codes"] == {503}
        assert result["custom_blocked_codes"] == set()

    def test_none_budget_inherits_defaults(self):
        result = engine_adapter_module.translate_status_retries({403: None, 500: None})
        assert result["custom_blocked_codes"] == {403, 500}

    def test_mixed_budgets(self):
        result = engine_adapter_module.translate_status_retries({403: 3, 503: 0, 429: None})
        assert result["max_blocked_retries"] == 3
        assert result["custom_blocked_codes"] == {403, 429}
        assert result["opt_out_codes"] == {503}


class TestBuildSpiderClass:
    """Dynamic spider class construction from config."""

    def test_fast_mode_defaults(self):
        spider_cls = engine_adapter_module.build_spider_class({})
        assert spider_cls.concurrent_requests == 4
        assert spider_cls.crawl_strategy == "bfs"
        assert spider_cls._mode == ScrapeMode.FAST

    def test_stealth_mode_flag(self):
        spider_cls = engine_adapter_module.build_spider_class({"mode": ScrapeMode.STEALTH})
        assert spider_cls._mode == ScrapeMode.STEALTH

    def test_custom_config_overrides(self):
        spider_cls = engine_adapter_module.build_spider_class(
            {
                "concurrent_requests": 16,
                "concurrent_requests_per_domain": 8,
                "download_delay": 0.25,
                "autothrottle_enabled": True,
                "crawl_strategy": "dfs",
                "max_blocked_retries": 5,
            }
        )
        assert spider_cls.concurrent_requests == 16
        assert spider_cls.concurrent_requests_per_domain == 8
        assert spider_cls.download_delay == 0.25
        assert spider_cls.autothrottle_enabled is True
        assert spider_cls.crawl_strategy == "dfs"
        assert spider_cls.max_blocked_retries == 5

    def test_translated_blocked_codes_take_effect(self):
        spider_cls = engine_adapter_module.build_spider_class({"custom_blocked_codes": {403}, "opt_out_codes": {503}})
        assert hasattr(spider_cls, "is_blocked")


class TestAdapterScrape:
    """Adapter scrape() blocking contract and guards."""

    def test_init_defaults(self):
        adapter = ScraplingEngineAdapter()
        assert adapter._config == {}
        assert adapter._spider_class is None
        assert adapter._fetcher_instance is None

    def test_init_with_config(self):
        adapter = ScraplingEngineAdapter({"concurrent_requests": 8})
        assert adapter._config["concurrent_requests"] == 8

    def test_empty_urls_returns_early(self):
        adapter = ScraplingEngineAdapter()
        results = []
        adapter.scrape([], lambda url, html: results.append((url, html)))
        assert results == []

    def test_invalid_mode_raises(self):
        adapter = ScraplingEngineAdapter()
        with pytest.raises(ValueError, match="Unsupported scrape mode"):
            adapter.scrape([A], lambda u, h: None, mode="invalid")

    def test_zero_concurrency_raises(self):
        adapter = ScraplingEngineAdapter()
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            adapter.scrape([A], lambda u, h: None, max_concurrency=0)

    def test_negative_max_retries_raises(self):
        adapter = ScraplingEngineAdapter()
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            adapter.scrape([A], lambda u, h: None, max_retries=-1)

    @patch.object(engine_adapter_module.ScraplingEngineAdapter, "_build_spider")
    def test_stealth_mode_starts_spider(self, mock_build_spider):
        mock_spider = MagicMock()
        mock_build_spider.return_value = mock_spider

        adapter = ScraplingEngineAdapter()
        delivered = []
        adapter.scrape(
            [A],
            lambda url, html: delivered.append(url),
            mode=ScrapeMode.STEALTH,
        )

        mock_build_spider.assert_called_once()
        mock_spider.start.assert_called_once()

    def test_fast_mode_requires_a_fetcher_instance(self):
        adapter = ScraplingEngineAdapter()
        with pytest.raises(FetcherError, match="No WebFetcher instance set"):
            adapter.scrape([A], lambda u, h: None)

    @patch.object(engine_adapter_module.ScraplingEngineAdapter, "_build_spider")
    def test_exception_becomes_fetcher_error(self, mock_build_spider):
        mock_spider = MagicMock()
        mock_spider.start.side_effect = RuntimeError("engine down")
        mock_build_spider.return_value = mock_spider

        adapter = ScraplingEngineAdapter()
        with pytest.raises(FetcherError, match="Scrape failed"):
            adapter.scrape([A], lambda u, h: None, mode=ScrapeMode.STEALTH)


class TestWebFetcherIntegration:
    """WebFetcher delegates FAST mode to its own fetch() policy."""

    def test_configure_wires_an_adapter(self):
        fetcher = WebFetcher(spider_config={"concurrent_requests": 8})
        assert fetcher._engine_adapter is not None
        assert fetcher._engine_adapter._config["concurrent_requests"] == 8

    def test_adapter_is_bound_to_its_fetcher(self):
        fetcher = WebFetcher(spider_config={"concurrent_requests": 8})
        assert fetcher._engine_adapter._fetcher_instance is fetcher

    def test_adapter_is_always_present(self):
        """Every WebFetcher dispatches through the single engine adapter."""
        assert WebFetcher()._engine_adapter is not None

    @patch.object(WebFetcher, "fetch")
    def test_scrape_delivers_every_url(self, mock_fetch):
        mock_fetch.side_effect = lambda url, **kw: f"<html>{url}</html>"
        fetcher = WebFetcher()
        delivered = []
        fetcher.scrape([A, B], callback=lambda url, html: delivered.append(url))
        assert sorted(delivered) == sorted([A, B])

    @patch.object(WebFetcher, "fetch")
    def test_hook_accept_delivers_once(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()
        delivered = []
        fetcher.scrape(
            [A],
            callback=lambda url, html: delivered.append(url),
            retry_hook=lambda url, html: None,
            max_retries=2,
        )
        assert delivered == [A]
        assert mock_fetch.call_count == 1

    @patch.object(WebFetcher, "fetch")
    def test_hook_exception_propagates(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()

        def bad_hook(url, html):
            raise RuntimeError("hook bug")

        with pytest.raises(RuntimeError, match="hook bug"):
            fetcher.scrape([A], callback=lambda u, h: None, retry_hook=bad_hook, max_retries=2)
        assert mock_fetch.call_count == 1

    @patch.object(WebFetcher, "fetch")
    def test_persistent_rejection_delivers_once_with_warning(self, mock_fetch, caplog):
        mock_fetch.return_value = "<html>reject</html>"
        fetcher = WebFetcher()
        with caplog.at_level(logging.WARNING, logger="scrape_kit.fetcher.engine_adapter"):
            delivered = []
            fetcher.scrape(
                [A],
                callback=lambda url, html: delivered.append(url),
                retry_hook=lambda url, html: "thin",
                max_retries=2,
            )
        assert delivered == [A]
        assert mock_fetch.call_count == 3
        assert any(A in record.message and "thin" in record.message for record in caplog.records)

    @patch.object(WebFetcher, "fetch")
    def test_empty_urls_with_hook_never_calls_hook(self, mock_fetch):
        fetcher = WebFetcher()
        hook = MagicMock(return_value="reason")
        fetcher.scrape([], callback=lambda u, h: None, retry_hook=hook, max_retries=3)
        hook.assert_not_called()

    def test_negative_max_retries_raises_before_fetch(self):
        fetcher = WebFetcher()
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            fetcher.scrape([A], callback=lambda u, h: None, max_retries=-1)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
