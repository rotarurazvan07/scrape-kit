"""Scrapling engine adapter: thin synchronous wrapper over Scrapling's spider engine."""

import logging
from collections.abc import Callable
from typing import Any

from scrapling.spiders import CrawlSpider, Response

from ..errors import FetcherError
from ..logger import get_logger
from .batch_types import RetryHook, ScrapeMode, _EngineError, _UserCodeError

logger = get_logger(__name__)

# Scrapling's default blocked codes (verified against the installed package).
_BLOCKED_CODES: set[int] = {401, 403, 407, 429, 444, 500, 502, 503, 504}


def translate_status_retries(status_retries: dict[int, int | None]) -> dict[str, Any]:
    """Convert scrape-kit status_retries to Scrapling spider config.

    Args:
        status_retries: Mapping of HTTP status code to retry budget.
            budget > 0 includes the code in the blocked set, budget == 0
            opts it out, and budget is None inherits default membership.

    Returns:
        Dict with max_blocked_retries, custom_blocked_codes, opt_out_codes.
    """
    positive_budgets = [v for v in status_retries.values() if v is not None and v > 0]
    max_blocked_retries = max(positive_budgets) if positive_budgets else 3

    blocked_codes: set[int] = set()
    opt_out_codes: set[int] = set()
    for status, budget in status_retries.items():
        if budget is None:
            if status in _BLOCKED_CODES:
                blocked_codes.add(status)
        elif budget > 0:
            blocked_codes.add(status)
        else:
            opt_out_codes.add(status)

    return {
        "max_blocked_retries": max_blocked_retries,
        "custom_blocked_codes": blocked_codes,
        "opt_out_codes": opt_out_codes,
    }


def build_spider_class(config: dict[str, Any]) -> type[CrawlSpider]:
    """Dynamically build a CrawlSpider subclass from config.

    Args:
        config: Spider configuration keys mapped 1:1 to Scrapling spider
            class attributes (concurrent_requests, download_delay,
            autothrottle_*, robots_txt_obey, crawl_strategy, development_mode,
            max_blocked_retries, logging_level, log_file), plus the translated
            custom_blocked_codes / opt_out_codes and a mode key selecting the
            session type.

    Returns:
        A CrawlSpider subclass configured for the given parameters.
    """
    mode = config.get("mode", ScrapeMode.FAST)
    custom_blocked: set[int] = set(config.get("custom_blocked_codes") or set())
    opt_out: set[int] = set(config.get("opt_out_codes") or set())

    class ConfiguredCrawlSpider(CrawlSpider):
        name = "configured_crawl_spider"
        start_urls: list[str] = []

        concurrent_requests = int(config.get("concurrent_requests", 4))
        concurrent_requests_per_domain = int(config.get("concurrent_requests_per_domain", 0))
        download_delay = float(config.get("download_delay", 0.0))
        autothrottle_enabled = bool(config.get("autothrottle_enabled", False))
        autothrottle_start_delay = float(config.get("autothrottle_start_delay", 5.0))
        autothrottle_max_delay = float(config.get("autothrottle_max_delay", 60.0))
        autothrottle_target_concurrency = float(config.get("autothrottle_target_concurrency", 1.0))
        robots_txt_obey = bool(config.get("robots_txt_obey", False))
        crawl_strategy: str = config.get("crawl_strategy", "bfs")  # type: ignore[assignment]
        development_mode = bool(config.get("development_mode", False))
        max_blocked_retries = int(config.get("max_blocked_retries", 3))
        logging_level = int(config.get("logging_level", logging.DEBUG))
        log_file = config.get("log_file")

        # Set per instance by the adapter; the engine passes the final-pass
        # flag as a third argument, so the type accepts both call shapes.
        _callback: Callable[..., None]

        async def is_blocked(self, response: Response) -> bool:
            status = response.status
            if status in opt_out:
                return False
            if status in custom_blocked:
                return True
            return status in _BLOCKED_CODES

        async def parse(self, response: Response) -> Any:
            if getattr(self, "_final", False):
                self._callback(response.url, response.html_content, True)
            else:
                self._callback(response.url, response.html_content)
            yield None

        _mode = mode

    return ConfiguredCrawlSpider


class ScraplingEngineAdapter:
    """Synchronous blocking adapter over Scrapling's async spider engine.

    Preserves the blocking scrape() contract: blocks until every URL is
    processed and invokes callback(url, html) once per successful fetch.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize adapter with spider configuration.

        Args:
            config: Spider configuration dictionary. Keys map to Scrapling
                spider class attributes (see build_spider_class).
        """
        self._config: dict[str, Any] = config or {}
        self._spider_class: Any = None
        self._fetcher_instance: Any = None

    def _build_spider(self, urls: list[str], callback: Callable[[str, str], None], mode: str, max_concurrency: int) -> Any:
        """Build a spider instance for this scrape pass.

        Args:
            urls: Start URLs for this pass.
            callback: Per-URL callback invoked from parse().
            mode: Scrape mode selecting the session type.
            max_concurrency: Concurrent requests for this pass.

        Returns:
            A configured spider instance ready for start().
        """
        call_config = {**self._config, "mode": mode, "concurrent_requests": max_concurrency}
        if self._spider_class is None:
            self._spider_class = build_spider_class(call_config)
        spider = self._spider_class(
            crawldir=self._config.get("crawldir") or None,
            interval=float(self._config.get("interval", 300.0)),
        )
        spider.start_urls = tuple(urls)
        spider._callback = callback
        return spider

    def set_fetcher_instance(self, fetcher_instance: Any) -> None:
        """Set the WebFetcher instance used for FAST-mode fetch delegation.

        Args:
            fetcher_instance: The WebFetcher whose fetch() owns escalation
                and status-retry policy.
        """
        self._fetcher_instance = fetcher_instance

    def _run_fast_mode(self, spider: Any) -> None:
        """Run a FAST pass by delegating each URL to WebFetcher.fetch().

        This keeps FAST mode on the single fetch() policy seam (escalation,
        status_retries, indicator retries) while the engine supplies
        scheduling, AutoThrottle and checkpointed concurrency.

        Args:
            spider: Spider instance whose start_urls and _callback drive the pass.

        Raises:
            FetcherError: If no fetcher instance is set, or a fetch fails.
        """
        if self._fetcher_instance is None:
            raise FetcherError("No WebFetcher instance set for FAST mode delegation")
        final = bool(getattr(spider, "_final", False))
        callback = spider._callback
        errors: list[tuple[str, Exception]] = []
        for url in list(spider.start_urls):
            try:
                html = self._fetcher_instance.fetch(url)
            except FetcherError as exc:
                errors.append((getattr(exc, "url", None) or url, exc))
                continue
            except Exception as exc:
                errors.append((url, exc))
                continue
            if final:
                callback(url, html, final)
            else:
                callback(url, html)
        spider._errors_urls = [u for u, _ in errors]
        if errors:
            summary = ", ".join(f"{u}: {e}" for u, e in errors[:5])
            raise _EngineError(f"FAST mode fetch failed for {len(errors)} URLs. Sample: {summary}")

    def scrape(
        self,
        urls: list[str],
        callback: Callable[[str, str], None],
        mode: str = ScrapeMode.FAST,
        max_concurrency: int = 1,
        *,
        retry_hook: RetryHook | None = None,
        max_retries: int = 0,
    ) -> None:
        """Scrape multiple URLs using Scrapling's spider engine.

        Args:
            urls: List of URLs to scrape.
            callback: Function to call with (url, html) for each successful fetch.
            mode: Scraping mode - "fast" or "stealth" (ScrapeMode values).
            max_concurrency: Maximum concurrent requests.
            retry_hook: Optional quality gate consulted after each successful,
                non-blocked fetch. Returning a non-empty reason string defers
                that URL to another pass; None accepts the page.
            max_retries: Additional passes for hook-flagged URLs.

        Raises:
            ValueError: If mode is unsupported or max_concurrency < 1.
            FetcherError: If scraping encounters fetch failures.
        """
        if not urls:
            return
        self._validate_scrape_args(mode, max_concurrency, max_retries)

        flagged_urls: list[str] = []
        delivered_urls: list[str] = []

        # The hook is only consulted when a retry budget exists, so a hook
        # configured with max_retries=0 stays inert, exactly as before.
        hook = retry_hook if (retry_hook is not None and max_retries > 0) else None
        callback = self._gated_callback(hook, callback, flagged_urls, delivered_urls)

        budget = max_retries if hook is not None else 0
        pending: list[str] = list(urls)
        for attempt in range(1, budget + 2):
            final = attempt > budget
            flagged_urls.clear()
            delivered_urls.clear()

            spider = self._build_spider(pending, callback, mode, max_concurrency)
            next_pending = self._run_spider_pass(spider, final, flagged_urls, delivered_urls, pending, mode)
            if next_pending is None:
                return
            pending = next_pending

    @staticmethod
    def _validate_scrape_args(mode: str, max_concurrency: int, max_retries: int) -> None:
        """Validate scrape arguments before the engine loop starts.

        Args:
            mode: Scraping mode, must be a supported ScrapeMode value.
            max_concurrency: Maximum concurrent requests, must be at least 1.
            max_retries: Additional hook passes, must be at least 0.

        Raises:
            ValueError: If any argument falls outside its supported range.
        """
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if mode not in (ScrapeMode.FAST, ScrapeMode.STEALTH):
            raise ValueError(f"Unsupported scrape mode: {mode}")

    @staticmethod
    def _gated_callback(
        hook: RetryHook | None,
        user_callback: Callable[[str, str], None],
        flagged_urls: list[str],
        delivered_urls: list[str],
    ) -> Callable[..., None]:
        """Build the engine callback that gates delivery through the retry hook.

        A hook rejection defers the URL to another pass, except on the final
        pass where the rejected page is delivered with a warning. Every
        exception raised by consumer code is marked as consumer-owned so the
        pass layer propagates it unwrapped rather than bundling it as an
        engine failure.

        Args:
            hook: The retry hook, or None when no retry budget exists.
            user_callback: The consumer callback receiving accepted pages.
            flagged_urls: Collector for URLs deferred to the next pass.
            delivered_urls: Collector for URLs delivered on this pass.

        Returns:
            A callback accepting (url, html, final) for one engine pass.
        """

        def spider_callback(url: str, html: str, final: bool = False) -> None:
            try:
                if hook is not None:
                    reason = hook(url, html)
                    if reason and not final:
                        flagged_urls.append(url)
                        return
                    if reason:
                        logger.warning(
                            "Retry hook rejected %s on the final pass (%s) — delivering content anyway", url, reason
                        )
                delivered_urls.append(url)
                user_callback(url, html)
            except FetcherError as exc:
                raise _UserCodeError(f"Hook or callback failed for {url}: {exc}", url=url) from exc
            except Exception as exc:
                exc._sc_user_code = True  # type: ignore[attr-defined]
                raise

        return spider_callback

    def _run_spider_pass(
        self,
        spider: Any,
        final: bool,
        flagged_urls: list[str],
        delivered_urls: list[str],
        pending: list[str],
        mode: str,
    ) -> list[str] | None:
        """Run one spider pass and return next pending list, or None if done."""
        spider._final = final
        try:
            if mode == ScrapeMode.FAST:
                self._run_fast_mode(spider)
            else:
                spider.start()
        except (_UserCodeError, Exception) as exc:
            # Consumer-owned failures propagate immediately, never retried.
            if isinstance(exc, _UserCodeError) or getattr(exc, "_sc_user_code", False):
                raise
            failed = list(getattr(spider, "_errors_urls", []))
            if final:
                raise FetcherError(f"Scrape failed: {exc}") from exc
            if not failed and not isinstance(exc, _EngineError):
                raise FetcherError(f"Scrape failed: {exc}") from exc
            return self._next_pending(pending, failed, flagged_urls, delivered_urls)

        if final:
            return None
        failed = list(getattr(spider, "_errors_urls", []))
        return self._next_pending(pending, failed, flagged_urls, delivered_urls)

    @staticmethod
    def _next_pending(
        pending: list[str],
        failed: list[str],
        flagged: list[str],
        delivered: list[str],
    ) -> list[str]:
        """Rebuild the next pass list from failed and hook-flagged entries.

        Failed and flagged URLs keep their per-entry identity, and URLs that
        were neither delivered nor failed nor flagged stay pending.
        """
        remaining = list(pending)
        for url in delivered + flagged + failed:
            if url in remaining:
                remaining.remove(url)
        return failed + flagged + remaining


__all__ = [
    "ScraplingEngineAdapter",
    "build_spider_class",
    "translate_status_retries",
]
