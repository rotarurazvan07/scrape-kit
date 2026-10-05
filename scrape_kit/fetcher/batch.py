"""Batch scraping strategies (fast thread pool / stealth async) and the ScrapeMode enum."""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any

from scrapling.fetchers import AsyncStealthySession

from ..errors import FetcherError
from ..logger import get_logger

logger = get_logger(__name__)


class ScrapeMode(str, Enum):
    """Batch scrape modes.

    A str-Enum: members are their plain string values, so raw strings
    ("fast"/"stealth") and ScrapeMode members are interchangeable wherever
    a mode is expected.
    """

    FAST = "fast"
    STEALTH = "stealth"


class BatchScraperMixin:
    """Fast/stealth batch scraping strategies for :class:`WebFetcher`.

    Mixed in as ``WebFetcher(BatchScraperMixin)``; relies on the host class
    providing ``fetch``, ``is_blocked`` and ``_page_status``.
    """

    # Host contract — implementations supplied by WebFetcher.
    fetch: Callable[..., str]
    is_blocked: Callable[[str], bool]
    _page_status: Callable[[Any], int]

    def scrape(
        self,
        urls: list[str],
        callback: Callable[[str, str], None],
        mode: str = ScrapeMode.FAST,
        max_concurrency: int = 1,
    ) -> None:
        """Scrape multiple URLs using the specified mode.

        Args:
            urls: List of URLs to scrape.
            callback: Function to call with (url, html) for each successful fetch.
                User-callback exceptions propagate immediately — they are not
                retried or reported as fetch failures.
            mode: Scraping mode - "fast" or "stealth" (ScrapeMode values).
                Defaults to "fast" (ScrapeMode.FAST).
            max_concurrency: Maximum concurrent requests. Defaults to 1.

        Raises:
            ValueError: If mode is not a supported scrape mode ("fast"/"stealth").
            FetcherError: If scraping encounters fetch failures.
        """
        if not urls:
            return
        if mode == ScrapeMode.FAST:
            logger.info("Batch scrape FAST %d URLs concurrency=%d", len(urls), max_concurrency)
            self._scrape_fast(urls, callback, max_concurrency)
        elif mode == ScrapeMode.STEALTH:
            logger.info("Batch scrape STEALTH %d URLs concurrency=%d", len(urls), max_concurrency)
            self._scrape_stealth(urls, callback, max_concurrency)
        else:
            raise ValueError(f"Unsupported scrape mode: {mode}")

    def _scrape_fast(self, urls: list[str], callback: Callable[[str, str], None], max_concurrency: int) -> None:
        """Scrape URLs using fast mode with thread pool parallelism.

        Args:
            urls: List of URLs to scrape.
            callback: Function to call with (url, html) for each successful fetch.
            max_concurrency: Maximum number of concurrent threads.

        Raises:
            FetcherError: If any fetches fail.
        """
        errors: list[tuple[str, Exception]] = []
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = [pool.submit(self._fetch_one_fast, url, callback) for url in urls]
            for future in futures:
                try:  # nosec PERF203
                    future.result()
                except FetcherError as exc:
                    # Non-FetcherError (user-callback bugs) propagates
                    # immediately instead of being reported as a fetch failure.
                    errors.append((exc.url or "unknown", exc))
        if errors:
            summary = ", ".join(f"{url}: {err}" for url, err in errors[:5])
            raise FetcherError(f"Fast scrape had {len(errors)} failures. Sample: {summary}")

    def _fetch_one_fast(self, url: str, callback: Callable[[str, str], None]) -> None:
        """Fetch a single URL in fast mode with retry logic.

        Args:
            url: The URL to fetch.
            callback: Function to call with (url, html) on success. Callback
                exceptions propagate to the caller, not into the retry loop.

        Raises:
            FetcherError: If fetching fails or remains blocked.
        """
        last_error: Exception | None = None
        for stealthy_headers in (False, True):
            try:
                html = self.fetch(url, stealthy_headers=stealthy_headers)
                if self.is_blocked(html):
                    continue
            except Exception as exc:
                last_error = exc
                continue
            # Callback runs unguarded: user-callback bugs must not be retried
            # or misreported as fetch failures.
            callback(url, html)
            return

        if last_error is not None:
            raise FetcherError(f"Fast scrape failed for {url}: {last_error}", url=url) from last_error

        raise FetcherError(f"Fast scrape remained blocked for {url}", url=url)

    def _scrape_stealth(self, urls: list[str], callback: Callable[[str, str], None], max_concurrency: int) -> None:
        """Scrape URLs using stealth mode with async concurrency.

        Args:
            urls: List of URLs to scrape.
            callback: Function to call with (url, html) for each successful fetch.
            max_concurrency: Maximum number of concurrent async operations.

        Raises:
            FetcherError: If any fetches fail.
        """
        asyncio.run(self._async_stealth_loop(urls, callback, max_concurrency))

    async def _async_stealth_loop(
        self,
        urls: list[str],
        callback: Callable[[str, str], None],
        max_concurrency: int,
    ) -> None:
        """Async worker loop for stealth mode scraping.

        Args:
            urls: List of URLs to scrape.
            callback: Function to call with (url, html) for each successful fetch.
            max_concurrency: Maximum number of concurrent workers.

        Raises:
            FetcherError: If any fetches fail.
        """
        concurrency = max(1, min(max_concurrency, len(urls)))
        queue: asyncio.Queue[str] = asyncio.Queue()
        for url in urls:
            queue.put_nowait(url)

        errors: list[tuple[str, Exception]] = []

        async with AsyncStealthySession(max_pages=concurrency, headless=True, solve_cloudflare=True) as session:

            async def _worker() -> None:
                """Consume the URL queue until empty, fetching each URL through the shared stealth session."""
                while True:
                    try:
                        url = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        await self._fetch_one_stealth(url, session, callback)
                    except FetcherError as exc:
                        errors.append((exc.url or url, exc))

            workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
            # Strict gather: a user-callback exception inside a worker must
            # surface, not be silently collected.
            await asyncio.gather(*workers)

        if errors:
            summary = ", ".join(f"{url}: {err}" for url, err in errors[:5])
            raise FetcherError(f"Stealth scrape had {len(errors)} failures. Sample: {summary}")

    async def _fetch_one_stealth(
        self,
        url: str,
        session: Any,
        callback: Callable[[str, str], None],
    ) -> None:
        """Fetch a single URL in stealth mode with retry logic.

        Args:
            url: The URL to fetch.
            session: The AsyncStealthySession to use.
            callback: Function to call with (url, html) on success. Callback
                exceptions propagate to the worker, not into the retry loop.

        Raises:
            FetcherError: If fetching fails after retries or stays blocked.
        """
        loop = asyncio.get_running_loop()
        for attempt in range(1, 5):
            try:
                page = await session.fetch(url, disable_resources=False, network_idle=True, timeout=90000)
                status = self._page_status(page)
                if status in (429, 503):
                    if attempt < 4:
                        await asyncio.sleep(30 * attempt)
                        continue
                    raise FetcherError(f"Blocked with status {status} on {url} after 4 attempts", url=url)
            except FetcherError:
                raise
            except Exception as e:
                if attempt < 4:
                    await asyncio.sleep(15 * attempt)
                else:
                    raise FetcherError(f"Stealth fetch failed after 4 retries: {e}", url=url) from e
            else:
                # Callback runs unguarded: user-callback bugs must not be retried
                # or misreported as fetch failures.
                await loop.run_in_executor(None, callback, url, page.html_content)
                return
