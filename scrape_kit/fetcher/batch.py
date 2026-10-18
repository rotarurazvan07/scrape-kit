"""Batch scraping strategies (fast thread pool / stealth async) and the ScrapeMode enum."""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any, TypeAlias

from scrapling.fetchers import AsyncStealthySession

from ..errors import FetcherError
from ..logger import get_logger
from .session import _looks_like_cloudflare

logger = get_logger(__name__)


class _UserCodeError(Exception):
    """Carrier for exceptions raised by the retry hook or the user callback.

    Not a ``FetcherError`` subclass: the FAST and STEALTH collectors only
    aggregate ``FetcherError``, so this type propagates immediately out of
    the batch dispatch instead of being reported as a fetch failure or
    retried. The original exception stays reachable via ``__cause__``.
    """

    def __init__(self, message: str, *, url: str) -> None:
        """Initialize the carrier with the originating URL.

        Args:
            message: Human-readable description of the user-code failure.
            url: URL whose hook or callback raised the original exception.
        """
        super().__init__(message)
        self.url = url


RetryHook: TypeAlias = Callable[[str, str], "str | None"]


class ScrapeMode(str, Enum):
    """Batch scrape modes.

    A str-Enum: members are their plain string values, so raw strings
    ("fast"/"stealth") and ScrapeMode members are interchangeable wherever
    a mode is expected.
    """

    FAST = "fast"
    STEALTH = "stealth"


def _pending_for_next_pass(pending: list[str], flagged: list[str], delivered: list[str]) -> list[str]:
    """Rebuild the next pass URL list after a mid-retry fetch failure.

    Keeps per-entry identity: duplicate URLs each keep their own retry, and a
    failed duplicate is retried instead of being dropped by URL-string sets.

    Args:
        pending: URLs dispatched on the failed pass.
        flagged: URLs the hook rejected on that pass.
        delivered: URLs delivered on that pass.

    Returns:
        Flagged URLs followed by the still-unfinished pending entries.
    """
    unfinished = list(pending)
    for url in delivered + flagged:
        if url in unfinished:
            unfinished.remove(url)
    return flagged + unfinished


class BatchScraperMixin:
    """Fast/stealth batch scraping strategies for :class:`WebFetcher`.

    Mixed in as ``WebFetcher(BatchScraperMixin)``; relies on the host class
    providing ``fetch``, ``is_blocked`` and ``_page_status``.
    """

    # Host contract — implementations supplied by WebFetcher.
    fetch: Callable[..., str]
    is_blocked: Callable[[str], bool]
    _page_status: Callable[[Any], int]
    retry_indicators: list[str]
    block_indicators: list[str]

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
        """Scrape multiple URLs using the specified mode.

        Args:
            urls: List of URLs to scrape.
            callback: Function to call with (url, html) for each successful fetch.
                User-callback exceptions propagate immediately — they are not
                retried or reported as fetch failures.
            mode: Scraping mode - "fast" or "stealth" (ScrapeMode values).
                Defaults to "fast" (ScrapeMode.FAST).
            max_concurrency: Maximum concurrent requests. Defaults to 1.
            retry_hook: Optional quality gate consulted after each successful,
                non-blocked fetch. Returning a non-empty reason string defers
                that URL to another full retry pass; None (or any falsy value)
                accepts the page. Hook exceptions propagate immediately.
                Ignored when max_retries is 0.
            max_retries: Additional full retry passes for hook-flagged URLs.
                Defaults to 0 (no hook retries). Ignored when retry_hook is None.

        STEALTH uses ``asyncio.run`` and cannot be called from a running event loop.

        Raises:
            ValueError: If mode is unsupported, ``max_concurrency`` is less than 1,
                or ``max_retries`` is less than 0.
            RuntimeError: If STEALTH is requested from a running event loop.
            FetcherError: If scraping encounters fetch failures.
        """
        # AC-01/AC-02: no hook or no retry budget keeps the legacy path intact.
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if retry_hook is None or max_retries <= 0:
            self._dispatch_scrape(urls, callback, mode, max_concurrency)
            return
        self._scrape_with_retries(urls, callback, mode, max_concurrency, retry_hook, max_retries)

    def _dispatch_scrape(
        self,
        urls: list[str],
        callback: Callable[[str, str], None],
        mode: str,
        max_concurrency: int,
    ) -> None:
        """Run one full scrape pass over ``urls`` in the requested mode.

        Args:
            urls: List of URLs to scrape in this pass.
            callback: Function to call with (url, html) for each successful fetch.
            mode: Scraping mode - "fast" or "stealth" (ScrapeMode values).
            max_concurrency: Maximum concurrent requests.

        Raises:
            ValueError: If mode is unsupported or ``max_concurrency`` is less than 1.
            RuntimeError: If STEALTH is requested from a running event loop.
            FetcherError: If scraping encounters fetch failures.
        """
        if not urls:
            return
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if mode == ScrapeMode.FAST:
            logger.info("Batch scrape FAST %d URLs concurrency=%d", len(urls), max_concurrency)
            self._scrape_fast(urls, callback, max_concurrency)
        elif mode == ScrapeMode.STEALTH:
            logger.info("Batch scrape STEALTH %d URLs concurrency=%d", len(urls), max_concurrency)
            self._scrape_stealth(urls, callback, max_concurrency)
        else:
            raise ValueError(f"Unsupported scrape mode: {mode}")

    def _scrape_with_retries(
        self,
        urls: list[str],
        callback: Callable[[str, str], None],
        mode: str,
        max_concurrency: int,
        retry_hook: RetryHook,
        max_retries: int,
    ) -> None:
        """Re-dispatch hook-flagged URLs through the same mode until retries run out.

        Every pass re-invokes ``_dispatch_scrape``, so each existing retry layer
        (inner fetch retries, fast second chance, stealth 4-attempt loop, browser
        escalation) still applies. The hook is consulted only for successful,
        non-blocked pages, before the user callback.

        Args:
            urls: URLs to scrape on the first pass.
            callback: Function to call with (url, html) for each accepted page.
            mode: Scraping mode preserved across every pass.
            max_concurrency: Concurrency preserved across every pass.
            retry_hook: Quality gate consulted before each callback.
            max_retries: Additional passes for hook-flagged URLs.

        Raises:
            ValueError: If mode is unsupported or ``max_concurrency`` is less than 1.
            RuntimeError: If STEALTH is requested from a running event loop.
            FetcherError: If the final pass encounters fetch failures.
            Exception: Hook and user-callback exceptions propagate immediately;
                a hook or callback raising ``FetcherError`` surfaces as a
                ``_UserCodeError`` chained from the original instead of being
                retried or reported as a fetch failure.
        """
        pending = list(urls)
        for attempt in range(1, max_retries + 2):
            final = attempt > max_retries
            flagged: list[str] = []
            delivered: list[str] = []

            def gated(
                url: str,
                html: str,
                *,
                final: bool = attempt > max_retries,
                hook: RetryHook = retry_hook,
                user_callback: Callable[[str, str], None] = callback,
                rejected: list[str] = flagged,
                accepted: list[str] = delivered,
            ) -> None:
                """Consult the hook, then either defer the URL or deliver the page."""
                try:
                    # CR: user-code FetcherError must escape the fetch-failure
                    # collectors (they aggregate FetcherError only) and this
                    # retry loop (it catches FetcherError only), so it is
                    # re-raised as a non-FetcherError carrier.
                    reason = hook(url, html)
                    if reason and not final:
                        # AC-04: a reject defers the URL and skips this callback.
                        rejected.append(url)
                        return
                    if reason:
                        # AC-08: the last pass still consults the hook but delivers anyway.
                        logger.warning(
                            "Retry hook rejected %s on the final pass (%s) — delivering content anyway",
                            url,
                            reason,
                        )
                    accepted.append(url)
                    user_callback(url, html)
                except FetcherError as exc:
                    raise _UserCodeError(f"Hook or callback failed for {url}: {exc}", url=url) from exc

            try:
                self._dispatch_scrape(pending, gated, mode, max_concurrency)
            except FetcherError:
                # AC-07: a non-final failure retries every still-pending URL.
                if final:
                    raise
                # CR: per-entry bookkeeping, not URL-string sets.
                pending = _pending_for_next_pass(pending, flagged, delivered)
                continue
            # AC-05/AC-08: nothing flagged, or the last pass finished — stop.
            if final or not flagged:
                return
            pending = flagged

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
                # Deliberate broad catch: any per-URL failure (transport
                # error or FetcherError from retry exhaustion) is retried
                # once with stealthy headers; breadth is the boundary
                # contract. Only final exhaustion converts to FetcherError.
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

        # AC-28: solve CF only when configured indicators look CF-like.
        indicators = " ".join(self.retry_indicators + self.block_indicators)
        solve_cf = _looks_like_cloudflare(indicators)
        async with AsyncStealthySession(max_pages=concurrency, headless=True, solve_cloudflare=solve_cf) as session:

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
                html = page.html_content
                if status in (429, 503) or self.is_blocked(html):
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
                await loop.run_in_executor(None, callback, url, html)
                return
