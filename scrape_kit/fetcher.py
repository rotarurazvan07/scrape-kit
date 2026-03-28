import asyncio
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, Literal, TypeVar

from scrapling.fetchers import (
    AsyncStealthySession,
    DynamicSession,
    Fetcher,
    StealthySession,
)

from .errors import FetcherError
from .logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T", bound="InteractiveSession")


class ScrapeMode:
    """Scraping mode constants."""

    FAST = "fast"  # Simple HTTP with TLS impersonation
    STEALTH = "stealth"  # Headless browser, Cloudflare bypass


class InteractiveSession:
    """Wrapper around Scrapling session to provide persistent page and JS execution."""

    def __init__(self, session: DynamicSession | StealthySession) -> None:
        self.session = session
        self.page = None
        logger.debug("InteractiveSession initialized with %s", type(session).__name__)

    def __enter__(self):
        logger.info("Starting browser session...")
        self.session.start()
        # Create a persistent page that we control
        self.page = self.session.context.new_page()
        logger.debug("Browser page created and session started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.page:
                logger.debug("Closing browser page...")
                self.page.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise FetcherError(f"Cleanup failed: {e}") from e
        logger.info("Closing browser session")
        self.session.close()

    def fetch(self, url: str, timeout: int = 90000, wait_until: str = "load") -> SimpleNamespace:
        if not self.page:
            raise RuntimeError("Session not started. Use 'with WebFetcher.browser(...) as session:'")

        logger.info("Browser fetching: %s (timeout=%dms)", url, timeout)
        self.page.goto(url, wait_until=wait_until, timeout=timeout)
        logger.debug("Waiting 2s for dynamic content / Cloudflare settle...")
        self.page.wait_for_timeout(2000)
        content = self.page.content()
        logger.debug("Fetch complete, content length: %d", len(content))
        return SimpleNamespace(html_content=content)

    def execute_script(self, script: str) -> Any:
        if not self.page:
            raise RuntimeError("Call fetch() first")

        clean_script = script.strip()
        logger.debug("Executing script: %s...", clean_script[:50])
        try:
            if clean_script.startswith("return "):
                return self.page.evaluate(f"() => {{ {clean_script} }}")
            return self.page.evaluate(script)
        except Exception as e:
            logger.error(f"Script Execution Error: {e}")
            raise

    def wait_for_selector(self, selector: str, timeout: int = 30000) -> None:
        if not self.page:
            raise RuntimeError("Call fetch() first")
        self.page.wait_for_selector(selector, timeout=timeout)

    def wait_for_function(self, expression: str, timeout: int = 30000) -> None:
        if not self.page:
            raise RuntimeError("Call fetch() first")
        self.page.wait_for_function(expression, timeout=timeout)

    def click(self, selector: str, timeout: int = 30000) -> None:
        if not self.page:
            raise RuntimeError("Call fetch() first")
        self.page.click(selector, timeout=timeout)

    def wait_for_timeout(self, ms: int) -> None:
        if not self.page:
            raise RuntimeError("Call fetch() first")
        self.page.wait_for_timeout(ms)

    def __getattr__(self, name):
        """Delegate other attributes to the underlying Scrapling session."""
        return getattr(self.session, name)


class WebFetcher:
    """Web Fetching framework wrapped over scrapling.
    Supports both class initiation for state holding, or usage as a configured module.
    """

    def __init__(
        self,
        retry_indicators: list[str] | None = None,
        block_indicators: list[str] | None = None,
    ) -> None:
        """
        Initialization allows passing custom retry and blocking identifiers.
        """
        self.retry_indicators = retry_indicators or []
        self.block_indicators = block_indicators or []

    def fetch(
        self,
        url: str,
        stealthy_headers: bool = False,
        retries: int = 3,
        backoff: float = 5.0,
    ) -> str:
        """Fast HTTP GET with TLS impersonation and stealth headers.
        Retries on any retry indicators match with exponential backoff.
        Escalates to browser session if persistently blocked.
        """
        if retries < 1:
            raise ValueError("retries must be >= 1")

        for attempt in range(1, retries + 1):
            try:
                logger.debug("Fast fetch attempt %d/%d for %s", attempt, retries, url)
                page = Fetcher.get(url, stealthy_headers=stealthy_headers)

                status = getattr(page, "status", getattr(page, "status_code", 200))
                logger.debug("Response status for %s: %d", url, status)

                if status in [403, 429, 503]:
                    if attempt == retries:
                        logger.error("Final attempt failed with status %d for %s", status, url)
                        raise FetcherError(f"Blocked with status {status} on {url} after {retries} attempts")
                    wait = backoff * attempt
                    logger.warning(f"Status {status} on {url} — retrying in {wait:.0f}s (attempt {attempt}/{retries})")
                    time.sleep(wait)
                    continue

                html = page.html_content
                matched = next(
                    (ind for ind in self.retry_indicators if ind.lower() in html.lower()),
                    None,
                )

                if matched:
                    logger.debug("Retry indicator '%s' matched for %s", matched, url)
                    if attempt < retries:
                        wait = backoff * attempt
                        logger.warning(
                            "Retry indicator '%s' on %s — retrying in %.0fs (attempt %d/%d)",
                            matched,
                            url,
                            wait,
                            attempt,
                            retries,
                        )
                        time.sleep(wait)
                        continue
                    else:
                        logger.info("Retries exhausted for %s, escalating to browser...", url)
                        return self._escalate_to_browser(url, matched)

                logger.debug("Successfully fetched content (len=%d) for %s", len(html), url)
                return html

            except Exception as e:
                if isinstance(e, FetcherError):
                    raise
                if attempt < retries:
                    wait = backoff * attempt
                    logger.warning(f"Error on {url}: {e} — retrying in {wait:.0f}s (attempt {attempt}/{retries})")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed after {retries} attempts on {url}: {e}")
                    raise FetcherError(f"Fetch failed after {retries} attempts: {e}") from e

        raise FetcherError(f"Fetch failed for {url} after {retries} attempts")

    def _escalate_to_browser(self, url: str, blocked_by: str) -> str:
        """Uses a real browser to solve more complex challenges (Cloudflare)."""
        logger.info(f"'{blocked_by}' detected on {url} — Escalating to browser for challenge solving...")
        try:
            with self.browser(solve_cloudflare=True, headless=True) as session:
                resp = session.fetch(url, timeout=120000)  # Simple fetch
                if resp and hasattr(resp, "html_content"):
                    logger.info(f"Browser successfully bypassed challenge for {url}")
                    return resp.html_content
        except Exception as browser_e:
            logger.error(f"Browser escalation failed for {url}: {browser_e}")
            raise FetcherError(f"Escalation failed: {browser_e}") from browser_e
        raise FetcherError(f"Escalation returned no content for {url}")

    def is_blocked(self, html: str) -> bool:
        """Helper to detect shadowbans or Cloudflare blocks based on registered identifiers."""
        if not html:
            return True
        return any(indicator.lower() in html.lower() for indicator in self.block_indicators)

    def browser(
        self,
        headless: bool = True,
        solve_cloudflare: bool = False,
        interactive: bool = True,
        **kwargs: Any,
    ) -> InteractiveSession:
        """Get a consistent interactive browser wrapper as a context manager."""
        kwargs.setdefault("disable_resources", not interactive and not solve_cloudflare)
        kwargs.setdefault("network_idle", interactive or solve_cloudflare)
        kwargs.setdefault("wait_until", "load")

        if solve_cloudflare:
            session = StealthySession(headless=headless, solve_cloudflare=True, **kwargs)
        else:
            session = DynamicSession(headless=headless, **kwargs)

        return InteractiveSession(session)

    def scrape(
        self,
        urls: list[str],
        callback: Callable,
        mode: Literal["fast", "stealth"] = ScrapeMode.FAST,
        max_concurrency: int = 1,
    ) -> None:
        """Batch scrape URLs with concurrency."""
        if not urls:
            return

        if mode == ScrapeMode.FAST:
            logger.info("Starting batch scrape (FAST mode) for %d URLs with concurrency %d", len(urls), max_concurrency)
            self._scrape_fast(urls, callback, max_concurrency)
        elif mode == ScrapeMode.STEALTH:
            logger.info("Starting batch scrape (STEALTH mode) for %d URLs with concurrency %d", len(urls), max_concurrency)
            self._scrape_stealth(urls, callback, max_concurrency)
        else:
            raise ValueError(f"Unsupported scrape mode: {mode}")

    def _scrape_fast(self, urls: list[str], callback: Callable, max_concurrency: int) -> None:
        errors: list[tuple[str, Exception]] = []
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = [pool.submit(self._fetch_one_fast, url, callback) for url in urls]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    url = getattr(exc, "url", "unknown")
                    errors.append((url, exc))

        if errors:
            summary = ", ".join(f"{url}: {err}" for url, err in errors[:5])
            raise FetcherError(f"Fast scrape had {len(errors)} failures. Sample: {summary}")

    def _fetch_one_fast(self, url: str, callback: Callable) -> None:
        last_error: Exception | None = None
        for stealthy_headers in (False, True):
            try:
                html = self.fetch(url, stealthy_headers=stealthy_headers)
                if self.is_blocked(html):
                    continue
                callback(url, html)
                return
            except Exception as exc:
                last_error = exc

        if last_error is not None:
            logger.error("[scrape/fast] Error on %s: %s", url, last_error)
            failure = FetcherError(f"Fast scrape failed for {url}: {last_error}")
            failure.url = url
            raise failure from last_error

        failure = FetcherError(f"Fast scrape remained blocked for {url}")
        failure.url = url
        raise failure

    def _scrape_stealth(self, urls: list[str], callback: Callable, max_concurrency: int) -> None:
        asyncio.run(self._async_stealth_loop(urls, callback, max_concurrency))

    async def _async_stealth_loop(self, urls: list[str], callback: Callable, max_concurrency: int) -> None:
        async with AsyncStealthySession(max_pages=max_concurrency, headless=True, solve_cloudflare=True) as session:
            sem = asyncio.Semaphore(max_concurrency)
            results = await asyncio.gather(
                *[self._fetch_one_stealth(url, session, sem, callback) for url in urls],
                return_exceptions=True,
            )

        errors: list[tuple[str, Exception]] = []
        for url, result in zip(urls, results, strict=False):
            if isinstance(result, Exception):
                errors.append((url, result))
        if errors:
            summary = ", ".join(f"{url}: {err}" for url, err in errors[:5])
            raise FetcherError(f"Stealth scrape had {len(errors)} failures. Sample: {summary}")

    async def _fetch_one_stealth(self, url: str, session: Any, sem: asyncio.Semaphore, callback: Callable) -> None:
        async with sem:
            for attempt in range(1, 5):
                try:
                    page = await session.fetch(url, disable_resources=False, network_idle=True, timeout=90000)
                    status = getattr(page, "status", getattr(page, "status_code", 200))

                    if status in [429, 503]:
                        if attempt < 4:
                            wait = 30 * attempt
                            logger.warning(f"Status {status} on {url} (attempt {attempt}/4) — retrying in {wait}s...")
                            await asyncio.sleep(wait)
                            continue
                        else:
                            raise FetcherError(f"Blocked with status {status} on {url} after 4 attempts")

                    callback(url, page.html_content)
                    return
                except Exception as e:
                    if attempt < 4:
                        wait = 15 * attempt
                        logger.warning(
                            f"Stealth fetch error/timeout on {url} (attempt {attempt}/4) — retrying in {wait}s: {e}"
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.error(f"Failed persistently on {url}: {e}")
                        raise FetcherError(f"Stealth fetch failed after 4 retries: {e}") from e
