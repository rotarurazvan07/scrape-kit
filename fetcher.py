import asyncio
import logging
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, TypeVar

from scrapling.fetchers import (
    AsyncStealthySession,
    DynamicSession,
    Fetcher,
    StealthySession,
)

from errors import FetcherError

# Configure structured logging
logger = logging.getLogger("scrape_kit.fetcher")

T = TypeVar("T", bound="InteractiveSession")


class ScrapeMode:
    """Scraping mode constants."""

    FAST = "fast"  # Simple HTTP with TLS impersonation
    STEALTH = "stealth"  # Headless browser, Cloudflare bypass


class InteractiveSession:
    """Wrapper around Scrapling session to provide persistent page and JS execution."""

    def __init__(self, session: DynamicSession | StealthySession):
        self.session = session
        self.page = None

    def __enter__(self):
        self.session.start()
        # Create a persistent page that we control
        self.page = self.session.context.new_page()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.page:
                self.page.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise FetcherError(f"Cleanup failed: {e}") from e
        self.session.close()

    def fetch(self, url: str, timeout: int = 90000, wait_until: str = "load") -> SimpleNamespace:
        if not self.page:
            raise RuntimeError("Session not started. Use 'with WebFetcher.browser(...) as session:'")

        self.page.goto(url, wait_until=wait_until, timeout=timeout)
        self.page.wait_for_timeout(2000)
        return SimpleNamespace(html_content=self.page.content())

    def execute_script(self, script: str) -> Any:
        if not self.page:
            raise RuntimeError("Call fetch() first")

        clean_script = script.strip()
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

    def __init__(self, retry_indicators: list[str] = None, block_indicators: list[str] = None):
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
        for attempt in range(1, retries + 1):
            try:
                page = Fetcher.get(url, stealthy_headers=stealthy_headers)

                status = getattr(page, "status", getattr(page, "status_code", 200))
                if status in [403, 429, 503]:
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
                    if attempt < retries:
                        wait = backoff * attempt
                        print(
                            f"[fetch] '{matched}' indicator on {url} — retrying in {wait:.0f}s (attempt {attempt}/{retries})",
                            file=sys.stderr,
                        )
                        time.sleep(wait)
                        continue
                    else:
                        return self._escalate_to_browser(url, matched)

                return html

            except Exception as e:
                if attempt < retries:
                    wait = backoff * attempt
                    logger.warning(f"Error on {url}: {e} — retrying in {wait:.0f}s (attempt {attempt}/{retries})")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed after {retries} attempts on {url}: {e}")
                    raise FetcherError(f"Fetch failed after {retries} attempts: {e}") from e

        return ""

    def _escalate_to_browser(self, url: str, blocked_by: str) -> str:
        """Uses a real browser to solve more complex challenges (Cloudflare)."""
        logger.info(f"'{blocked_by}' detected on {url} — Escalating to browser for challenge solving...")
        try:
            with self.browser(solve_cloudflare=True, headless=True, interactive=True) as session:
                resp = session.fetch(url, timeout=120000)  # Simple fetch
                if resp and hasattr(resp, "html_content"):
                    logger.info(f"Browser successfully bypassed challenge for {url}")
                    return resp.html_content
        except Exception as browser_e:
            logger.error(f"Browser escalation failed for {url}: {browser_e}")
            raise FetcherError(f"Escalation failed: {browser_e}") from browser_e
        return ""

    def is_blocked(self, html: str) -> bool:
        """Helper to detect shadowbans or Cloudflare blocks based on registered identifiers."""
        if not html:
            return True
        return any(indicator.lower() in html.lower() for indicator in self.block_indicators)

    def browser(
        self,
        headless: bool = True,
        solve_cloudflare: bool = False,
        interactive: bool = False,
        **kwargs: Any,
    ) -> DynamicSession | StealthySession | InteractiveSession:
        """Get a browser session as a context manager."""
        kwargs.setdefault("disable_resources", not interactive and not solve_cloudflare)
        kwargs.setdefault("network_idle", interactive or solve_cloudflare)
        kwargs.setdefault("wait_until", "load")

        if solve_cloudflare:
            session = StealthySession(headless=headless, solve_cloudflare=True, **kwargs)
        else:
            session = DynamicSession(headless=headless, **kwargs)

        return InteractiveSession(session) if interactive else session

    def scrape(
        self,
        urls: list[str],
        callback: Callable,
        mode: str = ScrapeMode.FAST,
        max_concurrency: int = 1,
    ):
        """Batch scrape URLs with concurrency."""
        if not urls:
            return

        if mode == ScrapeMode.FAST:
            self._scrape_fast(urls, callback, max_concurrency)
        elif mode == ScrapeMode.STEALTH:
            self._scrape_stealth(urls, callback, max_concurrency)

    def _scrape_fast(self, urls: list[str], callback: Callable, max_concurrency: int):
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            # Using partial or wrapping to pass state
            pool.map(lambda u: self._fetch_worker_fast(u, callback), urls)

    def _fetch_worker_fast(self, url: str, callback: Callable):
        try:
            html = self.fetch(url, stealthy_headers=False)
            if not self.is_blocked(html):
                callback(url, html)
                return
            html = self.fetch(url, stealthy_headers=True)
            if not self.is_blocked(html):
                callback(url, html)
                return
        except Exception as e:
            logger.error(f"[scrape/fast] Error on {url}: {e}")

    def _scrape_stealth(self, urls: list[str], callback: Callable, max_concurrency: int):
        asyncio.run(self._async_stealth_loop(urls, callback, max_concurrency))

    async def _async_stealth_loop(self, urls: list[str], callback: Callable, max_concurrency: int):
        async with AsyncStealthySession(max_pages=max_concurrency, headless=True, solve_cloudflare=True) as session:
            sem = asyncio.Semaphore(max_concurrency)
            await asyncio.gather(*[self._fetch_one_stealth(url, session, sem, callback) for url in urls])

    async def _fetch_one_stealth(self, url: str, session: Any, sem: asyncio.Semaphore, callback: Callable):
        async with sem:
            for attempt in range(1, 4):
                try:
                    page = await session.fetch(url, disable_resources=False, network_idle=True, timeout=90000)
                    callback(url, page.html_content)
                    return
                except Exception as e:
                    if attempt < 3:
                        wait = 15 * attempt
                        logger.warning(f"Challenge/Timeout on {url} (attempt {attempt}) — retrying in {wait}s...")
                        await asyncio.sleep(wait)
                    else:
                        logger.error(f"Failed persistently on {url}: {e}")
                        raise FetcherError(f"Stealth fetch failed after retries: {e}") from e
