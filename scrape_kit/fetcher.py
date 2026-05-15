import asyncio
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Literal

from scrapling.fetchers import (
    AsyncStealthySession,
    DynamicSession,
    Fetcher,
    StealthySession,
)

from .errors import FetcherError
from .logger import get_logger
from .page import Page
from .session import InteractiveSession

logger = get_logger(__name__)

# ── Module-level shared instance ──────────────────────────────────────────────
_shared: "WebFetcher | None" = None


def _get_shared() -> "WebFetcher":
    """Return the shared instance, creating a zero-config one if not yet configured."""
    global _shared
    if _shared is None:
        _shared = WebFetcher()
    return _shared


# ── Public module-level proxies ───────────────────────────────────────────────


def configure(config_path: str, config_key: str = "scraper_config") -> None:
    """Populate the shared WebFetcher instance from YAML config."""
    WebFetcher.configure(config_path, config_key, set_shared=True)


def configure_defaults() -> None:
    """Populate the shared WebFetcher instance with defaults."""
    WebFetcher.configure_defaults(set_shared=True)


def fetch(url: str, **kwargs: Any) -> Page:
    """Fetch a single URL and return a Page object."""
    return _get_shared().fetch(url, **kwargs)


def is_blocked(page: Page) -> bool:
    """Check if the given Page indicates we are blocked."""
    return _get_shared().is_blocked(page)


def browser(**kwargs: Any) -> InteractiveSession:
    """Open an InteractiveSession context manager."""
    return _get_shared().browser(**kwargs)


def scrape(urls: list[str], callback: Callable[[str, Page], None], **kwargs: Any) -> None:
    """Batch scrape URLs, passing Page objects to the callback."""
    return _get_shared().scrape(urls, callback, **kwargs)


class ScrapeMode:
    FAST = "fast"
    STEALTH = "stealth"


class WebFetcher:
    """Web fetching framework wrapping scrapling."""

    _DEFAULT_RETRY: list[str] = [
        "403 Forbidden",
        "Access Denied",
        "429 Too Many Requests",
        "Too Many Requests",
        "rate limit exceeded",
        "rate limited",
        "Request throttled",
        "Service Unavailable",
        "503 Service Unavailable",
        "Temporarily Unavailable",
        "overloaded",
        "quota exceeded",
        "Just a moment",
        "Checking your browser",
        "verify you are a human",
    ]
    _DEFAULT_BLOCK: list[str] = [
        "Just a moment...",
        "cf-browser-verification",
        "Access Denied",
        "Checking your browser",
        "verify you are a human",
        "403 Forbidden",
        "429 Too Many Requests",
        "Attention Required!",
    ]

    def __init__(
        self,
        retry_indicators: list[str] | None = None,
        block_indicators: list[str] | None = None,
    ) -> None:
        self.retry_indicators = retry_indicators if retry_indicators is not None else []
        self.block_indicators = block_indicators if block_indicators is not None else []

    @classmethod
    def configure(
        cls,
        config_path: str,
        config_key: str = "scraper_config",
        *,
        set_shared: bool = True,
    ) -> None:
        global _shared
        from .settings import SettingsManager

        sm = SettingsManager(config_path)
        cfg: dict[str, Any] = sm.get(config_key) or {}

        retry = cfg.get("retry_indicators", cls._DEFAULT_RETRY)
        block = cfg.get("block_indicators", cls._DEFAULT_BLOCK)

        instance = cls(retry_indicators=retry, block_indicators=block)
        logger.info("WebFetcher configured from '%s'", config_path)

        if set_shared:
            _shared = instance

    @classmethod
    def configure_defaults(cls, *, set_shared: bool = True) -> None:
        global _shared
        instance = cls(retry_indicators=cls._DEFAULT_RETRY, block_indicators=cls._DEFAULT_BLOCK)
        if set_shared:
            _shared = instance
        logger.info("WebFetcher configured with defaults")

    def fetch(
        self,
        url: str,
        stealthy_headers: bool = False,
        retries: int = 3,
        backoff: float = 5.0,
    ) -> Page:
        """Fetch a URL with retry logic and automatic escalation."""
        if retries < 1:
            raise ValueError("retries must be >= 1")

        for attempt in range(1, retries + 1):
            try:
                page = self._fetch_attempt(url, stealthy_headers, attempt, retries, backoff)
                if page is not None:
                    return page
            except FetcherError:
                raise
            except Exception as e:
                self._handle_fetch_error(url, e, attempt, retries, backoff)

        raise FetcherError(f"Fetch failed for {url} after {retries} attempts")

    def _fetch_attempt(
        self,
        url: str,
        stealthy_headers: bool,
        attempt: int,
        retries: int,
        backoff: float,
    ) -> Page | None:
        logger.debug("Fast fetch attempt %d/%d for %s", attempt, retries, url)
        resp = Fetcher.get(url, stealthy_headers=stealthy_headers)

        status = getattr(resp, "status", getattr(resp, "status_code", 200))
        if self._is_blocked_status(status, attempt, retries, url, backoff):
            return None

        html = resp.html_content
        page = Page.from_html(html)

        matched = self._check_retry_indicators(page.raw_html, url, attempt, retries, backoff)
        if matched is not None:
            if attempt >= retries:
                logger.info("Retries exhausted for %s, escalating to browser...", url)
                return self._escalate_to_browser(url, matched)
            return None

        return page

    def _is_blocked_status(self, status: int, attempt: int, retries: int, url: str, backoff: float) -> bool:
        if status not in [403, 429, 503]:
            return False
        if attempt >= retries:
            raise FetcherError(f"Blocked with status {status} on {url} after {retries} attempts")
        wait = backoff * attempt
        logger.warning("Status %d on %s — retrying in %.0fs", status, url, wait)
        time.sleep(wait)
        return True

    def _check_retry_indicators(self, html: str, url: str, attempt: int, retries: int, backoff: float) -> str | None:
        matched = next((ind for ind in self.retry_indicators if ind.lower() in html.lower()), None)
        if matched and attempt < retries:
            wait = backoff * attempt
            logger.warning("Retry indicator '%s' on %s — retrying in %.0fs", matched, url, wait)
            time.sleep(wait)
        return matched

    def _handle_fetch_error(self, url: str, error: Exception, attempt: int, retries: int, backoff: float) -> None:
        if attempt < retries:
            wait = backoff * attempt
            logger.warning("Error on %s: %s — retrying in %.0fs", url, error, wait)
            time.sleep(wait)
        else:
            raise FetcherError(f"Fetch failed after {retries} attempts: {error}") from error

    def _escalate_to_browser(self, url: str, blocked_by: str) -> Page:
        logger.info("'%s' detected — escalating to browser for %s", blocked_by, url)
        try:
            with self.browser(solve_cloudflare=True, headless=True) as session:
                return session.fetch(url)
        except Exception as e:
            logger.error("Browser escalation failed for %s: %s", url, e)
            raise FetcherError(f"Escalation failed: {e}") from e

    def is_blocked(self, page: Page) -> bool:
        """Check if the Page content indicates blocking."""
        html = page.raw_html
        # Empty HTML is always considered blocked
        if not html or not html.strip():
            return True
        html_lower = html.lower()
        return any(indicator.lower() in html_lower for indicator in self.block_indicators)

    def browser(self, headless: bool = True, solve_cloudflare: bool = False, **kwargs: Any) -> InteractiveSession:
        """Create an InteractiveSession."""
        is_heavy = solve_cloudflare
        kwargs.setdefault("disable_resources", not is_heavy)
        kwargs.setdefault("network_idle", is_heavy)
        kwargs.setdefault("wait_until", "load")

        low_mem_flags = ["--disable-dev-shm-usage", "--disable-gpu", "--no-sandbox", "--disable-setuid-sandbox"]
        kwargs["args"] = list(set(kwargs.get("args", [])) | set(low_mem_flags))

        if solve_cloudflare:
            session = StealthySession(headless=headless, solve_cloudflare=True, **kwargs)
        else:
            session = DynamicSession(headless=headless, **kwargs)

        return InteractiveSession(session)

    def scrape(
        self,
        urls: list[str],
        callback: Callable[[str, Page], None],
        mode: str = ScrapeMode.FAST,
        max_concurrency: int = 1,
    ) -> None:
        if not urls:
            return
        if mode == ScrapeMode.FAST:
            self._scrape_fast(urls, callback, max_concurrency)
        elif mode == ScrapeMode.STEALTH:
            self._scrape_stealth(urls, callback, max_concurrency)
        else:
            raise ValueError(f"Unsupported scrape mode: {mode}")

    def _scrape_fast(self, urls: list[str], callback: Callable[[str, Page], None], max_concurrency: int) -> None:
        errors: list[tuple[str, Exception]] = []
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = [pool.submit(self._fetch_one_fast, url, callback) for url in urls]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    errors.append(("unknown", exc))
        if errors:
            raise FetcherError(f"Fast scrape had {len(errors)} failures.")

    def _fetch_one_fast(self, url: str, callback: Callable[[str, Page], None]) -> None:
        for stealthy_headers in (False, True):
            try:
                page = self.fetch(url, stealthy_headers=stealthy_headers)
                if self.is_blocked(page):
                    continue
                callback(url, page)
                return
            except Exception:
                continue
        raise FetcherError(f"Fast scrape failed for {url}")

    def _scrape_stealth(self, urls: list[str], callback: Callable[[str, Page], None], max_concurrency: int) -> None:
        asyncio.run(self._async_stealth_loop(urls, callback, max_concurrency))

    async def _async_stealth_loop(self, urls: list[str], callback: Callable[[str, Page], None], max_concurrency: int) -> None:
        concurrency = max(1, min(max_concurrency, len(urls)))
        queue: asyncio.Queue[str] = asyncio.Queue()
        for url in urls: queue.put_nowait(url)

        async with AsyncStealthySession(max_pages=concurrency, headless=True, solve_cloudflare=True) as session:
            async def _worker():
                while not queue.empty():
                    url = await queue.get()
                    try:
                        resp = await session.get(url)
                        callback(url, Page.from_html(resp.html_content))
                    finally:
                        queue.task_done()
            
            await asyncio.gather(*[_worker() for _ in range(concurrency)])
