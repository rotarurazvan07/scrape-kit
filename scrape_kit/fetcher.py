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

# ── Module-level shared instance ──────────────────────────────────────────────
# Populated by WebFetcher.configure() or set directly.
# Module-level proxy functions below delegate to this instance.

_shared: "WebFetcher | None" = None


def _get_shared() -> "WebFetcher":
    """Return the shared instance, creating a zero-config one if not yet configured."""
    global _shared
    if _shared is None:
        _shared = WebFetcher()
    return _shared


# ── Public module-level proxies ───────────────────────────────────────────────
# These allow `from scrape_kit.fetcher import fetch` usage without instantiation.


def fetch(url: str, **kwargs: Any) -> str:
    """Module-level proxy — delegates to the shared WebFetcher instance."""
    return _get_shared().fetch(url, **kwargs)


def is_blocked(html: str) -> bool:
    """Module-level proxy — delegates to the shared WebFetcher instance."""
    return _get_shared().is_blocked(html)


def browser(**kwargs: Any) -> "InteractiveSession":
    """Module-level proxy — delegates to the shared WebFetcher instance."""
    return _get_shared().browser(**kwargs)


def scrape(urls: list[str], callback: Callable[[str, str], None], **kwargs: Any) -> None:
    """Module-level proxy — delegates to the shared WebFetcher instance."""
    return _get_shared().scrape(urls, callback, **kwargs)


class ScrapeMode:
    """Scraping mode constants."""

    FAST = "fast"
    STEALTH = "stealth"


class InteractiveSession:
    """Wrapper around Scrapling session to provide persistent page and JS execution."""

    def __init__(self, session: DynamicSession | StealthySession) -> None:
        """Initialize an interactive session with a Scrapling session.

        Args:
            session: A Scrapling DynamicSession or StealthySession instance.
        """
        self.session = session
        self.page = None
        logger.debug("InteractiveSession initialized with %s", type(session).__name__)

    def __enter__(self) -> "InteractiveSession":
        """Start the browser session and create a new page.

        Returns:
            The InteractiveSession instance for use in context manager.
        """
        logger.info("Starting browser session...")
        self.session.start()
        self.page = self.session.context.new_page()
        logger.debug("Browser page created and session started")
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Cleanly close the browser session and page.

        Args:
            _exc_type: Exception type if an exception occurred.
            _exc_val: Exception value if an exception occurred.
            _exc_tb: Traceback if an exception occurred.
        """
        try:
            if self.page:
                logger.debug("Closing browser page...")
                self.page.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise FetcherError(f"Cleanup failed: {e}") from e
        logger.info("Closing browser session")
        # close() method exists on both DynamicSession and StealthySession
        self.session.close()  # type: ignore[no-untyped-call]

    def fetch(self, url: str, timeout: int = 90000, wait_until: str = "load") -> SimpleNamespace:
        """Navigate to a URL and retrieve the page content.

        Args:
            url: The URL to fetch.
            timeout: Timeout in milliseconds. Defaults to 90000.
            wait_until: Page load state to wait for. Defaults to "load".

        Returns:
            SimpleNamespace with html_content attribute containing the page HTML.

        Raises:
            RuntimeError: If the session has not been started.
        """
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
        """Execute JavaScript in the page context.

        Args:
            script: JavaScript code to execute. If it starts with "return ", the
                    expression is wrapped in an arrow function and its result is returned.

        Returns:
            The result of the script execution, if any.

        Raises:
            RuntimeError: If fetch() has not been called first.
        """
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

    def wait_for_selector(self, selector: str, timeout: int = 30000, **kwargs: Any) -> None:
        """Wait for a DOM element to appear.

        Args:
            selector: CSS selector to wait for.
            timeout: Maximum wait time in milliseconds. Defaults to 30000.
            **kwargs: Additional arguments passed to Playwright's wait_for_selector.

        Raises:
            RuntimeError: If fetch() has not been called first.
        """
        if not self.page:
            raise RuntimeError("Call fetch() first")
        self.page.wait_for_selector(selector, timeout=timeout, **kwargs)

    def wait_for_function(self, expression: str, timeout: int = 30000, **kwargs: Any) -> None:
        """Wait for a JavaScript function to return a truthy value.

        Args:
            expression: JavaScript function or expression to evaluate.
            timeout: Maximum wait time in milliseconds. Defaults to 30000.
            **kwargs: Additional arguments passed to Playwright's wait_for_function.

        Raises:
            RuntimeError: If fetch() has not been called first.
        """
        if not self.page:
            raise RuntimeError("Call fetch() first")
        self.page.wait_for_function(expression, timeout=timeout, **kwargs)

    def click(self, selector: str, timeout: int = 30000, **kwargs: Any) -> None:
        """Click a DOM element.

        Args:
            selector: CSS selector for the element to click.
            timeout: Maximum wait time in milliseconds. Defaults to 30000.
            **kwargs: Additional arguments passed to Playwright's click.

        Raises:
            RuntimeError: If fetch() has not been called first.
        """
        if not self.page:
            raise RuntimeError("Call fetch() first")
        self.page.click(selector, timeout=timeout, **kwargs)

    def wait_for_timeout(self, ms: int, **kwargs: Any) -> None:
        """Pause execution for a specified duration.

        Args:
            ms: Time to wait in milliseconds.
            **kwargs: Additional arguments passed to Playwright's wait_for_timeout.

        Raises:
            RuntimeError: If fetch() has not been called first.
        """
        if not self.page:
            raise RuntimeError("Call fetch() first")
        self.page.wait_for_timeout(ms, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.session, name)


class WebFetcher:
    """Web fetching framework wrapping scrapling.

    Usage — instance (explicit config):
        fetcher = WebFetcher(retry_indicators=[...], block_indicators=[...])

    Usage — YAML config (recommended for projects):
        WebFetcher.configure("path/to/config")   # reads scraper_config.yaml
        html = fetch(url)                         # module-level proxy

    Usage — static class (legacy pattern, still works):
        class WebScraper:
            fetch = staticmethod(WebFetcher.configure(...).fetch)
    """

    # ── Default indicators — override via configure() or __init__ ─────────────
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
        """Initialize a WebFetcher with custom retry and block indicators.

        Args:
            retry_indicators: List of strings that indicate a retry is needed.
            block_indicators: List of strings that indicate the request is blocked.
        """
        self.retry_indicators = retry_indicators if retry_indicators is not None else []
        self.block_indicators = block_indicators if block_indicators is not None else []

    # ── Class-level factory: load from YAML ───────────────────────────────────

    @classmethod
    def configure(
        cls,
        config_path: str,
        config_key: str = "scraper_config",
        *,
        set_shared: bool = True,
    ) -> "WebFetcher":
        """Load retry/block indicators from a YAML file and (optionally) set the
        module-level shared instance so module-proxy functions use it.

        The YAML file should look like:
            retry_indicators:
              - "Just a moment"
              - "403 Forbidden"
            block_indicators:
              - "cf-browser-verification"
              - "Access Denied"

        Args:
            config_path:  Path to config directory (or file) passed to SettingsManager.
            config_key:   YAML key / filename stem to look up.  Defaults to
                          "scraper_config", so it reads ``scraper_config.yaml``.
            set_shared:   If True (default), store the new instance as the module-level
                          shared instance used by the proxy functions.

        Returns:
            The newly constructed WebFetcher instance.
        """
        global _shared
        # Import here to avoid circular imports at module load time
        from .settings import SettingsManager

        sm = SettingsManager(config_path)
        cfg: dict[str, Any] = sm.get(config_key) or {}

        retry = cfg.get("retry_indicators", cls._DEFAULT_RETRY)
        block = cfg.get("block_indicators", cls._DEFAULT_BLOCK)

        instance = cls(retry_indicators=retry, block_indicators=block)
        logger.info(
            "WebFetcher configured from '%s' (%d retry / %d block indicators)",
            config_path,
            len(retry),
            len(block),
        )

        if set_shared:
            _shared = instance

        return instance

    @classmethod
    def configure_defaults(cls, *, set_shared: bool = True) -> "WebFetcher":
        """Create an instance using the built-in default indicator lists.
        Useful when you want the full default indicator set without a config file.
        """
        global _shared
        instance = cls(retry_indicators=cls._DEFAULT_RETRY, block_indicators=cls._DEFAULT_BLOCK)
        if set_shared:
            _shared = instance
        logger.info(
            "WebFetcher configured with defaults (%d retry / %d block indicators)",
            len(cls._DEFAULT_RETRY),
            len(cls._DEFAULT_BLOCK),
        )
        return instance

    # ── Instance methods ──────────────────────────────────────────────────────

    def fetch(
        self,
        url: str,
        stealthy_headers: bool = False,
        retries: int = 3,
        backoff: float = 5.0,
    ) -> str:
        """Fetch a URL with retry logic and automatic escalation.

        Args:
            url: The URL to fetch.
            stealthy_headers: Whether to use stealthy headers. Defaults to False.
            retries: Number of retry attempts. Defaults to 3.
            backoff: Backoff multiplier in seconds. Defaults to 5.0.

        Returns:
            The HTML content as a string.

        Raises:
            ValueError: If retries is less than 1.
            FetcherError: If fetching fails after all retries.
        """
        if retries < 1:
            raise ValueError("retries must be >= 1")

        for attempt in range(1, retries + 1):
            try:  # nosec PERF203
                html = self._fetch_attempt(url, stealthy_headers, attempt, retries, backoff)
                if html is not None:
                    return html
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
    ) -> str | None:
        """Execute a single fetch attempt. Returns HTML on success, None to retry."""
        logger.debug("Fast fetch attempt %d/%d for %s", attempt, retries, url)
        page = Fetcher.get(url, stealthy_headers=stealthy_headers)

        status = getattr(page, "status", getattr(page, "status_code", 200))
        logger.debug("Response status for %s: %d", url, status)

        if self._is_blocked_status(status, attempt, retries, url, backoff):
            return None

        html = page.html_content
        matched = self._check_retry_indicators(html, url, attempt, retries, backoff)
        if matched is not None:
            if attempt >= retries:
                logger.info("Retries exhausted for %s, escalating to browser...", url)
                return self._escalate_to_browser(url, matched)
            return None

        logger.debug("Successfully fetched content (len=%d) for %s", len(html), url)
        return html

    def _is_blocked_status(
        self,
        status: int,
        attempt: int,
        retries: int,
        url: str,
        backoff: float,
    ) -> bool:
        """Check if status code indicates blocking. Returns True if should retry."""
        if status not in [403, 429, 503]:
            return False
        if attempt >= retries:
            raise FetcherError(f"Blocked with status {status} on {url} after {retries} attempts")
        wait = backoff * attempt
        logger.warning("Status %d on %s — retrying in %.0fs (attempt %d/%d)", status, url, wait, attempt, retries)
        time.sleep(wait)
        return True

    def _check_retry_indicators(
        self,
        html: str,
        url: str,
        attempt: int,
        retries: int,
        backoff: float,
    ) -> str | None:
        """Check retry indicators. Returns matched indicator or None."""
        matched = next(
            (ind for ind in self.retry_indicators if ind.lower() in html.lower()),
            None,
        )
        if matched and attempt < retries:
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
        return matched

    def _handle_fetch_error(
        self,
        url: str,
        error: Exception,
        attempt: int,
        retries: int,
        backoff: float,
    ) -> None:
        """Handle fetch error, retrying if attempts remain."""
        if attempt < retries:
            wait = backoff * attempt
            logger.warning("Error on %s: %s — retrying in %.0fs (attempt %d/%d)", url, error, wait, attempt, retries)
            time.sleep(wait)
        else:
            logger.error("Failed after %d attempts on %s: %s", retries, url, error)
            raise FetcherError(f"Fetch failed after {retries} attempts: {error}") from error

    def _escalate_to_browser(self, url: str, blocked_by: str) -> str:
        """Escalate to a browser session to bypass blocking.

        Args:
            url: The URL that was blocked.
            blocked_by: The indicator that triggered the block.

        Returns:
            The HTML content fetched via browser.

        Raises:
            FetcherError: If browser escalation fails.
        """
        logger.info("'%s' detected on %s — escalating to browser...", blocked_by, url)
        try:
            with self.browser(solve_cloudflare=True, headless=True) as session:
                resp = session.fetch(url, timeout=120000)
                if resp and hasattr(resp, "html_content"):
                    logger.info("Browser successfully bypassed challenge for %s", url)
                    return str(resp.html_content)
        except Exception as browser_e:
            logger.error("Browser escalation failed for %s: %s", url, browser_e)
            raise FetcherError(f"Escalation failed: {browser_e}") from browser_e
        raise FetcherError(f"Escalation returned no content for {url}")

    def is_blocked(self, html: str) -> bool:
        """Check if the HTML content indicates blocking.

        Args:
            html: The HTML content to check.

        Returns:
            True if blocking indicators are found, False otherwise.
        """
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
        """Create an interactive browser session.

        Args:
            headless: Whether to run in headless mode. Defaults to True.
            solve_cloudflare: Enable Cloudflare challenge solving. Defaults to False.
            interactive: Enable interactive features. Defaults to True.
            **kwargs: Additional arguments passed to the Scrapling session.

        Returns:
            An InteractiveSession instance.
        """
        is_heavy = interactive or solve_cloudflare
        defaults = {
            "disable_resources": not is_heavy,
            "network_idle": is_heavy,
            "wait_until": "load",
        }
        for key, value in defaults.items():
            kwargs.setdefault(key, value)

        low_mem_flags = {"--disable-dev-shm-usage", "--disable-gpu", "--no-sandbox", "--disable-setuid-sandbox"}
        kwargs["args"] = list(set(kwargs.get("args", [])) | low_mem_flags)

        if solve_cloudflare:
            session = StealthySession(headless=headless, solve_cloudflare=True, **kwargs)
        else:
            session: DynamicSession | StealthySession = DynamicSession(headless=headless, **kwargs)

        return InteractiveSession(session)

    def scrape(
        self,
        urls: list[str],
        callback: Callable[[str, str], None],
        mode: Literal["fast", "stealth"] = ScrapeMode.FAST,
        max_concurrency: int = 1,
    ) -> None:
        """Scrape multiple URLs using the specified mode.

        Args:
            urls: List of URLs to scrape.
            callback: Function to call with (url, html) for each successful fetch.
            mode: Scraping mode - "fast" or "stealth". Defaults to ScrapeMode.FAST.
            max_concurrency: Maximum concurrent requests. Defaults to 1.

        Raises:
            FetcherError: If scraping encounters failures.
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
                except Exception as exc:
                    errors.append((getattr(exc, "url", "unknown"), exc))
        if errors:
            summary = ", ".join(f"{url}: {err}" for url, err in errors[:5])
            raise FetcherError(f"Fast scrape had {len(errors)} failures. Sample: {summary}")

    def _fetch_one_fast(self, url: str, callback: Callable[[str, str], None]) -> None:
        """Fetch a single URL in fast mode with retry logic.

        Args:
            url: The URL to fetch.
            callback: Function to call with (url, html) on success.

        Raises:
            FetcherError: If fetching fails or remains blocked.
        """
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
            failure = FetcherError(f"Fast scrape failed for {url}: {last_error}")
            failure.url = url  # type: ignore[attr-defined]
            raise failure from last_error

        failure = FetcherError(f"Fast scrape remained blocked for {url}")
        failure.url = url  # type: ignore[attr-defined]
        raise failure

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

    async def _async_stealth_loop(self, urls: list[str], callback: Callable[[str, str], None], max_concurrency: int) -> None:
        """Async worker loop for stealth mode scraping.

        Args:
            urls: List of URLs to scrape.
            callback: Function to call with (url, html) for each successful fetch.
            max_concurrency: Maximum number of concurrent workers.
        """
        concurrency = max(1, min(max_concurrency, len(urls)))
        queue: asyncio.Queue[str] = asyncio.Queue()
        for url in urls:
            queue.put_nowait(url)

        errors: list[tuple[str, Exception]] = []

        async with AsyncStealthySession(max_pages=concurrency, headless=True, solve_cloudflare=True) as session:

            async def _worker() -> None:
                while True:
                    try:
                        url = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return

                    try:
                        await self._fetch_one_stealth(url, session, callback)
                    except Exception as exc:
                        errors.append((url, exc))
                    finally:
                        queue.task_done()

            workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
            await queue.join()
            await asyncio.gather(*workers, return_exceptions=True)

        if errors:
            summary = ", ".join(f"{url}: {err}" for url, err in errors[:5])
            raise FetcherError(f"Stealth scrape had {len(errors)} failures. Sample: {summary}")

    async def _fetch_one_stealth(self, url: str, session: Any, callback: Callable[[str, str], None]) -> None:
        """Fetch a single URL in stealth mode with retry logic.

        Args:
            url: The URL to fetch.
            session: The AsyncStealthySession to use.
            callback: Function to call with (url, html) on success.

        Raises:
            FetcherError: If fetching fails after retries.
        """
        loop = asyncio.get_running_loop()
        for attempt in range(1, 5):
            try:
                page = await session.fetch(url, disable_resources=False, network_idle=True, timeout=90000)
                status = getattr(page, "status", getattr(page, "status_code", 200))
                if status in [429, 503]:
                    if attempt < 4:
                        await asyncio.sleep(30 * attempt)
                        continue
                    raise FetcherError(f"Blocked with status {status} on {url} after 4 attempts")
                await loop.run_in_executor(None, callback, url, page.html_content)
                return
            except Exception as e:
                if attempt < 4:
                    await asyncio.sleep(15 * attempt)
                else:
                    raise FetcherError(f"Stealth fetch failed after 4 retries: {e}") from e
