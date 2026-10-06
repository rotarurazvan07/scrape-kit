"""WebFetcher: fast HTTP fetching with retry, block detection, and stealth-browser escalation."""

import time
from typing import Any, ClassVar

from scrapling.fetchers import DynamicSession, Fetcher, StealthySession

from ..errors import FetcherError
from ..logger import get_logger
from ..settings import SettingsManager
from ._state import _set_shared
from .batch import BatchScraperMixin
from .session import InteractiveSession

logger = get_logger(__name__)


class WebFetcher(BatchScraperMixin):
    """Anti-detection web fetching framework wrapping scrapling.

    Two equivalent usage tiers:

    Shared instance — configure once, then use the module-level proxies
    (fetch/browser/scrape/is_blocked) with no per-call wiring:

        WebFetcher.configure("path/to/config")   # or configure_defaults()
        html = fetch("https://example.com")       # module-level proxy

    Instance — explicit per-fetcher configuration, for when several
    fetchers with different indicator sets must coexist:

        fetcher = WebFetcher(retry_indicators=[...], block_indicators=[...])
        html = fetcher.fetch("https://example.com")

    Bound-method composition also works (e.g. staticmethod wiring in user
    classes), since configure() returns the configured instance.
    """

    # ── Default indicators — override via configure() or __init__ ─────────────
    _DEFAULT_RETRY: ClassVar[tuple[str, ...]] = (
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
    )
    _DEFAULT_BLOCK: ClassVar[tuple[str, ...]] = (
        "Just a moment...",
        "cf-browser-verification",
        "Access Denied",
        "Checking your browser",
        "verify you are a human",
        "403 Forbidden",
        "429 Too Many Requests",
        "Attention Required!",
    )

    def __init__(
        self,
        retry_indicators: list[str] | None = None,
        block_indicators: list[str] | None = None,
    ) -> None:
        """Initialize a WebFetcher with custom retry and block indicators.

        Args:
            retry_indicators: Strings that indicate a retry is needed. Defaults to empty.
            block_indicators: Strings that indicate the request is blocked. Defaults to empty.
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
            config_path: Path to config directory (or file) passed to SettingsManager.
            config_key: YAML key / filename stem to look up. Defaults to
                "scraper_config", so it reads ``scraper_config.yaml``.
            set_shared: If True (default), store the new instance as the module-level
                shared instance used by the proxy functions.

        Returns:
            The newly constructed WebFetcher instance.

        A missing directory or YAML key uses the class-level default lists.

        Raises:
            SettingsError: If a YAML file is present but malformed.
        """
        sm = SettingsManager(config_path)
        cfg: dict[str, Any] = sm.get(config_key) or {}

        retry = cfg.get("retry_indicators")
        block = cfg.get("block_indicators")
        if retry is None:
            retry = list(cls._DEFAULT_RETRY)  # copy: never alias the class-level list
        if block is None:
            block = list(cls._DEFAULT_BLOCK)

        instance = cls(retry_indicators=retry, block_indicators=block)
        logger.info(
            "WebFetcher configured from '%s' (%d retry / %d block indicators)",
            config_path,
            len(retry),
            len(block),
        )

        if set_shared:
            _set_shared(instance)
        return instance

    @classmethod
    def configure_defaults(cls, *, set_shared: bool = True) -> "WebFetcher":
        """Create an instance using the built-in default indicator lists.

        Useful when you want the full default indicator set without a config file.

        Args:
            set_shared: If True (default), store the new instance as the module-level
                shared instance used by the proxy functions.

        Returns:
            The newly constructed WebFetcher instance.
        """
        instance = cls(
            retry_indicators=list(cls._DEFAULT_RETRY),  # copy: never alias the class-level list
            block_indicators=list(cls._DEFAULT_BLOCK),
        )
        if set_shared:
            _set_shared(instance)
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
                # Deliberate broad catch: this is the retry boundary — any
                # attempt failure (transport error, scrapling runtime error,
                # parser error) is classified by _handle_fetch_error, which
                # retries or converts to FetcherError. FetcherError re-raises
                # above; breadth is the boundary contract.
                self._handle_fetch_error(url, e, attempt, retries, backoff)

        raise FetcherError(f"Fetch failed for {url} after {retries} attempts", url=url)

    def _fetch_attempt(
        self,
        url: str,
        stealthy_headers: bool,
        attempt: int,
        retries: int,
        backoff: float,
    ) -> str | None:
        """Execute a single fetch attempt. Returns HTML on success, None to retry.

        Args:
            url: The URL to fetch.
            stealthy_headers: Whether to use stealthy headers.
            attempt: Current attempt number (1-based).
            retries: Total retry attempts.
            backoff: Backoff multiplier in seconds.

        Returns:
            The HTML content, or None when the attempt should be retried.

        Raises:
            FetcherError: When blocking persists to the final attempt.
        """
        logger.debug("Fast fetch attempt %d/%d for %s", attempt, retries, url)
        page = Fetcher.get(url, stealthy_headers=stealthy_headers)

        status = self._page_status(page)
        logger.debug("Response status for %s: %d", url, status)

        if self._is_blocked_status(status):
            if attempt >= retries:
                raise FetcherError(f"Blocked with status {status} on {url} after {retries} attempts", url=url)
            self._log_and_wait(f"Status {status} on {url}", attempt, retries, backoff)
            return None

        html: str = page.html_content
        matched = self._check_retry_indicators(html)
        if matched is not None:
            if attempt >= retries:
                logger.info("Retries exhausted for %s, escalating to browser...", url)
                return self._escalate_to_browser(url, matched)
            self._log_and_wait(f"Retry indicator '{matched}' on {url}", attempt, retries, backoff)
            return None

        logger.debug("Successfully fetched content (len=%d) for %s", len(html), url)
        return html

    def _page_status(self, page: Any) -> int:
        """Return a scrapling page's HTTP status, preferring status over status_code.

        Args:
            page: A scrapling response/page object (or a mock in tests).

        Returns:
            The HTTP status code, defaulting to 200 when the page exposes neither attribute.
        """
        status: Any = getattr(page, "status", getattr(page, "status_code", 200))
        return int(status)

    def _is_blocked_status(self, status: int) -> bool:
        """Check whether a status code indicates blocking.

        Args:
            status: The HTTP status code.

        Returns:
            True if the status is one of the blocked statuses (403, 429, 503).
        """
        return status in (403, 429, 503)

    def _check_retry_indicators(self, html: str) -> str | None:
        """Check retry indicators. Returns the matched indicator or None.

        Args:
            html: The HTML content to check.

        Returns:
            The first retry indicator found in the HTML (case-insensitive), or None.
        """
        return next(
            (ind for ind in self.retry_indicators if ind.lower() in html.lower()),
            None,
        )

    def _log_and_wait(self, reason: str, attempt: int, retries: int, backoff: float) -> None:
        """Log a retry reason and sleep for the backoff interval.

        Args:
            reason: Human-readable reason for the retry (already formatted).
            attempt: Current attempt number (1-based).
            retries: Total retry attempts.
            backoff: Backoff multiplier in seconds.
        """
        wait = backoff * attempt
        logger.warning("%s — retrying in %.0fs (attempt %d/%d)", reason, wait, attempt, retries)
        time.sleep(wait)

    def _handle_fetch_error(
        self,
        url: str,
        error: Exception,
        attempt: int,
        retries: int,
        backoff: float,
    ) -> None:
        """Handle a fetch error, retrying if attempts remain.

        Args:
            url: The URL being fetched.
            error: The exception raised by the attempt.
            attempt: Current attempt number (1-based).
            retries: Total retry attempts.
            backoff: Backoff multiplier in seconds.

        Raises:
            FetcherError: If no attempts remain.
        """
        if attempt < retries:
            wait = backoff * attempt
            logger.warning("Error on %s: %s — retrying in %.0fs (attempt %d/%d)", url, error, wait, attempt, retries)
            time.sleep(wait)
        else:
            logger.error("Failed after %d attempts on %s: %s", retries, url, error)
            raise FetcherError(f"Fetch failed after {retries} attempts: {error}", url=url) from error

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
                html = session.fetch(url, timeout=120000)
                logger.info("Browser successfully bypassed challenge for %s", url)
                return html
        except Exception as browser_e:
            logger.error("Browser escalation failed for %s: %s", url, browser_e)
            raise FetcherError(f"Escalation failed: {browser_e}", url=url) from browser_e

    def is_blocked(self, html: str) -> bool:
        """Check if the HTML content indicates blocking.

        Args:
            html: The HTML content to check.

        Returns:
            True when html is empty/None (treated as blocked) or a blocking
            indicator is found; False otherwise.
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

        session: DynamicSession | StealthySession
        if solve_cloudflare:
            session = StealthySession(headless=headless, solve_cloudflare=True, **kwargs)
        else:
            session = DynamicSession(headless=headless, **kwargs)

        return InteractiveSession(session)
