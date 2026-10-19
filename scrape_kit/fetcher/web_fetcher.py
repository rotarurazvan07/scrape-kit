"""WebFetcher: fast HTTP fetching with retry, block detection, and stealth-browser escalation."""

import random
import threading
import time
from typing import Any, ClassVar, Final

from scrapling.fetchers import DynamicSession, Fetcher, StealthySession

from ..errors import FetcherError, SettingsError
from ..logger import get_logger
from ..settings import SettingsManager
from ._state import _set_shared
from .batch import BatchScraperMixin
from .session import InteractiveSession, _retry_after_seconds

logger = get_logger(__name__)

_DEFAULT_FAST_TIMEOUT = 30
# Per-status retry budgets. None means "inherit the call-time retries argument",
# which reproduces the legacy (403, 429, 503) behavior exactly.
_DEFAULT_STATUS_RETRIES: Final[dict[int, int | None]] = {403: None, 429: None, 503: None}
# Legacy escalation gate: browser statuses never delivered as a successful bypass.
_DEFAULT_ESCALATION_BLOCKED: Final[frozenset[int]] = frozenset({429, 503})


def _parse_status_retry_entry(key: Any, value: Any) -> tuple[int, int | None]:
    """Validate one ``status_retries`` entry and return it typed.

    Satisfies: AC-12 — a malformed key or budget is rejected at configure()
    time, consistent with the YAML-is-broken contract.

    Args:
        key: The raw status key (an int or a numeric string).
        value: The raw budget (a non-negative int, or None to inherit).

    Returns:
        The integer status code and its validated budget.

    Raises:
        SettingsError: If the key is not an integer status code, or the value
            is not None, a non-negative int, or a bool.
    """
    if isinstance(key, bool) or not isinstance(key, (int, str)):
        raise SettingsError(f"status_retries key {key!r} is not an HTTP status code")
    try:
        status = int(key)
    except ValueError as e:
        raise SettingsError(f"status_retries key {key!r} is not an HTTP status code") from e
    # AC-12: only real HTTP status codes are accepted, so a typo'd or absurd key
    # cannot become a retryable pseudo-status that changes fetch behavior.
    if not 100 <= status <= 599:
        raise SettingsError(f"status_retries key {key!r} is not an HTTP status code")
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
        raise SettingsError(f"status_retries[{status}] must be a non-negative integer or null")
    return status, value


def _parse_status_retries(raw: Any) -> dict[int, int | None]:
    """Validate a raw ``status_retries`` mapping and return it keyed by int.

    Satisfies: AC-12 — malformed configuration is rejected at configure() time,
    consistent with the YAML-is-broken contract.

    Args:
        raw: The raw YAML value for the ``status_retries`` key.

    Returns:
        A mapping of integer status code to a non-negative int budget or None.

    Raises:
        SettingsError: If the value is not a mapping, a key is not an integer
            status, or a budget is not None, a non-negative int, or a boolean.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise SettingsError("status_retries must be a mapping of HTTP status to retry count")
    return dict(_parse_status_retry_entry(key, value) for key, value in raw.items())


def _merge_status_retries(parsed: dict[int, int | None]) -> dict[int, int | None]:
    """Merge a validated status map over the class defaults.

    Satisfies: AC-02 — the knob is additive: unlisted default codes keep their
    default entry, so configuring one code never removes another's behavior.

    Args:
        parsed: A validated mapping from :func:`_parse_status_retries`.

    Returns:
        A fresh mapping merged over :data:`_DEFAULT_STATUS_RETRIES`.
    """
    merged = dict(_DEFAULT_STATUS_RETRIES)
    merged.update(parsed)
    return merged


class _RateLimit:
    """Private record of one observed 429, consumed by the batch layer.

    Attributes:
        url: The URL that was rate limited.
        retry_after: The bounded delay advertised by the response, if any.
        observed_at: Monotonic timestamp of the observation, used by the batch
            layer to ignore observations that predate the current batch.
    """

    def __init__(self, url: str, retry_after: float | None, observed_at: float) -> None:
        """Store which URL was rate limited, its delay, and when it was seen.

        Args:
            url: The URL that received the 429 response.
            retry_after: The bounded Retry-After delay, or None when absent.
            observed_at: Monotonic time at which the 429 was observed.
        """
        self.url = url
        self.retry_after = retry_after
        self.observed_at = observed_at


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
        *,
        status_retries: dict[int, int | None] | None = None,
    ) -> None:
        """Initialize a WebFetcher with custom retry and block indicators.

        Args:
            retry_indicators: Strings that indicate a retry is needed. Defaults to empty.
            block_indicators: Strings that indicate the request is blocked. Defaults to empty.
            status_retries: Per-HTTP-status retry budgets. None means "use the
                call-time ``retries`` argument"; a non-negative int is the number
                of extra attempts for that status; 0 opts the status out of
                retrying. Merged over the class defaults, so unlisted codes keep
                their default entry. Defaults to None (the legacy behavior).
        """
        self.retry_indicators = retry_indicators if retry_indicators is not None else []
        self.block_indicators = block_indicators if block_indicators is not None else []
        # Per-status retry budgets (AC-01/02/03/04): never alias the class default.
        parsed: dict[int, int | None] = _parse_status_retries(status_retries)
        self.status_retries = _merge_status_retries(parsed)
        # AC-02/AC-07: the codes the consumer explicitly configured, kept apart
        # from the merged map so a default entry is never mistaken for intent.
        self._configured_status_retries = parsed
        # Private status observation slot: the fast path cannot see the response
        # object (fetch returns HTML), so the attempt loop records the signal
        # here for the batch cooldown layer to consume (AC-09).
        self._rate_limit_signal: _RateLimit | None = None
        self._rate_limit_lock = threading.Lock()

    def _is_retryable_status(self, status: int) -> bool:
        """Return whether a status code is retried instead of delivered.

        Satisfies: AC-01/AC-04/AC-05/AC-06 — with no configuration the gate is
        exactly the legacy (403, 429, 503) set; an explicit 0 removes a code,
        an explicit positive budget adds one, and an unlisted non-default code
        is never gated.

        Args:
            status: The HTTP status code.

        Returns:
            True when the status is retried, False when its body is delivered.
        """
        entry = self.status_retries.get(status)
        if entry is None:
            # AC-01: a None entry is retryable only when it is a default member.
            return status in _DEFAULT_STATUS_RETRIES
        # AC-04/AC-05: an explicit budget decides membership outright.
        return entry > 0

    def _status_budget(self, status: int, retries: int) -> int:
        """Return the total attempt budget for a status on the fast path.

        Satisfies: AC-01/AC-03 — a configured budget overrides the call-time
        ``retries`` argument for that status; a None entry and an unlisted code
        inherit it unchanged.

        Args:
            status: The HTTP status code being retried.
            retries: The call-time total-attempts argument.

        Returns:
            The total number of attempts allowed for this status.
        """
        entry = self.status_retries.get(status)
        # AC-03: a configured count is a number of retries beyond the first
        # attempt, so the budget is one attempt higher.
        return retries if entry is None else entry + 1

    def _attempt_limit(self, retries: int) -> int:
        """Return the outer attempt bound that admits every configured budget.

        Satisfies: AC-03 — a configured budget can exceed the call-time
        ``retries`` argument, so the loop must be able to reach it. Transport
        errors and content-indicator retries keep raising at ``retries``
        unchanged, so an unconfigured fetcher behaves exactly as before.

        Args:
            retries: The call-time total-attempts argument.

        Returns:
            The maximum number of attempts the loop may run.
        """
        budgets = [entry + 1 for entry in self.status_retries.values() if entry is not None]
        return max([retries, *budgets])

    def _escalation_blocked(self, status: int) -> bool:
        """Return whether a browser status must never be delivered as success.

        Satisfies: AC-09/AC-10 — the gate is exactly (429, 503) when nothing is
        configured, gains every code with an explicit positive budget, and loses
        any code whose budget is explicitly 0. A listed-but-None code keeps its
        default membership, so naming a code cannot un-gate it.

        Args:
            status: The HTTP status code of the browser navigation.

        Returns:
            True when the body must be discarded and FetcherError raised.
        """
        entry = self.status_retries.get(status)
        if entry is None:
            return status in _DEFAULT_ESCALATION_BLOCKED
        return entry > 0

    def _note_rate_limit(self, url: str, retry_after: float | None) -> None:
        """Record a 429 observation for the batch cooldown layer.

        Satisfies: AC-09 (FAST workers learn about a 429 through this slot).

        Args:
            url: The URL that received the 429 response.
            retry_after: The bounded advertised delay, or None when absent.
        """
        with self._rate_limit_lock:
            # AC-13: stamp the observation so a batch can ignore signals that
            # predate it (a single-URL fetch() leaves the slot populated).
            self._rate_limit_signal = _RateLimit(url, retry_after, time.monotonic())

    def _take_rate_limit_signal(self) -> _RateLimit | None:
        """Consume and clear the recorded 429 observation, if any.

        Returns:
            The recorded :class:`_RateLimit`, or None when no 429 was observed.
        """
        with self._rate_limit_lock:
            signal = self._rate_limit_signal
            self._rate_limit_signal = None
            return signal

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
        # AC-12: a malformed map is rejected here, like any other broken YAML.
        status_retries = _parse_status_retries(cfg.get("status_retries"))

        instance = cls(retry_indicators=retry, block_indicators=block, status_retries=status_retries)
        logger.info(
            "WebFetcher configured from '%s' (%d retry / %d block indicators)",
            config_path,
            len(retry),
            len(block),
        )
        # AC-11: the map adds exactly one line; the two above stay unchanged.
        if "status_retries" in cfg:
            logger.info("Configured status retries for %d statuses", len(status_retries))

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

        # AC-03: the loop bound admits a configured per-status budget; transport
        # and indicator retries still raise at the call-time ``retries``.
        limit = self._attempt_limit(retries)
        for attempt in range(1, limit + 1):
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
        timeout = self._fast_timeout(attempt)
        page = Fetcher.get(url, stealthy_headers=stealthy_headers, timeout=timeout)

        status = self._page_status(page)
        logger.debug("Response status for %s: %d", url, status)

        if self._is_retryable_status(status):
            # AC-03/AC-07: a configured budget overrides the call-time retries for
            # this status; Retry-After and the batch cooldown observation apply to
            # every status that will actually be retried (429, or configured).
            budget = self._status_budget(status, retries)
            # AC-02/AC-07: the observation set is legacy 429 plus every code the
            # consumer explicitly configured, so an absent key never makes 403 or
            # 503 read Retry-After or start the batch cooldown.
            observes = status == 429 or status in self._configured_status_retries
            retry_after = _retry_after_seconds(page) if observes else None
            if observes:
                self._note_rate_limit(url, retry_after)
            if attempt >= budget:
                raise FetcherError(self._blocked_status_message(url, status, budget), url=url)
            self._log_and_wait(f"Status {status} on {url}", attempt, budget, backoff, wait=retry_after)
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

    @staticmethod
    def _blocked_status_message(url: str, status: int, attempts: int | None = None) -> str:
        """Return the exhaustion message for a blocked status.

        Satisfies: AC-07 — 429 exhaustion keeps the legacy wording and adds a
        rate-limiting label; every other status is byte-identical to before.

        Args:
            url: The URL that stayed blocked.
            status: The final blocked status code.
            attempts: The number of attempts that were made, or None to omit
                the attempt clause (used by the single-navigation browser path,
                where "after 1 attempts" would be wrong).

        Returns:
            The FetcherError message text.
        """
        base = f"Blocked with status {status} on {url}"
        if attempts is not None:
            base = f"{base} after {attempts} attempts"
        if status == 429:
            return f"{base} (rate limited)"
        return base

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

    @staticmethod
    def _fast_timeout(attempt: int) -> int:
        """Return the curl timeout for this attempt. Later attempts wait longer (AC-30)."""
        return _DEFAULT_FAST_TIMEOUT * attempt

    @staticmethod
    def _retry_wait(attempt: int, backoff: float) -> float:
        """Jittered backoff: ``backoff * attempt * U(0.5, 1.5)`` (AC-30)."""
        return backoff * attempt * random.uniform(0.5, 1.5)

    def _log_and_wait(
        self,
        reason: str,
        attempt: int,
        retries: int,
        backoff: float,
        wait: float | None = None,
    ) -> None:
        """Log a retry reason and sleep for the wait interval.

        Satisfies: AC-04/AC-06 — a server-advertised ``wait`` (Retry-After,
        already capped) replaces the jittered backoff for that attempt; every
        other case keeps the existing :meth:`_retry_wait` jitter. One wait
        implementation, no second sleep helper.

        Args:
            reason: Human-readable reason for the retry (already formatted).
            attempt: Current attempt number (1-based).
            retries: Total retry attempts.
            backoff: Backoff multiplier in seconds.
            wait: Explicit delay to sleep, or None to use the jittered backoff.
        """
        delay = self._retry_wait(attempt, backoff) if wait is None else wait
        logger.warning("%s — retrying in %.0fs (attempt %d/%d)", reason, delay, attempt, retries)
        time.sleep(delay)

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
            wait = self._retry_wait(attempt, backoff)
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
        html = ""
        try:
            with self.browser(solve_cloudflare=True, headless=True) as session:
                html = session.fetch(url, timeout=120000)
                # AC-01/AC-02: the browser path must not lose the HTTP status.
                status = session._last_response_status()
                retry_after = session._last_retry_after()
        except Exception as browser_e:
            logger.error("Browser escalation failed for %s: %s", url, browser_e)
            raise FetcherError(f"Escalation failed: {browser_e}", url=url) from browser_e
        # AC-09/AC-10: the gate is (429, 503) by default and follows the
        # configured status_retries map when one is set. A status of None
        # (no usable navigation response) is delivered exactly as before.
        if status is not None and self._escalation_blocked(status):
            hinted = status == 429 or status in self._configured_status_retries
            hint = f" Retry-After: {retry_after:.0f}s" if retry_after and hinted else ""
            logger.warning("Browser hit status %d on %s.%s", status, url, hint)
            # AC-02: single-navigation browser path omits the attempt clause.
            raise FetcherError(self._blocked_status_message(url, status), url=url)
        logger.info("Browser successfully bypassed challenge for %s", url)
        return html

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
