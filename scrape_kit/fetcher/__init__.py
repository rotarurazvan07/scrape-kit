"""Anti-detection web fetching around scrapling: shared-instance helpers, InteractiveSession, WebFetcher."""

from collections.abc import Callable
from typing import Any

from . import _state
from ._state import reset_shared
from .batch import ScrapeMode
from .session import InteractiveSession
from .web_fetcher import WebFetcher


# Shared-instance state lives in the ._state leaf (no import cycles, no `global`).


def _get_shared() -> WebFetcher:
    """Return the shared instance, creating a default-config one if not yet configured.

    The auto-created instance uses the built-in default indicator lists
    (configure_defaults() semantics), so the zero-config path behaves
    identically to an explicit configure_defaults() call.

    Returns:
        The shared WebFetcher instance.
    """
    shared = _state._peek_shared()
    if shared is None:
        # configure_defaults() also stores the instance (set_shared=True),
        # so the auto-created fetcher becomes the shared one — one call
        # does both, exactly like the old module-global assignment.
        return WebFetcher.configure_defaults(set_shared=True)
    return shared


# ── Public module-level proxies ───────────────────────────────────────────────
# These allow `from scrape_kit.fetcher import fetch` usage without instantiation
# after configure() / configure_defaults() has set the shared instance.


def fetch(url: str, stealthy_headers: bool = False, retries: int = 3, backoff: float = 5.0) -> str:
    """Fetch a URL via the shared WebFetcher instance.

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
    return _get_shared().fetch(url, stealthy_headers=stealthy_headers, retries=retries, backoff=backoff)


def is_blocked(html: str) -> bool:
    """Check if the HTML content indicates blocking, via the shared WebFetcher instance.

    Args:
        html: The HTML content to check.

    Returns:
        True when html is empty/None (treated as blocked) or a blocking
        indicator is found; False otherwise.
    """
    return _get_shared().is_blocked(html)


def browser(
    headless: bool = True,
    solve_cloudflare: bool = False,
    interactive: bool = True,
    **kwargs: Any,
) -> InteractiveSession:
    """Open an interactive browser session via the shared WebFetcher instance.

    Args:
        headless: Whether to run in headless mode. Defaults to True.
        solve_cloudflare: Enable Cloudflare challenge solving. Defaults to False.
        interactive: Enable interactive features. Defaults to True.
        **kwargs: Additional arguments passed to the Scrapling session.

    Returns:
        An InteractiveSession instance.
    """
    return _get_shared().browser(headless=headless, solve_cloudflare=solve_cloudflare, interactive=interactive, **kwargs)


def scrape(
    urls: list[str],
    callback: Callable[[str, str], None],
    mode: str = ScrapeMode.FAST,
    max_concurrency: int = 1,
) -> None:
    """Scrape multiple URLs via the shared WebFetcher instance.

    Args:
        urls: List of URLs to scrape.
        callback: Function to call with (url, html) for each successful fetch.
        mode: Scraping mode - "fast" or "stealth" (ScrapeMode values). Defaults to "fast".
        max_concurrency: Maximum concurrent requests. Defaults to 1.

    STEALTH uses ``asyncio.run`` and cannot be called from a running event loop.

    Raises:
        ValueError: If mode is unsupported or ``max_concurrency`` is less than 1.
        RuntimeError: If STEALTH is requested from a running event loop.
        FetcherError: If scraping encounters fetch failures.
    """
    _get_shared().scrape(urls, callback, mode=mode, max_concurrency=max_concurrency)


__all__ = [
    "WebFetcher",
    "InteractiveSession",
    "ScrapeMode",
    "fetch",
    "is_blocked",
    "browser",
    "scrape",
    "reset_shared",
]
