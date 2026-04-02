"""
Scrape-Kit: A flexible, high-performance scraping framework.

Quick-start (module-level static usage):

    # One-time setup — load indicators from your project's config dir
    from scrape_kit import configure
    configure("path/to/config")          # reads scraper_config.yaml

    # Or use built-in defaults with no config file needed
    from scrape_kit import configure_defaults
    configure_defaults()

    # Then use module-level proxies anywhere
    from scrape_kit import fetch, browser, scrape, is_blocked
    html = fetch("https://example.com")

Instance usage (explicit config per fetcher):

    from scrape_kit import WebFetcher
    fetcher = WebFetcher(retry_indicators=[...], block_indicators=[...])
    html = fetcher.fetch("https://example.com")

Legacy static-class pattern (still works):

    from scrape_kit.fetcher import fetch, browser, scrape, is_blocked
    # Use directly — no instantiation needed after configure()
"""

from .errors import FetcherError, ScrapeKitError, SettingsError, StorageError
from .fetcher import (  # Module-level proxies — use these after configure() or configure_defaults()
    InteractiveSession,
    ScrapeMode,
    WebFetcher,
    browser,
    fetch,
    is_blocked,
    scrape,
)
from .logger import get_logger, time_profiler
from .matching import SimilarityEngine
from .settings import SettingsManager
from .storage import BaseStorageManager, BufferedStorageManager


def configure(config_path: str, config_key: str = "scraper_config") -> WebFetcher:
    """Load scraper indicators from a YAML config directory and set the shared instance.

    Reads ``<config_path>/<config_key>.yaml`` (via SettingsManager) and
    configures the module-level shared WebFetcher used by ``fetch()``,
    ``scrape()``, ``browser()``, and ``is_blocked()``.

    Returns the configured WebFetcher instance in case you need it directly.
    """
    return WebFetcher.configure(config_path, config_key=config_key, set_shared=True)


def configure_defaults() -> WebFetcher:
    """Set the shared instance using the built-in default indicator lists.

    Use this when you don't have a config file but still want the full set
    of common retry/block indicators.
    """
    return WebFetcher.configure_defaults(set_shared=True)


__all__ = [
    # Settings
    "SettingsManager",
    # Storage
    "BaseStorageManager",
    "BufferedStorageManager",
    # Fetcher — class
    "WebFetcher",
    "InteractiveSession",
    "ScrapeMode",
    # Fetcher — module-level proxies (use after configure())
    "fetch",
    "is_blocked",
    "browser",
    "scrape",
    # Configure helpers
    "configure",
    "configure_defaults",
    # Matching
    "SimilarityEngine",
    # Errors
    "ScrapeKitError",
    "FetcherError",
    "StorageError",
    "SettingsError",
    # Logging
    "get_logger",
    "time_profiler",
]
