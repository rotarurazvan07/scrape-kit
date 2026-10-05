"""
Scrape-Kit: A flexible, high-performance scraping framework.

Heavy subsystems (fetcher, matching, storage) load lazily on first attribute
access (PEP 562), so ``import scrape_kit`` does not boot scrapling, rapidfuzz,
or pandas.

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

Shared-instance usage — submodule imports also work:

    from scrape_kit.fetcher import fetch, browser, scrape, is_blocked
    # Use directly — no instantiation needed after configure()
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .errors import FetcherError, MatchingError, ScrapeKitError, SettingsError, StorageError
from .logger import get_logger, time_profiler
from .settings import SettingsManager

if TYPE_CHECKING:
    from .fetcher import WebFetcher

__all__ = [
    # Settings
    "SettingsManager",
    # Storage
    "BaseStorageManager",
    "BufferedStorageManager",
    "MergeReport",
    # Fetcher — class
    "WebFetcher",
    "InteractiveSession",
    "ScrapeMode",
    # Fetcher — module-level proxies (use after configure())
    "fetch",
    "is_blocked",
    "browser",
    "scrape",
    "reset_shared",
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
    "MatchingError",
    # Logging
    "get_logger",
    "time_profiler",
]

# Lazily imported exports (PEP 562): public name -> submodule providing it.
_LAZY_IMPORTS: dict[str, str] = {
    "WebFetcher": "fetcher",
    "InteractiveSession": "fetcher",
    "ScrapeMode": "fetcher",
    "fetch": "fetcher",
    "is_blocked": "fetcher",
    "browser": "fetcher",
    "scrape": "fetcher",
    "reset_shared": "fetcher",
    "SimilarityEngine": "matching",
    "BaseStorageManager": "storage",
    "BufferedStorageManager": "storage",
    "MergeReport": "storage",
}


def __getattr__(name: str) -> Any:
    """Import and cache a lazily loaded export on first attribute access (PEP 562)."""
    submodule = _LAZY_IMPORTS.get(name)
    if submodule is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{submodule}", __package__), name)
    globals()[name] = value  # Cache so later accesses skip __getattr__.
    return value


def __dir__() -> list[str]:
    """List public names, including exports that are not imported yet (PEP 562)."""
    return sorted(set(globals()) | set(_LAZY_IMPORTS))


def configure(config_path: str, config_key: str = "scraper_config") -> "WebFetcher":
    """Load scraper indicators from a YAML config directory and set the shared instance.

    Reads ``<config_path>/<config_key>.yaml`` (via SettingsManager) and
    configures the module-level shared WebFetcher used by ``fetch()``,
    ``scrape()``, ``browser()``, and ``is_blocked()``.

    Returns:
        The configured WebFetcher instance in case you need it directly.

    Raises:
        SettingsError: If the config directory or YAML file is missing or malformed.
    """
    from .fetcher import WebFetcher

    return WebFetcher.configure(config_path, config_key=config_key, set_shared=True)


def configure_defaults() -> "WebFetcher":
    """Set the shared instance using the built-in default indicator lists.

    Use this when you don't have a config file but still want the full set
    of common retry/block indicators.

    Returns:
        The configured WebFetcher instance in case you need it directly.
    """
    from .fetcher import WebFetcher

    return WebFetcher.configure_defaults(set_shared=True)
