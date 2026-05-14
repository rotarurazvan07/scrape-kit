from .dedup import DedupConfig
from .errors import (
    FetcherError,
    FinderError,
    ParserError,
    ScrapeKitError,
    SettingsError,
    StorageError,
)
from .fetcher import ScrapeMode, browser, configure, configure_defaults, fetch, is_blocked, scrape
from .finder import BaseFinder
from .logger import get_logger, time_profiler
from .matching import SimilarityEngine, make_similarity_fn
from .page import Element, Page
from .settings import SettingsManager
from .storage import (
    BaseStorageManager,
    BufferedStorageManager,
    MergeReport,
)

__all__ = [
    # Configure
    "configure",
    "configure_defaults",
    # Fetch
    "fetch",
    "scrape",
    "browser",
    "is_blocked",
    # Page parsing
    "Page",
    "Element",
    # Finders
    "BaseFinder",
    "ScrapeMode",
    # Storage
    "BaseStorageManager",
    "BufferedStorageManager",
    "MergeReport",
    # Deduplication
    "DedupConfig",
    # Matching
    "SimilarityEngine",
    "make_similarity_fn",
    # Settings
    "SettingsManager",
    # Logging
    "get_logger",
    "time_profiler",
    # Errors
    "ScrapeKitError",
    "FetcherError",
    "StorageError",
    "SettingsError",
    "ParserError",
    "FinderError",
]
