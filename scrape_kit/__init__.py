"""
Scrape-Kit: A flexible, high-performance scraping framework.
"""

from .errors import FetcherError, ScrapeKitError, SettingsError, StorageError
from .fetcher import ScrapeMode, WebFetcher
from .logger import get_logger, time_profiler
from .matching import SimilarityEngine
from .settings import SettingsManager
from .storage import BaseStorageManager, BufferedStorageManager

__all__ = [
    "SettingsManager",
    "BaseStorageManager",
    "BufferedStorageManager",
    "WebFetcher",
    "ScrapeMode",
    "SimilarityEngine",
    "ScrapeKitError",
    "FetcherError",
    "StorageError",
    "SettingsError",
    "get_logger",
    "time_profiler",
]
