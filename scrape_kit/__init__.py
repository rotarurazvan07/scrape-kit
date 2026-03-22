"""
Scrape-Kit: A flexible, high-performance scraping framework.
"""

from .errors import FetcherError, ScrapeKitError, SettingsError, StorageError
from .fetcher import ScrapeMode, WebFetcher
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
]
