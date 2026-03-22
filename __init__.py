"""
Scrape-Kit: A flexible, high-performance scraping framework.
"""

from settings import SettingsManager
from storage import BaseStorageManager, BufferedStorageManager
from fetcher import WebFetcher, ScrapeMode
from matching import SimilarityEngine
from errors import (
    ScrapeKitError,
    FetcherError,
    StorageError,
    SettingsError
)

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
    "SettingsError"
]
