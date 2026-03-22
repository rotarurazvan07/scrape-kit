class ScrapeKitError(Exception):
    """Base exception for the scrape-kit framework."""


class FetcherError(ScrapeKitError):
    """Raised when fetching fails persistently or escalation crashes."""


class StorageError(ScrapeKitError):
    """Raised on SQLite or data integration failures."""


class SettingsError(ScrapeKitError):
    """Raised when configuration/settings files are missing or malformed."""
