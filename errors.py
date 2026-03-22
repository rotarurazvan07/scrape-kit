class ScrapeKitError(Exception):
    """Base exception for the scrape-kit framework."""
    pass

class FetcherError(ScrapeKitError):
    """Raised when fetching fails persistently or escalation crashes."""
    pass

class StorageError(ScrapeKitError):
    """Raised on SQLite or data integration failures."""
    pass

class SettingsError(ScrapeKitError):
    """Raised when configuration/settings files are missing or malformed."""
    pass
