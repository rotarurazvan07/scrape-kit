"""Exception hierarchy for scrape-kit: catch ScrapeKitError to handle any domain failure."""


class ScrapeKitError(Exception):
    """Base exception for the scrape-kit framework."""


class FetcherError(ScrapeKitError):
    """Raised when fetching fails persistently or escalation crashes.

    Attributes:
        url: The URL the failure relates to, when known; None otherwise.
    """

    def __init__(self, message: str, url: str | None = None) -> None:
        """Initialize the error.

        Args:
            message: Human-readable failure description.
            url: The URL the failure relates to, when known. Defaults to None.
        """
        super().__init__(message)
        self.url = url


class StorageError(ScrapeKitError):
    """Raised on SQLite or data integration failures."""


class SettingsError(ScrapeKitError):
    """Raised when configuration/settings files are missing or malformed."""


class MatchingError(ScrapeKitError):
    """Raised when SimilarityEngine receives invalid configuration."""
