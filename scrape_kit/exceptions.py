class ScrapeKitError(Exception):
    """Base exception for the scrape-kit framework."""
    pass

class ScraperError(ScrapeKitError):
    """Raised when scraping fails persistently or escalation crashes."""
    pass

class DatabaseError(ScrapeKitError):
    """Raised on SQLite or data integration failures."""
    pass

class ConfigError(ScrapeKitError):
    """Raised when configuration files are missing or malformed."""
    pass
