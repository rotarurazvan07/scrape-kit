from abc import ABC, abstractmethod
from typing import Any, Callable

from .fetcher import ScrapeMode, scrape
from .logger import get_logger
from .page import Page

logger = get_logger(__name__)


class BaseFinder(ABC):
    """Generic base class for scrapers.

    Handles the boilerplate of URL discovery, batch scraping, result routing,
    and skip pattern matching.
    """

    def __init__(
        self,
        on_result: Callable[[Any], bool] | None = None,
        *,
        skip_patterns: list[tuple[str, str]] | None = None,
        scrape_mode: str = ScrapeMode.FAST,
        max_concurrency: int = 1,
        **kwargs: Any,
    ):
        """Initialize the finder.

        Args:
            on_result: Optional callback for each result found. Should return True if accepted.
            skip_patterns: List of (substring, reason) tuples to filter out items.
            scrape_mode: "fast" or "stealth".
            max_concurrency: Concurrency for batch scraping.
            **kwargs: Additional configuration for the scraper.
        """
        self.on_result = on_result
        self.skip_patterns = skip_patterns or []
        self.scrape_mode = scrape_mode
        self.max_concurrency = max_concurrency
        self.kwargs = kwargs

    # ── MUST implement ───────────────────────────────────────────────────────

    @abstractmethod
    def get_urls(self) -> list[str]:
        """Return a list of URLs to be scraped."""
        pass

    @abstractmethod
    def _parse_page(self, url: str, page: Page) -> None:
        """Extract results from a single Page and call add_result()."""
        pass

    # ── CAN override ──────────────────────────────────────────────────────────

    def scrape(self, urls: list[str] | None = None) -> None:
        """Execute the scrape process for the given URLs (or all discovery URLs)."""
        target_urls = urls if urls is not None else self.get_urls()
        if not target_urls:
            logger.info("%s: No URLs to scrape.", self.__class__.__name__)
            return

        logger.info("%s: Scraping %d URLs in %s mode...", self.__class__.__name__, len(target_urls), self.scrape_mode)
        scrape(
            target_urls,
            callback=self._parse_page,
            mode=self.scrape_mode,
            max_concurrency=self.max_concurrency,
            **self.kwargs,
        )

    # ── Framework internals (call these inside _parse_page) ───────────────────

    def add_result(self, item: Any) -> bool:
        """Pass item to on_result callback. Returns True if accepted."""
        if self.on_result:
            return self.on_result(item)
        return True

    def skip_by_patterns(self, *text_fields: str) -> str | None:
        """Check strings against skip_patterns. Returns first matching reason or None."""
        for field in text_fields:
            if not field:
                continue
            field_lower = field.lower()
            for pattern, reason in self.skip_patterns:
                if pattern.lower() in field_lower:
                    return reason
        return None

    # ── Logging helpers ───────────────────────────────────────────────────────

    def log_skip(self, context: str, reason: Any = "") -> None:
        """Log a skipped item."""
        logger.warning("%s [SKIP]: %s | %s", self.__class__.__name__, context, reason)

    def log_error(self, context: str, error: Exception) -> None:
        """Log a parsing or runtime error."""
        logger.error("%s [ERROR]: %s | %s", self.__class__.__name__, context, error)

    def log_added(self, context: str, item: Any = "") -> None:
        """Log a successfully added item."""
        logger.info("%s [ADDED]: %s", self.__class__.__name__, context)
