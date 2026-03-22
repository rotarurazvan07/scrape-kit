from fetcher import WebFetcher, ScrapeMode

def test_scraper_init_defaults():
    # Verify defaults are now empty by user request
    scraper = WebFetcher()
    assert scraper.retry_indicators == []
    assert scraper.block_indicators == []

def test_scraper_init_custom():
    retry = ["403 Forbidden"]
    block = ["Access Denied"]
    scraper = WebFetcher(retry_indicators=retry, block_indicators=block)
    assert scraper.retry_indicators == retry
    assert scraper.block_indicators == block

def test_scraper_is_blocked():
    scraper = WebFetcher(block_indicators=["Blocked", "Access Denied"])

    assert scraper.is_blocked("Access Denied on this server") is True
    assert scraper.is_blocked("Welcome to our site") is False
    assert scraper.is_blocked("") is True

def test_scraper_mode():
    assert ScrapeMode.FAST == "fast"
    assert ScrapeMode.STEALTH == "stealth"

# Note: Integration tests for fetch() and browser() are excluded to avoid
# dependencies on external networks or Scrapling-specific environment setups
# during basic unit testing.
