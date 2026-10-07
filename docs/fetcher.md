# Fetching

Use the fetcher API to download HTML, escalate blocked requests into a stealth browser, drive a persistent page, and scrape URL lists.

Import from `scrape_kit`:

```python
from scrape_kit import (
    FetcherError,
    InteractiveSession,
    ScrapeMode,
    WebFetcher,
    browser,
    configure,
    configure_defaults,
    fetch,
    is_blocked,
    reset_shared,
    scrape,
)
```

## Choose a usage style

You have two equivalent tiers.

**Shared instance.** Call `configure()` or `configure_defaults()`, then use the module-level proxies `fetch()`, `browser()`, `scrape()`, and `is_blocked()`.

**Explicit instance.** Construct `WebFetcher(...)` when several fetchers with different indicator lists must coexist.

Unconfigured proxies auto-create a shared instance with `configure_defaults()` semantics. `WebFetcher()` with no arguments is different: it starts with empty indicator lists.

## Configure the shared fetcher

`configure(config_path, config_key="scraper_config")` reads `<config_path>/<config_key>.yaml` through `SettingsManager` and sets the shared `WebFetcher` used by the proxies. It returns that instance.

A missing directory or missing YAML key uses the built-in default indicator lists. Malformed YAML raises `SettingsError`.

```python
from scrape_kit import configure, fetch

configure("path/to/config")  # reads path/to/config/scraper_config.yaml
html = fetch("https://example.com")
```

Expected YAML shape:

```yaml
retry_indicators:
  - "Just a moment"
  - "403 Forbidden"
block_indicators:
  - "cf-browser-verification"
  - "Access Denied"
```

`configure_defaults()` sets the shared instance from the built-in indicator lists. Use it when you have no config file.

```python
from scrape_kit import configure_defaults, fetch

configure_defaults()
html = fetch("https://example.com")
```

`reset_shared()` clears the shared instance. The next proxy call creates a fresh default-configured instance. Use this in tests.

## Fetch one URL

```python
from scrape_kit import FetcherError, configure_defaults, fetch

configure_defaults()
try:
    html = fetch("https://example.com", stealthy_headers=False, retries=3, backoff=5.0)
except FetcherError as exc:
    print(exc.url, exc)
```

Signature: `fetch(url, stealthy_headers=False, retries=3, backoff=5.0) -> str`.

- `retries < 1` raises `ValueError`.
- Exhaustion raises `FetcherError` with `url=` set.
- Persistent retry indicators escalate to a stealth browser automatically.

The same signature lives on `WebFetcher.fetch()`.

## Check for blocks

```python
from scrape_kit import is_blocked

if is_blocked(html):
    print("blocked or empty")
```

Empty or missing HTML counts as blocked. Otherwise the check looks for any configured block indicator.

## Open a browser session

`browser()` returns an `InteractiveSession`. Enter it with `with` before you touch the page. State violations raise `FetcherError`.

```python
from scrape_kit import browser, configure_defaults

configure_defaults()

with browser(headless=True, solve_cloudflare=False, interactive=True) as session:
    html = session.fetch("https://example.com")
    session.wait_for_selector("h1")
    clicked = session.click("button", text="Submit")
    session.scroll_to_bottom()
```

`browser(headless=True, solve_cloudflare=False, interactive=True, **kwargs)` passes extra keywords to the underlying Scrapling session. `solve_cloudflare=True` uses a stealth session.

Do not construct `InteractiveSession` yourself. Obtain it from `browser()` or `WebFetcher.browser()`.

### InteractiveSession methods

| Method | What it does |
| --- | --- |
| `fetch(url, timeout=90000, wait_until="load")` | Navigate and return settled HTML. Timeout is milliseconds. |
| `execute_script(script)` | Run JavaScript. A script that starts with `return ` is wrapped so the expression result comes back. |
| `wait_for_selector(selector, timeout=30000, **kwargs)` | Wait for a CSS selector. |
| `wait_for_function(expression, timeout=30000, **kwargs)` | Wait until a JS expression is truthy. |
| `wait_for_timeout(ms, **kwargs)` | Pause for `ms` milliseconds. |
| `scroll_to_bottom(infinite=True, idle_ms=10000, cycle_delay_ms=None)` | Scroll until content stops growing. |
| `click(selector, text=None, visible_only=False, idle_ms=5000, hard_cap_ms=None)` | Click a selector (optionally by exact text). Returns `True` on a settled click, `False` if nothing matched or the script failed. |

Call `fetch()` on the session before the other page methods. `click()` raises `FetcherError` if you have not entered the session.

## Scrape many URLs

```python
from scrape_kit import ScrapeMode, configure_defaults, scrape

configure_defaults()

def on_page(url: str, html: str) -> None:
    print(url, len(html))

scrape(
    ["https://example.com", "https://example.org"],
    on_page,
    mode=ScrapeMode.FAST,
    max_concurrency=1,
)
```

Signature: `scrape(urls, callback, mode=ScrapeMode.FAST, max_concurrency=1)`.

`ScrapeMode` is a `str` Enum: `FAST = "fast"`, `STEALTH = "stealth"`. Raw strings and enum members are interchangeable.

- The callback receives `(url, html)` for each successful fetch. Callback exceptions propagate immediately.
- `max_concurrency < 1` or an unknown mode raises `ValueError`.
- `STEALTH` uses `asyncio.run` and cannot run from an already-running event loop (`RuntimeError`).
- Fetch failures raise `FetcherError`.

## Explicit WebFetcher

```python
from scrape_kit import WebFetcher

fetcher = WebFetcher(
    retry_indicators=["Just a moment"],
    block_indicators=["Access Denied"],
)
html = fetcher.fetch("https://example.com")

with fetcher.browser() as session:
    session.fetch("https://example.com")
```

Class factories `WebFetcher.configure(...)` and `WebFetcher.configure_defaults(...)` also exist. The package-level `configure()` / `configure_defaults()` helpers always set the shared instance.

## Related guides

- [Errors](errors.md) for `FetcherError.url`.
- [Settings](settings.md) for the YAML directory `configure()` reads.
