# Errors

Catch `ScrapeKitError` to handle every scrape-kit domain failure. Narrower types tell you which subsystem failed.

```python
from scrape_kit import (
    FetcherError,
    MatchingError,
    ScrapeKitError,
    SettingsError,
    StorageError,
)
```

## Hierarchy

| Exception | Raised when |
| --- | --- |
| `ScrapeKitError` | Base type for the library. |
| `FetcherError` | A fetch fails after retries or browser escalation, or an `InteractiveSession` is used before it is entered. |
| `StorageError` | SQLite or buffer work fails. |
| `SettingsError` | YAML is malformed, unreadable, or a settings path is invalid. |
| `MatchingError` | `SimilarityEngine` receives empty or invalid configuration. |

Call-time misuse still uses built-in exceptions: `ValueError` for `retries < 1`, non-string `similarity()` inputs, `max_concurrency < 1`, a bad scrape mode, or a non-mapping buffered insert. `STEALTH` scrape from a running event loop raises `RuntimeError`.

## FetcherError.url

`FetcherError(message, url=None)` stores the related URL on `exc.url` when the library knows it.

```python
from scrape_kit import FetcherError, configure_defaults, fetch

configure_defaults()
try:
    fetch("https://example.com", retries=1)
except FetcherError as exc:
    print(exc.url)
    print(exc)
```

Session-state errors (session not started, `fetch()` not called yet) may leave `url` as `None`.

## Related guides

- [Fetching](fetcher.md)
- [Matching](matching.md)
- [Storage](storage.md)
- [Settings](settings.md)
