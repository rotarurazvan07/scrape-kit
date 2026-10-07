# scrape-kit

A Python library you install into another project for anti-detection fetching, fuzzy entity matching, SQLite storage, and YAML settings. You call the public API from `scrape_kit`.

**Requires Python 3.11+.** Current version is **0.2.0**.

## Install

```bash
pip install git+https://github.com/rotarurazvan07/scrape-kit.git@v0.2.0
scrapling install
```

`scrapling install` downloads the browser binaries that `fetch()`, `scrape()`, and `browser()` need.

Without a version pin:

```bash
pip install git+https://github.com/rotarurazvan07/scrape-kit.git
```

## Quick start

```python
from scrape_kit import FetcherError, configure_defaults, fetch

configure_defaults()

try:
    html = fetch("https://example.com")
except FetcherError as exc:
    print(exc.url, exc)
else:
    print(html[:200])
```

`configure_defaults()` sets the shared fetcher from the built-in retry and block indicators. If you skip it, the first `fetch()`, `scrape()`, `browser()`, or `is_blocked()` call creates that same default instance for you.

To load indicators from YAML instead, call `configure("path/to/config")`. That reads `path/to/config/scraper_config.yaml`.

## Public modules

| Guide | What you use it for |
| --- | --- |
| [Usage index](docs/index.md) | Map of every consumer guide |
| [Fetching](docs/fetcher.md) | `configure`, `configure_defaults`, `WebFetcher`, `fetch`, `browser`, `scrape`, `is_blocked`, `reset_shared`, `InteractiveSession`, `ScrapeMode` |
| [Matching](docs/matching.md) | `SimilarityEngine` and `similarity()` |
| [Storage](docs/storage.md) | `BaseStorageManager`, `BufferedStorageManager`, `MergeReport` |
| [Settings](docs/settings.md) | `SettingsManager` |
| [Errors](docs/errors.md) | Exception hierarchy and `FetcherError.url` |
| [Logging](docs/logging.md) | `get_logger`, `time_profiler`, `SCRAPE_KIT_LOG_LEVEL` |
| [Local development](docs/development.md) | Editable install and pytest |

The public surface is `scrape_kit.__all__` plus `configure()` and `configure_defaults()`. Import from `scrape_kit`.

## License

See [LICENSE](LICENSE).
