# scrape-kit usage guide

These pages document the public installable API for integrators. Import from `scrape_kit`. Do not rely on private names (`_`-prefixed) or package-internal helpers.

## Start here

1. Install the package from the [README](../README.md).
2. Call `configure()` or `configure_defaults()` if you want an explicit shared fetcher.
3. Open the guide for the module you need.

## Guides

| Guide | Public names |
| --- | --- |
| [Fetching](fetcher.md) | `configure`, `configure_defaults`, `WebFetcher`, `fetch`, `browser`, `scrape`, `is_blocked`, `reset_shared`, `InteractiveSession`, `ScrapeMode` |
| [Matching](matching.md) | `SimilarityEngine` |
| [Storage](storage.md) | `BaseStorageManager`, `BufferedStorageManager`, `MergeReport` |
| [Settings](settings.md) | `SettingsManager` |
| [Errors](errors.md) | `ScrapeKitError`, `FetcherError`, `StorageError`, `SettingsError`, `MatchingError` |
| [Logging](logging.md) | `get_logger`, `time_profiler` |
| [Local development](development.md) | Editable install and pytest only |

## Typical paths

- Fetch one page: [Fetching](fetcher.md#fetch-one-url).
- Drive a browser: [Fetching](fetcher.md#open-a-browser-session).
- Batch URLs: [Fetching](fetcher.md#scrape-many-urls).
- Compare entity names: [Matching](matching.md).
- Persist rows in SQLite: [Storage](storage.md).
- Read and write YAML: [Settings](settings.md).
- Catch failures: [Errors](errors.md).
- Tune log output: [Logging](logging.md).
