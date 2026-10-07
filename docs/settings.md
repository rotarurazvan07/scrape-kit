# Settings

Use `SettingsManager` to load a directory of YAML files as a nested dict, then read, write, and delete keys.

```python
from scrape_kit import SettingsError, SettingsManager

settings = SettingsManager("path/to/config")
retry = settings.get("scraper_config", "retry_indicators")
```

`SettingsManager(directory)` recursively loads `*.yaml` files. You may also pass a single YAML file path. A missing directory yields an empty tree. Malformed YAML raises `SettingsError`.

A YAML stem that collides with a sibling directory (for example `db.yaml` next to `db/`) raises `SettingsError`.

## Read values

`get(*keys)` reloads from disk on every call.

- Pass a full path of keys: `settings.get("scraper_config", "retry_indicators")`.
- Or pass a leaf key. If the full path misses, `get()` runs a depth-first search for that last key.
- A key present with value `None` is a hit and returns `None`.
- No keys raises `SettingsError`.

```python
from scrape_kit import SettingsManager

settings = SettingsManager("path/to/config")
full = settings.get("scraper_config")
leaf = settings.get("retry_indicators")
```

The nested `settings.settings` dict is the in-memory tree after the last load.

## Write and delete

Writes are atomic: a temp file plus `os.replace`. Paths must stay inside the settings directory.

```python
from scrape_kit import SettingsManager

settings = SettingsManager("path/to/config")
settings.write(
    "scraper_config",
    {
        "retry_indicators": ["Just a moment"],
        "block_indicators": ["Access Denied"],
    },
)
settings.delete("scraper_config")
```

Both `write(name, data, *, subpath=None)` and `delete(name, *, subpath=None)` accept an optional subdirectory. Escaping the settings root raises `SettingsError`.

`configure()` in the [fetcher guide](fetcher.md) uses this same loader to read `scraper_config.yaml`.

## Related guides

- [Fetching](fetcher.md) for `configure()`.
- [Errors](errors.md) for `SettingsError`.
