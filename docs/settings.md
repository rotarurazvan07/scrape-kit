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

## Per-status retry budgets

`scraper_config.yaml` may also carry a `status_retries` map, which sets an HTTP-status-specific retry budget used by the fetcher.

```yaml
status_retries:
  429: 5
  503: 3
  403: 0
```

- Keys are HTTP status codes; integers and numeric strings are accepted.
- Values are a non-negative integer or `null`. An integer is that status's retry count, which replaces the call-time `retries` argument for that status only; `null` inherits the call-time argument.
- Configured entries merge over the built-in defaults (`401`, `403`, `407`, `429`, `444`, `500`, `502`, `503`, `504`), which apply to every fetch path, so listing one status never removes another's behavior.
- `0` opts a status out of retrying, and its response body is delivered instead of retried.
- Invalid keys or values raise `SettingsError` when `configure()` reads the file.

See the [fetcher guide](fetcher.md#per-status-retry-budgets) for how the budgets affect waits, the batch cooldown, and the browser path.

## Parser configuration

The `parser` section configures the HTML parsing behavior (used by the `scrape_kit.parse` module).

```yaml
parser:
  adaptive: true
  adaptive_domain: "example.com"
  markdown:
    main_content_only: false
```

- `adaptive` (bool): Enable adaptive parsing that adjusts selectors based on page structure. Default: `false`.
- `adaptive_domain` (string): Domain hint for adaptive parsing to improve accuracy. Default: `""`.
- `markdown.main_content_only` (bool): When converting to markdown, extract only the main content area. Default: `false`.

All keys are optional. The section is stored and made available to the parse module; `WebFetcher.configure()` does not validate its contents.

## Browser configuration

The `browser` section provides defaults for `WebFetcher.browser()` calls. Each key maps directly to a `browser()` keyword argument. Call-time arguments override configured values, which in turn override Scrapling defaults (depth-first fallback).

```yaml
browser:
  block_ads: false
  dns_over_https: false
  blocked_domains: []
  pierce_shadow: false
  record_requests: false
  max_recorded_requests: 1000
  user_data_dir: ""
  save_state_path: ""
  load_state_path: ""
  extra_flags: []
```

- `block_ads` (bool): Block ~3,500 known ad/tracking domains. Default: Scrapling default (`false`).
- `dns_over_https` (bool): Route DNS through Cloudflare DoH. Default: Scrapling default (`false`).
- `blocked_domains` (list of strings): Domains to block (subdomains matched). Converted to a set for Scrapling. Default: Scrapling default (empty).
- `pierce_shadow` (bool): Include open Shadow DOM content in response HTML. Default: Scrapling default (`false`).
- `record_requests` (bool): Enable network request recording. Default: Scrapling default (`false`).
- `max_recorded_requests` (int, >=1): Maximum requests to record when `record_requests=true`. Default: Scrapling default (`1000`).
- `user_data_dir` (string): Path to User Data Directory for session persistence. Empty string is omitted (never passed as `None` to Scrapling). Default: Scrapling default (temporary directory).
- `save_state_path` (string): Path for `session.save_state(path)` auto-save. **Not** a `StealthySession` constructor argument; used by application code after session creation. Default: `""` (no auto-save).
- `load_state_path` (string): Path for `session.load_state(path)` auto-load. **Not** a `StealthySession` constructor argument; used by application code after session creation. Default: `""` (no auto-load).
- `extra_flags` (list of strings): Additional browser launch flags (e.g., `"--custom-flag"`). Merged with built-in low-memory flags (`--disable-dev-shm-usage`, `--disable-gpu`, `--no-sandbox`, `--disable-setuid-sandbox`). Default: low-memory flags only.

All keys are optional. Missing keys fall back to Scrapling defaults.

**Security notes:**
- `record_requests=true` captures Authorization headers, cookies, and PII in network traffic.
- `user_data_dir` persists browser profile including cookies, localStorage, and credentials.
- `save_state()`/`load_state()` serialize full browser state including sensitive data.

## Spider configuration

The `spider` section configures crawling behavior passed through to the Scrapling spider engine.

```yaml
spider:
  concurrent_requests: 16
  concurrent_requests_per_domain: 8
  download_delay: 0.25
  autothrottle_enabled: true
  autothrottle_start_delay: 5.0
  autothrottle_max_delay: 60.0
  autothrottle_target_concurrency: 1.0
  robots_txt_obey: true
  crawl_strategy: "bfs"
  crawldir: ""
  development_mode: false
```

- `concurrent_requests` (int): Maximum concurrent requests globally. Default: `16`.
- `concurrent_requests_per_domain` (int): Maximum concurrent requests per domain. Default: `8`.
- `download_delay` (float): Base delay between requests to the same domain (seconds). Default: `0.25`.
- `autothrottle_enabled` (bool): Enable AutoThrottle extension. Default: `true`.
- `autothrottle_start_delay` (float): Initial download delay for AutoThrottle (seconds). Default: `5.0`.
- `autothrottle_max_delay` (float): Maximum download delay for AutoThrottle (seconds). Default: `60.0`.
- `autothrottle_target_concurrency` (float): Target concurrency for AutoThrottle. Default: `1.0`.
- `robots_txt_obey` (bool): Respect `robots.txt` rules. Default: `true`.
- `crawl_strategy` (string): `"bfs"` or `"dfs"`. Default: `"bfs"`.
- `crawldir` (string): Directory for crawl persistence. Default: `""`.
- `development_mode` (bool): Enable development features (verbose logging, etc.). Default: `false`.

All keys are optional. The section is stored and made available to the spider module; `WebFetcher.configure()` does not validate its contents.

## Complete example

```yaml
scraper_config:
  retry_indicators:
    - "Just a moment"
    - "403 Forbidden"
  block_indicators:
    - "cf-browser-verification"
    - "Access Denied"
  status_retries:
    429: 5
    503: 3
    403: 0
  parser:
    adaptive: true
    adaptive_domain: "example.com"
    markdown:
      main_content_only: false
  browser:
    block_ads: true
    dns_over_https: true
    blocked_domains:
      - "ads.example.com"
      - "tracker.example.com"
    pierce_shadow: true
    record_requests: true
    max_recorded_requests: 500
    user_data_dir: "/tmp/browser-profile"
    save_state_path: "/tmp/state.json"
    load_state_path: "/tmp/state.json"
    extra_flags:
      - "--custom-flag"
  spider:
    concurrent_requests: 16
    concurrent_requests_per_domain: 8
    download_delay: 0.25
    autothrottle_enabled: true
    autothrottle_start_delay: 5.0
    autothrottle_max_delay: 60.0
    autothrottle_target_concurrency: 1.0
    robots_txt_obey: true
    crawl_strategy: "bfs"
    crawldir: "/tmp/crawl"
    development_mode: false
```

## Proxy configuration

The `proxies` section provides the proxy pool used by `ProxyRotator`. When set, `WebFetcher.browser()` builds a `ProxyRotator` from the list and passes it to the session automatically.

```yaml
proxies:
  - "http://proxy1.example.com:8080"
  - server: "http://proxy2.example.com:8080"
    username: "user"
    password: "pass"
```

Entries are either proxy URL strings or mappings with `server`, `username`, and `password`. An empty or absent list means no proxy rotation. Call-time `browser(proxy_rotator=...)` takes precedence over the configured list.

## Related guides

- [Fetching](fetcher.md) for `configure()`.
- [Errors](errors.md) for `SettingsError`.
