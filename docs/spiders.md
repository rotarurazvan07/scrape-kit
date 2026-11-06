# Spider Migration Guide

This guide documents how to replace `scrape_kit.scrape()` patterns with Scrapling's native spider framework (`scrapling.spiders`).
Scrapling's spiders provide a production-ready scheduler with per-domain throttling, AutoThrottle, pause/resume checkpoints, and streaming — capabilities that go beyond scrape-kit's batch concurrency model.

All examples below are verified against `scrapling==0.5` and use the correct import path:

```python
from scrapling.spiders import CrawlSpider, SitemapSpider, SiteToMarkdownSpider, CrawlRule, Request, Response
```

> **Note:** Spiders are NOT exported from the top-level `scrapling` package. Use `from scrapling.spiders import ...` as shown above.

---

## 1. Why Migrate?

| scrape-kit `scrape()` | Scrapling Spider Framework |
| --- | --- |
| Flat concurrency (`max_concurrency`) | Per-domain limits (`concurrent_requests_per_domain`) |
| Custom `_BatchCooldown` for 429 | AutoThrottle (latency-driven, `Retry-After`-aware, robots `Crawl-delay`-aware) |
| No pause/resume | Atomic pickle checkpoints via `crawldir` |
| Batch-only, blocks until done | Streaming: `async for item in spider.stream()` yields live |
| Hand-rolled retry/escalation | `is_blocked()` + `retry_blocked_request()` hooks + `BLOCKED_CODES` |
| No priority scheduling | Priority queue scheduler with `CrawlRule` |
| No sitemap/feed support | `SitemapSpider`, `XMLFeedSpider`, `CSVFeedSpider`, `ShopifySpider` |
| No RAG-ready crawl | `SiteToMarkdownSpider` (alias of `ContentSpider`) |

If your workload needs any of the right column, the spider framework is the maintained path.

---

## 2. Quick-Start: `CrawlSpider` (Replaces `scrape(urls, callback)`)

### Before (scrape-kit)

```python
from scrape_kit import scrape

def handle(response, url):
    print(f"{url} -> {response.status}")

urls = ["https://example.com/page1", "https://example.com/page2"]
scrape(urls, handle, max_concurrency=8)
```

### After (Scrapling `CrawlSpider`)

```python
import asyncio
from scrapling.spiders import CrawlSpider, Request, Response

class MySpider(CrawlSpider):
    name = "my_spider"
    start_urls = ["https://example.com/page1", "https://example.com/page2"]
    concurrent_requests = 8  # replaces max_concurrency

    async def parse(self, response: Response):
        print(f"{response.url} -> {response.status}")
        # yield dict items for structured output
        yield {"url": response.url, "status": response.status, "title": response.css("title::text").get()}

if __name__ == "__main__":
    spider = MySpider()
    asyncio.run(spider.run())
```

**Key mappings:**
- `max_concurrency` → `concurrent_requests` (class attribute)
- callback → `async def parse(self, response)` method
- `scrape()` blocks → `asyncio.run(spider.run())`

---

## 3. `SitemapSpider` with Rules

`SitemapSpider` automatically discovers URLs from `sitemap.xml` and follows `CrawlRule` patterns.

```python
import asyncio
from scrapling.spiders import SitemapSpider, CrawlRule, Response

class DocsSpider(SitemapSpider):
    name = "docs_spider"
    sitemap_urls = ["https://example.com/sitemap.xml"]
    concurrent_requests = 16
    concurrent_requests_per_domain = 8
    download_delay = 0.5

    # Only crawl /docs/ and /blog/ paths, skip /api/
    rules = (
        CrawlRule(allow=r"/docs/.*"),
        CrawlRule(allow=r"/blog/.*"),
        CrawlRule(deny=r"/api/.*"),
    )

    async def parse(self, response: Response):
        yield {
            "url": response.url,
            "title": response.css("h1::text").get(),
            "markdown": response.markdown(main_content_only=True),  # requires scrapling[rag]
        }

if __name__ == "__main__":
    spider = DocsSpider()
    asyncio.run(spider.run())
```

**Output formats:** `spider.run()` returns `ItemList` with `.to_json()`, `.to_jsonl()`, `.to_csv()`, `.to_xml()`.

---

## 4. `SiteToMarkdownSpider` for RAG / Ingestion

`SiteToMarkdownSpider` is an **alias of `ContentSpider`**. It requires `allowed_domains` and yields `{url, title, markdown}` dicts.

```python
import asyncio
from scrapling.spiders import SiteToMarkdownSpider

class RagSpider(SiteToMarkdownSpider):
    name = "rag_spider"
    allowed_domains = ["example.com"]  # REQUIRED - ValueError if missing
    start_urls = ["https://example.com/docs/"]
    
    # Knobs (all optional)
    css_selector = "main, article, .content"  # default: "main, article, .content, #content"
    main_content_only = True                  # default: True
    output_dir = "./rag_output"               # default: None (yields only)
    max_pages = 100                            # default: None (no limit)
    concurrent_requests = 8

if __name__ == "__main__":
    spider = RagSpider()
    results = asyncio.run(spider.run())
    # results is ItemList; each item: {"url": ..., "title": ..., "markdown": ...}
    results.to_jsonl("rag_corpus.jsonl")
```

**Requires** `scrapling[rag]` extra for `Response.markdown()`: `pip install "scrapling[rag]"`.

---

## 5. Retry & AutoThrottle Migration

### scrape-kit: `retry_hook` + `status_retries`

```python
# scrape-kit pattern
from scrape_kit import scrape

my_retry_hook = lambda url, attempt, exc: attempt < 3

scrape(urls, callback, retry_hook=my_retry_hook, max_retries=5)

# YAML config
status_retries:
  429: 3
  503: 2
```

### Scrapling: `is_blocked()` + `retry_blocked_request()` + `max_blocked_retries`

Scrapling's spider framework has **no per-status retry table**. Instead:

1. Override `is_blocked(response)` to define what counts as "blocked"
2. Override `retry_blocked_request(request)` to decide whether to retry
3. Set `max_blocked_retries` (default: 3) for the retry budget

```python
import asyncio
from scrapling.spiders import CrawlSpider, Response, Request

class RetrySpider(CrawlSpider):
    name = "retry_spider"
    start_urls = ["https://example.com"]
    concurrent_requests = 8
    max_blocked_retries = 3  # global budget for blocked requests

    # BLOCKED_CODES = {401, 403, 407, 429, 444, 500, 502, 503, 504}
    def is_blocked(self, response: Response) -> bool:
        # Default: True for status in BLOCKED_CODES
        # Customize: e.g., treat 403 as NOT blocked (opt-out like status_retries 403: 0)
        if response.status == 403:
            return False
        return response.status in {401, 403, 407, 429, 444, 500, 502, 503, 504}

    def retry_blocked_request(self, request: Request) -> bool:
        # Return True to retry, False to drop
        # Can inspect request.meta, URL, etc.
        return True

    async def parse(self, response: Response):
        yield {"url": response.url, "status": response.status}

if __name__ == "__main__":
    spider = RetrySpider()
    asyncio.run(spider.run())
```

### AutoThrottle

```python
class ThrottledSpider(CrawlSpider):
    name = "throttled"
    start_urls = ["https://example.com"]
    
    # AutoThrottle settings
    autothrottle_enabled = True
    autothrottle_start_delay = 2.0      # initial delay
    autothrottle_max_delay = 30.0       # max delay
    autothrottle_target_concurrency = 1.0  # target parallelism per domain
    autothrottle_block_backoff = True   # double delay on blocked response
    
    # Effective target_concurrency = autothrottle_target_concurrency
    #   or concurrent_requests_per_domain
    #   or 1.0
    # Honors Retry-After header (seconds or HTTP-date via parse_retry_after)
```

**Migration note:** scrape-kit's `_BatchCooldown` (shared per-call 429 cooldown) maps to spider `download_delay` + AutoThrottle. The spider model is per-domain and adaptive; scrape-kit's is flat and batch-scoped.

---

## 6. Pause / Resume (Checkpoint via `crawldir`)

Spiders support atomic pickle checkpoints. `crawldir` and `interval` are **constructor arguments** of `CrawlerEngine`, not class attributes.

```python
import asyncio
from scrapling.spiders import CrawlSpider

class CheckpointSpider(CrawlSpider):
    name = "checkpoint_spider"
    start_urls = ["https://example.com/page/1"]
    concurrent_requests = 8

    async def parse(self, response):
        yield {"url": response.url}

if __name__ == "__main__":
    spider = CheckpointSpider()
    # crawldir and interval are CrawlerEngine constructor args
    asyncio.run(spider.run(crawldir="./crawl_state", interval=30))
    # Creates ./crawl_state/checkpoint.pkl (atomic pickle)
    # Saves periodically + on graceful SIGINT
    # Auto-resumes on rerun with same crawldir
    # File deleted ONLY on normal completion
```

**Checkpoint details:**
- `{crawldir}/checkpoint.pkl` — atomic pickle of pending requests + seen fingerprints
- `interval` — seconds between periodic saves (constructor arg)
- SIGINT triggers save; rerun with same `crawldir` auto-resumes
- **No SIGINT pause/resume in stream mode** (see Section 7)

---

## 7. Streaming (`async for item in spider.stream()`)

For memory-efficient large crawls, use streaming. Items yield live with `spider.stats` updated in real time.

```python
import asyncio
from scrapling.spiders import CrawlSpider

class StreamSpider(CrawlSpider):
    name = "stream_spider"
    start_urls = [f"https://example.com/page/{i}" for i in range(1000)]
    concurrent_requests = 16

    async def parse(self, response):
        yield {"url": response.url, "title": response.css("title::text").get()}

async def main():
    spider = StreamSpider()
    # Stream yields items as they're scraped
    async for item in spider.stream():
        print(f"Got: {item['url']} - stats: {spider.stats}")
        # Process item immediately (write to DB, queue, etc.)
        # spider.stats updates live: requests, responses, errors, bytes

if __name__ == "__main__":
    asyncio.run(main())
```

**Important:** Streaming mode does **not** support SIGINT pause/resume. Checkpointing only works with `spider.run(crawldir=...)`.

---

## 8. Per-Domain Limits (`concurrent_requests_per_domain`)

Unlike scrape-kit's flat `max_concurrency`, spiders enforce per-domain concurrency.

```python
class DomainLimitedSpider(CrawlSpider):
    name = "domain_limited"
    start_urls = [
        "https://site-a.com/page1",
        "https://site-a.com/page2",
        "https://site-b.com/page1",
        "https://site-b.com/page2",
    ]
    concurrent_requests = 16           # global cap
    concurrent_requests_per_domain = 4  # per-domain cap (site-a: 4, site-b: 4)
    download_delay = 0.5               # polite delay between requests to same domain

    async def parse(self, response):
        yield {"url": response.url}
```

**Interaction with AutoThrottle:** When `autothrottle_enabled=True`, the effective delay is adaptive (measured latency, `Retry-After`, robots `Crawl-delay`). `download_delay` is the floor.

---

## 9. Configuration Reference (YAML `spider:`, `browser:`, `parser:`)

If you use scrape-kit's `scraper_config.yaml` → `SettingsManager` → `WebFetcher.configure()`, the following optional sections are accepted (all keys optional, depth-first fallback preserves unknown-key tolerance):

```yaml
# Full schema with new optional sections
retry_indicators: []
block_indicators: []
status_retries: {}
parser:
  adaptive: false
  adaptive_domain: ""
  markdown:
    main_content_only: true
browser:
  block_ads: false
  dns_over_https: false
  blocked_domains: []
  pierce_shadow: false
  record_requests: false
  max_recorded_requests: 1000
  user_data_dir: null
  save_state_path: null
  load_state_path: null
spider:
  concurrent_requests: 16
  concurrent_requests_per_domain: 8
  download_delay: 0.5
  autothrottle_enabled: false
  autothrottle_start_delay: 2.0
  autothrottle_max_delay: 30.0
  autothrottle_block_backoff: true
  robots_txt_obey: false
  crawl_strategy: "bfs"      # or "dfs" (NOT "breadth"/"depth")
  crawldir: null
  development_mode: false
```

**Mapping notes:**
- `crawl_strategy`: Use `"bfs"` or `"dfs"` — **`"breadth"/"depth"` are incorrect** (fixed in `docs/settings.md`)
- `crawldir`: Path string; enables checkpointing when set
- `blocked_domains`: YAML sequence → converted to `Set[str]` internally
- `user_data_dir`: Plain string; never pass `null`/`None` to Scrapling
- `save_state_path`/`load_state_path`: Path strings for session persistence

---

## 10. Troubleshooting / FAQ

| Problem | Solution |
| --- | --- |
| `ValueError: allowed_domains is required` | `SiteToMarkdownSpider`/`ContentSpider` **requires** `allowed_domains` class attribute |
| `TypeError: crawl_strategy must be 'bfs' or 'dfs'` | Use `"bfs"` or `"dfs"`, not `"breadth"/"depth"` |
| `ModuleNotFoundError: No module named 'scrapling.spiders'` | Import from `scrapling.spiders`, not top-level `scrapling` |
| Checkpoint not resuming | Ensure same `crawldir` path; checkpoint file is `{crawldir}/checkpoint.pkl` |
| Streaming doesn't pause on SIGINT | Use `spider.run(crawldir=...)` for checkpointable runs; streaming is fire-and-forget |
| `response.markdown()` fails | Install `scrapling[rag]` extra: `pip install "scrapling[rag]"` |
| ProxyRotator not found | Import from `scrapling.fetchers`, not `scrapling.spiders` |

---

## 11. Migration Checklist

- [ ] Replace `scrape(urls, callback)` with `CrawlSpider` subclass
- [ ] Map `max_concurrency` → `concurrent_requests`
- [ ] Convert callback → `async def parse(self, response)` yielding dicts
- [ ] Replace `retry_hook` → override `is_blocked(response)` + `retry_blocked_request(request)`
- [ ] Replace YAML `status_retries` → `max_blocked_retries` + custom `is_blocked()`
- [ ] Add `concurrent_requests_per_domain` for per-domain throttling
- [ ] Enable `autothrottle_enabled=True` for adaptive delays
- [ ] Use `crawldir` + `interval` constructor args for pause/resume
- [ ] Use `async for item in spider.stream()` for memory-efficient large crawls
- [ ] For sitemap-driven crawls, use `SitemapSpider` with `CrawlRule`
- [ ] For RAG/Markdown ingestion, use `SiteToMarkdownSpider` (requires `allowed_domains`)
- [ ] Update `scraper_config.yaml` with `spider:` section using `bfs`/`dfs` for `crawl_strategy`

---

## 12. API Reference

Auto-generated API reference for all spider classes:

- **Base:** `Spider`, `Request`, `Response`, `CrawlResult`, `SessionManager`, `Scheduler`, `CrawlerEngine`
- **Spiders:** `CrawlSpider`, `SitemapSpider`, `SiteToMarkdownSpider` (alias of `ContentSpider`), `XMLFeedSpider`, `CSVFeedSpider`, `ShopifySpider`
- **Rules:** `CrawlRule`, `LinkExtractor`
- **Errors:** `SessionConfigurationError`

See: <https://scrapling.readthedocs.io/en/latest/spiders/architecture.html>

---

## P4 Trigger Condition: Optional Facade

> **`scrape_kit.spiders` facade is NOT built yet.** It will only be created when **>= 3 consumer issues/PRs** request it AND this guide is published.

If you want a scrape-kit facade re-exporting `Spider`, `CrawlSpider`, `SitemapSpider`, `Request`, `Response`, `CrawlRule`, `LinkExtractor` with `FetcherError` chaining and YAML config mapping, **open a GitHub issue** requesting it.

Current status: 0/3 consumer requests recorded.

---

## See Also

- [Fetching Guide](fetcher.md) — scrape-kit's `fetch`, `browser`, `scrape`
- [Settings Guide](settings.md) — YAML config schema (includes `spider:` section)
- [Parsing Guide](parse.md) — `scrape_kit.parse` selectors and adaptive selection
- [Scrapling Spider Docs](https://scrapling.readthedocs.io/en/latest/spiders/getting-started.html)