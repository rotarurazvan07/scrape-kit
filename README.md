# scrape-kit

A personal high-performance Python scraping framework. Handles HTTP fetching, stealth browser sessions, fuzzy entity matching, SQLite storage, and YAML-based configuration — packaged for direct installation from GitHub rather than as a submodule.

### CI Pipeline Overview

| Stage | Description | Status |
|-------|-------------|--------|
| **Auto-fix** | Removes unused imports, modernises syntax, adds type annotations, sorts imports, formats code, fixes lint/perf anti-patterns | ✅ Auto-commits |
| **Tests** | Runs test suite across Python 3.10, 3.11, 3.12 with coverage reporting | 🔒 Gate |
| **Audit** | Security SAST (bandit), pattern security (semgrep), type checking (mypy), CVE scan (pip-audit), complexity (radon), dead code (vulture), docstring coverage (interrogate), secret detection (detect-secrets), workflow validation (actionlint) | ℹ️ Advisory |

---

## Installation

### Into a project

```bash
# Latest from main
pip install "git+https://github.com/yourusername/scrape-kit.git"

# Pin to a specific release tag (recommended for stability)
pip install "git+https://github.com/yourusername/scrape-kit.git@v0.1.0"

# Pin to a specific commit
pip install "git+https://github.com/yourusername/scrape-kit.git@a3f2c91"
```

### In requirements.txt

```
git+https://github.com/yourusername/scrape-kit.git@v0.1.0
```

### After installing — browser binaries

scrapling requires a one-time step to download browser binaries:

```bash
scrapling install
```

### Local development

```bash
git clone https://github.com/yourusername/scrape-kit.git
cd scrape-kit
pip install -e .
scrapling install
```

---

## Imports

Everything public is re-exported from the top-level package:

```python
from scrape_kit import WebFetcher, ScrapeMode
from scrape_kit import SimilarityEngine
from scrape_kit import SettingsManager
from scrape_kit import BaseStorageManager, BufferedStorageManager
from scrape_kit import FetcherError, StorageError, SettingsError, ScrapeKitError
```

Or import directly from the module if you prefer to be explicit:

```python
from scrape_kit.fetcher import WebFetcher, InteractiveSession, ScrapeMode
from scrape_kit.matching import SimilarityEngine
from scrape_kit.settings import SettingsManager
from scrape_kit.storage import BaseStorageManager, BufferedStorageManager
from scrape_kit.errors import FetcherError, StorageError, SettingsError
```

---

## Modules

### `fetcher` — Web Fetching

`WebFetcher` wraps [scrapling](https://github.com/D4Vinci/Scrapling) to provide fast HTTP fetching with automatic escalation to a stealth browser when blocked.

#### Simple fetch

```python
from scrape_kit import WebFetcher

fetcher = WebFetcher(
    retry_indicators=["just a moment", "checking your browser"],  # retry on these
    block_indicators=["access denied", "403 forbidden"],          # detect blocks
)

html = fetcher.fetch("https://example.com")
```

#### Automatic Cloudflare escalation

If `retry_indicators` are found on every retry attempt, `fetch()` automatically escalates to a headless browser to solve the challenge — no manual intervention needed.

```python
# Fetcher escalates to browser automatically if "just a moment" persists
html = fetcher.fetch("https://cloudflare-protected-site.com", retries=3, backoff=5.0)
```

#### Manual browser session

For JavaScript-heavy pages or multi-step flows:

```python
with fetcher.browser(headless=True) as session:
    session.fetch("https://example.com")
    session.click("#load-more")
    session.wait_for_selector(".results")
    html = session.page.content()
```

#### Cloudflare bypass session

```python
with fetcher.browser(solve_cloudflare=True, headless=True) as session:
    session.fetch("https://protected.com", timeout=120000, wait_until="networkidle")
    title = session.execute_script("return document.title")
```

#### Batch scraping

```python
urls = ["https://site1.com", "https://site2.com", "https://site3.com"]

def on_page(url, html):
    print(f"Got {len(html)} bytes from {url}")

# Fast mode — concurrent HTTP requests
fetcher.scrape(urls, callback=on_page, mode=ScrapeMode.FAST, max_concurrency=5)

# Stealth mode — headless browser, Cloudflare bypass
fetcher.scrape(urls, callback=on_page, mode=ScrapeMode.STEALTH, max_concurrency=2)
```

#### Block detection

```python
html = fetcher.fetch("https://example.com")
if fetcher.is_blocked(html):
    print("Page is blocked or empty")
```

---

### `matching` — Fuzzy Entity Matching

`SimilarityEngine` uses a weighted hybrid of token ratio, substring presence, Soundex phonetics, and character ratio to match entity names. Designed for matching scraped names against a known dataset (sports teams, book titles, people, etc.).

#### Basic usage

```python
from scrape_kit import SimilarityEngine

engine = SimilarityEngine({
    "threshold": 75,
    "weights": {
        "token":    0.5,   # token set ratio — handles word reordering
        "substr":   0.1,   # substring presence
        "phonetic": 0.1,   # Soundex — catches spelling variations
        "ratio":    0.3,   # character-level ratio
    }
})

is_match, score = engine.is_similar("Manchester United", "Man United FC")
# True, 87.3
```

#### Synonyms — exact replacements

```python
engine = SimilarityEngine({
    "threshold": 70,
    "synonyms": {
        "man city":  "manchester city",
        "barca":     "fc barcelona",
        "psg":       "paris saint-germain",
    }
})

# "Barca" normalises to "fc barcelona" before comparison
is_match, score = engine.is_similar("Barca", "FC Barcelona")
# True, 100.0
```

#### Acronyms — substring replacements

```python
engine = SimilarityEngine({
    "threshold": 70,
    "acronyms": {
        "fc":  "football club",
        "utd": "united",
        "afc": "athletic football club",
    }
})

is_match, score = engine.is_similar("Liverpool FC", "Liverpool Football Club")
# True, 100.0
```

#### Diacritic stripping

Diacritics are stripped automatically before comparison — no config needed:

```python
is_match, score = engine.is_similar("Müller", "Muller")       # True
is_match, score = engine.is_similar("Résumé", "Resume")       # True
is_match, score = engine.is_similar("Târgu Mureș", "Targu Mures")  # True
```

#### Tuning for different domains

```python
# Strict mode — order matters, used for exact title matching
strict = SimilarityEngine({
    "threshold": 90,
    "weights": {"token": 0.0, "substr": 0.0, "phonetic": 0.0, "ratio": 1.0}
})

# Lenient mode — good for noisy scraped data
lenient = SimilarityEngine({
    "threshold": 55,
    "weights": {"token": 0.7, "substr": 0.2, "phonetic": 0.1, "ratio": 0.0}
})
```

---

### `storage` — SQLite Storage

Two classes: `BaseStorageManager` for general SQL operations, and `BufferedStorageManager` which adds an in-memory pandas buffer for high-speed deduplication lookups.

#### Subclass BaseStorageManager to define your schema

```python
from scrape_kit import BaseStorageManager

class ArticleDB(BaseStorageManager):
    def _create_tables(self):
        with self.db_lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    url     TEXT NOT NULL UNIQUE,
                    title   TEXT,
                    body    TEXT,
                    scraped INTEGER DEFAULT 0
                )
            """)
            self.conn.commit()

db = ArticleDB("articles.db")
```

#### Insert and query

```python
db.insert("articles", {"url": "https://example.com/1", "title": "Hello"})

rows = db.fetch_rows("SELECT * FROM articles WHERE scraped = ?", (0,))
for row in rows:
    print(row["url"], row["title"])

df = db.fetch_dataframe("SELECT * FROM articles")
print(df.head())
```

#### Batch insert

```python
params = [
    ("https://site.com/1", "Title 1"),
    ("https://site.com/2", "Title 2"),
    ("https://site.com/3", "Title 3"),
]
db.execute_batch(
    "INSERT INTO articles (url, title) VALUES (?, ?)",
    params
)
```

#### Check existence

```python
if not db.exists("articles", "url", "https://example.com/1"):
    db.insert("articles", {"url": "https://example.com/1", "title": "New"})
```

#### Indexes

```python
db.create_index("articles", ["url"], unique=True)
db.create_index("articles", ["scraped"])
```

#### Merge multiple database chunks

Useful when scraping in parallel across processes — each worker writes its own `.db` file, then you merge:

```python
# SQL-level bulk merge (fast, no per-row logic)
db.merge_databases("./chunks/", "articles")

# Row-by-row merge (when you need similarity checks or dedup logic per row)
def process_row(row):
    if not db.exists("articles", "url", row["url"]):
        db.insert("articles", dict(row))

db.merge_row_by_row("./chunks/", "articles", row_callback=process_row)
```

#### BufferedStorageManager — high-speed in-memory lookups

Loads the entire table into a pandas DataFrame on first access. `exists()` and `insert()` operate on the buffer — no SQL round-trips. Flush to disk when ready.

```python
from scrape_kit import BufferedStorageManager

db = BufferedStorageManager("matches.db", table_name="matches")

# Fast in-memory check — no SQL
if not db.exists("match_id", 12345):
    db.insert({"match_id": 12345, "home": "Arsenal", "away": "Chelsea"})

# Write buffer to disk
db.flush()
db.close()
```

---

### `settings` — YAML Configuration

`SettingsManager` recursively loads all `.yaml` files from a directory tree into a nested dictionary. Writes are atomic (temp file + `os.replace`).

#### Directory structure

```
config/
├── app.yaml
├── db.yaml
└── scrapers/
    ├── site1.yaml
    └── site2.yaml
```

#### Load and read

```python
from scrape_kit import SettingsManager

settings = SettingsManager("config")

# Full path lookup
db_host = settings.get("config", "db", "host")

# Shortcut — depth-first search by leaf key
# (useful when you don't know the full path)
timeout = settings.get("timeout")
```

#### Write (atomic)

```python
settings.write("config/scrapers", "site3", {
    "url": "https://site3.com",
    "rate_limit": 2,
    "retry_on": ["just a moment"],
})
```

#### Delete

```python
settings.delete("config/scrapers", "site3")
```

---

### `errors` — Exception Hierarchy

```
ScrapeKitError          base for all scrape-kit exceptions
├── FetcherError        fetch() failed after all retries / escalation crashed
├── StorageError        SQLite operation or buffer flush failed
└── SettingsError       YAML file missing, malformed, or unreadable
```

#### Usage

```python
from scrape_kit import FetcherError, StorageError, SettingsError, ScrapeKitError

try:
    html = fetcher.fetch(url)
except FetcherError as e:
    log.error("Fetch failed: %s", e)

try:
    db.flush()
except StorageError as e:
    log.error("Storage error: %s", e)

# Catch all scrape-kit errors at once
try:
    ...
except ScrapeKitError as e:
    log.error("scrape-kit error: %s", e)
```

---

## Releasing a new version

```bash
# Tag the release
git tag v0.2.0
git push origin v0.2.0

# Update a dependent project
pip install "git+https://github.com/yourusername/scrape-kit.git@v0.2.0"
```

---

## Running tests

```bash
pip install -e .
pip install pytest pytest-cov pytest-xdist
scrapling install
pytest
```
