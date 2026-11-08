# examples/quotes

Runnable quote-scraping example for scrape-kit (public facade only).

It fetches [quotes.toscrape.com](https://quotes.toscrape.com/) pages with the
fetch/web channels, parses them with `scrape_kit.Selector`, deduplicates with
`scrape_kit.SimilarityEngine`, and persists results with
`scrape_kit.BufferedStorageManager`.

## Layout

| File | Purpose | TR reference |
| --- | --- | --- |
| `scraper_config.yaml` | Declarative settings loaded through `sk.configure()` | TR §4.2 |
| `scrape_quotes.py` | argparse CLI entrypoint (fetch -> parse -> store) | TR §4.2 |
| `parse_quotes.py` | Quote extraction and similarity dedup | TR §4.1 |
| `storage_quotes.py` | Buffered CSV/SQLite persistence | TR §4.1 |
| `run_smoke_test.py` | Offline deterministic smoke test (no network) | TR §4.2 |
| `fixtures/page_1.html` | Local copy of page 1 markup | TR §4.2 |
| `fixtures/expected_quotes.csv` | Golden output used by the smoke test | TR §4.2 |

## Run against the live site

```bash
python examples/quotes/scrape_quotes.py --pages 2 --out quotes.sqlite --csv quotes.csv
```

Flags: `--pages N` (how many listing pages to fetch, default 1), `--out PATH`
(SQLite output path, default `quotes.sqlite`), `--csv PATH` (optional CSV export
of the stored rows), `--js` (escalate to the browser channel instead of plain
HTTP fetch). Settings come from `scraper_config.yaml` via `scrape_kit.configure()`.

## Run offline (no network required)

```bash
python examples/quotes/run_smoke_test.py
```

All calls go through the scrape-kit facade (`from scrape_kit import ...`), so the
example exercises the same public API consumers use.
