"""CLI entrypoint for the quotes example (TR 4.2).

Fulfills: TR 4.2 argparse story --pages/--out/--js and the four-stage pipeline
fetch -> parse -> dedupe -> store.
"""
from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from parse_quotes import dedupe, parse_quotes  # noqa: E402
from storage_quotes import QuoteStore  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser (TR 4.2: --pages/--out/--js)."""
    parser = argparse.ArgumentParser(description='Scrape quotes.toscrape.com with scrape-kit')
    parser.add_argument('--pages', type=int, default=1, help='Number of listing pages to fetch')
    parser.add_argument('--out', default='quotes.sqlite', help='Output SQLite database path')
    parser.add_argument('--csv', default=None, help='Optional CSV export path')
    parser.add_argument('--js', action='store_true', help='Escalate to the browser channel')
    return parser


def scrape(pages: int, js: bool, out: str, csv_path: str | None) -> int:
    """Run the four-stage pipeline; returns the number of quotes stored.

    Satisfies: TR 4.2 pipeline fetch -> parse -> dedupe -> store, with the
    browser-escalation channel demo (TR 4.1: degrade-stealthy-headers signal).
    """
    import scrape_kit as sk

    # Load example settings through the public config channel (TR 4.2).
    try:
        sk.configure(str(HERE), 'scraper_config')
    except OSError as exc:
        print(f'warning: could not load scraper_config.yaml ({exc}); using library defaults')

    all_quotes: list[dict[str, Any]] = []
    for page_num in range(1, pages + 1):
        url = f'https://quotes.toscrape.com/page/{page_num}/'
        try:
            if js:
                html = sk.browser(url, network_idle=True)
            else:
                html = sk.fetch(url, stealthy_headers=True)
        except sk.ScrapeKitError as exc:
            print(f'error: fetch failed for {url}: {exc}')
            return 0
        all_quotes.extend(parse_quotes(html))

    unique = dedupe(all_quotes)
    store = QuoteStore(out)
    store.add(unique)
    store.flush()
    if csv_path:
        store.to_csv(csv_path)
    store.close()
    return len(unique)


def main(argv: list[str] | None = None) -> int:
    """Entrypoint; returns a process exit code."""
    args = build_parser().parse_args(argv)
    count = scrape(args.pages, args.js, args.out, args.csv)
    print(f'Stored {count} unique quotes in {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
