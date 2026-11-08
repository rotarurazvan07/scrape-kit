"""Offline deterministic smoke test for the quotes example (TR 4.2).

Purpose: prove the fixture -> parse -> dedupe -> store path with zero network
access, so CI never touches quotes.toscrape.com.
"""
from __future__ import annotations

import csv
import pathlib
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

FIXTURE = HERE / 'fixtures' / 'page_1.html'
EXPECTED = HERE / 'fixtures' / 'expected_quotes.csv'


def main() -> int:
    """Run the offline pipeline and diff it against the golden CSV."""
    from parse_quotes import dedupe, parse_quotes
    from storage_quotes import QuoteStore

    html = FIXTURE.read_text(encoding='utf-8')
    quotes = dedupe(parse_quotes(html))
    if not quotes:
        print('FAIL: no quotes parsed from fixture')
        return 1

    with tempfile.TemporaryDirectory() as tmp:
        store = QuoteStore(str(pathlib.Path(tmp) / 'quotes.sqlite'))
        store.add(quotes)
        store.flush()
        rows = store.to_csv(str(pathlib.Path(tmp) / 'quotes.csv'))
        store.close()

    assert rows == len(quotes), f'csv rows {rows} != parsed {len(quotes)}'

    if not EXPECTED.exists():
        print(f'PASS (no golden file yet): parsed {len(quotes)} quotes')
        return 0

    expected = list(csv.DictReader(EXPECTED.read_text(encoding='utf-8').splitlines()))
    if len(expected) != len(quotes):
        print(f'FAIL: expected {len(expected)} rows, got {len(quotes)}')
        return 1
    for index, (want, got) in enumerate(zip(expected, quotes)):
        if (want['text'], want['author'], want['tags']) != (got['text'], got['author'], got['tags']):
            print(f'FAIL: row {index} mismatch')
            print(f'  expected: text={want["text"]!r} author={want["author"]!r} tags={want["tags"]!r}')
            print(f'  got:      text={got["text"]!r} author={got["author"]!r} tags={got["tags"]!r}')
            return 1
    print(f'PASS: {len(quotes)} quotes match golden fixture field-by-field')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
