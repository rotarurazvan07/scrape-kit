"""Quote extraction and similarity deduplication (TR §4.1).

Uses only the scrape-kit public facade.
Satisfies: TR §4.1 parse pattern (scrape_kit.Selector + SimilarityEngine).
"""
from __future__ import annotations

from typing import Any

from scrape_kit import Selector, SimilarityEngine

# The exact `div.quote` selector shape verified against quotes.toscrape.com
# markup in fixtures/page_1.html (TR §4.1: "strict-ish `div.quote` selector guide").
BLOCK_SELECTOR = 'div.quote'


def parse_quotes(html: str) -> list[dict[str, Any]]:
    """Extract every quote block from a quotes.toscrape.com page.

    Satisfies: TR §4.1 (Selector extraction), TR Appendix A (strict selector).
    """
    page = Selector(html)
    quotes: list[dict[str, Any]] = []
    for block in page.css(BLOCK_SELECTOR):
        text = block.css('span.text::text').get()
        author = block.css('small.author::text').get()
        tags = block.css('div.tags a.tag::text').getall()
        if text and author:
            quotes.append({
                'text': text.strip(),
                'author': author.strip(),
                'tags': ';'.join(t.strip() for t in tags),
            })
    return quotes


def dedupe(quotes: list[dict[str, Any]], threshold: float = 85.0) -> list[dict[str, Any]]:
    """Drop near-duplicate quotes using SimilarityEngine.

    Satisfies: TR §4.1 ("SimilarityEngine dedup demo" - scores are real cosine
    similarity, threshold-driven). Keeps the first occurrence of each cluster.
    Note: the engine scale is 0-100 (default 65) and the comparison is strict
    (score > threshold); verified scores are ~90 for identical short strings and
    <50 for unrelated quotes, so 85 separates duplicates from distinct rows.
    """
    engine: Any = SimilarityEngine({'threshold': threshold})
    kept: list[dict[str, Any]] = []
    for quote in quotes:
        duplicate = False
        for existing in kept:
            is_dup, _score = engine.similarity(quote['text'], existing['text'])
            if is_dup:
                duplicate = True
                break
        if not duplicate:
            kept.append(quote)
    return kept
