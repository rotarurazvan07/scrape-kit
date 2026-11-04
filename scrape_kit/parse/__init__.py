"""scrape_kit.parse: thin facade over scrapling's parser.

This package exposes scrapling's Selector capabilities (CSS, XPath,
find_by_text, find_similar, find_by_regex, adaptive relocation, link
extraction) behind scrape-kit's exception boundary.

Example:
    from scrape_kit import parse

    selector = parse(html, url="https://example.com")
    prices = selector.css(".price").getall()

    # Adaptive relocation survives site redesigns
    adaptive = parse.adaptive_select(html, url, ".product", auto_save=True)

    # Link extraction (optional explicit selector_type)
    links = parse.follow_links(response, "a.product")
    xpath_links = parse.follow_links(response, "//a[@class='product']", selector_type="xpath")
"""

from __future__ import annotations

from typing import Optional, Union

from ..errors import FetcherError
from .adaptive import adaptive_select, relocate
from .follow import extract_links, follow_links
from .selector import Selector, Selectors

ParseError = FetcherError  # alias for consumer convenience

__all__ = [
    "Selector",
    "Selectors",
    "adaptive_select",
    "relocate",
    "follow_links",
    "extract_links",
    "ParseError",
]


def parse(
    html: str | bytes,
    url: str = "",
    encoding: str = "utf-8",
    adaptive: bool = False,
    adaptive_domain: str | None = None,
) -> Selector:
    """Create a Selector from HTML content.

    This is the primary entry point for the parse package.

    Args:
        html: HTML string or bytes to parse.
        url: Base URL for relative link resolution and adaptive storage.
        encoding: Encoding when ``html`` is bytes.
        adaptive: Enable scrapling adaptive element relocation.
        adaptive_domain: Domain key for adaptive storage isolation.

    Returns:
        Selector instance.

    Raises:
        FetcherError: If parsing fails.
        ValueError: If html is empty.
    """
    if not html:
        raise ValueError("html must not be empty")
    return Selector(html, url=url, encoding=encoding, adaptive=adaptive, adaptive_domain=adaptive_domain)
