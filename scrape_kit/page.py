from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bs4 import BeautifulSoup, Tag

from .errors import ParserError


class Page:
    def __init__(self, soup: BeautifulSoup | Tag, raw_html: str | None = None):
        self._soup = soup
        self._raw_html = raw_html

    @classmethod
    def from_html(cls, html: str) -> "Page":
        try:
            # Use lxml for speed, fallback to html.parser if needed
            soup = BeautifulSoup(html, "lxml")
            return cls(soup, html)  # Store original HTML
        except Exception as e:
            raise ParserError(f"Failed to parse HTML: {e}") from e

    def find(self, selector: str) -> Element | None:
        """Find a single element. Returns None if not found."""
        tag = self._soup.select_one(selector)
        return Element(tag) if tag else None

    def find_by_id(self, id: str) -> Element | None:
        """Find an element by its ID. Returns None if not found."""
        tag = self._soup.find(id=id)
        return Element(tag) if isinstance(tag, Tag) else None

    def find_by_text(self, selector: str, text: str, exact: bool = False) -> Element | None:
        """Find an element by text content within a selector."""
        tags = self._soup.select(selector)
        for tag in tags:
            tag_text = tag.get_text(strip=True)
            if exact:
                if tag_text == text:
                    return Element(tag)
            else:
                if text in tag_text:
                    return Element(tag)
        return None

    def select(self, selector: str) -> list[Element]:
        """Find all elements matching selector."""
        return [Element(tag) for tag in self._soup.select(selector)]

    def text(self, selector: str, default: str = "") -> str:
        """Extract text from first matching element."""
        el = self.find(selector)
        return el.text() if el else default

    def texts(self, selector: str) -> list[str]:
        """Extract text from all matching elements."""
        return [el.text() for el in self.select(selector)]

    def attr(self, selector: str, attribute: str, default: str | None = None) -> str | None:
        """Extract attribute from first matching element."""
        el = self.find(selector)
        return el.attr(attribute, default) if el else default

    def attrs(self, selector: str, attribute: str) -> list[str]:
        """Extract attribute from all matching elements."""
        results = []
        for el in self.select(selector):
            val = el.attr(attribute)
            if val is not None:
                results.append(val)
        return results

    def link(self, selector: str = "a", default: str | None = None) -> str | None:
        """Extract href from first matching anchor."""
        el = self.find(selector)
        return el.link(default) if el else default

    def links(self, selector: str = "a") -> list[str]:
        """Extract hrefs from all matching anchors."""
        results = []
        for el in self.select(selector):
            val = el.link()
            if val is not None:
                results.append(val)
        return results

    def has(self, selector: str) -> bool:
        """Check if any element matches selector."""
        return self._soup.select_one(selector) is not None

    def contains(self, text: str) -> bool:
        """Check if text exists anywhere in the page."""
        return text in self._soup.get_text()

    def walk(self) -> Iterator[Element]:
        """Iterate over all elements in the page."""
        for tag in self._soup.find_all(True):
            yield Element(tag)

    def within(self, selector: str) -> Page | None:
        """Return a new Page scoped to the first matching element."""
        tag = self._soup.select_one(selector)
        return Page(tag) if tag else None

    @property
    def raw_html(self) -> str:
        """Get raw HTML string."""
        # Return original HTML if available, otherwise serialize soup
        if self._raw_html is not None:
            return self._raw_html
        return str(self._soup)

    @property
    def text_content(self) -> str:
        """Get all text content of the page, stripped."""
        return self._soup.get_text(strip=True, separator=" ")


class Element:
    def __init__(self, tag: Tag | None):
        self._tag = tag

    def find(self, selector: str) -> Element:
        """Find a single element relative to this one. Returns null-sentinel if not found."""
        if not self._tag:
            return Element(None)
        tag = self._tag.select_one(selector)
        return Element(tag)

    def select(self, selector: str) -> list[Element]:
        """Find all elements matching selector relative to this one."""
        if not self._tag:
            return []
        return [Element(tag) for tag in self._tag.select(selector)]

    def text(self, selector: str | None = None, default: str = "") -> str:
        """Extract text from this element or a child matching selector."""
        if not self._tag:
            return default
        if selector:
            target = self.find(selector)
            return target.text() if target else default
        return self._tag.get_text(strip=True)

    def attr(self, attribute: str, default: str | None = None) -> str | None:
        """Extract attribute from this element."""
        if not self._tag:
            return default
        val = self._tag.get(attribute, default)
        if isinstance(val, list):
            return " ".join(val)
        return val

    def link(self, default: str | None = None) -> str | None:
        """Shorthand for attr('href')."""
        return self.attr("href", default)

    def parent(self) -> Element | None:
        """Get parent element."""
        if not self._tag:
            return None
        p = self._tag.parent
        return Element(p) if isinstance(p, Tag) else None

    def children(self) -> list[Element]:
        """Get immediate child elements."""
        if not self._tag:
            return []
        return [Element(c) for c in self._tag.children if isinstance(c, Tag)]

    def next_sibling(self) -> Element | None:
        """Get next sibling element."""
        if not self._tag:
            return None
        ns = self._tag.find_next_sibling()
        return Element(ns) if ns else None

    def prev_sibling(self) -> Element | None:
        """Get previous sibling element."""
        if not self._tag:
            return None
        ps = self._tag.find_previous_sibling()
        return Element(ps) if ps else None

    def as_page(self) -> Page:
        """Convert this element into a scoped Page."""
        if not self._tag:
            raise ParserError("Cannot convert null element to page")
        return Page(self._tag)

    @property
    def tag(self) -> str:
        """Get tag name."""
        return self._tag.name if self._tag else ""

    @property
    def classes(self) -> list[str]:
        """Get list of CSS classes."""
        if not self._tag:
            return []
        c = self._tag.get("class", [])
        return c if isinstance(c, list) else [c]

    @property
    def id(self) -> str | None:
        """Get element ID."""
        return self.attr("id")

    def __bool__(self) -> bool:
        """Falsy if this is a null-sentinel element."""
        return self._tag is not None

    def __str__(self) -> str:
        """String representation returns text content."""
        return self.text()
