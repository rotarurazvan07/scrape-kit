"""Link following and extraction helpers."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from ..errors import FetcherError

_logger = logging.getLogger(__name__)


def _to_response(
    response_or_html: Union[Any, str, bytes],
    base_url: Optional[str],
) -> Any:
    """Return a scrapling Response, building one from raw HTML when needed.

    Note: when a Response is synthesized from raw HTML, metadata (status,
    reason, cookies, headers, request headers) is filled with defaults. The
    original response metadata is not available in that case; pass a real
    scrapling Response to preserve it.

    Args:
        response_or_html: A scrapling Response object, or raw HTML string/bytes.
        base_url: Base URL for the synthesized response when raw HTML is given.

    Returns:
        The original response object, or a synthesized scrapling Response.

    Raises:
        FetcherError: If raw HTML is passed without a base_url.
    """
    if isinstance(response_or_html, (str, bytes)):
        if base_url is None:
            raise FetcherError("base_url required when passing raw HTML", url="")
        from scrapling.engines.toolbelt.custom import Response as ScraplingResponse

        content = response_or_html if isinstance(response_or_html, str) else response_or_html.decode("utf-8", "replace")
        return ScraplingResponse(
            url=base_url,
            content=content,
            status=200,
            reason="OK",
            cookies={},
            headers={},
            request_headers={},
            encoding="utf-8",
            method="GET",
        )
    return response_or_html


def _dispatch_selector(
    sel: Any,
    css_or_xpath: str,
    selector_type: str = "auto",
) -> Any:
    """Dispatch to css or xpath based on selector_type.

    Args:
        sel: scrapling Selector instance.
        css_or_xpath: CSS or XPath selector string.
        selector_type: "css", "xpath", or "auto" (default: auto-detect).

    Returns:
        Selectors result from scrapling.

    Raises:
        ValueError: If selector_type is invalid or the selector is empty.
    """
    if not css_or_xpath:
        raise ValueError("selector must not be empty")
    if selector_type == "css":
        return sel.css(css_or_xpath)
    if selector_type == "xpath":
        return sel.xpath(css_or_xpath)
    if selector_type == "auto":
        # Heuristic: // (or / not followed by *) is XPath, otherwise CSS.
        # An explicit selector_type avoids the heuristic entirely.
        if css_or_xpath.startswith("//") or (css_or_xpath.startswith("/") and not css_or_xpath.startswith("/*")):
            return sel.xpath(css_or_xpath)
        return sel.css(css_or_xpath)
    raise ValueError(f"selector_type must be 'css', 'xpath', or 'auto', got '{selector_type}'")


def _validate_link_args(css_or_xpath: str, selector_type: str) -> None:
    """Validate common link extraction arguments."""
    if not css_or_xpath:
        raise ValueError("selector must not be empty")
    if selector_type not in ("css", "xpath", "auto"):
        raise ValueError(f"selector_type must be 'css', 'xpath', or 'auto', got '{selector_type}'")


def _create_follow_selector(
    response_or_html: Union[Any, str, bytes],
    base_url: Optional[str],
) -> tuple[Any, str, Any]:
    """Create scrapling Selector and base URL for link extraction.

    Returns:
        Tuple of (selector, base_url, response).
    """
    from scrapling.parser import Selector as ScraplingSelector

    response = _to_response(response_or_html, base_url)
    base = str(base_url or getattr(response, "url", "") or "")
    sel = ScraplingSelector(content=response.body, url=base)
    return sel, base, response


def _extract_href_from_element(el: Any) -> Optional[str]:
    """Extract href attribute from an element, returning None on failure."""
    try:
        return el.attrib.get("href")  # type: ignore[no-any-return]
    except Exception:
        return None


def follow_links(
    response_or_html: Union[Any, str, bytes],
    css_or_xpath: str,
    base_url: Optional[str] = None,
    selector_type: str = "auto",
) -> List[str]:
    """Extract and normalize links from a response or HTML using CSS/XPath.

    Returns absolute URLs ready for fetching.

    Args:
        response_or_html: A scrapling Response object, or raw HTML string/bytes.
        css_or_xpath: CSS or XPath selector to extract link elements.
        base_url: Base URL for resolving relative links (required if passing HTML).
        selector_type: "css", "xpath", or "auto" (default).

    Returns:
        List of absolute URLs extracted from the matched elements.

    Raises:
        FetcherError: If link extraction fails.
        ValueError: If selector_type is invalid or the selector is empty.
    """
    _validate_link_args(css_or_xpath, selector_type)

    try:
        sel, base, response = _create_follow_selector(response_or_html, base_url)
        elements = _dispatch_selector(sel, css_or_xpath, selector_type)

        urls: List[str] = []
        for el in elements:
            href = _extract_href_from_element(el)
            if href:
                absolute = response.urljoin(href)
                if absolute:
                    urls.append(absolute)
        return urls
    except FetcherError:
        raise
    except Exception as e:
        raise FetcherError(f"link extraction failed: {css_or_xpath}", url=base_url or "") from e


def _build_link_dict(
    response: Any,
    el: Any,
    href: str,
) -> Dict[str, Any]:
    """Build a link metadata dict from an element."""
    return {
        "url": response.urljoin(href),
        "text": el.text.strip(),
        "attributes": dict(el.attrib),
    }


def extract_links(
    response_or_html: Union[Any, str, bytes],
    css_or_xpath: str,
    base_url: Optional[str] = None,
    selector_type: str = "auto",
) -> List[Dict[str, Any]]:
    """Extract links with metadata (url, text, attributes).

    Args:
        response_or_html: A scrapling Response object, or raw HTML string/bytes.
        css_or_xpath: CSS or XPath selector to extract link elements.
        base_url: Base URL for resolving relative links (required if passing HTML).
        selector_type: "css", "xpath", or "auto" (default).

    Returns:
        List of dicts with keys: ``url``, ``text``, and ``attributes``.

    Raises:
        FetcherError: If link extraction fails.
        ValueError: If selector_type is invalid or the selector is empty.
    """
    _validate_link_args(css_or_xpath, selector_type)

    try:
        sel, base, response = _create_follow_selector(response_or_html, base_url)
        elements = _dispatch_selector(sel, css_or_xpath, selector_type)

        links: List[Dict[str, Any]] = []
        failed_count = 0
        for el in elements:
            try:
                href = _extract_href_from_element(el)
                if href:
                    links.append(_build_link_dict(response, el, href))
            except Exception:
                failed_count += 1
                continue
        if failed_count > 0:
            _logger.warning("extract_links: %d element(s) failed to process and were skipped", failed_count)
        return links
    except FetcherError:
        raise
    except Exception as e:
        raise FetcherError(f"link extraction failed: {css_or_xpath}", url=base_url or "") from e
