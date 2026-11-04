"""Adaptive element relocation helpers."""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from .selector import Selectors

from ..errors import FetcherError


def _validate_relocate_args(
    url: str,
    identifier: str,
    percentage: int,
) -> None:
    """Validate relocate function arguments."""
    if not url:
        raise ValueError("url must not be empty")
    if not identifier:
        raise ValueError("identifier must not be empty")
    if not 0 <= percentage <= 100:
        raise ValueError("percentage must be between 0 and 100")


def _create_adaptive_selector(
    html: Union[str, bytes],
    url: str,
    adaptive_domain: Optional[str],
    storage_args: Optional[Dict[str, Any]],
) -> Any:
    """Create a scrapling Selector with adaptive configuration."""
    from scrapling.parser import Selector as ScraplingSelector

    return ScraplingSelector(
        content=html,
        url=url,
        adaptive=True,
        adaptive_domain=adaptive_domain or url,
        storage_args=storage_args or {},
    )


def _perform_relocation(
    sel: Any,
    identifier: str,
    percentage: int,
    url: str,
) -> Selectors:
    """Perform the actual relocation and wrap results."""
    stored = sel.retrieve(identifier)
    if stored is None:
        return Selectors([], url)
    relocated = sel.relocate(stored, percentage=percentage)
    if relocated is None or (hasattr(relocated, "__len__") and len(relocated) == 0):
        return Selectors([], url)
    return Selectors(relocated, url)


def adaptive_select(
    html: Union[str, bytes],
    url: str,
    selector: str,
    auto_save: bool = False,
    adaptive_domain: Optional[str] = None,
    storage_args: Optional[Dict[str, Any]] = None,
) -> Selectors:
    """Run a selector with adaptive relocation enabled.

    When ``auto_save`` is True, the matched element fingerprints are stored
    for future relocation. The ``adaptive_domain`` isolates storage per site.

    Args:
        html: HTML string or bytes to parse.
        url: Base URL for the document (used for adaptive storage key).
        selector: CSS selector to match.
        auto_save: Whether to persist matched element fingerprints.
        adaptive_domain: Domain key for adaptive storage isolation.
        storage_args: Optional arguments passed to the underlying SQLite storage.

    Returns:
        Selectors of matched elements.

    Raises:
        FetcherError: If parsing or selection fails.
        ValueError: If url or selector is empty.
    """
    if not url:
        raise ValueError("url must not be empty")
    if not selector:
        raise ValueError("selector must not be empty")

    try:
        from scrapling.parser import Selector as ScraplingSelector

        sel = ScraplingSelector(
            content=html,
            url=url,
            adaptive=True,
            adaptive_domain=adaptive_domain or url,
            storage_args=storage_args or {},
        )
        result = sel.css(selector, adaptive=True, auto_save=auto_save)
        return Selectors(result, url)
    except Exception as e:
        raise FetcherError(f"adaptive selection failed: {selector}", url=url) from e


def relocate(
    html: Union[str, bytes],
    url: str,
    identifier: str,
    adaptive_domain: Optional[str] = None,
    storage_args: Optional[Dict[str, Any]] = None,
    percentage: int = 40,
) -> Selectors:
    """Relocate a previously saved element on a new page version.

    Uses scrapling's adaptive relocation to find the element that corresponds
    to the saved fingerprint identified by ``identifier``.

    Args:
        html: New HTML string or bytes to search in.
        url: Base URL of the new document.
        identifier: Identifier used when the element was originally saved.
        adaptive_domain: Domain key for adaptive storage isolation.
        storage_args: Optional arguments passed to the underlying SQLite storage.
        percentage: Similarity threshold percentage (0-100) for relocation.

    Returns:
        Selectors of relocated elements (may be empty if not found).

    Raises:
        FetcherError: If relocation fails.
        ValueError: If url or identifier is empty, or percentage is out of range.
    """
    _validate_relocate_args(url, identifier, percentage)

    try:
        sel = _create_adaptive_selector(html, url, adaptive_domain, storage_args)
        return _perform_relocation(sel, identifier, percentage, url)
    except Exception as e:
        raise FetcherError(f"relocation failed for identifier: {identifier}", url=url) from e
