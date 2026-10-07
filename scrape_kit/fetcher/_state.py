"""Leaf module holding the fetcher package's shared-instance state.

Separated from ``__init__`` so ``WebFetcher.configure*()`` can set the
shared instance with a plain module-level import (no deferred imports, no
``global`` statements). This module imports nothing from the package at
runtime, so it can never participate in an import cycle.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .web_fetcher import WebFetcher


class _SharedState:
    """Mutable holder for the package-level shared WebFetcher instance."""

    instance: "WebFetcher | None" = None


_state = _SharedState()


def _set_shared(instance: "WebFetcher") -> None:
    """Set the package-level shared WebFetcher instance.

    Args:
        instance: The WebFetcher instance to store as the shared instance.
    """
    _state.instance = instance


def reset_shared() -> None:
    """Reset the shared WebFetcher instance to None.

    Test-isolation helper: clears the shared instance so the next proxy
    call auto-creates a fresh default-configured instance.
    """
    _state.instance = None


def _peek_shared() -> "WebFetcher | None":
    """Return the shared instance, or None when not yet configured.

    Returns:
        The shared WebFetcher instance, or None.
    """
    return _state.instance
