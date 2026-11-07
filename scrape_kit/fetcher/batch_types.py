"""Public types extracted from batch.py for the fetcher package API."""

import threading
import time
from collections.abc import Callable
from enum import Enum
from typing import TypeAlias

from ..errors import FetcherError


class _UserCodeError(Exception):
    """Carrier for exceptions raised by the retry hook or the user callback.

    Not a ``FetcherError`` subclass: the FAST and STEALTH collectors only
    aggregate ``FetcherError``, so this type propagates immediately out of
    the batch dispatch instead of being reported as a fetch failure or
    retried. The original exception stays reachable via ``__cause__``.
    """

    def __init__(self, message: str, *, url: str) -> None:
        """Initialize the carrier with the originating URL.

        Args:
            message: Human-readable description of the user-code failure.
            url: URL whose hook or callback raised the original exception.
        """
        super().__init__(message)
        self.url = url


class _EngineError(FetcherError):
    """Engine-raised failure that the retry loop may defer.

    Marks a failure produced by the engine dispatch itself rather than by the
    fetcher, so the retry pass accounting can re-run the affected URLs while
    genuine fetch failures keep their immediate-propagation behavior.
    """


RetryHook: TypeAlias = Callable[[str, str], "str | None"]


class ScrapeMode(str, Enum):
    """Batch scrape modes.

    A str-Enum: members are their plain string values, so raw strings
    ("fast"/"stealth") and ScrapeMode members are interchangeable wherever
    a mode is expected.
    """

    FAST = "fast"
    STEALTH = "stealth"


# Per-batch 429 cooldown bounds: a bounded wait can never hang a run,
# and the cap applies to hostile or absurd Retry-After values.
_DEFAULT_COOLDOWN_SECONDS = 10.0
_MAX_COOLDOWN_SECONDS = 300.0


class _BatchCooldown:
    """Per-``scrape()``-call cooldown shared by FAST threads and STEALTH coroutines.

    Satisfies: the cooldown keeps one monotonic deadline behind one lock;
    every worker waits the remaining time before issuing its next request, the
    wait is bounded by the cap, the deadline never shrinks, and the state is
    per-call so concurrent independent batches never throttle each other.
    """

    def __init__(self, default_wait: float = _DEFAULT_COOLDOWN_SECONDS, max_wait: float = _MAX_COOLDOWN_SECONDS) -> None:
        """Initialize an empty cooldown with bounded waits.

        Args:
            default_wait: Delay applied when a 429 advertises no usable delay.
            max_wait: Hard cap for any registered deadline.
        """
        self._default_wait = default_wait
        self._max_wait = max_wait
        self._lock = threading.Lock()
        self._deadline = 0.0

    def register(self, now: float, retry_after: float | None = None) -> None:
        """Register a 429 cooldown deadline, never shortening an existing one.

        Satisfies: this method uses the advertised Retry-After when
        present, the bounded default otherwise, and caps the result.

        Args:
            now: Current monotonic time in seconds.
            retry_after: The bounded advertised delay, or None for the default.
        """
        wait = self._default_wait if retry_after is None else retry_after
        deadline = now + min(max(0.0, wait), self._max_wait)
        with self._lock:
            self._deadline = max(self._deadline, deadline)

    def remaining(self) -> float:
        """Return the remaining cooldown time in seconds.

        Returns:
            Seconds left before the deadline, or 0.0 when no cooldown is active.
        """
        with self._lock:
            return max(0.0, self._deadline - time.monotonic())

    def wait_if_needed(self) -> None:
        """Sleep out the remaining cooldown on the calling FAST worker thread."""
        remaining = self.remaining()
        if remaining > 0:
            time.sleep(remaining)

    async def wait_if_needed_async(self) -> None:
        """Await the remaining cooldown on a STEALTH coroutine."""
        remaining = self.remaining()
        if remaining > 0:
            import asyncio

            await asyncio.sleep(remaining)


__all__ = [
    "RetryHook",
    "ScrapeMode",
    "_UserCodeError",
    "_EngineError",
]
