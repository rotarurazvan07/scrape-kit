"""Facade contract tests for the PEP 562 lazy ``scrape_kit`` package.

Pins the three-way public-API sync (``__init__``/README/tests) that the
project context requires: a bare import must stay light, every documented
public name must resolve from both import tiers, and unknown attributes
must raise ``AttributeError``.

Checks run in fresh subprocesses so import side effects are observed
exactly as a user would see them.
"""

import subprocess
import sys

EXPECTED_PUBLIC_NAMES = frozenset(
    {
        # errors
        "ScrapeKitError",
        "FetcherError",
        "StorageError",
        "SettingsError",
        "MatchingError",
        # logger
        "get_logger",
        "time_profiler",
        # settings
        "SettingsManager",
        # fetcher
        "WebFetcher",
        "InteractiveSession",
        "ScrapeMode",
        "fetch",
        "browser",
        "scrape",
        "is_blocked",
        "configure",
        "configure_defaults",
        "reset_shared",
        # storage
        "BaseStorageManager",
        "BufferedStorageManager",
        "MergeReport",
        # matching
        "SimilarityEngine",
    }
)

_FACADE_CHECK = """
import sys

import scrape_kit

# Bare import must not boot heavy dependencies or any scrape_kit submodule.
heavy = [m for m in ("scrapling", "pandas", "rapidfuzz") if m in sys.modules]
subs = [m for m in sys.modules if m.startswith(("scrape_kit.fetcher", "scrape_kit.storage", "scrape_kit.matching"))]
assert not heavy, f"bare import booted heavy deps: {heavy}"
assert not subs, f"bare import booted heavy submodules: {subs}"

# Touching a public name resolves it lazily, and both import tiers agree.
import scrape_kit.fetcher
import scrape_kit.storage

assert scrape_kit.WebFetcher is scrape_kit.fetcher.WebFetcher
assert scrape_kit.MergeReport is scrape_kit.storage.MergeReport
assert scrape_kit.fetch is scrape_kit.fetcher.fetch

for name in scrape_kit.__all__:
    assert getattr(scrape_kit, name) is not None, name

try:
    scrape_kit.not_a_real_name
except AttributeError:
    pass
else:
    raise AssertionError("unknown attribute did not raise AttributeError")

assert len(scrape_kit.__all__) == len(set(scrape_kit.__all__)), "__all__ has duplicates"
"""


def test_lazy_facade_contract_in_fresh_interpreter() -> None:
    """Bare import stays light; both import tiers resolve identically."""
    subprocess.run([sys.executable, "-c", _FACADE_CHECK], check=True)


def test_public_surface_matches_documented_contract() -> None:
    """``__all__`` pins exactly the 22 documented public names."""
    import scrape_kit

    assert set(scrape_kit.__all__) == EXPECTED_PUBLIC_NAMES
