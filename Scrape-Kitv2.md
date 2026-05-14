Scrape-Kit v2 — Master Plan
New File Structure
scrape_kit/
├── __init__.py         # flat, clean public surface — full rework
├── errors.py           # add ParserError, FinderError
├── fetcher.py          # reworked — fetch/scrape/is_blocked return Page
├── session.py          # EXTRACTED from fetcher — InteractiveSession alone
├── page.py             # NEW — Page + Element, wraps BS4 internally
├── finder.py           # NEW — BaseFinder generic base class
├── storage.py          # reworked — merge_with_dedup, pre/post SQL hooks
├── dedup.py            # NEW — DedupConfig dataclass + helpers
├── matching.py         # reworked — SimilarityEngine, cleaner internals
├── settings.py         # cleaned up
└── logger.py           # minor cleanup only

Module-by-Module Plan

1. page.py — NEW
Purpose: The entire BS4 dependency disappears from every finder and parser. Scrape-kit owns the HTML parsing layer. Callers never see a soup object.
Core design: Page is the top-level object created from an HTML string. Element is a node within that page. Both share the same fluent API. Element methods are always relative to that node. All methods are safe by default — no exceptions on missing elements, always return a sensible default.
Page API:
pythonclass Page:
    @classmethod
    def from_html(cls, html: str) -> "Page"

    # Single element — returns None if not found
    def find(self, selector: str) -> "Element | None"
    def find_by_id(self, id: str) -> "Element | None"
    def find_by_text(self, selector: str, text: str, exact: bool = False) -> "Element | None"

    # Lists of elements
    def select(self, selector: str) -> list["Element"]

    # Direct value extraction — safe defaults, always stripped
    def text(self, selector: str, default: str = "") -> str
    def texts(self, selector: str) -> list[str]
    def attr(self, selector: str, attribute: str, default: str | None = None) -> str | None
    def attrs(self, selector: str, attribute: str) -> list[str]
    def link(self, selector: str = "a", default: str | None = None) -> str | None
    def links(self, selector: str = "a") -> list[str]

    # Existence checks
    def has(self, selector: str) -> bool
    def contains(self, text: str) -> bool  # full page text contains substring

    # Walk all descendants
    def walk(self) -> Iterator["Element"]

    # Sub-page scoped to first matching element
    def within(self, selector: str) -> "Page | None"

    # Raw escape hatch (avoid using in finders)
    @property
    def raw_html(self) -> str
    @property
    def text_content(self) -> str  # all text stripped
Element API:
pythonclass Element:
    # Same traversal as Page, but relative to this node
    def find(self, selector: str) -> "Element | None"
    def select(self, selector: str) -> list["Element"]

    # Value extraction — selector is optional (no selector = this element itself)
    def text(self, selector: str | None = None, default: str = "") -> str
    def attr(self, attribute: str, default: str | None = None) -> str | None
    def link(self, default: str | None = None) -> str | None  # shorthand for attr("href")

    # Navigation
    def parent(self) -> "Element | None"
    def children(self) -> list["Element"]
    def next_sibling(self) -> "Element | None"
    def prev_sibling(self) -> "Element | None"

    # Convert back up
    def as_page(self) -> "Page"

    # Metadata
    @property
    def tag(self) -> str
    @property
    def classes(self) -> list[str]
    @property
    def id(self) -> str | None

    # Truthy — False if this element is a null/missing sentinel
    def __bool__(self) -> bool
    def __str__(self) -> str   # returns .text()
Key implementation detail: Element.find() and Element.select() return a null-sentinel Element (not None) that is falsy, so callers can chain safely: anchor.find(".score").text() returns "" instead of raising.
Before / After illustration:
python# BEFORE (ForebetFinder)
soup = BeautifulSoup(html, "html.parser")
all_anchors = soup.find("div", id="body-main").find_all(class_="rcnt")
for anchor in all_anchors:
    home_team = anchor.find("div", class_="tnms").find("span", class_="homeTeam").get_text()

# AFTER
for anchor in page.within("#body-main").select(".rcnt"):
    home_team = anchor.text(".tnms .homeTeam")

2. session.py — EXTRACTED + REWORKED
Purpose: Isolate InteractiveSession so fetcher.py stays focused on the fetch/scrape logic.
Changes:

InteractiveSession moves here entirely
fetch() now returns Page instead of SimpleNamespace
wait_for_settle() extracted as a standalone reusable method, callable independently
execute_script() stays but is cleaned up
All existing wait_for_* methods stay, slightly cleaned up signatures

New wait_for_settle signature:
pythondef wait_for_settle(self, idle_ms: int = 2000, hard_cap_ms: int = 10000) -> None:
    """Wait for page DOM mutations to stop. Reusable after any navigation."""
InteractiveSession.fetch() calls wait_for_settle() internally, but callers can also call it again after a click().

3. fetcher.py — REWORKED
Purpose: Keep the module-level fetch, browser, scrape, is_blocked interface. Internally cleaner. Key change: fetch() returns Page, scrape() callback receives Page.
Module-level API (unchanged ergonomics, changed return types):
pythonconfigure(config_path, config_key="scraper_config") -> None
configure_defaults() -> None

fetch(url: str, **kwargs) -> Page
is_blocked(page: Page) -> bool
browser(**kwargs) -> InteractiveSession          # context manager
scrape(urls: list[str], callback: Callable[[str, Page], None], **kwargs) -> None
Changes inside WebFetcher:

_fetch_attempt returns Page instead of str
_escalate_to_browser returns Page
is_blocked accepts Page (checks page.raw_html internally, same logic)
_check_retry_indicators works on page.raw_html
scrape() callback signature is now (url: str, page: Page) -> None

The configure() function changes: instead of returning a WebFetcher, it's -> None. No return value needed since it populates the module-level _shared instance.

4. finder.py — NEW
Purpose: Generic base class for any project's scraper. Removes all the boilerplate that is copy-pasted across every finder in bet-assistant. A BookFinder, MatchFinder, or any domain finder just extends this.
pythonclass BaseFinder(ABC):
    def __init__(
        self,
        on_result: Callable | None = None,
        *,
        skip_patterns: list[tuple[str, str]] | None = None,
        scrape_mode: str = ScrapeMode.FAST,
        max_concurrency: int = 1,
        **kwargs,  # absorbed — subclasses can add their own
    ): ...

    # ── MUST implement ───────────────────────────────────────────────────────
    @abstractmethod
    def get_urls(self) -> list[str]: ...

    @abstractmethod
    def _parse_page(self, url: str, page: Page) -> None: ...

    # ── CAN override ──────────────────────────────────────────────────────────
    def scrape(self, urls: list[str]) -> None:
        """Default: uses scrape_kit.scrape() with configured mode/concurrency."""

    # ── Framework internals (call these inside _parse_page) ───────────────────
    def add_result(self, item: Any) -> bool:
        """Pass item to on_result callback. Returns True if accepted."""

    def skip_by_patterns(self, *text_fields: str) -> str | None:
        """Check strings against skip_patterns. Returns first matching reason or None."""

    # ── Logging helpers ───────────────────────────────────────────────────────
    def log_skip(self, context: str, reason: Any = "") -> None: ...
    def log_error(self, context: str, error: Exception) -> None: ...
    def log_added(self, context: str, item: Any = "") -> None: ...
What bet-assistant's BaseMatchFinder becomes: A thin subclass of BaseFinder in bet_framework that adds match-specific concerns: date range validation, timezone normalization, num_days_ahead filtering, contributes_odds flag. The generic boilerplate (get_matches_urls → get_urls, skip patterns, callback routing) is gone because BaseFinder handles it.

5. dedup.py — NEW
Purpose: A self-contained deduplication config that BufferedStorageManager.merge_with_dedup() consumes. Completely generic — knows nothing about matches, books, or any domain.
from dataclasses import dataclass
from typing import Callable, Literal

@dataclass
class DedupConfig:
    # Core: given two row dicts, does the new row match an existing one?
    similarity_fn: Callable[[dict, dict], bool]

    # Optional: narrow the set of existing rows to compare against.
    # Takes the full list of existing rows and the new row, returns a filtered list.
    # Use this for date windows, category scoping, etc. If None, compare against all.
    candidate_filter: Callable[[list[dict], dict], list[dict]] | None = None

    # How to merge when a match is found
    merge_strategy: Literal["update_missing", "prefer_new", "prefer_existing"] \
        | Callable[[dict, dict], dict] = "update_missing"

    # Source collision guard: skip if new row shares a source with the existing row
    # Only relevant for multi-source aggregation patterns
    source_field: str | None = None      # column name containing JSON list of sources
    source_key: str = "source"           # key within each source object
merge_strategy semantics:

"update_missing" — existing row updated with any non-null fields from the new row (current MatchesManager behaviour)
"prefer_new" — new row always wins every field
"prefer_existing" — existing row never modified (dedup = just skip duplicates)
Callable[[existing, new], merged] — full custom control


6. storage.py — REWORKED
Purpose: BaseStorageManager and BufferedStorageManager stay, but merge_with_dedup is added as a first-class method. Pre/post SQL hooks added to all merge paths.
New method on BufferedStorageManager:
pythondef merge_with_dedup(
    self,
    input_dir: str,
    dedup_config: DedupConfig | None = None,
    pre_merge_sql: str | list[str] | None = None,
    post_merge_sql: str | list[str] | None = None,
    row_transform: Callable[[dict], Any] | None = None,
    read_batch_size: int = 1000,
    flush_every_rows: int = 5000,
) -> MergeReport:
pre_merge_sql / post_merge_sql: Raw SQL string or list of strings executed in a single transaction before/after the merge rows loop. Intended for things like: DELETE FROM matches WHERE datetime < date('now', '-30 days'), REINDEX, custom cleanup, computed column updates.
row_transform: Optional callable (raw_dict) -> Any. Called on each row as it comes off a chunk DB. The return value is what gets passed to the dedup logic and ultimately inserted. This is where domain objects can be constructed/destructured if needed.
merge_row_by_row stays for cases where callers want full manual row callback control. merge_with_dedup is built on top of it.
BaseStorageManager.merge_databases gets pre_merge_sql / post_merge_sql hooks too (simpler bulk-attach path, no dedup).
Cleanup: The current exists() override in BufferedStorageManager that has dual legacy call shapes gets cleaned up to a single clear signature.

7. matching.py — REWORKED
Purpose: SimilarityEngine stays but is made more composable. Add a standalone factory function so callers don't have to manage the full config just to get a similarity callable.
New additions:
pythondef make_similarity_fn(config: dict) -> Callable[[str, str], bool]:
    """
    Returns a plain callable (a, b) -> bool from a similarity config dict.
    Useful for passing directly to DedupConfig.similarity_fn.
    """
    engine = SimilarityEngine(config)
    return lambda a, b: engine.is_similar(a, b)[0]
SimilarityEngine internals: The caches stay. The hybrid_match and _normalize logic stays — it works well. The main cleanup is documentation and removing the _replace_acronym complexity in favour of cleaner regex construction.

8. settings.py — CLEANED UP
Minor: Add get_required(*keys) that raises SettingsError with a clear message instead of returning None. Simplify the depth-first fallback search — it currently silently succeeds in confusing ways. Add explicit reload() method instead of reloading on every get() call (current behaviour is surprising).

9. errors.py — MINOR ADDITION
Add ParserError (raised by Page when HTML is completely unparseable) and FinderError (raised by BaseFinder on fatal setup failures).

10. __init__.py — FULL REWORK
Flat, clean, no layers. Everything importable from scrape_kit directly:
pythonfrom scrape_kit import (
    # Configure
    configure, configure_defaults,

    # Fetch
    fetch, scrape, browser, is_blocked,

    # Page parsing
    Page, Element,

    # Finders
    BaseFinder, ScrapeMode,

    # Storage
    BaseStorageManager, BufferedStorageManager, MergeReport,

    # Deduplication
    DedupConfig,

    # Matching
    SimilarityEngine, make_similarity_fn,

    # Settings
    SettingsManager,

    # Logging
    get_logger, time_profiler,

    # Errors
    ScrapeKitError, FetcherError, StorageError, SettingsError,
    ParserError, FinderError,
)
No from scrape_kit.fetcher import .... No from scrape_kit.storage import .... One import line and you have everything.

What Phase 2 (Bet-Assistant) Will Look Like (Preview)
This is not the plan yet, just to confirm the direction is right before you start Phase 1.
Every finder drops to ~40% of current line count. No BeautifulSoup import, no soup = line, no .get_text(), no .find() chains. _parse_page gets a Page, uses CSS selectors, done.
BaseMatchFinder slims to: date validation, timezone normalization, contributes_odds guard, add_match() wrapper calling BaseFinder.add_result(). Everything else inherited.
MatchesManager drops ~60 lines. merge_databases() becomes:
pythondef merge_databases(self, chunks_dir: str) -> None:
    self.merge_with_dedup(
        chunks_dir,
        dedup_config=self._dedup_config,   # built in __init__ from similarity_config
        post_merge_sql=None,
    )
BetAssistant._parse_match_result_html uses Page methods instead of raw BS4 + regex soup.

Execution Order for Phase 1
Suggested order to avoid circular dependencies during build:

errors.py — no deps
logger.py — no deps
settings.py — depends on errors, logger
page.py — depends on errors only
matching.py — depends on logger only
dedup.py — depends on matching (for make_similarity_fn helper), no storage dep
storage.py — depends on dedup, logger, errors
session.py — depends on page, logger, errors
fetcher.py — depends on session, page, logger, errors
finder.py — depends on fetcher, page, logger, errors
__init__.py — assembles everything