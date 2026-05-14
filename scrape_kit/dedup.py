from dataclasses import dataclass
from typing import Any, Callable, Literal


@dataclass
class DedupConfig:
    """Configuration for deduplication during storage merge operations.

    Completely generic — knows nothing about specific domains (matches, books, etc.).
    """

    # Core: given two row dicts, does the new row match an existing one?
    similarity_fn: Callable[[dict[str, Any], dict[str, Any]], bool]

    # Optional: narrow the set of existing rows to compare against.
    # Takes the full list of existing rows and the new row, returns a filtered list.
    # Use this for date windows, category scoping, etc. If None, compare against all.
    candidate_filter: Callable[[list[dict[str, Any]], dict[str, Any]], list[dict[str, Any]]] | None = None

    # How to merge when a match is found:
    # - "update_missing": existing row updated with any non-null fields from the new row
    # - "prefer_new": new row always wins every field
    # - "prefer_existing": existing row never modified (dedup = just skip duplicates)
    # - Callable: custom merge logic (existing, new) -> merged
    merge_strategy: (
        Literal["update_missing", "prefer_new", "prefer_existing"]
        | Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
    ) = "update_missing"

    # Source collision guard: skip if new row shares a source with the existing row
    # Only relevant for multi-source aggregation patterns
    source_field: str | None = None  # column name containing JSON list of sources
    source_key: str = "source"  # key within each source object
