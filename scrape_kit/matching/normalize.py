"""Pure name-normalisation helpers used by SimilarityEngine.

Standalone functions so the engine class stays small: diacritic stripping,
synonym expansion, positional acronym stripping, and strong-token
classification.  The engine wires these behind per-instance caches
(see ``scrape_kit.matching.engine``).
"""

import re
import unicodedata
from collections.abc import Mapping


def replace_acronym(name: str, key: str, replacement: str) -> str:
    """Apply one acronym substitution respecting word boundaries.

    Keys with leading/trailing spaces encode positional intent:
      ``"fc "``  → strip only at the *start* of the string
      ``" fc"``  → strip only at the *end*
      ``" de "`` → strip only when surrounded by spaces (mid-word safe)

    Args:
        name: Already-lowercased, whitespace-collapsed input name.
        key: Acronym key, possibly carrying positional space markers.
        replacement: Text to substitute in place of the matched key.

    Returns:
        The name with this acronym rule applied to all its matches.
    """
    token = " ".join(key.split()).lower()
    if not token:
        return name

    repl = f" {replacement.strip()} " if replacement.strip() else " "

    if key.startswith(" ") and key.endswith(" "):
        # Interior word – must be surrounded by non-word boundaries
        pattern = rf"(?<!\S){re.escape(token)}(?!\S)"
    elif key.startswith(" "):
        # Suffix
        pattern = rf"(?<!\S){re.escape(token)}$"
    elif key.endswith(" "):
        # Prefix
        pattern = rf"^{re.escape(token)}(?!\S)"
    elif re.search(r"\W$", key):
        pattern = rf"^{re.escape(token)}"
    elif re.search(r"^\W", key):
        pattern = rf"{re.escape(token)}$"
    else:
        pattern = rf"(?<!\w){re.escape(token)}(?!\w)"

    return re.sub(pattern, repl, name)


def normalize(raw: str, synonyms: Mapping[str, str], acronyms: Mapping[str, str]) -> str:
    """Normalise a raw team/entity name for matching.

    Steps
    -----
    1. Unicode NFD decomposition → strip combining diacritics (accents).
    2. Remove punctuation noise; collapse whitespace; lowercase.
    3. Synonym pass 1 – pre-acronym canonical expansions take priority so that
       e.g. ``"man utd"`` → "manchester united" before any acronym rule fires.
    4. Acronym stripping – removes organisational prefixes/suffixes.
    5. Synonym pass 2 – a stripped name may now equal a synonym key
       (e.g. ``"ac milan"`` → "milan" … unlikely but guarded).

    Args:
        raw: Raw string to normalise.
        synonyms: Canonical expansions applied on exact whole-string matches.
        acronyms: Positional acronym stripping rules.

    Returns:
        The normalised form of ``raw``.
    """
    # 1. Strip diacritics
    name = unicodedata.normalize("NFD", raw)
    name = "".join(ch for ch in name if unicodedata.category(ch) != "Mn")

    # 2. Punctuation / whitespace cleanup
    name = re.sub(r"[(),.`'\-]+", " ", name)
    name = " ".join(name.split()).lower()

    # 3. Synonym pass 1
    resolved = synonyms.get(name)
    if resolved is not None:
        name = " ".join(str(resolved).lower().split())
        return name

    # 4. Acronym stripping
    for k, v in acronyms.items():
        name = replace_acronym(name, k, v)
    name = " ".join(name.split())

    # 5. Synonym pass 2
    resolved = synonyms.get(name)
    if resolved is not None:
        name = " ".join(str(resolved).lower().split())

    return name


def strong_tokens(s: str, weak_tokens: frozenset[str]) -> frozenset[str]:
    """Return the subset of tokens that are NOT in ``weak_tokens``.

    Args:
        s: Already-normalised, whitespace-separated string.
        weak_tokens: Geographic/franchise filler words (lowercase).

    Returns:
        Frozenset of discriminative (strong) tokens.
    """
    return frozenset(t for t in s.split() if t not in weak_tokens)
