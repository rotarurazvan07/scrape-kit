import re
import unicodedata
from typing import Any

from rapidfuzz import fuzz

from .logger import get_logger

logger = get_logger(__name__)


class SimilarityEngine:
    """Encapsulates string similarity logic, primarily designed for entities/names.
    Exposes a single function `is_similar(a, b)` for external use. It normalizes inputs,
    strips diacritics, hashes them, and caches intermediate operations for high performance.
    """

    # If you want singleton behavior per-app, manage the instance from the app level.
    # Leaving out the strict __new__ singleton forces callers to instantiate properly and pass config.

    def __init__(self, cfg: dict[str, Any]) -> None:
        """
        Configuration accepts:
          acronyms: dict for generic sub-string replacement
          synonyms: exact match replacement dict (e.g. "teamA": "teamB")
          weights: token, substr, phonetic, ratio
          threshold: integer threshold for is_similar
        """
        if not cfg:
            raise ValueError("Configuration is required for SimilarityEngine")
        else:
            self.acronyms = cfg.get("acronyms", {})
            self.synonyms = cfg.get("synonyms", {})

            # weights for hybrid matching
            weights = cfg.get("weights")
            self.token_weight = weights.get("token")
            self.substr_weight = weights.get("substr")
            self.phonetic_weight = weights.get("phonetic")
            self.ratio_weight = weights.get("ratio")

            # similarity threshold
            self.similarity_threshold = cfg.get("threshold")

        # Caches
        self._norm_cache: dict[str, str] = {}
        self._soundex_cache: dict[str, str] = {}
        self._result_cache: dict[tuple[str, str], tuple[bool, float]] = {}

    def _soundex(self, name: str) -> str:
        """Compute the Soundex code for a name.

        Args:
            name: The name to compute Soundex for.

        Returns:
            A 4-character Soundex code.
        """
        if name in self._soundex_cache:
            return self._soundex_cache[name]

        orig_name = name
        name = name.upper()
        replacements = {
            "BFPV": "1",
            "CGJKQSXZ": "2",
            "DT": "3",
            "L": "4",
            "MN": "5",
            "R": "6",
        }
        if not name:
            return "0000"
        soundex_code = name[0]
        for char in name[1:]:
            for key, value in replacements.items():
                if char in key and soundex_code[-1] != value:
                    soundex_code += value
        soundex_code = soundex_code[:4].ljust(4, "0")
        res = soundex_code[:4]
        self._soundex_cache[orig_name] = res
        return res

    def _normalize(self, match_name: str) -> str:
        """Normalize a string for matching: lowercase, strip diacritics, apply synonyms/acronyms.

        Args:
            match_name: The string to normalize.

        Returns:
            The normalized string.
        """
        if match_name in self._norm_cache:
            return self._norm_cache[match_name]

        # Decompose Unicode and remove diacritics
        name = unicodedata.normalize("NFD", match_name)
        name = "".join(ch for ch in name if unicodedata.category(ch) != "Mn")
        name = re.sub(r"[(),.`]", "", name)

        name = " ".join(name.split()).lower()

        # Exact match replacements
        for k, v in self.synonyms.items():
            if name == k:
                name = v

        # Sub-string acronym replacements
        for k, v in self.acronyms.items():
            if k in name:
                name = name.replace(k, v)

        self._norm_cache[match_name] = name
        return name

    def _share_token(self, s1: str, s2: str) -> bool:
        """Check if two strings share at least one word token.

        Args:
            s1: First string.
            s2: Second string.

        Returns:
            True if they share at least one token, False otherwise.
        """
        tokens1 = set(s1.split())
        tokens2 = set(s2.split())
        return not tokens1.isdisjoint(tokens2)

    def hybrid_match(self, s1: str, s2: str) -> float:
        """Compute a hybrid similarity score between two strings.

        The score combines token set ratio, substring presence, phonetic (Soundex),
        and raw ratio using configured weights.

        Args:
            s1: First string.
            s2: Second string.

        Returns:
            A float score between 0 and 100.
        """
        # Fast path: token pre-filter
        if not self._share_token(s1, s2):
            return 0.0

        token_score = fuzz.token_set_ratio(s1, s2)
        substr_presence = any(word in s2 for word in s1.split())
        substr_score = 100 if substr_presence else 0

        soundex1 = self._soundex(s1.split()[0]) if s1.split() else "0000"
        soundex2 = self._soundex(s2.split()[0]) if s2.split() else "0000"
        phonetic_score = 100 if soundex1 == soundex2 else 0
        ratio_score = fuzz.ratio(s1, s2)

        final_score = (
            self.token_weight * token_score
            + self.substr_weight * substr_score
            + self.phonetic_weight * phonetic_score
            + self.ratio_weight * ratio_score
        )
        return final_score

    def is_similar(self, s1: str, s2: str) -> tuple[bool, float]:
        """Check if two strings are similar based on the configured threshold.

        Args:
            s1: First string.
            s2: Second string.

        Returns:
            A tuple of (is_similar, score) where is_similar is True if score
            exceeds the threshold, and score is the hybrid match score.
        """
        cache_key = tuple(sorted([s1, s2]))  # type: ignore[assignment]
        if cache_key in self._result_cache:
            return self._result_cache[cache_key]

        n1 = self._normalize(s1)
        n2 = self._normalize(s2)

        logger.debug("Matching '%s' via '%s' vs '%s' via '%s'", s1, n1, s2, n2)

        score = self.hybrid_match(n1, n2)
        res = (score > self.similarity_threshold, score)
        logger.debug("Match Result: %s | Score: %.2f", res[0], score)

        self._result_cache[cache_key] = res
        return res
