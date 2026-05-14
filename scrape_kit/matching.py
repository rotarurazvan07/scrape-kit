import re
import unicodedata
from typing import Any, Callable

from rapidfuzz import fuzz

from .logger import get_logger

logger = get_logger(__name__)


def make_similarity_fn(config: dict) -> Callable[[str, str], bool]:
    """
    Returns a plain callable (a, b) -> bool from a similarity config dict.
    Useful for passing directly to DedupConfig.similarity_fn.
    """
    engine = SimilarityEngine(config)
    return lambda a, b: engine.is_similar(a, b)[0]


class SimilarityEngine:
    """Encapsulates string similarity logic for sports team names and similar entities.

    Exposes ``is_similar(a, b)`` for external use.  Normalizes inputs (diacritic
    stripping, acronym removal, synonym expansion), then applies a hybrid scoring
    strategy with *strong-token enforcement*:

    - Tokens not in ``weak_tokens`` are considered *discriminative* (strong).
    - When BOTH sides carry strong tokens that are completely disjoint – even
      phonetically – the score is capped at ``strong_mismatch_cap`` regardless of
      how similar the weak/location tokens look.  This prevents "New York City" from
      merging with "New York Bulls" while still allowing "Manchester United" (strong =
      {united}) to merge with every source that normalises to the same string.
    - When only ONE side carries strong tokens, those tokens must appear verbatim in
      the other side's full token set; otherwise the same cap applies.
    - When NEITHER side has strong tokens (both names are purely location/generic
      words after normalization) the full fuzzy score is used unchanged.

    The cap is intentionally not zero: it preserves a small residual signal so that
    callers can inspect scores for debugging without hitting a hard wall.
    """

    def __init__(self, cfg: dict[str, Any]) -> None:
        """
        Configuration keys
        ------------------
        acronyms          : dict  – organisational prefix/suffix patterns to strip
                                    (e.g. ``"fc ": ""``).
        synonyms          : dict  – canonical expansions applied before and after
                                    acronym stripping (e.g. ``"man utd": "manchester united"``).
        weak_tokens       : list  – geographic / franchise words that alone cannot
                                    establish a positive match (e.g. ``new``, ``york``,
                                    ``inter``, ``dynamo``).
                                    Do NOT include discriminative qualifiers like
                                    ``city`` or ``united`` here – those ARE the
                                    tokens that tell two clubs apart.
        weights           : dict  – scoring weights:
                              token              (default 0.40) – best of token_set/sort ratio
                              substr             (default 0.10) – shared full-word bonus
                              phonetic           (default 0.10) – Soundex similarity across
                                                                  strong tokens
                              ratio              (default 0.30) – character-level ratio
                              partial            (default 0.10) – partial_ratio, applied only
                                                                  when the shorter string ≤ 8 chars
                              strong_mismatch_cap (default 35)  – score ceiling when strong
                                                                  tokens are present but disjoint
        threshold         : float – minimum score for ``is_similar`` to return True (default 65).
        """
        if not cfg:
            raise ValueError("Configuration is required for SimilarityEngine")

        self.acronyms: dict[str, str] = cfg.get("acronyms", {})
        self.synonyms: dict[str, str] = cfg.get("synonyms", {})
        self.weak_tokens: frozenset[str] = frozenset(str(t).lower() for t in cfg.get("weak_tokens", []))

        w = cfg.get("weights", {})
        self.token_weight: float = w.get("token", 0.40)
        self.substr_weight: float = w.get("substr", 0.10)
        self.phonetic_weight: float = w.get("phonetic", 0.10)
        self.ratio_weight: float = w.get("ratio", 0.30)
        self.partial_weight: float = w.get("partial", 0.10)
        self.strong_mismatch_cap: float = w.get("strong_mismatch_cap", 35.0)

        self.similarity_threshold: float = cfg.get("threshold", 65.0)

        self._norm_cache: dict[str, str] = {}
        self._soundex_cache: dict[str, str] = {}
        self._result_cache: dict[tuple[str, str], tuple[bool, float]] = {}

    # ------------------------------------------------------------------
    # Phonetic helpers
    # ------------------------------------------------------------------

    def _soundex(self, word: str) -> str:
        """Return the 4-character Soundex code for a single word."""
        if word in self._soundex_cache:
            return self._soundex_cache[word]

        upper = word.upper()
        if not upper:
            return "0000"

        table = {
            "BFPV": "1",
            "CGJKQSXZ": "2",
            "DT": "3",
            "L": "4",
            "MN": "5",
            "R": "6",
        }
        code = upper[0]
        for ch in upper[1:]:
            for chars, digit in table.items():
                if ch in chars and code[-1] != digit:
                    code += digit
                    break

        result = (code + "000")[:4]
        self._soundex_cache[word] = result
        return result

    def _phonetic_overlap(self, strong1: frozenset[str], strong2: frozenset[str]) -> float:
        """Fraction of strong tokens in the smaller set that have a Soundex match in the other.

        Returns a value in [0, 100].  We use the *smaller* set as the numerator so
        that a single-token abbreviation matching one of several long-form tokens
        still scores well.
        """
        if not strong1 or not strong2:
            return 0.0

        smaller, larger = (strong1, strong2) if len(strong1) <= len(strong2) else (strong2, strong1)
        matches = sum(1 for t in smaller if any(self._soundex(t) == self._soundex(u) for u in larger))
        return 100.0 * matches / len(smaller)

    # ------------------------------------------------------------------
    # Normalisation
    # ------------------------------------------------------------------

    def _normalize(self, raw: str) -> str:
        """Normalise a raw team/entity name for matching.

        Steps
        -----
        1. Unicode NFD decomposition → strip combining diacritics (accents).
        2. Remove punctuation noise; collapse whitespace; lowercase.
        3. Synonym pass 1 – pre-acronym canonical expansions take priority so that
           e.g. ``"man utd"`` → ``"manchester united"`` before any acronym rule fires.
        4. Acronym stripping – removes organisational prefixes/suffixes.
        5. Synonym pass 2 – a stripped name may now equal a synonym key
           (e.g. ``"ac milan"`` → ``"milan"`` … unlikely but guarded).
        """
        if raw in self._norm_cache:
            return self._norm_cache[raw]

        # 1. Strip diacritics
        name = unicodedata.normalize("NFD", raw)
        name = "".join(ch for ch in name if unicodedata.category(ch) != "Mn")

        # 2. Punctuation / whitespace cleanup
        name = re.sub(r"[(),.`'\-]+", " ", name)
        name = " ".join(name.split()).lower()

        # 3. Synonym pass 1
        resolved = self.synonyms.get(name)
        if resolved is not None:
            name = " ".join(str(resolved).lower().split())
            self._norm_cache[raw] = name
            return name

        # 4. Acronym stripping
        for k, v in self.acronyms.items():
            token = k.strip().lower()
            if not token:
                continue

            # Determine position based on whitespace in the key
            if k.startswith(" ") and k.endswith(" "):
                pattern = rf"\b{re.escape(token)}\b"
            elif k.startswith(" "):
                pattern = rf"\b{re.escape(token)}$"
            elif k.endswith(" "):
                pattern = rf"^{re.escape(token)}\b"
            else:
                pattern = rf"\b{re.escape(token)}\b"

            name = re.sub(pattern, v, name)

        name = " ".join(name.split())

        # 5. Synonym pass 2
        resolved = self.synonyms.get(name)
        if resolved is not None:
            name = " ".join(str(resolved).lower().split())

        self._norm_cache[raw] = name
        return name

    # ------------------------------------------------------------------
    # Token helpers
    # ------------------------------------------------------------------

    def _strong_tokens(self, s: str) -> frozenset[str]:
        """Return the subset of tokens that are NOT in ``weak_tokens``."""
        return frozenset(t for t in s.split() if t not in self.weak_tokens)

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def hybrid_match(self, s1: str, s2: str) -> float:
        """Return a 0–100 similarity score for two *already-normalised* strings.

        Scoring pipeline
        ----------------
        1. Compute four fuzzy metrics:
             - ``token_set_ratio``  – order-insensitive, rewards subset matches
             - ``token_sort_ratio`` – good for word-order inversions
             - ``ratio``            – raw character-level Levenshtein
             - ``partial_ratio``    – substring containment; applied conservatively
                                      only for short strings (≤ 8 chars on either side)
        2. Word-level shared-token bonus (``substr_score``).
        3. Phonetic score across *strong* tokens only (multi-token Soundex).
        4. Weighted sum → ``base_score``.

        Strong-token enforcement (the key guard)
        -----------------------------------------
        After computing ``base_score``, classify each side's tokens as strong
        (discriminative, not in ``weak_tokens``) or weak (geographic / franchise
        filler).

        • **Both sides have strong tokens, and they are disjoint (no shared token,
          no phonetic match)**
          → These strings almost certainly name *different* entities.
          Cap the score at ``strong_mismatch_cap``.
          Example: "new york city" vs "new york bulls"
                   strong1={city}, strong2={bulls} → disjoint → capped.

        • **One side has strong tokens, the other has none**
          → The all-weak side cannot confirm the strong tokens exist there.
          Cap unless the strong tokens appear verbatim in the other's token set.
          Example: "new york city" (strong={city}) vs "new york" (strong={})
                   "city" ∉ {"new","york"} → capped.

        • **Neither side has strong tokens**
          → Both names are purely geographic/generic after normalisation; fall
          through to the full ``base_score`` (rare, but e.g. two city-only names).

        Phonetic overlap in the cap check means a typo like "sevlla" vs "sevilla"
        (same Soundex S140) still passes even though the tokens aren't identical.
        """
        if not s1 or not s2:
            return 0.0

        tokens1: frozenset[str] = frozenset(s1.split())
        tokens2: frozenset[str] = frozenset(s2.split())
        strong1 = self._strong_tokens(s1)
        strong2 = self._strong_tokens(s2)

        # --- Fuzzy metrics ---------------------------------------------------
        tset = fuzz.token_set_ratio(s1, s2)
        tsort = fuzz.token_sort_ratio(s1, s2)
        ratio = fuzz.ratio(s1, s2)

        # Best token-based score
        best_token = float(max(tset, tsort))

        # partial_ratio helps short abbreviations/nicknames but can cause false
        # positives on longer strings, so apply it only when one side is short.
        partial_contribution = 0.0
        if min(len(s1), len(s2)) <= 8:
            partial_contribution = fuzz.partial_ratio(s1, s2) * 0.92

        # Word-level overlap (full-word shared token, not substring)
        substr_score = 100.0 if tokens1 & tokens2 else 0.0

        # Phonetic similarity across strong tokens (0–100)
        phonetic_score = self._phonetic_overlap(strong1, strong2)

        # --- Weighted combination --------------------------------------------
        base_score = (
            self.token_weight * best_token
            + self.substr_weight * substr_score
            + self.phonetic_weight * phonetic_score
            + self.ratio_weight * ratio
            + self.partial_weight * partial_contribution
        )

        # --- Strong-token enforcement ----------------------------------------
        if strong1 and strong2:
            # Both sides are discriminative; they must share at least one strong
            # token – or have a phonetic match – to avoid the cap.
            if strong1.isdisjoint(strong2) and phonetic_score == 0.0:
                logger.debug(
                    "Strong-token mismatch: %s ↔ %s  (strong: %s vs %s)",
                    s1,
                    s2,
                    strong1,
                    strong2,
                )
                return min(base_score, self.strong_mismatch_cap)

        elif strong1 and not strong2:
            # s2 is all weak; s1's strong tokens must appear verbatim in s2's
            # token set – otherwise s2 simply lacks the discriminative word.
            if not strong1.issubset(tokens2):
                logger.debug(
                    "Strong-token containment miss (s1→s2): %s ↔ %s  (strong1: %s)",
                    s1,
                    s2,
                    strong1,
                )
                return min(base_score, self.strong_mismatch_cap)

        elif not strong1 and strong2 and not strong2.issubset(tokens1):
            logger.debug(
                "Strong-token containment miss (s2→s1): %s ↔ %s  (strong2: %s)",
                s1,
                s2,
                strong2,
            )
            return min(base_score, self.strong_mismatch_cap)

        # Both empty → neither name has discriminative tokens; use full score.

        return base_score

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_similar(self, s1: str, s2: str) -> tuple[bool, float]:
        """Return ``(is_similar, score)`` for two raw strings.

        Args:
            s1: First raw string.
            s2: Second raw string.

        Returns:
            Tuple of ``(bool, float)`` – True when score > threshold.
        """
        # Canonical cache key: order-independent
        cache_key = (min(s1, s2), max(s1, s2))
        if cache_key in self._result_cache:
            return self._result_cache[cache_key]

        n1 = self._normalize(s1)
        n2 = self._normalize(s2)

        logger.debug("Matching '%s' → '%s'  vs  '%s' → '%s'", s1, n1, s2, n2)

        score = self.hybrid_match(n1, n2)
        result = (score > self.similarity_threshold, score)

        logger.debug("Result: %s | Score: %.2f", result[0], score)
        self._result_cache[cache_key] = result
        return result
