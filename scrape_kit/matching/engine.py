"""SimilarityEngine: hybrid fuzzy scoring, Soundex phonetics, strong-token enforcement."""

from typing import Any

from rapidfuzz import fuzz

from ..errors import MatchingError
from .normalize import normalize, strong_tokens


class SimilarityEngine:
    """Encapsulates string similarity logic for sports team names and similar entities.

    Exposes ``similarity(a, b)`` for external use.  Normalizes inputs (diacritic
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
                              token               (default 0.40) – best of token_set/sort ratio
                              shared_word         (default 0.10) – shared full-word bonus;
                                                   ``substr`` is accepted as a deprecated alias
                              phonetic            (default 0.10) – Soundex similarity across
                                                                  strong tokens
                              ratio               (default 0.30) – character-level ratio
                              partial             (default 0.10) – partial_ratio, applied only
                                                                  when the shorter string ≤ 8 chars
                              strong_mismatch_cap (default 35)  – score ceiling when strong
                                                                  tokens are present but disjoint
        threshold         : float – minimum score for ``similarity`` to return True (default 65).

        Unknown top-level or weight keys, negative weights, or a weight sum that
        deviates from 1.0 by more than 0.25 raise ``MatchingError`` at construction;
        upward drift is absorbed by the 0-100 score clamp applied in ``hybrid_match``.

        Raises:
            MatchingError: If ``cfg`` is empty, a top-level key is unknown, a weight
                key is unknown, a weight is negative, or the weight sum deviates
                from 1.0 by more than 0.25.
        """
        if not cfg:
            raise MatchingError("Configuration is required for SimilarityEngine")

        valid_top_keys = frozenset({"acronyms", "synonyms", "weak_tokens", "weights", "threshold"})
        unknown_top = set(cfg) - valid_top_keys
        if unknown_top:
            raise MatchingError(f"Unknown config key(s) {sorted(unknown_top)}; valid keys: {sorted(valid_top_keys)}")

        self.acronyms: dict[str, str] = cfg.get("acronyms", {})
        self.synonyms: dict[str, str] = cfg.get("synonyms", {})
        self.weak_tokens: frozenset[str] = frozenset(str(t).lower() for t in cfg.get("weak_tokens", []))

        w = dict(cfg.get("weights", {}))
        # 'substr' is accepted as a deprecated alias for 'shared_word' (the
        # metric always scored full-word token overlap, never substrings).
        if "shared_word" not in w and "substr" in w:
            w["shared_word"] = w.pop("substr")
        valid_keys = frozenset(
            {
                "token",
                "shared_word",
                "phonetic",
                "ratio",
                "partial",
                "strong_mismatch_cap",
            }
        )
        unknown = set(w) - valid_keys
        if unknown:
            raise MatchingError(f"Unknown weight key(s) {sorted(unknown)}; valid keys: {sorted(valid_keys)}")
        self.token_weight: float = w.get("token", 0.40)
        self.shared_word_weight: float = w.get("shared_word", 0.10)
        self.phonetic_weight: float = w.get("phonetic", 0.10)
        self.ratio_weight: float = w.get("ratio", 0.30)
        self.partial_weight: float = w.get("partial", 0.10)
        self.strong_mismatch_cap: float = w.get("strong_mismatch_cap", 35.0)

        # Weight sanity (explicit-errors philosophy): negative weights corrupt
        # individual metric contributions, and sums drifting far from 1.0
        # silently rescale every score — reject both at construction time.
        weights = (
            self.token_weight,
            self.shared_word_weight,
            self.phonetic_weight,
            self.ratio_weight,
            self.partial_weight,
        )
        if min(weights) < 0:
            raise MatchingError("Scoring weights must be non-negative")
        if abs(sum(weights) - 1.0) > 0.25:
            raise MatchingError(
                "Scoring weights must sum to approximately 1.0"
                f" (got {sum(weights):.2f}); fix the 'weights' config or omit it to use the defaults"
            )

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

    def _normalize(self, raw: str) -> str:
        """Return the cached normalised form of ``raw``.

        Thin cache wrapper around the pure ``normalize`` helper; caching semantics
        are identical to the pre-package-split single-module implementation.

        Args:
            raw: Raw string to normalise.

        Returns:
            The normalised form, cached per raw input.
        """
        if raw not in self._norm_cache:
            self._norm_cache[raw] = normalize(raw, self.synonyms, self.acronyms)
        return self._norm_cache[raw]

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
        2. Word-level shared-token bonus (``shared_word_score``).
        3. Phonetic score across *strong* tokens only (multi-token Soundex).
        4. Weighted sum → ``base_score``.
        5. Strong-token enforcement: cap the score when the sides' discriminative
           tokens cannot confirm each other (see below).

        Strong-token enforcement (the key guard)
        -----------------------------------------
        After computing ``base_score``, classify each side's tokens as strong
        (discriminative, not in ``weak_tokens``) or weak (geographic / franchise
        filler), then apply the first matching arm:

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

        Raises:
            ValueError: If either input is not a string.
        """
        if not isinstance(s1, str) or not isinstance(s2, str):
            raise ValueError(f"hybrid_match expects two strings, got {type(s1).__name__} and {type(s2).__name__}")
        if not s1 or not s2:
            return 0.0

        tokens1: frozenset[str] = frozenset(s1.split())
        tokens2: frozenset[str] = frozenset(s2.split())
        strong1 = strong_tokens(s1, self.weak_tokens)
        strong2 = strong_tokens(s2, self.weak_tokens)

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
        shared_word_score = 100.0 if tokens1 & tokens2 else 0.0

        # Phonetic similarity across strong tokens (0–100)
        phonetic_score = self._phonetic_overlap(strong1, strong2)

        # --- Weighted combination --------------------------------------------
        base_score = (
            self.token_weight * best_token
            + self.shared_word_weight * shared_word_score
            + self.phonetic_weight * phonetic_score
            + self.ratio_weight * ratio
            + self.partial_weight * partial_contribution
        )

        # Clamp to the documented 0-100 range: weight sets drifting slightly
        # above 1.0 (e.g. the documented example config) must never produce
        # scores above 100, and no metric combination can go below 0.
        base_score = max(0.0, min(100.0, base_score))

        # --- Strong-token enforcement ----------------------------------------
        # Arms are mutually exclusive (a side either has strong tokens or not);
        # _cap_score reproduces the original two-pass capping exactly.
        return self._cap_score(base_score, strong1, strong2, tokens1, tokens2, phonetic_score)

    def _cap_score(
        self,
        base_score: float,
        strong1: frozenset[str],
        strong2: frozenset[str],
        tokens1: frozenset[str],
        tokens2: frozenset[str],
        phonetic_score: float,
    ) -> float:
        """Apply strong-token enforcement caps to a computed base score.

        Arms are mutually exclusive (a side either has strong tokens or not).
        Both-strong-but-disjoint sides, or a one-sided strong set unconfirmed
        by the other's tokens, cap the score at ``strong_mismatch_cap``.
        """
        if strong1 and strong2:
            if strong1.isdisjoint(strong2) and phonetic_score == 0.0:
                return min(base_score, self.strong_mismatch_cap)
        elif strong1 and not strong1.issubset(tokens2) or strong2 and not strong2.issubset(tokens1):
            return min(base_score, self.strong_mismatch_cap)
        return base_score

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def similarity(self, s1: str, s2: str) -> tuple[bool, float]:
        """Return ``(is_match, score)`` for two raw strings.

        Args:
            s1: First raw string.
            s2: Second raw string.

        Returns:
            Tuple of ``(bool, float)`` – True when score > threshold.

        Raises:
            ValueError: If either input is not a string.
        """
        if not isinstance(s1, str) or not isinstance(s2, str):
            raise ValueError(f"similarity expects two strings, got {type(s1).__name__} and {type(s2).__name__}")
        # Canonical cache key: order-independent
        cache_key = (min(s1, s2), max(s1, s2))
        if cache_key in self._result_cache:
            return self._result_cache[cache_key]

        score = self.hybrid_match(self._normalize(s1), self._normalize(s2))
        result = (score > self.similarity_threshold, score)
        self._result_cache[cache_key] = result
        return result
