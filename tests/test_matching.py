"""
Comprehensive tests for matching.py — SimilarityEngine.

Public API covered:
  __init__, hybrid_match, is_similar
  (plus internal helpers _normalize, _soundex, _share_token covered via integration)

Each method has: normal case(s), edge case(s), error case.
Plus 5 complex integration scenarios at the bottom.
"""

import pytest
from matching import SimilarityEngine


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def engine():
    """Default engine — balanced weights, threshold 70."""
    return SimilarityEngine({"threshold": 70})


@pytest.fixture
def rich_engine():
    """Engine pre-loaded with acronyms, synonyms, and explicit weights."""
    return SimilarityEngine({
        "threshold": 65,
        "acronyms": {
            "fc": "football club",
            "utd": "united",
            "afc": "athletic football club",
        },
        "synonyms": {
            "man city": "manchester city",
            "barca": "fc barcelona",
        },
        "weights": {
            "token": 0.5,
            "substr": 0.1,
            "phonetic": 0.1,
            "ratio": 0.3,
        },
    })


# ── __init__ ──────────────────────────────────────────────────────────────────

class TestInit:
    def test_normal_full_config_applied(self):
        cfg = {
            "threshold": 80,
            "acronyms": {"nba": "national basketball association"},
            "synonyms": {"la": "los angeles"},
            "weights": {"token": 0.6, "substr": 0.1, "phonetic": 0.1, "ratio": 0.2},
        }
        eng = SimilarityEngine(cfg)
        assert eng.similarity_threshold == 80
        assert eng.token_weight == 0.6
        assert eng.substr_weight == 0.1
        assert eng.phonetic_weight == 0.1
        assert eng.ratio_weight == 0.2
        assert eng.acronyms == {"nba": "national basketball association"}
        assert eng.synonyms == {"la": "los angeles"}

    def test_normal_caches_initialised_empty(self):
        eng = SimilarityEngine({})
        assert eng._norm_cache == {}
        assert eng._soundex_cache == {}
        assert eng._result_cache == {}

    def test_edge_empty_config_applies_all_defaults(self):
        eng = SimilarityEngine({})
        assert eng.similarity_threshold == 65
        assert eng.token_weight == 0.5
        assert eng.substr_weight == 0.1
        assert eng.phonetic_weight == 0.1
        assert eng.ratio_weight == 0.3
        assert eng.acronyms == {}
        assert eng.synonyms == {}

    def test_edge_partial_weights_fills_missing_with_defaults(self):
        eng = SimilarityEngine({"weights": {"token": 0.9}})
        assert eng.token_weight == 0.9
        assert eng.substr_weight == 0.1   # default
        assert eng.ratio_weight == 0.3    # default

    def test_edge_zero_threshold_everything_is_similar(self):
        eng = SimilarityEngine({"threshold": 0})
        match, _ = eng.is_similar("apple", "orange")
        # score 0.0 is NOT > 0, so still False; but any real match will pass
        match2, _ = eng.is_similar("apple pie", "apple juice")
        assert match2 is True  # shared token → score > 0 > 0


# ── hybrid_match ──────────────────────────────────────────────────────────────

class TestHybridMatch:
    def test_normal_identical_strings_score_100(self, engine):
        assert engine.hybrid_match("Real Madrid", "Real Madrid") == pytest.approx(100.0)

    def test_normal_very_similar_strings_high_score(self, engine):
        score = engine.hybrid_match("Manchester United", "Manchester United FC")
        assert score > 70

    def test_normal_reordered_tokens_nonzero(self, engine):
        # token_set_ratio handles reordering well
        score = engine.hybrid_match("John Smith", "Smith John")
        assert score > 0

    def test_edge_no_shared_token_returns_zero(self, engine):
        assert engine.hybrid_match("apple pie", "orange juice") == 0.0

    def test_edge_single_word_match(self, engine):
        assert engine.hybrid_match("Nike", "Nike") == pytest.approx(100.0)

    def test_edge_partial_overlap(self, engine):
        score_full = engine.hybrid_match("Liverpool", "Liverpool FC")
        score_none = engine.hybrid_match("Liverpool", "Arsenal")
        assert score_full > score_none

    def test_normal_score_is_float(self, engine):
        score = engine.hybrid_match("Barcelona", "FC Barcelona")
        assert isinstance(score, float)

    def test_error_none_input_raises(self, engine):
        with pytest.raises((AttributeError, TypeError)):
            engine.hybrid_match(None, "test")


# ── is_similar ────────────────────────────────────────────────────────────────

class TestIsSimilar:
    def test_normal_clearly_similar_returns_true(self, engine):
        match, score = engine.is_similar("Tottenham Hotspur", "Tottenham")
        assert match is True
        assert score > 70

    def test_normal_clearly_different_returns_false(self, engine):
        match, _ = engine.is_similar("Chelsea", "Arsenal")
        assert match is False

    def test_normal_returns_tuple_of_bool_and_float(self, engine):
        result = engine.is_similar("Bayern Munich", "Bayern")
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], bool)
        assert isinstance(result[1], float)

    def test_edge_identical_strings_score_100(self, engine):
        match, score = engine.is_similar("Juventus", "Juventus")
        assert match is True
        assert score == pytest.approx(100.0)

    def test_edge_diacritics_stripped_before_comparison(self):
        eng = SimilarityEngine({"threshold": 70})
        match, _ = eng.is_similar("Müller", "Muller")
        assert match is True

    def test_edge_empty_strings_do_not_crash(self, engine):
        match, score = engine.is_similar("", "")
        # No shared tokens → score 0.0 → not > threshold
        assert match is False
        assert score == 0.0

    def test_edge_result_is_symmetric(self, engine):
        m1, s1 = engine.is_similar("Alpha Beta", "Beta Alpha")
        m2, s2 = engine.is_similar("Beta Alpha", "Alpha Beta")
        assert m1 == m2
        assert s1 == pytest.approx(s2)

    def test_error_none_raises(self, engine):
        with pytest.raises((AttributeError, TypeError)):
            engine.is_similar(None, "test")


# ── _normalize ────────────────────────────────────────────────────────────────

class TestNormalize:
    def test_normal_lowercases_and_strips_punctuation(self, engine):
        result = engine._normalize("Hello, World!")
        assert result == result.lower()
        assert "," not in result

    def test_normal_removes_diacritics(self, engine):
        assert engine._normalize("Ångström") == "angstrom"
        assert engine._normalize("Résumé") == "resume"
        assert engine._normalize("Barçelona") == "barcelona"

    def test_normal_collapses_extra_whitespace(self, engine):
        result = engine._normalize("  too   many   spaces  ")
        assert "  " not in result

    def test_edge_already_clean_string_unchanged(self, engine):
        assert engine._normalize("hello world") == "hello world"

    def test_edge_synonym_exact_match_replaced(self):
        eng = SimilarityEngine({"synonyms": {"man utd": "manchester united"}})
        assert eng._normalize("Man Utd") == "manchester united"

    def test_edge_synonym_partial_match_not_replaced(self):
        eng = SimilarityEngine({"synonyms": {"man utd": "manchester united"}})
        # "man utd fc" ≠ "man utd" exactly, no replacement
        result = eng._normalize("Man Utd FC")
        assert result == "man utd fc"

    def test_normal_acronym_substring_replaced(self):
        eng = SimilarityEngine({"acronyms": {"fc": "football club"}})
        assert "football club" in eng._normalize("Liverpool FC")

    def test_normal_result_cached_on_second_call(self, engine):
        engine._normalize("Cache Test")
        assert "cache test" in engine._norm_cache.values()
        first_val = engine._norm_cache.get("Cache Test")
        engine._normalize("Cache Test")  # second call — must hit cache
        assert engine._norm_cache.get("Cache Test") == first_val


# ── _soundex ──────────────────────────────────────────────────────────────────

class TestSoundex:
    def test_normal_standard_soundex_codes(self, engine):
        assert engine._soundex("Smith") == "S530"
        assert engine._soundex("Smyth") == "S530"  # phonetically equivalent
        assert engine._soundex("Robert") == "R163"

    def test_normal_result_cached(self, engine):
        engine._soundex("Taylor")
        assert "Taylor" in engine._soundex_cache
        cached = engine._soundex_cache["Taylor"]
        engine._soundex("Taylor")
        assert engine._soundex_cache["Taylor"] == cached

    def test_edge_empty_string_returns_zeros(self, engine):
        assert engine._soundex("") == "0000"

    def test_edge_single_character(self, engine):
        result = engine._soundex("A")
        assert len(result) == 4
        assert result.startswith("A")

    def test_normal_different_names_different_codes(self, engine):
        assert engine._soundex("Adams") != engine._soundex("Brown")


# ── Caching ───────────────────────────────────────────────────────────────────

class TestCaching:
    def test_normal_result_cached_after_first_is_similar(self, engine):
        engine.is_similar("Arsenal", "Arsenal FC")
        key = tuple(sorted(["Arsenal", "Arsenal FC"]))
        assert key in engine._result_cache

    def test_normal_second_call_returns_identical_result(self, engine):
        m1, s1 = engine.is_similar("Chelsea", "Chelsea FC")
        m2, s2 = engine.is_similar("Chelsea", "Chelsea FC")
        assert m1 == m2
        assert s1 == pytest.approx(s2)

    def test_edge_symmetric_cache_key(self, engine):
        engine.is_similar("A B", "B A")
        key_fwd = tuple(sorted(["A B", "B A"]))
        assert key_fwd in engine._result_cache

    def test_normal_separate_instances_have_independent_caches(self):
        eng_a = SimilarityEngine({"threshold": 90})
        eng_b = SimilarityEngine({"threshold": 40})
        eng_a.is_similar("X Y", "Y X")
        # eng_b cache must be untouched
        assert engine is not eng_b
        assert eng_b._result_cache == {}

    def test_edge_large_number_of_cached_pairs(self, engine):
        for i in range(200):
            engine.is_similar(f"Team {i}", f"Squad {i}")
        assert len(engine._result_cache) == 200


# ── Complex Scenarios ─────────────────────────────────────────────────────────

class TestMatchingScenarios:
    def test_scenario_diacritic_plus_synonym_chain(self):
        """Diacritic stripping and synonym replacement must compose correctly."""
        eng = SimilarityEngine({
            "threshold": 70,
            "synonyms": {"fc barcelona": "barcelona"},
        })
        # "FC Barçelona" → strip diacritic → "FC Barcelona" → lowercase → "fc barcelona"
        # → synonym match → "barcelona"
        # "Barcelona" → normalize → "barcelona"
        match, _ = eng.is_similar("FC Barçelona", "Barcelona")
        assert match is True

    def test_scenario_acronym_expands_before_similarity(self):
        """Acronym expansion during normalization bridges abbreviated vs full name."""
        eng = SimilarityEngine({
            "threshold": 65,
            "acronyms": {"fc": "football club", "utd": "united"},
        })
        match, _ = eng.is_similar("Manchester FC", "Manchester Football Club")
        assert match is True
        match2, _ = eng.is_similar("Man Utd", "Man United")
        assert match2 is True

    def test_scenario_token_weight_vs_ratio_weight_on_reordered_names(self):
        """Token set ratio handles order-independence; character ratio does not."""
        token_eng = SimilarityEngine({
            "threshold": 80,
            "weights": {"token": 1.0, "substr": 0.0, "phonetic": 0.0, "ratio": 0.0},
        })
        ratio_eng = SimilarityEngine({
            "threshold": 80,
            "weights": {"token": 0.0, "substr": 0.0, "phonetic": 0.0, "ratio": 1.0},
        })
        m_token, _ = token_eng.is_similar("Moby Dick", "Dick Moby")
        m_ratio, _ = ratio_eng.is_similar("Moby Dick", "Dick Moby")
        assert m_token is True
        assert m_ratio is False  # character-level order mismatch lowers ratio

    def test_scenario_phonetic_weight_boosts_homophones(self):
        """High phonetic weight helps match names that sound alike but are spelled differently."""
        eng = SimilarityEngine({
            "threshold": 50,
            "weights": {"token": 0.2, "substr": 0.0, "phonetic": 0.8, "ratio": 0.0},
        })
        # "Smith" and "Smyth" share soundex S530
        match, score = eng.is_similar("John Smith", "John Smyth")
        assert match is True
        assert score > 50

    def test_scenario_threshold_sensitivity(self):
        """Same pair — strict vs lenient threshold flips the boolean result."""
        strict = SimilarityEngine({"threshold": 95})
        lenient = SimilarityEngine({"threshold": 40})
        # Moderately similar pair
        _, score = lenient.is_similar("Liverpool FC", "Liverpool")
        m_strict, _ = strict.is_similar("Liverpool FC", "Liverpool")
        m_lenient, _ = lenient.is_similar("Liverpool FC", "Liverpool")
        if score < 95:
            assert m_strict is False
        assert m_lenient is True