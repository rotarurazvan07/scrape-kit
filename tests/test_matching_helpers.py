"""SimilarityEngine helpers: normalize/soundex/caching."""

import pytest
from conftest import (
    RICH_CONFIG,
    THRESHOLD_LENIENT,
    make_matching_cfg,
)

import scrape_kit.matching.normalize  # noqa: F401  # direct import edge pins normalize.py (transitive_only finding)
from scrape_kit.matching import SimilarityEngine

pytestmark = pytest.mark.p0


@pytest.fixture
def rich_engine():
    """Engine pre-loaded with rich config (alias for 'engine' now)."""
    return SimilarityEngine(RICH_CONFIG)


# ── _normalize ────────────────────────────────────────────────────────────────


class TestNormalize:
    """_normalize lowercases, strips diacritics, applies synonyms and acronyms."""

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
        cfg = make_matching_cfg()
        cfg["synonyms"] = {"man utd": "manchester united"}
        cfg["acronyms"] = {}  # Clear to avoid interference
        eng = SimilarityEngine(cfg)
        assert eng._normalize("Man Utd") == "manchester united"

    def test_edge_synonym_partial_match_not_replaced(self):
        cfg = make_matching_cfg()
        cfg["synonyms"] = {"man utd": "manchester united"}
        cfg["acronyms"] = {}  # Clear to avoid interference
        eng = SimilarityEngine(cfg)
        # "man utd fc" ≠ "man utd" exactly, no synonym replacement; no acronyms to replace either
        result = eng._normalize("Man Utd FC")
        assert result == "man utd fc"

    def test_normal_acronym_token_replaced(self):
        cfg = make_matching_cfg()
        cfg["acronyms"] = {"fc": "football club"}
        eng = SimilarityEngine(cfg)
        assert "football club" in eng._normalize("Liverpool FC")

    def test_edge_acronym_does_not_replace_inside_word(self):
        cfg = make_matching_cfg()
        cfg["acronyms"] = {"al ": "", "real ": ""}
        eng = SimilarityEngine(cfg)
        assert eng._normalize("Real Betis") == "betis"

    def test_normal_acronym_prefix_with_separator_replaced(self):
        cfg = make_matching_cfg()
        cfg["acronyms"] = {"al ": "", "al-": ""}
        eng = SimilarityEngine(cfg)
        assert eng._normalize("Al-Ahli") == "ahli"

    def test_normal_exact_synonym_is_protected_from_acronyms(self):
        cfg = make_matching_cfg()
        cfg["synonyms"] = {"inter": "inter milan"}
        cfg["acronyms"] = {"inter ": ""}
        eng = SimilarityEngine(cfg)
        assert eng._normalize("Inter") == "inter milan"

    def test_normal_result_cached_on_second_call(self, engine):
        engine._normalize("Cache Test")
        assert "cache test" in engine._norm_cache.values()
        first_val = engine._norm_cache.get("Cache Test")
        engine._normalize("Cache Test")  # second call — must hit cache
        assert engine._norm_cache.get("Cache Test") == first_val


# ── _soundex ──────────────────────────────────────────────────────────────────


class TestSoundex:
    """_soundex produces standard codes and caches per word."""

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
    """Result and normalization caches are symmetric, isolated and bounded."""

    def test_normal_result_cached_after_first_similarity(self, engine):
        engine.similarity("Arsenal", "Arsenal FC")
        key = tuple(sorted(["Arsenal", "Arsenal FC"]))
        assert key in engine._result_cache

    def test_normal_second_call_returns_identical_result(self, engine):
        m1, s1 = engine.similarity("Chelsea", "Chelsea FC")
        m2, s2 = engine.similarity("Chelsea", "Chelsea FC")
        assert m1 == m2
        assert s1 == pytest.approx(s2)

    def test_edge_symmetric_cache_key(self, engine):
        engine.similarity("A B", "B A")
        key_fwd = tuple(sorted(["A B", "B A"]))
        assert key_fwd in engine._result_cache

    def test_normal_separate_instances_have_independent_caches(self):
        cfg_a = make_matching_cfg()
        cfg_a["threshold"] = 90
        cfg_b = make_matching_cfg()
        cfg_b["threshold"] = THRESHOLD_LENIENT
        eng_a = SimilarityEngine(cfg_a)
        eng_b = SimilarityEngine(cfg_b)
        eng_a.similarity("X Y", "Y X")
        # eng_b cache must be untouched
        assert eng_a is not eng_b
        assert eng_b._result_cache == {}

    def test_edge_large_number_of_cached_pairs(self, engine):
        for i in range(200):
            engine.similarity(f"Team {i}", f"Squad {i}")
        assert len(engine._result_cache) == 200


class TestAcronymPositions:
    """Acronym keys fire at prefix, suffix and interior positions (#8)."""

    def test_normal_suffix_position_replaced(self):
        eng = SimilarityEngine(make_matching_cfg(acronyms={" ltd": "limited"}))
        assert eng._normalize("Foo LTD") == "foo limited"

    def test_normal_interior_position_replaced(self):
        eng = SimilarityEngine(make_matching_cfg(acronyms={" of ": ""}))
        assert eng._normalize("Ministry of Defence") == "ministry defence"
