"""SimilarityEngine helpers: normalize/soundex/caching."""

import pytest
from conftest import THRESHOLD_LENIENT, make_matching_cfg

from scrape_kit.matching import SimilarityEngine
from scrape_kit.matching.normalize import normalize

pytestmark = pytest.mark.p0


# ── normalize ─────────────────────────────────────────────────────────────────


class TestNormalize:
    """Public normalize() lowercases, strips diacritics, applies synonyms and acronyms."""

    def test_normal_lowercases_and_strips_punctuation(self):
        result = normalize("Hello, World!", {}, {})
        assert result == result.lower()
        assert "," not in result

    def test_normal_removes_diacritics(self):
        empty: dict[str, str] = {}
        assert normalize("Ångström", empty, empty) == "angstrom"
        assert normalize("Résumé", empty, empty) == "resume"
        assert normalize("Barçelona", empty, empty) == "barcelona"

    def test_normal_collapses_extra_whitespace(self):
        result = normalize("  too   many   spaces  ", {}, {})
        assert "  " not in result

    def test_edge_already_clean_string_unchanged(self):
        assert normalize("hello world", {}, {}) == "hello world"

    def test_edge_synonym_exact_match_replaced(self):
        assert normalize("Man Utd", {"man utd": "manchester united"}, {}) == "manchester united"

    def test_edge_synonym_partial_match_not_replaced(self):
        # "man utd fc" ≠ "man utd" exactly, no synonym replacement
        assert normalize("Man Utd FC", {"man utd": "manchester united"}, {}) == "man utd fc"

    def test_normal_acronym_token_replaced(self):
        assert "football club" in normalize("Liverpool FC", {}, {"fc": "football club"})

    def test_edge_acronym_does_not_replace_inside_word(self):
        assert normalize("Real Betis", {}, {"al ": "", "real ": ""}) == "betis"

    def test_normal_acronym_prefix_with_separator_replaced(self):
        assert normalize("Al-Ahli", {}, {"al ": "", "al-": ""}) == "ahli"

    def test_normal_exact_synonym_is_protected_from_acronyms(self):
        assert normalize("Inter", {"inter": "inter milan"}, {"inter ": ""}) == "inter milan"


# ── _soundex ──────────────────────────────────────────────────────────────────


class TestSoundex:
    """_soundex produces standard codes and caches per word."""

    def test_normal_standard_soundex_codes(self, engine):
        assert engine._soundex("Smith") == "S530"
        assert engine._soundex("Smyth") == "S530"  # phonetically equivalent
        assert engine._soundex("Robert") == "R163"

    def test_normal_result_cached(self, engine):
        first = engine._soundex("Taylor")
        assert engine._soundex("Taylor") == first

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
    """Public similarity() results are stable across repeated calls."""

    def test_normal_second_call_returns_identical_result(self, engine):
        m1, s1 = engine.similarity("Chelsea", "Chelsea FC")
        m2, s2 = engine.similarity("Chelsea", "Chelsea FC")
        assert m1 == m2
        assert s1 == pytest.approx(s2)

    def test_edge_symmetric_inputs_return_identical_result(self, engine):
        m1, s1 = engine.similarity("A B", "B A")
        m2, s2 = engine.similarity("B A", "A B")
        assert m1 == m2
        assert s1 == pytest.approx(s2)

    def test_normal_separate_instances_do_not_share_results(self):
        cfg_a = make_matching_cfg()
        cfg_a["threshold"] = 90
        cfg_b = make_matching_cfg()
        cfg_b["threshold"] = THRESHOLD_LENIENT
        eng_a = SimilarityEngine(cfg_a)
        eng_b = SimilarityEngine(cfg_b)
        assert eng_a is not eng_b
        match_a, _ = eng_a.similarity("X Y", "Y X")
        match_b, _ = eng_b.similarity("X Y", "Y X")
        assert match_a != match_b


class TestAcronymPositions:
    """Acronym keys fire at prefix, suffix and interior positions (#8)."""

    def test_normal_suffix_position_replaced(self):
        assert normalize("Foo LTD", {}, {" ltd": "limited"}) == "foo limited"

    def test_normal_interior_position_replaced(self):
        assert normalize("Ministry of Defence", {}, {" of ": ""}) == "ministry defence"
