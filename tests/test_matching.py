"""SimilarityEngine core: init/hybrid_match/is_similar."""

import pytest
from conftest import (
    RICH_CONFIG,
    THRESHOLD_HIGH,
    THRESHOLD_MODERATE,
    make_matching_cfg,
)

from scrape_kit.matching import SimilarityEngine

pytestmark = pytest.mark.p0


# ── __init__ ──────────────────────────────────────────────────────────────────


class TestInit:
    """SettingsManager loads YAML trees into nested settings dict."""

    def test_normal_full_config_applied(self):
        cfg = {
            "threshold": THRESHOLD_HIGH,
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
        eng = SimilarityEngine(RICH_CONFIG)
        assert eng._norm_cache == {}
        assert eng._soundex_cache == {}
        assert eng._result_cache == {}

    def test_error_empty_config_raises_value_error(self):
        with pytest.raises(ValueError, match="Configuration is required"):
            SimilarityEngine({})

    def test_normal_missing_weights_defaults_silently(self):
        # Post-rework contract (issue #2): missing weights silently fall back
        # to the documented defaults instead of raising.
        eng = SimilarityEngine({"threshold": 70})
        assert eng.token_weight == pytest.approx(0.40)
        assert eng.substr_weight == pytest.approx(0.10)
        assert eng.phonetic_weight == pytest.approx(0.10)
        assert eng.ratio_weight == pytest.approx(0.30)
        assert eng.partial_weight == pytest.approx(0.10)

    def test_error_weight_sum_deviation_raises_value_error(self):
        # Issue #1: weight sets drifting far from 1.0 silently rescale every
        # score — reject them at construction (explicit-errors philosophy).
        cfg = make_matching_cfg()
        cfg["weights"] = {"token": 0.9, "substr": 0.3, "phonetic": 0.1, "ratio": 0.3}
        with pytest.raises(ValueError, match="weights must sum to approximately 1.0"):
            SimilarityEngine(cfg)

    def test_error_negative_weight_raises_value_error(self):
        # Review finding: negative weights can sum to exactly 1.0 while
        # corrupting individual metric contributions — must still be rejected.
        cfg = make_matching_cfg()
        cfg["weights"] = {"token": -0.5, "substr": 1.5, "phonetic": 0.0, "ratio": 0.0, "partial": 0.0}
        with pytest.raises(ValueError, match="weights must be non-negative"):
            SimilarityEngine(cfg)

    def test_error_unknown_weight_key_raises_value_error(self):
        # Clean-baseline contract: typo'd weight keys must not vanish silently —
        # raise ValueError naming the valid keys; missing keys keep defaults.
        cfg = make_matching_cfg()
        cfg["weights"] = {"tokn": 0.4, "substr": 0.1, "phonetic": 0.1, "ratio": 0.3}
        with pytest.raises(ValueError, match="Unknown weight key.*valid keys"):
            SimilarityEngine(cfg)

    def test_edge_partial_weights_documented_sum_still_accepted(self, engine):
        # RICH_CONFIG sums to 1.1 (explicit weights 1.0 + default partial 0.1);
        # documented configs must stay constructible — the clamp absorbs drift.
        match, score = engine.is_similar("Real Madrid", "Real Madrid")
        assert match is True
        assert score == pytest.approx(100.0)

    def test_edge_zero_threshold_works_with_weights(self):
        cfg = make_matching_cfg()
        cfg["threshold"] = 0
        eng = SimilarityEngine(cfg)
        match, _ = eng.is_similar("apple", "orange")
        # score 0.0 is NOT > 0, so still False
        match2, _ = eng.is_similar("apple pie", "apple juice")
        assert match2 is True  # shared token → score > 0


# ── hybrid_match ──────────────────────────────────────────────────────────────


class TestHybridMatch:
    """hybrid_match returns clamped 0-100 scores with contract error paths."""

    def test_normal_identical_strings_score_100(self, engine):
        assert engine.hybrid_match("Real Madrid", "Real Madrid") == pytest.approx(100.0)

    def test_normal_very_similar_strings_high_score(self, engine):
        score = engine.hybrid_match("Manchester United", "Manchester United FC")
        assert score > 70

    def test_normal_reordered_tokens_nonzero(self, engine):
        # token_set_ratio handles reordering well
        score = engine.hybrid_match("John Smith", "Smith John")
        assert score > 0

    def test_edge_no_shared_token_capped_residual(self, engine):
        # Post-rework contract (issue #2): disjoint strong tokens leave a small
        # residual under strong_mismatch_cap=35 (documented 28.57), not zero.
        score = engine.hybrid_match("apple pie", "orange juice")
        assert score == pytest.approx(28.57, abs=0.01)
        assert 0.0 < score <= 35.0

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
        # Post-rework contract (issue #2): designed ValueError raised up front,
        # not an accidental TypeError from the cache-key computation.
        with pytest.raises(ValueError, match="hybrid_match expects two strings"):
            engine.hybrid_match(None, "test")

    def test_error_non_string_input_raises(self, engine):
        with pytest.raises(ValueError, match="hybrid_match expects two strings"):
            engine.hybrid_match("test", 123)


# ── is_similar ────────────────────────────────────────────────────────────────


class TestIsSimilar:
    """is_similar thresholds the hybrid score and returns (bool, float)."""

    @pytest.mark.smoke
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
        cfg = make_matching_cfg()
        cfg["threshold"] = THRESHOLD_MODERATE
        eng = SimilarityEngine(cfg)
        match, _ = eng.is_similar("Müller", "Muller")
        assert match is True

    def test_edge_empty_strings_do_not_crash(self, engine):
        match, score = engine.is_similar("", "")
        # No shared tokens → score 0.0 → not > threshold
        assert match is False
        assert score == pytest.approx(0.0)

    def test_edge_result_is_symmetric(self, engine):
        m1, s1 = engine.is_similar("Alpha Beta", "Beta Alpha")
        m2, s2 = engine.is_similar("Beta Alpha", "Alpha Beta")
        assert m1 == m2
        assert s1 == pytest.approx(s2)

    def test_error_none_raises(self, engine):
        # Post-rework contract (issue #2): ValueError raised before the
        # order-independent cache-key computation at the top of is_similar.
        with pytest.raises(ValueError, match="is_similar expects two strings"):
            engine.is_similar(None, "test")

    def test_error_non_string_raises_value_error(self, engine):
        with pytest.raises(ValueError, match="is_similar expects two strings"):
            engine.is_similar("test", 123)


class TestBoundaryMatrix:
    """Designed boundary partitions: empty, whitespace, single-token, unicode, typo-phonetic (#8)."""

    @pytest.mark.parametrize(
        ("left", "right"),
        [("", ""), ("   ", "   "), ("", "word"), ("word", ""), ("  ", "word"), ("word", "   ")],
    )
    def test_edge_empty_and_whitespace_score_zero(self, engine, left, right):
        match, score = engine.is_similar(left, right)
        assert match is False
        assert score == pytest.approx(0.0)

    def test_edge_single_token_identical_scores_100(self, engine):
        assert engine.is_similar("Nike", "Nike") == (True, pytest.approx(100.0))

    def test_edge_single_token_disjoint_scores_residual(self, engine):
        match, score = engine.is_similar("Alpha", "Omega")
        assert match is False
        assert score == pytest.approx(19.07, abs=0.01)  # single-token residual under the 35 cap

    def test_edge_multi_token_disjoint_hits_cap(self, engine):
        _, score = engine.is_similar("Alpha Beta", "Omega Delta")
        assert score == pytest.approx(35.0)  # strong_mismatch_cap residual (documented 28.57→35 family)

    def test_normal_unicode_diacritics_match_exactly(self, engine):
        assert engine.is_similar("Café", "Cafe") == (True, pytest.approx(100.0))

    def test_normal_typo_phonetic_bridge(self, engine):
        match, score = engine.is_similar("Smith", "Smyth")
        assert match is True
        assert score > 50  # soundex bridges the homophone typo

    def test_edge_internal_whitespace_does_not_merge_tokens(self, engine):
        # "sp  ace" keeps two strong tokens — disjoint with "space" → capped residual
        match, score = engine.is_similar("  sp  ace  ", "space")
        assert match is False
        assert score == pytest.approx(35.0)


class TestStrongTokenContainmentArms:
    """Asymmetric containment: one discriminative side vs an all-weak side caps at 35.

    Both containment arms are reachable only as misses - a containment hit is
    impossible (any token shared verbatim would make the weak side strong too),
    so every arm entry asserts the documented strong_mismatch_cap.
    """

    def test_edge_strong_left_all_weak_right_miss_caps(self):
        # ACR-2 (arm s1->s2): 'city' is strong, 'new york' is all weak, city is absent.
        eng = SimilarityEngine(make_matching_cfg(weak_tokens=["new", "york"]))
        match, score = eng.is_similar("new york city", "new york")
        assert match is False
        assert score == pytest.approx(35.0)

    def test_edge_strong_right_all_weak_left_miss_caps(self):
        # ACR-2 (arm s2->s1): fresh engine - the order-independent result cache
        # would short-circuit the reverse pair on an engine that already saw it.
        eng = SimilarityEngine(make_matching_cfg(weak_tokens=["new", "york"]))
        match, score = eng.is_similar("new york", "new york city")
        assert match is False
        assert score == pytest.approx(35.0)

    def test_edge_single_weak_token_strong_left_miss_caps(self):
        # ACR-2: minimal weak list - 'corp' strong vs all-weak 'acme'.
        eng = SimilarityEngine(make_matching_cfg(weak_tokens=["acme"]))
        match, score = eng.is_similar("acme corp", "acme")
        assert match is False
        assert score == pytest.approx(35.0)
