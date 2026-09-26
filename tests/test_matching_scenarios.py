"""Matching integration scenarios (4th-tier test_scenario_)."""

import pytest
from conftest import (
    RICH_CONFIG,
    THRESHOLD_DEFAULT,
    THRESHOLD_EXACT,
    THRESHOLD_HIGH,
    THRESHOLD_LENIENT,
    THRESHOLD_MODERATE,
    make_matching_cfg,
)

from scrape_kit.matching import SimilarityEngine

pytestmark = pytest.mark.p1


@pytest.fixture
def rich_engine():
    """Engine pre-loaded with rich config (alias for 'engine' now)."""
    return SimilarityEngine(RICH_CONFIG)


# ── Complex Scenarios ─────────────────────────────────────────────────────────


class TestMatchingScenarios:
    """Matching journeys (4th-tier test_scenario_ integration)."""

    def test_scenario_diacritic_plus_synonym_chain(self):
        """Diacritic stripping and synonym replacement must compose correctly."""
        cfg = make_matching_cfg()
        cfg.update(
            {
                "threshold": THRESHOLD_MODERATE,
                "synonyms": {"fc barcelona": "barcelona"},
            }
        )
        eng = SimilarityEngine(cfg)
        # "FC Barçelona" → strip diacritic → "FC Barcelona" → lowercase → "fc barcelona"
        # → synonym match → "barcelona"
        # "Barcelona" → normalize → "barcelona"
        match, _ = eng.is_similar("FC Barçelona", "Barcelona")
        assert match is True

    def test_scenario_acronym_expands_before_similarity(self):
        """Acronym expansion during normalization bridges abbreviated vs full name."""
        cfg = make_matching_cfg()
        cfg.update(
            {
                "threshold": THRESHOLD_DEFAULT,
                "acronyms": {"fc": "football club", "utd": "united"},
            }
        )
        eng = SimilarityEngine(cfg)
        match, _ = eng.is_similar("Manchester FC", "Manchester Football Club")
        assert match is True
        match2, _ = eng.is_similar("Man Utd", "Man United")
        assert match2 is True

    def test_scenario_short_prefix_rule_does_not_break_real_betis(self):
        """Short prefix rules must not corrupt longer words before matching."""
        cfg = make_matching_cfg()
        cfg.update(
            {
                "threshold": THRESHOLD_DEFAULT,
                "acronyms": {"al ": "", "real ": ""},
            }
        )
        eng = SimilarityEngine(cfg)
        match, score = eng.is_similar("Real Betis", "Betis")
        assert match is True
        assert score > 65

    def test_scenario_weak_tokens_do_not_establish_match_alone(self):
        """Ambiguous shared words should not merge clearly different clubs."""
        cfg = make_matching_cfg()
        cfg.update(
            {
                "threshold": THRESHOLD_DEFAULT,
                "weak_tokens": ["new", "york", "sporting", "inter"],
            }
        )
        eng = SimilarityEngine(cfg)
        match, score = eng.is_similar("New York City", "New York Red Bulls")
        assert match is False
        assert score == pytest.approx(35.0)  # strong_mismatch_cap residual (issue #2)

        match, score = eng.is_similar("Sporting CP", "Sporting Kansas City")
        assert match is False
        assert score == pytest.approx(35.0)

    def test_scenario_weak_tokens_still_allow_exact_canonical_synonyms(self):
        cfg = make_matching_cfg()
        cfg.update(
            {
                "threshold": THRESHOLD_DEFAULT,
                "synonyms": {"inter": "inter milan"},
                "weak_tokens": ["inter"],
            }
        )
        eng = SimilarityEngine(cfg)
        match, score = eng.is_similar("Inter", "Inter Milan")
        assert match is True
        assert score == pytest.approx(100.0)

    def test_scenario_token_weight_vs_ratio_weight_on_reordered_names(self):
        """Token set ratio handles order-independence; character ratio does not."""
        cfg_token = make_matching_cfg()
        cfg_token.update(
            {
                "threshold": THRESHOLD_HIGH,
                "weights": {"token": 1.0, "substr": 0.0, "phonetic": 0.0, "ratio": 0.0},
            }
        )
        cfg_ratio = make_matching_cfg()
        cfg_ratio.update(
            {
                "threshold": THRESHOLD_HIGH,
                "weights": {"token": 0.0, "substr": 0.0, "phonetic": 0.0, "ratio": 1.0},
            }
        )
        token_eng = SimilarityEngine(cfg_token)
        ratio_eng = SimilarityEngine(cfg_ratio)
        m_token, _ = token_eng.is_similar("Moby Dick", "Dick Moby")
        m_ratio, _ = ratio_eng.is_similar("Moby Dick", "Dick Moby")
        assert m_token is True
        assert m_ratio is False  # character-level order mismatch lowers ratio

    def test_scenario_phonetic_weight_boosts_homophones(self):
        """High phonetic weight helps match names that sound alike but are spelled differently."""
        cfg = make_matching_cfg()
        cfg.update(
            {
                "threshold": 50,
                "weights": {"token": 0.2, "substr": 0.0, "phonetic": 0.8, "ratio": 0.0},
            }
        )
        eng = SimilarityEngine(cfg)
        # "Smith" and "Smyth" share soundex S530
        match, score = eng.is_similar("John Smith", "John Smyth")
        assert match is True
        assert score > 50

    @pytest.mark.parametrize(
        ("threshold", "expected"),
        [
            (THRESHOLD_EXACT, False),  # 95 > 86.88 — moderately similar pair rejected
            (THRESHOLD_DEFAULT, True),
            (THRESHOLD_LENIENT, True),
        ],
    )
    def test_scenario_threshold_sensitivity(self, threshold, expected):
        """Same pair scores 86.88 — threshold alone flips the boolean result."""
        cfg = make_matching_cfg()
        cfg["threshold"] = threshold
        match, _ = SimilarityEngine(cfg).is_similar("Liverpool FC", "Liverpool")
        assert match is expected
