from matching import SimilarityEngine

def test_similarity_basic():
    cfg = {"threshold": 70}
    engine = SimilarityEngine(cfg)

    # Exact match after normalization
    assert engine.is_similar(" The Great Gatsby ", "Great Gatsby, The")[0] is True

def test_similarity_normalization():
    cfg = {"threshold": 70, "synonyms": {"ion": "ion (rebreanu)"}}
    engine = SimilarityEngine(cfg)

    # Normalization including diacritics
    match, score = engine.is_similar("Târgul Cărții", "Targul Cartii")
    assert match is True

    match, score = engine.is_similar("ion", "ion (rebreanu)")
    assert match is True

def test_similarity_weights():
    # Only if token weight is 1.0 it will pass a heavily similar name
    cfg = {"threshold": 80, "weights": {"token": 1.0, "substr": 0.0, "phonetic": 0.0, "ratio": 0.0}}
    engine = SimilarityEngine(cfg)

    match, score = engine.is_similar("Moby Dick", "Dick Moby")
    assert match is True

    # If using ratio weight as high, then order matters
    cfg_ratio = {"threshold": 80, "weights": {"token": 0.0, "substr": 0.0, "phonetic": 0.0, "ratio": 1.0}}
    engine_ratio = SimilarityEngine(cfg_ratio)
    match_ratio, score_ratio = engine_ratio.is_similar("Moby Dick", "Dick Moby")
    assert match_ratio is False

def test_similarity_acronyms():
    cfg = {"threshold": 70, "acronyms": {"fc": "football club"}}
    engine = SimilarityEngine(cfg)

    # Normalizing with acronyms
    match, score = engine.is_similar("Manchester FC", "Manchester Football Club")
    assert match is True

def test_similarity_caching():
    cfg = {"threshold": 70}
    engine = SimilarityEngine(cfg)

    # First call
    m1, s1 = engine.is_similar("Book A", "Book B")
    # Second call (should be cached)
    m2, s2 = engine.is_similar("Book A", "Book B")

    assert m1 == m2
    assert s1 == s2
