# Matching

Use `SimilarityEngine` to decide whether two entity names refer to the same thing. The public method is `similarity()`.

```python
from scrape_kit import MatchingError, SimilarityEngine

try:
    engine = SimilarityEngine(
        {
            "acronyms": {"fc ": ""},
            "synonyms": {"man utd": "manchester united"},
            "weak_tokens": ["new", "york"],
            "weights": {
                "token": 0.40,
                "shared_word": 0.10,
                "phonetic": 0.10,
                "ratio": 0.30,
                "partial": 0.10,
                "strong_mismatch_cap": 35,
            },
            "threshold": 65,
        }
    )
except MatchingError as exc:
    raise SystemExit(exc) from exc

is_match, score = engine.similarity("Man Utd", "Manchester United")
```

`similarity(s1, s2)` returns `(bool, float)`. The bool is `True` when the score is **greater than** the threshold. Non-strings raise `ValueError`. Diacritics are stripped automatically.

## Required configuration

`SimilarityEngine(cfg)` requires a non-empty `cfg`. Valid top-level keys:

| Key | Role |
| --- | --- |
| `acronyms` | Prefix/suffix replacements stripped while preparing strings (for example `"fc ": ""`). |
| `synonyms` | Canonical expansions applied around acronym stripping. |
| `weak_tokens` | Generic words that alone cannot prove a match (locations, franchise words). Do not put discriminative tokens such as `city` or `united` here. |
| `weights` | Scoring weights. See below. |
| `threshold` | Minimum score for a positive match. Default `65`. |

Unknown top-level keys raise `MatchingError` at construction.

## Weights

| Weight key | Default | Role |
| --- | --- | --- |
| `token` | `0.40` | Best of token-set / sort ratio |
| `shared_word` | `0.10` | Shared full-word bonus. `substr` is a deprecated alias. |
| `phonetic` | `0.10` | Soundex overlap across strong tokens |
| `ratio` | `0.30` | Character-level ratio |
| `partial` | `0.10` | Partial ratio when the shorter string is at most 8 characters |
| `strong_mismatch_cap` | `35` | Score ceiling when strong tokens disagree |

Unknown weight keys, negative scoring weights, or a scoring-weight sum that differs from `1.0` by more than `0.25` raise `MatchingError`.

Tokens that are not in `weak_tokens` are strong. Disjoint strong tokens cap the score at `strong_mismatch_cap`, so similar location words do not merge distinct entities.

## Related guides

- [Errors](errors.md) for `MatchingError`.
