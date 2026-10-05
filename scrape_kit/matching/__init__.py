"""Hybrid name matching: fuzzy metrics, phonetics, and strong-token enforcement via SimilarityEngine."""

from ..errors import MatchingError
from .engine import SimilarityEngine

__all__ = ["MatchingError", "SimilarityEngine"]
