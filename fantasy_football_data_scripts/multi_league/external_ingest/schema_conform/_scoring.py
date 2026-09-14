"""Scoring primitives + composite compute_score.

Score formula:
    score = w_name * name_similarity + w_value * value_overlap
          + w_dtype * dtype_match + w_shape * shape_cosine

Co-occurrence is NOT a score component; it's a filter (see _passes.py).
"""

from __future__ import annotations
import math
from collections import Counter

import numpy as np
import pandas as pd

from ._config import (
    MIN_REF_SIZE_BY_SLOT,
    DEFAULT_MIN_REF_SIZE,
    WEIGHTS_THIN_REF,
    WEIGHTS_THICK_REF,
)
from ._normalize import _alias_norm


def token_jaccard(name_a: str, name_b: str) -> float:
    """Token-set Jaccard after alias normalization."""
    a = set(_alias_norm(name_a).split("_"))
    b = set(_alias_norm(name_b).split("_"))
    if not a or not b:
        return 0.0
    inter = a & b
    union = a | b
    return len(inter) / len(union)


def value_jaccard(set_a: set, set_b: set) -> float:
    """Value-set Jaccard. Returns 0.0 for empty sets."""
    if not set_a or not set_b:
        return 0.0
    inter = set_a & set_b
    union = set_a | set_b
    return len(inter) / len(union)


def dtype_match(src: pd.Series, ref: pd.Series) -> float:
    """1.0 same dtype / 0.5 numeric-compat / 0.0 incompat."""
    src_numeric = pd.api.types.is_numeric_dtype(src)
    ref_numeric = pd.api.types.is_numeric_dtype(ref)
    if src.dtype == ref.dtype:
        return 1.0
    if src_numeric and ref_numeric:
        return 0.5
    return 0.0


def shape_cosine(src: pd.Series, ref: pd.Series) -> float:
    """Length-distribution cosine for strings, numeric-distribution cosine for numbers.
    Returns 0.0 if either series is empty after dropna.
    """
    s = src.dropna()
    r = ref.dropna()
    if len(s) == 0 or len(r) == 0:
        return 0.0

    if pd.api.types.is_numeric_dtype(s) and pd.api.types.is_numeric_dtype(r):
        # Bin into 10 quantile buckets, compare histograms
        try:
            edges = np.linspace(min(s.min(), r.min()), max(s.max(), r.max()), 11)
            hs, _ = np.histogram(s, bins=edges)
            hr, _ = np.histogram(r, bins=edges)
            return _cosine(hs, hr)
        except Exception:
            return 0.0

    # String length distribution
    s_lengths = s.astype(str).str.len()
    r_lengths = r.astype(str).str.len()
    edges = list(range(0, 41, 2)) + [200]
    hs, _ = np.histogram(s_lengths, bins=edges)
    hr, _ = np.histogram(r_lengths, bins=edges)
    return _cosine(hs, hr)


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def weights_for(slot: str, ref_distinct_count: int) -> dict:
    """Adaptive weights per (slot, ref-thickness)."""
    threshold = MIN_REF_SIZE_BY_SLOT.get(slot, DEFAULT_MIN_REF_SIZE)
    return WEIGHTS_THIN_REF if ref_distinct_count < threshold else WEIGHTS_THICK_REF


def compute_score(
    src_col_name: str,
    src_values: pd.Series,
    slot: str,
    ref_values: pd.Series,
    ref_distinct_count: int,
) -> float:
    """Composite score for binding (src_col → slot)."""
    w = weights_for(slot, ref_distinct_count)
    name_sim = token_jaccard(src_col_name, slot)
    val_ov = value_jaccard(set(src_values.dropna().astype(str).unique()), set(ref_values.dropna().astype(str).unique()))
    dt = dtype_match(src_values, ref_values)
    sh = shape_cosine(src_values, ref_values)
    return w["name"] * name_sim + w["value"] * val_ov + w["dtype"] * dt + w["shape"] * sh


def _shannon_entropy_per_char(values: pd.Series) -> float:
    """Per-character Shannon entropy of concatenated string values.
    Used as a transaction_id invariant — high-entropy IDs look random-ish.
    """
    s = values.dropna().astype(str)
    if len(s) == 0:
        return 0.0
    chars = "".join(s)
    if not chars:
        return 0.0
    counter = Counter(chars)
    total = len(chars)
    return -sum((c / total) * math.log2(c / total) for c in counter.values())
