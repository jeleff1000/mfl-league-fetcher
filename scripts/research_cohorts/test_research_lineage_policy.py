import pytest

from research_lineage_policy import (
    CANONICAL_RESEARCH_MATCHUP_CACHE_KEY,
    CANONICAL_RESEARCH_MATCHUP_LINEAGE,
    require_canonical_cache_key,
    require_canonical_lineage,
)


def test_canonical_cache_key_is_accepted():
    assert require_canonical_cache_key(CANONICAL_RESEARCH_MATCHUP_CACHE_KEY) == (
        CANONICAL_RESEARCH_MATCHUP_CACHE_KEY
    )


@pytest.mark.parametrize(
    "cache_key",
    [
        "research-public-lake-v3-Linux-20260730",
        "research-public-lake-v4-Linux-20260806-signal-rescue-canonical",
        CANONICAL_RESEARCH_MATCHUP_CACHE_KEY + "-31199999999",
        "",
        None,
    ],
)
def test_noncanonical_cache_key_fails_fast(cache_key):
    with pytest.raises(RuntimeError, match="canonical research-matchup lineage"):
        require_canonical_cache_key(cache_key)


def test_only_canonical_lineage_is_allowed():
    assert require_canonical_lineage(CANONICAL_RESEARCH_MATCHUP_LINEAGE) == (
        CANONICAL_RESEARCH_MATCHUP_LINEAGE
    )

    with pytest.raises(RuntimeError, match="canonical research-matchup lineage"):
        require_canonical_lineage("research-matchup-experiment-31199999999")
