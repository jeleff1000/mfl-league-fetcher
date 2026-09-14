"""Hard policy for research matchup artifact lineage.

Research matchup artifacts are cumulative and must not silently fork into
run-specific or experiment-specific cache lineages.  Keep this module small
and dependency-free so workflows and local validation can use the same guard.
"""

CANONICAL_RESEARCH_MATCHUP_CACHE_KEY = (
    "research-public-lake-v4-Linux-20260806-"
    "championship-v2-final-playoff-anchor-outcomes-31147899613"
)
CANONICAL_RESEARCH_MATCHUP_LINEAGE = (
    "research-matchup-v2-final-playoff-anchor-outcomes"
)


def require_canonical_cache_key(cache_key: str | None, *, context: str = "") -> str:
    """Reject every cache key other than the frozen research-matchup key."""
    if cache_key != CANONICAL_RESEARCH_MATCHUP_CACHE_KEY:
        location = f" ({context})" if context else ""
        raise RuntimeError(
            "Refusing to use a non-canonical research-matchup lineage"
            f"{location}: {cache_key!r}. "
            "Creating or selecting a new research-matchup lineage is illegal. "
            f"Use exactly {CANONICAL_RESEARCH_MATCHUP_CACHE_KEY!r}."
        )
    return cache_key


def require_canonical_lineage(lineage: str | None, *, context: str = "") -> str:
    """Reject every artifact lineage other than the frozen canonical lineage."""
    if lineage != CANONICAL_RESEARCH_MATCHUP_LINEAGE:
        location = f" ({context})" if context else ""
        raise RuntimeError(
            "Refusing to create or promote a non-canonical research-matchup "
            f"lineage{location}: {lineage!r}. "
            "Only the frozen canonical research-matchup lineage is legal: "
            f"{CANONICAL_RESEARCH_MATCHUP_LINEAGE!r}."
        )
    return lineage
