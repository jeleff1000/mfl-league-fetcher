"""Registry behavior for the typed PFR award source relations."""

from scripts.sota_recon.sources import registry


def test_pfr_award_membership_sources_are_explicit_primary_relations() -> None:
    """Catches awards being hidden in player_bio or split into independent lineages."""
    sources = registry(include_subject=False)

    expected = {
        "pfr_all_pro_members": ("pfr_id+year", "pfr", "primary"),
        "pfr_pro_bowl_members": ("pfr_id+year", "pfr", "primary"),
    }
    assert {
        key: (sources[key].join, sources[key].lineage, sources[key].witness_class)
        for key in expected
    } == expected
