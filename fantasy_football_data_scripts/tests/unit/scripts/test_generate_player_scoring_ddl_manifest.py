from scripts.generate_player_scoring_ddl_manifest import render_manifest
from multi_league.core.canonical_settings import ALL_SCORING_KEYS


def test_render_manifest_is_deterministic_and_exhaustive():
    first = render_manifest(ALL_SCORING_KEYS)
    second = render_manifest(list(ALL_SCORING_KEYS))

    assert first == second
    assert "AUTO-GENERATED" in first
    assert "CANONICAL_SCORING_DDL_KEYS" in first
    for key in ALL_SCORING_KEYS:
        assert f'  "{key}",' in first
