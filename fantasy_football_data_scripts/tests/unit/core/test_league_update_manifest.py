from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

import pytest

from multi_league.core.league_update_manifest import (
    NFL_PRECOMPUTED_SCORING_PREFIXES,
    NFL_SCORING_INPUT_COLUMNS,
    LeagueSegment,
    ResourceRevision,
    SourceManifest,
    build_nfl_revision_rows,
    canonical_manifest_json,
    diff_manifests,
    manifest_digest,
    nfl_game_revision,
    resolve_nfl_scoring_input_columns,
    source_manifest_from_mapping,
)
from multi_league.transformations.common.sql_base import (
    BONUS_COL_MAP,
    DEF_COL_MAP,
    IDP_COL_MAP,
    KICK_COL_MAP,
)


FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "league_update_manifest_v1.json"


def _manifest(*, base_generation: int = 7, observed_at: str = "2026-09-14T12:00:00Z") -> SourceManifest:
    return SourceManifest(
        schema_version=1,
        database_name="keeper_league",
        active_season=2026,
        segments=(
            LeagueSegment(
                provider="yahoo",
                active_league_id="470.l.80971",
                seasons=(2025, 2026),
                renewal_chain=((2025, "461.l.90939"), (2026, "470.l.80971")),
            ),
        ),
        nfl_revisions=(
            ResourceRevision("nfl", "game", "2026:10:BUF@MIA", "nfl-week-10"),
            ResourceRevision("nfl", "game", "2026:2:BUF@MIA", "nfl-week-2"),
        ),
        provider_revisions=(
            ResourceRevision("yahoo", "transactions", "2026", "tx-empty", "ok", 0, 0),
            ResourceRevision("yahoo", "rosters", "2026:10", "roster-10", "ok", 10, 10),
            ResourceRevision("yahoo", "settings", "2026", "settings-1", "ok", 1, 1),
        ),
        base_generation=base_generation,
        observed_at=observed_at,
    )


def test_canonical_manifest_sorts_structured_week_scopes_numerically():
    """A week-10 entry must not sort ahead of week 2 as a preformatted string."""
    payload = canonical_manifest_json(_manifest())

    assert payload.index('"scope":"2026:2:BUF@MIA"') < payload.index('"scope":"2026:10:BUF@MIA"')


def test_source_digest_ignores_snapshot_metadata_but_not_source_content():
    """Probe timestamps and Fly generation cannot make unchanged provider data look stale."""
    original = _manifest()
    same_source_later = replace(original, base_generation=8, observed_at="2026-09-14T12:05:00Z")
    changed_source = replace(
        original,
        provider_revisions=(
            *original.provider_revisions[:-1],
            replace(original.provider_revisions[-1], revision="settings-2"),
        ),
    )

    assert manifest_digest(same_source_later) == manifest_digest(original)
    assert manifest_digest(changed_source) != manifest_digest(original)


def test_source_digest_is_independent_of_resource_and_segment_order():
    """Provider response ordering cannot create a freshness delta."""
    original = _manifest()
    reordered = replace(
        original,
        nfl_revisions=tuple(reversed(original.nfl_revisions)),
        provider_revisions=tuple(reversed(original.provider_revisions)),
    )

    assert canonical_manifest_json(reordered) == canonical_manifest_json(original)
    assert manifest_digest(reordered) == manifest_digest(original)


def test_manifest_rejects_duplicate_resource_identity():
    """Two revisions for one provider/resource/scope are ambiguous and must fail closed."""
    original = _manifest()
    duplicate = replace(
        original,
        provider_revisions=(*original.provider_revisions, original.provider_revisions[0]),
    )

    with pytest.raises(ValueError, match="duplicate resource revision"):
        canonical_manifest_json(duplicate)


def test_idp_passes_defended_change_updates_nfl_game_revision():
    """An IDP correction used by league scoring must change the compact NFL revision."""
    base = {
        "NFL_player_id": "00-0031234",
        "game_date": "2026-09-13",
        "week": 1,
        "season_type": "REG",
        "nfl_team": "BUF",
        "opponent_nfl_team": "MIA",
        "passing_yards": 0,
        "rushing_yards": 0,
        "receiving_yards": 0,
        "def_pass_defended": 1,
    }

    corrected = {**base, "def_pass_defended": 2}

    assert nfl_game_revision([base]) != nfl_game_revision([corrected])


def test_nfl_game_revision_uses_canonical_identity_and_stable_row_order():
    """The compact NFL revision must be stable when DuckDB returns rows in a different order."""
    first = {
        "NFL_player_id": "00-0031234",
        "game_date": "2026-09-13",
        "week": 1,
        "season_type": "REG",
        "nfl_team": "BUF",
        "opponent_nfl_team": "MIA",
        "def_pass_defended": 1,
    }
    second = {
        "NFL_player_id": "00-0035678",
        "game_date": "2026-09-13",
        "week": 1,
        "season_type": "REG",
        "nfl_team": "MIA",
        "opponent_nfl_team": "BUF",
        "def_pass_defended": 0,
    }

    assert nfl_game_revision([first, second]) == nfl_game_revision([second, first])


def test_build_nfl_revision_rows_groups_both_team_sides_into_one_game():
    """One NFL game/week revision must include players from both teams."""
    buf = {
        "NFL_player_id": "00-0031234",
        "game_date": "2026-09-13",
        "year": 2026,
        "week": 2,
        "season_type": "REG",
        "nfl_team": "BUF",
        "opponent_nfl_team": "MIA",
        "def_pass_defended": 1,
    }
    mia = {
        "NFL_player_id": "00-0035678",
        "game_date": "2026-09-13",
        "year": 2026,
        "week": 2,
        "season_type": "REG",
        "nfl_team": "MIA",
        "opponent_nfl_team": "BUF",
        "def_pass_defended": 0,
    }

    rows = build_nfl_revision_rows([buf, mia], schema_version=1)

    assert rows == [
        {
            "season": 2026,
            "week": 2,
            "game_key": "BUF@MIA",
            "revision": nfl_game_revision([buf, mia]),
            "schema_version": 1,
        }
    ]


def test_scoring_revision_registry_uses_unique_source_columns():
    """Duplicate scoring fields waste work and can hide accidental aliases."""
    assert len(NFL_SCORING_INPUT_COLUMNS) == len(set(NFL_SCORING_INPUT_COLUMNS))
    assert "def_pass_defended" in NFL_SCORING_INPUT_COLUMNS


def test_shared_v1_fixture_locks_canonical_json_and_digest():
    """Python and browser contracts must compare the same persisted digest bytes."""
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    manifest = source_manifest_from_mapping(fixture["manifest"])

    assert canonical_manifest_json(manifest) == fixture["canonical_json"]
    assert manifest_digest(manifest) == fixture["digest"]


def test_scoring_schema_resolution_includes_registered_raw_and_all_precomputed_columns():
    """A newly materialized scoring output must participate without a browser code change."""
    columns = resolve_nfl_scoring_input_columns({
        "headshot_url",
        "def_pass_defended",
        "pts_idp_pd",
        "bonus_pass_300yd",
        "rank_qb_4pt",
        "rank_alltime_qb_4pt",
        "recon_correction_log",
    })

    assert columns == (
        "bonus_pass_300yd", "def_pass_defended", "pts_idp_pd",
        "rank_alltime_qb_4pt", "rank_qb_4pt",
    )


def test_explicit_scoring_schema_changes_revision_for_precomputed_output():
    """A precomputed scoring correction must change the compact game revision."""
    base = {
        "NFL_player_id": "00-1",
        "game_date": "2026-09-13",
        "week": 1,
        "season_type": "REG",
        "nfl_team": "BUF",
        "opponent_nfl_team": "MIA",
        "pts_idp_pd": 1.0,
    }

    assert nfl_game_revision([base], scoring_columns=("pts_idp_pd",)) != nfl_game_revision(
        [{**base, "pts_idp_pd": 2.0}],
        scoring_columns=("pts_idp_pd",),
    )


def test_every_quick_pipeline_scoring_mapping_is_covered_by_revision_contract():
    """Adding a mapped scoring output without freshness coverage must fail CI."""
    mapped = set().union(
        DEF_COL_MAP.values(),
        IDP_COL_MAP.values(),
        BONUS_COL_MAP.values(),
        KICK_COL_MAP.values(),
    )
    uncovered = {
        column
        for column in mapped
        if column not in NFL_SCORING_INPUT_COLUMNS
        and not column.startswith(NFL_PRECOMPUTED_SCORING_PREFIXES)
    }

    assert uncovered == set()


def test_manifest_delta_is_structured_and_ignores_snapshot_metadata():
    published = replace(
        _manifest(),
        nfl_revisions=(
            ResourceRevision("nfl", "game", "2026:1:BUF@MIA", "old-game"),
            ResourceRevision("nfl", "game", "2026:2:BUF@MIA", "removed-game"),
        ),
        provider_revisions=(
            ResourceRevision("yahoo", "settings", "2026", "same-settings"),
        ),
        base_generation=10,
        observed_at="2026-09-13T10:00:00Z",
    )
    observed = replace(
        _manifest(),
        nfl_revisions=(
            ResourceRevision("nfl", "game", "2026:1:BUF@MIA", "corrected-game"),
        ),
        provider_revisions=(
            ResourceRevision("yahoo", "settings", "2026", "same-settings"),
            ResourceRevision("yahoo", "transactions", "2026", "new-transactions"),
        ),
        base_generation=99,
        observed_at="2026-09-14T10:00:00Z",
    )

    delta = diff_manifests(observed, published)

    assert delta.identity_changed is False
    assert delta.segments_changed is False
    assert [item.scope for item in delta.nfl_changed] == ["2026:1:BUF@MIA"]
    assert [item.scope for item in delta.nfl_removed] == ["2026:2:BUF@MIA"]
    assert [item.resource for item in delta.provider_added] == ["transactions"]
    assert delta.is_empty is False


def test_manifest_delta_rejects_cross_league_comparison():
    observed = replace(_manifest(), database_name="other_league")

    with pytest.raises(ValueError, match="different leagues"):
        diff_manifests(observed, _manifest())
