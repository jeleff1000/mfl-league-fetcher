import pandas as pd
from multi_league.external_ingest.apply_mappings import (
    apply_mappings,
    ColumnMap,
    IdentityDecision,
    synthesize_external_guid,
)


def test_synthesize_external_guid_is_deterministic():
    g1 = synthesize_external_guid("kmffl", "Donny T")
    g2 = synthesize_external_guid("kmffl", "Donny T")
    assert g1 == g2
    assert g1.startswith("external_")
    assert len(g1) == len("external_") + 16


def test_synthesize_external_guid_differs_per_input():
    assert synthesize_external_guid("kmffl", "X") != synthesize_external_guid("kmffl", "Y")
    assert synthesize_external_guid("kmffl", "X") != synthesize_external_guid("other", "X")


def test_synthesize_external_guid_is_namespaced_by_platform():
    # Two leagues that share a league_key string across different platforms
    # must not collide. Same guard against shared league_name slugs.
    a = synthesize_external_guid("nfl.l.123", "Donny", platform="yahoo")
    b = synthesize_external_guid("nfl.l.123", "Donny", platform="sleeper")
    assert a != b


def test_user_decision_overrides_existing_manager_guid_in_source():
    # Regression: source file contained a stale/garbage manager_guid for "Ezra".
    # The wizard's identity decision (owner_guid='real_g1') must win, otherwise
    # rows route to the wrong franchise.
    df = pd.DataFrame(
        {
            "yr": [2013, 2013],
            "wk": [1, 2],
            "owner": ["Ezra", "Ezra"],
            "manager_guid": ["stale_garbage", "stale_garbage"],
        }
    )
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={"year": "yr", "week": "wk", "manager": "owner", "manager_guid": "manager_guid"},
            )
        ],
        identity_decisions={"Ezra": IdentityDecision(franchise_id="g_1", owner_guid="real_g1")},
        league_db="kmffl",
    )
    out = result.dfs["matchup"]
    assert list(out["manager_guid"]) == ["real_g1", "real_g1"]


def test_franchise_id_alone_resolves_to_canonical_manager_guid():
    # Regression for the KMFFL pilot: the wizard persists external_identity_maps
    # as `{"<manager>": {"franchise_id": "<fid>"}}` — owner_guid is NOT supplied.
    # When only franchise_id is set, apply_mappings must still produce a
    # manager_guid that matches the canonical franchise (manager_guid == fid for
    # Yahoo OAuth GUIDs / Sleeper user_ids / ESPN swid). Otherwise the staged
    # rows land in matchup with NULL manager_guid and the franchise_registry
    # synthesizes a `hidden_<slug>` franchise_id that never merges with the
    # canonical Yahoo years.
    df = pd.DataFrame(
        {
            "yr": [2013, 2014],
            "wk": [1, 1],
            "owner": ["Yaacov's Team", "Yaacov's Team"],
        }
    )
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={"year": "yr", "week": "wk", "manager": "owner"},
            )
        ],
        # Production shape: only franchise_id, no owner_guid.
        identity_decisions={"Yaacov's Team": IdentityDecision(franchise_id="2OUWRYUIY4HHEW72H5YVBHPILA")},
        league_db="kmffl",
    )
    out = result.dfs["matchup"]
    assert list(out["manager_guid"]) == [
        "2OUWRYUIY4HHEW72H5YVBHPILA",
        "2OUWRYUIY4HHEW72H5YVBHPILA",
    ]


def test_identity_decisions_apply_without_column_map_for_canonical_staging():
    df = pd.DataFrame(
        {
            "year": [2025, 2025],
            "manager": ["Iossi", "Joshua"],
            "player": ["Jakobi Meyers", "Kenneth Walker III"],
        }
    )
    result = apply_mappings(
        staged_dfs={"draft": df},
        column_maps=[],
        identity_decisions={
            "Iossi": IdentityDecision(franchise_id="tyler_fid", owner_guid="tyler_guid"),
            "Joshua": IdentityDecision(franchise_id="josh_fid", owner_guid="josh_guid"),
        },
        league_db="live_draft_beer_league",
        platform="yahoo",
    )

    out = result.dfs["draft"]
    assert out["manager_guid"].tolist() == ["tyler_guid", "josh_guid"]
    assert out["manager"].tolist() == ["Iossi", "Joshua"]


def test_identity_decision_can_rewrite_external_manager_display_name():
    df = pd.DataFrame(
        {
            "year": [2025],
            "manager": ["Iossi"],
            "player": ["Ja'Marr Chase"],
        }
    )
    result = apply_mappings(
        staged_dfs={"draft": df},
        column_maps=[],
        identity_decisions={
            "Iossi": IdentityDecision(
                franchise_id="tyler_fid",
                owner_guid="tyler_guid",
                display_name="Tyler",
            ),
        },
        league_db="live_draft_beer_league",
        platform="espn",
    )

    out = result.dfs["draft"]
    assert out["manager_guid"].tolist() == ["tyler_guid"]
    assert out["manager"].tolist() == ["Tyler"]


def test_existing_manager_guid_survives_for_managers_with_no_decision():
    # Inverse of the above: when there is no identity decision for a manager,
    # an existing real guid in the source file must NOT be wiped.
    df = pd.DataFrame(
        {
            "yr": [2013, 2013],
            "wk": [1, 1],
            "owner": ["Mapped", "Unmapped"],
            "manager_guid": ["stale", "real_existing"],
        }
    )
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={"year": "yr", "week": "wk", "manager": "owner", "manager_guid": "manager_guid"},
            )
        ],
        identity_decisions={"Mapped": IdentityDecision(franchise_id="g_1", owner_guid="real_g1")},
        league_db="kmffl",
    )
    out = result.dfs["matchup"]
    by_owner = dict(zip(out["manager"], out["manager_guid"]))
    assert by_owner["Mapped"] == "real_g1"
    assert by_owner["Unmapped"] == "real_existing"
    assert "Unmapped" in result.unmapped_managers


def test_apply_renames_columns_per_column_map():
    df = pd.DataFrame({"yr": [2013], "wk": [1], "owner": ["Ezra"], "opp": ["Marc"], "pf": [100.0], "pa": [90.0]})
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={
                    "year": "yr",
                    "week": "wk",
                    "manager": "owner",
                    "opponent": "opp",
                    "team_points": "pf",
                    "opponent_points": "pa",
                },
            )
        ],
        identity_decisions={"Ezra": IdentityDecision(franchise_id="g_1", owner_guid="real_g1")},
        league_db="kmffl",
    )
    out = result.dfs["matchup"]
    assert set(out.columns) >= {"year", "week", "manager", "opponent", "team_points", "opponent_points"}
    # Source columns dropped
    assert "yr" not in out.columns


def test_apply_drops_unmapped_columns():
    df = pd.DataFrame(
        {
            "year": [2013],
            "week": [1],
            "manager": ["Ezra"],
            "opponent": ["Marc"],
            "team_points": [100.0],
            "opponent_points": [90.0],
            "gpa": [3.5],
            "felo_score": [600.0],
        }
    )  # enrichment columns to drop
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={
                    "year": "year",
                    "week": "week",
                    "manager": "manager",
                    "opponent": "opponent",
                    "team_points": "team_points",
                    "opponent_points": "opponent_points",
                },
            )
        ],
        identity_decisions={"Ezra": IdentityDecision(franchise_id="g_1", owner_guid="real_g1")},
        league_db="kmffl",
    )
    out = result.dfs["matchup"]
    assert "gpa" not in out.columns
    assert "felo_score" not in out.columns


def test_apply_synthesizes_guid_for_existing_franchise_mapping():
    df = pd.DataFrame(
        {
            "year": [2013],
            "week": [1],
            "manager": ["Ezra"],
            "opponent": ["Marc"],
            "team_points": [100.0],
            "opponent_points": [90.0],
        }
    )
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={
                    "year": "year",
                    "week": "week",
                    "manager": "manager",
                    "opponent": "opponent",
                    "team_points": "team_points",
                    "opponent_points": "opponent_points",
                },
            )
        ],
        identity_decisions={"Ezra": IdentityDecision(franchise_id="g_1", owner_guid="real_g1")},
        league_db="kmffl",
    )
    out = result.dfs["matchup"]
    assert out["manager_guid"].iloc[0] == "real_g1"


def test_apply_synthesizes_external_guid_for_create_new():
    df = pd.DataFrame(
        {
            "year": [2013],
            "week": [1],
            "manager": ["NewGuy"],
            "opponent": ["Marc"],
            "team_points": [100.0],
            "opponent_points": [90.0],
        }
    )
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={
                    "year": "year",
                    "week": "week",
                    "manager": "manager",
                    "opponent": "opponent",
                    "team_points": "team_points",
                    "opponent_points": "opponent_points",
                },
            )
        ],
        identity_decisions={"NewGuy": IdentityDecision(create_new=True)},
        league_db="kmffl",
    )
    out = result.dfs["matchup"]
    expected_guid = synthesize_external_guid("kmffl", "NewGuy")
    assert out["manager_guid"].iloc[0] == expected_guid


def test_apply_drops_rows_for_ignored_managers():
    df = pd.DataFrame(
        {
            "year": [2013, 2013],
            "week": [1, 1],
            "manager": ["Ezra", "TEST_USER"],
            "opponent": ["Marc", "Donny"],
            "team_points": [100.0, 0.0],
            "opponent_points": [90.0, 0.0],
        }
    )
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={
                    "year": "year",
                    "week": "week",
                    "manager": "manager",
                    "opponent": "opponent",
                    "team_points": "team_points",
                    "opponent_points": "opponent_points",
                },
            )
        ],
        identity_decisions={
            "Ezra": IdentityDecision(franchise_id="g_1", owner_guid="real_g1"),
            "TEST_USER": IdentityDecision(ignore=True),
        },
        league_db="kmffl",
    )
    out = result.dfs["matchup"]
    assert len(out) == 1
    assert "TEST_USER" not in out["manager"].values
    assert result.ignored_rows_dropped == {"TEST_USER": 1}


def test_apply_preserves_platform_identity_columns_when_unmapped():
    # Regression for KMFFL 2014: the player_fantasy manifest does NOT include
    # yahoo_player_id as a slot, so the wizard's column_map never references
    # it. Pre-fix, apply_mappings stripped yahoo_player_id from staging rows.
    # Downstream the staging-merge dedup falls back to (year, week,
    # yahoo_player_id, manager) when player_week is NULL — and pandas treats
    # all the NULL yahoo_player_id rows as equal, collapsing 2,560 staging
    # rows to ~160 unique (year, week, NULL, manager) combos. That destroyed
    # all the BN/IR/FLX lineup info for 2014 and prevented the franchise
    # merge with the canonical 2015+ Yahoo years.
    #
    # Fix: apply_mappings preserves stable platform identity columns
    # (yahoo_player_id, sleeper_player_id, espn_player_id, NFL_player_id,
    # player_week, player_key) even when they aren't in column_map. These
    # are stable IDs, not enrichments — no staleness risk from old exports.
    df = pd.DataFrame(
        {
            "year": [2014, 2014],
            "week": [1, 1],
            "owner": ["Ezra", "Marc"],
            "player": ["Drew Brees", "Adrian Peterson"],
            "points": [25.0, 18.0],
            "fantasy_position": ["QB", "RB"],
            "yahoo_player_id": [5479, 8261],
            "player_key": ["331.p.5479", "331.p.8261"],
        }
    )
    result = apply_mappings(
        staged_dfs={"player_fantasy": df},
        column_maps=[
            ColumnMap(
                table="player_fantasy",
                # Wizard maps only the manifest slots — yahoo_player_id and
                # player_key are NOT slots in PLAYER_FANTASY_MANIFEST.
                column_map={
                    "year": "year",
                    "week": "week",
                    "manager": "owner",
                    "player": "player",
                    "points": "points",
                    "fantasy_position": "fantasy_position",
                },
            )
        ],
        identity_decisions={
            "Ezra": IdentityDecision(franchise_id="g_ezra"),
            "Marc": IdentityDecision(franchise_id="g_marc"),
        },
        league_db="kmffl",
    )
    out = result.dfs["player_fantasy"]
    # Platform IDs survived even though they weren't in column_map
    assert (
        "yahoo_player_id" in out.columns
    ), "yahoo_player_id must be preserved from source — needed for staging-merge dedup"
    assert "player_key" in out.columns, "player_key must be preserved from source"
    assert list(out["yahoo_player_id"]) == [5479, 8261]
    assert list(out["player_key"]) == ["331.p.5479", "331.p.8261"]
    # And the canonical slots were renamed correctly
    assert "manager" in out.columns
    assert list(out["manager"]) == ["Ezra", "Marc"]


def test_apply_preserves_draft_identity_hints_when_unmapped():
    df = pd.DataFrame(
        {
            "year": [2025],
            "owner": ["Iossi"],
            "player": ["Jakobi Meyers"],
            "position": ["WR"],
            "nfl_team_api": ["LV"],
        }
    )
    result = apply_mappings(
        staged_dfs={"draft": df},
        column_maps=[
            ColumnMap(
                table="draft",
                column_map={
                    "year": "year",
                    "manager": "owner",
                    "player": "player",
                },
            )
        ],
        identity_decisions={"Iossi": IdentityDecision(franchise_id="tyler_fid", owner_guid="tyler_guid")},
        league_db="live_draft_beer_league",
        platform="yahoo",
    )

    out = result.dfs["draft"]
    assert out["position"].tolist() == ["WR"]
    assert out["nfl_team_api"].tolist() == ["LV"]


def test_apply_does_not_double_add_when_id_already_mapped():
    # If the wizard explicitly mapped a slot to the platform ID column
    # (unusual but possible), don't shadow / overwrite the rename target.
    df = pd.DataFrame(
        {
            "year": [2014],
            "week": [1],
            "owner": ["Ezra"],
            "player": ["Drew Brees"],
            "points": [25.0],
            "fantasy_position": ["QB"],
            "yahoo_player_id": [5479],
        }
    )
    # Pretend the user mapped the canonical "player" slot to the
    # yahoo_player_id source column (weird, but it's a column_map flexibility
    # test — what matters is we don't inject yahoo_player_id twice).
    result = apply_mappings(
        staged_dfs={"player_fantasy": df},
        column_maps=[
            ColumnMap(
                table="player_fantasy",
                column_map={
                    "year": "year",
                    "week": "week",
                    "manager": "owner",
                    "player": "yahoo_player_id",  # weird mapping
                    "points": "points",
                    "fantasy_position": "fantasy_position",
                },
            )
        ],
        identity_decisions={"Ezra": IdentityDecision(franchise_id="g_ezra")},
        league_db="kmffl",
    )
    out = result.dfs["player_fantasy"]
    # `player` slot got the yahoo_player_id value (because that's how the
    # weird column_map specified it). yahoo_player_id should NOT be
    # silently re-injected, because the source column was consumed by
    # the rename — preserving it would shadow the explicit mapping.
    assert out["player"].iloc[0] == 5479
    assert "yahoo_player_id" not in out.columns


def test_apply_counts_unmapped_managers():
    df = pd.DataFrame(
        {
            "year": [2013],
            "week": [1],
            "manager": ["NoDecisionForMe"],
            "opponent": ["Marc"],
            "team_points": [100.0],
            "opponent_points": [90.0],
        }
    )
    result = apply_mappings(
        staged_dfs={"matchup": df},
        column_maps=[
            ColumnMap(
                table="matchup",
                column_map={
                    "year": "year",
                    "week": "week",
                    "manager": "manager",
                    "opponent": "opponent",
                    "team_points": "team_points",
                    "opponent_points": "opponent_points",
                },
            )
        ],
        identity_decisions={},  # No decision for "NoDecisionForMe"
        league_db="kmffl",
    )
    assert result.unmapped_managers_count == 1
    assert "NoDecisionForMe" in result.unmapped_managers
