from __future__ import annotations

import pandas as pd

from scripts.build_kmffl_cookie_model import (
    build_team_identity_maps,
    infer_bracket_settings,
    prepare_matchup_source,
    prepare_roster_source,
    settings_payload_to_flat_row,
    prepare_draft_source,
    prepare_transaction_source,
    repair_trade_rows_from_cached_pages,
    map_web_transaction_windows,
)
from scripts.quick_import_kmffl_2025_web import parse_transactions_html


def test_build_team_identity_maps_uses_year_and_team_number() -> None:
    identities = pd.DataFrame(
        [
            {"year": 2025, "team_number": 1, "manager": "Jason", "manager_guid": "guid-a"},
            {"year": 2024, "team_number": 1, "manager": "Old Jason", "manager_guid": "guid-old"},
        ]
    )

    result = build_team_identity_maps(identities)

    assert result[(2025, 1)] == {"manager": "Jason", "manager_guid": "guid-a"}
    assert result[(2024, 1)]["manager"] == "Old Jason"


def test_prepare_roster_source_adds_canonical_identity_fields() -> None:
    roster = pd.DataFrame(
        [
            {
                "year": 2025,
                "week": 1,
                "team_number": 1,
                "yahoo_player_id": "123",
                "player": "Example Player",
                "lineup_position": "QB",
                "is_started": 1,
                "fantasy_points": 20.5,
            }
        ]
    )
    identities = {(2025, 1): {"manager": "Jason", "manager_guid": "guid-a"}}
    team_names = {(2025, 1): "Team A"}
    player_bio = pd.DataFrame(
        [{"yahoo_player_id": 123.0, "NFL_player_id": "nfl-a", "nfl_position": "QB"}]
    )

    result = prepare_roster_source(roster, identities, team_names, player_bio, "461.l.90939")

    row = result.iloc[0]
    assert row["manager"] == "Jason"
    assert row["manager_guid"] == "guid-a"
    assert row["franchise_id"] == "guid-a"
    assert row["team_name"] == "Team A"
    assert row["team_key"] == "461.l.90939.t.1"
    assert row["fantasy_position"] == "QB"
    assert row["NFL_player_id"] == "nfl-a"


def test_map_web_transaction_windows_uses_canonical_matchup_dates() -> None:
    tx = pd.DataFrame([{"year": 2025, "timestamp": 1757289600}])
    windows = pd.DataFrame(
        [
            {"year": 2025, "week": 1, "week_start": "2025-09-04", "week_end": "2025-09-08"},
            {"year": 2025, "week": 2, "week_start": "2025-09-11", "week_end": "2025-09-15"},
        ]
    )

    result = map_web_transaction_windows(tx, windows)

    assert int(result.iloc[0]["week"]) == 1
    assert result.iloc[0]["week_start"] == "2025-09-04"


def test_prepare_matchup_source_resolves_both_team_identities() -> None:
    matchups = pd.DataFrame(
        [
            {
                "year": 2025,
                "week": 1,
                "team_number": 1,
                "opponent_team_number": 2,
                "team": "Team A",
                "opponent": "Team B",
                "team_points": 100.0,
                "opponent_points": 90.0,
            }
        ]
    )
    identities = {
        (2025, 1): {"manager": "Jason", "manager_guid": "guid-a"},
        (2025, 2): {"manager": "Joe", "manager_guid": "guid-b"},
    }

    result = prepare_matchup_source(matchups, identities, "461.l.90939")

    row = result.iloc[0]
    assert row["manager"] == "Jason"
    assert row["manager_guid"] == "guid-a"
    assert row["opponent"] == "Joe"
    assert row["opponent_guid"] == "guid-b"
    assert row["team_key"] == "461.l.90939.t.1"
    assert row["opponent_team_key"] == "461.l.90939.t.2"


def test_settings_payload_to_flat_row_preserves_scoring_and_roster() -> None:
    payload = {
        "metadata": {
            "num_teams": 10,
            "draft_type": "offline",
            "playoff_start_week": 15,
            "num_playoff_teams": 6,
            "uses_fractional_points": True,
        },
        "roster_positions": ["QB", "RB", "RB", "W/R/T", "BN"],
        "scoring": {"scoring_rec": 0.5, "scoring_pass_td": 4.0},
    }

    row = settings_payload_to_flat_row(payload, 2025, "461.l.90939", "kmffl")

    assert row["db_name"] == "kmffl"
    assert row["year"] == 2025
    assert row["roster_QB"] == 1
    assert row["roster_RB"] == 2
    assert row["scoring_rec"] == 0.5
    assert row["scoring_pass_td"] == 4.0


def test_settings_payload_to_flat_row_prefers_observed_standings_team_count() -> None:
    payload = {"metadata": {"num_teams": 12}, "roster_positions": [], "scoring": {}}

    row = settings_payload_to_flat_row(
        payload,
        2010,
        "242.l.302674",
        "the_booze_bowl",
        observed_team_count=8,
    )

    assert row["num_teams"] == 8


def test_infer_bracket_settings_requires_the_complete_six_team_shape() -> None:
    rows = [
        {"year": 2015, "week": 13} for _ in range(10)
    ] + [
        {"year": 2015, "week": 14} for _ in range(4)
    ] + [
        {"year": 2015, "week": 15} for _ in range(10)
    ] + [
        {"year": 2015, "week": 16} for _ in range(8)
    ]

    assert infer_bracket_settings(pd.DataFrame(rows), 2015) == {
        "playoff_teams": 6,
        "playoff_start_week": 14,
        "regular_season_weeks": 13,
        "end_week": 16,
    }
    assert infer_bracket_settings(pd.DataFrame(rows[:-1]), 2015) is None


def test_infer_bracket_settings_supports_archived_ten_team_two_week_shape() -> None:
    rows = [
        {"year": 2015, "week": week}
        for week in range(1, 17)
        for _ in range(10 if week < 15 else 8)
    ]

    assert infer_bracket_settings(pd.DataFrame(rows), 2015) == {
        "playoff_teams": 4,
        "playoff_start_week": 15,
        "regular_season_weeks": 14,
        "end_week": 16,
    }


def test_infer_bracket_settings_prefers_literal_yahoo_bracket_sections() -> None:
    rows = [
        {
            "year": 2025,
            "week": week,
            "team_number": team_number,
            "is_playoffs": int(week >= 14),
            "is_consolation": int(week >= 14 and team_number >= 7),
        }
        for week in range(1, 17)
        for team_number in range(1, 11)
    ]
    # First-round byes mean championship teams 1 and 2 do not appear until
    # the second bracket week. The union across literal championship cards is
    # the six-team playoff field despite every weekly card count staying flat.
    for row in rows:
        if row["week"] == 14 and row["team_number"] in {1, 2}:
            row["is_consolation"] = 1
        elif row["week"] >= 14 and row["team_number"] in {1, 2, 3, 4, 5, 6}:
            row["is_consolation"] = 0

    assert infer_bracket_settings(pd.DataFrame(rows), 2025) == {
        "playoff_teams": 6,
        "playoff_start_week": 14,
        "regular_season_weeks": 13,
        "end_week": 16,
    }


def test_infer_bracket_settings_backfills_a_late_reduced_week_after_a_complete_schedule() -> None:
    """Archived Yahoo settings can omit playoffs despite a complete scorecard history."""
    rows = [
        {"year": 2018, "week": week}
        for week in range(1, 17)
        for _ in range(10 if week != 15 else 4)
    ]

    assert infer_bracket_settings(pd.DataFrame(rows), 2018) == {
        "playoff_start_week": 15,
        "regular_season_weeks": 14,
        "end_week": 16,
    }


def test_infer_bracket_settings_backfills_a_three_week_legacy_bracket() -> None:
    rows = [
        {"year": 2020, "week": week}
        for week in range(1, 17)
        for _ in range(12 if week not in {14, 16} else 8)
    ]

    assert infer_bracket_settings(pd.DataFrame(rows), 2020) == {
        "playoff_start_week": 14,
        "regular_season_weeks": 13,
        "end_week": 16,
    }


def test_infer_bracket_settings_backfills_completed_reduced_postseason_suffix() -> None:
    """Completed Yahoo brackets can stay reduced through the final scorecard."""
    old_ten_team_rows = [
        {"year": 2009, "week": week}
        for week in range(1, 17)
        for _ in range(10 if week < 14 else {14: 4, 15: 6, 16: 4}[week])
    ]
    modern_twelve_team_rows = [
        {"year": 2018, "week": week}
        for week in range(1, 17)
        for _ in range(12 if week < 15 else 8)
    ]

    assert infer_bracket_settings(pd.DataFrame(old_ten_team_rows), 2009) == {
        "playoff_start_week": 14,
        "regular_season_weeks": 13,
        "end_week": 16,
    }
    assert infer_bracket_settings(pd.DataFrame(modern_twelve_team_rows), 2018) == {
        "playoff_start_week": 15,
        "regular_season_weeks": 14,
        "end_week": 16,
    }


def test_prepare_draft_source_detects_draft_type_per_year() -> None:
    draft = pd.DataFrame(
        [
            {"year": 2015, "pick": 1, "player": "A", "team": "Team A", "cost": None},
            {"year": 2025, "pick": 1, "player": "B", "team": "Team A", "cost": 10},
        ]
    )
    result = prepare_draft_source(
        draft,
        {(2015, 1): "Team A", (2025, 1): "Team A"},
        {
            (2015, 1): {"manager": "M", "manager_guid": "g1"},
            (2025, 1): {"manager": "M", "manager_guid": "g1"},
        },
        {2015: "348.l.89552", 2025: "461.l.90939"},
    )

    assert result.set_index("year")["draft_type"].to_dict() == {2015: "snake", 2025: "auction"}


def test_prepare_draft_source_uses_each_season_observed_team_count_for_rounds() -> None:
    draft = pd.DataFrame(
        [
            {"year": 2010, "pick": 9, "player": "A", "team": "Team A", "cost": None},
            {"year": 2018, "pick": 9, "player": "B", "team": "Team B", "cost": None},
        ]
    )
    result = prepare_draft_source(
        draft,
        {(2010, 1): "Team A", (2018, 1): "Team B"},
        {
            (2010, 1): {"manager": "A", "manager_guid": "g1"},
            (2018, 1): {"manager": "B", "manager_guid": "g2"},
        },
        {2010: "242.l.302674", 2018: "380.l.62205"},
        team_count=12,
        team_count_by_year={2010: 8, 2018: 12},
    )

    assert result.set_index("year")["round"].to_dict() == {2010: 2, 2018: 1}


def test_prepare_draft_source_recovers_missing_player_ids_from_rosters() -> None:
    draft = pd.DataFrame(
        [{"year": 2015, "pick": 1, "player": "Example Player", "team": "Team A", "cost": None}]
    )
    roster = pd.DataFrame(
        [{"year": 2015, "player": "Example Player", "yahoo_player_id": "12345"}]
    )
    result = prepare_draft_source(
        draft,
        {(2015, 1): "Team A"},
        {(2015, 1): {"manager": "M", "manager_guid": "g1"}},
        {2015: "348.l.89552"},
        roster=roster,
    )

    assert result.iloc[0]["yahoo_player_id"] == "12345"


def test_prepare_transaction_source_prefers_season_team_number_over_name() -> None:
    transactions = pd.DataFrame(
        [
            {
                "year": 2025,
                "editorial_player_id": "123",
                "player": "Example Player",
                "action": "Added Player",
                "team": "Display Name That Could Change",
                "team_number": 2,
                "timestamp": "Sep 10, 8:00 pm",
            }
        ]
    )
    result = prepare_transaction_source(
        transactions,
        {(2025, 2): "Canonical Display Name"},
        {(2025, 2): {"manager": "Jason", "manager_guid": "guid-a"}},
        {2025: "461.l.90939"},
    )

    row = result.iloc[0]
    assert row["manager_guid"] == "guid-a"
    assert row["franchise_id"] == "guid-a"
    assert row["team_key"] == "461.l.90939.t.2"
    assert pd.notna(row["timestamp"])


def test_prepare_transaction_source_preserves_web_normalized_fields() -> None:
    source = pd.DataFrame(
        [
            {
                "year": 2025,
                "action": "Added Player",
                "transaction_id": "web-2025-000042",
                "transaction_type": "add",
                "source_type": "waivers",
                "destination": "team",
                "status": "successful",
                "faab_bid": 14,
                "timestamp": "Oct 29,4:52 am",
                "editorial_player_id": "33989",
                "player": "Christian Watson",
                "team": "Alpha",
                "team_number": 1,
            }
        ]
    )

    result = prepare_transaction_source(
        source,
        {(2025, 1): "Alpha"},
        {(2025, 1): {"manager": "Alice", "manager_guid": "guid-1"}},
        {2025: "461.l.90939"},
    )

    row = result.iloc[0]
    assert row["transaction_id"] == "web-2025-000042"
    assert row["source_type"] == "waivers"
    assert row["destination"] == "team"
    assert row["status"] == "successful"
    assert row["faab_bid"] == 14


def test_prepare_transaction_source_recovers_defense_id_from_roster() -> None:
    transactions = pd.DataFrame(
        [
            {
                "year": 2025,
                "editorial_player_id": "team:detroit",
                "player": "Lions",
                "action": "Added Player",
                "team": "Team A",
                "team_number": 1,
                "timestamp": "Sep 10, 8:00 pm",
            }
        ]
    )
    roster = pd.DataFrame([{"year": 2024, "player": "Lions", "yahoo_player_id": "100001"}])

    result = prepare_transaction_source(
        transactions,
        {(2025, 1): "Team A"},
        {(2025, 1): {"manager": "Jason", "manager_guid": "guid-a"}},
        {2025: "461.l.90939"},
        roster=roster,
    )

    assert result.iloc[0]["yahoo_player_id"] == "100001"


def test_prepare_transaction_source_only_populates_trade_destinations_for_trades() -> None:
    transactions = pd.DataFrame(
        [
            {
                "year": 2025,
                "editorial_player_id": "123",
                "player": "Example Player",
                "action": "Added Player",
                "team": "Team A",
                "team_number": 1,
                "timestamp": "Sep 10, 8:00 pm",
            }
        ]
    )
    result = prepare_transaction_source(
        transactions,
        {(2025, 1): "Team A"},
        {(2025, 1): {"manager": "Jason", "manager_guid": "guid-a"}},
        {2025: "461.l.90939"},
    )

    row = result.iloc[0]
    assert pd.isna(row["destination_manager_guid"])
    assert pd.isna(row["destination_team_name"])


def test_prepare_transaction_source_emits_both_trade_perspectives() -> None:
    transactions = pd.DataFrame(
        [
            {
                "year": 2025,
                "editorial_player_id": "123",
                "player": "Example Player",
                "action": "Trade",
                "team": "Alpha",
                "team_number": 1,
                "trade_partner_team": "Beta",
                "trade_partner_team_number": 2,
                "timestamp": "Sep 10, 8:00 pm",
            }
        ]
    )
    result = prepare_transaction_source(
        transactions,
        {(2025, 1): "Alpha", (2025, 2): "Beta"},
        {
            (2025, 1): {"manager": "Alice", "manager_guid": "guid-a"},
            (2025, 2): {"manager": "Bob", "manager_guid": "guid-b"},
        },
        {2025: "461.l.90939"},
    )

    assert result[["manager_guid", "source_manager_guid", "trade_direction"]].to_dict("records") == [
        {"manager_guid": "guid-a", "source_manager_guid": "guid-b", "trade_direction": "received"},
        {"manager_guid": "guid-b", "source_manager_guid": "guid-a", "trade_direction": "sent"},
    ]


def test_parse_transactions_html_extracts_team_number_from_team_link() -> None:
    html = """
    <tr>
      <td><a href="https://sports.yahoo.com/nfl/players/123">Example Player</a></td>
      <td><span class="F-position">QB</span></td>
      <td><a class="Tst-team-name" href="http://football.fantasysports.yahoo.com/2025/f1/90939/2">Team A</a></td>
      <td><span class="F-timestamp">Sep 10, 8:00 pm</span></td>
    </tr>
    """

    rows = parse_transactions_html(html, page_start=0)

    assert rows[0]["team_number"] == 2


def test_repair_trade_rows_handles_multi_asset_two_sided_trade(tmp_path) -> None:
    html = """
    <table>
      <tr><td class="F-trade"><a href="https://sports.yahoo.com/nfl/players/101">One</a>
        <a href="https://sports.yahoo.com/nfl/players/102">Two</a>
        <a href="https://sports.yahoo.com/nfl/players/103">Three</a>
        <span>Traded to</span><a href="/2025/f1/90939/6">Alpha</a><span class="F-timestamp">Nov 25, 9:38 pm</span></td></tr>
      <tr><td><a href="https://sports.yahoo.com/nfl/players/201">Four</a>
        <a href="https://sports.yahoo.com/nfl/players/202">Five</a>
        <a href="https://sports.yahoo.com/nfl/players/203">Six</a>
        <span>Traded to</span><a href="/2025/f1/90939/9">Beta</a><span class="F-timestamp">Nov 25, 9:38 pm</span></td></tr>
    </table>
    """
    transaction_dir = tmp_path / "2025" / "transactions"
    transaction_dir.mkdir(parents=True)
    (transaction_dir / "page_001.html").write_text(html, encoding="utf-8")
    transactions = pd.DataFrame(
        [
            {"year": 2025, "editorial_player_id": str(player_id), "action": "", "timestamp": "Nov 25, 9:38 pm"}
            for player_id in (101, 102, 103, 201, 202, 203)
        ]
    )

    result = repair_trade_rows_from_cached_pages(transactions, tmp_path)

    assert result["action"].tolist() == ["Trade"] * 6
    assert result["trade_partner_team_number"].tolist() == [9, 9, 9, 6, 6, 6]
