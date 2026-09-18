"""Yahoo identity -> transaction SQL -> strict homepage trade contract."""

from xml.etree import ElementTree as ET

import duckdb
import pandas as pd
import pytest

from multi_league.core.canonical_matchup import create_matchup_table_sql, normalize_matchup_df
from multi_league.core.canonical_player import create_player_fantasy_table_sql
from multi_league.core.canonical_schedule import create_schedule_table_sql, normalize_schedule_df
from multi_league.core.canonical_transaction import create_transaction_table_sql, normalize_transaction_df
from multi_league.data_fetchers.yahoo import yahoo_transactions as transactions
from multi_league.data_fetchers.yahoo.yahoo_identity import resolve_yahoo_manager_guids
from multi_league.data_fetchers.yahoo.yahoo_matchups import extract_team
from multi_league.data_fetchers.yahoo.yahoo_schedules import _derive_schedule_df_from_matchup_df
from multi_league.transformations.aggregation.homepage_summary import _compute_best_trade
from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.matchup.sql_matchup_enrichments import MatchupEnrichmentsMixin
from multi_league.transformations.transaction.sql_transaction_enrichments import TransactionEnrichmentsMixin


class _TradeRunner(MatchupEnrichmentsMixin, TransactionEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def _team_xml(slot, name, profile):
    return f"""<team><team_key>461.l.1.t.{slot}</team_key><name>{name}</name>
        <managers><manager><guid>--hidden--</guid><nickname>{name}</nickname>
        <image_url>https://s.yimg.com/ag/images/{profile}_64sq.jpg</image_url>
        </manager></managers></team>"""


def _asset_xml(player_id, source, destination):
    return f"""<player><player_key>461.p.{player_id}</player_key>
        <name><full>Player {player_id}</full></name><transaction_data>
        <type>trade</type><source_type>team</source_type><destination_type>team</destination_type>
        <source_team_key>461.l.1.t.{source}</source_team_key>
        <destination_team_key>461.l.1.t.{destination}</destination_team_key>
        </transaction_data></player>"""


@pytest.mark.parametrize(
    "guid,nickname,image,expected",
    [
        ("real-guid", "Alpha", "shared", "real-guid"),
        ("--hidden--", "Alpha", "shared", "yh-img-shared"),
        ("--hidden--", "Alpha", "default_user_profile", "yh-nick-alpha"),
        ("--hidden--", "--hidden--", "default_user_profile", "yh-team-461.l.1.t.4"),
    ],
)
def test_transaction_team_mapping_preserves_yahoo_identity_inputs(monkeypatch, guid, nickname, image, expected):
    team = ET.fromstring(_team_xml(4, "Team Name", image))
    team.find(".//manager/guid").text = guid
    team.find(".//manager/nickname").text = nickname
    root = ET.Element("teams")
    root.append(team)
    monkeypatch.setattr(transactions, "fetch_url", lambda url, oauth: root)
    _, guids, _ = transactions.fetch_team_mappings(None, "461.l.1")
    assert guids == {"461.l.1.t.4": expected}


@pytest.fixture
def yahoo_trade_pipeline(monkeypatch, tmp_path):
    # NFL14's provider shape: hidden GUIDs and two team slots sharing a profile
    # identity. The schedule retains that base identity after matchup splits it.
    teams = ET.fromstring(
        "<teams>"
        + "".join(
            [
                _team_xml(4, "Alpha", "alpha"),
                _team_xml(8, "Beta", "shared"),
                _team_xml(10, "Gamma", "gamma"),
                _team_xml(11, "Delta", "shared"),
            ]
        )
        + "</teams>"
    )
    trades = ET.fromstring(
        "<transactions>"
        + "".join(
            f"""<transaction><transaction_key>{tx}</transaction_key><type>trade</type>
        <status>successful</status><timestamp>1757000000</timestamp><players>{assets}</players>
        </transaction>"""
            for tx, assets in [
                ("166", _asset_xml(101, 4, 8) + _asset_xml(102, 8, 4)),
                ("298", _asset_xml(103, 11, 10) + _asset_xml(104, 10, 11)),
            ]
        )
        + "</transactions>"
    )

    def provider_response(url, oauth):
        if url.endswith("/teams"):
            return teams
        assert "/transactions;" in url
        return trades

    monkeypatch.setattr(transactions, "fetch_url", provider_response)
    names, guids, team_names = transactions.fetch_team_mappings(None, "461.l.1")
    windows = pd.DataFrame(columns=["year", "week", "week_start", "week_end", "cumulative_week"])
    fetched = transactions.fetch_transactions_for_year(None, "461.l.1", 2025, names, guids, team_names, windows)
    txn_df = normalize_transaction_df(transactions.transactions_to_dataframe(fetched), "yahoo", "461.l.1")

    matchup_df = pd.DataFrame(
        [
            dict(extract_team(team, league_key="461.l.1"), year=2025, week=1, opponent="Opponent", opponent_points=0)
            for team in teams.findall("team")
        ]
    )
    matchup_df["manager_guid"] = resolve_yahoo_manager_guids(matchup_df)
    schedule_df = normalize_schedule_df(_derive_schedule_df_from_matchup_df(matchup_df, 2025, {}), "yahoo")
    matchup_df = normalize_matchup_df(matchup_df, "yahoo")
    for frame in (txn_df, matchup_df, schedule_df):
        frame["db_name"] = "yahoo_trade_fixture"

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        for ddl in (
            create_matchup_table_sql,
            create_schedule_table_sql,
            create_transaction_table_sql,
            create_player_fantasy_table_sql,
        ):
            conn.execute(ddl("memory"))
        for table, frame in (("matchup", matchup_df), ("schedule", schedule_df), ("transactions", txn_df)):
            conn.register("fixture_rows", frame)
            conn.execute(f"INSERT INTO public.{table} BY NAME SELECT * FROM fixture_rows")
            conn.unregister("fixture_rows")
        conn.execute("ATTACH ':memory:' AS ___ops")
        conn.execute("CREATE SCHEMA ___ops.nfl_historical")
        conn.execute("""CREATE TABLE ___ops.nfl_historical.player_bio
            (NFL_player_id VARCHAR, yahoo_player_id VARCHAR, player VARCHAR, headshot_url VARCHAR)""")
        runner = _TradeRunner(db_name="yahoo_trade_fixture", data_dir=str(tmp_path), conn=conn)
        runner.resolve_hidden_managers()
        runner.populate_franchise_id()
        # Hand-specified test earnings, deliberately different on sent rows.
        # The trade calculation must copy received earnings, never value sent
        # assets independently or hide the error by zeroing them.
        conn.execute("""UPDATE public.transactions SET manager_lamar_ros_managed =
            CASE WHEN trade_direction='sent' THEN 999 ELSE
                CASE yahoo_player_id WHEN '101' THEN 10 WHEN '102' THEN 3
                                     WHEN '103' THEN 8 WHEN '104' THEN 2 END END
            WHERE db_name='yahoo_trade_fixture'""")
        yield runner
    finally:
        conn.close()


def test_hidden_yahoo_trade_identities_preserve_all_eight_mirrored_valuations(yahoo_trade_pipeline):
    runner = yahoo_trade_pipeline
    # A repeat must preserve both the earned values and balanced packages.
    for _ in range(2):
        runner._compute_trade_net_lamar()
        highlight = _compute_best_trade(runner.conn, "yahoo_trade_fixture", platform="yahoo")
        assert (highlight["winner"], highlight["winner_lamar"], highlight["loser_lamar"], highlight["net_lamar"]) == (
            "Beta",
            10,
            3,
            7,
        )
        assert runner.conn.execute("""SELECT yahoo_player_id, trade_direction, trade_asset_lamar
            FROM public.transactions WHERE db_name='yahoo_trade_fixture'
            ORDER BY yahoo_player_id, trade_direction""").fetchall() == [
            ("101", "received", 10),
            ("101", "sent", 10),
            ("102", "received", 3),
            ("102", "sent", 3),
            ("103", "received", 8),
            ("103", "sent", 8),
            ("104", "received", 2),
            ("104", "sent", 2),
        ]
        assert runner.conn.execute("""SELECT transaction_id, SUM(net) FROM (
            SELECT DISTINCT transaction_id, franchise_id, trade_net_lamar AS net
            FROM public.transactions WHERE db_name='yahoo_trade_fixture')
            GROUP BY transaction_id ORDER BY transaction_id""").fetchall() == [
            ("461.l.1.tr.166", 0),
            ("461.l.1.tr.298", 0),
        ]


@pytest.mark.parametrize("broken", ["missing_sent", "wrong_identity", "wrong_asset", "missing_value"])
def test_homepage_still_rejects_broken_yahoo_trade_mirrors(yahoo_trade_pipeline, broken):
    runner = yahoo_trade_pipeline
    runner._compute_trade_net_lamar()
    assert _compute_best_trade(runner.conn, "yahoo_trade_fixture", platform="yahoo")["net_lamar"] == 7
    target = "db_name='yahoo_trade_fixture' AND yahoo_player_id='101' AND trade_direction='sent'"
    if broken == "missing_sent":
        runner.conn.execute(f"DELETE FROM public.transactions WHERE {target}")
    else:
        assignment = {
            "wrong_identity": "source_franchise_id='unrelated-franchise'",
            "wrong_asset": "yahoo_player_id='different-player'",
            "missing_value": "trade_asset_lamar=NULL",
        }[broken]
        runner.conn.execute(f"UPDATE public.transactions SET {assignment} WHERE {target}")
    with pytest.raises(RuntimeError, match="trade assets lack complete mirrored valuations"):
        _compute_best_trade(runner.conn, "yahoo_trade_fixture", platform="yahoo")
