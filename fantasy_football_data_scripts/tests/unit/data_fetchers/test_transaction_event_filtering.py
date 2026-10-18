"""No-op provider events must not hide, corrupt, or block actual roster moves."""

from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pandas as pd
import pytest

from multi_league.core.transaction_periods import TransactionWeekError


PLATFORMS = ["yahoo", "espn", "sleeper"]


def event(platform, *, kind="add", status="complete", assets=True, week=True):
    if platform == "yahoo":
        kind = {"admin": "commish"}.get(kind, kind)
        players = ("<players><player><player_key>461.p.1</player_key><name><full>Real Player</full></name>"
                   "<transaction_data><type>" + kind + "</type><source_type>team</source_type>"
                   "<destination_type>team</destination_type><source_team_key>461.l.1.t.1</source_team_key>"
                   "<destination_team_key>461.l.1.t.2</destination_team_key></transaction_data></player></players>") if assets else ""
        status = "successful" if status == "complete" else status
        return (f"<transaction><transaction_key>461.l.1.tr.7</transaction_key><type>{kind}</type>"
                f"<status>{status}</status><timestamp>{1728520236 if week else 1767096000}</timestamp>"
                f"{players}</transaction>")
    if platform == "espn":
        kind = "TRADE_ACCEPT" if kind == "trade" else "FREEAGENT"
        return {"id": "7", "type": kind, "status": status.upper(), "scoringPeriodId": 6 if week else None,
                "processDate": 1728520236000, "acceptDate": 1728520236000,
                "items": [{"playerId": 1, "type": "ADD", "fromTeamId": 1, "toTeamId": 2,
                           "playerName": "Real Player"}] if assets else []}
    return {"transaction_id": "7", "type": {"add": "free_agent", "admin": "commissioner"}.get(kind, kind),
            "status": status, "leg": 6 if week else None, "created": 1728520236000,
            "adds": {"1": 2} if assets else None, "drops": {"1": 1} if assets and kind == "trade" else None,
            "draft_picks": [], "waiver_budget": [], "roster_ids": [1, 2]}


class PlayerCache:
    VALID_NFL_TEAMS = set()
    NFL_TEAM_SHORT_NAMES = {}
    def refresh_if_stale(self, client): pass
    def get_player_name(self, player_id): return "Real Player"
    def get_player_team(self, player_id): return "BUF"
    def get_player_position(self, player_id): return "WR"


ROSTERS = {1: {"manager_name": "Scott", "manager_guid": "scott-id", "team_name": "Scott Team"},
           2: {"manager_name": "Maxx", "manager_guid": "maxx-id", "team_name": "Maxx Team"}}

YAHOO_PICK = ("<picks><pick><source_team_key>461.l.1.t.1</source_team_key>"
              "<destination_team_key>461.l.1.t.2</destination_team_key>"
              "<original_team_key>461.l.1.t.1</original_team_key><original_team_name>Scott Team</original_team_name>"
              "<round>4</round></pick></picks>")


def parse_events(platform, events, monkeypatch, *, retry=False, active=False):
    if platform == "yahoo":
        from multi_league.data_fetchers.yahoo import yahoo_transactions as module
        attempts = 0
        def response(url, oauth):
            nonlocal attempts
            attempts += 1
            if retry and attempts == 1:
                raise module.RecoverableAPIError("transient transport failure")
            return ET.fromstring("<fantasy_content><transactions>" + "".join(events) + "</transactions></fantasy_content>")
        monkeypatch.setattr(module, "fetch_url", response)
        monkeypatch.setattr(module.time, "sleep", lambda seconds: None)
        windows = pd.DataFrame([{"year": 2024, "week": 6, "week_start": "2024-10-08",
                                 "week_end": "2024-10-14", "cumulative_week": 202406}])
        mappings = {f"461.l.1.t.{rid}": info["manager_name"] for rid, info in ROSTERS.items()}
        guids = {f"461.l.1.t.{rid}": info["manager_guid"] for rid, info in ROSTERS.items()}
        teams = {f"461.l.1.t.{rid}": info["team_name"] for rid, info in ROSTERS.items()}
        result = module.fetch_transactions_for_year(None, "461.l.1", 2024, mappings, guids, teams, windows)
        return module.transactions_to_dataframe(result)
    if platform == "espn":
        from multi_league.data_fetchers.espn.espn_transactions import fetch_espn_transactions_modern
        class Client:
            def get_raw_waivers(self, year, **kwargs): return [e for e in events if e["type"] != "TRADE_ACCEPT"]
            def get_raw_trades(self, year, **kwargs): return [e for e in events if e["type"] == "TRADE_ACCEPT"]
        ctx = SimpleNamespace(get_league_id_for_year=lambda year: 1,
            get_manager_name=lambda rid, **kw: ROSTERS[rid]["manager_name"],
            get_manager_guid=lambda rid, **kw: ROSTERS[rid]["manager_guid"],
            get_franchise_id=lambda rid, **kw: ROSTERS[rid]["manager_guid"],
            get_team_name=lambda rid, year: ROSTERS[rid]["team_name"])
        return fetch_espn_transactions_modern(ctx, 2024, max_week=6 if active else None,
                                               client=Client(), league=SimpleNamespace(teams=[]))
    from multi_league.data_fetchers.sleeper.sleeper_transactions import SleeperTransactionFetcher
    fetcher = SleeperTransactionFetcher(SimpleNamespace(), client=object(), player_cache=PlayerCache())
    return pd.DataFrame([row for e in events for row in fetcher._parse_transaction(e, 2024, ROSTERS)])


@pytest.mark.parametrize("platform", PLATFORMS)
@pytest.mark.parametrize("kind", ["add", "trade"])
@pytest.mark.parametrize("status", ["failed", "pending", "cancelled"])
def test_inactive_events_without_period_do_not_block_real_trade(platform, kind, status, monkeypatch):
    # Failed/pending moves must not require a week or appear as completed moves.
    ignored = event(platform, kind=kind, status=status, assets=False, week=False)
    real = event(platform, kind="trade")
    frame = parse_events(platform, [ignored, real], monkeypatch, active=True)
    assert len(frame) == 1
    assert frame.iloc[0]["transaction_type"] == "trade"
    assert frame.iloc[0]["week"] == 6
    assert frame.iloc[0]["source_manager_guid"] == "scott-id"
    assert frame.iloc[0]["destination_manager_guid"] == "maxx-id"


@pytest.mark.parametrize("platform", PLATFORMS)
def test_failed_events_with_player_payload_do_not_import_moves(platform, monkeypatch):
    frame = parse_events(platform, [event(platform, status="failed"), event(platform, kind="trade")], monkeypatch)
    assert len(frame) == 1
    assert frame.iloc[0]["transaction_type"] == "trade"


@pytest.mark.parametrize("platform", PLATFORMS)
@pytest.mark.parametrize("kind", ["add", "trade"])
def test_completed_moves_without_assets_are_not_silently_lost(platform, kind, monkeypatch):
    with pytest.raises(TransactionWeekError, match="payload"):
        parse_events(platform, [event(platform, kind=kind, assets=False)], monkeypatch)


@pytest.mark.parametrize("platform", ["yahoo", "sleeper"])
def test_playerless_administrative_events_need_no_period(platform, monkeypatch):
    frame = parse_events(platform, [event(platform, kind="admin", assets=False, week=False),
                                    event(platform, kind="trade")], monkeypatch)
    assert len(frame) == 1


@pytest.mark.parametrize("retry", [False, True])
def test_yahoo_payload_checks_are_identical_after_transport_retry(monkeypatch, retry):
    with pytest.raises(TransactionWeekError, match="payload"):
        parse_events("yahoo", [event("yahoo", assets=False)], monkeypatch, retry=retry)


@pytest.mark.parametrize("platform", PLATFORMS)
def test_completed_move_with_missing_player_id_is_rejected(platform, monkeypatch):
    txn = event(platform)
    if platform == "yahoo":
        txn = txn.replace("<player_key>461.p.1</player_key>", "")
    elif platform == "espn":
        txn["items"][0].pop("playerId")
    else:
        txn["adds"] = {"": 2}
    with pytest.raises(TransactionWeekError, match="payload"):
        parse_events(platform, [txn], monkeypatch)


@pytest.mark.parametrize("platform", PLATFORMS)
def test_completed_trade_requires_both_native_owners(platform, monkeypatch):
    txn = event(platform, kind="trade")
    if platform == "yahoo":
        txn = txn.replace("<source_team_key>461.l.1.t.1</source_team_key>", "")
    elif platform == "espn":
        txn["items"][0].pop("fromTeamId")
    else:
        txn["drops"] = None
    with pytest.raises(TransactionWeekError, match="payload"):
        parse_events(platform, [txn], monkeypatch)


def test_yahoo_trade_uses_existing_parent_owner_keys(monkeypatch):
    txn = event("yahoo", kind="trade").replace("<source_team_key>461.l.1.t.1</source_team_key>", "")
    txn = txn.replace("</transaction>", "<trader_team_key>461.l.1.t.1</trader_team_key></transaction>")
    frame = parse_events("yahoo", [txn], monkeypatch)
    assert frame.iloc[0]["source_manager_guid"] == "scott-id"


@pytest.mark.parametrize("field", ["season", "round", "roster_id", "owner_id", "previous_owner_id"])
def test_sleeper_pick_trade_never_invents_missing_native_fields(monkeypatch, field):
    txn = event("sleeper", kind="trade", assets=False)
    pick = {"season": "2025", "round": 1, "roster_id": 1, "owner_id": 2, "previous_owner_id": 1}
    pick.pop(field)
    txn["draft_picks"] = [pick]
    with pytest.raises(TransactionWeekError, match="payload"):
        parse_events("sleeper", [txn], monkeypatch)


@pytest.mark.parametrize("valid_leg", [True, False])
def test_sleeper_pick_only_trade_keeps_native_owners_and_week(monkeypatch, valid_leg):
    txn = event("sleeper", kind="trade", assets=False, week=valid_leg)
    txn["draft_picks"] = [{"season": "2025", "round": 1, "roster_id": 1,
                           "owner_id": 2, "previous_owner_id": 1}]
    if not valid_leg:
        with pytest.raises(TransactionWeekError, match="transaction week"):
            parse_events("sleeper", [txn], monkeypatch)
        return
    frame = parse_events("sleeper", [txn], monkeypatch)
    row = frame.iloc[0]
    assert len(frame) == 1
    assert (row["transaction_type"], row["sleeper_player_id"], row["week"]) == ("trade_pick", "pick_2025_1_1", 6)
    assert (row["source_manager_guid"], row["destination_manager_guid"]) == ("scott-id", "maxx-id")


@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("with_player", [False, True])
def test_yahoo_native_pick_trades_keep_every_asset(monkeypatch, retry, with_player):
    # Yahoo returns picks/pick beside players, including successful playerless trades.
    txn = event("yahoo", kind="trade", assets=with_player).replace("</transaction>", YAHOO_PICK + "</transaction>")
    frame = parse_events("yahoo", [txn], monkeypatch, retry=retry)
    assert len(frame) == (2 if with_player else 1)
    row = frame.loc[frame["transaction_type"] == "trade_pick"].iloc[0]
    assert (row["source_manager_guid"], row["destination_manager_guid"]) == ("scott-id", "maxx-id")
    assert row["traded_pick_round"] == 4
    assert row["position"] == "PICK"
    assert row["NFL_player_id"] == "PICK_yahoo_461.l.1.t.1_4"
    # The native payload has no draft season: preserve it without inventing one.
    assert pd.isna(row["traded_pick_season"])


@pytest.mark.parametrize("active", [False, True])
def test_sleeper_season_with_only_noops_returns_empty_without_sort_failure(monkeypatch, active):
    from multi_league.data_fetchers.sleeper import sleeper_transactions as module
    class Client:
        def get_league_transactions(self, league_id, week):
            return [event("sleeper", kind="admin", assets=False, week=False)] if week == 1 else []
    monkeypatch.setattr(module, "build_year_roster_map", lambda *args, **kwargs: ROSTERS)
    ctx = SimpleNamespace(get_league_id_for_year=lambda year: "qa")
    fetcher = module.SleeperTransactionFetcher(ctx, client=Client(), player_cache=PlayerCache())
    assert fetcher.fetch_transactions_for_year(2024, max_week=1 if active else None).empty


@pytest.mark.parametrize("lane", ["quick", "full", "refresh"])
@pytest.mark.parametrize("platform", PLATFORMS)
def test_native_trade_assets_survive_each_publication_lane(tmp_path, monkeypatch, platform, lane):
    """Parse -> canonicalize -> stage -> export: verify both actual trade parties."""
    import tarfile
    import duckdb
    from multi_league.core.canonical_transaction import normalize_transaction_df
    from multi_league.core.delta_publish import build_delta_bundle
    from multi_league.core.fleet_publish import build_fleet_partition_bundle
    from multi_league.core.league_refresh import stage_refresh_partitions

    txn = event(platform, kind="trade")
    if platform == "yahoo":
        txn = txn.replace("</transaction>", YAHOO_PICK + "</transaction>")
    elif platform == "sleeper":
        txn["draft_picks"] = [{"season": "2025", "round": 1, "roster_id": 1,
                               "owner_id": 2, "previous_owner_id": 1}]
    frame = parse_events(platform, [event(platform, status="failed", assets=False, week=False), txn],
                         monkeypatch, active=lane == "refresh")
    frame = normalize_transaction_df(frame, platform=platform, league_id="1")
    frame["db_name"] = "qa"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE SCHEMA public")
        conn.register("parsed_transactions", frame)
        conn.execute("CREATE TABLE public.transactions AS SELECT * FROM parsed_transactions")
        conn.execute("CREATE TABLE public.matchup AS SELECT 'qa' AS db_name, 2024 AS year, 6 AS week, "
                     "'2024-10-08' AS week_start, '2024-10-14' AS week_end, 'qa_2024_6' AS manager_week")
        if lane == "refresh":
            with stage_refresh_partitions(conn, db_name="qa", active_year=2024, tables=["transactions", "matchup"]) as stage:
                bundle = build_fleet_partition_bundle(stage, active_year=2024, league_generations={"qa": 0},
                                                       tables=["transactions"], output_dir=tmp_path / "bundle")
        else:
            bundle = build_delta_bundle(conn, db_name="qa", import_mode=lane, platform=platform,
                                        base_generation=0, output_dir=tmp_path / "bundle")
        with tarfile.open(bundle.path) as archive:
            payload = archive.extractfile("tables/transactions.parquet").read()
        published = tmp_path / "transactions.parquet"
        published.write_bytes(payload)
        actual = conn.execute("SELECT manager, franchise_id, trade_direction, transaction_type "
                              "FROM read_parquet(?) ORDER BY transaction_type, manager", [str(published)]).fetchall()
        expected = [("Maxx", "maxx-id", "received", "trade"), ("Scott", "scott-id", "sent", "trade")]
        if platform != "espn":
            expected += [("Maxx", "maxx-id", "received", "trade_pick"),
                         ("Scott", "scott-id", "sent", "trade_pick")]
        assert actual == expected
        assert conn.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE db_name <> 'qa' OR "
                            "week <> 6 OR cumulative_week <> 202406", [str(published)]).fetchone()[0] == 0
