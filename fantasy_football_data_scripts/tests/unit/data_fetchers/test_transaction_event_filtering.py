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


@pytest.mark.parametrize("active", [False, True])
def test_real_sleeper_commissioner_drop_is_retained(monkeypatch, active):
    from multi_league.data_fetchers.sleeper import sleeper_transactions as module
    # Native payload from Tortures Fantasy Department's failed quick import.
    txn = {"status": "complete", "type": "commissioner", "metadata": None,
           "created": 1792336046150, "settings": None, "leg": 6, "draft_picks": [],
           "creator": "870870993638477824", "transaction_id": "1417563757499990016",
           "adds": None, "drops": {"9753": 10}, "consenter_ids": None, "roster_ids": [10],
           "status_updated": 1792336046150, "waiver_budget": []}
    class Client:
        def get_league_transactions(self, league_id, week):
            return [txn] if week == 6 else []
    # Keep native period paging live, including history imports' five-empty-week stop.
    client = Client()
    original = client.get_league_transactions
    client.get_league_transactions = lambda lid, week: [event("sleeper", kind="admin", assets=False)] if week == 1 else original(lid, week)
    monkeypatch.setattr(module, "build_year_roster_map", lambda *args, **kw: {10: ROSTERS[2]})
    ctx = SimpleNamespace(get_league_id_for_year=lambda year: "1393635131826118656")
    fetcher = module.SleeperTransactionFetcher(ctx, client=client, player_cache=PlayerCache())
    frame = fetcher.fetch_transactions_for_year(2026, max_week=6 if active else None)
    assert frame[["transaction_id", "transaction_type", "sleeper_player_id", "week", "manager_guid", "destination"]].values.tolist() == [
        ["1417563757499990016", "drop", "9753", 6, "maxx-id", "waivers"]]


@pytest.mark.parametrize("kind", ["drop", "transfer", "mixed"])
def test_sleeper_commissioner_moves_do_not_duplicate_transferred_players(monkeypatch, kind):
    txn = event("sleeper", kind="admin", assets=False)
    txn["drops"] = {"1": 1}
    if kind != "drop":
        txn["adds"] = {"1": 2}
    if kind == "mixed":
        txn["drops"]["2"] = 1
    frame = parse_events("sleeper", [txn], monkeypatch)
    actual = frame[["sleeper_player_id", "manager_guid", "destination", "transaction_type"]].values.tolist()
    expected = [["1", "scott-id", "waivers", "drop"]] if kind == "drop" else [["1", "maxx-id", "team", "commish"]]
    if kind == "mixed":
        expected.append(["2", "scott-id", "waivers", "drop"])
    assert actual == expected


@pytest.mark.parametrize("with_player", [False, True])
def test_sleeper_faab_trade_retains_currency_once_per_party(monkeypatch, with_player):
    from multi_league.core.canonical_transaction import normalize_transaction_df
    txn = event("sleeper", kind="trade", assets=with_player)
    txn["waiver_budget"] = [{"sender": 1, "receiver": 2, "amount": 55}]
    raw = parse_events("sleeper", [txn], monkeypatch)
    frame = normalize_transaction_df(raw, platform="sleeper", league_id="1")
    cash = frame.loc[frame["position"] == "FAAB"]
    assert sorted(cash[["manager_guid", "trade_direction", "faab_traded"]].values.tolist()) == [
        ["maxx-id", "received", 55], ["scott-id", "sent", 55]]
    assert len(frame) == (4 if with_player else 2)
    assert cash["NFL_player_id"].tolist() == ["FAAB_7_1_2", "FAAB_7_1_2"]
    assert cash["faab_bid"].tolist() == [0, 0]


def test_zero_faab_only_trade_is_a_noop_without_a_period(monkeypatch):
    txn = event("sleeper", kind="trade", assets=False, week=False)
    txn["waiver_budget"] = [{"sender": 1, "receiver": 2, "amount": 0}]
    frame = parse_events("sleeper", [txn, event("sleeper", kind="trade")], monkeypatch)
    assert len(frame) == 1
    assert frame.iloc[0]["sleeper_player_id"] == "1"


@pytest.mark.parametrize("settings,expected", [(None, 0), ({"waiver_bid": None}, 0),
                                               ({"waiver_bid": 44}, 44)])
def test_sleeper_optional_settings_preserve_actual_waiver_bid(monkeypatch, settings, expected):
    txn = event("sleeper")
    txn.update(type="waiver", settings=settings)
    frame = parse_events("sleeper", [txn], monkeypatch)
    assert frame.iloc[0]["faab_bid"] == expected


@pytest.mark.parametrize("budget", [[{"sender": 1, "receiver": 2}],
    [{"sender": None, "receiver": 2, "amount": 5}],
    [{"sender": 1, "receiver": 2, "amount": -5}],
    [{"sender": 1, "receiver": 2, "amount": float("nan")}],
    [{"sender": 1, "receiver": 2, "amount": float("inf")}], [None]])
def test_sleeper_incomplete_real_currency_transfer_is_rejected(monkeypatch, budget):
    txn = event("sleeper", kind="trade")
    txn["waiver_budget"] = budget
    with pytest.raises(TransactionWeekError, match="Incomplete sleeper"):
        parse_events("sleeper", [txn], monkeypatch)


def parse_native_espn(events, monkeypatch):
    """Stub only HTTP; retain native relationship resolution and parsing."""
    from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient
    client = ESPNAPIClient(1)
    monkeypatch.setattr(client, "get_raw_transactions", lambda year, period, **kw: events if period == 6 else [])
    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.time.sleep", lambda _: None)
    from multi_league.data_fetchers.espn.espn_transactions import fetch_espn_transactions_modern
    ctx = SimpleNamespace(get_league_id_for_year=lambda year: 1,
        get_manager_name=lambda rid, **kw: ROSTERS[rid]["manager_name"],
        get_manager_guid=lambda rid, **kw: ROSTERS[rid]["manager_guid"],
        get_franchise_id=lambda rid, **kw: ROSTERS[rid]["manager_guid"],
        get_team_name=lambda rid, year: ROSTERS[rid]["team_name"])
    return fetch_espn_transactions_modern(ctx, 2024, max_week=6, client=client, league=SimpleNamespace(teams=[]))


@pytest.mark.parametrize("processed", [False, True])
def test_espn_native_trade_order_and_collateral_drop_preserve_all_moves(monkeypatch, processed):
    from multi_league.core.canonical_transaction import normalize_transaction_df
    proposal = event("espn", kind="trade")
    proposal.update(id="proposal", type="TRADE_PROPOSAL", status="PENDING")
    proposal["items"][0].update(type="TRADE", player=None)
    ack = event("espn", kind="trade")
    ack.update(id="ack", relatedTransactionId="proposal", status=None, executionType="EXECUTE",
               proposedDate=1728520236000, processDate=None, acceptDate=None,
               items=[{"playerId": 2, "type": "DROP", "fromTeamId": 2, "toTeamId": 0}])
    events = [ack, proposal]
    if processed:
        final = dict(proposal, id="processed", type="TRADE_ACCEPT", relatedTransactionId="proposal",
                     status="EXECUTED", executionType="PROCESS", isPending=True)
        final["items"] = proposal["items"] + [dict(ack["items"][0], player=None)]
        events.insert(0, final)
    raw = parse_native_espn(events, monkeypatch)
    frame = normalize_transaction_df(raw, platform="espn", league_id="1")
    assert sorted(frame[["espn_player_id", "manager_guid", "transaction_type"]].values.tolist()) == [
        ["1", "maxx-id", "trade"], ["1", "scott-id", "trade"], ["2", "maxx-id", "drop"]]
    assert set(frame["week"]) == {6}
    assert frame["timestamp"].notna().all()


def test_espn_purged_proposal_still_retains_collateral_drop(monkeypatch):
    ack = event("espn", kind="trade")
    ack.update(id="ack", relatedTransactionId="purged", status=None, executionType="EXECUTE",
               proposedDate=1728520236000, items=[{"playerId": 2, "type": "DROP", "fromTeamId": 1, "toTeamId": 0}])
    raw = parse_native_espn([ack], monkeypatch)
    assert raw[["espn_player_id", "manager_guid", "transaction_type"]].values.tolist() == [[2, "scott-id", "drop"]]


def test_espn_acknowledgement_never_borrows_an_unrelated_proposal(monkeypatch):
    proposal = event("espn", kind="trade")
    proposal.update(id="unrelated", type="TRADE_PROPOSAL", status="CANCELED")
    ack = event("espn", kind="trade", assets=False)
    ack.update(relatedTransactionId="purged", status=None, executionType="EXECUTE", teamId=1)
    raw = parse_native_espn([proposal, ack, event("espn")], monkeypatch)
    assert raw["transaction_type"].tolist() == ["add"]


def test_espn_processed_trade_missing_real_assets_remains_an_error(monkeypatch):
    final = event("espn", kind="trade", assets=False)
    final.update(executionType="PROCESS", status="EXECUTED")
    with pytest.raises(TransactionWeekError, match="Incomplete espn"):
        parse_native_espn([final], monkeypatch)


@pytest.mark.parametrize("platform", PLATFORMS)
def test_missing_player_names_never_collapse_distinct_traded_players(monkeypatch, platform):
    from multi_league.core.canonical_transaction import normalize_transaction_df
    txn = event(platform, kind="trade")
    if platform == "yahoo":
        xml = ET.fromstring(txn)
        player = ET.fromstring(ET.tostring(xml.find("players/player")))
        player.find("player_key").text = "461.p.2"
        xml.find("players").append(player)
        txn = ET.tostring(xml, encoding="unicode")
    elif platform == "espn":
        txn["items"].append(dict(txn["items"][0], playerId=2))
    else:
        txn["adds"]["2"] = 2
        txn["drops"]["2"] = 1
    raw = parse_events(platform, [txn], monkeypatch)
    frame = normalize_transaction_df(raw, platform=platform, league_id="1")
    assert len(frame) == 4
    assert frame.groupby("manager_guid").size().to_dict() == {"maxx-id": 2, "scott-id": 2}
    again = normalize_transaction_df(frame, platform=platform, league_id="1")
    assert len(again) == 4


@pytest.mark.parametrize("platform", PLATFORMS)
def test_null_optional_metadata_does_not_block_native_moves(monkeypatch, platform):
    txn = event(platform)
    if platform != "yahoo":
        txn.update(metadata=None, settings=None)
    if platform == "espn":
        txn.update(bidAmount=None, proposalDate=None)
    frame = parse_events(platform, [txn], monkeypatch)
    assert len(frame) == 1
    assert frame.iloc[0]["week"] == 6


@pytest.mark.parametrize("lane", ["quick", "full", "refresh"])
@pytest.mark.parametrize("platform,asset", [(p, a) for p in PLATFORMS for a in ("trade", "drop")] + [("sleeper", "faab")])
def test_native_trade_assets_survive_each_publication_lane(tmp_path, monkeypatch, platform, lane, asset):
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
    if asset == "drop":
        if platform == "yahoo":
            txn = event("yahoo", kind="drop").replace("<destination_type>team</destination_type>",
                                                       "<destination_type>waivers</destination_type>")
        elif platform == "espn":
            txn = event("espn")
            txn["items"][0]["type"] = "DROP"
        else:
            txn = event("sleeper", kind="admin", assets=False)
            txn["drops"] = {"1": 1}
    elif asset == "faab":
        txn = event("sleeper", kind="trade", assets=False)
        txn["waiver_budget"] = [{"sender": 1, "receiver": 2, "amount": 55}]
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
        if asset == "drop":
            expected = [("Scott", "scott-id", None, "drop")]
        elif asset == "faab":
            expected = [("Maxx", "maxx-id", "received", "trade"), ("Scott", "scott-id", "sent", "trade")]
        assert actual == expected
        if asset == "faab":
            assert conn.execute("SELECT faab_traded FROM read_parquet(?) ORDER BY manager", [str(published)]).fetchall() == [(55,), (55,)]
        assert conn.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE db_name <> 'qa' OR "
                            "week <> 6 OR cumulative_week <> 202406", [str(published)]).fetchone()[0] == 0
