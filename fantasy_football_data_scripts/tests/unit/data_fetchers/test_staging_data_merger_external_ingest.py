"""Tests for external_ingest hook inside merge_staging_data.

The hook runs apply_mappings on each staging_df per-table. We test the
integration shape: staging_df with wrong column names + ignored-manager rows,
plus ctx.external_column_maps + ctx.external_identity_maps, transforms correctly.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.table_exists.return_value = False
    db.row_count.return_value = 0
    return db


@pytest.fixture
def mock_ctx():
    ctx = MagicMock()
    ctx.league_id = "league_x"
    ctx.league_name = "kmffl"
    ctx.platform = "yahoo"
    ctx.external_column_maps = []
    ctx.external_identity_maps = {}
    # Explicit default — MagicMock would otherwise return a truthy mock for
    # any unset attribute, masking the foreign-league_id guard.
    ctx.has_external_data = False
    return ctx


def test_merge_staging_skips_storage_in_corpus_mode(mock_db, mock_ctx, monkeypatch):
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    monkeypatch.setenv("CORPUS_MODE", "1")
    monkeypatch.setattr(
        staging_reader,
        "read_staging_data",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("corpus mode must not read centralized staging data")
        ),
    )

    assert staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b)) == {
        "status": "no_staging"
    }


def test_merge_staging_no_external_mappings_is_noop(mock_db, mock_ctx, monkeypatch):
    """With no external_column_maps/identity_maps, merge_staging_data behaves as before."""
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    staging_data = {
        "matchup": pd.DataFrame(
            {
                "year": [2013],
                "week": [1],
                "manager": ["Ezra"],
                "opponent": ["Marc"],
                "team_points": [100.0],
                "opponent_points": [90.0],
                "league_id": ["league_x"],
            }
        ),
    }
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: staging_data)

    stats = staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    # save_table should have been called with the ORIGINAL (unchanged) staging df
    save_calls = mock_db.save_table.call_args_list
    assert len(save_calls) >= 1
    saved_df = save_calls[0].args[1]
    assert "year" in saved_df.columns
    assert "manager" in saved_df.columns
    # No rename occurred: Ezra still present
    assert "Ezra" in saved_df["manager"].values


def test_merge_staging_applies_column_renames(mock_db, mock_ctx, monkeypatch):
    """external_column_maps rename staging columns before merge/save."""
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    staging_data = {
        "matchup": pd.DataFrame(
            {
                "yr": [2013],
                "wk": [1],
                "owner": ["Ezra"],
                "opp": ["Marc"],
                "pf": [100.0],
                "pa": [90.0],
                "league_id": ["league_x"],
            }
        ),
    }
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: staging_data)

    mock_ctx.external_column_maps = [
        {
            "table": "matchup",
            "column_map": {
                "year": "yr",
                "week": "wk",
                "manager": "owner",
                "opponent": "opp",
                "team_points": "pf",
                "opponent_points": "pa",
            },
        }
    ]
    mock_ctx.external_identity_maps = {
        "Ezra": {"franchise_id": "g_1", "owner_guid": "real_g1"},
    }

    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    saved_df = mock_db.save_table.call_args_list[0].args[1]
    # Original source columns gone, canonical columns present
    assert "yr" not in saved_df.columns
    assert "year" in saved_df.columns
    assert "owner" not in saved_df.columns
    assert "manager" in saved_df.columns
    # manager_guid synthesized from identity decision
    assert saved_df["manager_guid"].iloc[0] == "real_g1"


def test_merge_staging_player_dedup_does_not_collapse_null_player_week(mock_db, mock_ctx, monkeypatch):
    """Regression: when both staging AND existing Yahoo rows for the same year
    flow through the merger, the NULL-fallback dedup must not collapse all
    NULL-player_week rows into one.

    Original bug: pandas.drop_duplicates treats every NULL key as equal, so
    100 distinct Yahoo rows with NULL player_week collapsed to 1. Fix: split
    the frame and fall back to (year, week, yahoo_player_id, manager) for
    NULL-key rows.

    Note: under the year-scoping fix, only staging-touched years go through
    the merger at all. To exercise the dedup path, both staging and existing
    must overlap in the same year — which is exactly the realistic shape
    when staging carries an active-season year that the Yahoo fetcher also
    populates.
    """
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    # 5 staging player rows for 2014 with populated player_week
    staging_player = pd.DataFrame(
        {
            "year": [2014, 2014, 2014, 2014, 2014],
            "week": [1, 1, 1, 1, 1],
            "yahoo_player_id": ["1001", "1002", "1003", "1004", "1005"],
            "manager": ["Ezra", "Marc", "Adin", "Tani", "Yaacov"],
            "fantasy_position": ["QB", "RB", "WR", "TE", "BN"],
            "player_week": ["1001_2014_1", "1002_2014_1", "1003_2014_1", "1004_2014_1", "1005_2014_1"],
            "league_id": ["league_x"] * 5,
        }
    )
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: {"player": staging_player})

    # Existing Yahoo data IN THE SAME YEAR: 100 distinct (player_id, week, manager)
    # combinations with NULL player_week (not yet resolved by enrichments).
    # All 100 must survive dedup — they're distinct players, not duplicates.
    existing_yahoo = pd.DataFrame(
        {
            "year": [2014] * 100,
            "week": [(i // 17) + 1 for i in range(100)],
            "yahoo_player_id": [f"y{i}" for i in range(100)],
            "manager": ["Ezra"] * 100,
            "fantasy_position": ["QB", "BN", "WR", "RB", "TE"] * 20,
            "player_week": [None] * 100,
            "league_id": ["league_x"] * 100,
        }
    )

    # Simulate the year-scoped read: staging covers year 2014, so the merger
    # reads existing rows WHERE year IN (2014). Return existing_yahoo for any
    # SELECT, since all rows here are 2014.
    class FakeConn:
        def execute(self, sql: str):
            class _Result:
                def fetchdf(_self):
                    return existing_yahoo

            return _Result()

    mock_db.table_exists.return_value = True
    mock_db.row_count.return_value = len(existing_yahoo)
    mock_db.connect.return_value = FakeConn()
    mock_db.read_table.return_value = existing_yahoo  # fallback path

    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    # Find the player save call
    player_saves = [c for c in mock_db.save_table.call_args_list if c.args[0] == "player_fantasy"]
    assert player_saves, "player_fantasy was never saved"
    saved_df = player_saves[0].args[1]

    # All 100 Yahoo rows survive (distinct yahoo_player_id+week+manager) plus 5 staging.
    yahoo_rows = saved_df[saved_df["yahoo_player_id"].astype(str).str.startswith("y")]
    staging_rows = saved_df[saved_df["yahoo_player_id"].astype(str).isin(["1001", "1002", "1003", "1004", "1005"])]
    assert len(yahoo_rows) == 100, f"Yahoo rows collapsed by NULL-key dedup: got {len(yahoo_rows)}, expected 100"
    assert len(staging_rows) == 5, f"Staging rows wrong: got {len(staging_rows)}, expected 5"

    # Lineup-slot info preserved (BN/QB/etc. all present in 2014 yahoo rows)
    assert "BN" in yahoo_rows["fantasy_position"].values


def test_merge_staging_preserves_yahoo_player_id_through_apply_mappings(mock_db, mock_ctx, monkeypatch):
    """Regression for KMFFL 2014: apply_mappings used to strip yahoo_player_id
    from staging player rows because it isn't a slot in PLAYER_FANTASY_MANIFEST.

    Downstream, the staging-merger NULL-fallback dedup uses
    `(year, week, yahoo_player_id, manager)`. With yahoo_player_id stripped to
    NULL, every distinct player-week row collapsed to ONE row per
    (year, week, manager) — destroying the entire 17-slot lineup (BN/IR/FLX
    info) and blocking the franchise merge with the canonical 2015+ years.

    For 2014 KMFFL: 2,560 staging rows representing ~16 lineup slots × 12
    managers × 17 weeks collapsed to ~160 rows (one row per manager-week).

    The fix preserves stable platform identity columns through
    apply_mappings even when not in column_map.
    """
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    # 8 distinct players for 1 manager in 1 week — these MUST all survive
    # dedup. yahoo_player_id is the only thing that distinguishes them under
    # the NULL-fallback dedup key (year + week + yahoo_player_id + manager).
    staging_player = pd.DataFrame(
        {
            "year": [2014] * 8,
            "week": [1] * 8,
            "manager": ["Ezra"] * 8,
            "player": [f"Player{i}" for i in range(8)],
            "points": [10.0 + i for i in range(8)],
            "fantasy_position": ["QB", "RB", "RB", "WR", "WR", "TE", "K", "BN"],
            # yahoo_player_id is populated in the source parquet but not in
            # the manifest — pre-fix it was stripped here, leaving NULL in
            # the merged frame and collapsing all 8 rows to 1.
            "yahoo_player_id": [5479, 8261, 9999, 1001, 1002, 1003, 1004, 1005],
            "manager_guid": ["FYW5PMKZ23OGUAONDX6AGROCZQ"] * 8,
            "league_id": ["league_x"] * 8,
        }
    )
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: {"player": staging_player})

    # Wizard column_map only contains canonical manifest slots — no
    # yahoo_player_id. This is the production shape for KMFFL.
    mock_ctx.external_column_maps = [
        {
            "table": "player",
            "column_map": {
                "year": "year",
                "week": "week",
                "manager": "manager",
                "player": "player",
                "points": "points",
                "fantasy_position": "fantasy_position",
                "manager_guid": "manager_guid",
            },
        }
    ]
    mock_ctx.external_identity_maps = {
        "Ezra": {"franchise_id": "FYW5PMKZ23OGUAONDX6AGROCZQ"},
    }
    mock_ctx.has_external_data = True

    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    player_saves = [c for c in mock_db.save_table.call_args_list if c.args[0] == "player_fantasy"]
    assert player_saves, "player_fantasy was never saved"
    saved_df = player_saves[0].args[1]

    # All 8 player rows survived (one per yahoo_player_id, not collapsed).
    assert len(saved_df) == 8, (
        f"Staging player rows collapsed by NULL-yahoo_player_id dedup: "
        f"got {len(saved_df)}, expected 8 (one per distinct player)"
    )
    # yahoo_player_id was preserved end-to-end
    assert "yahoo_player_id" in saved_df.columns
    assert saved_df["yahoo_player_id"].notna().all()
    assert set(saved_df["yahoo_player_id"].astype(str)) == {
        "5479",
        "8261",
        "9999",
        "1001",
        "1002",
        "1003",
        "1004",
        "1005",
    }
    # Lineup-slot info preserved (BN row didn't get collapsed into starters)
    assert "BN" in saved_df["fantasy_position"].values
    # Canonical Yahoo guid plumbed through (matches 2015+ canonical)
    assert (saved_df["manager_guid"] == "FYW5PMKZ23OGUAONDX6AGROCZQ").all()


def test_draft_dedup_preserves_blank_pick_rows():
    from multi_league.data_fetchers.shared import staging_data_merger

    df = pd.DataFrame(
        {
            "year": [2025, 2025, 2025],
            "round": [None, None, 1],
            "pick": [None, None, 1],
            "player": ["Jakobi Meyers", "Kenneth Walker III", "Saquon Barkley"],
        }
    )

    deduped = staging_data_merger._drop_duplicates_preserve_null_keys(df, ["year", "round", "pick"])

    assert deduped["player"].tolist() == ["Jakobi Meyers", "Kenneth Walker III", "Saquon Barkley"]


def test_merge_staging_draft_maps_external_managers_and_backfills_players(mock_db, mock_ctx, monkeypatch):
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    staging_draft = pd.DataFrame(
        {
            "year": [2025, 2025],
            "round": [None, None],
            "pick": [None, None],
            "manager": ["Iossi", "Joshua"],
            "player": ["Jacobi Myers, LV WR", "Ken Walker III, SEA RB"],
            "cost": ["2", "40"],
            "draft_type": ["Auction", "Auction"],
            "league_id": ["FROM_EXTERNAL_CSV", "FROM_EXTERNAL_CSV"],
        }
    )
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: {"draft": staging_draft})

    existing_yahoo = pd.DataFrame(
        {
            "year": [2025, 2025],
            "round": [7, 2],
            "pick": [80, 15],
            "manager": ["Tyler", "Josh"],
            "player": ["Jakobi Meyers", "Kenneth Walker III"],
            "NFL_player_id": ["00-0034960", "00-0038134"],
            "position": ["WR", "RB"],
            "nfl_team_api": ["LV", "SEA"],
            "league_id": ["league_x", "league_x"],
        }
    )

    class FakeConn:
        def execute(self, sql: str):
            class _Result:
                def fetchdf(_self):
                    return existing_yahoo

            return _Result()

    mock_db.table_exists.return_value = True
    mock_db.row_count.return_value = len(existing_yahoo)
    mock_db.connect.return_value = FakeConn()
    mock_ctx.has_external_data = True
    mock_ctx.external_identity_maps = {
        "Iossi": {"franchise_id": "tyler_fid", "owner_guid": "tyler_guid", "display_name": "Tyler"},
        "Joshua": {"franchise_id": "josh_fid", "owner_guid": "josh_guid", "display_name": "Josh"},
    }

    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    draft_saves = [c for c in mock_db.save_table.call_args_list if c.args[0] == "draft"]
    assert draft_saves, "draft was never saved"
    saved_df = draft_saves[0].args[1]

    assert len(saved_df) == 2
    assert saved_df["player"].tolist() == ["Jakobi Meyers", "Kenneth Walker III"]
    assert saved_df["NFL_player_id"].tolist() == ["00-0034960", "00-0038134"]
    assert saved_df["manager"].tolist() == ["Tyler", "Josh"]
    assert saved_df["manager_guid"].tolist() == ["tyler_guid", "josh_guid"]
    assert saved_df["round"].tolist() == [7, 2]
    assert saved_df["pick"].tolist() == [80, 15]
    assert saved_df["position"].tolist() == ["WR", "RB"]
    assert saved_df["nfl_team_api"].tolist() == ["LV", "SEA"]


def test_merge_staging_draft_keeps_external_pick_slots_when_present(mock_db, mock_ctx, monkeypatch):
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    staging_draft = pd.DataFrame(
        {
            "year": [2025],
            "round": [1],
            "pick": [1],
            "manager": ["Iossi"],
            "player": ["Jacobi Myers, LV WR"],
            "cost": ["2"],
            "draft_type": ["Auction"],
            "league_id": ["FROM_EXTERNAL_CSV"],
        }
    )
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: {"draft": staging_draft})

    existing_yahoo = pd.DataFrame(
        {
            "year": [2025],
            "round": [7],
            "pick": [80],
            "manager": ["Tyler"],
            "player": ["Jakobi Meyers"],
            "NFL_player_id": ["00-0034960"],
            "position": ["WR"],
            "nfl_team_api": ["LV"],
            "league_id": ["league_x"],
        }
    )

    class FakeConn:
        def execute(self, sql: str):
            class _Result:
                def fetchdf(_self):
                    return existing_yahoo

            return _Result()

    mock_db.table_exists.return_value = True
    mock_db.row_count.return_value = len(existing_yahoo)
    mock_db.connect.return_value = FakeConn()
    mock_ctx.has_external_data = True
    mock_ctx.external_identity_maps = {
        "Iossi": {"franchise_id": "tyler_fid", "owner_guid": "tyler_guid", "display_name": "Tyler"},
    }

    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    draft_saves = [c for c in mock_db.save_table.call_args_list if c.args[0] == "draft"]
    saved_df = draft_saves[0].args[1]

    assert saved_df["round"].tolist() == [1]
    assert saved_df["pick"].tolist() == [1]
    assert saved_df["NFL_player_id"].tolist() == ["00-0034960"]


def test_merge_staging_scopes_delete_and_save_to_staging_years_only(mock_db, mock_ctx, monkeypatch):
    """Regression for KMFFL quick→full enrichment loss.

    The Yahoo Full Import worker runs quick import then full import on the
    SAME runner with the SAME local DuckDB. Quick import populates 2025 with
    all SQL enrichments (player_lamar, manager_lamar, optimal_player, etc.)
    and uploads to Fly. The full import opens the same local DuckDB, finds
    2025 already enriched, and ideally leaves it alone except where staging
    contributes new rows (e.g., 2013/2014 for KMFFL).

    Pre-fix the merger did `DELETE FROM public.{table}` (all years!) then
    `save_table(merged_df)`, which routed everything through the canonical
    normalizer — stripping ~67 enrichment columns from 2025 rows that
    staging never touched. PHASE 3.5 then had to recompute everything from
    scratch, and gaps (rookies not in player_bio, sparse 2025 super_table)
    caused visible failures like 'Best Pickup empty' and 'Bo Nix VALUE 0.00'.

    Fix: scope read+DELETE+save to the years actually present in staging_df.
    Untouched years stay in local DuckDB with their enrichments intact.
    """
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    # Staging carries only 2014 matchup rows (the KMFFL external-import shape).
    staging_data = {
        "matchup": pd.DataFrame(
            {
                "year": [2014, 2014],
                "week": [1, 2],
                "manager": ["Marc", "Marc"],
                "opponent": ["Ezra", "Tani"],
                "team_points": [120.0, 130.0],
                "opponent_points": [115.0, 125.0],
                "league_id": ["league_x", "league_x"],
            }
        ),
    }
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: staging_data)

    # Simulate local DB state after quick import: 2025 already there.
    # The mock returns this only for the year-scoped SELECT (year IN (2014)),
    # so the existing_df should be empty for the matchup merge call.
    mock_db.table_exists.return_value = True
    mock_db.row_count.return_value = 100  # pretend 100 rows in 2025 + maybe 2014

    # Track the SELECT and DELETE SQL the merger emits — we want to assert
    # both are scoped to year IN (2014).
    select_calls: list[str] = []
    delete_calls: list[str] = []

    class FakeConn:
        def execute(self, sql: str):
            select_calls.append(sql)

            # Pretend no existing 2014 rows in local DB.
            class _Result:
                def fetchdf(self):
                    return pd.DataFrame(
                        columns=["year", "week", "manager", "opponent", "team_points", "opponent_points", "league_id"]
                    )

            return _Result()

    mock_db.connect.return_value = FakeConn()

    def _capture_delete(sql):
        delete_calls.append(sql)

    mock_db.execute_sql.side_effect = _capture_delete

    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    # SELECT must be scoped to the staging years.
    matchup_selects = [s for s in select_calls if "matchup" in s.lower()]
    assert matchup_selects, f"merger did not SELECT existing matchup rows; got: {select_calls}"
    assert any(
        "WHERE year IN (2014)" in s for s in matchup_selects
    ), f"matchup SELECT must be scoped to staging years (2014); got: {matchup_selects}"

    # DELETE must be scoped, NOT a wholesale `DELETE FROM public.matchup`.
    matchup_deletes = [s for s in delete_calls if "public.matchup" in s.lower()]
    assert matchup_deletes, f"merger did not DELETE matchup; got: {delete_calls}"
    for sql in matchup_deletes:
        assert "WHERE year IN" in sql, f"matchup DELETE must be year-scoped to preserve untouched years; got: {sql}"
        assert "2014" in sql, f"DELETE should target 2014; got: {sql}"

    # save_table received a frame containing only 2014 rows.
    matchup_saves = [c for c in mock_db.save_table.call_args_list if c.args[0] == "matchup"]
    assert matchup_saves, "matchup was never saved"
    saved_df = matchup_saves[0].args[1]
    assert (
        saved_df["year"] == 2014
    ).all(), f"saved matchup must be scoped to staging years; got years {saved_df['year'].unique()}"


def test_merge_staging_drops_ignored_rows(mock_db, mock_ctx, monkeypatch):
    """ignored managers' rows are removed from staging before save."""
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    staging_data = {
        "matchup": pd.DataFrame(
            {
                "year": [2013, 2013],
                "week": [1, 1],
                "manager": ["Ezra", "TEST_USER"],
                "opponent": ["Marc", "Donny"],
                "team_points": [100.0, 0.0],
                "opponent_points": [90.0, 0.0],
                "league_id": ["league_x", "league_x"],
            }
        ),
    }
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: staging_data)

    # Column map that identifies matchup columns AS themselves (identity rename)
    mock_ctx.external_column_maps = [
        {
            "table": "matchup",
            "column_map": {
                "year": "year",
                "week": "week",
                "manager": "manager",
                "opponent": "opponent",
                "team_points": "team_points",
                "opponent_points": "opponent_points",
            },
        }
    ]
    mock_ctx.external_identity_maps = {
        "Ezra": {"franchise_id": "g_1", "owner_guid": "real_g1"},
        "TEST_USER": {"ignore": True},
    }

    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    saved_df = mock_db.save_table.call_args_list[0].args[1]
    assert len(saved_df) == 1
    assert "TEST_USER" not in saved_df["manager"].values


# ---------------------------------------------------------------------------
# H4 — refuse foreign league_id unless has_external_data is set
# ---------------------------------------------------------------------------


def test_merge_staging_rejects_foreign_league_id_without_external_flag(mock_db, mock_ctx, monkeypatch):
    """If staging carries a different league_id and has_external_data=False,
    we refuse — silently relabelling would corrupt this league's history."""
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    staging_data = {
        "matchup": pd.DataFrame(
            {
                "year": [2013],
                "week": [1],
                "manager": ["Ezra"],
                "opponent": ["Marc"],
                "team_points": [100.0],
                "opponent_points": [90.0],
                "league_id": ["WRONG_LEAGUE"],
            }
        ),
    }
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: staging_data)

    mock_ctx.has_external_data = False

    with pytest.raises(RuntimeError, match="foreign league_id"):
        staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    # Nothing should have been written to the local DB before the raise.
    assert mock_db.save_table.call_count == 0


def test_merge_staging_allows_foreign_league_id_when_external_flag_set(mock_db, mock_ctx, monkeypatch):
    """The wizard sets has_external_data=True. In that flow, foreign league_id
    is expected (the whole point of external data ingest) and we normalize it
    quietly."""
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    staging_data = {
        "matchup": pd.DataFrame(
            {
                "year": [2013],
                "week": [1],
                "manager": ["Ezra"],
                "opponent": ["Marc"],
                "team_points": [100.0],
                "opponent_points": [90.0],
                "league_id": ["FROM_EXTERNAL_CSV"],
            }
        ),
    }
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: staging_data)

    mock_ctx.has_external_data = True

    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    saved_df = mock_db.save_table.call_args_list[0].args[1]
    # league_id was normalized to the import target.
    assert (saved_df["league_id"] == "league_x").all()


def test_merge_staging_treats_blank_league_id_as_not_foreign(mock_db, mock_ctx, monkeypatch):
    """Empty/whitespace/NaN league_id values are not 'foreign' — they're
    just unset. has_external_data=False should still allow them through."""
    from multi_league.data_fetchers.shared import staging_data_merger
    from multi_league.data_fetchers.shared import staging_reader

    staging_data = {
        "matchup": pd.DataFrame(
            {
                "year": [2013, 2013, 2013],
                "week": [1, 2, 3],
                "manager": ["Ezra", "Ezra", "Ezra"],
                "opponent": ["Marc", "Marc", "Marc"],
                "team_points": [100.0, 110.0, 120.0],
                "opponent_points": [90.0, 95.0, 100.0],
                "league_id": [None, "", "   "],
            }
        ),
    }
    monkeypatch.setattr(staging_reader, "read_staging_data", lambda _: staging_data)

    mock_ctx.has_external_data = False

    # Should not raise even though has_external_data is False — none of the
    # values count as a real foreign league_id.
    staging_data_merger.merge_staging_data(mock_ctx, mock_db, lambda a, b: (a, b))

    saved_df = mock_db.save_table.call_args_list[0].args[1]
    assert (saved_df["league_id"] == "league_x").all()
