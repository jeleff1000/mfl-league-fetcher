from __future__ import annotations

import duckdb
import pytest

import scripts.research_cohorts.plan_balanced_matchup_tasks as planner
from scripts.research_cohorts.plan_balanced_matchup_tasks import (
    inventory,
    make_capacity_audit_plan,
    make_plan,
    pack_weighted_player_tasks,
)


def test_weighted_player_tasks_keep_high_exposure_players_in_separate_lanes() -> None:
    """A CMC-sized player must not be randomly clumped with another heavy player."""

    tasks = [
        {"year": 2025, "position": "RB", "NFL_player_id": "heavy_a", "rows": 100},
        {"year": 2025, "position": "RB", "NFL_player_id": "heavy_b", "rows": 95},
        {"year": 2025, "position": "RB", "NFL_player_id": "light_a", "rows": 5},
        {"year": 2025, "position": "RB", "NFL_player_id": "light_b", "rows": 5},
    ]

    lanes = pack_weighted_player_tasks(tasks, runners=2)

    assert [[task["NFL_player_id"] for task in lane["players"]] for lane in lanes] == [
        ["heavy_a", "light_b"],
        ["heavy_b", "light_a"],
    ]
    assert [lane["rows"] for lane in lanes] == [105, 100]


def test_inventory_uses_rostered_canonical_output_population(tmp_path) -> None:
    snapshot = tmp_path / "snapshot.duckdb"
    ops = tmp_path / "ops.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute("""
      CREATE TABLE public.player_fantasy (
        year INTEGER, week INTEGER, position VARCHAR, NFL_player_id VARCHAR,
        is_rostered INTEGER
      )
    """)
    con.execute("""
      INSERT INTO public.player_fantasy VALUES
        (2025,1,'CB','db_rostered',1),
        (2025,2,'CB','db_rostered',NULL),
        (2025,1,'CB','db_never',NULL),
        (2025,1,'RB','rb_rostered',1),
        (2025,1,'RB','rb_never',0),
        (2025,1,'FB','fb_rostered',1),
        (2025,1,'P','punter_rostered',1),
        (2025,1,'LS','long_snapper_rostered',1)
    """)
    con.close()

    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    con.execute("""
      CREATE OR REPLACE TABLE nfl_historical.nfl_player_stats_all (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR, position VARCHAR
      )
    """)
    con.execute("""
      INSERT INTO nfl_historical.nfl_player_stats_all VALUES
        ('db_rostered',2025,1,'REG','DB'),
        ('rb_rostered',2025,1,'REG','RB'),
        ('fb_rostered',2025,1,'REG','RB'),
        ('punter_rostered',2025,1,'REG','P'),
        ('long_snapper_rostered',2025,1,'REG','OL')
    """)
    con.close()

    # P and OL are not supported research-table positions.  The build must
    # not spend a lane on them or permit them to surface in IDP results.
    assert inventory(snapshot, ops, 2025, 2025) == {
        (2025, "DB"): (2, 1),
        (2025, "RB"): (2, 2),
    }
    plan = make_plan(snapshot, ops, 2025, 2025, 15)
    # A position/year source is indivisible: one task builds its complete
    # denominator inventory once, then emits every qualifying player for it.
    assert len(plan) == 2
    assert all(bucket["tasks"] for bucket in plan)
    assert {
        (task["year"], task["position"])
        for lane in plan
        for task in lane["tasks"]
    } == {(2025, "DB"), (2025, "RB")}
    assert all("player_ids" not in task for lane in plan for task in lane["tasks"])


def test_make_plan_rejects_non_serving_position_pilots(tmp_path) -> None:
    """P and OL have no research-table UI lane, including isolated pilots."""
    snapshot = tmp_path / "snapshot.duckdb"
    ops = tmp_path / "ops.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_fantasy "
        "(year INTEGER, week INTEGER, position VARCHAR, NFL_player_id VARCHAR, is_rostered INTEGER)"
    )
    con.execute("INSERT INTO public.player_fantasy VALUES (2020,1,'OL','ol',1),(2020,1,'RB','rb',1)")
    con.close()
    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    con.execute("DROP TABLE IF EXISTS nfl_historical.nfl_player_stats_all")
    con.execute(
        "CREATE TABLE nfl_historical.nfl_player_stats_all "
        "(NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR, position VARCHAR)"
    )
    con.execute("INSERT INTO nfl_historical.nfl_player_stats_all VALUES ('ol',2020,1,'REG','OL'),('rb',2020,1,'REG','RB')")
    con.close()

    with pytest.raises(ValueError, match="unsupported canonical position"):
        make_plan(snapshot, ops, 2020, 2020, 1, positions=("OL",))


def test_make_plan_splits_heavy_position_years_into_fifteen_balanced_lanes(monkeypatch) -> None:
    """A full build uses all 15 runners instead of one serial WR/RB lane."""
    monkeypatch.setattr(
        planner,
        "base_tasks",
        lambda *args, **kwargs: [
            {"year": 2025, "position": "WR", "rows": 15_577_315, "players": 252},
            {"year": 2025, "position": "RB", "rows": 11_948_905, "players": 166},
            {"year": 2025, "position": "QB", "rows": 6_250_082, "players": 81},
            {"year": 2025, "position": "TE", "rows": 6_245_276, "players": 145},
            {"year": 2025, "position": "LB", "rows": 1_863_153, "players": 307},
            {"year": 2025, "position": "DB", "rows": 1_591_184, "players": 431},
            {"year": 2025, "position": "DL", "rows": 959_702, "players": 338},
            {"year": 2025, "position": "DEF", "rows": 714_276, "players": 32},
            {"year": 2025, "position": "K", "rows": 610_787, "players": 42},
        ],
    )

    plan = make_plan(None, None, 2025, 2025, 15)

    assert len(plan) == 15
    assert all(lane["tasks"] for lane in plan)
    assert any(task["player_buckets"] > 1 for lane in plan for task in lane["tasks"])
    estimates = [lane["estimate"] for lane in plan]
    # Discrete QB/TE and small-position tasks make exact equality impossible;
    # this keeps the slowest lane below two-thirds above the fastest instead
    # of the prior 165s-vs-4s position split.
    assert max(estimates) <= min(estimates) * 1.7


def test_make_plan_fills_every_requested_runner_when_player_buckets_allow(monkeypatch) -> None:
    """Rounding must not strand requested runners on a normal compact build."""
    monkeypatch.setattr(
        planner,
        "base_tasks",
        lambda *args, **kwargs: [
            {"year": 2025, "position": "WR", "rows": 2_000_000, "players": 40},
            {"year": 2025, "position": "RB", "rows": 2_000_000, "players": 40},
            {"year": 2025, "position": "QB", "rows": 2_000_000, "players": 40},
        ],
    )

    plan = make_plan(None, None, 2025, 2025, 15)

    assigned = [task for lane in plan for task in lane["tasks"]]
    assert len(plan) == 15
    assert len(assigned) == 15
    assert all(lane["tasks"] for lane in plan)


def test_capacity_audit_plan_balances_one_full_position_year_per_task(monkeypatch) -> None:
    """A capacity audit never duplicates a denominator via player buckets."""
    counts = {
        (2025, "WR"): 15_577_315,
        (2025, "RB"): 11_948_905,
        (2025, "QB"): 6_250_082,
        (2024, "WR"): 5_245_276,
    }
    monkeypatch.setattr(planner, "capacity_inventory", lambda *args, **kwargs: counts)

    plan = make_capacity_audit_plan(None, None, 2024, 2025, 3)

    assigned = [task for lane in plan for task in lane["tasks"]]
    assert len(plan) == 3
    assert {(task["year"], task["position"]) for task in assigned} == {
        (2025, "WR"), (2025, "RB"), (2025, "QB"), (2024, "WR"),
    }
    assert all(task.get("player_buckets", 1) == 1 for task in assigned)
    assert all(task.get("player_bucket") is None for task in assigned)


def test_capacity_audit_plan_uses_only_the_cache_position_inventory(tmp_path) -> None:
    """Capacity preflight must not attach or scan the ops lookup database."""
    snapshot = tmp_path / "snapshot.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_fantasy "
        "(year INTEGER, week INTEGER, position VARCHAR, NFL_player_id VARCHAR)"
    )
    con.execute(
        "INSERT INTO public.player_fantasy VALUES "
        "(2025,1,'RB','rb'),(2025,1,'CB','db'),(2025,1,'P','punter')"
    )
    con.close()

    plan = make_capacity_audit_plan(
        snapshot,
        tmp_path / "missing-ops-cache.duckdb",
        2025,
        2025,
        15,
    )

    assigned = [task for lane in plan for task in lane["tasks"]]
    assert {(task["year"], task["position"], task["rows"]) for task in assigned} == {
        (2025, "RB", 1),
        (2025, "DB", 1),
    }
