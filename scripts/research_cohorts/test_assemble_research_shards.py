from __future__ import annotations

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from assemble_research_shards import (
    assemble_matchup,
    assemble_transactions,
    assemble_transactions_weekly,
)


KEY = dict(
    teams="12t", roster="flx", ppr="half", td="4pt", bracket="6po",
    league_type="ALL", lineup_mode="ALL", keeper_mode="ALL",
    cohort_level=4, format_level=0, year=2024, NFL_player_id="p1",
)


def _write(path, rows):
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def test_transaction_shards_merge_sufficient_statistics(tmp_path):
    rows = []
    for n_lg, adds, faab_sum, faab_n in [(10, 2, 30.0, 2), (30, 9, 140.0, 4)]:
        rows.append({
            **KEY, "n_leagues": n_lg, "n_add_leagues": adds, "n_drop_leagues": 1,
            "sum_faab_pct": faab_sum, "n_faab_pct": faab_n,
            "sum_faab_bid": faab_sum, "n_faab_bid": faab_n,
            "sum_transaction_score": 200.0, "n_transaction_score": 2,
            "sum_add_lamar": 40.0, "n_add_lamar": 2,
            "sum_drop_regret": 10.0, "n_drop_regret": 1,
        })
    paths = [_write(tmp_path / f"t{i}.parquet", [row]) for i, row in enumerate(rows)]
    out = assemble_transactions(paths, tmp_path / "out")
    got = duckdb.connect().execute(
        f"SELECT add_rate_pct,avg_faab_pct,n_leagues FROM read_parquet('{out.as_posix()}')"
    ).fetchone()
    assert got == pytest.approx((27.5, 170.0 / 6.0, 40))


def test_weekly_transaction_shards_merge_sufficient_statistics(tmp_path):
    rows = []
    for n_lg, adds, lamar_sum, lamar_n, faab_sum, faab_n in [
        (10, 2, 40.0, 2, 30.0, 2),
        (30, 9, 120.0, 4, 140.0, 4),
    ]:
        rows.append({
            **KEY, "week": 3, "n_leagues": n_lg, "n_add_lg": adds,
            "sum_add_lamar": lamar_sum, "n_add_lamar": lamar_n,
            "sum_faab_pct": faab_sum, "n_faab_pct": faab_n,
            "sum_faab_bid": faab_sum, "n_faab_bid": faab_n,
        })
    paths = [_write(tmp_path / f"wt{i}.parquet", [row]) for i, row in enumerate(rows)]

    out = assemble_transactions_weekly(paths, tmp_path / "out")
    got = duckdb.connect().execute(
        f"SELECT add_rate_pct,avg_add_lamar,avg_faab_pct,n_leagues,n_add_lg "
        f"FROM read_parquet('{out.as_posix()}')"
    ).fetchone()

    assert got == pytest.approx((27.5, 160.0 / 6.0, 170.0 / 6.0, 40, 11))


def test_transaction_assembly_rejects_partial_additive_schema(tmp_path):
    row = {
        **KEY, "n_leagues": 10, "n_add_leagues": 2, "n_drop_leagues": 1,
        "sum_faab_pct": 30.0, "n_faab_pct": 2,
        "sum_faab_bid": 30.0, "n_faab_bid": 2,
        "sum_transaction_score": 200.0, "n_transaction_score": 2,
        "sum_add_lamar": 40.0, "n_add_lamar": 2,
        "sum_drop_regret": 10.0, "n_drop_regret": 1,
    }
    del row["sum_faab_pct"]
    shard = _write(tmp_path / "partial_transactions.parquet", [row])

    with pytest.raises(
        ValueError,
        match=r"season transaction shard.*sum_faab_pct.*partial_transactions\.parquet",
    ):
        assemble_transactions([shard], tmp_path / "out")


def test_weekly_transaction_assembly_rejects_partial_additive_schema(tmp_path):
    row = {
        **KEY, "week": 3, "n_leagues": 10, "n_add_lg": 2,
        "sum_add_lamar": 40.0, "n_add_lamar": 2,
        "sum_faab_pct": 30.0, "n_faab_pct": 2,
        "sum_faab_bid": 30.0, "n_faab_bid": 2,
    }
    del row["n_add_lamar"]
    shard = _write(tmp_path / "partial_transactions_weekly.parquet", [row])

    with pytest.raises(
        ValueError,
        match=(
            r"weekly transaction shard.*n_add_lamar.*"
            r"partial_transactions_weekly\.parquet"
        ),
    ):
        assemble_transactions_weekly([shard], tmp_path / "out")


def _season_row(n_lg, starts, wins, points, clutch, canon_num, canon_den):
    return {
        **KEY, "pos_grp": "SKILL", "n_rostered_leagues": n_lg, "rostered_weeks": n_lg,
        "rostered_active_weeks": n_lg,
        "started_team_game_weeks": starts, "started_active_weeks": starts,
        "elig_league_weeks": n_lg,
        "roster_eligible_league_weeks": n_lg,
        "team_game_eligible_league_weeks": n_lg,
        "healthy_eligible_league_weeks": n_lg,
        "nfl_active_weeks": 1, "active_week_mask": 1,
        "wins_started_active": wins, "started_champ_active": 1,
        "champ_elig_leagues": n_lg, "sum_champ": 1,
        "sum_pts_started": points, "sum_clutch_started_active": clutch,
        "sum_clutch_started": clutch, "sum_lamar_started": 5.0,
        "canon_num_sr": canon_num, "canon_den_sr": canon_den,
        "canon_num_abs": 50.0, "n_lg_started": starts,
        "wins_started": wins, "losses_started": starts - wins,
        "po_started_weeks": 1, "po_wins_started": 1,
        "reg_started_weeks": starts - 1, "reg_wins_started": max(wins - 1, 0),
        "sum_clutch_po": 1.0, "po_wkwt_credit": 2.0, "n_final_po": 1,
        "po_n_rostered_leagues": n_lg, "n_started_po": 1,
        "playoff_eligible_leagues": n_lg,
        "started_weeks": starts, "n_leagues": n_lg,
        "total_points_observed": points,
        # T7/T8: league-counted champ numerators, the active-scoped playoff-start count,
        # and the inactive-week bitmask (bits 1 and 3 set -> weeks 1 and 3 inactive).
        "n_champ_leagues": 1, "n_champ_start_leagues": 1,
        "po_started_active_weeks": 1, "inactive_week_mask": 0b101,
    }


def _weekly_row(n_lg, starts, wins, points, clutch, losses=None):
    return {
        **KEY, "pos_grp": "SKILL", "week": 1, "rostered_leagues": n_lg, "started_leagues": starts,
        "healthy_started_leagues": starts,
        "roster_eligible_leagues": n_lg, "team_game_eligible_leagues": n_lg,
        "healthy_eligible_leagues": n_lg, "eligible_leagues": n_lg, "wins_started": wins,
        "active_wins_started": wins,
        "losses_started": starts - wins if losses is None else losses,
        "points_started": points,
        "clutch_sum": clutch, "champ_started": 1, "champ_eligible": n_lg,
        "n_leagues": n_lg, "avg_lamar_started": 10.0,
    }


def test_matchup_shards_recalculate_combined_weekly_and_season_rates(tmp_path):
    seasons = [
        _write(tmp_path / "s1.parquet", [_season_row(10, 2, 1, 40.0, 2.0, 5.0, .5)]),
        _write(tmp_path / "s2.parquet", [_season_row(30, 9, 6, 180.0, 9.0, 30.0, 3.0)]),
    ]
    weeklies = [
        _write(tmp_path / "w1.parquet", [_weekly_row(10, 2, 1, 40.0, 2.0)]),
        _write(tmp_path / "w2.parquet", [_weekly_row(30, 9, 6, 180.0, 9.0)]),
    ]
    season_out, weekly_out = assemble_matchup(seasons, weeklies, tmp_path / "out")
    con = duckdb.connect()
    weekly = con.execute(
        f"SELECT start_rate_pct,won_pct,ppg_when_started FROM read_parquet('{weekly_out.as_posix()}')"
    ).fetchone()
    season = con.execute(
        f"SELECT start_rate_pct,won_pct,expected_starts,avg_lamar_started,active_week_mask "
        f"FROM read_parquet('{season_out.as_posix()}')"
    ).fetchone()
    assert weekly == pytest.approx((27.5, 17.5, 20.0))
    # Season rates are weighted over the combined eligible league-week population.
    assert season == pytest.approx((27.5, 17.5, .275, 10.0, 1))


def test_matchup_expected_record_complements_unknown_results(tmp_path):
    season = _write(
        tmp_path / "season.parquet",
        [_season_row(100, 50, 20, 800.0, 10.0, 100.0, 10.0)],
    )
    # Fifty starts, but only forty decided results (20-20). Win% uses all starts;
    # Expected L is the complement of Expected W on that same full basis.
    weekly = _write(
        tmp_path / "weekly.parquet",
        [_weekly_row(100, 50, 20, 800.0, 10.0, losses=20)],
    )

    season_out, weekly_out = assemble_matchup([season], [weekly], tmp_path / "out")
    con = duckdb.connect()
    weekly_values = con.execute(
        f"SELECT win_rate_pct,expected_wins,expected_losses,expected_starts "
        f"FROM read_parquet('{weekly_out.as_posix()}')"
    ).fetchone()
    season_values = con.execute(
        f"SELECT win_rate_pct,expected_wins,expected_losses,expected_starts "
        f"FROM read_parquet('{season_out.as_posix()}')"
    ).fetchone()

    assert weekly_values == pytest.approx((40.0, 0.20, 0.30, 0.5))
    assert season_values == pytest.approx((40.0, 0.20, 0.30, 0.5))


def test_matchup_season_complements_a_zero_decision_week(tmp_path):
    season = _write(
        tmp_path / "season.parquet",
        [_season_row(10, 15, 6, 300.0, 15.0, 20.0, 2.0)],
    )
    decided = _weekly_row(10, 10, 6, 200.0, 10.0, losses=4)
    unknown = _weekly_row(10, 5, 0, 100.0, 5.0, losses=0)
    unknown["week"] = 2

    season_out, _ = assemble_matchup(
        [season],
        [_write(tmp_path / "weekly.parquet", [decided, unknown])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(
        f"SELECT expected_wins,expected_losses,expected_starts,won_pct,lost_pct "
        f"FROM read_parquet('{season_out.as_posix()}')"
    ).fetchone()

    # Week 2's 0.5 expected start remains in Exp Starts and is represented in
    # Expected L so the displayed W-L always sums to Exp Starts.
    assert got == pytest.approx((0.6, 0.9, 1.5, 30.0, 45.0))


def test_matchup_assembly_rejects_weekly_shards_without_explicit_losses(tmp_path):
    season = _write(
        tmp_path / "season.parquet",
        [_season_row(100, 50, 20, 800.0, 10.0, 100.0, 10.0)],
    )
    legacy_week = _weekly_row(100, 50, 20, 800.0, 10.0, losses=20)
    del legacy_week["losses_started"]
    weekly = _write(tmp_path / "legacy_weekly.parquet", [legacy_week])

    with pytest.raises(
        ValueError,
        match=r"weekly matchup shard.*losses_started.*legacy_weekly\.parquet",
    ):
        assemble_matchup([season], [weekly], tmp_path / "out")


def test_matchup_season_uses_weighted_weekly_rates_and_weekly_value_sums(tmp_path):
    season_row = _season_row(100, 10, 6, 200.0, 20.0, 100.0, 10.0)
    season_row.update(
        elig_league_weeks=110,
        canon_num_abs=50.0,
        n_lg_started=10,
        sum_clutch_started_active=20.0,
    )
    week_one = _weekly_row(10, 10, 6, 200.0, 10.0)
    week_two = _weekly_row(100, 0, 0, 0.0, 0.0)
    week_two["week"] = 2

    season_out, _ = assemble_matchup(
        [_write(tmp_path / "season.parquet", [season_row])],
        [_write(tmp_path / "weekly.parquet", [week_one, week_two])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(
        f"SELECT start_rate_pct,won_pct,lost_pct,total_lamar_started,avg_clutch_started "
        f"FROM read_parquet('{season_out.as_posix()}')"
    ).fetchone()

    assert got == pytest.approx((9.0909091, 30.0, 20.0, 10.0, 0.2))


def test_matchup_season_win_rate_is_expected_wins_over_expected_starts(tmp_path):
    """Season Win% must use the same weekly-weighted start denominator as Expected W-L."""
    season_row = _season_row(100, 101, 51, 0.0, 0.0, 0.0, 1.0)
    week_one = _weekly_row(10, 1, 1, 0.0, 0.0, losses=0)
    week_two = _weekly_row(100, 100, 50, 0.0, 0.0, losses=50)
    week_two["week"] = 2

    season_out, _ = assemble_matchup(
        [_write(tmp_path / "season.parquet", [season_row])],
        [_write(tmp_path / "weekly.parquet", [week_one, week_two])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(
        f"SELECT win_rate_pct,expected_wins,expected_losses,expected_starts "
        f"FROM read_parquet('{season_out.as_posix()}')"
    ).fetchone()

    # Expected W-L is 0.6-0.5 over 1.1 expected starts, so Win% is 54.545...%.
    # The old season rollup used raw decided starts (51/101 = 50.495%), which is
    # not the requested weekly weighted definition when league support changes by week.
    assert got == pytest.approx((100.0 * 0.6 / 1.1, 0.6, 0.5, 1.1))


def test_matchup_season_rebuilds_active_wins_from_weekly_active_starts(tmp_path):
    """The legacy active-win diagnostic must use the same weekly active scope."""
    season_row = _season_row(100, 10, 9, 0.0, 0.0, 0.0, 1.0)
    season_row["started_active_weeks"] = 2
    season_row["wins_started_active"] = 9  # stale all-starts numerator
    week = _weekly_row(100, 10, 9, 0.0, 0.0)
    week["healthy_started_leagues"] = 2
    week["healthy_eligible_leagues"] = 2
    week["active_wins_started"] = 2

    season_out, _ = assemble_matchup(
        [_write(tmp_path / "active_wins.parquet", [season_row])],
        [_write(tmp_path / "active_wins_weekly.parquet", [week])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(
        f"SELECT wins_started_active,win_rate_started_pct "
        f"FROM read_parquet('{season_out.as_posix()}')"
    ).fetchone()

    assert got == pytest.approx((2.0, 100.0))


def test_matchup_assembler_preserves_canonical_rostered_league_weeks(tmp_path):
    """The release shard schema's weighted roster exposure must survive sraw."""
    season_row = _season_row(100, 10, 6, 200.0, 20.0, 100.0, 10.0)
    season_row["rostered_weeks"] = 80
    # Presence of the canonical field selects the weighted-exposure branch;
    # sraw reconstructs it from the additive raw rostered_weeks field.
    season_row["rostered_league_weeks"] = 80
    weekly = _weekly_row(10, 8, 6, 200.0, 10.0)
    weekly["rostered_leagues"] = 8

    season_out, _ = assemble_matchup(
        [_write(tmp_path / "season.parquet", [season_row])],
        [_write(tmp_path / "weekly.parquet", [weekly])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(
        f"SELECT roster_rate_pct FROM read_parquet('{season_out.as_posix()}')"
    ).fetchone()[0]
    assert got == pytest.approx(80.0)


def test_matchup_clutch_uses_champion_signal_denominator(tmp_path):
    """Clutch uses champion-eligible leagues, not the broader position population."""
    season_row = _season_row(100, 20, 10, 0.0, 20.0, 0.0, 1.0)
    week = _weekly_row(100, 20, 10, 0.0, 10.0)
    week["champ_eligible"] = 10

    season_out, weekly_out = assemble_matchup(
        [_write(tmp_path / "season.parquet", [season_row])],
        [_write(tmp_path / "weekly.parquet", [week])],
        tmp_path / "out",
    )
    con = duckdb.connect()
    weekly_clutch = con.execute(
        f"SELECT avg_clutch_started FROM read_parquet('{weekly_out.as_posix()}')"
    ).fetchone()[0]
    season_clutch = con.execute(
        f"SELECT avg_clutch_started FROM read_parquet('{season_out.as_posix()}')"
    ).fetchone()[0]

    assert weekly_clutch == pytest.approx(1.0)  # 10 clutch / 10 champion leagues
    assert season_clutch == pytest.approx(0.2)  # 20 season clutch / 100 champ-eligible leagues


def test_matchup_fallback_uses_position_and_signal_denominators(tmp_path):
    """The no-weekly-lane fallback must preserve the denominator contract."""
    row = _season_row(100, 20, 10, 40.0, 30.0, 5.0, .5)
    row.update(
        rostered_active_weeks=50,
        roster_eligible_league_weeks=100,
        team_game_eligible_league_weeks=80,
        healthy_eligible_league_weeks=60,
        losses_started=5,
        champ_elig_leagues=40,
        n_champ_leagues=8,
    )
    # A zero-eligible weekly row makes the weekly rollup intentionally absent, forcing
    # the season fallback expressions to run.
    week = _weekly_row(0, 0, 0, 0.0, 0.0)
    week.update(
        eligible_leagues=0,
        roster_eligible_leagues=0,
        team_game_eligible_leagues=0,
        healthy_eligible_leagues=0,
        champ_eligible=0,
    )
    season_out, _ = assemble_matchup(
        [_write(tmp_path / "fallback.parquet", [row])],
        [_write(tmp_path / "fallback_week.parquet", [week])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(
        f"""SELECT start_rate_pct, healthy_start_rate_pct, won_pct, lost_pct,
            avg_clutch_started
            FROM read_parquet('{season_out.as_posix()}')"""
    ).fetchone()
    assert got == pytest.approx((25.0, 100.0 * 20 / 60, 12.5, 6.25, 30.0 / 40.0))


def test_matchup_locked_denominators_and_inactive_week_union(tmp_path):
    """T7/T8 shard-additivity. Two DISJOINT league shards for the same player-year:

    * champ/playoff numerators are LEAGUE counts, so they sum across shards, and every rate
      divides by the summed eligible-league count -- never by the leagues that rostered him;
    * inactive weeks are a week SET, so they union with BIT_OR. Shard A saw weeks 1+3 idle,
      shard B saw weeks 3+5. The truth is three idle weeks {1,3,5}; SUMming the per-shard
      counts would say four.
    """
    a = _season_row(100, 10, 6, 200.0, 20.0, 100.0, 10.0)
    a.update(n_champ_leagues=4, n_champ_start_leagues=1, n_final_po=40,
             n_started_po=25, inactive_week_mask=0b00101)     # weeks 1, 3
    b = _season_row(300, 30, 18, 600.0, 60.0, 300.0, 30.0)
    b.update(n_champ_leagues=8, n_champ_start_leagues=3, n_final_po=110,
             n_started_po=70, inactive_week_mask=0b10100)     # weeks 3, 5

    season_out, _ = assemble_matchup(
        [_write(tmp_path / "a.parquet", [a]), _write(tmp_path / "b.parquet", [b])],
        [_write(tmp_path / "wa.parquet", [_weekly_row(100, 10, 6, 200.0, 20.0)]),
         _write(tmp_path / "wb.parquet", [_weekly_row(300, 30, 18, 600.0, 60.0)])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(f"""SELECT champ_total_pct, champ_as_starter_pct,
        playoff_total_pct, playoff_as_starter_pct, inactive_weeks, n_leagues
        FROM read_parquet('{season_out.as_posix()}')""").fetchone()

    assert got[5] == 400, "eligible leagues sum across disjoint shards"
    assert got[0] == pytest.approx(100.0 * 12 / 400)   # champ total
    assert got[1] == pytest.approx(100.0 * 4 / 400)    # champ as-starter
    assert got[2] == pytest.approx(100.0 * 150 / 400)  # playoff total
    assert got[3] == pytest.approx(100.0 * 95 / 400)   # playoff as-starter
    assert got[4] == 3, "week 3 is idle in both shards and must be counted once"


def test_matchup_conditional_win_rate_cannot_exceed_100(tmp_path):
    """Regression for the 1,700% win_rate_started_pct / 1,200% pct_starts_in_playoffs.

    An IDP-shaped season: rostered and started all year, but on an NFL field for only one
    week. The all-weeks numerators (wins_started=17, po_started_weeks=4) over the
    active-scoped started_weeks=1 produced 1,700% / 400%. Both sides are active-scoped now.
    """
    row = _season_row(50, 1, 1, 20.0, 1.0, 5.0, 0.5)
    row.update(wins_started=17, losses_started=0, started_weeks=1,
               started_active_weeks=1, wins_started_active=1,
               po_started_weeks=4, po_started_active_weeks=1)
    season_out, _ = assemble_matchup(
        [_write(tmp_path / "idp.parquet", [row])],
        [_write(tmp_path / "idpw.parquet", [_weekly_row(50, 1, 1, 20.0, 1.0)])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(
        f"SELECT win_rate_started_pct, pct_starts_in_playoffs "
        f"FROM read_parquet('{season_out.as_posix()}')").fetchone()
    assert got == pytest.approx((100.0, 100.0))


def test_matchup_undecided_starts_are_in_expected_loss_complement(tmp_path):
    """Expected W + Expected L must equal the start share with no decided result."""
    row = _season_row(3, 1, 0, 20.0, 1.0, 5.0, 0.5)
    row.update(wins_started=0, losses_started=0, wins_started_active=0,
               started_active_weeks=1, elig_league_weeks=3)
    undecided_week = _weekly_row(3, 1, 0, 20.0, 1.0, losses=0)
    season_out, _ = assemble_matchup(
        [_write(tmp_path / "u.parquet", [row])],
        [_write(tmp_path / "uw.parquet", [undecided_week])],
        tmp_path / "out",
    )
    got = duckdb.connect().execute(
        f"SELECT start_rate_pct, won_pct, lost_pct "
        f"FROM read_parquet('{season_out.as_posix()}')").fetchone()
    assert got[0] == pytest.approx(100.0 / 3.0)
    assert got[1] == pytest.approx(0.0)
    assert got[2] == pytest.approx(100.0 / 3.0)


def _manifest(path, rows):
    """Write a shard_manifest.parquet with explicit column types (bucket may be all-NULL)."""
    schema = pa.schema([("table", pa.string()), ("year_start", pa.int64()),
                        ("year_end", pa.int64()), ("bucket", pa.int64()),
                        ("buckets", pa.int64()), ("fingerprint", pa.string())])
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def _shard_dir(tmp_path, name, manifest_rows):
    d = tmp_path / name
    _write(_ensure(d) / "research_matchup_player_season.parquet",
           [_season_row(10, 2, 1, 40.0, 2.0, 5.0, .5)])
    _write(d / "research_matchup_weekly.parquet", [_weekly_row(10, 2, 1, 40.0, 2.0)])
    _manifest(d / "shard_manifest.parquet", manifest_rows)
    return d


def _ensure(d):
    d.mkdir(parents=True, exist_ok=True)
    return d


def _assemble(tmp_path, roots, **kw):
    from assemble_research_shards import _files
    return assemble_matchup(
        _files(roots, "research_matchup_player_season.parquet"),
        _files(roots, "research_matchup_weekly.parquet"),
        tmp_path / "out", roots=roots, **kw)


def test_cover_accepts_an_exact_bucket_partition(tmp_path):
    roots = [_shard_dir(tmp_path, f"b{i}", [{
        "table": "matchup", "year_start": 2024, "year_end": 2024,
        "bucket": i, "buckets": 5, "fingerprint": "fp"}]) for i in range(5)]
    season_out, _ = _assemble(tmp_path, roots)
    assert season_out.exists()


def test_cover_rejects_a_year_covered_twice(tmp_path):
    """The whole point: a whole-year shard PLUS that year's buckets doubles every count,
    and the rates still look right, so nothing downstream would ever notice."""
    roots = [_shard_dir(tmp_path, "whole", [{
        "table": "matchup", "year_start": 2024, "year_end": 2024,
        "bucket": None, "buckets": None, "fingerprint": "fp"}])]
    roots += [_shard_dir(tmp_path, f"b{i}", [{
        "table": "matchup", "year_start": 2024, "year_end": 2024,
        "bucket": i, "buckets": 5, "fingerprint": "fp"}]) for i in range(5)]
    with pytest.raises(ValueError, match=r"2024: covered BOTH"):
        _assemble(tmp_path, roots)


def test_cover_rejects_a_missing_bucket(tmp_path):
    """A rescue bucket that failed silently drops its leagues out of every denominator."""
    roots = [_shard_dir(tmp_path, f"b{i}", [{
        "table": "matchup", "year_start": 2024, "year_end": 2024,
        "bucket": i, "buckets": 5, "fingerprint": "fp"}]) for i in (0, 1, 3, 4)]
    with pytest.raises(ValueError, match=r"buckets \[2\] MISSING"):
        _assemble(tmp_path, roots)


def test_cover_fails_closed_without_manifests(tmp_path):
    d = tmp_path / "legacy"
    _write(_ensure(d) / "research_matchup_player_season.parquet",
           [_season_row(10, 2, 1, 40.0, 2.0, 5.0, .5)])
    _write(d / "research_matchup_weekly.parquet", [_weekly_row(10, 2, 1, 40.0, 2.0)])
    with pytest.raises(ValueError, match="no shard_manifest.parquet"):
        _assemble(tmp_path, [d])
    season_out, _ = _assemble(tmp_path, [d], allow_unmanifested=True)
    assert season_out.exists()


def test_population_denominator_is_not_summed_across_buckets(tmp_path):
    """The denominator is a property of league SETTINGS, so it must not be sharded.

    Two buckets, each holding 100 eligible leagues (200 in the population). The player is
    rostered in bucket 0 ONLY, so he appears in one shard carrying that bucket's count of
    100. Summing shard denominators gives him 100 -- half the population -- and doubles
    every rate he has. Measured on the real build: ~51% of rows in every bucketed year,
    averaging 0.68-0.71 of the true denominator, worst case 0.004.

    With the population lattice supplied, his denominator is 200 regardless of where he
    happened to be rostered, which is the whole point of an eligible-leagues denominator.
    """
    season = _season_row(100, 1, 1, 20.0, 1.0, 5.0, 0.5)
    season.update(n_leagues=100, n_rostered_leagues=1, rostered_weeks=100,
                  roster_eligible_league_weeks=200, n_champ_leagues=1,
                  n_final_po=1, n_started_po=1)
    weekly = _weekly_row(100, 1, 1, 20.0, 1.0)
    weekly.update(n_leagues=100, eligible_leagues=100, champ_eligible=100)
    d = tmp_path / "b0"
    d.mkdir(parents=True, exist_ok=True)
    _write(d / "research_matchup_player_season.parquet", [season])
    _write(d / "research_matchup_weekly.parquet", [weekly])
    _manifest(d / "shard_manifest.parquet", [{
        "table": "matchup", "year_start": 2024, "year_end": 2024,
        "bucket": 0, "buckets": 1, "fingerprint": "fp"}])

    dims = {k: KEY[k] for k in ("teams", "roster", "ppr", "td", "bracket",
                                "league_type", "lineup_mode", "keeper_mode")}
    ds = _write(tmp_path / "denom_season.parquet",
                [{**dims, "year": 2024, "pos_grp": "SKILL", "n_leagues": 200,
                  "n_leagues_champ": 100, "n_leagues_po": 80}])
    dw = _write(tmp_path / "denom_week.parquet",
                [{**dims, "year": 2024, "week": 1, "pos_grp": "SKILL",
                  "n_lg": 200, "n_champ_lg": 200}])
    pg = _write(tmp_path / "pos_grp.parquet",
                [{"NFL_player_id": "p1", "year": 2024, "pos_grp": "SKILL"}])

    from assemble_research_shards import _files
    season_out, weekly_out = assemble_matchup(
        _files([d], "research_matchup_player_season.parquet"),
        _files([d], "research_matchup_weekly.parquet"),
        tmp_path / "out", roots=[d],
        denom_season=ds, denom_week=dw, pos_grp=pg)

    con = duckdb.connect()
    got = con.execute(
        f"SELECT n_leagues, roster_rate_pct, champ_total_pct, playoff_total_pct, "
        f"start_rate_pct FROM read_parquet('{season_out.as_posix()}')").fetchone()
    wk = con.execute(
        f"SELECT eligible_leagues, champ_eligible, start_rate_pct "
        f"FROM read_parquet('{weekly_out.as_posix()}')").fetchone()

    assert got[0] == 200, "the season denominator must be the population, not the bucket"
    assert got[1] == pytest.approx(50.0)   # 100 rostered league-weeks / 200 eligible
    assert got[2] == pytest.approx(1.0)    # champ total uses champion-signal support
    assert got[3] == pytest.approx(1.25)   # 1 playoff league / 80 signal-eligible leagues
    assert wk[0] == 200 and wk[1] == 200
    assert wk[2] == pytest.approx(0.5)     # 1 started / 200 eligible


def test_population_denominator_must_cover_every_row(tmp_path):
    """A denominator lattice that misses a cohort cell fails closed rather than silently
    falling back to the sharded value that is being replaced."""
    d = tmp_path / "b0"
    d.mkdir(parents=True, exist_ok=True)
    _write(d / "research_matchup_player_season.parquet",
           [_season_row(100, 1, 1, 20.0, 1.0, 5.0, 0.5)])
    _write(d / "research_matchup_weekly.parquet", [_weekly_row(100, 1, 1, 20.0, 1.0)])
    _manifest(d / "shard_manifest.parquet", [{
        "table": "matchup", "year_start": 2024, "year_end": 2024,
        "bucket": None, "buckets": None, "fingerprint": "fp"}])
    dims = {k: KEY[k] for k in ("teams", "roster", "ppr", "td", "bracket",
                                "league_type", "lineup_mode", "keeper_mode")}
    ds = _write(tmp_path / "ds.parquet",
                [{**dims, "year": 1999, "pos_grp": "SKILL", "n_leagues": 200}])
    dw = _write(tmp_path / "dw.parquet",
                [{**dims, "year": 1999, "week": 1, "pos_grp": "SKILL",
                  "n_lg": 200, "n_champ_lg": 200}])
    pg = _write(tmp_path / "pg.parquet",
                [{"NFL_player_id": "p1", "year": 2024, "pos_grp": "SKILL"}])
    from assemble_research_shards import _files
    with pytest.raises(ValueError, match="population denominator missing"):
        assemble_matchup(
            _files([d], "research_matchup_player_season.parquet"),
            _files([d], "research_matchup_weekly.parquet"),
            tmp_path / "out", roots=[d],
            denom_season=ds, denom_week=dw, pos_grp=pg)
