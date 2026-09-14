"""
Unit tests for playoff bracket SQL tracers.

Tests verify:
- Championship week formula + validation
- Backward walk with byes, symmetry, elimination tracking
- Forfeit tiebreak
- Championship tracer does NOT write consolation columns
- Consolation tracer placement_rank 1..N, sacko, postseason derivation
"""

import duckdb
import sys
from pathlib import Path

# Add paths for imports
SCRIPT_DIR = Path(__file__).resolve().parent
TESTS_DIR = SCRIPT_DIR.parent.parent
SCRIPTS_DIR = TESTS_DIR.parent
MULTI_LEAGUE_DIR = SCRIPTS_DIR / "multi_league"
MODULES_DIR = MULTI_LEAGUE_DIR / "transformations" / "matchup" / "modules"

sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(MULTI_LEAGUE_DIR))
sys.path.insert(0, str(MODULES_DIR))

from multi_league.transformations.matchup.modules.playoff_bracket import bracket_tracer


# ---------------------------------------------------------------------------
# Helper: create a DuckDB with regular season + playoff data for bracket tests
# ---------------------------------------------------------------------------


def _build_bracket_db(
    regular_season_rows: list[dict],
    playoff_rows: list[dict],
) -> duckdb.DuckDBPyConnection:
    """Build an in-memory DuckDB with matchup table containing both
    regular-season and playoff data."""
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER DEFAULT 0,
            is_consolation INTEGER DEFAULT 0,
            is_championship BOOLEAN DEFAULT FALSE,
            champion INTEGER DEFAULT 0,
            playoff_round VARCHAR DEFAULT '',
            final_playoff_seed INTEGER,
            sacko INTEGER DEFAULT 0,
            placement_rank INTEGER DEFAULT 0,
            placement_game INTEGER DEFAULT 0,
            consolation_round VARCHAR DEFAULT '',
            postseason INTEGER DEFAULT 0
        )
    """)

    all_rows = regular_season_rows + playoff_rows
    for r in all_rows:
        conn.execute(
            "INSERT INTO matchup (year, week, franchise_id, opponent, "
            "opponent_franchise_id, team_points, opponent_points, win, loss, tie, "
            "is_playoffs, is_consolation, is_championship, champion, "
            "playoff_round, final_playoff_seed) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r.get("year"),
                r.get("week"),
                r.get("franchise_id"),
                r.get("opponent"),
                r.get("opponent_franchise_id"),
                r.get("team_points"),
                r.get("opponent_points"),
                r.get("win"),
                r.get("loss"),
                r.get("tie"),
                r.get("is_playoffs", 0),
                r.get("is_consolation", 0),
                r.get("is_championship", False),
                r.get("champion", 0),
                r.get("playoff_round", ""),
                r.get("final_playoff_seed"),
            ],
        )

    return conn


def test_consolation_tracer_can_preserve_platform_api_flags():
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER DEFAULT 0,
            is_consolation INTEGER DEFAULT 0,
            is_championship BOOLEAN DEFAULT FALSE,
            champion INTEGER DEFAULT 0,
            playoff_round VARCHAR DEFAULT '',
            final_playoff_seed INTEGER,
            sacko INTEGER DEFAULT 0,
            placement_rank INTEGER DEFAULT 0,
            placement_game INTEGER DEFAULT 0,
            consolation_round VARCHAR DEFAULT '',
            postseason INTEGER DEFAULT 0
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO matchup (
            year, week, franchise_id, opponent, opponent_franchise_id,
            team_points, opponent_points, win, loss, tie,
            is_playoffs, is_consolation, is_championship, champion,
            playoff_round, final_playoff_seed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (2014, 15, "T1", "T2", "T2", 120.0, 100.0, 1, 0, 0, 1, 0, True, 1, "championship", 1),
            (2014, 15, "T2", "T1", "T1", 100.0, 120.0, 0, 1, 0, 1, 0, True, 0, "championship", 2),
            (2014, 15, "T3", "T4", "T4", 90.0, 80.0, 1, 0, 0, 0, 1, False, 0, "", 3),
            (2014, 15, "T4", "T3", "T3", 80.0, 90.0, 0, 1, 0, 0, 1, False, 0, "", 4),
            (2014, 15, "T5", "T6", "T6", 70.0, 60.0, 1, 0, 0, 0, 0, False, 0, "", 5),
            (2014, 15, "T6", "T5", "T5", 60.0, 70.0, 0, 1, 0, 0, 0, False, 0, "", 6),
        ],
    )

    trace_consolation_sql(
        conn,
        2014,
        {
            "playoff_teams": 2,
            "playoff_start_week": 15,
            "end_week": 15,
            "num_teams": 6,
            "preserve_consolation_flags": True,
        },
    )

    rows = conn.execute(
        """
        SELECT franchise_id, is_consolation, consolation_round
        FROM matchup
        WHERE is_playoffs = 0
        ORDER BY franchise_id
        """
    ).fetchall()
    assert rows == [
        ("T3", 1, "consolation_final"),
        ("T4", 1, "consolation_final"),
        ("T5", 0, None),
        ("T6", 0, None),
    ]


def _make_reg_season(year, num_teams, num_weeks, id_prefix="T"):
    """Generate simple regular-season rows where team i beats team i+1."""
    rows = []
    teams = [f"{id_prefix}{i}" for i in range(1, num_teams + 1)]
    for wk in range(1, num_weeks + 1):
        # Simple round-robin: pair teams i with teams[num_teams-1-i]
        for i in range(num_teams // 2):
            a = teams[i]
            b = teams[num_teams - 1 - i]
            # Higher-seeded team (lower index) wins more often
            a_pts = 120.0 - i * 5 + wk * 0.1
            b_pts = 80.0 + i * 3 + wk * 0.1
            rows.append(
                {
                    "year": year,
                    "week": wk,
                    "franchise_id": a,
                    "opponent": b,
                    "opponent_franchise_id": b,
                    "team_points": a_pts,
                    "opponent_points": b_pts,
                }
            )
            rows.append(
                {
                    "year": year,
                    "week": wk,
                    "franchise_id": b,
                    "opponent": a,
                    "opponent_franchise_id": a,
                    "team_points": b_pts,
                    "opponent_points": a_pts,
                }
            )
    return rows


def _make_matchup_pair(year, week, a, b, a_pts, b_pts):
    """Create a symmetric pair of matchup rows."""
    return [
        {
            "year": year,
            "week": week,
            "franchise_id": a,
            "opponent": b,
            "opponent_franchise_id": b,
            "team_points": a_pts,
            "opponent_points": b_pts,
        },
        {
            "year": year,
            "week": week,
            "franchise_id": b,
            "opponent": a,
            "opponent_franchise_id": a,
            "team_points": b_pts,
            "opponent_points": a_pts,
        },
    ]


def _make_bye_row(year, week, team_id, seed=None):
    """Create a bye row (NULL opponent, NULL points)."""
    return {
        "year": year,
        "week": week,
        "franchise_id": team_id,
        "opponent": None,
        "opponent_franchise_id": None,
        "team_points": None,
        "opponent_points": None,
        "final_playoff_seed": seed,
    }


def test_sql_winner_trusts_platform_win_flag_when_scores_tie():
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO matchup (
            year, week, franchise_id, opponent_franchise_id,
            team_points, opponent_points, win
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (2018, 16, "seed_1", "seed_2_api_winner", 135.0, 135.0, 0),
            (2018, 16, "seed_2_api_winner", "seed_1", 135.0, 135.0, 1),
        ],
    )

    winner = bracket_tracer._sql_winner(
        conn,
        "matchup",
        2018,
        "seed_1",
        "seed_2_api_winner",
        16,
        16,
        "franchise_id",
        "opponent_franchise_id",
        {"seed_1": 1, "seed_2_api_winner": 2},
    )

    assert winner == "seed_2_api_winner"


# ---------------------------------------------------------------------------
# Phase 2: Championship bracket tracer tests
# ---------------------------------------------------------------------------


class TestChampionshipWeek:
    """Tests for _compute_championship_week_with_validation."""

    def test_championship_week_formula_matches_actual(self):
        """When formula and actual data agree, both return the same week."""
        year = 2024
        reg = _make_reg_season(year, 8, 14)
        playoff = []
        # 4-team playoff, weeks 15-16, prt=0
        # Round 1: week 15 (semis), Round 2: week 16 (championship)
        playoff += _make_matchup_pair(year, 15, "T1", "T4", 120, 80)
        playoff += _make_matchup_pair(year, 15, "T2", "T3", 110, 90)
        playoff += _make_matchup_pair(year, 16, "T1", "T2", 130, 100)

        conn = _build_bracket_db(reg, playoff)
        rounds = bracket_tracer._round_weeks(4, 15, 16, 0)

        result = bracket_tracer._compute_championship_week_with_validation(
            conn, "matchup", year, "franchise_id", 15, 16, 4, 0, rounds
        )
        assert result == 16
        conn.close()

    def test_championship_week_off_by_one_trusts_actual(self):
        """When formula is off by 1 from actual data (formula > actual), trust actual.

        Handles short/truncated seasons where the actual data ends before the
        formula-predicted championship week.
        """
        year = 2024
        reg = _make_reg_season(year, 8, 14)
        playoff = []
        # Data has games in weeks 15 and 16, but formula says champ is week 17
        playoff += _make_matchup_pair(year, 15, "T1", "T4", 120, 80)
        playoff += _make_matchup_pair(year, 15, "T2", "T3", 110, 90)
        playoff += _make_matchup_pair(year, 16, "T1", "T2", 130, 100)
        # No data in week 17

        conn = _build_bracket_db(reg, playoff)
        # Formula says 3 rounds (weeks 15, 16, 17) for 8-team playoff
        rounds = bracket_tracer._round_weeks(8, 15, 17, 0)
        # Formula championship week = 17, actual = 16 (off by 1 -> trust actual)

        result = bracket_tracer._compute_championship_week_with_validation(
            conn, "matchup", year, "franchise_id", 15, 17, 8, 0, rounds
        )
        assert result == 16
        conn.close()

    def test_championship_week_caps_at_formula_when_consolation_extends_past(self):
        """When actual data extends past formula (consolation games keep the
        league active a week beyond the championship), cap at formula.

        Concrete fleet repro: dingleberry_derby 2018 — 4-team bracket
        (formula championship W15) but `last_scored_leg=16` and W16 has
        consolation games (skeeton vs brfarley, JSandlin vs Werthers). The
        max-week-with-games SQL returns 16 because consolation games count.
        Without capping, the tracer treats W16 as the championship week and
        writes (champion, 16) and (runner_up, 16) into classifications,
        flagging W16 phantom rows as is_playoffs=1.
        """
        year = 2018
        reg = _make_reg_season(year, 10, 13)
        playoff = []
        # 4-team championship bracket: W14 semifinals, W15 championship
        playoff += _make_matchup_pair(year, 14, "T1", "T4", 120, 80)  # SF1
        playoff += _make_matchup_pair(year, 14, "T2", "T3", 110, 90)  # SF2
        playoff += _make_matchup_pair(year, 15, "T1", "T2", 130, 100)  # Championship
        # W16 consolation games (NOT championship-bracket teams)
        playoff += _make_matchup_pair(year, 16, "T5", "T8", 100, 95)
        playoff += _make_matchup_pair(year, 16, "T6", "T7", 110, 105)

        conn = _build_bracket_db(reg, playoff)
        # Formula: 4-team bracket, weeks 14-15 (rounds=[(14,14), (15,15)])
        rounds = bracket_tracer._round_weeks(4, 14, 16, 0)
        assert rounds == [(14, 14), (15, 15)], f"sanity: rounds={rounds}"

        # formula_week=15, actual_week=16 (W16 has consolation games).
        # Per phantom-row policy, the tracer must cap at formula_week so it
        # doesn't classify W16 phantom rows as part of the championship.
        result = bracket_tracer._compute_championship_week_with_validation(
            conn, "matchup", year, "franchise_id", 14, 16, 4, 0, rounds
        )
        assert (
            result == 15
        ), f"championship_week must cap at formula (15) when consolation games extend past it; got {result}"
        conn.close()


class TestChampionshipMatchupIdentification:
    def test_ignores_fetcher_championship_flags_from_non_playoff_seeds(self):
        """A stale lower-bracket championship flag must not override seeds."""
        year = 2025
        reg = _make_reg_season(year, 10, 13)
        playoff = []

        for week in (14, 15):
            playoff += _make_matchup_pair(year, week, "T1", "T4", 120, 100)
            playoff += _make_matchup_pair(year, week, "T2", "T3", 115, 95)
            playoff += _make_matchup_pair(year, week, "T7", "T10", 90, 80)
            playoff += _make_matchup_pair(year, week, "T8", "T9", 92, 70)

        for week in (16, 17):
            playoff += _make_matchup_pair(year, week, "T1", "T2", 125, 110)
            playoff += _make_matchup_pair(year, week, "T4", "T3", 100, 105)
            lower_final = _make_matchup_pair(year, week, "T7", "T8", 88, 99)
            for row in lower_final:
                row["is_championship"] = True
                if row["franchise_id"] == "T8":
                    row["champion"] = 1
            playoff += lower_final

        conn = _build_bracket_db(reg, playoff)
        try:
            result = bracket_tracer.trace_championship_bracket_sql(
                conn,
                year,
                {
                    "playoff_teams": 4,
                    "bye_teams": 0,
                    "playoff_start_week": 14,
                    "end_week": 17,
                    "num_teams": 10,
                    "playoff_round_type": 1,
                },
                table="matchup",
                id_col="franchise_id",
                write_back=False,
            )
        finally:
            conn.close()

        assert set(result["championship_teams"]) == {"T1", "T2"}
        assert result["champion"] == "T1"


class TestBackwardWalk:
    """Tests for _backward_walk with byes."""

    def test_backward_walk_6_team_2_byes(self):
        """6-team playoff with 2 byes: seeds 1,2 get byes in round 1.

        Round 1 (week 15): 3v6, 4v5 (seeds 1,2 on bye)
        Round 2 (week 16): 1 vs winner(4v5), 2 vs winner(3v6) — semis
        Round 3 (week 17): championship

        Backward walk from championship should find 6 teams total in
        the champ bracket, with byes handled correctly.
        """
        year = 2024
        reg = _make_reg_season(year, 12, 14)  # 12 teams, 14 weeks

        playoff = []
        # Bye rows for seeds 1 and 2 in week 15
        playoff.append(_make_bye_row(year, 15, "T1", seed=1))
        playoff.append(_make_bye_row(year, 15, "T2", seed=2))
        # Round 1 (QF): T3 vs T6 (T3 wins), T4 vs T5 (T4 wins)
        playoff += _make_matchup_pair(year, 15, "T3", "T6", 120, 80)
        playoff += _make_matchup_pair(year, 15, "T4", "T5", 110, 90)
        # Round 2 (SF): T1 vs T4 (T1 wins), T2 vs T3 (T2 wins)
        playoff += _make_matchup_pair(year, 16, "T1", "T4", 130, 100)
        playoff += _make_matchup_pair(year, 16, "T2", "T3", 125, 95)
        # Round 3 (Championship): T1 vs T2 (T1 wins)
        playoff += _make_matchup_pair(year, 17, "T1", "T2", 140, 110)

        conn = _build_bracket_db(reg, playoff)
        opp_col = bracket_tracer._pick_opp_col(conn, "matchup", "franchise_id")
        seeds = bracket_tracer._sql_seeds(conn, "matchup", year, 15, "franchise_id", False)
        rounds = bracket_tracer._round_weeks(6, 15, 17, 0)

        result = bracket_tracer._backward_walk(
            conn, "matchup", year, "franchise_id", opp_col, seeds, "T1", "T2", 17, rounds, 2
        )

        # Champion and runner-up
        assert result["champion"] == "T1"
        assert result["runner_up"] == "T2"

        # All 6 playoff teams should be in champ_bracket_teams
        assert len(result["champ_bracket_teams"]) == 6
        for t in ("T1", "T2", "T3", "T4", "T5", "T6"):
            assert t in result["champ_bracket_teams"], f"{t} not in champ_bracket_teams"

        # Elimination rounds
        elim = result["elimination_round"]
        assert elim.get("T2") == 3  # Lost in championship (round 3)
        assert elim.get("T3") == 2  # Lost in semifinal (round 2)
        assert elim.get("T4") == 2  # Lost in semifinal (round 2)
        assert elim.get("T5") == 1  # Lost in quarterfinal (round 1)
        assert elim.get("T6") == 1  # Lost in quarterfinal (round 1)
        # T1 (champion) should NOT be in elimination_round
        assert "T1" not in elim

        # Classifications: all PLAYED playoff games classified as "playoff".
        # Per project policy (memory: feedback_phantom_rows_no_flags) bye rows
        # are NOT classified — they represent weeks the team didn't play, and
        # phantom rows must never carry is_playoffs / playoff_round flags.
        cls = result["classifications"]
        # Week 17: T1, T2
        assert cls.get(("T1", 17)) == "playoff"
        assert cls.get(("T2", 17)) == "playoff"
        # Week 16: T1, T2, T3, T4
        assert cls.get(("T1", 16)) == "playoff"
        assert cls.get(("T4", 16)) == "playoff"
        # Week 15: T3, T4, T5, T6
        assert cls.get(("T3", 15)) == "playoff"
        assert cls.get(("T6", 15)) == "playoff"
        # Bye teams should NOT be classified in week 15
        assert ("T1", 15) not in cls
        assert ("T2", 15) not in cls

        # 3 rounds in the log
        assert len(result["rounds_log"]) == 3

        assert result["is_complete"] is True
        conn.close()


class TestForfeitTiebreak:
    """Tests for _forfeit_tiebreak."""

    def test_forfeit_tiebreak_lower_seed_wins(self):
        """Lower seed number (= better seed) wins on tiebreak."""
        seeds = {"T1": 1, "T2": 2, "T5": 5, "T8": 8}

        # Seed 1 vs seed 2: seed 1 wins
        assert bracket_tracer._forfeit_tiebreak(seeds, "T1", "T2") == "T1"

        # Seed 5 vs seed 8: seed 5 wins
        assert bracket_tracer._forfeit_tiebreak(seeds, "T5", "T8") == "T5"
        assert bracket_tracer._forfeit_tiebreak(seeds, "T8", "T5") == "T5"

        # Same seed (edge case): first arg wins
        assert bracket_tracer._forfeit_tiebreak(seeds, "T1", "T1") == "T1"

        # Unknown teams (both seed 999): first arg wins
        assert bracket_tracer._forfeit_tiebreak(seeds, "TX", "TY") == "TX"


class TestWriteBackScope:
    """Tests verifying championship tracer preserves consolation-owned state except the cleanup reset."""

    def test_championship_tracer_only_resets_is_consolation_for_clean_retrace(self):
        """After trace_championship_bracket_sql with write_back=True,
        sacko, placement_rank, and consolation_round must be unchanged from
        their initial values, while is_consolation is reset so the
        consolation tracer can rebuild it cleanly."""
        year = 2024
        reg = _make_reg_season(year, 8, 14)

        playoff = []
        # 4-team playoff (seeds 1-4), weeks 15-16
        playoff += _make_matchup_pair(year, 15, "T1", "T4", 120, 80)
        playoff += _make_matchup_pair(year, 15, "T2", "T3", 110, 90)
        playoff += _make_matchup_pair(year, 16, "T1", "T2", 130, 100)
        # Consolation games (should not be touched)
        playoff += _make_matchup_pair(year, 15, "T5", "T8", 95, 85)
        playoff += _make_matchup_pair(year, 15, "T6", "T7", 92, 88)
        playoff += _make_matchup_pair(year, 16, "T5", "T6", 100, 90)
        playoff += _make_matchup_pair(year, 16, "T7", "T8", 80, 75)

        conn = _build_bracket_db(reg, playoff)

        # Set initial consolation-owned values that should survive
        conn.execute(
            "UPDATE matchup SET sacko = 99, placement_rank = 88, "
            "consolation_round = 'initial_value', is_consolation = 1 "
            "WHERE year = 2024 AND week >= 15 AND franchise_id IN ('T5', 'T6', 'T7', 'T8')"
        )

        settings = {
            "playoff_teams": 4,
            "bye_teams": 0,
            "playoff_start_week": 15,
            "end_week": 16,
            "num_teams": 8,
            "uses_median": False,
        }

        bracket_tracer.trace_championship_bracket_sql(
            conn,
            year,
            settings,
            table="matchup",
            id_col="franchise_id",
            write_back=True,
        )

        # Verify consolation-owned columns were preserved except for the
        # intentional is_consolation cleanup reset.
        rows = conn.execute(
            "SELECT franchise_id, week, sacko, placement_rank, "
            "consolation_round, CAST(is_consolation AS INTEGER) "
            "FROM matchup "
            "WHERE year = 2024 AND week >= 15 "
            "AND franchise_id IN ('T5', 'T6', 'T7', 'T8') "
            "ORDER BY franchise_id, week"
        ).fetchall()

        for fid, wk, sacko, prank, cround, is_cons in rows:
            assert sacko == 99, f"sacko was modified for {fid} week {wk}: got {sacko}, expected 99"
            assert prank == 88, f"placement_rank was modified for {fid} week {wk}: got {prank}, expected 88"
            assert (
                cround == "initial_value"
            ), f"consolation_round was modified for {fid} week {wk}: got '{cround}', expected 'initial_value'"
            assert is_cons == 0, f"is_consolation should be reset for clean retrace on {fid} week {wk}: got {is_cons}"

        # Verify championship columns WERE written for playoff teams
        champ_rows = conn.execute(
            "SELECT franchise_id, CAST(is_playoffs AS INTEGER) "
            "FROM matchup "
            "WHERE year = 2024 AND week = 16 "
            "AND franchise_id IN ('T1', 'T2') "
        ).fetchall()
        for fid, is_pl in champ_rows:
            assert is_pl == 1, f"is_playoffs should be 1 for {fid} in championship week"

        conn.close()


class TestByeRowsAreNotLabeled:
    """Bye rows must NOT receive is_playoffs or playoff_round flags.

    Per project policy (memory: feedback_phantom_rows_no_flags), any matchup
    row where the team didn't play — whether a pre-game bye for an upcoming
    playoff game, a post-elimination week, or a post-championship phantom
    week — must never carry is_playoffs=1, is_consolation=1, or
    playoff_round / consolation_round labels. Those flags only belong on
    rows representing weeks the team actually played.
    """

    def test_pregame_byes_in_championship_bracket_remain_unlabeled(self):
        """6-team bracket, weeks 15-17, top 2 seeds bye in week 15 (QF).

        Round 1 (week 15, quarterfinal): T1 bye, T2 bye, T3 v T6, T4 v T5.
        Round 2 (week 16, semifinal): T1 v T4, T2 v T3.
        Round 3 (week 17, championship): T1 v T2.

        After tracer writeback, the bye rows for T1/T2 in week 15 must have
        playoff_round IS NULL and is_playoffs=0 — they didn't play that week.
        """
        year = 2024
        reg = _make_reg_season(year, 12, 14)

        playoff = []
        # Bye rows for top 2 seeds in week 15
        playoff.append(_make_bye_row(year, 15, "T1", seed=1))
        playoff.append(_make_bye_row(year, 15, "T2", seed=2))
        # Round 1 (QF): T3 v T6, T4 v T5
        playoff += _make_matchup_pair(year, 15, "T3", "T6", 120, 80)
        playoff += _make_matchup_pair(year, 15, "T4", "T5", 110, 90)
        # Round 2 (SF): T1 v T4, T2 v T3
        playoff += _make_matchup_pair(year, 16, "T1", "T4", 130, 100)
        playoff += _make_matchup_pair(year, 16, "T2", "T3", 125, 95)
        # Round 3 (Championship): T1 v T2
        playoff += _make_matchup_pair(year, 17, "T1", "T2", 140, 110)

        conn = _build_bracket_db(reg, playoff)

        settings = {
            "playoff_teams": 6,
            "bye_teams": 2,
            "playoff_start_week": 15,
            "end_week": 17,
            "num_teams": 12,
            "uses_median": False,
        }

        bracket_tracer.trace_championship_bracket_sql(
            conn,
            year,
            settings,
            table="matchup",
            id_col="franchise_id",
            write_back=True,
        )

        # The bye rows for T1 and T2 in week 15 must NOT be labeled.
        bye_labels = conn.execute(
            "SELECT franchise_id, playoff_round, COALESCE(CAST(is_playoffs AS INTEGER), 0) "
            "FROM matchup "
            "WHERE year = 2024 AND week = 15 AND franchise_id IN ('T1', 'T2') "
            "ORDER BY franchise_id"
        ).fetchall()

        assert len(bye_labels) == 2, f"Expected 2 bye rows, got {bye_labels}"
        for fid, prnd, is_pl in bye_labels:
            assert (
                prnd is None
            ), f"{fid} week 15 bye row should have NULL playoff_round per phantom-row policy; got {prnd!r}"
            assert is_pl == 0, f"{fid} week 15 bye row should have is_playoffs=0 per phantom-row policy; got {is_pl}"

        # Played-game rows (T3-T6 in W15) should still be labeled correctly.
        played_labels = conn.execute(
            "SELECT franchise_id, playoff_round "
            "FROM matchup "
            "WHERE year = 2024 AND week = 15 AND franchise_id IN ('T3', 'T4', 'T5', 'T6') "
            "ORDER BY franchise_id"
        ).fetchall()
        for fid, prnd in played_labels:
            assert prnd == "quarterfinal", f"{fid} week 15 played row should be labeled 'quarterfinal'; got {prnd!r}"

        conn.close()


# ---------------------------------------------------------------------------
# Phase 3: Consolation tracer SQL tests
# ---------------------------------------------------------------------------


from multi_league.transformations.matchup.modules.playoff_bracket.consolation_tracer_sql import (
    trace_consolation_sql,
)


def _build_consolation_db(
    regular_season_rows: list[dict],
    playoff_rows: list[dict],
) -> duckdb.DuckDBPyConnection:
    """Build an in-memory DuckDB with matchup table for consolation tests.

    Includes all columns needed by the consolation tracer.
    """
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER DEFAULT 0,
            is_consolation INTEGER DEFAULT 0,
            is_championship BOOLEAN DEFAULT FALSE,
            champion INTEGER DEFAULT 0,
            playoff_round VARCHAR DEFAULT '',
            final_playoff_seed INTEGER,
            sacko INTEGER DEFAULT 0,
            placement_rank INTEGER,
            placement_game INTEGER DEFAULT 0,
            consolation_round VARCHAR,
            postseason INTEGER DEFAULT 0
        )
    """)

    all_rows = regular_season_rows + playoff_rows
    for r in all_rows:
        conn.execute(
            "INSERT INTO matchup (year, week, franchise_id, opponent, "
            "opponent_franchise_id, team_points, opponent_points, win, loss, tie, "
            "is_playoffs, is_consolation, is_championship, champion, "
            "playoff_round, final_playoff_seed) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r.get("year"),
                r.get("week"),
                r.get("franchise_id"),
                r.get("opponent"),
                r.get("opponent_franchise_id"),
                r.get("team_points"),
                r.get("opponent_points"),
                r.get("win"),
                r.get("loss"),
                r.get("tie"),
                r.get("is_playoffs", 0),
                r.get("is_consolation", 0),
                r.get("is_championship", False),
                r.get("champion", 0),
                r.get("playoff_round", ""),
                r.get("final_playoff_seed"),
            ],
        )

    return conn


class TestConsolationTracerSQL:
    """Tests for the SQL-based consolation tracer."""

    def _build_10_team_scenario(self):
        """Build a 10-team league with 6 playoff teams and complete results.

        Returns (conn, settings) tuple.
        """
        year = 2024
        reg = _make_reg_season(year, 10, 14)

        playoff = []
        # Championship bracket: 6 teams, 2 byes (seeds 1,2)
        # Round 1 (QF, week 15): T3 vs T6 (T3 wins), T4 vs T5 (T4 wins)
        playoff.append(_make_bye_row(year, 15, "T1", seed=1))
        playoff.append(_make_bye_row(year, 15, "T2", seed=2))
        playoff += _make_matchup_pair(year, 15, "T3", "T6", 120, 80)
        playoff += _make_matchup_pair(year, 15, "T4", "T5", 110, 90)
        # Round 2 (SF, week 16): T1 vs T4 (T1 wins), T2 vs T3 (T2 wins)
        playoff += _make_matchup_pair(year, 16, "T1", "T4", 130, 100)
        playoff += _make_matchup_pair(year, 16, "T2", "T3", 125, 95)
        # Round 3 (Championship, week 17): T1 vs T2 (T1 wins)
        playoff += _make_matchup_pair(year, 17, "T1", "T2", 140, 110)

        # Set championship bracket flags on playoff rows
        for r in playoff:
            if r.get("team_points") is not None and r.get("opponent_points") is not None:
                fid = r.get("franchise_id")
                if fid in ("T1", "T2", "T3", "T4", "T5", "T6"):
                    r["is_playoffs"] = 1

        # Mark championship
        for r in playoff:
            if r.get("week") == 17 and r.get("franchise_id") in ("T1", "T2"):
                r["is_championship"] = True
                if r.get("franchise_id") == "T1":
                    r["champion"] = 1

        # Consolation games (not is_playoffs)
        # Week 16: 5th place game (SF losers from QF: T5 vs T6)
        consolation = []
        consolation += _make_matchup_pair(year, 16, "T5", "T6", 95, 85)
        # Week 17: 3rd place game (SF losers: T3 vs T4)
        consolation += _make_matchup_pair(year, 17, "T3", "T4", 105, 100)

        # Non-playoff bracket (seeds 7-10)
        consolation += _make_matchup_pair(year, 15, "T7", "T10", 80, 70)
        consolation += _make_matchup_pair(year, 15, "T8", "T9", 75, 65)
        consolation += _make_matchup_pair(year, 16, "T7", "T8", 85, 78)
        consolation += _make_matchup_pair(year, 16, "T9", "T10", 68, 72)
        consolation += _make_matchup_pair(year, 17, "T7", "T9", 82, 74)
        consolation += _make_matchup_pair(year, 17, "T8", "T10", 77, 88)

        # Set seeds on all rows
        for r in reg + playoff + consolation:
            fid = r.get("franchise_id")
            if fid and fid.startswith("T"):
                try:
                    seed = int(fid[1:])
                    r["final_playoff_seed"] = seed
                except ValueError:
                    pass

        conn = _build_consolation_db(reg, playoff + consolation)

        settings = {
            "playoff_teams": 6,
            "bye_teams": 2,
            "playoff_start_week": 15,
            "end_week": 17,
            "num_teams": 10,
            "uses_median": False,
            "playoff_round_type": 0,
        }

        return conn, settings

    def test_consolation_tracer_skips_when_no_champion(self):
        """No champion=1 -> placement_rank and sacko stay NULL/0."""
        year = 2024
        reg = _make_reg_season(year, 6, 14)

        # 4-team playoff but no champion set
        playoff = []
        playoff += _make_matchup_pair(year, 15, "T1", "T4", 120, 80)
        playoff += _make_matchup_pair(year, 15, "T2", "T3", 110, 90)
        # Set is_playoffs but NOT champion
        for r in playoff:
            r["is_playoffs"] = 1

        # Consolation
        consolation = _make_matchup_pair(year, 15, "T5", "T6", 70, 60)

        for r in reg + playoff + consolation:
            fid = r.get("franchise_id")
            if fid and fid.startswith("T"):
                try:
                    r["final_playoff_seed"] = int(fid[1:])
                except ValueError:
                    pass

        conn = _build_consolation_db(reg, playoff + consolation)

        settings = {
            "playoff_teams": 4,
            "bye_teams": 0,
            "playoff_start_week": 15,
            "end_week": 16,
            "num_teams": 6,
            "uses_median": False,
            "playoff_round_type": 0,
        }

        result = trace_consolation_sql(conn, year, settings, table="matchup", id_col="franchise_id")

        # No champion -> placement_rank should be empty, sacko should be None
        assert result["placements"] == {}
        assert result["sacko"] is None
        assert result["season_complete"] is False

        # Verify placement_rank is NULL in the DB
        rows = conn.execute(
            "SELECT placement_rank FROM matchup WHERE year = 2024 AND placement_rank IS NOT NULL"
        ).fetchall()
        assert len(rows) == 0

        # Verify sacko is 0
        rows = conn.execute("SELECT COUNT(*) FROM matchup WHERE year = 2024 AND sacko = 1").fetchall()
        assert rows[0][0] == 0

        # But is_consolation should still be set
        rows = conn.execute("SELECT COUNT(*) FROM matchup WHERE year = 2024 AND is_consolation = 1").fetchall()
        assert rows[0][0] > 0

        conn.close()

    def test_placement_ranks_full_coverage_10_teams(self):
        """10 teams, 6 playoff: ranks 1-10 all assigned."""
        conn, settings = self._build_10_team_scenario()

        result = trace_consolation_sql(conn, 2024, settings, table="matchup", id_col="franchise_id")

        placements = result["placements"]
        assert len(placements) == 10, f"Expected 10 placements, got {len(placements)}"

        # All ranks 1-10 should be assigned
        assigned_ranks = sorted(placements.values())
        assert assigned_ranks == list(range(1, 11)), f"Expected ranks 1-10, got {assigned_ranks}"

        # Champion should be rank 1
        assert placements.get("T1") == 1, f"Champion T1 should be rank 1, got {placements.get('T1')}"

        # Runner-up should be rank 2
        assert placements.get("T2") == 2, f"Runner-up T2 should be rank 2, got {placements.get('T2')}"

        conn.close()

    def test_sacko_is_bottom_bracket_loser_path(self):
        """Sacko follows the bottom-bracket loser path and owns the last rank."""
        conn, settings = self._build_10_team_scenario()

        result = trace_consolation_sql(conn, 2024, settings, table="matchup", id_col="franchise_id")

        sacko_id = result["sacko"]
        assert sacko_id is not None, "Sacko should be identified"
        assert sacko_id == "T9"
        assert result["placements"][sacko_id] == 10

        # Verify sacko=1 in the DB
        rows = conn.execute(
            "SELECT franchise_id, week, placement_rank FROM matchup WHERE year = 2024 AND sacko = 1"
        ).fetchall()
        assert len(rows) > 0, "Sacko should have at least one row with sacko=1"
        assert all(r[0] == sacko_id for r in rows), "All sacko rows should be for the sacko franchise"
        assert all(r[2] == 10 for r in rows), "Sacko rows should carry the last placement rank"

        conn.close()

    def test_sacko_fallback_does_not_mark_all_rows_without_postseason_row(self):
        from multi_league.transformations.matchup.modules.playoff_bracket.consolation_tracer_sql import _write_sacko

        conn = duckdb.connect()
        try:
            conn.execute(
                """
                CREATE TABLE matchup (
                    year INTEGER,
                    week INTEGER,
                    franchise_id VARCHAR,
                    is_playoffs INTEGER,
                    is_consolation INTEGER,
                    sacko INTEGER
                )
                """
            )
            conn.executemany(
                "INSERT INTO matchup VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (2024, 1, "A", 0, 0, 0),
                    (2024, 2, "A", 0, 0, 0),
                    (2024, 1, "B", 0, 0, 0),
                    (2024, 2, "B", 0, 0, 0),
                ],
            )

            result = _write_sacko(conn, "matchup", 2024, "franchise_id", {"A": 1, "B": 2}, 2)
            rows = conn.execute(
                "SELECT week, franchise_id FROM matchup WHERE COALESCE(CAST(sacko AS INTEGER), 0) = 1"
            ).fetchall()
        finally:
            conn.close()

        assert result is None
        assert rows == []

    def test_postseason_derived_correctly(self):
        """postseason = is_playoffs | is_consolation."""
        conn, settings = self._build_10_team_scenario()

        trace_consolation_sql(conn, 2024, settings, table="matchup", id_col="franchise_id")

        # Check postseason is correct
        rows = conn.execute(
            "SELECT week, franchise_id, "
            "COALESCE(CAST(is_playoffs AS INTEGER), 0) as ip, "
            "COALESCE(CAST(is_consolation AS INTEGER), 0) as ic, "
            "COALESCE(postseason, 0) as ps "
            "FROM matchup "
            "WHERE year = 2024 AND week >= 15"
        ).fetchall()

        for wk, fid, ip, ic, ps in rows:
            expected_ps = 1 if (ip == 1 or ic == 1) else 0
            assert ps == expected_ps, (
                f"Postseason wrong for {fid} week {wk}: "
                f"is_playoffs={ip}, is_consolation={ic}, "
                f"postseason={ps}, expected={expected_ps}"
            )

        # Regular season rows should have postseason=0
        rows = conn.execute(
            "SELECT COUNT(*) FROM matchup WHERE year = 2024 AND week < 15 AND postseason = 1"
        ).fetchall()
        assert rows[0][0] == 0, "Regular season rows should have postseason=0"

        conn.close()

    def test_zero_point_placement_games_with_recorded_outcomes_stay_consolation(self):
        """Zero-point postseason placement games still count when result columns prove they happened."""
        year = 2025
        reg = _make_reg_season(year, 8, 14)

        playoff = []
        playoff += _make_matchup_pair(year, 15, "T1", "T4", 130, 90)
        playoff += _make_matchup_pair(year, 15, "T2", "T3", 120, 100)
        playoff += _make_matchup_pair(year, 16, "T1", "T2", 140, 110)
        for row in playoff:
            row["is_playoffs"] = 1
        for row in playoff[-2:]:
            if row["franchise_id"] == "T1":
                row["champion"] = 1
                row["is_championship"] = True

        placement = [
            {
                "year": year,
                "week": 15,
                "franchise_id": "T5",
                "opponent": "T6",
                "opponent_franchise_id": "T6",
                "team_points": 0.0,
                "opponent_points": 0.0,
                "win": 1,
                "loss": 0,
                "tie": 0,
                "final_playoff_seed": 5,
            },
            {
                "year": year,
                "week": 15,
                "franchise_id": "T6",
                "opponent": "T5",
                "opponent_franchise_id": "T5",
                "team_points": 0.0,
                "opponent_points": 0.0,
                "win": 0,
                "loss": 1,
                "tie": 0,
                "final_playoff_seed": 6,
            },
        ]

        for row in reg + playoff + placement:
            fid = row.get("franchise_id")
            if fid and fid.startswith("T") and row.get("final_playoff_seed") is None:
                row["final_playoff_seed"] = int(fid[1:])

        conn = _build_consolation_db(reg, playoff + placement)
        settings = {
            "playoff_teams": 4,
            "bye_teams": 0,
            "playoff_start_week": 15,
            "end_week": 16,
            "num_teams": 8,
            "uses_median": False,
            "playoff_round_type": 0,
        }

        trace_consolation_sql(conn, year, settings, table="matchup", id_col="franchise_id")

        rows = conn.execute(
            "SELECT franchise_id, COALESCE(CAST(is_consolation AS INTEGER), 0), consolation_round, postseason "
            "FROM matchup WHERE year = 2025 AND week = 15 AND franchise_id IN ('T5', 'T6') "
            "ORDER BY franchise_id"
        ).fetchall()

        assert [row[0] for row in rows] == ["T5", "T6"]
        assert all(row[1] == 1 for row in rows)
        assert all(row[3] == 1 for row in rows)
        assert all(isinstance(row[2], str) and "consolation" in row[2].lower() for row in rows)

        conn.close()

    def test_consolation_rounds_past_championship_are_labeled(self):
        """Consolation games played AFTER the championship week must still
        receive a consolation_round label.

        Mirrors dingleberry_derby (handoff_2026_04_27_consolation_round_post_champ):
        4-team championship at W14-W15 with an 8-team consolation bracket
        continuing through W16. The W16 consolation rows are flagged
        is_consolation=1 by `_sweep_consolation`, but `_label_consolation_rounds`
        only iterates championship rounds [(14,14),(15,15)] — so W16 never
        gets a label and the validator fails playoffs_consolation_round_populated.
        """
        year = 2024
        reg = _make_reg_season(year, 12, 13)

        # Championship bracket: T1-T4 (4 teams, 2 rounds)
        # W14 (semifinal): T1 v T4, T2 v T3
        # W15 (championship): T1 v T2 (T1 wins)
        playoff = []
        playoff += _make_matchup_pair(year, 14, "T1", "T4", 130, 100)
        playoff += _make_matchup_pair(year, 14, "T2", "T3", 125, 95)
        playoff += _make_matchup_pair(year, 15, "T1", "T2", 140, 110)
        for r in playoff:
            r["is_playoffs"] = 1
        for r in playoff:
            if r.get("week") == 15 and r.get("franchise_id") in ("T1", "T2"):
                r["is_championship"] = True
                if r.get("franchise_id") == "T1":
                    r["champion"] = 1

        # Consolation bracket: T5-T12 (8 teams, 3 rounds — extends to W16)
        consolation = []
        # W14 (consolation R1): T5/T12, T6/T11, T7/T10, T8/T9
        consolation += _make_matchup_pair(year, 14, "T5", "T12", 95, 75)
        consolation += _make_matchup_pair(year, 14, "T6", "T11", 90, 80)
        consolation += _make_matchup_pair(year, 14, "T7", "T10", 88, 78)
        consolation += _make_matchup_pair(year, 14, "T8", "T9", 85, 82)
        # W15 (consolation R2): winners' SF + losers' SF
        consolation += _make_matchup_pair(year, 15, "T5", "T6", 92, 86)
        consolation += _make_matchup_pair(year, 15, "T7", "T8", 87, 83)
        consolation += _make_matchup_pair(year, 15, "T11", "T12", 79, 73)
        consolation += _make_matchup_pair(year, 15, "T9", "T10", 81, 76)
        # W16 (consolation R3): placement finals
        consolation += _make_matchup_pair(year, 16, "T5", "T7", 91, 84)
        consolation += _make_matchup_pair(year, 16, "T6", "T8", 88, 82)
        consolation += _make_matchup_pair(year, 16, "T9", "T11", 80, 75)
        consolation += _make_matchup_pair(year, 16, "T10", "T12", 78, 72)

        for r in reg + playoff + consolation:
            fid = r.get("franchise_id")
            if fid and fid.startswith("T"):
                try:
                    r["final_playoff_seed"] = int(fid[1:])
                except ValueError:
                    pass

        conn = _build_consolation_db(reg, playoff + consolation)
        settings = {
            "playoff_teams": 4,
            "bye_teams": 0,
            "playoff_start_week": 14,
            "end_week": 16,
            "num_teams": 12,
            "uses_median": False,
            "playoff_round_type": 0,
        }

        trace_consolation_sql(conn, year, settings, table="matchup", id_col="franchise_id")

        # Every is_consolation=1 row must have a non-NULL/empty consolation_round.
        unlabeled = conn.execute(
            "SELECT week, franchise_id, opponent FROM matchup "
            "WHERE year = 2024 "
            "AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 1 "
            "AND (consolation_round IS NULL OR consolation_round = '') "
            "ORDER BY week, franchise_id"
        ).fetchall()
        assert (
            unlabeled == []
        ), f"All is_consolation=1 rows must have consolation_round labeled; unlabeled rows: {unlabeled}"

        # Specifically W16 (post-championship) consolation rows must be labeled.
        w16_rows = conn.execute(
            "SELECT franchise_id, consolation_round FROM matchup "
            "WHERE year = 2024 AND week = 16 "
            "AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 1 "
            "ORDER BY franchise_id"
        ).fetchall()
        assert len(w16_rows) == 8, f"Expected 8 W16 consolation rows, got {len(w16_rows)}"
        for fid, label in w16_rows:
            assert label, f"W16 consolation row for {fid} should have a label, got {label!r}"

        conn.close()
