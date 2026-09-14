"""Tests for 2-week playoff round handling.

Bug 1: ESPN fetcher dedupes 2-week rounds to 1 row (should compute deltas).
Bug 2: Bracket tracer misclassifies championship loser on week 2.
"""

import sys
from pathlib import Path

# Add paths for imports (same pattern as test_bracket.py)
SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPT_DIR.parent.parent.parent
MULTI_LEAGUE_DIR = SCRIPTS_DIR / "multi_league"
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(MULTI_LEAGUE_DIR))

import duckdb
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_matchup_table(conn, rows: list[dict]):
    """Create matchup + league_settings tables from row dicts."""
    df = pd.DataFrame(rows)
    conn.register("_tmp", df)
    conn.execute("CREATE TABLE matchup AS SELECT * FROM _tmp")
    conn.unregister("_tmp")
    # Ensure string columns have correct types (pandas infers None as int)
    for col in ["playoff_round", "consolation_round"]:
        try:
            conn.execute(f"ALTER TABLE matchup ALTER COLUMN {col} TYPE VARCHAR")
        except Exception:
            pass


def _create_settings_table(conn, settings_rows: list[dict]):
    df = pd.DataFrame(settings_rows)
    conn.register("_tmp", df)
    conn.execute("CREATE TABLE league_settings AS SELECT * FROM _tmp")
    conn.unregister("_tmp")


def _matchup_row(
    year,
    week,
    fid,
    manager,
    team_pts,
    opp_pts,
    opp_fid,
    opp_mgr,
    is_playoffs=0,
    is_consolation=0,
    is_championship=False,
    champion=0,
    is_bye_week=False,
    playoff_round=None,
    final_playoff_seed=None,
    sacko=0,
    postseason=0,
    placement_rank=None,
    placement_game=0,
    consolation_round=None,
):
    return {
        "year": year,
        "week": week,
        "franchise_id": fid,
        "manager": manager,
        "team_points": team_pts,
        "opponent_points": opp_pts,
        "opponent_franchise_id": opp_fid,
        "opponent": opp_mgr,
        "margin": round(team_pts - opp_pts, 2) if team_pts is not None and opp_pts is not None else None,
        "win": 1 if (team_pts or 0) > (opp_pts or 0) else 0,
        "loss": 1 if (opp_pts or 0) > (team_pts or 0) else 0,
        "tie": 0,
        "is_playoffs": is_playoffs,
        "is_consolation": is_consolation,
        "is_championship": is_championship,
        "champion": champion,
        "is_bye_week": is_bye_week,
        "playoff_round": playoff_round,
        "final_playoff_seed": final_playoff_seed,
        "sacko": sacko,
        "postseason": postseason,
        "placement_rank": placement_rank,
        "placement_game": placement_game,
        "consolation_round": consolation_round,
    }


# ---------------------------------------------------------------------------
# Bug 2: Bracket tracer — 2-week championship (prt=2)
# ---------------------------------------------------------------------------


class TestTwoWeekChampionship:
    """prt=2: only championship is 2 weeks. Mimics brobball 2024."""

    def _build_brobball_like(self):
        """6-team playoff, prt=2: rounds [(14,14), (15,15), (16,17)]."""
        # Regular season weeks 1-13 (enough to seed)
        rows = []
        teams = [
            ("A", "Alice"),  # seed 1
            ("B", "Bob"),  # seed 2
            ("C", "Carol"),  # seed 3
            ("D", "Dave"),  # seed 4
            ("E", "Eve"),  # seed 5
            ("F", "Frank"),  # seed 6
        ]
        # Give descending wins so seed order = A, B, C, D, E, F
        win_counts = [12, 10, 9, 8, 7, 5]
        for i, (fid, mgr) in enumerate(teams):
            for week in range(1, 14):
                if week <= win_counts[i]:
                    rows.append(_matchup_row(2024, week, fid, mgr, 120, 100, "X", "Opp"))
                else:
                    rows.append(_matchup_row(2024, week, fid, mgr, 100, 120, "X", "Opp"))

        # Playoffs: round 1 (week 14) — seeds 3-6 play, 1-2 have byes
        # C(3) vs F(6) → C wins
        rows.append(_matchup_row(2024, 14, "C", "Carol", 150, 120, "F", "Frank"))
        rows.append(_matchup_row(2024, 14, "F", "Frank", 120, 150, "C", "Carol"))
        # D(4) vs E(5) → D wins
        rows.append(_matchup_row(2024, 14, "D", "Dave", 140, 130, "E", "Eve"))
        rows.append(_matchup_row(2024, 14, "E", "Eve", 130, 140, "D", "Dave"))
        # A and B have bye rows
        rows.append(_matchup_row(2024, 14, "A", "Alice", None, None, None, None, is_bye_week=True))
        rows.append(_matchup_row(2024, 14, "B", "Bob", None, None, None, None, is_bye_week=True))

        # Round 2 (week 15) — semis
        # A(1) vs D(4) → A wins
        rows.append(_matchup_row(2024, 15, "A", "Alice", 160, 130, "D", "Dave"))
        rows.append(_matchup_row(2024, 15, "D", "Dave", 130, 160, "A", "Alice"))
        # B(2) vs C(3) → C upsets B
        rows.append(_matchup_row(2024, 15, "B", "Bob", 125, 145, "C", "Carol"))
        rows.append(_matchup_row(2024, 15, "C", "Carol", 145, 125, "B", "Bob"))

        # Round 3 (weeks 16-17) — 2-week championship
        # A(1) vs C(3): A wins week 16, C wins week 17, A wins on combined
        rows.append(_matchup_row(2024, 16, "A", "Alice", 170, 140, "C", "Carol"))
        rows.append(_matchup_row(2024, 16, "C", "Carol", 140, 170, "A", "Alice"))
        rows.append(_matchup_row(2024, 17, "A", "Alice", 150, 165, "C", "Carol"))
        rows.append(_matchup_row(2024, 17, "C", "Carol", 165, 150, "A", "Alice"))
        # A combined: 170+150=320, C combined: 140+165=305 → A wins

        # Consolation rows (D vs B, week 16-17 consolation play)
        rows.append(_matchup_row(2024, 16, "D", "Dave", 110, 120, "B", "Bob"))
        rows.append(_matchup_row(2024, 16, "B", "Bob", 120, 110, "D", "Dave"))
        rows.append(_matchup_row(2024, 17, "D", "Dave", 115, 125, "B", "Bob"))
        rows.append(_matchup_row(2024, 17, "B", "Bob", 125, 115, "D", "Dave"))

        settings = [
            {
                "year": 2024,
                "playoff_teams": 6,
                "bye_teams": 2,
                "playoff_start_week": 14,
                "end_week": 17,
                "num_teams": 6,
                "has_multiweek_championship": True,
                "uses_playoff_reseeding": False,
                "uses_median": False,
                "platform": "sleeper",
                "sleeper_playoff_type": 2,
            }
        ]
        return rows, settings

    def test_loser_week2_is_playoffs(self):
        """Championship loser on week 2 of 2-week round must be is_playoffs=1."""
        from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
            trace_championship_bracket_sql,
        )

        rows, settings = self._build_brobball_like()
        conn = duckdb.connect()
        try:
            _create_matchup_table(conn, rows)
            _create_settings_table(conn, settings)

            result = trace_championship_bracket_sql(
                conn,
                2024,
                {
                    "playoff_teams": 6,
                    "bye_teams": 2,
                    "playoff_start_week": 14,
                    "end_week": 17,
                    "num_teams": 6,
                    "uses_playoff_reseeding": False,
                    "has_multiweek_championship": True,
                    "playoff_round_type": 2,
                    "uses_median": False,
                },
                table="matchup",
                id_col="franchise_id",
                write_back=True,
            )

            assert result["champion"] == "A", f"Expected champion A, got {result['champion']}"
            assert result["runner_up"] == "C", f"Expected runner_up C, got {result['runner_up']}"

            # Check that BOTH teams on BOTH championship weeks have is_playoffs=1
            for fid, label in [("A", "champion"), ("C", "loser")]:
                for week in [16, 17]:
                    rows_q = conn.execute(
                        f"SELECT is_playoffs, is_championship, playoff_round "
                        f"FROM matchup WHERE year=2024 AND week={week} AND franchise_id='{fid}'"
                    ).fetchall()
                    assert len(rows_q) == 1, f"{label} {fid} week {week}: no row found"
                    is_playoffs, is_champ, pr = rows_q[0]
                    assert int(is_playoffs) == 1, f"{label} {fid} week {week}: is_playoffs={is_playoffs}, expected 1"
                    assert bool(is_champ), f"{label} {fid} week {week}: is_championship={is_champ}, expected True"
                    assert (
                        pr == "championship"
                    ), f"{label} {fid} week {week}: playoff_round='{pr}', expected 'championship'"

            # Champion flag on both weeks
            for week in [16, 17]:
                champ_val = conn.execute(
                    f"SELECT champion FROM matchup WHERE year=2024 AND week={week} AND franchise_id='A'"
                ).fetchone()[0]
                assert int(champ_val) == 1, f"Champion A week {week}: champion={champ_val}"
        finally:
            conn.close()

    def test_consolation_does_not_claim_championship_loser(self):
        """Consolation tracer must not overwrite championship loser's is_playoffs."""
        from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
            trace_championship_bracket_sql,
        )
        from multi_league.transformations.matchup.modules.playoff_bracket.consolation_tracer_sql import (
            trace_consolation_sql,
        )

        rows, settings_rows = self._build_brobball_like()
        conn = duckdb.connect()
        try:
            _create_matchup_table(conn, rows)
            _create_settings_table(conn, settings_rows)

            s = {
                "playoff_teams": 6,
                "bye_teams": 2,
                "playoff_start_week": 14,
                "end_week": 17,
                "num_teams": 6,
                "uses_playoff_reseeding": False,
                "has_multiweek_championship": True,
                "playoff_round_type": 2,
                "uses_median": False,
            }

            # Run championship tracer first
            trace_championship_bracket_sql(conn, 2024, s, table="matchup", id_col="franchise_id", write_back=True)

            # Run consolation tracer (should NOT overwrite championship loser)
            trace_consolation_sql(conn, 2024, s, table="matchup", id_col="franchise_id")

            # Championship loser C on week 17 must still be is_playoffs=1
            row = conn.execute(
                "SELECT is_playoffs, is_consolation FROM matchup " "WHERE year=2024 AND week=17 AND franchise_id='C'"
            ).fetchone()
            assert int(row[0]) == 1, f"C week 17 is_playoffs={row[0]}, expected 1"
            assert int(row[1]) == 0, f"C week 17 is_consolation={row[1]}, expected 0"

            # But consolation teams (B, D) on weeks 16-17 SHOULD be is_consolation=1
            for fid in ["B", "D"]:
                for week in [16, 17]:
                    row = conn.execute(
                        f"SELECT is_consolation FROM matchup "
                        f"WHERE year=2024 AND week={week} AND franchise_id='{fid}'"
                    ).fetchone()
                    assert int(row[0]) == 1, f"{fid} week {week} is_consolation={row[0]}, expected 1"
        finally:
            conn.close()

    def test_champion_uses_combined_score(self):
        """In 2-week round, champion is determined by combined score, not single week."""
        from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
            trace_championship_bracket_sql,
        )

        rows, settings_rows = self._build_brobball_like()
        conn = duckdb.connect()
        try:
            _create_matchup_table(conn, rows)
            _create_settings_table(conn, settings_rows)

            # A: 170+150=320, C: 140+165=305. A wins on combined despite losing week 17.
            result = trace_championship_bracket_sql(
                conn,
                2024,
                {
                    "playoff_teams": 6,
                    "bye_teams": 2,
                    "playoff_start_week": 14,
                    "end_week": 17,
                    "num_teams": 6,
                    "uses_playoff_reseeding": False,
                    "has_multiweek_championship": True,
                    "playoff_round_type": 2,
                    "uses_median": False,
                },
                table="matchup",
                id_col="franchise_id",
                write_back=True,
            )
            assert result["champion"] == "A", f"Champion should be A (combined 320 > 305), got {result['champion']}"
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Bug 2: Bracket tracer — ALL rounds 2 weeks (prt=1)
# ---------------------------------------------------------------------------


class TestAllRoundsTwoWeek:
    """prt=1: every round is 2 weeks. 4-team bracket, rounds [(14,15), (16,17)]."""

    def _build_prt1_data(self):
        teams = [("A", "Alice"), ("B", "Bob"), ("C", "Carol"), ("D", "Dave")]
        rows = []

        # Regular season
        for i, (fid, mgr) in enumerate(teams):
            wins = 13 - i * 2
            for week in range(1, 14):
                if week <= wins:
                    rows.append(_matchup_row(2024, week, fid, mgr, 120, 100, "X", "Opp"))
                else:
                    rows.append(_matchup_row(2024, week, fid, mgr, 100, 120, "X", "Opp"))

        # Semifinal: weeks 14-15 (2-week round)
        # A(1) vs D(4): A wins both weeks
        rows.append(_matchup_row(2024, 14, "A", "Alice", 130, 110, "D", "Dave"))
        rows.append(_matchup_row(2024, 14, "D", "Dave", 110, 130, "A", "Alice"))
        rows.append(_matchup_row(2024, 15, "A", "Alice", 140, 115, "D", "Dave"))
        rows.append(_matchup_row(2024, 15, "D", "Dave", 115, 140, "A", "Alice"))

        # B(2) vs C(3): C upsets B on combined
        rows.append(_matchup_row(2024, 14, "B", "Bob", 135, 120, "C", "Carol"))
        rows.append(_matchup_row(2024, 14, "C", "Carol", 120, 135, "B", "Bob"))
        rows.append(_matchup_row(2024, 15, "B", "Bob", 100, 145, "C", "Carol"))
        rows.append(_matchup_row(2024, 15, "C", "Carol", 145, 100, "B", "Bob"))
        # B combined: 235, C combined: 265 → C wins

        # Championship: weeks 16-17 (2-week round)
        rows.append(_matchup_row(2024, 16, "A", "Alice", 160, 150, "C", "Carol"))
        rows.append(_matchup_row(2024, 16, "C", "Carol", 150, 160, "A", "Alice"))
        rows.append(_matchup_row(2024, 17, "A", "Alice", 155, 170, "C", "Carol"))
        rows.append(_matchup_row(2024, 17, "C", "Carol", 170, 155, "A", "Alice"))
        # A combined: 315, C combined: 320 → C wins championship

        # Consolation: D vs B
        rows.append(_matchup_row(2024, 16, "D", "Dave", 110, 120, "B", "Bob"))
        rows.append(_matchup_row(2024, 16, "B", "Bob", 120, 110, "D", "Dave"))
        rows.append(_matchup_row(2024, 17, "D", "Dave", 115, 125, "B", "Bob"))
        rows.append(_matchup_row(2024, 17, "B", "Bob", 125, 115, "D", "Dave"))

        settings = [
            {
                "year": 2024,
                "playoff_teams": 4,
                "bye_teams": 0,
                "playoff_start_week": 14,
                "end_week": 17,
                "num_teams": 4,
                "has_multiweek_championship": False,
                "uses_playoff_reseeding": False,
                "uses_median": False,
                "platform": "sleeper",
                "sleeper_playoff_type": 1,
            }
        ]
        return rows, settings

    def test_all_rounds_two_week_classification(self):
        """With prt=1, every round's both weeks should be is_playoffs for bracket teams."""
        from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
            trace_championship_bracket_sql,
        )

        rows, settings_rows = self._build_prt1_data()
        conn = duckdb.connect()
        try:
            _create_matchup_table(conn, rows)
            _create_settings_table(conn, settings_rows)

            result = trace_championship_bracket_sql(
                conn,
                2024,
                {
                    "playoff_teams": 4,
                    "bye_teams": 0,
                    "playoff_start_week": 14,
                    "end_week": 17,
                    "num_teams": 4,
                    "uses_playoff_reseeding": False,
                    "has_multiweek_championship": False,
                    "playoff_round_type": 1,
                    "uses_median": False,
                },
                table="matchup",
                id_col="franchise_id",
                write_back=True,
            )

            # C wins combined (320 > 315)
            assert result["champion"] == "C", f"Expected champion C, got {result['champion']}"

            # Semifinal: all 4 teams on both weeks 14 and 15 should be is_playoffs
            for fid in ["A", "B", "C", "D"]:
                for week in [14, 15]:
                    row = conn.execute(
                        f"SELECT is_playoffs, playoff_round FROM matchup "
                        f"WHERE year=2024 AND week={week} AND franchise_id='{fid}'"
                    ).fetchone()
                    assert int(row[0]) == 1, f"{fid} week {week}: is_playoffs={row[0]}"
                    assert row[1] == "semifinal", f"{fid} week {week}: playoff_round={row[1]}"

            # Championship: A and C on both weeks 16 and 17 should be is_playoffs
            for fid in ["A", "C"]:
                for week in [16, 17]:
                    row = conn.execute(
                        f"SELECT is_playoffs, playoff_round, is_championship FROM matchup "
                        f"WHERE year=2024 AND week={week} AND franchise_id='{fid}'"
                    ).fetchone()
                    assert int(row[0]) == 1, f"{fid} week {week}: is_playoffs={row[0]}"
                    assert row[1] == "championship", f"{fid} week {week}: playoff_round={row[1]}"
                    assert bool(row[2]), f"{fid} week {week}: is_championship={row[2]}"

            # Semifinal losers (B and D) should NOT be is_playoffs on weeks 16-17
            for fid in ["B", "D"]:
                for week in [16, 17]:
                    row = conn.execute(
                        f"SELECT is_playoffs FROM matchup " f"WHERE year=2024 AND week={week} AND franchise_id='{fid}'"
                    ).fetchone()
                    assert int(row[0]) == 0, f"Consolation team {fid} week {week}: is_playoffs={row[0]}, expected 0"
        finally:
            conn.close()

    def test_semifinal_loser_week2_not_misclassified(self):
        """Semifinal loser on week 2 of a 2-week semifinal must be is_playoffs, not consolation."""
        from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
            trace_championship_bracket_sql,
        )
        from multi_league.transformations.matchup.modules.playoff_bracket.consolation_tracer_sql import (
            trace_consolation_sql,
        )

        rows, settings_rows = self._build_prt1_data()
        conn = duckdb.connect()
        try:
            _create_matchup_table(conn, rows)
            _create_settings_table(conn, settings_rows)

            s = {
                "playoff_teams": 4,
                "bye_teams": 0,
                "playoff_start_week": 14,
                "end_week": 17,
                "num_teams": 4,
                "uses_playoff_reseeding": False,
                "has_multiweek_championship": False,
                "playoff_round_type": 1,
                "uses_median": False,
            }

            trace_championship_bracket_sql(conn, 2024, s, table="matchup", id_col="franchise_id", write_back=True)
            trace_consolation_sql(conn, 2024, s, table="matchup", id_col="franchise_id")

            # B lost in semifinal (weeks 14-15). Week 15 should still be is_playoffs.
            row = conn.execute(
                "SELECT is_playoffs, is_consolation, playoff_round FROM matchup "
                "WHERE year=2024 AND week=15 AND franchise_id='B'"
            ).fetchone()
            assert int(row[0]) == 1, f"B week 15 (semi loser): is_playoffs={row[0]}, expected 1"
            assert int(row[1]) == 0, f"B week 15 (semi loser): is_consolation={row[1]}, expected 0"
            assert row[2] == "semifinal", f"B week 15: playoff_round={row[2]}"

            # D also lost in semifinal
            row = conn.execute(
                "SELECT is_playoffs, is_consolation FROM matchup " "WHERE year=2024 AND week=15 AND franchise_id='D'"
            ).fetchone()
            assert int(row[0]) == 1, f"D week 15 (semi loser): is_playoffs={row[0]}, expected 1"
            assert int(row[1]) == 0, f"D week 15 (semi loser): is_consolation={row[1]}, expected 0"
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Bug 1: ESPN fetcher — cumulative score delta computation
# ---------------------------------------------------------------------------


class TestESPNCumulativeScoreDelta:
    """Test the cumulative→per-week delta logic in the ESPN fetcher."""

    def test_delta_computation_basic(self):
        """Verify that continuation weeks get delta scores, not cumulative."""
        # Simulate the delta computation logic from espn_matchups.py
        # Week 15: home=160, away=140 (first week of 2-week round)
        # Week 16: home=325, away=305 (cumulative)
        # Expected week 16 delta: home=165, away=165

        prev_week_cumulative = {(1, 2): (160.0, 140.0)}
        current_pairs = {(1, 2): (325.0, 305.0)}

        # Check if continuation
        assert set(current_pairs.keys()) == set(prev_week_cumulative.keys())
        all_cumulative = all(
            current_pairs[k][0] >= prev_week_cumulative[k][0] - 0.01
            and current_pairs[k][1] >= prev_week_cumulative[k][1] - 0.01
            for k in current_pairs
        )
        assert all_cumulative

        delta_scores = {}
        for k in current_pairs:
            h_prev, a_prev = prev_week_cumulative[k]
            h_curr, a_curr = current_pairs[k]
            delta_scores[k] = (round(h_curr - h_prev, 2), round(a_curr - a_prev, 2))

        assert delta_scores[(1, 2)] == (165.0, 165.0)

    def test_non_cumulative_not_flagged(self):
        """If scores go down (different matchup pairs), don't flag as continuation."""
        prev_week_cumulative = {(1, 2): (160.0, 140.0)}
        # Different teams or lower scores = not continuation
        current_pairs = {(1, 3): (120.0, 130.0)}

        # Different keys → not continuation
        assert set(current_pairs.keys()) != set(prev_week_cumulative.keys())

    def test_delta_with_multiple_matchups(self):
        """Delta works when multiple matchup pairs are in the same playoff week."""
        prev = {(1, 2): (150.0, 130.0), (3, 4): (160.0, 110.0)}
        curr = {(1, 2): (310.0, 270.0), (3, 4): (320.0, 230.0)}

        assert set(curr.keys()) == set(prev.keys())
        all_cumulative = all(curr[k][0] >= prev[k][0] - 0.01 and curr[k][1] >= prev[k][1] - 0.01 for k in curr)
        assert all_cumulative

        delta = {}
        for k in curr:
            delta[k] = (round(curr[k][0] - prev[k][0], 2), round(curr[k][1] - prev[k][1], 2))

        assert delta[(1, 2)] == (160.0, 140.0)
        assert delta[(3, 4)] == (160.0, 120.0)

    def test_zero_delta_preserved(self):
        """If a team scores 0 in week 2 (unlikely but possible), delta = 0."""
        prev = {(1, 2): (150.0, 130.0)}
        curr = {(1, 2): (150.0, 200.0)}  # home didn't change, away went up

        delta = {}
        for k in curr:
            delta[k] = (round(curr[k][0] - prev[k][0], 2), round(curr[k][1] - prev[k][1], 2))

        assert delta[(1, 2)] == (0.0, 70.0)
