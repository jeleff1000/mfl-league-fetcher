import duckdb

from build_source_matchup_rescue_sidecar import resolve_mfl_target_id
from run_source_playoff_clutch_backfill import source_readiness


def _db(tmp_path, ddl, rows=()):
    path = tmp_path / "sample.duckdb"
    con = duckdb.connect(str(path))
    con.execute("create schema public")
    con.execute(ddl)
    for row in rows:
        con.execute("insert into public.player_fantasy values (?, ?, ?, ?)", row)
    con.close()
    return path


def test_empty_unrostered_source_is_skipped(tmp_path):
    path = _db(
        tmp_path,
        "create table public.player_fantasy(manager varchar, team_points double, opponent_points double, year integer)",
        [("Unrostered", None, None, 2023)],
    )
    result = source_readiness(path)
    assert result["status"] == "skipped_no_matchup_evidence"


def test_missing_matchup_with_real_outcomes_is_not_silently_skipped(tmp_path):
    path = _db(
        tmp_path,
        "create table public.player_fantasy(manager varchar, team_points double, opponent_points double, year integer)",
        [("Team A", 100.0, 90.0, 2023)],
    )
    result = source_readiness(path)
    assert result["status"] == "source_missing_matchup"
    assert result["outcome_rows"] == 1


def test_nonempty_matchup_is_ready(tmp_path):
    path = tmp_path / "sample.duckdb"
    con = duckdb.connect(str(path))
    con.execute("create schema public")
    con.execute("create table public.player_fantasy(manager varchar, team_points double, opponent_points double)")
    con.execute("create table public.matchup(year integer, week integer)")
    con.execute("insert into public.matchup values (2023, 1)")
    con.close()
    assert source_readiness(path)["status"] == "ready"


class _MFLHistoryProbe:
    def fetch_league(self, league_id, year):
        if str(league_id) == "25065" and year == 2003:
            return {"id": "25065", "year": 2003}
        if str(league_id) == "11490" and year == 2015:
            return {"history": {"league": [
                {"year": 2003, "url": "https://www53.myfantasyleague.com/2003/home/25065"},
            ]}}
        return None


def test_mfl_manifest_target_id_is_used_when_already_resolved():
    assert resolve_mfl_target_id(_MFLHistoryProbe(), "25065", "smpl_mfl_2015_11490", 2003) == "25065"


def test_mfl_seed_id_resolves_through_source_history():
    assert resolve_mfl_target_id(_MFLHistoryProbe(), "11490", "smpl_mfl_2015_11490", 2003) == "25065"
