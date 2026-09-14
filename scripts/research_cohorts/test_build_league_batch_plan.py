import importlib.util
from pathlib import Path

import duckdb


def _module():
    path = Path(__file__).with_name("build_league_batch_plan.py")
    spec = importlib.util.spec_from_file_location("build_league_batch_plan", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_planning_connection_uses_only_raw_population_inputs(tmp_path):
    module = _module()
    lake = tmp_path / "lake.duckdb"
    con = duckdb.connect(str(lake))
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.league_settings(db_name VARCHAR, year INTEGER)")
    con.execute("CREATE TABLE public.player_fantasy(db_name VARCHAR, year INTEGER, NFL_player_id VARCHAR, fantasy_points DOUBLE, is_rostered INTEGER)")
    con.execute("CREATE TABLE public.matchup(db_name VARCHAR, year INTEGER)")
    con.execute("INSERT INTO public.league_settings VALUES ('l1', 2025)")
    con.execute("""INSERT INTO public.player_fantasy
        SELECT 'l1', 2025, 'p' || CAST(i AS VARCHAR), 10, 1
        FROM range(100) AS r(i)""")
    con.execute("INSERT INTO public.matchup VALUES ('l1', 2025)")
    con.close()

    planning = module.open_planning_connection(lake)
    try:
        assert planning.execute("SELECT COUNT(*) FROM public.league_settings").fetchone()[0] == 1
        assert planning.execute("SELECT COUNT(*) FROM _lg_has_data").fetchone()[0] == 1
        assert planning.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] == 100
        assert planning.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0] == 1
    finally:
        planning.close()
