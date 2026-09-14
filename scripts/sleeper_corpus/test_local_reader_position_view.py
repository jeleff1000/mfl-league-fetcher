import duckdb

from local_reader import configured_reader_memory_mb, player_position_view_sql


def test_reader_memory_limit_is_bounded_and_configurable(monkeypatch):
    monkeypatch.delenv("RESEARCH_READER_MEMORY_MB", raising=False)
    assert configured_reader_memory_mb() == 3000
    monkeypatch.setenv("RESEARCH_READER_MEMORY_MB", "5500")
    assert configured_reader_memory_mb() == 5500
    monkeypatch.setenv("RESEARCH_READER_MEMORY_MB", "99999")
    assert configured_reader_memory_mb() == 6000


def test_position_view_is_one_row_per_player_year_across_multiple_labels():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public._pos_raw(NFL_player_id VARCHAR, year INTEGER, position VARCHAR)")
    con.execute("CREATE TABLE _pos_tax(detailed VARCHAR, broad VARCHAR)")
    con.execute("INSERT INTO _pos_tax VALUES ('RDE','DL'),('OLB','LB'),('K','K'),('P','P')")
    con.execute("""INSERT INTO public._pos_raw VALUES
        ('idp',2008,'RDE'),('idp',2008,'OLB'),('leg',2008,'K,P')""")

    con.execute(player_position_view_sql())

    assert con.execute("SELECT COUNT(*) FROM public.player_position").fetchone()[0] == 2
    assert con.execute("""SELECT position,broad_positions FROM public.player_position
        WHERE NFL_player_id='idp'""").fetchone() == ('DL', ['DL', 'LB'])
    assert con.execute("""SELECT position,broad_positions FROM public.player_position
        WHERE NFL_player_id='leg'""").fetchone() == ('K', ['K', 'P'])
