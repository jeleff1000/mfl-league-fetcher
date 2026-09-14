from pathlib import Path


def test_matchup_builder_reuses_local_reader_connection():
    source = Path(__file__).with_name("build_research_matchup_cohort.py").read_text()
    assert "con = fly.con" in source
    assert "con = duckdb.connect(); con.execute(\"SET memory_limit='1500MB'\")" not in source
