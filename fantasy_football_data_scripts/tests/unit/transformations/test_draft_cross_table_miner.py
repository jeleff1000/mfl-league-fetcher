import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def test_cross_table_sql_uses_point_in_time_predictors():
    from multi_league.transformations.draft.cross_table_miner import build_cross_table_base_sql

    sql = build_cross_table_base_sql("demo_league")

    assert "pfs.year = b.year - 1" in sql
    assert "pfs.year < b.year" in sql
    assert "dms.year < b.year" in sql
    assert "ms.year < b.year" in sql
    assert "ls.year = b.year" in sql
    assert "player_fantasy_career" not in sql
    assert "d.db_name = 'demo_league'" in sql
