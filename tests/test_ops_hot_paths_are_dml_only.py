import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_remote_ops_hot_paths_do_not_issue_ddl():
    targets = [
        (ROOT / "fantasy_football_data_scripts/multi_league/core/league_update_status.py", "record_league_update_status"),
        (ROOT / "fantasy_football_data_scripts/multi_league/core/offseason_update_status.py", "record_offseason_update_status"),
        (ROOT / "fantasy_football_data_scripts/multi_league/core/targets/fly_target.py", "mark_league_imported"),
        (ROOT / "fantasy_football_data_scripts/multi_league/utils/credential_store.py", "_store_league_credentials_fly"),
        (ROOT / "fantasy_football_data_scripts/multi_league/utils/credential_store.py", "_store_yahoo_cookie_credentials_fly"),
    ]
    forbidden = ("CREATE SCHEMA", "CREATE TABLE", "ALTER TABLE", "DROP TABLE")

    offenders = {}
    for path, function_name in targets:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        function = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function_name
        )
        function_source = ast.get_source_segment(source, function).upper()
        for keyword in forbidden:
            if keyword in function_source:
                offenders[f"{path.relative_to(ROOT)}:{function_name}"] = keyword

    assert offenders == {}
