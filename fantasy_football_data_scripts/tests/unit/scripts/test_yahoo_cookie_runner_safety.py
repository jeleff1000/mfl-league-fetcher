from pathlib import Path


def test_cookie_runner_is_local_and_drops_legacy_motherduck_token():
    source = Path("scripts/yahoo_cookie_runner.py").read_text(encoding="utf-8")
    assert 'os.environ["DATABASE_BACKEND"] = "local"' in source
    assert 'os.environ.pop("MOTHERDUCK_TOKEN", None)' in source
    assert 'setdefault("MOTHERDUCK_TOKEN", "local-only")' not in source


def test_expected_record_reports_the_actual_data_dir_backend():
    source = Path(
        "fantasy_football_data_scripts/multi_league/transformations/matchup/expected_record_v2.py"
    ).read_text(encoding="utf-8")
    assert 'connection_target = "local DuckDB" if args.data_dir else "Fly DuckDB"' in source
    assert 'print(f"Connected to {connection_target}:' in source


def test_worker_has_no_healthy_season_cooldown_by_default():
    source = Path("scripts/yahoo_cookie_worker.py").read_text(encoding="utf-8")
    assert 'parser.add_argument("--year-throttle-cooldown", type=float, default=0.0)' in source
