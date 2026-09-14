import json
from types import SimpleNamespace

from multi_league.core.fetch_runtime import (
    load_runtime,
    resolve_context_path,
    runtime_from_source,
)
from multi_league.core.league_context import LeagueContext


def test_runtime_from_yahoo_context_uses_pre_resolved_db_name(tmp_path):
    ctx = SimpleNamespace(
        league_id="461.l.90939",
        league_name="KMFFL",
        data_directory=tmp_path,
        oauth_file_path=tmp_path / "Oauth.json",
        motherduck_db_name="kmffl_prod",
        manager_name_overrides={"Old Name": "New Name"},
        league_ids={"2024": "461.l.111", "2025": "461.l.222"},
        start_year=2024,
        end_year=2025,
    )

    runtime = runtime_from_source(ctx)

    assert runtime.platform == "yahoo"
    assert runtime.db_name == "kmffl_prod"
    assert runtime.data_dir == tmp_path
    assert runtime.league_id_for_year(2024) == "461.l.111"
    assert runtime.auth["oauth_file"] == tmp_path / "Oauth.json"
    assert runtime.manager_name_overrides == {"Old Name": "New Name"}


def test_runtime_from_session_like_source_preserves_database_name_and_auth(tmp_path):
    session = SimpleNamespace(
        platform="espn",
        database_name="the_league_of_record_iii",
        league_name="The League Of Record III",
        league_id="1312071192336666624",
        league_ids={"2024": "123", "2025": "456"},
        data_dir=tmp_path,
        auth={"espn_s2": "cookie", "swid": "{SWID}"},
        start_year=2024,
        end_year=2025,
    )

    runtime = runtime_from_source(session)

    assert runtime.platform == "espn"
    assert runtime.db_name == "the_league_of_record_iii"
    assert runtime.auth == {"espn_s2": "cookie", "swid": "{SWID}"}
    assert runtime.league_id_for_year(2025) == "456"


def test_load_runtime_detects_espn_context_file(tmp_path):
    context_path = tmp_path / "espn_context.json"
    context_path.write_text(
        json.dumps(
            {
                "platform": "espn",
                "league_id": 71580,
                "league_name": "The League Of Record III",
                "data_directory": tmp_path.as_posix(),
                "league_ids": {"2024": 71580, "2025": 71580},
                "motherduck_db_name": "the_league_of_record_iii",
            }
        ),
        encoding="utf-8",
    )

    runtime = load_runtime(context_path)

    assert runtime.platform == "espn"
    assert runtime.db_name == "the_league_of_record_iii"
    assert runtime.data_dir == tmp_path
    assert runtime.context_path == context_path


def test_legacy_league_context_loader_detects_espn(tmp_path):
    context_path = tmp_path / "espn_context.json"
    context_path.write_text(
        json.dumps(
            {
                "platform": "espn",
                "league_id": 71580,
                "league_name": "The League Of Record III",
                "data_directory": tmp_path.as_posix(),
            }
        ),
        encoding="utf-8",
    )

    ctx = LeagueContext.load(context_path)

    assert type(ctx).__name__ == "ESPNContext"


def test_resolve_context_path_prefers_platform_specific_filename(tmp_path):
    context_file = tmp_path / "sleeper_context.json"
    context_file.write_text("{}", encoding="utf-8")
    ctx = SimpleNamespace(platform="sleeper", data_directory=tmp_path)

    resolved = resolve_context_path(ctx)

    assert resolved == context_file
