import argparse
import json
from pathlib import Path
import pytest
from multi_league.core.import_args import add_import_args, resolve_import_args


def test_resolve_from_db_and_data_dir():
    parser = argparse.ArgumentParser()
    add_import_args(parser)
    args = parser.parse_args(["--db", "kmffl", "--data-dir", "/tmp/data"])
    db_name, data_dir, is_quick = resolve_import_args(args)
    assert db_name == "kmffl"
    assert data_dir == Path("/tmp/data")
    assert is_quick is False


def test_resolve_quick_flag():
    parser = argparse.ArgumentParser()
    add_import_args(parser)
    args = parser.parse_args(["--db", "kmffl", "--data-dir", "/tmp/data", "--quick"])
    _, _, is_quick = resolve_import_args(args)
    assert is_quick is True


def test_resolve_requires_db_or_context():
    parser = argparse.ArgumentParser()
    add_import_args(parser)
    args = parser.parse_args(["--data-dir", "/tmp/data"])
    with pytest.raises(SystemExit):
        resolve_import_args(args)


def test_resolve_db_without_data_dir_fails():
    parser = argparse.ArgumentParser()
    add_import_args(parser)
    args = parser.parse_args(["--db", "kmffl"])
    with pytest.raises(SystemExit):
        resolve_import_args(args)


def test_resolve_from_espn_context(tmp_path):
    context_path = tmp_path / "espn_context.json"
    context_path.write_text(
        json.dumps(
            {
                "platform": "espn",
                "league_id": 71580,
                "league_name": "The League Of Record III",
                "data_directory": tmp_path.as_posix(),
                "league_ids": {"2024": 71580},
                "motherduck_db_name": "the_league_of_record_iii",
            }
        ),
        encoding="utf-8",
    )

    parser = argparse.ArgumentParser()
    add_import_args(parser)
    args = parser.parse_args(["--context", str(context_path)])

    db_name, data_dir, is_quick = resolve_import_args(args)

    assert db_name == "the_league_of_record_iii"
    assert data_dir == tmp_path
    assert is_quick is False
