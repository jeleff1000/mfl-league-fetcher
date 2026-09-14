from __future__ import annotations

import duckdb
import pytest

from validate_player_sidecar_delta import CANONICAL_COLUMNS, validate_player_sidecar


def _row(db: str, player: str, manager: str, points: float | None, win: int | None):
    values = {c: None for c in CANONICAL_COLUMNS}
    values.update(
        {
            "db_name": db,
            "year": 2024,
            "week": 1,
            "NFL_player_id": player,
            "manager": manager,
            "team_name": manager,
            "team_key": manager,
            "fantasy_points": points,
            "win": win,
            "is_started": 1,
            "is_rostered": 1,
        }
    )
    return [values[c] for c in CANONICAL_COLUMNS]


def test_player_sidecar_inserts_new_rows_and_only_fills_nulls(tmp_path):
    base = duckdb.connect(str(tmp_path / "base.duckdb"))
    base.execute("create schema public")
    cols = ", ".join(f'"{c}" VARCHAR' for c in CANONICAL_COLUMNS)
    base.execute(f"create table public.player_fantasy ({cols})")
    base.execute(
        f"insert into public.player_fantasy values ({','.join(['?'] * len(CANONICAL_COLUMNS))})",
        _row("db", "p1", "m1", None, None),
    )
    source = tmp_path / "source.parquet"
    source_con = duckdb.connect()
    source_con.execute(f"create table source ({cols})")
    source_con.execute(
        f"insert into source values ({','.join(['?'] * len(CANONICAL_COLUMNS))})",
        _row("db", "p1", "m1", 12.5, 1),
    )
    source_con.execute(
        f"insert into source values ({','.join(['?'] * len(CANONICAL_COLUMNS))})",
        _row("db", "p2", "m2", 8.0, 0),
    )
    source_con.execute("copy source to ? (format parquet)", [str(source)])
    source_con.close()
    base.close()

    report = validate_player_sidecar(
        tmp_path / "base.duckdb", source, tmp_path / "out", 0, 1
    )
    assert report["source_rows"] == 2
    assert report["matched_rows"] == 1
    assert report["new_rows"] == 1
    assert report["promotable_rows"] == 2
    assert report["improvements_by_field"]["fantasy_points"] == 1
    assert report["improvements_by_field"]["win"] == 1
    assert report["schema_unchanged"] is True


def test_player_sidecar_rejects_team_only_source(tmp_path):
    base = duckdb.connect(str(tmp_path / "base.duckdb"))
    base.execute("create schema public")
    cols = ", ".join(f'"{c}" VARCHAR' for c in CANONICAL_COLUMNS)
    base.execute(f"create table public.player_fantasy ({cols})")
    base.close()
    source = tmp_path / "team_only.parquet"
    source_con = duckdb.connect()
    source_con.execute(
        'copy (select \'db\' as "db_name", 2024 as "year", 1 as "week", \'m1\' as "manager", \'m1\' as "team_name", \'1\' as "team_key", 1 as "win") to ? (format parquet)',
        [str(source)],
    )
    source_con.close()
    with pytest.raises(ValueError, match="exact canonical 29 columns"):
        validate_player_sidecar(tmp_path / "base.duckdb", source, tmp_path / "out", 0, 1)


def test_player_sidecar_accepts_platform_id_when_nfl_id_is_null(tmp_path):
    base = duckdb.connect(str(tmp_path / "base.duckdb"))
    base.execute("create schema public")
    cols = ", ".join(f'"{c}" VARCHAR' for c in CANONICAL_COLUMNS)
    base.execute(f"create table public.player_fantasy ({cols})")
    base.close()
    source = tmp_path / "platform_id.parquet"
    row = {c: None for c in CANONICAL_COLUMNS}
    row.update({"db_name": "db", "year": 2024, "week": 1, "NFL_player_id": None,
                "sleeper_player_id": "s1", "manager": "m1", "team_name": "m1",
                "team_key": "1", "is_started": 1, "is_rostered": 1})
    con = duckdb.connect()
    con.execute(f"create table source ({cols})")
    con.execute(f"insert into source values ({','.join(['?'] * len(CANONICAL_COLUMNS))})",
                 [row[c] for c in CANONICAL_COLUMNS])
    con.execute("copy source to ? (format parquet)", [str(source)])
    con.close()
    report = validate_player_sidecar(tmp_path / "base.duckdb", source, tmp_path / "out", 0, 1)
    assert report["new_rows"] == 1
