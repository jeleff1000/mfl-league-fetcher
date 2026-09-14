from __future__ import annotations

import duckdb

from apply_player_sidecar_delta import CANONICAL_COLUMNS, apply


def _schema() -> dict[str, str]:
    numeric = {"year": "INTEGER", "week": "INTEGER", "is_started": "INTEGER", "is_rostered": "INTEGER", "fantasy_points": "DOUBLE", "win": "INTEGER", "champion": "INTEGER", "clutch_equity": "DOUBLE", "manager_lamar": "DOUBLE", "team_points": "DOUBLE", "final_playoff_seed": "INTEGER", "is_playoffs": "INTEGER", "has_po_signal": "INTEGER", "made_playoffs": "INTEGER"}
    return {c: numeric.get(c, "VARCHAR") for c in CANONICAL_COLUMNS}


def _row(**values):
    row = {c: None for c in CANONICAL_COLUMNS}
    row.update(values)
    return [row[c] for c in CANONICAL_COLUMNS]


def test_player_sidecar_preserves_and_adds_rows(tmp_path):
    base_path = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base_path))
    con.execute("create schema public")
    types = _schema()
    ddl = ", ".join(f'"{c}" {types[c]}' for c in CANONICAL_COLUMNS)
    con.execute(f"create table public.player_fantasy ({ddl})")
    con.execute(
        f"insert into public.player_fantasy values ({','.join('?' for _ in CANONICAL_COLUMNS)})",
        _row(db_name="db", year=2024, week=1, NFL_player_id="nfl-1", platform="sleeper", sleeper_player_id="s-1", manager="m1", team_key="t1"),
    )
    con.execute("create table public.league_settings (db_name varchar, year integer)")
    con.execute("insert into public.league_settings values ('db', 2024)")
    con.close()

    sidecar_dir = tmp_path / "sidecars"
    sidecar_dir.mkdir()
    sidecar_path = sidecar_dir / "promotable_player_delta_0.parquet"
    source = duckdb.connect()
    source.execute(f"create table rows ({ddl})")
    placeholders = ",".join("?" for _ in CANONICAL_COLUMNS)
    source.execute(
        f"insert into rows values ({placeholders}), ({placeholders})",
        _row(db_name="db", year=2024, week=1, NFL_player_id="nfl-1", platform="sleeper", sleeper_player_id="s-1", manager="m1", team_key="t1", win=1)
        + _row(db_name="db", year=2024, week=1, NFL_player_id="nfl-2", platform="sleeper", sleeper_player_id="s-2", manager="m2", team_key="t2", is_rostered=1),
    )
    source.execute("copy rows to ? (format parquet)", [str(sidecar_path)])
    source.close()

    out = tmp_path / "out.duckdb"
    report = apply(base_path, sidecar_dir, out, "promotable_player_delta*.parquet")
    assert report["new_player_rows"] == 1
    assert report["output_player_rows"] == 2
    assert report["duplicate_output_keys"] == 0
    assert report["missing_base_keys"] == 0

    check = duckdb.connect(str(out), read_only=True)
    assert check.execute("select win from public.player_fantasy where NFL_player_id='nfl-1'").fetchone()[0] == 1
    assert check.execute("select count(*) from public.player_fantasy where NFL_player_id='nfl-2'").fetchone()[0] == 1
    assert [r[0] for r in check.execute("describe public.player_fantasy").fetchall()] == CANONICAL_COLUMNS
    check.close()
