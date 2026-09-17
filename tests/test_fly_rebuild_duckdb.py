from pathlib import Path

import duckdb
import pytest

from scripts.fly_rebuild_duckdb import rebuild_database


def _build_source(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE SCHEMA merge_admin")
    conn.execute(
        "CREATE TABLE public.healthy "
        "(db_name VARCHAR NOT NULL, value INTEGER, PRIMARY KEY (db_name))"
    )
    conn.execute("INSERT INTO public.healthy VALUES ('alpha', 1), ('beta', 2)")
    conn.execute(
        "CREATE TABLE public.damaged "
        "(db_name VARCHAR NOT NULL, year INTEGER NOT NULL, value VARCHAR, "
        "PRIMARY KEY (db_name, year))"
    )
    conn.execute("INSERT INTO public.damaged VALUES ('alpha', 2026, 'bad')")
    conn.execute(
        "CREATE TABLE merge_admin.generations "
        "(db_name VARCHAR NOT NULL, generation BIGINT NOT NULL)"
    )
    conn.execute("INSERT INTO merge_admin.generations VALUES ('alpha', 4)")
    conn.close()


def test_rebuild_copies_healthy_tables_and_preserves_empty_table_schema(tmp_path, capsys):
    source = tmp_path / "source.duckdb"
    target = tmp_path / "target.duckdb"
    _build_source(source)

    result = rebuild_database(
        source,
        target,
        empty_tables={("public", "damaged")},
    )

    progress = capsys.readouterr().out
    assert '"table": "merge_admin.generations"' in progress
    assert '"table": "public.damaged"' in progress
    assert '"table": "public.healthy"' in progress

    assert result["tables"]["public.healthy"]["target_rows"] == 2
    assert result["tables"]["public.damaged"] == {
        "source_rows": -1,
        "target_rows": 0,
        "emptied": True,
    }
    conn = duckdb.connect(str(target), read_only=True)
    assert conn.execute("SELECT * FROM public.healthy ORDER BY db_name").fetchall() == [
        ("alpha", 1),
        ("beta", 2),
    ]
    assert conn.execute("SELECT COUNT(*) FROM public.damaged").fetchone()[0] == 0
    assert conn.execute("SELECT * FROM merge_admin.generations").fetchall() == [("alpha", 4)]
    source_schema = conn.execute("DESCRIBE public.damaged").fetchall()
    conn.close()

    original = duckdb.connect(str(source), read_only=True)
    assert source_schema == original.execute("DESCRIBE public.damaged").fetchall()
    original.close()

    writable = duckdb.connect(str(target))
    writable.execute("INSERT INTO public.damaged VALUES ('alpha', 2026, 'recovered')")
    with pytest.raises(duckdb.ConstraintException):
        writable.execute("INSERT INTO public.damaged VALUES ('alpha', 2026, 'duplicate')")
    writable.close()


def test_rebuild_rejects_unlisted_catalog_objects(tmp_path):
    source = tmp_path / "source.duckdb"
    target = tmp_path / "target.duckdb"
    _build_source(source)
    conn = duckdb.connect(str(source))
    conn.execute("CREATE VIEW public.healthy_view AS SELECT * FROM public.healthy")
    conn.close()

    with pytest.raises(RuntimeError, match="unsupported catalog objects"):
        rebuild_database(source, target, empty_tables={("public", "damaged")})


def test_rebuild_requires_every_excluded_table_to_exist(tmp_path):
    source = tmp_path / "source.duckdb"
    target = tmp_path / "target.duckdb"
    _build_source(source)

    with pytest.raises(RuntimeError, match="excluded tables are absent"):
        rebuild_database(source, target, empty_tables={("public", "missing")})
