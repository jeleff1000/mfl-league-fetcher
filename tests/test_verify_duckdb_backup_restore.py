import json
from pathlib import Path
import shutil
import subprocess
import sys

import duckdb


SCRIPT = Path(__file__).parents[1] / "scripts" / "verify_duckdb_backup_restore.py"


def _make_database(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE TABLE public.items(id INTEGER, value VARCHAR)")
    conn.execute("INSERT INTO public.items VALUES (1, 'one'), (2, 'two')")
    conn.execute("CHECKPOINT")
    conn.close()


def test_restore_verifier_accepts_identical_offline_database(tmp_path):
    source = tmp_path / "source.duckdb"
    restored = tmp_path / "restored.duckdb"
    _make_database(source)
    shutil.copy2(source, restored)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--source", str(source), "--restored", str(restored)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    evidence = json.loads(result.stdout)
    assert evidence["match"] is True
    assert evidence["source"]["tables"]["public.items"]["rows"] == 2
    assert evidence["source"]["tables"]["public.items"]["logical_checksum"]


def test_restore_verifier_rejects_logical_mismatch(tmp_path):
    source = tmp_path / "source.duckdb"
    restored = tmp_path / "restored.duckdb"
    _make_database(source)
    shutil.copy2(source, restored)
    conn = duckdb.connect(str(restored))
    conn.execute("UPDATE public.items SET value = 'changed' WHERE id = 2")
    conn.close()

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--source", str(source), "--restored", str(restored)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1
    evidence = json.loads(result.stdout)
    assert evidence["match"] is False
    assert evidence["mismatches"] == ["public.items:logical_checksum"]
