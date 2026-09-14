from pathlib import Path

import duckdb
import pandas as pd

import multi_league.core.targets.fly_target as fly_target
from multi_league.core.local_db import LocalLeagueDB, _registerable_frame


def test_registerable_frame_json_encodes_nested_pandas_values_without_numpy_isnan():
    """ESPN roster payloads can retain nested values in object columns."""
    frame = pd.DataFrame({"payload": [{"id": 1}, ["BN", "IR"], None]})

    registered = _registerable_frame(frame)

    assert registered["payload"].tolist() == ['{"id": 1}', '["BN", "IR"]', None]


def test_upload_to_fly_stages_only_canonical_tables(monkeypatch, tmp_path):
    captured = {}

    class FakeFlyTarget:
        def merge_league(self, db_name, local_path):
            captured["db_name"] = db_name
            captured["exists_during_merge"] = Path(local_path).exists()

            conn = duckdb.connect(str(local_path), read_only=True)
            try:
                captured["tables"] = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                    """
                ).fetchall()
                captured["rows"] = conn.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0]
                captured["managers"] = conn.execute("SELECT manager FROM public.matchup ORDER BY week").fetchall()
            finally:
                conn.close()

            return {"status": "merged", "tables": {"matchup": captured["rows"]}}

        def mark_league_imported(self, *args, **kwargs):
            captured["marked"] = (args, kwargs)
            return []

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)

    db = LocalLeagueDB(tmp_path, "speed_test")
    conn = db.connect()
    conn.execute("CREATE TABLE public.matchup (year INTEGER, week INTEGER, manager VARCHAR)")
    conn.execute("INSERT INTO public.matchup VALUES (2026, 1, 'Alice'), (2026, 2, 'Bob')")
    conn.execute("CREATE TABLE public.scratch_table (value INTEGER)")
    conn.execute("INSERT INTO public.scratch_table VALUES (99)")

    db.upload_to_fly("speed_test", import_mode="quick", platform="sleeper", finalize_merge_source=False)
    db.close()

    assert captured["db_name"] == "speed_test"
    assert captured["exists_during_merge"] is True
    assert captured["tables"] == [("matchup",)]
    assert captured["rows"] == 2
    assert captured["managers"] == [("Alice",), ("Bob",)]
    assert captured["marked"][1] == {"import_mode": "quick", "platform": "sleeper"}
