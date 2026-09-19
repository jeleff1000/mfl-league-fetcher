"""Saved owner merges must retain indexed franchise identity and write scope."""

import duckdb
import pytest

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.matchup.sql_matchup_enrichments import MatchupEnrichmentsMixin


class MergeRunner(MatchupEnrichmentsMixin, SQLEnrichmentsBase):
    pass


TARGETS = {
    "matchup": (("franchise_id", "manager"), ("opponent_franchise_id", "opponent")),
    "player_fantasy": (("franchise_id", "manager"), ("opponent_franchise_id", "opponent")),
    "draft": (("franchise_id", "manager"),),
    "transactions": (
        ("franchise_id", "manager"),
        ("source_franchise_id", "source_manager"),
        ("destination_franchise_id", "destination_manager"),
    ),
    "schedule": (("franchise_id", "manager"), ("opponent_franchise_id", "opponent")),
}
FIDS = [
    "old-guid_0", "new-guid_0", "old-guid_1", "new-guid_1",
    "new-guid-extra_0", "new-guid_x", "new-guid_0_extra", "old-guid", "new-guid",
]


@pytest.fixture
def merge_db():
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA ___leagues.public")
    for table, pairs in TARGETS.items():
        identity_ddl = ", ".join(f"{fid} VARCHAR, {name} VARCHAR" for fid, name in pairs)
        conn.execute(f"""
            CREATE TABLE ___leagues.public.{table} (
                db_name VARCHAR, year INTEGER, row_id INTEGER, points DOUBLE,
                manager_guid VARCHAR, team_key VARCHAR, franchise_name VARCHAR,
                {identity_ddl}
            )
        """)
        rows = []
        for league, year in (("merge_test", 2025), ("merge_test", 2026), ("other_league", 2026)):
            for i, fid in enumerate(FIDS):
                rows.append((league, year, i, 100.0 + i, "source-owner", f"team-{i}", "Original",
                             *[value for _ in pairs for value in (fid, "Original")]))
        conn.executemany(
            f"INSERT INTO ___leagues.public.{table} VALUES ({', '.join('?' for _ in rows[0])})", rows,
        )
    try:
        yield conn
    finally:
        conn.close()


def snapshot(conn):
    return {
        table: conn.execute(f"SELECT * FROM ___leagues.public.{table} ORDER BY db_name, year, row_id").fetchall()
        for table in TARGETS
    }


@pytest.mark.parametrize("years", [{2026}, None])
def test_owner_ids_merge_indexed_fids_without_collapsing_second_team(merge_db, years):
    before = snapshot(merge_db)
    runner = MergeRunner(
        db_name="merge_test", conn=merge_db,
        franchise_merges=[{"owner_ids": ["old-guid", "new-guid"], "display_name": "Saved Owner"}],
    )
    runner._apply_saved_franchise_merges(season_years=years)
    expected_fids = [
        "old-guid_0", "old-guid_0", "old-guid_1", "old-guid_1",
        "new-guid-extra_0", "new-guid_x", "new-guid_0_extra", "old-guid", "old-guid",
    ]
    after = snapshot(merge_db)
    for table, pairs in TARGETS.items():
        assert [row[7] for row in after[table] if row[:2] == ("merge_test", 2026)] == expected_fids
        for original, actual in zip(before[table], after[table], strict=True):
            league, year, row_id = original[:3]
            if league != "merge_test" or (years is not None and year not in years):
                assert actual == original
                continue
            assert actual[:6] == original[:6]  # facts and provider identity remain intact
            expected_name = "Saved Owner" if row_id in (0, 1, 2, 3, 7, 8) else "Original"
            assert actual[6] == expected_name
            assert actual[7:] == tuple(value for _ in pairs for value in (expected_fids[row_id], expected_name))
    runner._apply_saved_franchise_merges(season_years=years)
    assert snapshot(merge_db) == after


@pytest.mark.parametrize("merge", [
    {"from_franchise_id": "new-guid_1", "into_franchise_id": "old-guid_0"},
    {"franchise_ids": ["old-guid_0", "new-guid_1"]},
    {"owner_ids": ["old-guid_0", "new-guid_1"], "into_franchise_id": "old-guid_0"},
])
def test_explicit_franchise_merges_remain_exact(merge_db, merge):
    before = snapshot(merge_db)
    runner = MergeRunner(db_name="merge_test", conn=merge_db, franchise_merges=[merge])
    runner._apply_saved_franchise_merges(season_years={2026})
    after = snapshot(merge_db)
    for table, pairs in TARGETS.items():
        for original, actual in zip(before[table], after[table], strict=True):
            if original[:3] == ("merge_test", 2026, 3):
                assert actual[:7] == original[:7]
                assert actual[7:] == tuple(value for _ in pairs for value in ("old-guid_0", "Original"))
            else:
                assert actual == original


def test_empty_year_scope_still_rejects_unscoped_write_without_changes(merge_db):
    before = snapshot(merge_db)
    runner = MergeRunner(
        db_name="merge_test", conn=merge_db,
        franchise_merges=[{"owner_ids": ["old-guid", "new-guid"]}],
    )
    with pytest.raises(RuntimeError, match="UNSAFE SQL"):
        runner._apply_saved_franchise_merges(season_years=set())
    assert snapshot(merge_db) == before
