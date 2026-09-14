import duckdb

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.draft.sql_draft_enrichments import DraftEnrichmentsMixin


class _DraftRunner(DraftEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def test_roster_slot_helpers_ignore_scoring_metadata_and_normalize_aliases():
    base = SQLEnrichmentsBase("test_db", dry_run=True)

    roster_settings = {
        "QB": 1,
        "RB": 2,
        "W/R/T": 1,
        "Q/W/R/T": 1,
        "D/ST": 1,
        "BN": 6,
        "kick_col": "pts_k_yds",
        "scoring_settings": {"rec": 0.5, "pass_td": 4},
        "bonus_multipliers": {"bonus_pass_300yd": 1.0},
        "te_premium": 0.5,
    }

    dedicated = base._get_dedicated_slots(roster_settings)
    flex_positions = {pos: count for pos, _eligible, count in base._identify_flex_positions(roster_settings)}

    assert dedicated == {"QB": 1, "RB": 2, "DEF": 1}
    assert flex_positions == {"FLX": 1, "SUPER_FLEX": 1}


def test_draft_starter_designation_uses_normalized_league_settings_bucket(tmp_path):
    db_name = "draft_starter_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            primary_position VARCHAR,
            round INTEGER,
            pick INTEGER,
            cost DOUBLE,
            position_draft_rank INTEGER,
            position_draft_label VARCHAR,
            starter_slots_available INTEGER,
            drafted_as_starter INTEGER,
            drafted_as_backup INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.draft (
            year, manager, franchise_id, primary_position, round, pick, cost,
            position_draft_rank, position_draft_label, starter_slots_available,
            drafted_as_starter, drafted_as_backup
        ) VALUES
            (2025, 'Alice', 'fid_alice', 'QB', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL),
            (2025, 'Alice', 'fid_alice', 'RB', 1, 2, NULL, NULL, NULL, NULL, NULL, NULL),
            (2025, 'Alice', 'fid_alice', 'RB', 1, 3, NULL, NULL, NULL, NULL, NULL, NULL),
            (2025, 'Alice', 'fid_alice', 'WR', 1, 4, NULL, NULL, NULL, NULL, NULL, NULL),
            (2025, 'Alice', 'fid_alice', 'WR', 1, 5, NULL, NULL, NULL, NULL, NULL, NULL),
            (2025, 'Alice', 'fid_alice', 'TE', 1, 6, NULL, NULL, NULL, NULL, NULL, NULL),
            (2025, 'Alice', 'fid_alice', 'DEF', 1, 7, NULL, NULL, NULL, NULL, NULL, NULL),
            (2025, 'Alice', 'fid_alice', 'WR', 1, 8, NULL, NULL, NULL, NULL, NULL, NULL),
            (2025, 'Alice', 'fid_alice', 'QB', 1, 9, NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )
    conn.close()

    runner = _DraftRunner(
        db_name=db_name,
        data_dir=str(tmp_path),
        roster_by_year={
            2025: {
                "QB": 1,
                "RB": 2,
                "WR": 2,
                "TE": 1,
                "W/R/T": 1,
                "Q/W/R/T": 1,
                "D/ST": 1,
                "BN": 6,
                "kick_col": "pts_k_yds",
                "scoring_settings": {"rec": 0.5, "pass_td": 4},
            }
        },
    )

    try:
        runner.draft_starter_designation()
        rows = runner.conn.execute(
            """
            SELECT pick, primary_position, starter_slots_available, drafted_as_starter, drafted_as_backup
            FROM public.draft
            ORDER BY pick
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows[0] == (1, "QB", 1, 1, 0)
    assert rows[1] == (2, "RB", 2, 1, 0)
    assert rows[2] == (3, "RB", 2, 1, 0)
    assert rows[3] == (4, "WR", 2, 1, 0)
    assert rows[4] == (5, "WR", 2, 1, 0)
    assert rows[5] == (6, "TE", 1, 1, 0)
    assert rows[6] == (7, "DEF", 1, 1, 0)
    assert rows[7] == (8, "WR", 2, 1, 0)
    assert rows[8] == (9, "QB", 1, 1, 0)
