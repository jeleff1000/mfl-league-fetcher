"""Full-history owner disambiguation changes labels, never matchup facts."""

import duckdb

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.matchup.sql_matchup_enrichments import MatchupEnrichmentsMixin


class _Runner(MatchupEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def test_quick_to_full_history_splits_shared_account_without_changing_scores(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_SERVER_URL", raising=False)
    monkeypatch.delenv("DATABASE_READ_TOKEN", raising=False)
    db_name = "full_history_identity_scores"
    owner = "yh-img-4737-38195850280-792c52"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA public")
    conn.execute("""
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER, team_key VARCHAR,
            manager_guid VARCHAR, franchise_id VARCHAR, manager VARCHAR,
            team_name VARCHAR, opponent VARCHAR, opponent_guid VARCHAR,
            opponent_franchise_id VARCHAR, team_points DOUBLE,
            opponent_points DOUBLE, win INTEGER, loss INTEGER,
            is_playoffs INTEGER, is_consolation INTEGER, is_bye_week INTEGER,
            franchise_name VARCHAR
        )
    """)

    def add_pair(year, league_key, slot, team_name, other_slot, points, other_points):
        other = f"opponent-{other_slot}"
        other_name = f"Raw opponent {other_slot}"
        rows = [
            (db_name, year, 1, f"{league_key}.t.{slot}", owner, owner,
             "Jonah", team_name, other_name, other, other,
             points, other_points, int(points > other_points), int(points < other_points),
             0, 0, 0, None),
            (db_name, year, 1, f"{league_key}.t.{other_slot}", other, other,
             other_name, f"Other {other_slot}", "Jonah", owner, owner,
             other_points, points, int(other_points > points), int(other_points < points),
             0, 0, 0, None),
        ]
        conn.executemany("INSERT INTO public.matchup VALUES (" + ",".join(["?"] * 19) + ")", rows)

    add_pair(2026, "470.l.109944", 1, "Dunesday Night Football", 2, 155.37, 149.12)
    conn.close()
    aliases = {"Raw opponent 2": "Saved opponent"}
    runner = _Runner(db_name=db_name, data_dir=str(tmp_path), manager_name_overrides=aliases)
    try:
        runner.resolve_hidden_managers()
        conn = runner._get_connection()
        quick_facts = conn.execute("""
            SELECT year, week, team_key, manager_guid, team_name, team_points,
                   opponent_points, win, loss, is_playoffs, is_consolation
            FROM public.matchup WHERE year = 2026 ORDER BY team_key
        """).fetchall()
        assert conn.execute("SELECT franchise_id FROM public.matchup WHERE manager_guid = ?", [owner]).fetchone() == (owner,)

        # The old linked season reveals three teams under the same account.
        add_pair(2003, "79.l.109944", 1, "Dunesday Night Football", 2, 101.25, 91.50)
        add_pair(2003, "79.l.109944", 3, "Ze` Furor", 4, 115.75, 85.25)
        add_pair(2003, "79.l.109944", 5, "Like A Boss", 6, 90.50, 110.10)
        all_facts = conn.execute("""
            SELECT year, week, team_key, manager_guid, team_name, team_points,
                   opponent_points, win, loss, is_playoffs, is_consolation
            FROM public.matchup ORDER BY year, team_key
        """).fetchall()

        runner.resolve_hidden_managers()
        assert conn.execute("""
            SELECT year, week, team_key, manager_guid, team_name, team_points,
                   opponent_points, win, loss, is_playoffs, is_consolation
            FROM public.matchup ORDER BY year, team_key
        """).fetchall() == all_facts
        assert [row for row in all_facts if row[0] == 2026] == quick_facts
        assert conn.execute("""
            SELECT COUNT(DISTINCT franchise_id) FROM public.matchup
            WHERE manager_guid = ? AND year = 2003
        """, [owner]).fetchone() == (3,)
        current = conn.execute("""
            SELECT manager, franchise_id FROM public.matchup
            WHERE year = 2026 AND manager_guid = ?
        """, [owner]).fetchone()
        assert current == ("Jonah - Dunesday Night Football", owner + "_1"), current
        saved_names = conn.execute("""
            SELECT DISTINCT manager FROM public.matchup WHERE manager_guid = 'opponent-2'
        """).fetchall()
        assert saved_names == [("Saved opponent",)], saved_names
        assert conn.execute("""
            SELECT COUNT(*) FROM public.matchup m JOIN public.matchup o
              ON m.year = o.year AND m.week = o.week
             AND m.team_points = o.opponent_points AND m.opponent_points = o.team_points
            WHERE m.opponent_franchise_id IS DISTINCT FROM o.franchise_id
        """).fetchone() == (0,)
        resolved = conn.execute("SELECT * FROM public.matchup ORDER BY year, team_key").fetchall()
        runner.resolve_hidden_managers()
        assert conn.execute("SELECT * FROM public.matchup ORDER BY year, team_key").fetchall() == resolved
        assert runner.manager_name_overrides == aliases
    finally:
        if runner._conn is not None:
            runner._conn.close()
