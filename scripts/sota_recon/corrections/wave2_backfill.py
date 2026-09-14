"""
sota_recon/corrections/wave2_backfill.py

Wave 2: identity- and anchor-based DEF backfills. No estimation. All joins use the
YEAR-AWARE franchise number (nfl_franchise_number), never a static team-code map,
so relocated franchises (STL Cardinals vs RAM Rams, BAL Colts vs BAL Ravens) never
collide.

  def_int_identity   A defense's interceptions in a game EQUAL the opposing QBs'
                     interceptions thrown (every INT is one thrown + one caught). This
                     ENFORCES the identity: overwrites def_interceptions wherever it
                     differs from the opposing offense aggregate (fills NULLs AND fixes
                     wrong values, e.g. an explicit 0 where 5 INTs were thrown).
  def_sack_identity  (1982+) team sacks EQUAL opposing QBs' sacks-suffered. Same identity.
  game_date_from_sched   game_date re-derived from the schedule anchor (overwrite).
  home_away_from_sched   home_away re-derived from the schedule anchor (overwrite).

Deterministic from authoritative inputs => idempotent.
"""

from __future__ import annotations

from ..sources import registry
from .framework import PROV_COL, _count, Correction


def _ensure_col(con, name, typ="VARCHAR"):
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if name not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {name} {typ}")


def _def_int_identity(con) -> int:
    # opposing offense INTs thrown, keyed by franchise: off_fn played at def_fn
    con.execute("""
        CREATE OR REPLACE TEMP VIEW off_int AS
        SELECT nfl_franchise_number AS off_fn, opponent_nfl_franchise_number AS def_fn,
               year, CAST(week AS INTEGER) AS wk, SUM(passing_interceptions) AS opp_int
        FROM st
        WHERE position IN ('QB','RB','WR','TE') AND passing_interceptions IS NOT NULL
        GROUP BY 1,2,3,4
        HAVING SUM(passing_interceptions) IS NOT NULL
    """)
    where = """position='DEF' AND EXISTS (
        SELECT 1 FROM off_int o WHERE o.def_fn=st.nfl_franchise_number
          AND o.off_fn=st.opponent_nfl_franchise_number
          AND o.year=st.year AND o.wk=CAST(st.week AS INTEGER)
          AND st.def_interceptions IS DISTINCT FROM o.opp_int)"""
    n = _count(con, where)
    con.execute(f"""
        UPDATE st SET
            def_interceptions = o.opp_int,
            {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave2.def_int_identity'
                              ELSE {PROV_COL} || ',wave2.def_int_identity' END
        FROM off_int o
        WHERE st.position='DEF'
          AND o.def_fn=st.nfl_franchise_number AND o.off_fn=st.opponent_nfl_franchise_number
          AND o.year=st.year AND o.wk=CAST(st.week AS INTEGER)
          AND st.def_interceptions IS DISTINCT FROM o.opp_int
    """)
    return n


def _def_sack_identity(con) -> int:
    con.execute("""
        CREATE OR REPLACE TEMP VIEW off_sack AS
        SELECT nfl_franchise_number AS off_fn, opponent_nfl_franchise_number AS def_fn,
               year, CAST(week AS INTEGER) AS wk, SUM(sacks_suffered) AS opp_sacks
        FROM st
        WHERE position IN ('QB','RB','WR','TE') AND sacks_suffered IS NOT NULL
        GROUP BY 1,2,3,4
        HAVING SUM(sacks_suffered) IS NOT NULL
    """)
    where = """position='DEF' AND year>=1982 AND EXISTS (
        SELECT 1 FROM off_sack o WHERE o.def_fn=st.nfl_franchise_number
          AND o.off_fn=st.opponent_nfl_franchise_number
          AND o.year=st.year AND o.wk=CAST(st.week AS INTEGER)
          AND st.def_sacks IS DISTINCT FROM o.opp_sacks)"""
    n = _count(con, where)
    con.execute(f"""
        UPDATE st SET
            def_sacks = o.opp_sacks,
            {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave2.def_sack_identity'
                              ELSE {PROV_COL} || ',wave2.def_sack_identity' END
        FROM off_sack o
        WHERE st.position='DEF' AND st.year>=1982
          AND o.def_fn=st.nfl_franchise_number AND o.off_fn=st.opponent_nfl_franchise_number
          AND o.year=st.year AND o.wk=CAST(st.week AS INTEGER)
          AND st.def_sacks IS DISTINCT FROM o.opp_sacks
    """)
    return n


def _game_date_from_sched(con) -> int:
    _ensure_col(con, "game_date", "TIMESTAMP")
    con.execute("UPDATE st SET game_date = NULL")  # reset (wave2-exclusive col) -> clean re-derive
    sched = registry()["schedule_master"].path
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW sched_d AS
        SELECT CAST(franchise_id AS INTEGER) AS fn, CAST(opponent_franchise_id AS INTEGER) AS ofn,
               year, week, game_date, home_away
        FROM '{sched}' WHERE franchise_id IS NOT NULL AND game_date IS NOT NULL
    """)
    where = """game_date IS NULL AND EXISTS (
        SELECT 1 FROM sched_d s WHERE s.fn=st.nfl_franchise_number
          AND s.ofn=st.opponent_nfl_franchise_number
          AND s.year=st.year AND s.week=CAST(st.week AS INTEGER))"""
    n = _count(con, where)
    con.execute(f"""
        UPDATE st SET
            game_date = s.game_date,
            {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave2.game_date_from_sched'
                              ELSE {PROV_COL} || ',wave2.game_date_from_sched' END
        FROM sched_d s
        WHERE st.game_date IS NULL
          AND s.fn=st.nfl_franchise_number AND s.ofn=st.opponent_nfl_franchise_number
          AND s.year=st.year AND s.week=CAST(st.week AS INTEGER)
    """)
    return n


def _home_away_from_sched(con) -> int:
    _ensure_col(con, "home_away", "VARCHAR")
    con.execute("UPDATE st SET home_away = NULL")  # reset (wave2-exclusive col) -> clean re-derive
    sched = registry()["schedule_master"].path
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW sched_h AS
        SELECT CAST(franchise_id AS INTEGER) AS fn, CAST(opponent_franchise_id AS INTEGER) AS ofn,
               year, week, home_away
        FROM '{sched}' WHERE franchise_id IS NOT NULL AND home_away IS NOT NULL
    """)
    where = """home_away IS NULL AND EXISTS (
        SELECT 1 FROM sched_h s WHERE s.fn=st.nfl_franchise_number
          AND s.ofn=st.opponent_nfl_franchise_number
          AND s.year=st.year AND s.week=CAST(st.week AS INTEGER))"""
    n = _count(con, where)
    con.execute(f"""
        UPDATE st SET
            home_away = s.home_away,
            {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave2.home_away_from_sched'
                              ELSE {PROV_COL} || ',wave2.home_away_from_sched' END
        FROM sched_h s
        WHERE st.home_away IS NULL
          AND s.fn=st.nfl_franchise_number AND s.ofn=st.opponent_nfl_franchise_number
          AND s.year=st.year AND s.week=CAST(st.week AS INTEGER)
    """)
    return n


CORRECTIONS = [
    Correction("wave2.def_int_identity",
               "def_interceptions = opposing offense INTs thrown (franchise identity, enforce)",
               _def_int_identity, None),
    Correction("wave2.def_sack_identity",
               "def_sacks = opposing offense sacks-suffered, 1982+ (franchise identity, enforce)",
               _def_sack_identity, None),
    Correction("wave2.game_date_from_sched",
               "game_date from schedule anchor (franchise join)",
               _game_date_from_sched, None),
    Correction("wave2.home_away_from_sched",
               "home_away from schedule anchor (franchise join)",
               _home_away_from_sched, None),
]
